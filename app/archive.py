"""Immutable batch archive for validated standardization runs.

A *batch* bundles everything a warehouse auditor needs to re-derive a
standardized CSV: the two original uploads (field contract JSON + ledger
CSV), the standardized result CSV, the field mapping, and the SHA-256 of
the three data files.

Storage layout under the data directory::

    <data_dir>/
      archive.db            SQLite registry (the only source of "queryable")
      batches/<batch_id>/   published, immutable file sets
        contract.json       original field contract upload
        ledger.csv          original ledger upload
        standardized.csv    standard-column result
        mapping.json        field mapping + hashes + metadata
      staging/              write-then-rename scratch space (cleaned on boot)

Publishing protocol (all-or-nothing):

1. Files are written to a fresh ``staging/`` directory and fsynced.
2. The staging directory is atomically renamed to ``batches/<batch_id>``.
3. The batch row is inserted and committed in SQLite.

If any step fails, every file written for the batch is removed and nothing
is registered, so a half-published batch can never be queried or listed.
A batch becomes visible only after the SQLite commit succeeds; files left
behind by a crash between the rename and the commit are swept on startup
(``_reconcile``), keeping "registered in SQLite" and "files on disk" in
lockstep.  Published batches are never modified or deleted by this module.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BATCHES_DIR_NAME = "batches"
STAGING_DIR_NAME = "staging"
DB_FILE_NAME = "archive.db"

# Canonical file names inside a batch directory.  Fixed names keep downloads
# path-safe regardless of how the original uploads were named.
CONTRACT_FILE = "contract.json"
LEDGER_FILE = "ledger.csv"
STANDARDIZED_FILE = "standardized.csv"
MAPPING_FILE = "mapping.json"

FILE_KINDS = ("contract", "ledger", "standardized", "mapping")
_KIND_TO_FILE = {
    "contract": CONTRACT_FILE,
    "ledger": LEDGER_FILE,
    "standardized": STANDARDIZED_FILE,
    "mapping": MAPPING_FILE,
}
_KIND_TO_MEDIA_TYPE = {
    "contract": "application/json; charset=utf-8",
    "ledger": "text/csv; charset=utf-8",
    "standardized": "text/csv; charset=utf-8",
    "mapping": "application/json; charset=utf-8",
}

BATCH_ID_RE = re.compile(r"B[0-9]{8}-[0-9A-F]{8}")

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id            TEXT PRIMARY KEY,
    created_at          TEXT NOT NULL,
    contract_filename   TEXT NOT NULL,
    ledger_filename     TEXT NOT NULL,
    row_count           INTEGER NOT NULL,
    contract_sha256     TEXT NOT NULL,
    ledger_sha256       TEXT NOT NULL,
    standardized_sha256 TEXT NOT NULL,
    mapping_json        TEXT NOT NULL
);
"""

MAX_STORED_FILENAME = 200


class ArchiveError(RuntimeError):
    """Publishing failed; the store was left without any queryable batch."""


@dataclass(frozen=True)
class Batch:
    """A registered batch as seen through the SQLite registry."""

    batch_id: str
    created_at: str
    contract_filename: str
    ledger_filename: str
    row_count: int
    contract_sha256: str
    ledger_sha256: str
    standardized_sha256: str
    mapping_doc: dict[str, Any] = field(compare=False)

    @property
    def field_mapping(self) -> list[dict[str, Any]]:
        return list(self.mapping_doc.get("field_mapping", []))

    @property
    def ignored_columns(self) -> list[str]:
        return list(self.mapping_doc.get("ignored_columns", []))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _new_batch_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"B{stamp}-{secrets.token_hex(4).upper()}"


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _clip_filename(name: str | None, fallback: str) -> str:
    cleaned = Path(name or "").name.strip() or fallback
    return cleaned[:MAX_STORED_FILENAME]


class ArchiveStore:
    """File + SQLite backed registry of immutable batches.

    A single process may share one instance across threads; SQLite is
    opened per operation and the registry insert is serialized by a lock.
    """

    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        self.batches_dir = self.data_dir / BATCHES_DIR_NAME
        self.staging_dir = self.data_dir / STAGING_DIR_NAME
        self.db_path = self.data_dir / DB_FILE_NAME
        self._lock = threading.Lock()
        self.batches_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._reconcile()

    # ------------------------------------------------------------- sqlite --

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    def _register(self, row: tuple) -> None:
        """Insert the batch row and commit; raise if the commit fails."""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    INSERT INTO batches (
                        batch_id, created_at, contract_filename,
                        ledger_filename, row_count, contract_sha256,
                        ledger_sha256, standardized_sha256, mapping_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    row,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    # ------------------------------------------------------------ cleanup --

    def _reconcile(self) -> None:
        """Drop on-disk leftovers that no committed registry row points to.

        Runs once at startup: covers crashes between the directory rename
        and the SQLite commit, and interrupted cleanups.  Registered
        batches are never touched.
        """
        conn = self._connect()
        try:
            registered = {
                str(row[0]) for row in conn.execute("SELECT batch_id FROM batches")
            }
        finally:
            conn.close()
        for child in self.staging_dir.iterdir():
            self._remove(child)
        for child in self.batches_dir.iterdir():
            if not child.is_dir() or child.name not in registered:
                self._remove(child)

    @staticmethod
    def _remove(path: Path) -> None:
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)

    # ----------------------------------------------------------- publishing --

    @staticmethod
    def _write_file(path: Path, payload: bytes) -> None:
        with open(path, "xb") as handle:  # fail loudly if it ever exists
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:  # pragma: no cover - platform dependent
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def publish(
        self,
        *,
        contract_bytes: bytes,
        ledger_bytes: bytes,
        standardized_csv: bytes,
        contract_filename: str | None,
        ledger_filename: str | None,
        mapping_entries: list[dict[str, Any]],
        ignored_columns: list[str],
        row_count: int,
    ) -> Batch:
        """Publish one immutable batch; raise ArchiveError on any failure.

        Either the returned batch is fully registered and downloadable, or
        no trace of it remains queryable and its files are removed.
        """
        contract_name = _clip_filename(contract_filename, "contract.json")
        ledger_name = _clip_filename(ledger_filename, "ledger.csv")
        digests = {
            "contract": _sha256(contract_bytes),
            "ledger": _sha256(ledger_bytes),
            "standardized": _sha256(standardized_csv),
        }

        last_error: Exception | None = None
        for _attempt in range(3):  # retry only covers a batch-id collision
            batch_id = _new_batch_id()
            created_at = _utc_now_iso()
            mapping_doc = {
                "batch_id": batch_id,
                "created_at": created_at,
                "source_files": {
                    "contract": {
                        "stored_as": CONTRACT_FILE,
                        "original_filename": contract_name,
                        "sha256": digests["contract"],
                    },
                    "ledger": {
                        "stored_as": LEDGER_FILE,
                        "original_filename": ledger_name,
                        "sha256": digests["ledger"],
                    },
                },
                "result": {
                    "stored_as": STANDARDIZED_FILE,
                    "sha256": digests["standardized"],
                    "row_count": row_count,
                },
                "field_mapping": mapping_entries,
                "ignored_columns": list(ignored_columns),
            }
            mapping_bytes = json.dumps(
                mapping_doc, ensure_ascii=False, indent=2
            ).encode("utf-8")
            files = {
                CONTRACT_FILE: contract_bytes,
                LEDGER_FILE: ledger_bytes,
                STANDARDIZED_FILE: standardized_csv,
                MAPPING_FILE: mapping_bytes,
            }
            try:
                # File stage cleans up after itself; if it raised, nothing
                # queryable exists and no unregistered files remain.
                self._publish_files(batch_id, files)
            except Exception as exc:
                raise ArchiveError(
                    "批次发布失败：已清理未登记的文件，未产生任何可查批次"
                ) from exc
            # The rename succeeded, so batches/<batch_id> is definitely the
            # file set written above.  If registration now fails, those
            # files must not survive as unregistered leftovers.
            try:
                self._register(
                    (
                        batch_id,
                        created_at,
                        contract_name,
                        ledger_name,
                        row_count,
                        digests["contract"],
                        digests["ledger"],
                        digests["standardized"],
                        json.dumps(mapping_doc, ensure_ascii=False),
                    )
                )
            except sqlite3.IntegrityError as exc:
                # Primary-key collision on a fresh random id; the directory
                # we just renamed is ours, remove it and retry once more.
                self._remove(self.batches_dir / batch_id)
                last_error = exc
                continue
            except Exception as exc:
                self._remove(self.batches_dir / batch_id)
                raise ArchiveError(
                    "批次登记失败：已清理落盘文件，未产生任何可查批次"
                ) from exc
            return Batch(
                batch_id=batch_id,
                created_at=created_at,
                contract_filename=contract_name,
                ledger_filename=ledger_name,
                row_count=row_count,
                contract_sha256=digests["contract"],
                ledger_sha256=digests["ledger"],
                standardized_sha256=digests["standardized"],
                mapping_doc=mapping_doc,
            )
        raise ArchiveError("批次号分配失败，请重试") from last_error

    def _publish_files(self, batch_id: str, files: dict[str, bytes]) -> None:
        """Write all batch files into place; clean up only what we wrote.

        ``final`` is removed solely when this invocation successfully
        renamed its own staging directory onto it — a pre-existing
        directory under the same id (a live earlier batch) is never
        touched.
        """
        staging = self.staging_dir / f"{batch_id}.{secrets.token_hex(4)}"
        final = self.batches_dir / batch_id
        renamed = False
        try:
            staging.mkdir(parents=True, exist_ok=False)
            for name, payload in files.items():
                self._write_file(staging / name, payload)
            self._fsync_dir(staging)
            os.replace(staging, final)  # same filesystem: atomic rename
            renamed = True
            self._fsync_dir(self.batches_dir)
        except Exception:
            # Saving originals or the result failed midway: remove every
            # byte written for this batch so nothing unregistered remains.
            self._remove(staging)
            if renamed:
                self._remove(final)
            raise

    # ------------------------------------------------------------- queries --

    @staticmethod
    def _row_to_batch(row: sqlite3.Row) -> Batch:
        return Batch(
            batch_id=row["batch_id"],
            created_at=row["created_at"],
            contract_filename=row["contract_filename"],
            ledger_filename=row["ledger_filename"],
            row_count=row["row_count"],
            contract_sha256=row["contract_sha256"],
            ledger_sha256=row["ledger_sha256"],
            standardized_sha256=row["standardized_sha256"],
            mapping_doc=json.loads(row["mapping_json"]),
        )

    def list_batches(self) -> list[Batch]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM batches ORDER BY created_at DESC, batch_id DESC"
            ).fetchall()
        finally:
            conn.close()
        return [self._row_to_batch(row) for row in rows]

    def get_batch(self, batch_id: str) -> Batch | None:
        if not BATCH_ID_RE.fullmatch(batch_id):
            return None
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        finally:
            conn.close()
        return self._row_to_batch(row) if row is not None else None

    def resolve_file(
        self, batch_id: str, kind: str
    ) -> tuple[Path, str, str] | None:
        """Return (path, download filename, media type) for a batch file.

        Only registered batches and the four canonical kinds resolve, so a
        published batch is always fully downloadable and nothing else on
        disk is reachable.
        """
        if kind not in _KIND_TO_FILE or self.get_batch(batch_id) is None:
            return None
        path = (self.batches_dir / batch_id / _KIND_TO_FILE[kind]).resolve()
        if path.parent != self.batches_dir.resolve() / batch_id:
            return None  # pragma: no cover - batch ids cannot contain '/'
        if not path.is_file():
            return None
        download_name = f"{batch_id}_{kind}{path.suffix}"
        return path, download_name, _KIND_TO_MEDIA_TYPE[kind]
