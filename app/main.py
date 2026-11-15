"""FastAPI web entry point for the carbon-ledger standardizer."""

from __future__ import annotations

import os
import secrets
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .archive import ArchiveError, ArchiveStore
from .engine import Analysis, LedgerError, analyze
from .spec import Contract, ContractError, parse_contract

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

MAX_UPLOAD_BYTES = 10 * 1024 * 1024  # 10 MiB per file
RESULT_TTL_SECONDS = 30 * 60
MAX_RESULTS = 200

DEFAULT_ARCHIVE_DIR = BASE_DIR.parent / "data"


@asynccontextmanager
async def lifespan(app: FastAPI):
    data_dir = Path(os.environ.get("ARCHIVE_DATA_DIR", str(DEFAULT_ARCHIVE_DIR)))
    app.state.archive_store = ArchiveStore(data_dir)
    yield


app = FastAPI(title="碳核算台账标准化", docs_url=None, redoc_url=None, lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@dataclass
class StoredResult:
    filename: str
    payload: bytes
    created_at: float


class ResultStore:
    """Tiny in-memory store for validated downloads (nothing touches disk)."""

    def __init__(self, ttl: float = RESULT_TTL_SECONDS, capacity: int = MAX_RESULTS):
        self._items: dict[str, StoredResult] = {}
        self._lock = threading.Lock()
        self._ttl = ttl
        self._capacity = capacity

    def put(self, filename: str, payload: bytes) -> str:
        token = secrets.token_urlsafe(24)
        now = time.monotonic()
        with self._lock:
            self._prune(now)
            while len(self._items) >= self._capacity:
                oldest = min(self._items, key=lambda key: self._items[key].created_at)
                del self._items[oldest]
            self._items[token] = StoredResult(filename, payload, now)
        return token

    def get(self, token: str) -> StoredResult | None:
        with self._lock:
            self._prune(time.monotonic())
            return self._items.get(token)

    def _prune(self, now: float) -> None:
        expired = [
            key for key, item in self._items.items() if now - item.created_at > self._ttl
        ]
        for key in expired:
            del self._items[key]


store = ResultStore()


def _safe_stem(filename: str | None) -> str:
    """ASCII-only stem: the Content-Disposition header must be latin-1 encodable."""
    name = Path(filename or "ledger").name
    stem = name.rsplit(".", 1)[0] if "." in name else name
    cleaned = "".join(
        ch if (ch.isascii() and (ch.isalnum() or ch in "-_")) else "_" for ch in stem
    )
    cleaned = cleaned.strip("_")[:80]
    return cleaned or "ledger"


def _render(
    request: Request,
    template: str,
    context: dict,
    status_code: int = 200,
) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name=template,
        context=context,
        status_code=status_code,
    )


async def _read_upload_pair(
    contract_upload: UploadFile | None, ledger_upload: UploadFile | None
) -> tuple[bytes, bytes, list[str]]:
    """Read both uploads and report size/presence problems."""
    errors: list[str] = []

    contract_raw = b""
    if contract_upload is not None:
        contract_raw = await contract_upload.read(MAX_UPLOAD_BYTES + 1)
    ledger_raw = b""
    if ledger_upload is not None:
        ledger_raw = await ledger_upload.read(MAX_UPLOAD_BYTES + 1)

    if len(contract_raw) > MAX_UPLOAD_BYTES:
        errors.append("字段合同文件超过 10 MiB 上限")
    elif not contract_raw:
        errors.append("请上传字段合同 JSON 文件")
    if len(ledger_raw) > MAX_UPLOAD_BYTES:
        errors.append("台账文件超过 10 MiB 上限")
    elif not ledger_raw:
        errors.append("请上传台账 CSV 文件")
    return contract_raw, ledger_raw, errors


def _validate_upload(
    contract_raw: bytes, ledger_raw: bytes, errors: list[str]
) -> tuple[Contract | None, Analysis | None]:
    """Parse the contract and run the whole-file mapping + row validation."""
    contract: Contract | None = None
    if not errors:
        try:
            contract = parse_contract(contract_raw)
        except ContractError as exc:
            errors.append(str(exc))

    analysis: Analysis | None = None
    if not errors and contract is not None:
        try:
            analysis = analyze(contract, ledger_raw)
        except LedgerError as exc:
            errors.append(str(exc))
    return contract, analysis


def _mapping_entries(contract: Contract, analysis: Analysis) -> list[dict]:
    """One entry per contract field, in contract order (None = no source)."""
    source_for = {a.field.name: a for a in analysis.mapping}
    entries = []
    for field_spec in contract.fields:
        assignment = source_for.get(field_spec.name)
        entries.append(
            {
                "standard_column": field_spec.name,
                "type": field_spec.type,
                "required": field_spec.required,
                "source_header": assignment.source_header if assignment else None,
                "source_column": (assignment.source_index + 1) if assignment else None,
            }
        )
    return entries


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return _render(request, "index.html", {"request": request})


@app.get("/healthz")
async def healthz():
    return JSONResponse({"status": "ok"})


@app.post("/standardize", response_class=HTMLResponse)
async def standardize(
    request: Request,
    contract_upload: UploadFile | None = File(default=None, alias="contract"),
    ledger_upload: UploadFile | None = File(default=None, alias="ledger"),
):
    contract_raw, ledger_raw, upload_errors = await _read_upload_pair(
        contract_upload, ledger_upload
    )
    contract, analysis = _validate_upload(contract_raw, ledger_raw, upload_errors)

    download_url = None
    if analysis is not None and analysis.valid and analysis.output_csv is not None:
        token = store.put(
            f"{_safe_stem(ledger_upload.filename if ledger_upload else None)}"
            "_standardized.csv",
            analysis.output_csv,
        )
        download_url = f"/download/{token}"

    status = 200 if analysis is not None and analysis.valid else 422
    return _render(
        request,
        "result.html",
        {
            "request": request,
            "upload_errors": upload_errors,
            "analysis": analysis,
            "contract": contract,
            "download_url": download_url,
            "contract_name": (contract_upload.filename if contract_upload else None)
            or "—",
            "ledger_name": (ledger_upload.filename if ledger_upload else None) or "—",
        },
        status_code=status,
    )


@app.get("/download/{token}")
async def download(token: str):
    item = store.get(token)
    if item is None:
        return HTMLResponse(
            "<!doctype html><meta charset='utf-8'>"
            "<p>下载链接已过期或不存在，请重新上传并校验。</p>",
            status_code=404,
        )
    return Response(
        content=item.payload,
        media_type="text/csv; charset=utf-8",
        headers={
            # Filename is restricted to [A-Za-z0-9_-] plus a fixed suffix.
            "Content-Disposition": f'attachment; filename="{item.filename}"',
            "Cache-Control": "no-store",
        },
    )


# ---------------------------------------------------------------- archive --


def _archive_store(request: Request) -> ArchiveStore:
    return request.app.state.archive_store


@app.get("/archive", response_class=HTMLResponse)
async def archive_index(request: Request):
    batches = _archive_store(request).list_batches()
    return _render(request, "archive.html", {"request": request, "batches": batches})


@app.post("/archive/publish", response_class=HTMLResponse)
async def archive_publish(
    request: Request,
    contract_upload: UploadFile | None = File(default=None, alias="contract"),
    ledger_upload: UploadFile | None = File(default=None, alias="ledger"),
):
    contract_raw, ledger_raw, upload_errors = await _read_upload_pair(
        contract_upload, ledger_upload
    )
    contract, analysis = _validate_upload(contract_raw, ledger_raw, upload_errors)

    context = {
        "request": request,
        "upload_errors": upload_errors,
        "analysis": analysis,
        "contract": contract,
        "publish_error": None,
        "contract_name": (contract_upload.filename if contract_upload else None) or "—",
        "ledger_name": (ledger_upload.filename if ledger_upload else None) or "—",
    }

    if analysis is None or not analysis.valid or analysis.output_csv is None:
        return _render(request, "archive_result.html", context, status_code=422)
    if contract is None:  # pragma: no cover - analysis implies a contract
        return _render(request, "archive_result.html", context, status_code=422)

    try:
        batch = _archive_store(request).publish(
            contract_bytes=contract_raw,
            ledger_bytes=ledger_raw,
            standardized_csv=analysis.output_csv,
            contract_filename=contract_upload.filename if contract_upload else None,
            ledger_filename=ledger_upload.filename if ledger_upload else None,
            mapping_entries=_mapping_entries(contract, analysis),
            ignored_columns=analysis.ignored_columns,
            row_count=analysis.row_count,
        )
    except ArchiveError as exc:
        context["publish_error"] = str(exc)
        return _render(request, "archive_result.html", context, status_code=500)

    return RedirectResponse(
        f"/archive/batches/{batch.batch_id}", status_code=303
    )


@app.get("/archive/batches/{batch_id}", response_class=HTMLResponse)
async def archive_batch(request: Request, batch_id: str):
    batch = _archive_store(request).get_batch(batch_id)
    if batch is None:
        return _render(
            request,
            "archive_missing.html",
            {"request": request, "batch_id": batch_id},
            status_code=404,
        )
    return _render(
        request, "archive_batch.html", {"request": request, "batch": batch}
    )


@app.get("/archive/batches/{batch_id}/files/{kind}")
async def archive_file(request: Request, batch_id: str, kind: str):
    resolved = _archive_store(request).resolve_file(batch_id, kind)
    if resolved is None:
        return HTMLResponse(
            "<!doctype html><meta charset='utf-8'>"
            "<p>批次或文件不存在。归档批次一旦发布即可完整下载，"
            "请回到归档页核对批次号。</p>",
            status_code=404,
        )
    path, download_name, media_type = resolved
    return FileResponse(
        path,
        media_type=media_type,
        filename=download_name,
        headers={"Cache-Control": "no-store"},
    )
