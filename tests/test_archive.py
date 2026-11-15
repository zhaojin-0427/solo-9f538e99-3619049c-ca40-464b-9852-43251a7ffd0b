import hashlib
import json
import re
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.archive import ArchiveStore
from app.main import app

CONTRACT = {
    "fields": [
        {"name": "厂区", "aliases": ["工厂", "site"], "type": "text", "required": True},
        {"name": "月份", "aliases": ["统计月份", "period"], "type": "month", "required": True},
        {"name": "用电量(kWh)", "aliases": ["电量", "kwh"], "type": "decimal", "required": True},
        {"name": "抄表次数", "aliases": ["次数"], "type": "integer", "required": False},
    ]
}

GOOD_CSV = (
    "site,period,kwh,次数,备注列\n"
    "华东厂,2025-01,12.5,3,忽略\n"
    "华南厂,2025-02,8.25,,\n"
)

BAD_CSV = (
    "site,period,kwh,次数\n"
    "华东厂,2025-01,12.5,3\n"
    "华南厂,2025-02,八,\n"
)

BATCH_ID_RE = re.compile(r"B\d{8}-[0-9A-F]{8}")


@pytest.fixture()
def client():
    with TestClient(app) as test_client:
        yield test_client


def contract_bytes():
    return json.dumps(CONTRACT, ensure_ascii=False).encode("utf-8")


def publish(client, csv_text=GOOD_CSV, contract_name="contract.json", ledger_name="ledger.csv"):
    return client.post(
        "/archive/publish",
        files={
            "contract": (contract_name, contract_bytes(), "application/json"),
            "ledger": (ledger_name, csv_text.encode("utf-8"), "text/csv"),
        },
        follow_redirects=False,
    )


def batch_id_from(response):
    assert response.status_code == 303, response.text
    location = response.headers["location"]
    batch_id = location.rsplit("/", 1)[-1]
    assert BATCH_ID_RE.fullmatch(batch_id)
    return batch_id


def data_dir():
    import os

    return Path(os.environ["ARCHIVE_DATA_DIR"])


def test_archive_page_renders_form_and_empty_list(client):
    response = client.get("/archive")
    assert response.status_code == 200
    assert 'action="/archive/publish"' in response.text
    assert 'enctype="multipart/form-data"' in response.text
    assert "尚无批次" in response.text


def test_publish_success_redirects_to_queryable_batch(client):
    response = publish(client)
    batch_id = batch_id_from(response)

    detail = client.get(f"/archive/batches/{batch_id}")
    assert detail.status_code == 200
    assert batch_id in detail.text
    assert "不可变批次已发布" in detail.text
    # Mapping rows: standard column <- source header from the ledger file.
    assert "厂区" in detail.text and "<code>site</code>" in detail.text
    assert "用电量(kWh)" in detail.text and "<code>kwh</code>" in detail.text
    assert "已忽略的额外列" in detail.text and "备注列" in detail.text
    # All three SHA-256 digests are displayed in full.
    for digest in re.findall(r'class="hash">([0-9a-f]{64})<', detail.text):
        assert len(digest) == 64
    assert len(re.findall(r'class="hash">[0-9a-f]{64}<', detail.text)) == 3

    listing = client.get("/archive")
    assert batch_id in listing.text
    assert "ledger.csv" in listing.text


def test_published_batch_files_download_with_original_bytes(client):
    response = publish(client, ledger_name="华东台账.csv")
    batch_id = batch_id_from(response)

    contract_dl = client.get(f"/archive/batches/{batch_id}/files/contract")
    assert contract_dl.status_code == 200
    assert contract_dl.content == contract_bytes()
    assert "attachment" in contract_dl.headers["content-disposition"]

    ledger_dl = client.get(f"/archive/batches/{batch_id}/files/ledger")
    assert ledger_dl.status_code == 200
    assert ledger_dl.content == GOOD_CSV.encode("utf-8")

    std_dl = client.get(f"/archive/batches/{batch_id}/files/standardized")
    assert std_dl.status_code == 200
    lines = std_dl.content.decode("utf-8-sig").splitlines()
    assert lines[0] == "厂区,月份,用电量(kWh),抄表次数"
    assert lines[1] == "华东厂,2025-01,12.5,3"

    mapping_dl = client.get(f"/archive/batches/{batch_id}/files/mapping")
    assert mapping_dl.status_code == 200
    doc = json.loads(mapping_dl.content.decode("utf-8"))
    assert doc["batch_id"] == batch_id
    assert doc["source_files"]["contract"]["original_filename"] == "contract.json"
    assert doc["source_files"]["ledger"]["original_filename"] == "华东台账.csv"
    assert doc["source_files"]["ledger"]["sha256"] == hashlib.sha256(
        GOOD_CSV.encode("utf-8")
    ).hexdigest()
    assert doc["result"]["sha256"] == hashlib.sha256(std_dl.content).hexdigest()
    assert doc["result"]["row_count"] == 2
    mapping = {entry["standard_column"]: entry for entry in doc["field_mapping"]}
    assert mapping["厂区"]["source_header"] == "site"
    assert mapping["厂区"]["source_column"] == 1
    assert mapping["用电量(kWh)"]["source_header"] == "kwh"
    assert mapping["用电量(kWh)"]["source_column"] == 3
    assert mapping["抄表次数"]["source_header"] == "次数"
    assert doc["ignored_columns"]

    # Digests shown on the detail page match the downloaded bytes.
    detail = client.get(f"/archive/batches/{batch_id}")
    assert hashlib.sha256(contract_dl.content).hexdigest() in detail.text
    assert hashlib.sha256(ledger_dl.content).hexdigest() in detail.text
    assert hashlib.sha256(std_dl.content).hexdigest() in detail.text


def test_failed_validation_publishes_nothing(client):
    response = publish(client, csv_text=BAD_CSV)
    assert response.status_code == 422
    assert "未发布任何批次" in response.text
    assert "类型不符" in response.text

    listing = client.get("/archive")
    assert "尚无批次" in listing.text
    assert not list((data_dir() / "batches").iterdir())


def test_upload_errors_publish_nothing(client):
    response = client.post(
        "/archive/publish",
        files={"contract": ("contract.json", b"{oops", "application/json")},
        follow_redirects=False,
    )
    assert response.status_code == 422
    assert "请上传台账 CSV 文件" in response.text
    assert "尚无批次" in client.get("/archive").text


def test_batches_survive_restart(tmp_path, monkeypatch):
    shared = tmp_path / "shared-data"
    monkeypatch.setenv("ARCHIVE_DATA_DIR", str(shared))

    with TestClient(app) as client1:
        response = publish(client1)
        batch_id = batch_id_from(response)

    # A brand-new app instance (fresh ArchiveStore) over the same data dir.
    with TestClient(app) as client2:
        listing = client2.get("/archive")
        assert batch_id in listing.text
        detail = client2.get(f"/archive/batches/{batch_id}")
        assert detail.status_code == 200
        assert "不可变批次已发布" in detail.text
        for kind in ("contract", "ledger", "standardized", "mapping"):
            download = client2.get(f"/archive/batches/{batch_id}/files/{kind}")
            assert download.status_code == 200, kind
        ledger_dl = client2.get(f"/archive/batches/{batch_id}/files/ledger")
        assert ledger_dl.content == GOOD_CSV.encode("utf-8")


def test_file_write_failure_leaves_no_queryable_batch(client, monkeypatch):
    original = ArchiveStore._write_file
    calls = []

    def fail_on_second(self, path, payload):
        calls.append(path.name)
        if len(calls) == 2:
            raise OSError("injected disk failure")
        return original(path, payload)

    monkeypatch.setattr(ArchiveStore, "_write_file", fail_on_second)

    response = publish(client)
    assert response.status_code == 500
    assert "未产生任何可查批次" in response.text

    assert "尚无批次" in client.get("/archive").text
    assert not list((data_dir() / "batches").iterdir())
    assert not list((data_dir() / "staging").iterdir())


def test_sqlite_commit_failure_cleans_up_files(client, monkeypatch):
    class CommitFailingConnection(sqlite3.Connection):
        def commit(self):
            raise sqlite3.OperationalError("injected commit failure")

    original_connect = ArchiveStore._connect

    def connect_failing(self):
        conn = sqlite3.connect(
            self.db_path, timeout=30, factory=CommitFailingConnection
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    # Patch after startup so schema creation is unaffected.
    monkeypatch.setattr(ArchiveStore, "_connect", connect_failing)

    response = publish(client)
    assert response.status_code == 500
    assert "未产生任何可查批次" in response.text

    # Restore normal connections so the store works again.
    monkeypatch.setattr(ArchiveStore, "_connect", original_connect)
    assert "尚无批次" in client.get("/archive").text
    assert not list((data_dir() / "batches").iterdir())
    assert not list((data_dir() / "staging").iterdir())


def test_orphan_files_are_swept_on_startup(tmp_path, monkeypatch):
    shared = tmp_path / "shared-data"
    monkeypatch.setenv("ARCHIVE_DATA_DIR", str(shared))

    with TestClient(app) as client1:
        batch_id = batch_id_from(publish(client1))

    # Simulate a crash: files on disk that were never registered.
    orphan = shared / "batches" / "B20990101-DEADBEEF"
    orphan.mkdir(parents=True)
    (orphan / "contract.json").write_bytes(b"{}")
    leftover = shared / "staging" / "half-written"
    leftover.mkdir(parents=True)
    (leftover / "ledger.csv").write_bytes(b"x")

    with TestClient(app) as client2:
        assert not orphan.exists()
        assert not leftover.exists()
        # The registered batch is untouched and fully downloadable.
        detail = client2.get(f"/archive/batches/{batch_id}")
        assert detail.status_code == 200
        download = client2.get(f"/archive/batches/{batch_id}/files/standardized")
        assert download.status_code == 200
        assert download.content.decode("utf-8-sig").splitlines()[0] == (
            "厂区,月份,用电量(kWh),抄表次数"
        )


def test_unknown_or_malformed_batch_id_is_404(client):
    assert client.get("/archive/batches/B20990101-DEADBEEF").status_code == 404
    assert "批次不存在" in client.get("/archive/batches/B20990101-DEADBEEF").text
    assert client.get("/archive/batches/not-a-batch").status_code == 404
    assert client.get(
        "/archive/batches/B20990101-DEADBEEF/files/contract"
    ).status_code == 404


def test_unknown_file_kind_is_404(client):
    batch_id = batch_id_from(publish(client))
    assert client.get(f"/archive/batches/{batch_id}/files/evil").status_code == 404


def test_standardize_endpoint_still_works(client):
    response = client.post(
        "/standardize",
        files={
            "contract": ("contract.json", contract_bytes(), "application/json"),
            "ledger": ("ledger.csv", GOOD_CSV.encode("utf-8"), "text/csv"),
        },
    )
    assert response.status_code == 200
    assert "校验通过" in response.text
    # The ad-hoc standardize flow publishes nothing to the archive.
    assert "尚无批次" in client.get("/archive").text
