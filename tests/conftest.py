import pytest


@pytest.fixture(autouse=True)
def archive_data_dir(monkeypatch, tmp_path):
    """Point every app's archive store at a throwaway per-test directory."""
    monkeypatch.setenv("ARCHIVE_DATA_DIR", str(tmp_path / "archive-data"))
