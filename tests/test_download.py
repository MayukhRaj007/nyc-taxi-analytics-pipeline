import urllib.error

import pytest

from taxi_lib import download
from taxi_lib.download import download_month, month_path, month_url


def test_month_url_and_path(tmp_path):
    assert month_url("2025-03").endswith("/trip-data/yellow_tripdata_2025-03.parquet")
    assert month_path("2025-03", tmp_path) == tmp_path / "raw" / "yellow_tripdata_2025-03.parquet"


def test_existing_file_is_not_downloaded_again(tmp_path, monkeypatch):
    target = month_path("2025-01", tmp_path)
    target.parent.mkdir(parents=True)
    target.write_bytes(b"already here")

    def boom(*_a, **_k):
        raise AssertionError("must not hit the network when the file exists")

    monkeypatch.setattr(download.urllib.request, "urlopen", boom)
    assert download_month("2025-01", tmp_path) == target
    assert target.read_bytes() == b"already here"


def test_failed_download_leaves_no_partial_file(tmp_path, monkeypatch):
    def fail(*_a, **_k):
        raise urllib.error.URLError("network down")

    monkeypatch.setattr(download.urllib.request, "urlopen", fail)
    with pytest.raises(urllib.error.URLError):
        download_month("2025-01", tmp_path)

    leftovers = list((tmp_path / "raw").glob("*"))
    assert leftovers == [], f"unexpected files: {leftovers}"


def test_successful_download_writes_final_file(tmp_path, monkeypatch):
    class FakeResponse:
        def __init__(self):
            self._chunks = [b"abc", b"def", b""]

        def read(self, _n):
            return self._chunks.pop(0)

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

    monkeypatch.setattr(download.urllib.request, "urlopen", lambda *_a, **_k: FakeResponse())
    path = download_month("2025-02", tmp_path)
    assert path.read_bytes() == b"abcdef"
    assert not path.with_suffix(".parquet.part").exists()
