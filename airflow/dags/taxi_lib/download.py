"""Download one month of NYC TLC yellow-taxi trip data."""

from __future__ import annotations

import logging
import urllib.request
from pathlib import Path

from taxi_lib.months import validate_month

log = logging.getLogger(__name__)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
CHUNK_BYTES = 1024 * 1024


def month_url(month: str) -> str:
    return f"{BASE_URL}/yellow_tripdata_{validate_month(month)}.parquet"


def month_path(month: str, data_dir: str | Path) -> Path:
    return Path(data_dir) / "raw" / f"yellow_tripdata_{validate_month(month)}.parquet"


def download_month(month: str, data_dir: str | Path, *, timeout: int = 60) -> Path:
    """Fetch the month's Parquet file; do nothing if it already exists.

    Idempotent: a finished file is never re-downloaded. The download goes to a
    ``.part`` file that is renamed only on success, so an interrupted run can
    never leave a truncated file that later looks "present".
    """
    target = month_path(month, data_dir)
    if target.exists() and target.stat().st_size > 0:
        log.info("%s already present (%.1f MB), skipping download", target.name, target.stat().st_size / 1e6)
        return target

    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(".parquet.part")
    url = month_url(month)
    log.info("Downloading %s", url)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp, partial.open("wb") as out:  # noqa: S310
            while chunk := resp.read(CHUNK_BYTES):
                out.write(chunk)
        partial.replace(target)
    finally:
        partial.unlink(missing_ok=True)
    log.info("Saved %s (%.1f MB)", target.name, target.stat().st_size / 1e6)
    return target
