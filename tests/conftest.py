from pathlib import Path

import pytest

SAMPLE = Path(__file__).resolve().parent.parent / "sample_data" / "yellow_tripdata_sample_2025-01.parquet"


@pytest.fixture
def sample_parquet() -> Path:
    assert SAMPLE.exists(), "run scripts/make_sample.py to create the sample file"
    return SAMPLE


@pytest.fixture
def db_path(tmp_path) -> str:
    return str(tmp_path / "test.duckdb")
