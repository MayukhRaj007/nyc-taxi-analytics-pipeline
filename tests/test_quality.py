import duckdb
import pytest

from taxi_lib.load import load_month
from taxi_lib.quality import DataQualityError, check_quality


@pytest.fixture
def warehouse(sample_parquet, db_path) -> str:
    """Raw January loaded, plus a stand-in fct_trips holding every row (all dated mid-January)."""
    load_month(sample_parquet, "2025-01", db_path)
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA marts")
    con.execute("CREATE TABLE marts.fct_trips AS SELECT DATE '2025-01-15' AS pickup_date FROM raw.yellow_trips")
    con.close()
    return db_path


def test_all_checks_pass_on_healthy_month(warehouse):
    results = check_quality(warehouse, "2025-01")
    assert all(r.passed for r in results)
    assert {r.name for r in results} >= {"raw_row_count", "fct_trips_retention", "null_rate[fare_amount]"}


def test_too_few_rows_fails_with_readable_message(warehouse):
    with pytest.raises(DataQualityError, match=r"raw_row_count -> 3,000 rows \(minimum 5,000\)"):
        check_quality(warehouse, "2025-01", min_rows=5_000)


def test_missing_month_fails(warehouse):
    with pytest.raises(DataQualityError, match="raw_row_count"):
        check_quality(warehouse, "2025-02")


def test_critical_null_rate_fails(warehouse):
    con = duckdb.connect(warehouse)
    con.execute("UPDATE raw.yellow_trips SET fare_amount = NULL WHERE rowid % 10 = 0")
    con.close()
    with pytest.raises(DataQualityError, match=r"null_rate\[fare_amount\]"):
        check_quality(warehouse, "2025-01")


def test_low_retention_fails(warehouse):
    con = duckdb.connect(warehouse)
    con.execute("DELETE FROM marts.fct_trips WHERE rowid % 2 = 0")
    con.close()
    with pytest.raises(DataQualityError, match="fct_trips_retention"):
        check_quality(warehouse, "2025-01")


def test_failure_message_lists_every_failed_check(warehouse):
    con = duckdb.connect(warehouse)
    con.execute("DELETE FROM marts.fct_trips")
    con.close()
    with pytest.raises(DataQualityError) as err:
        check_quality(warehouse, "2025-01", min_rows=5_000)
    assert "2 data quality check(s) failed" in str(err.value)
