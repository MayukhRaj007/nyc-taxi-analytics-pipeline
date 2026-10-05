import duckdb
import pytest

from taxi_lib.load import RAW_COLUMNS, load_month


def _count(db, month_start):
    con = duckdb.connect(db, read_only=True)
    try:
        return con.execute("SELECT count(*) FROM raw.yellow_trips WHERE source_month = ?", [month_start]).fetchone()[0]
    finally:
        con.close()


def _copy(select_sql, target):
    duckdb.execute(f"COPY ({select_sql}) TO '{target.as_posix()}' (FORMAT PARQUET)")


def test_load_is_idempotent(sample_parquet, db_path):
    first = load_month(sample_parquet, "2025-01", db_path)
    second = load_month(sample_parquet, "2025-01", db_path)
    assert first == second == 3000
    assert _count(db_path, "2025-01-01") == 3000


def test_reloading_one_month_leaves_other_months_untouched(sample_parquet, db_path):
    load_month(sample_parquet, "2025-01", db_path)
    load_month(sample_parquet, "2025-02", db_path)
    load_month(sample_parquet, "2025-01", db_path)  # rerun January
    assert _count(db_path, "2025-01-01") == 3000
    assert _count(db_path, "2025-02-01") == 3000


def test_table_has_pinned_columns_plus_audit_columns(sample_parquet, db_path):
    load_month(sample_parquet, "2025-01", db_path)
    con = duckdb.connect(db_path, read_only=True)
    cols = [r[0] for r in con.execute("DESCRIBE raw.yellow_trips").fetchall()]
    con.close()
    assert cols == [*RAW_COLUMNS, "source_month", "loaded_at"]


def test_file_missing_a_column_loads_it_as_null(sample_parquet, db_path, tmp_path):
    # Older TLC files have no cbd_congestion_fee; the loader must still accept them.
    older = tmp_path / "older.parquet"
    _copy(f"SELECT * EXCLUDE (cbd_congestion_fee) FROM read_parquet('{sample_parquet.as_posix()}')", older)
    assert load_month(older, "2024-06", db_path) == 3000
    con = duckdb.connect(db_path, read_only=True)
    non_null = con.execute("SELECT count(cbd_congestion_fee) FROM raw.yellow_trips").fetchone()[0]
    con.close()
    assert non_null == 0


def test_column_name_case_differences_are_tolerated(sample_parquet, db_path, tmp_path):
    odd = tmp_path / "odd.parquet"
    _copy(f"SELECT * EXCLUDE (Airport_fee), 1.5 AS airport_fee FROM read_parquet('{sample_parquet.as_posix()}')", odd)
    load_month(odd, "2025-01", db_path)
    con = duckdb.connect(db_path, read_only=True)
    assert con.execute('SELECT min("Airport_fee") FROM raw.yellow_trips').fetchone()[0] == 1.5
    con.close()


def test_failed_load_rolls_back_and_keeps_previous_data(sample_parquet, db_path, tmp_path):
    load_month(sample_parquet, "2025-01", db_path)
    corrupt = tmp_path / "corrupt.parquet"
    corrupt.write_bytes(b"not a parquet file")
    with pytest.raises(duckdb.Error):
        load_month(corrupt, "2025-01", db_path)
    assert _count(db_path, "2025-01-01") == 3000


def test_invalid_month_is_rejected(sample_parquet, db_path):
    with pytest.raises(ValueError, match="YYYY-MM"):
        load_month(sample_parquet, "January", db_path)
