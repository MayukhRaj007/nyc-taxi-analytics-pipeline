"""Structural tests for the DAG. Skipped where Airflow is not installed (e.g. GitHub Actions)."""

import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

# Not "airflow": the repo has its own airflow/ folder, which Python would accept as a namespace package.
pytest.importorskip("airflow.models")

from airflow.models import DagBag
from airflow.timetables.base import TimeRestriction

DAG_DIR = Path(__file__).resolve().parent.parent / "airflow" / "dags"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAG_DIR), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.dags["taxi_elt"]  # not get_dag(): that would query the metadata DB


def test_task_order(dag):
    assert {"resolve_month", "download", "load_raw", "dbt_run", "dbt_test", "data_quality_check"} == set(dag.task_dict)
    assert dag.task_dict["dbt_run"].upstream_task_ids == {"load_raw"}
    assert dag.task_dict["dbt_test"].upstream_task_ids == {"dbt_run"}
    assert dag.task_dict["data_quality_check"].upstream_task_ids == {"dbt_test", "resolve_month"}


def test_scheduling_settings(dag):
    assert dag.catchup is True
    assert dag.max_active_runs == 1  # DuckDB is single-writer
    assert str(dag.timetable.summary) == "0 0 1 * *"  # @monthly


def test_catchup_covers_exactly_three_months(dag):
    restriction = TimeRestriction(earliest=dag.start_date, latest=dag.end_date, catchup=True)
    info = dag.timetable.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
    months = []
    while info is not None and len(months) < 10:
        months.append(info.data_interval.start.strftime("%Y-%m"))
        info = dag.timetable.next_dagrun_info(last_automated_data_interval=info.data_interval, restriction=restriction)
    assert months == ["2025-01", "2025-02", "2025-03"]


def test_every_task_retries_with_backoff_and_reports_failures(dag):
    for task in dag.tasks:
        assert task.retries >= 3
        assert task.retry_exponential_backoff is True
        assert task.on_failure_callback


def test_failure_callback_logs_a_readable_summary(caplog):
    from taxi_elt import log_failure

    ti = SimpleNamespace(dag_id="taxi_elt", task_id="load_raw", try_number=3, max_tries=3, log_url="http://x/log")
    context = {
        "task_instance": ti,
        "exception": ValueError("boom"),
        "run_id": "scheduled__2025-02-01",
        "data_interval_start": "2025-02-01",
        "data_interval_end": "2025-03-01",
    }
    with caplog.at_level(logging.ERROR):
        log_failure(context)
    for expected in ("TASK FAILED", "load_raw", "attempt:  3 of 4", "ValueError: boom", "http://x/log"):
        assert expected in caplog.text
