"""taxi_elt: monthly ELT of NYC yellow-taxi trips into DuckDB, modelled with dbt.

    resolve_month -> download_month -> load_raw -> dbt_run -> dbt_test -> data_quality_check

The heavy lifting lives in ``taxi_lib`` (plain Python, unit-tested without Airflow)
and in the dbt project. This file only wires the steps together and decides
*when* and *how often* things run, which is what an orchestrator is for.

Rerun a month (idempotent): `make trigger-dag MONTH=2025-02`, which uses
`airflow dags backfill`. Do not use the UI "Trigger DAG" button: it stamps the
run with today's date, which is past this DAG's end_date, so Airflow finishes it
with no tasks.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from taxi_lib.download import download_month
from taxi_lib.load import load_month
from taxi_lib.months import month_bounds
from taxi_lib.quality import check_quality

log = logging.getLogger(__name__)

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/project/data")
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/opt/airflow/project/warehouse/taxi.duckdb")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/project/taxi_dbt")


def log_failure(context: dict) -> None:
    """Log a readable summary of a failed task.

    Why a callback: Airflow's default failure output is a raw traceback buried in
    a task log. One clear ERROR line (what, which month, why, where to look) is
    what you want to see at a glance, and it is the hook where an alert
    (Slack, email) would plug in for a real deployment.
    """
    ti = context["task_instance"]
    exc = context.get("exception")
    log.error(
        "\n"
        "============================================================\n"
        " TASK FAILED\n"
        "   dag:      %s\n"
        "   task:     %s\n"
        "   run:      %s\n"
        "   attempt:  %s of %s\n"
        "   window:   %s -> %s\n"
        "   error:    %s: %s\n"
        "   log:      %s\n"
        "============================================================",
        ti.dag_id,
        ti.task_id,
        context.get("run_id"),
        ti.try_number,
        ti.max_tries + 1,
        context.get("data_interval_start"),
        context.get("data_interval_end"),
        type(exc).__name__,
        exc,
        ti.log_url,
    )


@dag(
    dag_id="taxi_elt",
    # Monthly because TLC publishes one file per month.
    schedule="@monthly",
    # Jan, Feb and Mar 2025: start_date is the first interval's start, and
    # end_date stops scheduling after the March interval. Together with
    # catchup=True this gives exactly 3 historical runs (small enough for a laptop).
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2025, 3, 31, tz="UTC"),
    catchup=True,
    # DuckDB allows a single writer process, so runs must not overlap. This is a
    # deliberate trade-off of the single-file warehouse and is documented in the README.
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        # Downloads and file locks fail transiently; retry with exponential
        # backoff instead of failing the whole month on a blip.
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
        "on_failure_callback": log_failure,
    },
    dagrun_timeout=timedelta(hours=2),
    tags=["taxi", "elt", "dbt", "duckdb"],
    doc_md=__doc__,
)
def taxi_elt():
    @task
    def resolve_month(data_interval_start=None) -> dict:
        """Turn the Airflow data interval into the month to process.

        Why: every downstream task derives its work from this single dict, so
        scheduled runs, backfills and reruns all go through one code path
        and tasks stay deterministic (same month in -> same result out).
        """
        month = data_interval_start.strftime("%Y-%m")
        start, end = month_bounds(month)
        window = {"month": month, "start_date": start.isoformat(), "end_date": end.isoformat()}
        log.info("Processing month %s (%s <= pickup < %s)", month, window["start_date"], window["end_date"])
        return window

    @task(retries=5)
    def download(window: dict) -> str:
        """Fetch the month's Parquet file; skipped if it is already on disk.

        Why more retries than the default: it is the only task that depends on a
        network service we do not control.
        """
        return str(download_month(window["month"], DATA_DIR))

    @task
    def load_raw(parquet_path: str, window: dict) -> int:
        """Replace this month's rows in raw.yellow_trips (delete + insert in one transaction).

        Why delete-and-insert per month instead of append: a retry or a rerun
        must never double-count a month.
        """
        return load_month(parquet_path, window["month"], DUCKDB_PATH)

    # BashOperator + the dbt CLI instead of an Airflow-dbt integration: the
    # simplest thing that works, and the same commands run locally, in CI and here.
    # The month window reaches dbt as vars so fct_trips (incremental) rebuilds
    # exactly that month, whatever order the runs execute in.
    dbt_run = BashOperator(
        task_id="dbt_run",
        cwd=DBT_PROJECT_DIR,
        append_env=True,
        env={
            "START_DATE": "{{ ti.xcom_pull(task_ids='resolve_month')['start_date'] }}",
            "END_DATE": "{{ ti.xcom_pull(task_ids='resolve_month')['end_date'] }}",
        },
        bash_command=(
            "set -euo pipefail; "
            "dbt deps && dbt seed && "
            'dbt run --vars "{start_date: $START_DATE, end_date: $END_DATE}" && '
            "dbt snapshot"
        ),
    )

    # A separate task (not `dbt build`) so a test failure is visible as its own
    # red box in the graph, and the run fails here before bad data is reported as good.
    dbt_test = BashOperator(
        task_id="dbt_test",
        cwd=DBT_PROJECT_DIR,
        append_env=True,
        bash_command="set -euo pipefail; dbt test",
    )

    @task
    def data_quality_check(window: dict) -> list[str]:
        """Row counts, null rates and cleaning retention, logged as PASS/FAIL lines.

        Why in addition to dbt tests: dbt tests validate the models' rules; this
        validates the *delivery* (did the month land completely, did cleaning keep
        a sane share of rows). Failing here fails the DAG run.
        """
        results = check_quality(DUCKDB_PATH, window["month"])
        return [f"{r.name}: {r.detail}" for r in results]

    window = resolve_month()
    parquet = download(window)
    loaded = load_raw(parquet, window)
    quality = data_quality_check(window)
    loaded >> dbt_run >> dbt_test >> quality


taxi_elt()
