# One image for every service (webserver, scheduler, init, tools).
# Why: the host only needs Docker. dbt lives in its own virtualenv so its
# dependency pins can never conflict with Airflow's constrained environment.
FROM apache/airflow:2.10.5-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11

COPY airflow/requirements-airflow.txt /tmp/requirements-airflow.txt
COPY airflow/requirements-dbt.txt /tmp/requirements-dbt.txt

# Airflow's own environment: loader + analytics libs, installed against the
# official constraints file so Airflow itself is never destabilised.
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Isolated environment for dbt + lint tooling.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

# dbt/ruff/sqlfluff on PATH *after* the Airflow python so `python` is unchanged.
ENV PATH="${PATH}:/home/airflow/dbt-venv/bin"
