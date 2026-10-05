# Everything runs inside Docker, so the only host requirements are Docker (and make).
# Windows: run from WSL2 or Git Bash; every target is plain `docker compose`.

COMPOSE ?= docker compose
TOOLS   := $(COMPOSE) run --rm tools -c
MONTH   ?= 2025-03

.DEFAULT_GOAL := help
.PHONY: help up down trigger-dag dbt-build dbt-docs charts test lint format clean

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

up: ## Build the image and start Airflow (UI: http://localhost:8080, admin/admin); catchup runs 3 months
	$(COMPOSE) up -d --build

down: ## Stop all containers (keeps the warehouse, data and Airflow history)
	$(COMPOSE) down

trigger-dag: ## Rerun one month through the DAG (idempotent): make trigger-dag MONTH=2025-02
	$(COMPOSE) exec -T airflow-scheduler airflow dags backfill taxi_elt -s $(MONTH)-01 -e $(MONTH)-01 --reset-dagruns -y

dbt-build: ## Run dbt deps + build (models and tests) against the warehouse
	$(TOOLS) "cd taxi_dbt && dbt deps && dbt build"

dbt-docs: ## Generate dbt docs and serve them at http://localhost:8081 (Ctrl+C to stop)
	$(COMPOSE) run --rm --service-ports tools -c "cd taxi_dbt && dbt deps && dbt docs generate && dbt docs serve --host 0.0.0.0 --port 8081 --no-browser"

charts: ## Render the README charts from the marts into docs/images
	$(TOOLS) "python scripts/make_charts.py"

test: ## Run the pytest suite
	$(TOOLS) "python -m pytest -q"

lint: ## ruff + sqlfluff (read-only checks)
	$(TOOLS) "ruff check . && ruff format --check . && cd taxi_dbt && dbt deps -q && cd .. && sqlfluff lint taxi_dbt/models taxi_dbt/snapshots taxi_dbt/tests --disable-progress-bar"

format: ## Auto-format Python and SQL
	$(TOOLS) "ruff check --fix . && ruff format . && sqlfluff fix taxi_dbt/models taxi_dbt/snapshots taxi_dbt/tests --disable-progress-bar -f"

clean: ## Stop everything AND delete volumes, the warehouse and downloaded data
	$(COMPOSE) down -v
	rm -f warehouse/*.duckdb warehouse/*.duckdb.wal
	rm -rf data/raw taxi_dbt/target taxi_dbt/dbt_packages taxi_dbt/logs
