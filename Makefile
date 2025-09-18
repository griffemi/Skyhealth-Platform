.PHONY: help init lint fmt test ci dagster-dev dagster-materialize spark-bronze spark-silver spark-gold publish-bq pipeline-dev streamlit-dev iceberg-housekeeping nuke-pave validation-snapshots clickhouse-up
.RECIPEPREFIX := >
.DEFAULT_GOAL := help

DATE ?= $(shell date -I)
PARTITION ?= $(DATE)

help: ## List supported targets
>@printf "Usage: make <target>\n\nTargets:\n"
>@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*##"} { printf "  %-22s %s\n", $$1, $$2 }'

init: ## Install Poetry environment
>poetry install --no-root

lint: ## Run Ruff lint checks
>poetry run ruff check .

fmt: ## Run Black formatter (in place)
>poetry run black .

test: ## Execute pytest suite
>poetry run pytest -q

ci: ## Run lint, format check, and tests
>$(MAKE) lint
>$(MAKE) fmt
>$(MAKE) test

spark-bronze: ## Backfill a single Bronze partition
>poetry run python spark/jobs/bronze_openmeteo.py --mode backfill --range-start $(PARTITION) --range-end $(PARTITION)

spark-silver: ## Build a Silver partition from Bronze data
>poetry run python spark/jobs/silver_climate_daily_features.py --date $(PARTITION)

spark-gold: ## Produce a Gold partition from Silver features
>poetry run python spark/jobs/gold_climate_daily_summary.py --date $(PARTITION)

publish-bq: ## Publish Gold: ClickHouse (dev) or BigQuery (prod)
>poetry run python -m skyhealth.publish_bigquery --date $(PARTITION)

pipeline-dev: ## Run Bronze → Silver → Gold → BigQuery locally
>$(MAKE) spark-bronze PARTITION=$(PARTITION)
>$(MAKE) spark-silver PARTITION=$(PARTITION)
>$(MAKE) spark-gold PARTITION=$(PARTITION)
>$(MAKE) publish-bq PARTITION=$(PARTITION)

streamlit-dev: ## Launch the Streamlit dashboard locally
>poetry run streamlit run apps/streamlit_app.py

dagster-dev: ## Run Dagster UI + daemon in dev mode
>poetry run dagster dev -m skyhealth.orchestration.definitions

dagster-materialize: ## Materialize Dagster Bronze → Publish assets for one partition
>poetry run dagster asset materialize --select bronze_openmeteo_daily+publish_gold_to_bigquery --partition $(PARTITION)

iceberg-housekeeping: ## Clean up Iceberg snapshots and orphan files
>poetry run dagster asset materialize --select iceberg_housekeeping

validation-snapshots: ## Print the local path where validation docs are written
>python -c "from skyhealth.config import settings; print(settings.ge_data_docs_path)"

nuke-pave: ## Danger! Reset the local lake directories after explicit confirmation
>@read -p "This will DELETE local lake data. Type 'DELETE' to continue: " CONFIRM; \
>  if [ "$$CONFIRM" = "DELETE" ]; then \
>    rm -rf lake/checkpoints lake/exports lake/bronze lake/silver lake/gold lake/warehouse; \
>    mkdir -p lake/{bronze,silver,gold,checkpoints,exports,warehouse}; \
>    echo "Lake reset complete."; \
>  else \
>    echo "Aborted."; \
>  fi

clickhouse-up: ## Launch a local ClickHouse container (detached) on ports 8123/9000
>set -e; \
>if docker ps --format '{{.Names}}' | grep -q '^skyhealth-clickhouse$$'; then \
>  echo "ClickHouse container 'skyhealth-clickhouse' is already running."; \
>else \
>  docker run --rm -d --name skyhealth-clickhouse \
>    -e CLICKHOUSE_USER=skyhealth \
>    -e CLICKHOUSE_PASSWORD=skyhealth \
>    -e CLICKHOUSE_DB=skyhealth \
>    -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server >/tmp/skyhealth-clickhouse.cid; \
>  echo "ClickHouse container started (name: skyhealth-clickhouse, user/password: skyhealth)."; \
>fi
