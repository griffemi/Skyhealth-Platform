.PHONY: help init lint fmt test ci dagster-dev dagster-materialize dagster-backfill publish-bq pipeline-dev streamlit-dev iceberg-housekeeping nuke-pave validation-snapshots clickhouse-up
.RECIPEPREFIX := >
.DEFAULT_GOAL := help

DATE ?= $(shell date -I)
PARTITION ?= $(DATE)
START ?= $(PARTITION)
END ?= $(START)

# LOCATION_SET defaults dynamically inside dagster-backfill

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

publish-bq: ## Publish Gold: ClickHouse (dev) or BigQuery (prod)
>poetry run python -m pipelines.publish_bigquery --date $(PARTITION)

pipeline-dev: ## Materialize Bronze → Gold assets for one partition via Dagster
>poetry run dagster job execute -m pipelines.definitions -j daily_partition_job --partition $(PARTITION)

streamlit-dev: ## Launch the Streamlit dashboard locally
>poetry run streamlit run apps/streamlit_app.py

dagster-dev: ## Run Dagster UI + daemon in dev mode
>poetry run dagster dev -m pipelines.definitions

dagster-materialize: ## Materialize Dagster Bronze → Publish assets for one partition
>poetry run dagster asset materialize --select bronze_openmeteo_daily+publish_gold_partition_asset --partition $(PARTITION) -m pipelines.definitions

dagster-backfill: ## Backfill Bronze → Gold for an arbitrary date range via Dagster job
>START=$(START) END=$(END) LOCATION_SET=$(LOCATION_SET) poetry run python - <<'PY'
import json
import os
from pathlib import Path
from pipelines.config import settings

start = os.environ.get("START")
if not start:
    raise SystemExit("START is required")
end = os.environ.get("END") or start
location = os.environ.get("LOCATION_SET") or settings.locations_file

payload = {
    "ops": {
        "prepare_backfill": {
            "config": {
                "start": start,
                "end": end,
                "location_set": location,
            }
        }
    }
}

Path(".dagster_backfill.json").write_text(json.dumps(payload))
PY
>poetry run dagster job execute -m pipelines.definitions -j climate_backfill_job -c .dagster_backfill.json; \
  STATUS=$$?; rm -f .dagster_backfill.json; exit $$STATUS

iceberg-housekeeping: ## Clean up Iceberg snapshots and orphan files
>poetry run dagster asset materialize --select iceberg_housekeeping -m pipelines.definitions

validation-snapshots: ## Print the local path where validation docs are written
>python -c "from pipelines.config import settings; print(settings.ge_data_docs_path)"

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
