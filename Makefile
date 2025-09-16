.PHONY: init lint fmt test dagster-dev dagster-materialize spark-bronze spark-silver spark-gold publish-bq pipeline-dev streamlit-dev delta-housekeeping nuke-pave validation-snapshots
.RECIPEPREFIX := >

DATE ?= $(shell date -I)
PARTITION ?= $(DATE)

init:
>poetry install --no-root

lint:
>poetry run ruff check .

fmt:
>poetry run black .

test:
>poetry run pytest -q

ci: lint fmt test

spark-bronze:
>poetry run python spark/jobs/bronze_openmeteo.py --mode backfill --range-start $(PARTITION) --range-end $(PARTITION)

spark-silver:
>poetry run python spark/jobs/silver_climate_daily_features.py --date $(PARTITION)

spark-gold:
>poetry run python spark/jobs/gold_climate_daily_summary.py --date $(PARTITION)

publish-bq:
>poetry run python -m skyhealth.publish_bigquery --date $(PARTITION)

pipeline-dev:
>$(MAKE) spark-bronze PARTITION=$(PARTITION)
>$(MAKE) spark-silver PARTITION=$(PARTITION)
>$(MAKE) spark-gold PARTITION=$(PARTITION)
>$(MAKE) publish-bq PARTITION=$(PARTITION)

streamlit-dev:
>poetry run streamlit run apps/streamlit_app.py

dagster-dev:
>poetry run dagster dev -m skyhealth.orchestration.definitions

dagster-materialize:
>poetry run dagster asset materialize --select bronze_openmeteo_daily+publish_gold_to_bigquery --partition $(PARTITION)

delta-housekeeping:
>poetry run dagster asset materialize --select delta_housekeeping

validation-snapshots:
>python -c "from skyhealth.config import settings; print(settings.ge_data_docs_path)"

nuke-pave:
>@read -p "This will DELETE local lake data. Type 'DELETE' to continue: " CONFIRM; \
>  if [ "$$CONFIRM" = "DELETE" ]; then \
>    rm -rf lake/checkpoints lake/exports lake/bronze lake/silver lake/gold lake/warehouse; \
>    mkdir -p lake/{bronze,silver,gold,checkpoints,exports}; \
>    echo "Lake reset complete."; \
>  else \
>    echo "Aborted."; \
>  fi
