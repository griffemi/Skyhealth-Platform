# Skyhealth

Skyhealth monitors local climate change by ingesting weather data, deriving daily climate indicators, and surfacing analytics through dbt and Streamlit/Lightdash.

```
API -> Bronze (Iceberg/Hadoop) -> Silver (Spark features) -> Gold (dbt) -> Lightdash/Streamlit
```

## Why Hadoop Catalog on GCS
Using the Iceberg Hadoop catalog keeps metadata in the GCS bucket alongside data and avoids running a Hive metastore. Spark is the only writer which simplifies operations. To upgrade to a Hive metastore later:
1. Provision a metastore database (e.g. Cloud SQL).
2. Install Hive on the Spark master and start the metastore service.
3. Update `spark.sql.catalog.local` to use `org.apache.iceberg.spark.SparkCatalog` with `type=hive` and metastore URIs.
4. Migrate existing tables with Iceberg's `snapshot` procedure.

## Development Quickstart
Prereqs: Docker, Poetry, Python 3.13.

```bash
cp .env.example .env
make init        # install deps
make up          # start Spark, Streamlit, Lightdash
# add your locations to data/locations.csv
make spark-bronze-batch
make spark-bronze-incremental
make spark-silver
make dbt-dev
make map
make map-timeslider
```

## Production Setup
1. `make tf-apply` – provisions VPC, bucket (with lifecycle rules), dataset, a Spark master VM and 0-sized worker MIG, Artifact Registry, Cloud Run Jobs and Services, and Scheduler.
2. Build and push images: submitters, Streamlit, Lightdash: `make ar-build-push`.
3. Configure the Spark master with Ansible: `make ansible-deploy`.
4. Cloud Scheduler invokes `spark_submitter` for Bronze (hourly) and Silver (daily) jobs; the submitter handles master start/stop and worker scaling. Streamlit/Lightdash run as scale‑to‑zero Cloud Run services.

## Operations
- Unified `bronze_openmeteo.py` handles backfill (`--mode backfill --range-start 1970-01-01`) and hourly incremental batches (`--mode incremental --lookback 0`).
- Silver features upsert into Iceberg via MERGE and update a Parquet export path derived from the warehouse (override with `PARQUET_EXPORT`).
- Cloud Run submitter controls worker MIG scaling and master start/stop. Tunable knobs via env: `WORKER_PRESCALE`, `SPARK_EXEC_MAX`, `KEEP_MASTER_AFTER`.
- dbt gold builds run after silver via `dbt_builder` job.
- Periodically compact tables with Iceberg `rewrite` (see Makefile).

## Data Model
- **Bronze daily**: `location_id`, `d`, `tavg_c`, `prcp_mm` partitioned by location and date.
- **Silver daily features**: adds `hdd18`, `cdd18`, `gdd10_30`, `prcp_flag`.
- **Gold monthly**: rollups, normals (1991‑2020), anomalies.

## Visualization
Dev uses docker-compose. In prod, Streamlit and Lightdash deploy on Cloud Run; obtain URLs from Terraform outputs.

## Testing & CI/CD
GitHub Actions run `pytest` and `dbt compile` on pull requests. Pushing to `prod` branch triggers the deploy workflow which builds images, applies Terraform, runs Ansible, updates Cloud Run Jobs and executes a smoke test.

## Troubleshooting
- Verify Spark master at `http://localhost:8080` and history server at `http://localhost:18080`.
- Workers not registering: check firewall rules and `WORKER_MIG_NAME`; ensure `spark_submitter` started the master.
- Cloud Run UIs may cold start; invoke and wait a few seconds.
- BigQuery external tables require readable GCS paths; refresh if parquet paths change.
- Long tails: lower `WORKER_PRESCALE` and rely on Dynamic Resource Allocation.

## Security & Cost
Use small VM sizes and stop workers when idle. Avoid storing secrets in the repo; use environment variables and secret managers. Restrict firewall rules to known IPs.
