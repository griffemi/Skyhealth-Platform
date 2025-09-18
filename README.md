# Skyhealth Platform

Skyhealth delivers a daily Bronze → Silver → Gold weather lake on Google Cloud. Dagster orchestrates Spark jobs on a hardened GCE VM, Iceberg tables live in GCS, lightweight pandas contracts guard quality, and finished gold data lands in BigQuery for production while ClickHouse mirrors the same tables locally for rapid iteration. Streamlit (Cloud Run) turns the outputs into dashboards.

---

## Platform Highlights

- **Control plane:** Dagster runs on a single GCE instance (systemd services for webserver + daemon, backed by Cloud SQL Postgres, logs shipped via Ops Agent).
- **Compute:** Spark 3.5.x is local-only for development; production batches are expected to run on Dataproc Serverless with the same runtime.
- **Lake:** GCS buckets hold Iceberg (1.10.0) tables for Bronze, Silver, and Gold plus a shared warehouse for metadata. Lifecycle policies age data to cheaper storage.
- **Quality:** Simple Pandas-based validations run on Silver and Gold partitions, writing quick HTML snapshots to `great_expectations/uncommitted/data_docs`.
- **Serving:** Gold partitions publish into BigQuery (`climate_daily_summary`) in prod; local workflows mirror the table into ClickHouse for fast iteration. Streamlit reads from the appropriate backend (ClickHouse locally, BigQuery in prod) and is deployed via Cloud Run using Workload Identity.

---

## Prerequisites

- Python 3.13 (the Poetry lockfile is pinned to 3.13.x).
- [Poetry](https://python-poetry.org/) 1.8+ for dependency management.
- GNU Make (optional but recommended for the helper targets below).
- Java 17 (required for the Spark 3.5.x runtime).
- ClickHouse server reachable at `localhost:8123` for local serving; quick start with `make clickhouse-up` (spawns a Docker container with `skyhealth/skyhealth` credentials).
- Google Cloud SDK (`gcloud`) if you plan to interact with GCP resources.
- Access to a Google Cloud project and credentials with permission to the buckets/datasets referenced in your `.env`.

---

## Getting Started Locally

1. Clone the repo and install dependencies: `make init` (runs `poetry install --no-root`).
2. Copy or create a `.env` file if you need to override defaults from `skyhealth.config.Settings` (project IDs, bucket URIs, etc.).
3. List available targets any time with `make help`.
4. Start the local ClickHouse instance: `make clickhouse-up` (required before publishing data locally).
5. Spin up the Dagster UI locally: `make dagster-dev`.
6. Materialize a development partition end-to-end: `make pipeline-dev PARTITION=2024-07-01`.
7. Explore the Streamlit dashboard against your dev data: `make streamlit-dev`.

`PARTITION` defaults to today's date (`date -I`). Override it either by passing `PARTITION=YYYY-MM-DD` when you invoke a target or by exporting it in your shell.

> Tip: launch a local ClickHouse instance before publishing or viewing data locally (`make clickhouse-up`, which wraps `docker run -p 8123:8123 -p 9000:9000 --name skyhealth-clickhouse -e CLICKHOUSE_USER=skyhealth -e CLICKHOUSE_PASSWORD=skyhealth -e CLICKHOUSE_DB=skyhealth clickhouse/clickhouse-server`).

Local data and Iceberg lakehouse state live under `./lake/`; use `make nuke-pave` (destructive!) to reset.

---

## Configuration

Runtime configuration is provided by `skyhealth.config.Settings`. Important fields include:

- `env` / `region` / `project_id` – logical environment naming.
- `bronze_bucket`, `silver_bucket`, `gold_bucket`, `checkpoints_bucket` – remote GCS URIs; fall back to the local lake otherwise.
- `iceberg_catalog` / `iceberg_warehouse_uri` – Iceberg catalog name and lakehouse location.
- `bigquery_project` / `bigquery_dataset` – where Gold data is published.
- `ge_data_docs_path` – directory where validation HTML is written.
- `duckdb_path` – local DuckDB catalog for ad hoc exploration.
- `clickhouse_*` – host, port, credentials, and database/table used for local serving (defaults match `make clickhouse-up`).

Settings read from `.env` by default; see `skyhealth/config.py` for the full list.

---

## Make Targets

| Target | Purpose |
|--------|---------|
| `make help` | Print documented targets. |
| `make init` | Install the Poetry environment. |
| `make lint` / `make fmt` / `make test` | Ruff linting, Black formatting, and pytest. |
| `make ci` | Run lint, format, and tests sequentially (local CI smoke test). |
| `make spark-bronze PARTITION=...` | Backfill a single Bronze partition. |
| `make spark-silver PARTITION=...` | Build Silver features for a partition. |
| `make spark-gold PARTITION=...` | Build Gold summary for a partition. |
| `make publish-bq PARTITION=...` | Publish Gold to ClickHouse locally or BigQuery in prod. |
| `make clickhouse-up` | Start a disposable ClickHouse server in Docker. |
| `make pipeline-dev PARTITION=...` | Bronze → Silver → Gold → Publish (ClickHouse locally / BigQuery in prod). |
| `make dagster-dev` | Run Dagster UI and daemon locally. |
| `make dagster-materialize PARTITION=...` | Trigger Dagster assets for a single partition. |
| `make iceberg-housekeeping` | Run the Iceberg maintenance asset. |
| `make validation-snapshots` | Print the path to Great Expectations HTML docs. |
| `make streamlit-dev` | Launch the Streamlit dashboard. |
| `make nuke-pave` | Reset the local lake directories after confirmation. |

---

## Environments & Naming

| Environment | Example Project Resources | Notes |
|-------------|---------------------------|-------|
| **dev**     | `gs://<project>-dev-bronze`, `skyhealth_dev_gold` dataset | Local Spark, Dataproc optional |
| **stg**     | `gs://<project>-stg-bronze`, `skyhealth_stg_gold` dataset | Pre-prod validation |
| **prod**    | `gs://<project>-prod-bronze`, `skyhealth_prod_gold` dataset | Dagster VM + Dataproc Serverless + Cloud Run |

Buckets follow `gs://<project>-<env>-{bronze,silver,gold,checkpoints,lakehouse}` with uniform access and lifecycle rules. BigQuery datasets mirror the environment suffix.

For non-production work, the gold table is also mirrored into ClickHouse as `skyhealth.climate_daily_summary` on the local developer machine.

---

## Orchestration Path

Dagster defines daily-partitioned assets:

1. **`bronze_openmeteo_daily`** – fetches Open-Meteo observations, landing an Iceberg partition in Bronze.
2. **`silver_climate_daily_features`** – derives heating/cooling degree days, precipitation flags, and runs Pandas validations.
3. **`gold_climate_daily_summary`** – aggregates per-region metrics, validates, then publishes downstream.
4. **`publish_gold_to_bigquery`** – publishes the partition downstream (BigQuery in prod; ClickHouse in non-prod for local analytics).
5. **`iceberg_housekeeping`** – weekly snapshot expiry + orphan cleanup for Bronze/Silver/Gold tables.

A daily schedule (`daily_iceberg_schedule`, 03:00 UTC) materializes Bronze→Gold→Publish. A `bronze_partition_sensor` catches missing bronze partitions and triggers catch-up runs. Weekly housekeeping runs every Monday at 06:00 UTC via `weekly_iceberg_housekeeping`.

---

## Lake Layout & Retention

- **Bronze** (`openmeteo_daily`): partitioned by `ingest_date`; raw API response with location metadata.
- **Silver** (`climate_daily_features`): same partitioning with derived features (`hdd18`, `cdd18`, `gdd10_30`, precipitation flag). Pandas checks ensure ID/date completeness and plausible ranges.
- **Gold** (`climate_daily_summary`): aggregated per `region_id` (maps to station) with daily temperature/precipitation summaries; longer precipitation windows are calculated on the fly in analytics. Contracts guard for nulls and negative precipitation.

Validation data docs render under `great_expectations/uncommitted/data_docs/{suite}/<partition>.html`. Use `make validation-snapshots` to print the path.

---

## Local ClickHouse Serving

- Publish jobs in non-prod environments write to ClickHouse (`skyhealth.climate_daily_summary`) running on `localhost:8123`.
- Start a disposable instance with Docker via `make clickhouse-up` (username/password: `skyhealth`/`skyhealth`).
- Connection settings (`clickhouse_host`, `clickhouse_port`, `clickhouse_user`, `clickhouse_password`, `clickhouse_database`) live in `Settings`; override them in `.env` if needed.
- Streamlit reads from the same ClickHouse table locally; prod Streamlit continues to query BigQuery. Use `make publish-bq` to re-publish specific partitions after re-running pipelines.

---

## Deploy Flow

1. **Terraform (`infra/terraform`)** provisions network, Dagster VM, Cloud SQL, GCS buckets, BigQuery datasets, Streamlit Cloud Run service, IAM alerts, and budget policy.
2. **Ansible (`infra/ansible`)** hardens the Dagster VM, installs Poetry + Google Cloud SDK + Cloud SQL proxy, and sets up systemd units (`dagster-webserver`, `dagster-daemon`, `cloud-sql-proxy`).
3. **Dataproc**: production Spark runs submit to Dataproc Serverless on the Spark 3.5 runtime (staging bucket/service accounts from Terraform). Local dev uses `spark-submit` via Poetry with the same Spark/Iceberg combo.
4. **Streamlit**: container built from `docker/streamlit.Dockerfile`, deployed via `gcloud run deploy` with Workload Identity (`STREAMLIT_SERVICE_ACCOUNT`).

GitHub Actions workflow `deploy_prod.yml` ties these steps together (Terraform plan/apply, Ansible playbook, Cloud Build submit, Cloud Run deploy) using GCP service-account credentials.

---

## Security Notes

- Service accounts: Dagster VM (`dagster-runner`), Dataproc batches (`spark-runner`), and Streamlit (`streamlit-svc`) run without downloaded keys. Workload Identity handles authentication.
- OS Login is enabled on the Dagster VM; firewalls only allow IAP ranges on SSH/port 3000.
- Secrets (Cloud SQL password, Dagster UI secret) live in Secret Manager and are pulled on-demand via `skyhealth-render-env`.

---

## Troubleshooting

- **Dagster**: `systemctl status dagster-webserver dagster-daemon`, `journalctl -u dagster-daemon -f`. In Cloud Logging use `resource.type="gce_instance" AND textPayload:"dagster"`.
- **Spark/Iceberg**: inspect table state with `spark-sql -e "CALL skyhealth.system.snapshots('dev_bronze.openmeteo_daily')"`. List object paths via `gsutil ls gs://<bucket>-warehouse/dev_bronze.openmeteo_daily/`. For Dataproc batches, `gcloud dataproc batches list --region=$REGION` and `describe` for driver output.
- **ClickHouse**: verify the local server is running (`docker ps`), and inspect data via `clickhouse-client --query="SELECT * FROM skyhealth.climate_daily_summary LIMIT 10"`.
- **BigQuery**: check latest partition –
  ```sql
  SELECT MAX(observation_date)
  FROM `<project>.<dataset>.climate_daily_summary`;
  ```
- **Streamlit**: verify deployment via `gcloud run services describe <service> --region=$REGION --format='value(status.url)'`; logs stream to `resource.type="cloud_run_revision"`.

---

## CI/CD

- `make ci` runs linting + formatting + tests locally.
- `.github/workflows/ci.yml`: Poetry install → Ruff → Black → Pytest on every PR.
- `.github/workflows/deploy_prod.yml`: Terraform init/plan/apply, Ansible playbook, Cloud Build + Cloud Run deploy, and a Streamlit smoke check.

Protected branches should require CI green; `prod` pushes trigger the full infra + app rollout.

---

## Glossary

- **BQ** – BigQuery
- **ADR** – Architecture Decision Record
- **IaC** – Infrastructure as Code
- **SLO** – Service-Level Objective

---

## Roadmap

### Phase 0 — Foundations
- [x] Repo scaffolding (layers, contracts, Makefile)
- [x] Sample ingestion + example model + validation hooks
- [x] Minimal UI pattern for exploration (map/time-series friendly)

### Phase 1 — Version 1 Build
- [x] Determine best fitting architecture pattern
- [ ] Expand connectors for U.S., Canada, Mexico observations
- [ ] Normalize station metadata + timezones + unit conversions
- [ ] Partitioning + retention strategy suitable for low cost
- [ ] Exposure in models and API/UI
- [ ] Dagster alerting for bronze ingestion delays

### Phase 1.1 — Precipitation rates
- [ ] Derive precipitation rates from raw observations
- [ ] Rolling-window quality checks (spikes, dropouts, stale sensors)

### Phase 2 — Predictions (nowcasting + short-term)
- [ ] Integrate forecast feeds and/or simple nowcasting baselines
- [ ] Backtesting harness and error metrics (MAE/RMSE by region)
- [ ] Feature store pattern for reusable signals

### Phase 3 — Access & UX
- [ ] Read-only API for programmatic access
- [ ] Dashboard: map + time-series + region drill-downs
- [ ] Auth keys + thoughtful rate limits

> “Ops/cost/scale as we go”: scheduled runs with retries, lifecycle rules, incremental processing, and caching are layered into each phase, not deferred.
