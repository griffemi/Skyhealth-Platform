# Skyhealth Platform

Skyhealth delivers a daily Bronze → Silver → Gold weather lake on Google Cloud: Dagster orchestrates Spark jobs on a hardened GCE VM, Delta tables live in GCS, lightweight pandas contracts guard quality, BigQuery serves the curated gold tables, and Streamlit (Cloud Run) turns them into dashboards.

---

## Quick Tour

- **Control plane:** Dagster runs on a single GCE instance (systemd services for webserver + daemon, backed by Cloud SQL Postgres, logs shipped via Ops Agent).
- **Compute:** Spark is local-only for development; production batches are expected to run on Dataproc Serverless.
- **Lake:** GCS buckets hold Delta tables for Bronze, Silver, and Gold. Lifecycle policies age data to cheaper storage.
- **Quality:** Simple Pandas-based validations run on Silver and Gold partitions, writing quick HTML snapshots to `great_expectations/uncommitted/data_docs`.
- **Serving:** Gold partitions publish into BigQuery (`climate_daily_summary`). Streamlit reads directly from BigQuery and is deployed to Cloud Run using Workload Identity.

---

## Getting Started Locally

1. Install dependencies: `make init` (requires Python 3.13).
2. Run the Dagster UI: `make dagster-dev`
3. Materialize a dev partition end-to-end: `make pipeline-dev PARTITION=2024-07-01`
4. Explore the dashboard locally: `make streamlit-dev`

`make` contains one-shot helpers (Spark for individual layers, Dagster materializations, BigQuery publish, etc.). Refer to [Task Reference](#task-reference).

---

## Environments & Naming

| Environment | Example Project Resources | Notes |
|-------------|---------------------------|-------|
| **dev**     | `gs://<project>-dev-bronze`, `skyhealth_dev_gold` dataset | Local Spark, Dataproc optional |
| **stg**     | `gs://<project>-stg-bronze`, `skyhealth_stg_gold` dataset | Pre-prod validation |
| **prod**    | `gs://<project>-prod-bronze`, `skyhealth_prod_gold` dataset | Dagster VM + Dataproc Serverless + Cloud Run |

Buckets are `gs://<project>-<env>-{bronze,silver,gold,checkpoints}` with uniform access and lifecycle rules (Bronze → Nearline after 60d, Silver after 120d, Gold retained 365d). BigQuery datasets mirror the env suffix.

---

## Orchestration Path

Dagster defines daily-partitioned assets:

1. **`bronze_openmeteo_daily`** – fetches Open-Meteo observations, lands Delta partition in Bronze.
2. **`silver_climate_daily_features`** – derives heating/cooling degree days, precipitation flags, and validates with Pandas contracts.
3. **`gold_climate_daily_summary`** – aggregates per-region metrics, runs contract checks, then publishes.
4. **`publish_gold_to_bigquery`** – loads the partition into BigQuery (time-partitioned, clustered on `region_id`).
5. **`delta_housekeeping`** – weekly VACUUM for Bronze/Silver/Gold tables.

A daily schedule (`daily_delta_schedule`, 03:00 UTC) materializes Bronze→Gold→Publish. A sensor checks for missing Bronze partitions (older than 24h) and triggers a catch-up run. Weekly housekeeping runs every Monday at 06:00 UTC.

---

## Lake Layout & Retention

- **Bronze** (`openmeteo_daily`): partitioned by `ingest_date`; raw API response with location metadata.
- **Silver** (`climate_daily_features`): same partitioning with derived features (`hdd18`, `cdd18`, `gdd10_30`, precipitation flag). Pandas checks: non-null IDs/dates, plausible temperature/precip ranges.
- **Gold** (`climate_daily_summary`): aggregated per `region_id` (currently maps to station) with 7/30 day precipitation windows computed by Streamlit queries. Contract checks ensure non-null region/date and precipitation ≥ 0.

Delta data docs render under `great_expectations/uncommitted/data_docs/{suite}/<partition>.html`.

---

## Deploy Flow

1. **Terraform (`infra/terraform`)** provisions network, Dagster VM, Cloud SQL, GCS buckets, BigQuery datasets, Streamlit Cloud Run service, IAM alerts, and budget policy. Run with `terraform plan` → `terraform apply`.
2. **Ansible (`infra/ansible`)** hardens the Dagster VM (updates + fail2ban), installs Poetry, Google Cloud SDK, Cloud SQL proxy, and lays down systemd units (`dagster-webserver`, `dagster-daemon`, `cloud-sql-proxy`).
3. **Dataproc**: production Spark runs should submit to Dataproc Serverless (staging bucket + service accounts managed by Terraform). Local dev continues to use `spark-submit` via Poetry.
4. **Streamlit**: container built from `docker/streamlit.Dockerfile`, deployed via `gcloud run deploy` with Workload Identity (`STREAMLIT_SERVICE_ACCOUNT`).

The GitHub Actions workflow `deploy_prod.yml` wires these steps together (Terraform plan/apply, Ansible playbook, Cloud Build submit, Cloud Run deploy) using GCP service-account credentials.

---

## Security Notes

- Service accounts: Dagster VM (`dagster-runner`), Dataproc batches (`spark-runner`), and Streamlit (`streamlit-svc`) run without downloaded keys. Workload Identity is enforced for Cloud Run and Dataproc.
- OS Login is enabled on the Dagster VM; firewalls only allow IAP ranges on SSH/port 3000.
- Secrets (Cloud SQL password, Dagster UI secret) live in Secret Manager and are pulled on-demand via `skyhealth-render-env`.

---

## Troubleshooting

- **Dagster**: `systemctl status dagster-webserver dagster-daemon`, `journalctl -u dagster-daemon -f`. In Cloud Logging use `resource.type="gce_instance" AND textPayload:"dagster"`.
- **Spark/Delta**: inspect partitions with `gsutil ls gs://<bucket>/climate_daily_summary/`. For Dataproc batches, `gcloud dataproc batches list --region=$REGION` and `describe` for driver output.
- **BigQuery**: check latest partition –
  ```sql
  SELECT MAX(observation_date)
  FROM `<project>.<dataset>.climate_daily_summary`;
  ```
- **Streamlit**: verify deployment via `gcloud run services describe <service> --region=$REGION --format='value(status.url)'`; logs stream to `resource.type="cloud_run_revision"`.


---

## Task Reference

| Command | Purpose |
|---------|---------|
| `make pipeline-dev PARTITION=YYYY-MM-DD` | Bronze → Silver → Gold → BigQuery for the given date |
| `make spark-bronze PARTITION=...` | Run only the Bronze ingestion job |
| `make spark-silver PARTITION=...` | Build Silver features for a date |
| `make spark-gold PARTITION=...` | Build Gold summary for a date |
| `make publish-bq PARTITION=...` | Re-publish a Gold partition to BigQuery |
| `make dagster-materialize PARTITION=...` | Materialize via Dagster (`bronze_openmeteo_daily+publish_gold_to_bigquery`) |
| `make delta-housekeeping` | Trigger Delta VACUUM asset |
| `make validation-snapshots` | Print the folder storing validation HTML snapshots |
| `make streamlit-dev` | Run Streamlit against local BigQuery credentials |
| `make nuke-pave` | Interactive *DELETE* of local lake directories |

---

## Nuke & Pave

`make nuke-pave` prompts for `DELETE` before clearing `lake/{bronze,silver,gold,checkpoints,exports,warehouse}`—use this before rebuilding local Delta tables. Production wipe must be a manual, multi-step process (confirm with stakeholders before running destructive GCS deletes).

---

## CI/CD

- `.github/workflows/ci.yml`: Poetry install → Ruff → Black → Pytest on every PR.
- `.github/workflows/deploy_prod.yml`: Terraform init/plan/apply, Ansible playbook, Cloud Build + Cloud Run deploy, smoke curl of Streamlit.

Protected branches should require CI green; `prod` pushes trigger full infra + app rollout.

---

## Glossary

- **BQ** – BigQuery
- **ADR** – Architecture Decision Record
- **IaC** – Infrastructure as Code
- **SLO** – Service-Level Objective

---

---

## Roadmap

### Phase 0 — Foundations
- [x] Repo scaffolding (layers, contracts, Makefile)
- [x] Sample ingestion + example model + validation hooks
- [x] Minimal UI pattern for exploration (map/time‑series friendly)

### Phase 1 — Version 1 Build
- [x] Determine best fitting architecture pattern
- [ ] Expand connectors for U.S., Canada, Mexico observations
- [ ] Normalize station metadata + timezones + unit conversions
- [ ] Partitioning + retention strategy suitable for low cost
- [ ] Exposure in models and API/UI

### Phase 1.1 — Precipitation rates
- [ ] Derive precipitation rates from raw observations
- [ ] Rolling‑window quality checks (spikes, dropouts, stale sensors)

### Phase 2 — Predictions (nowcasting + short‑term)
- [ ] Integrate forecast feeds and/or simple nowcasting baselines
- [ ] Backtesting harness and error metrics (MAE/RMSE by region)
- [ ] Feature store pattern for reusable signals

### Phase 3 — Access & UX
- [ ] Read‑only API for programmatic access
- [ ] Dashboard: map + time‑series + region drill‑downs
- [ ] Auth keys + thoughtful rate limits

> “Ops/cost/scale as we go”: scheduled runs with retries, lifecycle rules, incremental processing, and caching are layered into each phase, not deferred.

