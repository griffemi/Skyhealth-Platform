# Skyhealth Platform

Skyhealth monitors local climate change by ingesting weather data, deriving daily climate indicators, and surfacing analytics through dbt and Streamlit/Lightdash. Skyhealth is built to be a *portable weather & climate data platform—built GCP‑first, designed to migrate cleanly to other cloud providers.*

---

## The Story (why I’m building this)

I’m Emi, and weather is personal for me. I’m heat‑sensitive (autism related), and rain has always been this small, magical reset—calming, vivid, and grounding. Skyhealth is my way to put numbers and structure around that experience: **track the conditions that affect me right now** and **show how those conditions will change over the next 50 years**. 

This project blends two things I care about: boring‑reliable engineering and meaningful signals. I want a system that **I** would trust day‑to‑day (“Is today going to knock my energy out?” “Do I get my rain walk?”) and that’s honest about uncertainty when peeking into the future. The build is intentionally straightforward, expandable, and transparent—so anyone can follow the shape of it without reading the entire codebase.

---

## BLUF (Bottom Line Up Front)

- **Purpose:** End‑to‑end pipelines for North American weather & climate data with clear models, quality gates, precipitation rates, and baseline predictions (nowcasting/short‑term).
- **Principles:** boring‑reliable infra; explicit data contracts; **test comprehensively before prod**; incremental delivery; portability by design.
- **Cloud:** **GCP by default** (GCS + BigQuery + Cloud Run). Abstractions keep migration to AWS/Azure straightforward.
- **Roadmap (short):** North America coverage → precipitation rates → predictions → simple API/UI for access.

> Acronyms I use: **E2E** (end‑to‑end), **PR** (pull request), **CI** (continuous integration), **IaC** (infrastructure as code).

---

## Environments & Deployment Policy

- **Branches**
  - `feature/*`: active development
  - `dev`: integration and validation
  - `prod`: production

- **Deployment trigger**
  - **Merging to `prod` is deployment.** Treat `prod` as sacred.

- **Expectations before merging to `prod`**
  1. Unit + integration tests pass locally and in CI.
  2. Data validation gates (schema + quality thresholds) pass on a representative sample.
  3. A smoke E2E run on `dev` completes (ingest → model → validate → serve).
  4. PR includes a short “risk & rollback” note.

> TL;DR—if it merges to `prod`, it ships. Be comprehensive before that point.

---

## Roadmap

The plan delivers value early while keeping scope safe. Ops, cost, and scale work are **embedded throughout** (no separate phase).

### Phase 0 — MVP foundations
- [x] Repo scaffolding (layers, contracts, Makefile)
- [x] Sample ingestion + example model + validation hooks
- [x] Minimal UI pattern for exploration (map/time‑series friendly)

### Phase 1 — North America coverage
- [ ] Expand connectors for U.S., Canada, Mexico observations
- [ ] Normalize station metadata + timezones + unit conversions
- [ ] Partitioning + retention strategy suitable for low cost

### Phase 1.1 — Precipitation rates
- [ ] Derive precipitation rates from raw observations
- [ ] Rolling‑window quality checks (spikes, dropouts, stale sensors)
- [ ] Exposure in models and API/UI

### Phase 2 — Predictions (nowcasting + short‑term)
- [ ] Integrate forecast feeds and/or simple nowcasting baselines
- [ ] Backtesting harness and error metrics (MAE/RMSE by region)
- [ ] Feature store pattern for reusable signals

### Phase 3 — Access & UX
- [ ] Read‑only API for programmatic access
- [ ] Dashboard: map + time‑series + region drill‑downs
- [ ] Auth keys + thoughtful rate limits

> “Ops/cost/scale as we go”: scheduled runs with retries, lifecycle rules, incremental processing, and caching are layered into each phase, not deferred.

---

## Architecture at a glance

**Flow:** `Ingest (Bronze) → Model (Silver/Gold) → Validate → Serve (API/UI)`

- **Ingest (Bronze):** Land raw feeds intact. Immutable storage, partitioned by date/source. 
- **Model (Silver):** Normalize units/timezones, harmonize schemas, geospatial joins.
- **Model (Gold):** Derive **precipitation rates** and region/day summaries for fast access.
- **Validate:** Contracts (schema/expectations) and rolling quality checks.
- **Serve:** Read‑only API for data access + simple UI for maps and time series.

**Default runtime:** GCP (GCS, BigQuery, Cloud Run) with local‑first dev (DuckDB + Docker). Components are replaceable to keep migration low‑friction.

---

## Components & Technology Choices (what and why)

> This section is intentionally detailed—**purpose first**, then the tech. Where I name tools, I also name a portable alternative.

### Ingestion
- **What:** Pull weather/climate observations and forecasts from public providers; store **raw** unchanged (CSV/JSON/Parquet).
- **Why:** Raw, immutable landings give auditability and simpler reprocessing.
- **Default tech (GCP‑first):** Python workers on Cloud Run (or Jobs) writing to **GCS**; retries with backoff; signed requests as needed.
- **Local dev:** Dockerized workers writing to a local folder; make targets for sample pulls.
- **Portable:** Swap GCS → S3/Azure Blob; Cloud Run → ECS/Fargate/Azure Container Apps. 

### Storage & Lake
- **What:** Partitioned object storage for raw/processed data; columnar formats.
- **Why:** Cheap, fast scans and easy incremental processing.
- **Default:** **GCS** with **Parquet**, partitioned by `ingest_date`, `region`, `source`.
- **Optional:** Table format like **Apache Iceberg** for ACID and time travel if/when needed.
- **Portable:** S3 or Azure Blob with the same layout.

### Transform & Modeling
- **What:** Turn raw observations into tidy, query‑ready tables and **precipitation rates**, then into curated region/day summaries.
- **Why:** Clear separation of concerns; stable interfaces for consumers.
- **Default:** 
  - **dbt Core** for SQL models (Silver/Gold) on **BigQuery** (with **BigQuery GIS** for geospatial ops).
  - Lightweight Python transforms for edge cases (e.g., **xarray** for gridded data, **pandas** for reshaping).
- **Local dev:** **DuckDB** (with spatial) targets for fast iteration.
- **Portable:** dbt adapters exist for Snowflake/Redshift/Postgres; Python stays the same.

### Orchestration
- **What:** Define and schedule E2E runs, retries, and asset dependencies.
- **Why:** Reproducibility and observability of the full graph.
- **Default:** **Dagster** (Dockerized locally; deployed via Cloud Run or a small VM).
- **Portable:** Prefect or Airflow can substitute with minimal graph changes.

### Data Validation & Contracts
- **What:** Schema/expectation checks on load + rolling quality guards (null %, range, station counts, spikes).
- **Why:** Catch drift early; protect `prod` from silent breaks.
- **Default:** **dbt tests** + **Great Expectations** (or pydantic contracts in Python) gated in CI and `dev` runs.
- **Portable:** Same tools run on any warehouse; assertions live next to code.

### Serving (API & UI)
- **What:** Read‑only API for programmatic access; simple UI for maps and time series.
- **Why:** Fast, honest answers—“what’s happening here?” and “how is it trending?”
- **Default:** 
  - **FastAPI** on Cloud Run (rate‑limited).
  - **Streamlit** UI with **Leaflet/Folium** or deck.gl for maps; time‑series plots.
  - Optionally **Lightdash** for BI explorers over dbt models.
- **Portable:** Any container platform; map libs are frontend‑agnostic.

### Predictions (nowcasting + short‑term)
- **What:** Baseline models (persistence, simple regressors) with backtesting; later, more capable nowcasting.
- **Why:** Start honest and interpretable, measure errors, then iterate.
- **Default:** **scikit‑learn**/**statsmodels**/**xarray** with a backtesting harness; metrics: **MAE/RMSE** by region/season.
- **Portable:** All Python; storage/compute abstractions isolate cloud specifics.

### Observability, Cost, and Reliability (baked in)
- **Logs/metrics:** Structured logs; basic metrics and alerting on failures/stale data (Cloud Monitoring). 
- **Cost controls:** GCS lifecycle rules, partition pruning, query quotas, and incremental models.
- **Resilience:** Exponential backoff, idempotent writers, small batch sizes, and dead‑letter queues if volume warrants.
- **Security:** Least‑privilege service accounts, per‑environment secrets, and narrow egress rules.

### Portability Map (at a glance)
- GCS ↔ S3 ↔ Azure Blob
- BigQuery ↔ Snowflake/Redshift/Postgres/DuckDB
- Cloud Run ↔ ECS/Fargate ↔ Azure Container Apps
- Cloud Scheduler ↔ EventBridge ↔ Azure Scheduler
- Cloud Monitoring ↔ CloudWatch ↔ Azure Monitor

---

## Data Model (layers)

- **Bronze (raw):** exact payloads + producer metadata; partitioned; checksums; never mutated.
- **Silver (normalized):** typed columns, unit/timezone normalization, harmonized station metadata, geospatial joins.
- **Gold (serving):** 
  - `precipitation_rate_*` tables (by station/region/time).
  - `region_day_summary` (temp ranges, humidity, wind, precipitation totals/rates).
  - Indices/materializations tuned for API/UI access.

> Contracts: every table has documented schemas, units, and assumptions. Changes are versioned and tested.

---

## Testing & Quality Gates

- **Unit tests:** transforms, utilities, schema conversions.
- **Integration tests:** ingest → model → validate on sample slices.
- **Data validation:** schema expectations + rolling quality thresholds.
- **Smoke E2E:** one green run on `dev` before any `prod` PR.
- **CI:** runs tests/validations on every PR; blocks `prod` merges on failures.

---

## Getting Started (local)

Prereqs (use equivalents if you prefer):
- Python 3.13, `pipx` or `poetry`
- Docker (helpful for API/UI and services)
- `make` for common tasks

Common flows:
```bash
# 1) Setup
make init           # or: poetry install

# 2) Pull a small sample
make ingest-sample

# 3) Build models (Silver/Gold)
make models

# 4) Validate data
make validate

# 5) Run API and/or UI locally
make api
make ui
```

> Local targets default to DuckDB/file‑based paths; cloud targets switch to GCP configs.

---

## Environments & Branching (quick reference)

- `feature/*` → `dev` → `prod`
- **Merge to `prod` = deploy.** Comprehensive tests/validations and a short risk/rollback note are required.
- Secrets, permissions, and configs are per‑environment with least privilege.

---

## Contributing (PR checklist)

Before opening a PR—especially into `prod`:
- [ ] Tests pass and critical coverage didn’t drop
- [ ] Data validation passes on a representative sample
- [ ] E2E smoke run is green on `dev`
- [ ] Short “risk & rollback” notes included in PR
- [ ] Contracts/docs updated if a schema changed

---

## License

TBD — permissive open‑source license intended. Open an issue if you have constraints and we’ll align.

---

## Notes

- Built GCP‑first (GCS, BigQuery, Cloud Run), but **portable by design**—component choices above include equivalents.
- Long‑horizon climate context (50‑year framing) informs modeling choices and data retention; the platform stays honest about uncertainty and keeps versioned assumptions visible.
