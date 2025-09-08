# Skyhealth Platform

*A pragmatic, personal weather & climate data platform built to be boring‑reliable and recruiter‑readable.*

---

## The Story (why this exists)

Hi, I’m Emi. I’ve spent the last few years building data platforms that actually ship. Skyhealth is me putting those habits into a clean, end‑to‑end weather/climate project: something I’d trust to run for a small team, that I can extend quickly, and that showcases how I work (design for reliability first, then scale).

I’m moving toward Seattle, and I care a lot about day‑to‑day decisions that depend on weather—commuting, power usage, air quality, flood risk, and “can we do this outdoor plan without getting soaked.” Skyhealth makes that data accessible, testable, and eventually predictive, starting pragmatic and growing intentionally.

If you’re a recruiter or hiring manager skimming this: this repo shows my approach to platform work—clear contracts, CI that protects production, sensible data modeling, and a roadmap that reduces scope risk while increasing business value.

---

## BLUF (Bottom Line Up Front)

- **Purpose:** unified, testable pipelines for North American weather & climate data, with readable models and a simple dashboard.
- **Principles:** boring‑reliable infra; data contracts; test‑before‑prod; incremental delivery.
- **What’s here:** ingestion scaffolding, modeling patterns, validation hooks, and a minimal analytics UI pattern.
- **Roadmap highlights:** full North America coverage, **precipitation rates**, and **predictions** (nowcasting + short‑term).

---

## Issue → Solution → Why

- **Issue:** Weather data is fragmented, schema‑inconsistent, and hard to trust across sources and regions.
- **Solution:** A repeatable pipeline with clear layers (ingest → model → validate → serve) and CI that rejects bad changes before production.
- **Why it works:** Consistent contracts + automated checks reduce surprise in prod and keep iteration fast without sacrificing safety.

---

## Environments & Deployment Policy

- **Branches**
  - `feature/*`: active development
  - `dev`: integration and validation
  - `prod`: **production**

- **Deployment trigger**
  - **Merging to `prod` is production deployment.** That’s the contract.
  - Before opening a PR to `prod`, **test comprehensively** (see “Testing & Quality Gates”).

- **Expectations before merging to `prod`**
  1. All unit and integration tests pass locally.
  2. Data validation gates (schema + basic quality checks) pass on a representative sample.
  3. A smoke run completes end‑to‑end on `dev` (ingest → model → UI/API).
  4. The PR includes a short “risk & rollback” note.

> TL;DR: treat `prod` as sacred. If it merges, it ships.

---

## High‑level Architecture (implementation‑agnostic)

- **Ingest (Bronze):** fetch and land raw weather feeds (HTTP/CSV/Parquet/JSON). Immutable storage, partitioned by date/source.
- **Model (Silver/Gold):** normalize units/timezones, harmonize schemas, derive precipitation rates, build tidy tables for analysis.
- **Validate:** schema + expectation checks; sample‑based and rolling window checks.
- **Serve:** simple analytics UI and/or API for retrieval (maps, time series, summaries).

This design is cloud‑agnostic and intentionally “boring.” It can back onto local files/DuckDB for development and a warehouse (e.g., BigQuery/Snowflake/Redshift) for production.

---

## Getting Started (local)

Prereqs (any equivalent tooling is fine):
- Python 3.11+ (I use 3.13), `pipx` or `poetry`
- Docker (optional but recommended for local services)
- `make` (quality‑of‑life for common tasks)

Common flows:
```bash
# 1) Setup
make init         # or: poetry install

# 2) Run a local data pull (example; see Makefile/ops docs if present)
make ingest-sample

# 3) Build models
make models       # e.g., run transforms / notebooks / dbt models if configured

# 4) Validate
make validate     # run data quality checks

# 5) Launch demo UI (if available)
make ui
```

> If your environment differs, adapt the commands—this repo is designed to be portable.

---

## Testing & Quality Gates

- **Unit tests:** core transforms, utilities, schema conversions (`pytest`).
- **Integration tests:** end‑to‑end sample flow: ingest → model → serve.
- **Data validation:** schema checks + basic quality thresholds (e.g., null %, range checks, station counts). 
- **Smoke test:** minimal end‑to‑end run on `dev` before any `prod` PR.
- **CI (recommended):** run tests + validations on every PR; block `prod` merges on failures.

> Acronyms I use: **CI** (Continuous Integration), **PR** (Pull Request), **E2E** (End‑to‑End).

---

## Data & Domains

- **Core domains:** observations, forecasts, and derived metrics (e.g., precipitation rates).
- **Regions:** North America first (U.S., Canada, Mexico) with consistent spatial joins and timezones.
- **Units:** explicit unit conversions (SI/imperial) with clear metadata.

> Data providers may include national weather services and open/public datasets. Exact sources are documented per connector in the code/config as they’re added.

---

## Roadmap

The roadmap is ordered to deliver value early while keeping risk low. Checkboxes indicate intent/milestones.

### Phase 0 — MVP foundations
- [x] Repo and platform scaffolding (layers, contracts, make targets)
- [x] Sample ingestion + example model + validation hooks
- [x] Minimal UI pattern for exploration (map/time‑series friendly)

### Phase 1 — North America coverage
- [ ] Expand connectors for U.S., Canada, Mexico observations
- [ ] Normalize station metadata + timezone handling
- [ ] Regionally aware partitioning + retention policy

### Phase 1.1 — Precipitation rates
- [ ] Derive precipitation rates from raw observations
- [ ] Rolling window quality checks (spikes, dropouts)
- [ ] Exposure in models and UI/API

### Phase 2 — Predictions (nowcasting + short‑term)
- [ ] Integrate forecast feeds and/or lightweight nowcasting
- [ ] Backtesting harness and error metrics (MAE/RMSE, by region)
- [ ] Feature store pattern for reusable signals

### Phase 3 — Access & UX
- [ ] Simple read‑only API for programmatic access
- [ ] Dashboard: map + time‑series + region drill‑downs
- [ ] Basic auth/API keys + usage limits


Predictions are intentionally scoped after solid coverage + precipitation—modeling is only useful if the data is consistently trustworthy.

---

## Contributing (PR checklist)

Before opening a PR—especially into `prod`:
- [ ] Tests pass (`pytest -q`) and coverage didn’t drop on critical paths
- [ ] Data validation passes on a representative sample
- [ ] E2E smoke run is green on `dev`
- [ ] Short “risk & rollback” notes included in the PR description
- [ ] If changing schemas, update the contracts/docs

---

## FAQ

- **Can I run this without cloud credits?** Yes—dev runs locally; production is cloud‑agnostic.
- **What happens on merge to `prod`?** Deployment happens. That’s the rule—tests and validations must be green before that point.

---

## License

TBD — permissive open‑source license intended. If you have constraints, open an issue and we’ll align.

---

## Contact

If you’re evaluating me for a Data Platform / Analytics Engineering role, this repo is designed to be read quickly at the top, with pragmatic engineering choices reflected throughout. Happy to walk through trade‑offs and roadmap sequencing on a call.
