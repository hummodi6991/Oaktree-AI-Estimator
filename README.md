# Oaktree Atlas — Geospatial Real-Estate Intelligence for Riyadh

> A full-stack geospatial decision platform that turns a map polygon or a plain-language brief into an investment-grade real-estate recommendation. Built end-to-end for **Riyadh, Saudi Arabia** — from PostGIS data pipelines through a deterministic scoring engine to a bilingual (Arabic/English) React map UI and exported PDF memos.

Oaktree Atlas (a.k.a. Oaktree Estimator) is a production-shaped application I designed and built across the entire stack: spatial data ingestion, a FastAPI/PostGIS backend, machine-learning and LLM-assisted scoring, a MapLibre front end, and Kubernetes deployment automation. This README is a guided tour of that work.

---

## Table of contents

- [What it does](#what-it-does)
- [Why it's interesting (engineering highlights)](#why-its-interesting-engineering-highlights)
- [Architecture at a glance](#architecture-at-a-glance)
- [Tech stack](#tech-stack)
- [The two product surfaces](#the-two-product-surfaces)
- [Data platform & ingestion](#data-platform--ingestion)
- [Machine learning & LLM layer](#machine-learning--llm-layer)
- [Repository map](#repository-map)
- [API surface](#api-surface)
- [Running it locally](#running-it-locally)
- [Testing, CI & deployment](#testing-ci--deployment)
- [Engineering principles I followed](#engineering-principles-i-followed)

---

## What it does

The platform answers two high-value questions for real-estate operators in Riyadh:

1. **"Is this site worth developing, and what does the pro-forma look like?"**
   Draw or identify a parcel on the map, enter program assumptions (FAR, use mix, timeline), and the **Development Feasibility / Estimator** computes land cost, financing, revenue, parking compliance, FAR-driven build-up area, and a full feasibility distribution (P5/P50/P95) — exportable as a PDF memo in English or Arabic.

2. **"Where should this brand open its next branch?"**
   Describe a brand and its constraints (or paste a free-text brief), and the **Expansion Advisor** generates, scores, and ranks candidate locations across the city — with explainable gate logic, cannibalization analysis, delivery-market and competitor intelligence, economics/payback estimates, and a deterministic decision memo per candidate.

Both surfaces are built on the same spatial backbone: a PostGIS database of Riyadh parcels, building footprints, roads, points of interest, rents, and demand proxies, surfaced through vector tiles and a `/v1/*` JSON API.

---

## Why it's interesting (engineering highlights)

This isn't a CRUD app. The parts I'm most proud of:

- **Real geospatial engineering.** Parcel identify-by-click, server-side vector-tile generation with zoom-based simplification, and metric computations done correctly via `EPSG:4326 ↔ EPSG:32638` transforms. Spatial joins against parcels, OSM roads, and parking assets feed an explainable access/frontage/visibility scoring model.
- **A deterministic, explainable scoring engine.** The Expansion Advisor's ranking is reproducible and auditable: every candidate carries a `score_breakdown_json` (weights → inputs → weighted components → final score), a gate checklist split into `passed` / `failed` / `unknown` buckets, and a `feature_snapshot_json` that distinguishes *unavailable context* from *weak candidate quality*. Decision quality and explainability were treated as first-class requirements, not afterthoughts.
- **ML where it earns its place.** Hedonic land-price models, restaurant-suitability and profitability models, and a demand heatmap — trained via dedicated pipelines (MLflow-tracked) and served behind feature flags so model changes can ship safely.
- **An LLM layer that stays grounded.** GPT-based components extract structured search briefs from free text, draft decision memos, classify listing suitability, and re-rank candidates — but always on top of deterministic features, behind flags, with the heuristic engine as the source of truth.
- **Bilingual, RTL-correct by construction.** Full i18next coverage (Arabic/English) on the front end, plus a genuinely hard piece of work: rendering **Arabic PDF memos** with correct contextual shaping (`arabic-reshaper`) and BiDi reordering (`python-bidi`).
- **A serious data platform.** ~25 ingestion jobs pull and normalize ArcGIS parcels, Microsoft GlobalML building footprints, Overture buildings, OSM roads, Aqar/Bayut listings, REGA indicators, Black Marble night-lights radiance, and population density — orchestrated through GitHub Actions and Kubernetes CronJobs.
- **Production deployment reality.** Dockerized, deployed to **Alibaba Cloud ACK** (sccc by stc, Riyadh `me-central-1`) via GitHub Actions, with Kubernetes manifests validated in CI before apply.

**By the numbers:** ~143 Python modules · ~135 React/TypeScript files · 91 Alembic migrations · 132 test files · 37 CI/automation workflows.

---

## Architecture at a glance

```
                         ┌────────────────────────────────────────────┐
                         │  React 18 + TypeScript + Vite + MapLibre GL │
                         │  • Estimator (draw → pro-forma → PDF)       │
                         │  • Expansion Advisor (brief → ranked sites) │
                         │  • i18next (AR/EN, RTL)                      │
                         └───────────────────┬────────────────────────┘
                                             │  /v1/* JSON + vector tiles (.pbf)
                         ┌───────────────────▼────────────────────────┐
                         │            FastAPI (Python 3.11)            │
                         │  api/  → routers   services/ → business     │
                         │  security/ → auth modes & request guards    │
                         │  ┌───────────────┬──────────────────────┐   │
                         │  │ Estimator     │ Expansion Advisor     │  │
                         │  │ costs/revenue │ candidate gen + gates │  │
                         │  │ FAR/parking   │ scoring + memos       │  │
                         │  │ financing/tax │ cannibalization       │  │
                         │  └───────┬───────┴───────────┬───────────┘   │
                         │     ml/ (hedonic, scoring)  LLM layer        │
                         └──────────┬──────────────────────────────────┘
                                    │ SQLAlchemy 2.0 / Alembic
                         ┌──────────▼──────────────────────────────────┐
                         │       PostgreSQL 15 + PostGIS                │
                         │  parcels · buildings · roads · POIs · rents  │
                         │  proxy & materialized views (app contract)  │
                         └──────────▲──────────────────────────────────┘
                                    │ ingest/  (≈25 jobs)
                  ArcGIS · MS Buildings · Overture · OSM · Aqar/Bayut ·
                  REGA · Black Marble radiance · population density
```

---

## Tech stack

| Layer | Technologies |
|---|---|
| **Backend** | Python 3.11, FastAPI, Pydantic v2, SQLAlchemy 2.0, Alembic |
| **Database** | PostgreSQL 15 + PostGIS (proxy & materialized views) |
| **Geospatial** | Shapely, pyproj, mapbox-vector-tile, h3, EPSG:4326 / EPSG:32638 |
| **ML / data** | scikit-learn, pandas, numpy, pyarrow, MLflow, joblib |
| **LLM** | OpenAI GPT-4o-mini (brief extraction, memos, suitability, rerank) — flag-gated |
| **Frontend** | React 18, TypeScript, Vite, MapLibre GL, i18next, proj4 |
| **PDF** | fpdf2 + arabic-reshaper + python-bidi (RTL Arabic rendering) |
| **Observability** | OpenTelemetry (FastAPI + httpx instrumentation, OTLP export) |
| **Testing** | pytest (backend), Vitest (frontend) |
| **Tooling** | black, flake8 |
| **Deploy** | Docker, Kubernetes on Alibaba Cloud ACK (Riyadh `me-central-1`), GitHub Actions |

---

## The two product surfaces

### 1) Development Feasibility / Estimator

From a polygon to a memo: draw or click-identify a parcel, set program inputs, and run an estimate that ties together a full development pro-forma.

- **Land pricing** via a blended engine (Suhail anchor + Kaggle Aqar district medians, district-resolved once).
- **FAR resolution** from polygon features first, falling back to a district-level FAR rules table.
- **Build-up area & use mix** (residential / retail / office / mixed-use) with Riyadh-calibrated assumptions.
- **Riyadh parking minimums** enforced from the municipal guide — required vs. provided stalls, with an optional `auto_add_basement` policy that resolves deficits by growing below-grade area (excluded from FAR scaling).
- **Financing, tax, residual, and pro-forma** modules produce a P5/P50/P95 feasibility distribution.
- **PDF memos** in English and Arabic (RTL-correct).
- **Scenario deltas** to test sensitivity (e.g. ±x% price).

Core code: `app/api/estimates.py`, `app/services/{costs,revenue,parking,far_rules,financing,tax,residual,proforma,land_price_engine,excel_method}.py`.

### 2) Expansion Advisor

The primary user-facing workflow — restaurant/retail location intelligence with a strong emphasis on **explainability**.

- **Candidate generation** across a bounding box / target districts, respecting brand profile and service model (`qsr` / `dine_in` / `delivery_first` / `cafe`).
- **Deterministic gates** — zoning fit, area fit, frontage/access, parking, district policy, cannibalization, delivery-market, economics, and an overall pass — each with explicit `passed` / `failed` / `unknown` reasoning (unknown context never silently fails a candidate).
- **Scoring breakdown** exposing weights, inputs, weighted components, and the final score, so a ranking can always be defended.
- **Cannibalization analysis** — deterministic, distance-based, adjusted by service model and existing branches.
- **Provider & delivery-market intelligence** — provider density, whitespace, multi-platform presence, delivery competition.
- **Economics** — estimated rent, fit-out, revenue index, payback band (`strong`/`promising`/`borderline`/`weak`).
- **Decision memos & reports** — per-candidate memos with a `go`/`consider`/`caution` verdict, comparison endpoints, and an executive-style search report.
- **Saved studies** — persist briefs, shortlists, and UI state for later hydration.

Core code: `app/api/expansion_advisor.py`, `app/services/expansion_advisor*.py`, `app/services/expansion_rerank.py`, migrations under `alembic/versions/20260310_*…20260314_*`, and `frontend/src/features/expansion-advisor/`.

---

## Data platform & ingestion

The app's intelligence comes from a curated Riyadh spatial corpus. `app/ingest/` contains ~25 jobs (run locally, via GitHub Actions, or as Kubernetes CronJobs):

| Domain | Sources |
|---|---|
| **Parcels** | ArcGIS Riyadh parcels (production default, via `riyadh_parcels_arcgis_proxy`), Suhail tiles (resumable importer), inferred parcels from building footprints |
| **Buildings** | Microsoft GlobalML Building Footprints (Riyadh-filtered), Overture buildings |
| **Roads / context** | OSM roads & parking assets, expansion road/parking context tables |
| **Listings & rents** | Aqar and Bayut scrapers, Kaggle Aqar comps, REGA indicators, rent comps |
| **Demand proxies** | Black Marble night-lights radiance, population density, restaurant POIs, Google Places grid search |

Parcel sourcing is configurable but defaults to ArcGIS production tables (`PARCEL_TILE_TABLE` / `PARCEL_IDENTIFY_TABLE`). Vector tiles are served at all zoom levels with zoom-based simplification and minimum-area filtering for legible low-zoom outlines.

Detailed ingestion docs live in [`docs/`](docs/) (e.g. `arcgis_parcel_ingest.md`, `expansion_advisor_data_ingest.md`).

---

## Machine learning & LLM layer

**Models** (`app/ml/`, trained via the `train-*` GitHub workflows, MLflow-tracked):

- `hedonic_train.py` — hedonic land-price model
- `restaurant_score_train.py` / `restaurant_heatmap_train.py` — suitability & demand heatmap
- `profitability_train.py` — branch profitability
- `name_normalization.py` — brand/POI name normalization

**LLM components** (`app/services/llm_*.py`, all flag-gated, all grounded on deterministic features):

- `llm_brief_extraction.py` — turn a free-text brand brief into a structured search request
- `llm_decision_memo.py` — draft candidate decision memos on top of computed evidence
- `llm_suitability.py` — GPT-4o-mini listing-suitability classification
- `expansion_rerank.py` — optional LLM re-rank layered over the heuristic ranking

The deterministic engine is always the source of truth; LLM output augments explainability rather than replacing scoring. Investigation/validation write-ups for many of these changes live in `docs/investigations/`.

---

## Repository map

```
app/
  api/          FastAPI routers (estimates, expansion_advisor, search, tiles,
                geo_portal, pricing, comps, indices, metadata, analytics, …)
  services/     Business logic — costs, revenue, parking, FAR, financing, tax,
                residual, proforma, land pricing, expansion scoring, memos, PDF
  ml/           Model training & feature utilities
  ingest/       Data ingestion / refresh / pipeline jobs (~25)
  models/       SQLAlchemy models (tables.py)
  db/           Engine / session wiring
  core/         Runtime settings & feature flags (config.py)
  security/     Auth modes (disabled / api_key / oidc) & request guards
  connectors/   External data-source clients
  delivery/     Delivery-market logic
frontend/
  src/features/expansion-advisor/   Advisor UI (forms, candidate cards, memos…)
  src/map/                          MapLibre layers, parcel & overlay rendering
  src/lib/api/                      Typed API clients + normalizers
  src/i18n/                         i18next setup + en.json / ar.json
  src/components, ui-v2, utils      Shared UI & helpers
alembic/versions/   91 migrations (schema history)
k8s/                Deployment, service, CronJobs (Alibaba ACK)
.github/workflows/  37 workflows: CI, deploy, ingestion, model training
docs/               Focused technical docs & investigation reports
tests/              132 pytest modules
sql/, scripts/      Helper SQL & operational scripts
```

---

## API surface

All application endpoints live under `/v1/*` and JSON list endpoints use `{ "items": [...] }`.

**Estimator / feasibility**
- `POST /v1/estimates` — full pro-forma (FAR, parking, financing, P5/P50/P95)
- `POST /v1/geo/building-metrics` — coverage / floors proxy / BUA from footprints
- `GET /v1/pricing/land`, `GET /v1/comps`, `GET /v1/indices/{cci,rates}`
- `GET /v1/metadata/parking-rules`, `GET /v1/metadata/freshness`

**Geo / map**
- `GET /v1/tiles/parcels/{z}/{x}/{y}.pbf` — parcel vector tiles
- `GET /v1/geo/identify?lng=&lat=&tol_m=` — parcel identify-by-click
- `GET /v1/search` — bilingual (Arabic/English) parcel & place search

**Expansion Advisor**
- `POST /v1/expansion-advisor/searches` — generate ranked candidates
- `GET  /v1/expansion-advisor/searches/{id}` · `/candidates` · `/report`
- `POST /v1/expansion-advisor/candidates/compare`
- `GET  /v1/expansion-advisor/candidates/{id}/memo`
- `… /saved-searches` (full CRUD for saved studies)

Interactive docs are served at `/docs` when the API is running. Response contracts are intentionally strict and deterministic — gate/score/feature JSON always include their default keys so the front end can render without defensive null-checking.

---

## Running it locally

**Prerequisites:** Docker, Python 3.11, Node 18+.

```bash
# Backend
cp .env.example .env
docker compose up -d db            # PostgreSQL 15 + PostGIS
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
alembic upgrade heads
uvicorn app.main:app --reload --port 8000
# open http://127.0.0.1:8000/docs
```

```bash
# Frontend
cd frontend
npm install
cp .env.development.example .env.development   # set VITE_API_BASE_URL
npm run dev
```

Convenience targets (`Makefile`): `make db-up`, `make db-init`, `make api`, `make test`, `make fmt`, `make lint`, plus `make ingest-*` / `make fetch-*` for data jobs.

**Optional sample data**

```bash
python scripts/ingest_samples.py
curl -fsS 127.0.0.1:8000/v1/metadata/freshness
```

**Geospatial smoke check**

```bash
curl -fsS "http://127.0.0.1:8000/v1/tiles/parcels/15/20634/14062.pbf" -o /tmp/parcels.pbf && ls -lh /tmp/parcels.pbf
curl -fsS "http://127.0.0.1:8000/v1/geo/identify?lng=46.675&lat=24.713&tol_m=25"
```

> Detailed ingestion recipes (shapefiles, FAR rules CSV, Microsoft buildings, Suhail tiles) are in [`docs/`](docs/).

---

## Testing, CI & deployment

- **Backend:** `make test` (pytest) — 132 test modules covering scoring determinism, gate logic, pricing, parking, and API contracts.
- **Frontend:** `cd frontend && npm run test` (Vitest) and `npm run build` (tsc + Vite). Components, API normalizers, and i18n are unit-tested.
- **CI:** `.github/workflows/ci.yml` runs lint + tests; ingestion and model-training workflows run on schedule/dispatch.
- **Schema:** every schema change ships an Alembic migration; the upgrade path is validated as part of the workflow.
- **Deploy:** pushes to `main` trigger `.github/workflows/deploy-sccc.yml`, which builds the Docker image, pushes to Alibaba Enterprise ACR, validates `k8s/` manifests with `kubectl apply --dry-run=client`, then applies them to the **ACK cluster in Riyadh (`me-central-1`)**.

**Auth modes** (`app/security/`): `disabled`, `api_key`, and a placeholder `oidc` path — no secrets are committed; production credentials are injected via GitHub Actions variables/secrets.

---

## Engineering principles I followed

These are encoded in [`CLAUDE.md`](CLAUDE.md) and reflected throughout the history:

- **Riyadh-first correctness** — coordinate systems, parking/FAR rules, and rent/demand data are all calibrated to Riyadh; no non-Riyadh assumptions leak into shared logic.
- **Smallest safe diff** — grounded, targeted fixes over speculative refactors; existing architecture preserved unless there's a strong reason.
- **Explainability over black boxes** — provenance, evidence, and score breakdowns are surfaced, never silently dropped for convenience.
- **Deterministic, testable contracts** — strict response shapes, default-populated JSON, and behavior that's verifiable from both the UI and the API.
- **Front-end / back-end / schema kept in lock-step** — contract changes land on both sides in the same patch, with i18n keys updated in both locales.

---

## License

See [`LICENSE`](LICENSE).

---

*Oaktree Atlas is a personal portfolio project demonstrating full-stack, geospatial, ML, and LLM engineering applied to a real, domain-specific problem. It is Riyadh-focused by design.*
