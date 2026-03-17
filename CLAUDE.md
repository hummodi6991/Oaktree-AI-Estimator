# CLAUDE.md

This file tells Claude Code how to work effectively in this repo.

## Mission

Optimize for:

1. **Accuracy of app behavior in production**
2. **Riyadh-specific correctness**
3. **Safe, minimal, targeted diffs**
4. **High signal patches that are easy to review and merge**

Prefer grounded fixes over speculative refactors.

## Product context

Oaktree Estimator / Oaktree Atlas is a full-stack geospatial real-estate platform focused on **Riyadh, Saudi Arabia**.

Two major product surfaces:

1. **Development Feasibility / Estimator**
   - Draw or identify a site / parcel
   - Compute land costs, financing, revenues, parking, FAR-related outcomes, and feasibility metrics
   - Export PDF-style outputs

2. **Expansion Advisor**
   - Restaurant / retail location intelligence
   - Search for optimal sites
   - Rank candidates
   - Compare branches / cannibalization
   - Generate decision-oriented outputs and memos

When making product decisions in code, prioritize what improves **decision quality**, **explainability**, and **internal consistency** of outputs.

## Non-negotiable repo truths

- This project is **Riyadh-first and Riyadh-only for now**.
- Do not generalize behavior to other cities unless the code already supports it and the task explicitly requires it.
- Prefer the **production parcel source defaults** unless the task is explicitly about alternative parcel sources:
  - `PARCEL_TILE_TABLE=public.riyadh_parcels_arcgis_proxy`
  - `PARCEL_IDENTIFY_TABLE=public.riyadh_parcels_arcgis_proxy`
- Spatial data is primarily handled in **EPSG:4326**, with metric computations commonly done through **EPSG:32638** transforms.
- Backend endpoints live under `/v1/*`.
- JSON list endpoints should use `{ "items": [...] }`.

## Working style for Claude Code

### Default approach

- Make the **smallest patch** that fully solves the problem.
- Preserve existing architecture unless there is a strong reason not to.
- Do not introduce broad abstractions for one local fix.
- Do not silently change business logic unless the task explicitly calls for it.
- Keep behavior deterministic and easy to validate from the UI and API.

### Be careful about

- Geospatial coordinate-system mistakes
- Riyadh vs non-Riyadh data leakage
- Search / ranking regressions caused by over-aggressive dedupe or filtering
- Performance regressions inside candidate scoring loops
- Changes that alter output semantics without updating tests / docs
- Frontend/backend contract drift

### When proposing a patch

Always try to include:

1. What is wrong now
2. Why it happens
3. Smallest safe fix
4. Validation steps
5. Any risk or tradeoff

## Tech stack

- **Backend**: Python 3.11, FastAPI, SQLAlchemy, Alembic, PostGIS
- **Frontend**: React 18, TypeScript, Vite, MapLibre GL
- **Database**: PostgreSQL 15 + PostGIS
- **Testing**: pytest, Vitest
- **Formatting / linting**: black, flake8
- **Deployment**: Docker, Kubernetes on Alibaba Cloud ACK via GitHub Actions

## Repo map

- `app/api/` — FastAPI routers
- `app/services/` — business logic
- `app/models/tables.py` — SQLAlchemy models
- `app/db/` — DB engine/session wiring
- `app/core/config.py` — runtime settings
- `app/security/` — auth modes and request guards
- `app/ingest/` — ingestion / refresh / pipeline jobs
- `app/ml/` — model training / feature utilities
- `frontend/src/` — React application
- `alembic/versions/` — schema history
- `.github/workflows/` — CI, deploy, ingest, training automation
- `docs/` — focused technical docs

## Highest-value areas to understand before editing

### If the task touches Expansion Advisor

Read / inspect first:

- `app/api/expansion_advisor.py`
- `app/services/expansion_advisor.py`
- related migrations under `alembic/versions/20260310_*` through `20260314_*`
- relevant ingestion jobs under `app/ingest/expansion_advisor_*`
- any affected frontend feature files under `frontend/src/features/`

Focus on:

- candidate generation
- ranking / score breakdowns
- gate logic
- feature snapshot completeness
- decision-summary consistency
- shortlist diversity
- cannibalization / provider density / delivery-market logic

### If the task touches Estimator / Feasibility

Read / inspect first:

- `app/api/estimates.py`
- `app/services/` modules for costs, revenue, parking, FAR, financing, tax, residual, proforma
- parcel identify / tiles code if the issue is map driven

Focus on:

- internal consistency of calculations
- Riyadh parking/FAR assumptions
- evidence / assumptions surfaced to the user

### If the task touches parcel search / map behavior

Read / inspect first:

- `app/api/search.py`
- `app/api/tiles.py`
- `app/api/geo_portal.py`
- `app/core/config.py`
- parcel proxy/materialized-view migrations

Focus on:

- active parcel table
- identify behavior
- tile output correctness
- Arabic/English search quality
- query performance

## Commands

```bash
# Backend
make test
make fmt
make lint
make api
make db-up
make db-init

# Frontend
cd frontend && npm run dev
cd frontend && npm run build
cd frontend && npm run test
```

## Validation playbook

### For backend-only patches

- Run the narrowest relevant pytest targets first
- Then run `make test` if the change is broad or risky
- If schema changed, verify Alembic upgrade path is valid
- If API response shape changed, update docs / README accordingly

### For frontend-only patches

- Run `cd frontend && npm run build`
- Run relevant Vitest tests if present
- Check for nullability / API contract mismatches
- If UI text changes, update i18n keys in both locales

### For backend + frontend contract changes

- Validate both sides in the same patch
- Keep naming aligned
- Prefer additive response changes over breaking removals
- Update tests on both sides when needed

### For geospatial / ranking / scoring changes

Validate with real-world sanity checks:

- Are results still in Riyadh?
- Do scores remain internally consistent?
- Are top candidates overly repetitive?
- Did dedupe become too aggressive?
- Are distances / areas / reach metrics plausible?
- Did performance get materially worse?

## Database and migrations

- Schema changes require Alembic migrations.
- Keep Alembic revisions focused and reviewable.
- Prefer additive migrations over destructive ones.
- PostGIS-heavy queries should be checked for index usage and avoid unnecessary repeated transforms.
- Materialized views / proxy views are part of the app contract; change them carefully.

## Auth and deploy reality

- Do **not** assume OIDC is fully live everywhere.
- Current repo state supports:
  - `AUTH_MODE=disabled`
  - `AUTH_MODE=api_key`
  - `AUTH_MODE=oidc` exists but is still a placeholder in app auth unless explicitly wired further
- Current deploy workflow for Alibaba ACK uses GitHub Actions and currently authenticates with **Alibaba access key / secret** in the workflow.
- Pushes to `main` trigger deployment.

When editing docs or automation, prefer describing the **current implemented behavior**, not the aspirational end state.

## Frontend conventions

- Use i18next for user-facing strings.
- Add new translation keys to both locale files.
- Keep Arabic support intact.
- Avoid UI-only patches that break backend assumptions.
- Prefer explaining score/output changes in the UI when the underlying model behavior changes.

## Coding conventions

- Use Pydantic models for request/response validation.
- Keep endpoint families under `/v1/`.
- Keep patches readable rather than clever.
- Follow existing naming patterns in the touched module.
- Update README/docs when behavior meaningfully changes.

## What not to do

- Do not hardcode non-Riyadh assumptions into shared logic.
- Do not commit secrets or invent secret names not already used by the repo.
- Do not claim OIDC is fully live if the code path is still placeholder-based.
- Do not make broad refactors when the user asked for a targeted fix.
- Do not weaken ranking quality just to make results look more diverse.
- Do not optimize performance by dropping evidence, provenance, or explainability fields unless explicitly requested.

## Preferred patch shape

When possible, produce:

1. A concise diagnosis
2. A unified diff
3. Exact validation commands
4. Merge recommendation with risk level

## Definition of done

A patch is "done" when:

- it solves the reported problem,
- it is consistent with Riyadh production behavior,
- it keeps frontend/backend/schema contracts aligned,
- it includes or implies a concrete validation path,
- and it does not introduce obvious regressions in accuracy, explainability, or deployability.
