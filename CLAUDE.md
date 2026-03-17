# CLAUDE.md

## Project Overview

Oaktree Estimator — a full-stack geospatial real estate analysis platform for computing land costs, financing, revenues, and investment metrics for development projects (primarily Riyadh, Saudi Arabia). Two main applications:

1. **Cost/Revenue Estimator**: Draw site polygons on a map, input parameters, generate proforma analysis with PDF export.
2. **Expansion Advisor**: Restaurant/retail location intelligence — search for optimal sites, rank candidates, generate decision memos.

## Tech Stack

- **Backend**: Python 3.11, FastAPI, SQLAlchemy 2.0+, Alembic, PostGIS
- **Frontend**: React 18, TypeScript, Vite, MapLibre GL
- **Database**: PostgreSQL 15 with PostGIS 3.3
- **Testing**: pytest (backend), Vitest (frontend)
- **Formatting/Linting**: black, flake8
- **Deployment**: Docker, Kubernetes (Alibaba Cloud ACK), GitHub Actions

## Common Commands

```bash
# Backend
make test          # Run pytest -q
make fmt           # Format with black (app tests)
make lint          # Lint with flake8 (app tests)
make api           # Start dev server (uvicorn, port 8000)
make db-up         # Start PostgreSQL via Docker Compose
make db-init       # Run Alembic migrations

# Frontend
cd frontend
npm run dev        # Vite dev server (proxies to localhost:8000)
npm run build      # TypeScript check + production build
npm run test       # Vitest
```

## Project Structure

- `app/` — Backend Python package
  - `api/` — FastAPI routers (all endpoints under `/v1/`)
  - `services/` — Business logic
  - `models/tables.py` — SQLAlchemy ORM models
  - `db/` — Database session and dependency injection
  - `ingest/` — Data pipeline scripts
  - `ml/` — ML utilities
  - `core/config.py` — Settings
  - `security/` — API key auth
- `frontend/src/` — React SPA
  - `components/`, `features/`, `types/`, `i18n/`, `map/`, `styles/`
- `alembic/` — Database migrations
- `tests/` — pytest test suite (100+ tests)
- `k8s/` — Kubernetes manifests
- `data/` — Data files (shapefiles, buildings)

## Coding Conventions

- JSON list responses use `{ "items": [...] }` wrapper.
- All API endpoints are prefixed with `/v1/`.
- Use Pydantic models for request/response validation.
- Coordinate system: WGS84 (EPSG:4326).
- Update README when changing API behavior.
- Frontend uses i18next for English/Arabic translations.
- Frontend uses CSS modules.

## Testing

- Backend tests live in `tests/` and use pytest with transaction-based isolation.
- Always run `make test` before committing backend changes.
- Frontend tests use Vitest: `cd frontend && npm run test`.

## Database

- Schema changes require an Alembic migration (`alembic revision --autogenerate`).
- PostGIS spatial queries use GiST indexes.
- Parcel data source is configurable via `PARCEL_TILE_TABLE` and `PARCEL_IDENTIFY_TABLE` env vars.
