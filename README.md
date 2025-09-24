# Oaktree Estimator – Starter (GCP / KSA)

FastAPI + PostgreSQL starter for Oaktree’s cost/revenue estimator app. Built to match the approved blueprint and phased plan. Runs locally via Docker and deploys to **Google Cloud Run** in **Dammam (me-central2)** using keyless GitHub OIDC.  [oai_citation:2‡AI App Blueprint .docx](file-service://file-ALgZg1S1QWVEsFVxeedqkv)  [oai_citation:3‡comprehensive, step‑by‑step, end‑to‑end build guide.docx](file-service://file-2mLQo2SYnT3iuikLqGJy8N)

## Quick start (local)

```bash
cp .env.example .env
docker compose up -d db
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
alembic upgrade head
uvicorn app.main:app --reload --port 8000
# open http://127.0.0.1:8000/docs
pytest -q
```

### Endpoints (MVP)

- `GET /health`
- `GET /v1/indices/cci`
- `GET /v1/indices/rates`
- `GET /v1/comps`
- `POST /v1/estimates` (returns placeholder P50 pro-forma)

## Deploy (Google Cloud Run, me-central2)

1. Create a GCP project bound to CNTXT billing; enable: Cloud Run, Cloud SQL Admin, Artifact Registry, Secret Manager, Cloud Build.
2. Create an Artifact Registry repo (Docker) and Cloud SQL (Postgres). Put the DB password in Secret Manager as `DB_PASSWORD`.
3. Configure Workload Identity Federation (OIDC) for GitHub; create service account `github-deployer` with the required roles.
4. In GitHub → **Settings → Actions** configure:
   - **Variables**: `PROJECT_ID`, `REGION=me-central2`, `AR_REPO`, `SERVICE`
   - **Secrets**: `GCP_PROJECT_NUMBER`
5. Merge this PR; pushes to `main` trigger the **Deploy to GCP** workflow.

No secrets are committed. For production, switch the database to HA and add RBAC/SSO per the roadmap.  [oai_citation:5‡AI App Blueprint .docx](file-service://file-ALgZg1S1QWVEsFVxeedqkv)
