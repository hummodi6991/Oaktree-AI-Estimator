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
