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

## Deploy (sccc by stc / Alibaba Cloud Riyadh, me-central-1)

1. In sccc by stc (Alibaba Cloud Riyadh), provision an ACK cluster in `me-central-1` and an ACR instance reachable at `cr.me-central-1.aliyuncs.com`. Create a RAM role that trusts GitHub’s OIDC provider so the workflow can exchange its short-lived token for STS credentials—no long-lived secrets are required.
2. In GitHub → **Settings → Actions** configure:
   - **Variables**: `ALIBABA_REGION=me-central-1`, `ACR_NAMESPACE`, `SERVICE_NAME`, `ACK_CLUSTER_ID`
   - **Secrets**: `ALIBABA_CLOUD_ACR_INSTANCE_ID`, `ALIBABA_CLOUD_RAM_ROLE_ARN`, `ALIBABA_CLOUD_RAM_OIDC_ARN`
3. Pushes to `main` trigger `.github/workflows/deploy-sccc.yml`. The workflow builds the Docker image, pushes it to ACR at `cr.me-central-1.aliyuncs.com`, then applies the manifests in `k8s/` to update the ACK deployment.

No secrets are committed. For production, switch the database to HA and add RBAC/SSO per the roadmap.  [oai_citation:5‡AI App Blueprint .docx](file-service://file-ALgZg1S1QWVEsFVxeedqkv)
