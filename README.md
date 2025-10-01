# Oaktree Estimator – Starter (GCP / KSA)

## Operator Quickstart (from a polygon to a memo)
1. Start the API locally (or hit staging) and the React UI (Vite).
2. In the UI, draw a site polygon (Riyadh default). Enter city/FAR/timeline.
3. Click **Run Estimate** to compute land, costs, financing, revenues, and P5/P50/P95.
4. Click **Open PDF Memo** to export.
5. Use **Scenario** to test deltas (e.g., +x% price).

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

### Load sample data (optional)
```bash
python scripts/ingest_samples.py
curl -fsS 127.0.0.1:8000/v1/metadata/freshness
```

### Endpoints (MVP)

- `GET /health`
- `GET /v1/indices/cci`
- `GET /v1/indices/rates`
- `GET /v1/comps`
- `POST /v1/estimates` (returns placeholder P50 pro-forma)

## Frontend dev

Start the API by following the Quick start above (or `make db-up && make db-init && make api`). Then run the Vite dev server:

```bash
cd frontend
npm install
cp .env.development.example .env.development
# For Codespaces: open the forwarded 8000 port link and paste that as VITE_API_BASE_URL.
npm run dev
```

If you're using Codespaces, the FastAPI URL will look like:

```
https://<your-codespace>-8000.app.github.dev
```

### Using the UI against staging (ACK)
If the API is deployed on sccc/ACK, set:

```
VITE_API_BASE_URL=https://<your-loadbalancer-dns-or-ip>
```

## Deploy (sccc by stc / Alibaba Cloud Riyadh, me-central-1)

1. In sccc by stc (Alibaba Cloud Riyadh), provision an ACK cluster in `me-central-1` and an **Enterprise ACR instance** (the registry should expose a domain such as `oaktree-ai-estimator-registry.me-central-1.cr.aliyuncs.com`).
2. Until GitHub OIDC is enabled in the region, the workflow authenticates with AK/SK credentials. In GitHub → **Settings → Actions** configure:
   - **Variables**: `ALIBABA_REGION=me-central-1`, `ACR_NAMESPACE`, `SERVICE_NAME`, `ACK_CLUSTER_ID`, `ACR_LOGIN_SERVER=<enterprise-acr-domain>`
   - **Secrets**: `ALIBABA_CLOUD_ACR_INSTANCE_ID`, `ALIBABA_ACCESS_KEY_ID`, `ALIBABA_ACCESS_KEY_SECRET`
3. Pushes to `main` trigger `.github/workflows/deploy-sccc.yml`. The workflow builds the Docker image, pushes it to the Enterprise ACR domain specified in `ACR_LOGIN_SERVER`, then applies the manifests in `k8s/` to update the ACK deployment.
4. (Preferred, once available) Switch back to GitHub OIDC by providing `ALIBABA_CLOUD_RAM_ROLE_ARN` and `ALIBABA_CLOUD_RAM_OIDC_ARN` secrets and removing the AK/SK credentials.

No secrets are committed. For production, switch the database to HA and add RBAC/SSO per the roadmap.  [oai_citation:5‡AI App Blueprint .docx](file-service://file-ALgZg1S1QWVEsFVxeedqkv)
