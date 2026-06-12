# Brief Extraction Flag Flip — Full Report

**Branch:** `claude/enable-brief-extraction-c4hwwg` (based on latest `main` @ `745e52f21`)
**Commit:** `927e6e1f5` — `chore(deploy): enable brief extraction; declare expansion flags in manifest`
**Files changed:** 3 (`k8s/deployment.yaml`, `Dockerfile`, `.github/workflows/deploy-sccc.yml`) — no app code, no other flags.

---

## 1. Discovery findings

### 1.1 Where the container env is defined, and the missing flags

`k8s/deployment.yaml` is the only manifest for the `oaktree-estimator` Deployment. Before this
change, `EXPANSION_WEIGHT_STACK` and `EXPANSION_ARCHETYPE_PROFILES` were **not declared anywhere
in `k8s/`** — the code defaults are `v1` and `false` (`app/core/config.py:427` and
`app/core/config.py:463`), so the production values must have been set imperatively via
`kubectl set env`.

This was a live footgun: `deploy-sccc.yml` runs `kubectl apply -f k8s/` on every push to `main`,
which re-applies the Deployment spec and would have silently reverted both flags to the code
defaults on the next deploy. Both are now declared in the manifest (`v2` / `"true"`), in the same
commit as the brief-extraction flip.

### 1.2 How `OPENAI_API_KEY` reaches the container

- It lives in the Kubernetes Secret **`oaktree-db-env`** under the key **`OPENAI_API_KEY`**.
- The Deployment loads it via `envFrom: secretRef: oaktree-db-env` (`k8s/deployment.yaml`).
- Critically, the deploy workflow **recreates that Secret on every deploy** from the GitHub
  Actions secret `OPENAI_API_KEY` (`.github/workflows/deploy-sccc.yml`, step
  "Create/Update DB Secret (oaktree-db-env)", line ~170). A `kubectl`-only edit of the Secret is
  therefore overwritten by the next deploy.
- The key value was **not touched** in this change.

### 1.3 How the frontend build gets its `VITE_*` vars

The production frontend is built inside the Docker image. The chain is:

```
deploy-sccc.yml  →  docker build --build-arg VITE_…=…
Dockerfile (webbuild stage)  →  ARG VITE_… / ENV VITE_…
npm run build (Vite)  →  value baked into the static bundle
```

`frontend/.env.production` exists but only sets `VITE_API_BASE_URL` and `VITE_MAP_STYLE`;
process-level env from the Dockerfile takes precedence in Vite, matching the proven
`VITE_PARCEL_TILE_TABLE` pattern. The frontend flag reader is
`frontend/src/features/expansion-advisor/briefExtraction.ts:12-15`, which accepts `"1"` or
`"true"`.

---

## 2. Changes made

### `k8s/deployment.yaml` — three new explicit `env:` entries

```yaml
# Declared here (not via kubectl set env) so deploys that
# re-apply k8s/ never revert them. v2 weight stack and
# archetype profiles are the live production scoring config.
- name: EXPANSION_WEIGHT_STACK
  value: "v2"
- name: EXPANSION_ARCHETYPE_PROFILES
  value: "true"
# "Describe your brand" LLM brief extraction (backend flag;
# the matching frontend flag is baked at image build time via
# VITE_EXPANSION_BRIEF_EXTRACTION_ENABLED in deploy-sccc.yml).
- name: EXPANSION_BRIEF_EXTRACTION_ENABLED
  value: "true"
```

### `Dockerfile` — new ARG/ENV in the webbuild stage

```dockerfile
ARG VITE_EXPANSION_BRIEF_EXTRACTION_ENABLED
ENV VITE_EXPANSION_BRIEF_EXTRACTION_ENABLED=$VITE_EXPANSION_BRIEF_EXTRACTION_ENABLED
```

### `.github/workflows/deploy-sccc.yml` — new build arg

```yaml
docker build \
  --build-arg VITE_PARCEL_TILE_TABLE=public.riyadh_parcels_arcgis_proxy \
  --build-arg VITE_EXPANSION_BRIEF_EXTRACTION_ENABLED=true \
  -t "$IMAGE" .
```

Validation done: both YAML files parse cleanly (`yaml.safe_load`). Kubernetes precedence
guarantees explicit `env:` entries override `envFrom` Secret values, so the manifest values
always win regardless of what the secrets contain.

---

## 3. Every env var this deployment now declares, and where each lives

| Env var | Value | Where it lives |
|---|---|---|
| `APP_ENV` | `prod` | `k8s/deployment.yaml` explicit `env:` (also in `oaktree-app-env` Secret) |
| `PARCEL_TILE_TABLE` | `public.riyadh_parcels_arcgis_proxy` | `k8s/deployment.yaml` explicit `env:` (also in `oaktree-app-env`) |
| `PARCEL_IDENTIFY_TABLE` | `public.riyadh_parcels_arcgis_proxy` | `k8s/deployment.yaml` explicit `env:` (also in `oaktree-app-env`) |
| `PARCEL_IDENTIFY_GEOM_COLUMN` | `geom` | `k8s/deployment.yaml` explicit `env:` (also in `oaktree-app-env`) |
| `PARCEL_TARGET_SRID` | `4326` | `k8s/deployment.yaml` explicit `env:` (also in `oaktree-app-env`) |
| `EXPANSION_MEMO_PREWARM_ENABLED` | `false` | `k8s/deployment.yaml` explicit `env:` |
| `EXPANSION_WEIGHT_STACK` | `v2` | **new** — `k8s/deployment.yaml` explicit `env:` (was `kubectl set env` only) |
| `EXPANSION_ARCHETYPE_PROFILES` | `true` | **new** — `k8s/deployment.yaml` explicit `env:` (was `kubectl set env` only) |
| `EXPANSION_BRIEF_EXTRACTION_ENABLED` | `true` | **new** — `k8s/deployment.yaml` explicit `env:` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` / `POSTGRES_HOST` / `POSTGRES_PORT` / `PGSSLMODE` | — | `oaktree-db-env` Secret via `envFrom` (recreated each deploy from GitHub Actions secrets) |
| `OPENAI_API_KEY` | — | `oaktree-db-env` Secret via `envFrom` (sourced from GitHub Actions secret `OPENAI_API_KEY`) |
| `AUTH_MODE` | from GH secret, default `disabled` | `oaktree-app-env` Secret via `envFrom` |
| `PARCEL_IDENTIFY_TOLERANCE_M` / `PARCEL_ENVELOPE_PAD_M` / `PARCEL_SIMPLIFY_TOLERANCE_M` | `15` / `5` / `1` | `oaktree-app-env` Secret via `envFrom` |
| `API_KEYS_JSON` / `ADMIN_API_KEYS_JSON` | — | `oaktree-app-env` Secret via `envFrom` (only added if the GH secret is non-empty) |
| `VITE_EXPANSION_BRIEF_EXTRACTION_ENABLED` | `true` | **new** — build-time only: `deploy-sccc.yml` `--build-arg` → `Dockerfile` ARG/ENV → baked into the frontend bundle |
| `VITE_PARCEL_TILE_TABLE` | `public.riyadh_parcels_arcgis_proxy` | build-time only: same build-arg mechanism |

---

## 4. How to update the OpenAI key

The key lives in the Kubernetes Secret **`oaktree-db-env`**, key **`OPENAI_API_KEY`** — but that
Secret is **recreated from the GitHub Actions secret on every deploy**. So the durable update is
in GitHub:

1. Repo → **Settings → Secrets and variables → Actions** → update secret **`OPENAI_API_KEY`**.
2. Run the **"Deploy to sccc (Alibaba Cloud Riyadh)"** workflow (or merge this PR — pushes to
   `main` trigger it). The deploy rewrites `oaktree-db-env` and rolls the pods.

To rotate immediately without waiting for a deploy (will be re-applied identically next deploy as
long as step 1 is done first):

```bash
kubectl create secret generic oaktree-db-env \
  --from-literal=POSTGRES_USER=... --from-literal=POSTGRES_PASSWORD=... \
  --from-literal=POSTGRES_DB=... --from-literal=POSTGRES_HOST=... \
  --from-literal=POSTGRES_PORT=... --from-literal=PGSSLMODE=require \
  --from-literal=OPENAI_API_KEY="<new key>" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/oaktree-estimator
```

Note: `kubectl create secret` replaces the **whole** Secret, so all keys must be supplied
together — omitting the `POSTGRES_*` literals would break the database connection.

---

## 5. Risk & rollout notes

- **Risk: low.** Pure config/deploy change; explicit manifest env overrides secrets, behavior is
  deterministic. The backend flag gates new endpoints/fields only; the frontend flag gates UI
  rendering (off ⇒ payload byte-identical to today per locked decision L6).
- **Rollback:** revert the commit and redeploy; or set the manifest values back to `"false"`.
- **Note on branch name:** the request asked for `claude/enable-brief-extraction`; this session
  is locked to pushing `claude/enable-brief-extraction-c4hwwg`, so the commit lives there.
- Per the hard stop, **no PR was created** — section 3 + 4 above are the ready-to-paste PR
  description content.
