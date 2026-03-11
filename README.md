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

### Ingest shapefiles (external layers)
Zip the `.shp/.shx/.dbf/.prj` components together, then upload them via the ingest API:

```bash
curl -F "file=@/path/to/rydpolygons.zip" "http://127.0.0.1:8000/v1/ingest/shapefile?layer=rydpolygons"
curl -F "file=@/path/to/rydpoints.zip"   "http://127.0.0.1:8000/v1/ingest/shapefile?layer=rydpoints"
```

Each feature is stored in the `external_feature` table with GeoJSON geometry and original properties so the UI can surface them immediately.

### Ingest FAR rules (CSV/Excel)
If you maintain a tabular **district-level** FAR list (no geometry), load it via:

```bash
curl -F "file=@/path/to/far_rules_riyadh_v1.csv" "http://127.0.0.1:8000/v1/ingest/far_rules?city_default=Riyadh"
```

**CSV columns**
```
district,far_max,city,zoning,road_class,frontage_min_m,asof_date,source_url
```
Only `district` and `far_max` are required. When an estimate runs, the API first tries polygon features for FAR; if none are found, it falls back to this rules table by matching the inferred **district**.

### ArcGIS parcels (default outlines + identify)
ArcGIS parcels (`public.riyadh_parcels_arcgis_raw`) are the default geometry source via the proxy view
`public.riyadh_parcels_arcgis_proxy`. Ensure the migration that creates the view and GiST index has run, or run:
`alembic upgrade head` before using the endpoints.

Default settings (override via env vars as needed):
- `PARCEL_TILE_TABLE=public.riyadh_parcels_arcgis_proxy`
- `PARCEL_IDENTIFY_TABLE=public.riyadh_parcels_arcgis_proxy`
- `PARCEL_IDENTIFY_GEOM_COLUMN=geom`
- `PARCEL_TARGET_SRID=4326`

Tiles from the ArcGIS proxy are served at all zoom levels with zoom-based simplification and minimum-area
filters to keep low-zoom outlines readable.

### Suhail parcel tiles import (resumable)
- Workflow: trigger `.github/workflows/suhail-parcels-import.yml` (dispatch inputs: `zoom`, `layer`, `force_resume_from`, `max_tiles`). The job runs Alembic, ensures PostGIS, and resumes via `suhail_tile_ingest_state`.
- Local check: `python -m app.ingest.suhail_parcels_tiles --zoom 15 --layer parcels-base --max-tiles 2`.
- Parcel identify: set `PARCEL_IDENTIFY_TABLE=suhail_parcels_proxy` and `PARCEL_IDENTIFY_GEOM_COLUMN=geom` to route lookups through the Suhail proxy view.

### Inferred parcels (optional outlines + identify)
Inferred parcels are computed from building footprints (`public.inferred_parcels_v1`) and can be enabled for parcel
outlines and identify by setting `PARCEL_TILE_TABLE=public.inferred_parcels_v1`,
`PARCEL_IDENTIFY_TABLE=public.inferred_parcels_v1`, and `PARCEL_IDENTIFY_GEOM_COLUMN=geom`.

**Smoke check (local)**
```bash
curl -fsS "http://127.0.0.1:8000/v1/tiles/parcels/15/20634/14062.pbf" -o /tmp/parcels.pbf
ls -lh /tmp/parcels.pbf
curl -fsS "http://127.0.0.1:8000/v1/geo/identify?lng=46.675&lat=24.713&tol_m=25"
```
Confirm the tile output is non-empty when `public.riyadh_parcels_arcgis_proxy` has rows and that the identify response includes a `parcel_id` from the ArcGIS proxy view.

### Microsoft GlobalML Building Footprints (Saudi Arabia)
- Download the Saudi Arabia `.csv.gz` files from the `dataset-links.csv` manifest in `microsoft/GlobalMLBuildingFootprints` (filter the CSV for `Saudi Arabia`).
- Microsoft distributes building footprints as `.csv.gz`; each row/line includes a geometry (often GeoJSON), and some variants may be JSONL. The ingester auto-detects JSONL vs CSV and loads both.

```bash
MS_BUILDINGS_DIR=/path/to/saudi-arabia/files make ingest-ms-buildings
```

### Microsoft GlobalML Building Footprints (Riyadh-only)
Use the `dataset-links.csv` manifest to fetch only the Riyadh tiles, avoiding the blocked blob URLs and STAC `abfs://` links.

```bash
make fetch-ms-buildings-riyadh-links
make ingest-ms-buildings
```

The fetch step writes Riyadh-only `.csv.gz` JSONL files into `data/ms_buildings/`, which the existing ingest pipeline loads into `public.ms_buildings_raw`.

### Endpoints (MVP)

- `GET /health`
- `GET /v1/indices/cci`
- `GET /v1/indices/rates`
- `GET /v1/comps`
- `POST /v1/geo/building-metrics` (coverage, floors proxy stats, BUA from Overture buildings)
- `POST /v1/estimates` (uses Overture-built FAR defaults + Excel-style outputs)
  - Mixed-use (`m`) inputs: the API assumes **3.5 above-ground floors** and applies **Option B**
    (scales above-ground `area_ratio` by `3.5 / baseline_floors`) so BUA/FAR reflect that.
  - Land pricing defaults to **blended_v1** (Suhail anchor + Kaggle Aqar median, district-resolved once via `resolve_district`), shared with `GET /v1/pricing/land`.

### Expansion Advisor API (v6)

Endpoints:
- `POST /v1/expansion-advisor/searches`
- `GET /v1/expansion-advisor/searches/{search_id}`
- `GET /v1/expansion-advisor/searches/{search_id}/candidates`
- `POST /v1/expansion-advisor/candidates/compare`
- `GET /v1/expansion-advisor/candidates/{candidate_id}/memo`
- `GET /v1/expansion-advisor/searches/{search_id}/report`
- `POST /v1/expansion-advisor/saved-searches`
- `GET /v1/expansion-advisor/saved-searches`
- `GET /v1/expansion-advisor/saved-searches/{saved_id}`
- `PATCH /v1/expansion-advisor/saved-searches/{saved_id}`
- `DELETE /v1/expansion-advisor/saved-searches/{saved_id}`

`POST /v1/expansion-advisor/searches` request fields:
- `brand_name` (string)
- `category` (string)
- `service_model` (`qsr` | `dine_in` | `delivery_first` | `cafe`)
- `min_area_m2`, `max_area_m2`, `target_area_m2` (numbers)
- `target_districts` (string array; enforced if provided)
- `existing_branches` (array of `{name?, lat, lon, district?}`)
- `comparison_candidate_ids` (optional string array for client workflows)
- `bbox` (`min_lon`, `min_lat`, `max_lon`, `max_lat`)
- `limit` (1..100)
- `brand_profile` (optional object) with fields: `price_tier`, `average_check_sar`, `primary_channel`, `parking_sensitivity`, `frontage_sensitivity`, `visibility_sensitivity`, `target_customer`, `expansion_goal`, `cannibalization_tolerance_m`, `preferred_districts`, `excluded_districts`

Search/candidate responses now include (v6 adds on top of v5):
- `existing_branches` in search payloads
- candidate `district`, `cannibalization_score`, `distance_to_nearest_branch_m`
- candidate economics fields: `estimated_rent_sar_m2_year`, `estimated_annual_rent_sar`, `estimated_fitout_cost_sar`, `estimated_revenue_index`, `economics_score`, `estimated_payback_months`, `payback_band`
- candidate decision memo fields: `decision_summary`, `key_strengths_json`, `key_risks_json`
- candidate `compare_rank` (stored ranked order)
- candidate `brand_fit_score` and provider intelligence fields: `provider_density_score`, `provider_whitespace_score`, `multi_platform_presence_score`, `delivery_competition_score`
- **new v6 decision layer fields**: `zoning_fit_score`, `frontage_score`, `access_score`, `parking_score`, `access_visibility_score`, `gate_reasons_json`, `feature_snapshot_json`
- `feature_snapshot_json` includes deterministic parcel/market context (parcel area/perimeter, district and land use, nearest major road distance, nearby road count, road touch signal, nearby parking amenities when available, provider counts/platform breadth, competitor count, nearest branch distance, and rent/economics context)

`POST /v1/expansion-advisor/candidates/compare` request:
- `search_id` (string)
- `candidate_ids` (2..6 candidate IDs from the same search)

Compare response:
- `items` in the same order as requested `candidate_ids`
- per-item score breakdown + pros/cons + economics fields
- v6 compare fields include zoning/frontage/access/parking/access-visibility, gate pass/fail, gate reasons, confidence grade, and feature snapshot
- `summary` with best-overall, lowest-cannibalization, highest-demand, best-fit, best-economics, lowest-rent-burden, fastest-payback, best-brand-fit, strongest-delivery-market, and strongest-whitespace candidate IDs

`GET /v1/expansion-advisor/candidates/{candidate_id}/memo` returns a deterministic decision memo with:
- candidate economics + scoring breakdown
- gate checklist, gate reasons, and feature snapshot
- comparable competitors plus demand/cost thesis
- deterministic recommendation verdict (`go` | `consider` | `caution`)
- a headline, best-use-case branch format, and primary watchout
- deterministic `market_research` summaries for delivery-market context, competitive context, and district fit

`GET /v1/expansion-advisor/searches/{search_id}/report` returns a deterministic executive-style JSON recommendation with `top_candidates` (top 3), `recommendation` (best, runner-up, best pass, best confidence, why, risk, best format, summary), and explicit assumptions. Top candidates include confidence + gate verdict + mini feature snapshot.


Saved studies payload fields:
- `search_id`, `title`, `description?`, `status` (`draft` | `final`)
- `selected_candidate_ids` (shortlist/favorites/compare set)
- `filters_json` (brief form restore payload)
- `ui_state_json` (lightweight map/list UI state restore)

Saved study detail response includes linked `search` and `candidates` for frontend hydration.

Notes:
- Expansion Advisor is now the primary user-facing workflow and replaces Restaurant Finder in navigation.
- Restaurant Finder remains available only as a legacy/internal market-intelligence flow.
- ArcGIS Riyadh parcels are the only supported parcel source.
- Suhail and inferred parcels are intentionally excluded.
- Cannibalization scoring is deterministic and distance-based, adjusted by service model.
- Hard gates include zoning fit, area fit, frontage/access, parking, district policy, cannibalization, delivery-market, economics, plus overall pass.
- Access/frontage/parking scoring is deterministic and explainable from ArcGIS parcels + OSM road/parking context.
- Revenue index, provider intelligence, and payback are heuristic decision-support signals (not guaranteed revenue forecasts).

Payback band meanings:
- `strong`: <= 18 months
- `promising`: > 18 and <= 28 months
- `borderline`: > 28 and <= 40 months
- `weak`: > 40 months

### Rent benchmarks (Excel mode)

The Excel pathway blends data sources for rent: if REGA city-level rents exist and Kaggle Aqar district medians are available, the API scales REGA by the district/city ratio from Aqar. When only Aqar rents exist, it falls back to the district median; otherwise the REGA city benchmark (or manual/template rents) is used. Load Kaggle-derived rent comps via `app/ingest/aqar_rent_comps.py` to enable the blend.

## Frontend dev

Start the API by following the Quick start above (or `make db-up && make db-init && make api`). Then run the Vite dev server:

```bash
cd frontend
npm install
cp .env.development.example .env.development
# For Codespaces: open the forwarded 8000 port link and paste that as VITE_API_BASE_URL.
npm run dev
```

Note: the UI now uses the live Esri basemap only; offline static tiles have been deprecated.

If you're using Codespaces, the FastAPI URL will look like:

```
https://<your-codespace>-8000.app.github.dev
```

### Using the UI against staging (ACK)
If the API is deployed on sccc/ACK, set:

```
VITE_API_BASE_URL=https://<your-loadbalancer-dns-or-ip>
```

### Frontend translations
The React UI uses i18next via `frontend/src/i18n/` (`index.ts`, `en.json`, `ar.json`). Add new keys to both JSON files and reference them with `t(\"...\")` from `react-i18next`. The active locale is persisted in `localStorage` under `oaktree_locale`.

## Deploy (sccc by stc / Alibaba Cloud Riyadh, me-central-1)

1. In sccc by stc (Alibaba Cloud Riyadh), provision an ACK cluster in `me-central-1` and an **Enterprise ACR instance** (the registry should expose a domain such as `oaktree-ai-estimator-registry.me-central-1.cr.aliyuncs.com`).
2. Until GitHub OIDC is enabled in the region, the workflow authenticates with AK/SK credentials. In GitHub → **Settings → Actions** configure:
   - **Variables**: `ALIBABA_REGION=me-central-1`, `ACR_NAMESPACE`, `SERVICE_NAME`, `ACK_CLUSTER_ID`, `ACR_LOGIN_SERVER=<enterprise-acr-domain>`
   - **Secrets**: `ALIBABA_CLOUD_ACR_INSTANCE_ID`, `ALIBABA_ACCESS_KEY_ID`, `ALIBABA_ACCESS_KEY_SECRET`
3. Pushes to `main` trigger `.github/workflows/deploy-sccc.yml`. The workflow builds the Docker image, pushes it to the Enterprise ACR domain specified in `ACR_LOGIN_SERVER`, validates the Kubernetes manifests with `kubectl apply --dry-run=client -f k8s/`, then applies the manifests in `k8s/` to update the ACK deployment. (If you edit the manifests manually, keep `spec.template.spec.containers` as a YAML list — each container needs its own leading hyphen.)
4. (Preferred, once available) Switch back to GitHub OIDC by providing `ALIBABA_CLOUD_RAM_ROLE_ARN` and `ALIBABA_CLOUD_RAM_OIDC_ARN` secrets and removing the AK/SK credentials.

No secrets are committed. For production, switch the database to HA and add RBAC/SSO per the roadmap.  [oai_citation:5‡AI App Blueprint .docx](file-service://file-ALgZg1S1QWVEsFVxeedqkv)

## Parking minimums (Riyadh)

The API now enforces **minimum parking requirements for Riyadh** using the municipal guide
(see `GET /v1/metadata/parking-rules` for the exact ruleset and source URL).

### How it works
- **Required spaces** are computed from the project program:
  - Residential: uses `unit_mix` (1 space/unit if <180 m², 2 spaces/unit if ≥180 m²).
  - Retail: 1 space per 45 m² GFA.
  - Office: 1 space per 40 m² GFA.
- **Provided spaces** are derived from below‑grade + explicit parking area using a gross
  “m² per stall” conversion.
- If there is a deficit and `parking_minimum_policy="auto_add_basement"` (default), the engine
  **automatically increases `area_ratio.basement`** to eliminate the deficit (basement is excluded
  from FAR scaling).

### Inputs (excel_inputs)
You can control the behavior with these optional keys:
- `parking_apply` (bool, default `true`)
- `parking_minimum_policy` (`"auto_add_basement"` | `"flag_only"` | `"disabled"`)
- `parking_supply_gross_m2_per_space` (float, default `30`)
- `parking_supply_layout_efficiency` (float, default `55`)
  - If `<= 1.0`: treated as a **layout efficiency fraction** (e.g. 0.85)
    and spaces are computed as: `parking_area * efficiency / gross_m2_per_space`
  - If `> 1.0`: treated as an **effective gross m² per space** (e.g. 55),
    and spaces are computed as: `parking_area / effective_gross_m2_per_space`
  - The default `55` is calibrated to real Riyadh basement layouts (~34–36 spaces per ~1,980 m²)
- `parking_assumed_avg_apartment_m2` (float, default `120`) – only used if `unit_mix` is missing/empty.

### Outputs
Parking fields are available in:
- `totals.*` (high-level) and
- `notes.excel_breakdown.*` and `notes.parking.*` (detailed).

Key fields:
- `parking_required_spaces`
- `parking_provided_spaces`
- `parking_deficit_spaces`
- `parking_compliant`
