# OSM Data Freshness — Derived/Cached Layer Audit

**Investigation date:** 2026-05-31 · **OSM import completed:** 2026-06-01 00:46 · **Mode:** read-only, no files changed.

## TL;DR

The OSM import (`osm-import.yml`) refreshes only **two** things: the base tables `planet_osm_*` (via osm2pgsql) and `osm_parcels_proxy` (rebuilt in-workflow at `.github/workflows/osm-import.yml:285-287`). **Every OSM-derived cache is on its own separate schedule and does NOT re-derive when OSM imports.** Live-read paths (parcel identify, tiles, parcel search against the proxy) reflect the new data immediately; derived layers are stale until their own job runs.

---

## 1. Parking derivation (`app/ingest/expansion_advisor_parking.py`)

**Sources read:** `planet_osm_polygon` (`expansion_advisor_parking.py:107`, `:177`) and `planet_osm_point` (`:196`, `:262`). Google Places is a *separate* module (`expansion_advisor_parking_google.py`), not this one.

**Output written:** table `expansion_parking_asset` — polygon INSERT at `expansion_advisor_parking.py:132-184` (`INSERT INTO expansion_parking_asset (city, source, name, amenity_type, geom, capacity, covered, public_access, walk_access_score, dropoff_score)`), point INSERT at `:233-269`. In `--replace` mode it deletes only its own source rows first: `DELETE FROM expansion_parking_asset WHERE city = 'riyadh' AND source = 'osm_polygon'` (`:117`) / `'osm_point'` (`:206`). Table DDL: `alembic/versions/d4e5f6a1b2c3_create_expansion_advisor_tables.py:47-60`, timestamp column `created_at TIMESTAMPTZ NOT NULL DEFAULT now()` (`:59`). Note: `created_at` defaults to insert time, so it tracks the **last parking ingest**, not the OSM import.

**Trigger:** its own workflow `.github/workflows/expansion-advisor-data-parking.yml`, which runs `python -m app.ingest.expansion_advisor_parking` (`:48`). Schedule: `cron: "0 4 * * 2"` — **weekly, Tuesday 04:00 UTC** (`:5`), plus `workflow_dispatch`. It is **not** a step inside `osm-import.yml` (that file never invokes the parking module).

> **Plainly:** After an OSM refresh, parking does **NOT** re-derive automatically. `expansion_parking_asset` stays stale until the separate weekly Tuesday job (or a manual dispatch) runs. The OSM import finished 2026-06-01 (a Monday-night/Tuesday-UTC boundary); parking will only catch up on its own Tuesday cron.

---

## 2. Search-index materialized view (`alembic/versions/20260202_search_index_mat.py`)

**View name & definition:** `public.search_index_mat` (`CREATE MATERIALIZED VIEW public.search_index_mat AS …`, `20260202_search_index_mat.py:30`). It is a `UNION ALL` of: POIs from `planet_osm_point` (`:71`), POIs from `planet_osm_polygon` (`:119`), roads from `planet_osm_line` (`:148`), and districts from `external_feature` (`:179`) — so it caches OSM-derived rows directly. It bundles indexes plus a convenience refresh function `public.refresh_search_index_mat()` that runs `REFRESH MATERIALIZED VIEW CONCURRENTLY public.search_index_mat` (`:237-241`).

**Every `REFRESH MATERIALIZED VIEW` call site in the repo (full grep):**
- `alembic/versions/20260202_search_index_mat.py:239` — inside the `refresh_search_index_mat()` function body (definition only; nothing calls it automatically).
- `app/services/suhail_parcels.py:12` & `:20` — refreshes `suhail_parcels_mat` (a *different*, Suhail-parcel view, not OSM-derived).
- `app/services/external_feature_refresh.py:34` — refreshes `external_feature_polygons_mat` (different view).
- `app/ingest/expansion_advisor_refresh.py:57` — a **comment/placeholder only** (`# Future: REFRESH MATERIALIZED VIEW CONCURRENTLY expansion_*_mv;`); the function logs "No materialized views to refresh" (`:58`).
- `alembic/versions/20260501a_external_feature_polygons_mat.py:48` — comment about a different view.

There is **no caller of `refresh_search_index_mat()` or any `REFRESH … search_index_mat` anywhere** in `app/`, `.github/workflows/`, or `sql/`. The only documented way to refresh it is manual, confirmed by `docs/osm_districts_removal.md:64-72`: *"There is no auto-refresh path. Trigger it once after deploy: `SELECT public.refresh_search_index_mat();`"*

**Read-paths:** `app/api/search.py` *prefers* the matview when it exists — `if _table_exists(db, "public.search_index_mat")` (`search.py:1328`) → query `FROM public.search_index_mat` (`search.py:930`, built in `_build_search_index_sql` at `:895`). Only when the matview is absent does it fall back to querying `planet_osm_point/polygon/line` directly (`search.py:1360`, `:1377`, `:1394`).

> **Plainly:** Nothing refreshes `search_index_mat` after an OSM import — it is **manual only**. Because parcel/POI **search autocomplete preferentially reads the matview**, search results will keep showing the *old* OSM POIs/roads until someone runs `SELECT public.refresh_search_index_mat();`. (This is the one stale layer that directly degrades the user-facing search the task cares about.)

---

## 3. Expansion candidate freshness (`expansion_candidate`)

Rows are produced **per-search** by the Expansion Advisor pipeline, written via `INSERT INTO expansion_candidate (…)` in `app/services/expansion_advisor.py:9564` (inside the search-run persistence path; each row carries `search_id` FK to `expansion_search`). The OSM import never touches this table (no reference to `expansion_candidate` in `osm-import.yml` or any ingest job).

**Freshness timestamp:** `computed_at TIMESTAMPTZ NOT NULL DEFAULT now()` — DDL at `alembic/versions/20260310_exp_adv_v0.py:71-76`; it is selected back and used for ordering in the read query at `app/services/expansion_advisor.py:10001` and `:10028` (`ORDER BY … computed_at DESC`). Confirmed: the column is `computed_at` as expected.

> **Plainly:** Existing `expansion_candidate` rows were scored against the OSM data that existed at *their* `computed_at`. They are **not** recomputed by the OSM import. Only running a **fresh Expansion Advisor search** recomputes candidates against the new OSM data (and, transitively, only against whichever derived inputs — parking/roads — have themselves been refreshed).

---

## 4. Other OSM-derived caches (derive-and-store, can go stale)

Found by grepping all `planet_osm_*` / `osm_parcels_proxy` consumers and filtering to ones that **write** OSM-derived values into another table:

| Derived layer | Source → Output | File:line | Trigger | Auto-refresh after OSM import? |
|---|---|---|---|---|
| **Parking (OSM)** | `planet_osm_polygon`/`_point` → `expansion_parking_asset` | `expansion_advisor_parking.py:132,233` | Workflow `expansion-advisor-data-parking.yml:48`, cron `0 4 * * 2` (weekly Tue) | **No** — separate weekly job |
| **Roads / access** | `planet_osm_line` (fallback `planet_osm_roads`/`osm_roads`) → `expansion_road_context` | `expansion_advisor_roads.py:61,92`; DDL `d4e5f6a1b2c3_…:21-38` (`created_at` default now) | Workflow `expansion-advisor-data-roads.yml:116`, cron `0 3 * * 1` (weekly Mon) | **No** — separate weekly job |
| **Search matview** | `planet_osm_point/polygon/line` (+`external_feature`) → `search_index_mat` | `20260202_search_index_mat.py:30` | Manual only (`refresh_search_index_mat()`); no caller | **No** — manual |
| **Inferred parcels v1** | `planet_osm_polygon` buildings / `planet_osm_line` fallback → `inferred_parcels_v1` | `inferred_parcels_v1.py:56,426` | Workflow `inferred-parcels-refresh.yml:140`, **`workflow_dispatch` only** (no cron) | **No** — on-demand only |
| **Expansion candidates** | derived per-search (consumes parking/roads/etc.) → `expansion_candidate` | `expansion_advisor.py:9564` | Per user search at runtime | **No** — only on a fresh search |
| **Parking (Google)** | Google Places API (not OSM) → `expansion_parking_asset` (`source='google_places'`) | `expansion_advisor_parking_google.py:68` | Workflow `expansion-advisor-data-parking-google.yml:49`, cron `0 4 1 1,7 *` (Jan 1 & Jul 1) | N/A — not OSM-derived; listed for completeness since it shares the parking table |

**Notes / non-issues:**
- `candidate_location` (`candidate-locations-refresh.yml`) is built from Aqar/Bayut scrapers, **not** from `planet_osm_*` — not an OSM cache.
- `riyadh_urban_parcels_raw` (`riyadh_urban_parcels.py:20`) loads from a bundled zip dataset, not OSM.
- `external_feature_polygons_mat` and `suhail_parcels_mat` are non-OSM views; refreshed by their own paths.
- `osm_parcels_proxy` **is** rebuilt inside `osm-import.yml:285-287` (`psql -f sql/rebuild_osm_parcels_proxy.sql`), so it's the one OSM-derived layer that *does* refresh in-workflow — treat it as fresh.

---

## What I need to do to make the app fully feel the new OSM data

> Do NOT run any of this automatically — checklist for the operator:

1. **Refresh the search matview (highest priority — directly affects search autocomplete):**
   `SELECT public.refresh_search_index_mat();`
2. **Re-run parking derivation** — dispatch the `Expansion Advisor Data — Parking Context` workflow (or `python -m app.ingest.expansion_advisor_parking --replace true`).
3. **Re-run roads derivation** — dispatch `Expansion Advisor Data — Roads & Access` (or `python -m app.ingest.expansion_advisor_roads`).
4. **(Optional) Rebuild inferred parcels** — dispatch `Refresh inferred parcels v1` if you rely on `inferred_parcels_v1`.
5. **Run a fresh Expansion Advisor search** — existing `expansion_candidate` rows won't update; only a new search recomputes them (and only against inputs you refreshed in steps 2-4).
6. `osm_parcels_proxy` and live identify/tiles/proxy-search need nothing — already current as of the import.

---

## DB-state confirmation commands (run in codespace — verbatim, not executed here)

Compare each against the OSM import timestamp **2026-06-01 00:46**. These use the real table/column names confirmed above.

**OSM base + proxy (should be fresh):**
```
psql -XtAc "SELECT 'planet_osm_polygon' AS t, count(*) FROM planet_osm_polygon;"
psql -XtAc "SELECT 'osm_parcels_proxy' AS t, count(*) FROM osm_parcels_proxy;"
psql -XtAc "SELECT id,last_tile,updated_at FROM osm_import_state WHERE id=1;"
```

**Parking output (created_at = last parking ingest; expect < OSM import):**
```
psql -XtAc "SELECT source, count(*), max(created_at) FROM expansion_parking_asset GROUP BY source ORDER BY source;"
```

**Roads output:**
```
psql -XtAc "SELECT count(*), max(created_at) FROM expansion_road_context WHERE city='riyadh';"
```

**Search matview (no built-in timestamp — compare row count drift; if it lags planet_osm it's stale):**
```
psql -XtAc "SELECT count(*) FROM public.search_index_mat;"
psql -XtAc "SELECT count(*) FROM public.search_index_mat WHERE source IN ('osm_point','osm_polygon','osm_line');"
```

**Inferred parcels (if used):**
```
psql -XtAc "SELECT count(*), max(updated_at) FROM public.inferred_parcels_v1;"
```

**Expansion candidates (latest scoring time across all searches):**
```
psql -XtAc "SELECT count(*), max(computed_at) FROM expansion_candidate;"
```

**Interpretation:** any of `expansion_parking_asset.max(created_at)`, `expansion_road_context.max(created_at)`, or `expansion_candidate.max(computed_at)` earlier than **2026-06-01 00:46** = that layer is stale w.r.t. the new OSM data and needs its step from the checklist above. The matview has no timestamp, so treat it as stale by default until you've run `refresh_search_index_mat()`.
