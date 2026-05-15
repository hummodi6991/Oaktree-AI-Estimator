# Expansion Advisor — End-to-End Technical Overview

> Scope: This document explains how the **Expansion Advisor** product surface
> currently works, from the user's brief through candidate generation,
> scoring, gating, decision memos, and the frontend experience. It is written
> against the current repo state and **explicitly flags inactive,
> feature-flagged, or placeholder code** where it exists.
>
> Context: Oaktree Atlas is a Riyadh-first geospatial real-estate platform.
> The Expansion Advisor is the restaurant/retail location-intelligence
> surface (the other surface being the Development Feasibility / Estimator).

---

## 1. What the Expansion Advisor does

The Expansion Advisor helps a restaurant or retail brand decide **where to open
its next location in Riyadh**. The user supplies a *brand brief* (brand name,
category, service model, target size, target districts, existing branches, and
optional brand-profile preferences). The system then:

1. Generates a pool of real candidate sites (parcels and live commercial-unit
   listings).
2. Scores each candidate across nine analytical components.
3. Runs each candidate through a set of pass/fail/unknown **gates**.
4. Applies a **market-viability pass** (hard floors + soft demotions).
5. Ranks, deduplicates, and persists the candidates.
6. Produces per-candidate **decision memos**, comparison views, and an
   executive **recommendation report**.

Everything is Riyadh-scoped. Spatial work is done in EPSG:4326 with metric
computations via EPSG:32638 transforms. Backend endpoints live under
`/v1/expansion-advisor/*`.

---

## 2. System architecture at a glance

```
            ┌──────────────────────────────────────────────────────┐
  Brief ───▶│ POST /v1/expansion-advisor/searches                   │
            │   app/api/expansion_advisor.py                        │
            │            │                                          │
            │            ▼                                          │
            │   run_expansion_search()                              │
            │   app/services/expansion_advisor.py (~10.6k lines)    │
            │     1. candidate pool query (parcels + listings)      │
            │     2. spatial context bulk-fetch (roads/parking/pop) │
            │     3. nine scoring components                        │
            │     4. gate evaluation (tri-state)                    │
            │     5. market-viability pass (hard floors + demotes)  │
            │     6. (optional) LLM rerank                          │
            │     7. dedupe + persist to expansion_candidate        │
            │     8. decision summaries / theses                    │
            └──────────────────────────────────────────────────────┘
                         │                         │
                         ▼                         ▼
            PostgreSQL 15 + PostGIS         Decision memos (LLM)
            expansion_* tables              prewarmed in background
                         │
                         ▼
            React + MapLibre frontend
            frontend/src/features/expansion-advisor/
```

### Tech stack
- **Backend:** Python 3.11, FastAPI, SQLAlchemy, Alembic, PostGIS
- **Frontend:** React 18, TypeScript, Vite, MapLibre GL
- **Database:** PostgreSQL 15 + PostGIS
- **Data:** Aqar listings, ArcGIS parcel proxy, delivery-platform scrapes,
  OSM-derived roads/parking, restaurant POIs

---

## 3. Data model

All Expansion Advisor tables were introduced through migrations
`alembic/versions/20260310_*` through `20260314_*`.

| Table | Created by | Purpose |
|-------|-----------|---------|
| `expansion_search` | `20260310_exp_adv_v0` | One row per search; holds the brief (`request_json`), `notes`, area bounds, bbox. |
| `expansion_candidate` | `20260310_exp_adv_v0` | One row per scored candidate site; extended by every later migration. |
| `expansion_branch` | `20260310_exp_adv_v1_branches_compare` | Existing branches supplied by the brand (for cannibalization). |
| `expansion_saved_search` | `20260311_exp_adv_saved_v1` | Persisted "studies" (title, status draft/final, shortlist, UI state). |
| `expansion_brand_profile` | `20260311_exp_adv_brand_v4` | Brand-profile inputs (price tier, channel, sensitivities, goal, preferred/excluded districts). |

### `expansion_candidate` column growth (by migration)
- **v0** — core scores: `demand/whitespace/fit/confidence/final_score`,
  `population_reach`, `competitor_count`, `delivery_listing_count`.
- **v1** — `district`, `cannibalization_score`,
  `distance_to_nearest_branch_m`, `compare_rank`.
- **v2 (econ)** — rent/fitout/revenue economics, `economics_score`,
  `payback_*`, `decision_summary`, `key_risks_json`, `key_strengths_json`.
- **brand_v4** — `brand_fit_score`, `provider_density_score`,
  `provider_whitespace_score`, `multi_platform_presence_score`,
  `delivery_competition_score`.
- **v5 (decision)** — `gate_status_json`, `confidence_grade`,
  `demand_thesis`, `cost_thesis`, `comparable_competitors_json`.
- **v6 (features)** — `zoning_fit_score`, `frontage_score`, `access_score`,
  `parking_score`, `access_visibility_score`, `gate_reasons_json`,
  `feature_snapshot_json`.
- **v61 (outputs)** — `score_breakdown_json`, `top_positives_json`,
  `top_risks_json`, `rank_position`.

The migrations are additive and reviewable; no destructive schema changes.

---

## 4. Ingestion pipeline

The Expansion Advisor scores against pre-normalized tables that are populated
by dedicated ingestion jobs under `app/ingest/expansion_advisor_*`. These run
out-of-band (scheduled / manual), not during a search.

| Job | Output table | Source(s) |
|-----|--------------|-----------|
| `expansion_advisor_delivery.py` | `expansion_delivery_market` | Delivery-platform scrapes (HungerStation, Jahez, Keeta, Talabat, Mrsool) via `SCRAPER_REGISTRY`. |
| `expansion_advisor_rent_comps.py` | `expansion_rent_comp` | Aqar rent listings rolled up to district percentiles (25/50/75th). |
| `expansion_advisor_competitors.py` | `expansion_competitor_quality` | `restaurant_poi` cleaned/deduped, with `chain_strength_score`. |
| `expansion_advisor_parking.py` | `expansion_parking_asset` | `planet_osm_polygon` parking amenities (+ optional commercial-unit parking). |
| `expansion_advisor_roads.py` | `expansion_road_context` | `planet_osm_line` highways, classified major/minor with distances. |
| `expansion_advisor_district_labels.py` | enriches `riyadh_parcels_arcgis_proxy` | Spatial join of parcels to `aqar_district_hulls`. |
| `expansion_advisor_brand_aliases.py` | `expansion_brand_alias` | Arabic↔English brand-name variants for cross-platform dedupe. |
| `expansion_advisor_refresh.py` | refreshes `external_feature_polygons_mat` | Idempotent materialized-view refresh (district momentum). |

`expansion_advisor_common.py` holds shared helpers (`RIYADH_BBOX`, table-exists
checks, count logging). The delivery job fails loudly on zero useful rows
unless `ALLOW_EMPTY_DELIVERY_INGEST=true`.

> See `docs/expansion_advisor_data_ingest.md` for ingestion details.

---

## 5. API surface

All routes are registered under `/v1/expansion-advisor/*` in
`app/api/expansion_advisor.py`.

| Method & path | Purpose |
|---------------|---------|
| `POST /searches` | Run a search from a brief; returns candidates, `notes`, `meta`. Triggers background memo prewarm. |
| `GET /searches/{search_id}` | Full search definition (`request_json`, brand profile, branches, meta). |
| `GET /searches/{search_id}/candidates` | Candidate list with all scoring metadata. |
| `GET /searches/{search_id}/report` | Executive recommendation report (top-3 + dimension winners). |
| `GET /districts` | Deduplicated Riyadh districts (AR/EN labels + aliases) for pickers. |
| `GET /branch-suggestions?q=&limit=` | Autocomplete for existing branches (from `restaurant_poi` + delivery records). |
| `POST /candidates/compare` | Compare 2–6 candidates; returns per-dimension winners. |
| `GET /candidates/{candidate_id}/memo` | Detailed decision memo for one candidate. |
| `POST /decision-memo` | Generate / regenerate an LLM decision memo. |
| `POST /saved-searches` | Save a study. |
| `GET /saved-searches?status=&limit=` | List saved studies (graceful empty list if table absent). |
| `GET /saved-searches/{saved_id}` | Load a saved study. |
| `PATCH /saved-searches/{saved_id}` | Update title/description/status/shortlist/UI state. |
| `DELETE /saved-searches/{saved_id}` | Delete a saved study. |

Response models use a `FlexibleResponseModel` base (`extra="allow"`) for the
JSONB-heavy candidate payloads so the API stays forward-compatible as scoring
metadata evolves. JSON list endpoints follow the repo `{ "items": [...] }`
convention.

---

## 6. The scoring pipeline (`run_expansion_search`)

`run_expansion_search()` (`app/services/expansion_advisor.py`) is the
end-to-end engine. Stages:

### 6.1 Candidate pool generation
- Sources: `commercial_unit` (live Aqar listings) **and**
  `riyadh_parcels_arcgis_proxy` (parcel fallback layer).
- Stratified, fair-share sampling per district with 3× headroom; per-district
  caps (min 5, max 200) and a global pool limit of 2000.
- Filters: area within `[min_area, max_area]`, optional bbox, optional target
  districts, valid coordinates, and a centroid clip (`ST_DWithin`, default
  10 km).
- District names are normalized (`_resolve_district_to_ar_key`,
  `_canonicalize_district_label`) so English input matches Arabic SQL storage,
  with mojibake detection on garbled labels.

### 6.2 Spatial context bulk-fetch
Roads, parking, population, delivery listings, and competitors are fetched in
**bulk queries keyed by candidate** to avoid N+1 query patterns inside the
scoring loop. Normalized `expansion_*` tables are preferred; OSM tables
(`planet_osm_line`, `planet_osm_polygon`) are used as a fallback when the
normalized table is unavailable.

### 6.3 The nine scoring components

| Component | What it measures |
|-----------|-----------------|
| **Demand** | Population reach + delivery-market activity, blended with service-model-tuned weights. |
| **Fit** | Zoning fit + frontage + access + parking suitability. |
| **Whitespace** | Competitor density (log-scaled decay; sparse data resolves to neutral, not perfect). |
| **Chain strength** | Whether established brands validate the area (neutral 50 when no competitors). |
| **Economics** | Rent burden (percentile-based) blended with revenue potential and fit-out burden. |
| **Confidence** | Data-quality signal; listings can reach 100, parcels are capped at 70. |
| **Listing quality** | Freshness + suitability + image + furnished + **district momentum**. |
| **Brand fit** | Preferred/excluded districts, cannibalization tolerance, expansion goal, channel, price tier, sensitivities. |
| **Delivery-market signals** | Provider density, multi-platform presence, delivery competition, whitespace. |

Service model (`qsr` / `dine_in` / `delivery_first` / `cafe`) tunes reference
values and blend weights throughout — e.g. `delivery_first` weights delivery
activity 0.6 vs population 0.4, while `dine_in` weights population 0.75.

The combination produces a `final_score`, a structured `score_breakdown_json`
(weights, inputs, weighted components, display), and a `value_score` chip
(geometric mean of revenue index and rent-burden score) with a `value_band`
of `best_value` / `neutral` / `above_market`.

### 6.4 Gate evaluation (tri-state)

Each candidate is run through gates that return **pass / fail / unknown**:

`zoning_fit`, `area_fit`, `frontage_access`, `parking`, `district`,
`cannibalization`, `delivery_market`, `economics`, `radiance_growth`,
`population_floor`, `commercial_floor`, `construction_proximity`.

`overall_pass` is itself tri-state:
- **True** — all hard-fail gates pass.
- **False** — any hard-fail gate fails.
- **None** — a hard-fail gate is unknown (insufficient data) and none failed.

The hard-fail gate set is built **at module load** from environment
thresholds, so disabling a floor via env var disables it everywhere
consistently. `radiance_growth` is advisory-only — its absence must never
collapse the overall verdict.

### 6.5 Market-viability pass
- **Hard-floor pre-pass:** drops candidates failing population / commercial /
  construction floors; the dropped counts and thresholds are written to
  `notes.viability.hard_floors`.
- **Soft-demote legs:** three "pillars" demote (rather than drop) weak
  candidates by a fixed number of positions:
  - *Demand leg* — per-search demand cohort below the 25th percentile.
  - *RPC leg* — rent-per-check above the 75th percentile.
  - *Radiance leg* — nightlight YoY growth below threshold.
  - Telemetry is written to `notes.viability.demote_legs`.

### 6.6 Deduplication
- `_dedupe_candidates()` — collapses near-clones by `parcel_id` first (a
  non-empty parcel id is never collapsed), then by a tight spatial+attribute
  grid (≈55 m snap, district, area/rent/branch-distance buckets). Report
  shortlists use a more aggressive economic-similarity key for diversity.
- `_dedupe_score_clones()` — collapses candidates that are visually identical
  to a user (same district, area within 5%, score within 0.3, same rent rate).

### 6.7 Persistence and memos
Candidates are written to `expansion_candidate` with full scoring metadata.
Per-candidate decision artifacts are produced: `demand_thesis`, `cost_thesis`,
`top_positives_json`, `top_risks_json`, and `comparable_competitors_json`
(top-3 same-category POIs nearby).

---

## 7. Decision memos

Two memo paths exist:

1. **Structured memos** — preferred path; produces `decision_memo_json` with
   sections such as property overview, financial framing, market context, and
   competitive landscape.
2. **Legacy text memos** — fallback narrative.

`POST /decision-memo` accepts a candidate, brief, and language (`en`/`ar`) and
returns both shapes. After a search completes, top-N memos are **prewarmed**
in a background task (bounded by a wall-clock budget and a concurrency limit)
so they are ready when the user opens a candidate.

---

## 8. Frontend experience

The frontend lives in `frontend/src/features/expansion-advisor/` (~58 files).
`ExpansionAdvisorPage.tsx` is the orchestrator; `lib/api/expansionAdvisor.ts`
wraps all 14 endpoints with response normalization (Decimal-string coercion,
nested-field defaults).

### User flow
1. **Brand brief** — `ExpansionBriefForm` collects brand name, category,
   service model, area range, target districts, existing branches
   (`BranchLocationPicker`), and brand-profile preferences.
2. **Results** — `ExpansionResultsPanel` renders ranked
   `ExpansionCandidateCard`s; `SortFilterBar` filters by gate status and
   sorts by rank/score/rent/fit-out/revenue/distance.
3. **Map** — MapLibre (`Map.tsx` + `map/expansionOverlay.ts`) plots candidates
   and existing branches as GeoJSON; selection, shortlist, compare, and lead
   states are reflected on the map and synced both ways.
4. **Decision memo** — `ExpansionMemoPanel` drawer with Economics / Market /
   Site / Risks / Breakdown tabs, score breakdown (`DecisionLogicCard`), and a
   structured narrative.
5. **Compare** — `ExpansionComparePanel` does side-by-side comparison of 2–6
   shortlisted candidates with a per-dimension winner banner.
6. **Report** — `ExpansionReportPanel` shows the executive recommendation
   (best candidate, runners-up, best-on-each-criterion).
7. **Saved studies** — `SaveStudyDialog` / `SavedSearchesPanel` persist a
   study with shortlist and UI state (map center/zoom, open drawer).

State is held in `ExpansionAdvisorPage` (25+ `useState` hooks) with in-memory
`useRef` caches for memos and reports. User-facing strings use i18next with
English and Arabic locale files.

---

## 9. Inactive, feature-flagged, and placeholder pieces

> **This section explicitly lists code that is currently inactive, disabled,
> behind a feature flag, dev-only, or a deliberate placeholder.** It reflects
> the present repo state and should be re-checked if flags or defaults change.

### 9.1 Intentionally disabled

| Item | Location | State |
|------|----------|-------|
| **Decision-memo cache *reads*** | `app/api/expansion_advisor.py` `_decision_memo_cache_lookup()` (~line 1451) | **Inactive — always returns `None`.** Every `POST /decision-memo` and every prewarm call regenerates against the live LLM. Cache *writes* still run, purely so the `decision_memo_present` flag and later `GET /candidates/{id}/memo` reads work. This is deliberate (memo freshness), not a bug. |
| **LLM reranking (Phase 2)** | flag `EXPANSION_LLM_RERANK_ENABLED`, default `""` (false); used at `expansion_advisor.py` ~line 1059 | **Inactive by default.** When off, every candidate is tagged `rerank_status="flag_off"`, `final_rank == deterministic_rank`, and ordering is unchanged. The rerank code path, shortlist caps, and max-move bounds all exist but are dormant unless the flag is enabled. |

### 9.2 Feature-flagged but ON by default

These are active in the default configuration but can be turned off via env var:

| Flag | Default | Effect when disabled |
|------|---------|---------------------|
| `EXPANSION_MEMO_STRUCTURED_ENABLED` | `true` | Falls back to the legacy text-memo path. |
| `EXPANSION_VALUE_SCORE_ENABLED` | `true` | `value_score` / `value_band` left `None`. |
| `EXPANSION_REALIZED_DEMAND_ENABLED` | `true` | Delivery score reverts to listing-count-only (legacy). |
| `EXPANSION_MEMO_PREWARM_ENABLED` | `true` | No background memo generation after a search. |
| `EXPANSION_VIABILITY_DEMAND_LEG_ENABLED` | `true` | Soft demand-cohort demotion skipped. |
| `EXPANSION_VIABILITY_RADIANCE_GROWTH_LEG_ENABLED` | `true` | Soft radiance-growth demotion skipped. |

Hard-floor gates are also configurable and **disabled when set to 0**:
`EXPANSION_VIABILITY_POPULATION_HARD_FLOOR` (default 20000),
`EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR` (default 1),
`EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M` (default 75 m).

`_MOMENTUM_ENABLED` is hardcoded `True` in `expansion_advisor.py` (~line 2368);
if flipped to `False`, listing-quality scoring reverts to its pre-momentum
weights and ignores district momentum.

### 9.3 Dev-only / placeholder frontend code

| Item | Location | State |
|------|----------|-------|
| **Score-invariant validation** | `frontend/src/.../scoreInvariants.ts:37` (`if (!import.meta.env.DEV) return;`) | Runs in development only; **skipped entirely in production builds.** |
| **Design token preview** | `frontend/src/main.tsx:397` (`import.meta.env.DEV ? <DesignTokenPreview/> : null`) | Dev-only component; **not mounted in production.** |
| **Legacy UI branch** | `frontend/src/main.tsx:392–394`, flag `VITE_UI_V2` | The non-V2 UI is a **fallback only**, reachable via `?ui=legacy` or `VITE_UI_V2=0`. The default path is V2. |
| **`initialTab` on `ExpansionMemoPanel`** | `ExpansionMemoPanel.tsx:87–97` | Test-only affordance for SSR snapshots; production never passes it. |

### 9.4 Graceful-degradation paths (defensive, not bugs)
- `GET /saved-searches` returns an **empty list** instead of a 500 if the
  `expansion_saved_search` table is absent (post-migration safety).
- `GET /searches/{id}/report` soft-handles 404/422 — the frontend logs and
  shows no report rather than erroring.
- Road/parking context falls back from `expansion_*` tables to OSM tables, and
  marks `context_available=false` in the feature snapshot if all attempts fail.
- Brand-presence and construction-proximity gates **pass defensively** when
  their data block is missing/malformed (avoids penalizing rows computed
  before those features existed).

### 9.5 Known coverage gap
- The Arabic locale file has **~52 fewer `expansionAdvisor` keys** than the
  English file (572 vs 624) — mostly advanced diagnostic / score-breakdown
  strings. Primary UI is fully translated; some secondary diagnostic text is
  not yet localized to Arabic.

### 9.6 No expansion-specific ML training code
`app/ml/` contains no Expansion-Advisor-specific training module. The advisor
*consumes* pre-trained models (e.g. restaurant suitability / listing-quality
classifiers) but the training code there belongs to other surfaces.

---

## 10. Validation guidance

When changing Expansion Advisor logic, sanity-check that:
- Results stay inside Riyadh.
- Scores remain internally consistent (the `scoreInvariants` dev check helps).
- Top candidates are not overly repetitive (dedupe not too weak/aggressive).
- Distances, areas, and reach metrics are plausible.
- Candidate-loop performance has not regressed (bulk queries, no N+1).
- Frontend/backend/schema contracts stay aligned, and any new user-facing
  string is added to **both** locale files.

Run the narrowest relevant `pytest` targets first, then `make test` for
broader changes; run `cd frontend && npm run build` for frontend changes.

---

## Appendix — Key files

**Backend**
- `app/api/expansion_advisor.py` — routers, request/response models.
- `app/services/expansion_advisor.py` — scoring engine (~10.6k lines).
- `alembic/versions/20260310_*` … `20260314_*` — schema.
- `app/ingest/expansion_advisor_*` — ingestion jobs.

**Frontend**
- `frontend/src/features/expansion-advisor/` — feature UI (~58 files).
- `frontend/src/features/expansion-advisor/ExpansionAdvisorPage.tsx` — orchestrator.
- `frontend/src/lib/api/expansionAdvisor.ts` — API client + types.
- `frontend/src/components/Map.tsx`, `frontend/src/map/expansionOverlay.ts` — map layer.
- `frontend/src/i18n/en.json`, `frontend/src/i18n/ar.json` — translations.

---

# Expansion Advisor — Why It Matters for Your Brand

*The technical sections above describe how the system is built. This section
explains, for a business audience, why the Expansion Advisor is a valuable
decision tool.*

## Stop guessing where to open next

Choosing the wrong location is the most expensive mistake a growing food or
retail brand can make — a single underperforming branch can absorb years of
profit from your strongest stores. The Expansion Advisor replaces gut feel,
broker pitches, and scattered spreadsheets with a **single, evidence-based
decision engine** purpose-built for Riyadh.

You describe your brand once. The Advisor does the rest.

## What you get

**Real candidate sites, not abstract heatmaps.**
The Advisor searches actual parcels and live commercial listings across
Riyadh, filtered to your size requirements and target districts — so every
recommendation is a place you could actually sign a lease on.

**A decision, not just data.**
Each site arrives ranked, with a clear verdict, a plain-language decision
memo, the top reasons in its favor, and the top risks to watch. Your team
reads a recommendation — not a pile of numbers.

**Nine dimensions of analysis on every site.**
Demand, location fit, competitive whitespace, economics, brand fit, delivery
market strength, and more — each scored and explained. The Advisor weighs
these differently depending on whether you run quick-service, dine-in,
delivery-first, or café concepts, because a great delivery kitchen and a great
flagship restaurant are not the same site.

**Cannibalization protection built in.**
Tell the Advisor where your existing branches are, and it actively measures
how a new site would compete with your own network — so you grow your
footprint without eating your own sales.

**Honest about economics.**
The Advisor estimates annual rent, fit-out cost, and revenue potential, and
flags whether a site is genuinely good *value* or simply expensive. It
benchmarks rent against real district-level market comparables rather than
asking-price optimism.

**Compare, shortlist, and report.**
Put your top sites side by side, see which one wins on each criterion, and
generate an executive recommendation report your leadership or investors can
act on immediately. Save studies as drafts, refine them, and finalize when
you are ready to commit.

## Why it is technically credible

The Expansion Advisor is not a black box. Every score is **explainable**: you
can open any candidate and see exactly which factors moved its ranking and
why. It is grounded in **real Riyadh data** — parcel boundaries, live property
listings, delivery-platform activity across the major apps, road and parking
context, and competitor presence — refreshed through dedicated ingestion
pipelines.

It is also **disciplined**. A market-viability layer screens out sites that
fail basic thresholds and quietly demotes weak ones, so your shortlist is
genuinely your best options — not padded with filler. Deduplication ensures
you are comparing distinct opportunities, not the same block five times.

## The bottom line

The Expansion Advisor turns Riyadh expansion from a slow, opinion-driven
process into a **fast, repeatable, defensible** one. Better location decisions,
fewer costly mistakes, and a clear story you can take to your board — for
every single new branch.
