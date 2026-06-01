# Expansion Advisor — Google Cloud Data Consumption & Refresh Cadence

> Audit of where the **Expansion Advisor** consumes data from Google Cloud (Google Maps Platform / Places API), what it pulls, how it is used, and how often each source should be refreshed.
>
> Scope: Riyadh-only production behavior. All Google usage is gated by a single key — `GOOGLE_PLACES_API_KEY` (`app/core/config.py:56`).

---

## 1. Executive summary

The Expansion Advisor uses **one Google Cloud product: the Google Places API** (Google Maps Platform). It appears in **three functional areas**, each backed by an ingestion job and a GitHub Actions workflow:

| # | Function | Google API | Lands in | Used for |
|---|----------|-----------|----------|----------|
| 1 | Restaurant POI **enrichment** (ratings, reviews, place_id) | Places Text Search + Details | `restaurant_poi.google_*` columns | Competitor quality / density scoring; "comparable competitors" report context |
| 2 | Restaurant POI **discovery** (grid bootstrap) | Places Text / Nearby Search over a Riyadh grid | `restaurant_poi` rows | Building the competitor POI universe |
| 3 | **Parking amenity** ingestion | Places Nearby Search (`type=parking`) | `expansion_parking_asset` (`source='google_places'`) | Parking-availability signal in candidate scores |

**Google is a soft / optional layer.** If `GOOGLE_PLACES_API_KEY` is absent, jobs log a warning and the system falls back gracefully:

- Competitors: `expansion_competitor_quality` → `restaurant_poi` → `delivery_source_record` (Hungerstation / Jahez)
- Parking: `expansion_parking_asset` → OSM (`planet_osm_polygon`)

No errors; graceful degradation.

---

## 2. Google Places API — Restaurant / POI enrichment

### Files

| File | Purpose |
|------|---------|
| `app/connectors/google_places.py` | Sync Places client (Text Search, Details) |
| `app/connectors/google_places_async.py` | Async Places client (rate limiting, multi-variant search) |
| `app/ingest/google_reviews_enrich.py` | Enrichment job: matches `restaurant_poi` rows to Google Places and fetches ratings |
| `alembic/versions/0011_google_reviews_columns.py` | Adds `google_place_id`, `google_fetched_at`, `google_confidence` to `restaurant_poi` |
| `alembic/versions/0012_google_reviews_enrich_state.py` | Resumable enrichment cursor table |
| `.github/workflows/enrich-google-reviews.yml` | On-demand restaurant enrichment workflow |
| `.github/workflows/expansion-advisor-data-competitors.yml` | Weekly competitor build (optional Google refresh first) |

### Data pulled

**Text Search** (`maps.googleapis.com/maps/api/place/textsearch/json`):
- `place_id` — stable Google identifier
- `name`, `geometry.location` (lat/lng)
- `rating` — average user rating (0–5)
- `user_ratings_total` — review count
- `price_level` — cost indicator (1–4)
- `types` — semantic categories (restaurant, cafe, bakery, meal_delivery, …)
- `business_status` — OPERATIONAL / CLOSED_TEMPORARILY / PERMANENTLY_CLOSED
- `formatted_address`

**Details** (`maps.googleapis.com/maps/api/place/details/json`): subset of the above (place_id, name, rating, user_ratings_total, price_level, geometry, types, formatted_address, business_status).

### Schema changes on `restaurant_poi`

| Column | Type | Notes |
|--------|------|-------|
| `google_place_id` | TEXT | indexed, nullable |
| `google_fetched_at` | TIMESTAMP | freshness tracker, nullable |
| `google_confidence` | NUMERIC | match confidence (0–1) |
| `rating` | NUMERIC | merged with Google data |
| `review_count` | INT | from `user_ratings_total` |
| `price_level` | TEXT | cost tier |

### How the Expansion Advisor uses it

- **Competitor quality scoring** (`app/services/expansion_advisor.py` ~3455–3577): fallback chain `expansion_competitor_quality → restaurant_poi (Google) → delivery_source_record`. Uses Google `rating` / `review_count`, category match, distance-weighted competitor density.
- **Comparable competitors context** (~6324–6434): top competitors surfaced with Google rating + review count.
- **Feature snapshot provenance**: `feature_snapshot_json.context_sources` records `competitor_source`, pool size, and `top_competitors[]` (id/name/rating/review_count/score).

---

## 3. Google Places API — Restaurant discovery (grid bootstrap)

### Files

| File | Purpose |
|------|---------|
| `scripts/google_places_grid_search.py` | Grid-driven Places search to discover restaurant POIs |
| `.github/workflows/google-places-grid-search.yml` | On-demand discovery workflow (cost estimate / dry-run supported) |

### Behavior

- Configurable place types (restaurant, cafe, bakery, meal_takeaway, meal_delivery) and search radius.
- Cell-level progress checkpointing; supports dry-run (cost estimate only) and full re-process.
- Populates `restaurant_poi` rows that the enrichment job (Section 2) later enriches with ratings.

---

## 4. Google Places API — Parking amenity ingestion

### Files

| File | Purpose |
|------|---------|
| `app/ingest/expansion_advisor_parking_google.py` | Grid-driven parking ingest via Nearby Search |
| `app/connectors/google_places_async.py` (`nearby_search`) | Nearby Search endpoint |
| `.github/workflows/expansion-advisor-data-parking-google.yml` | Scheduled parking ingest (6-month cadence) |

### Data pulled

**Nearby Search** (`maps.googleapis.com/maps/api/place/nearbysearch/json`):
- Params: `location` (grid point), `radius` (350 m per cell), `type=parking`
- Results: `place_id`, `name`, `geometry.location`, `types`, `business_status`

### Schema (`expansion_parking_asset`)

| Column | Value from Google |
|--------|-------------------|
| `source` | `'google_places'` (vs `'osm'`) |
| `name` | facility name |
| `geom` | point, SRID 4326 |
| `amenity_type` | `'unknown'` (Google does not provide detail) |
| `capacity`, `covered`, `public_access` | NULL (not provided by Google) |
| `walk_access_score` | flat default 65.0 |
| `dropoff_score` | flat default 55.0 |

### Ingestion strategy

- Grid ~500 m spacing over built-up Riyadh bbox (≈46.55–46.95°E, 24.55–24.85°N).
- Per cell: Nearby Search radius 350 m, max 20 results/cell (API limit).
- Dedup in-memory on `place_id`; **replace mode** clears existing `source='google_places'` rows and rebuilds.

### How the Expansion Advisor uses it

- **Parking score** (`app/services/expansion_advisor.py` ~2130–2180): counts parking amenities within candidate radius (`nearby_parking_amenity_count`). Falls back to OSM `planet_osm_polygon` estimate when the table is empty.

---

## 5. Refresh cadence (current + recommended)

| Data source | Workflow | Current cadence | Built-in freshness rule | Recommended |
|-------------|----------|-----------------|--------------------------|-------------|
| **Restaurant reviews / ratings** | `enrich-google-reviews.yml` | **On-demand only** (`workflow_dispatch`) | Skips rows refreshed within **30 days** (`STALE_DAYS=30`, `google_reviews_enrich.py:56`) | **~Monthly** (match the 30-day TTL) |
| **Competitor refresh** (optionally re-runs Google enrich) | `expansion-advisor-data-competitors.yml` | **Weekly — Fri 07:00 UTC** (`cron: 0 7 * * 5`) | Rebuilds competitor quality each run | Weekly (already automated) |
| **Parking amenities** | `expansion-advisor-data-parking-google.yml` | **Biannual — Jan 1 & Jul 1, 04:00 UTC** (`cron: 0 4 1 1,7 *`) | Full replace of `google_places` rows | Every 6 months (infrastructure changes slowly) |
| **Restaurant discovery (grid)** | `google-places-grid-search.yml` | **On-demand only** | Cell-level checkpointing | Only when expanding POI coverage |

### Freshness / resilience details

- **Reviews enrichment**: 30-day stale gate (`STALE_DAYS=30`), cursor-resumable via `google_reviews_enrich_state.last_cursor`; rate-limited ~8 QPS with exponential backoff on 429/5xx; default batch ~200 rows.
- **Competitors**: optional input `refresh_google_reviews=true` runs Google enrichment before rebuild; missing key → competitors built from existing `restaurant_poi` as-is, then `expansion_advisor_refresh` recomputes scores.
- **Parking**: checkpoint JSON flushed every 100 cells; replace-mode rebuild inside a transaction.

### ⚠️ Cadence gap to note

The **reviews/ratings enrichment is the fastest-decaying data** (ratings & review counts change continuously) but is the **only Google source with no automatic schedule** — it refreshes only on manual runs or when the weekly competitors job is invoked with `refresh_google_reviews=true`. To keep ratings within their own 30-day staleness window automatically, add a monthly `cron` to `enrich-google-reviews.yml` (or default `refresh_google_reviews=true`).

---

## 6. Configuration & cost

### Configuration

- `GOOGLE_PLACES_API_KEY: str | None = os.getenv("GOOGLE_PLACES_API_KEY")` — `app/core/config.py:56`
- Workflows read it from `secrets.GOOGLE_PLACES_API_KEY`.
- Missing key → warnings + fallbacks; no failures.

### Client configuration

| | Sync (`google_places.py`) | Async (`google_places_async.py`) |
|--|---------------------------|----------------------------------|
| QPS | 8 | 8 (token-bucket) |
| Concurrency | n/a | 20 in-flight (semaphore) |
| Retries | 4 (backoff 1.5/3/6/12 s) | 4 (exponential backoff) |
| Caching | in-memory (name+loc, place_id) | same + multi-variant name matching, category-aware type filtering, radius escalation (500 m → 1500 m), generic-name rejection |

### Rough API cost

| Component | Per call | Per run | Frequency |
|-----------|----------|---------|-----------|
| Restaurant enrichment | $0.003 (Text Search) + opt. $0.003 (Details) | ~$1.50–3.00 (~500 rows) | On-demand |
| Parking ingest | $0.003 (Nearby Search) | ~$0.60–0.90 (~200–300 cells) | 2×/year (~$1.20–1.80/yr) |
| Competitor refresh (optional Google) | as enrichment | varies | Weekly (off by default) |

---

## 7. Key file / line reference

| Topic | File | Lines |
|-------|------|-------|
| Google API clients | `app/connectors/google_places.py` / `google_places_async.py` | 95–278 / 206–627 |
| Restaurant enrichment | `app/ingest/google_reviews_enrich.py` | 56 (STALE_DAYS), 124–129, 143–192 |
| Parking ingest | `app/ingest/expansion_advisor_parking_google.py` | 55–116 |
| Schema | `alembic/versions/0011_google_reviews_columns.py`, `0012_google_reviews_enrich_state.py` | 19–46 / 19–35 |
| Service integration | `app/services/expansion_advisor.py` | 2130–2180 (parking), 3455–3577 & 6324–6434 (competitors) |
| Workflows | `.github/workflows/enrich-google-reviews.yml`, `expansion-advisor-data-parking-google.yml`, `expansion-advisor-data-competitors.yml`, `google-places-grid-search.yml` | cron at lines 5 / 5 / 5 |
| Config | `app/core/config.py` | 56 |

---

*Generated as a read-only audit. No application behavior was modified.*
