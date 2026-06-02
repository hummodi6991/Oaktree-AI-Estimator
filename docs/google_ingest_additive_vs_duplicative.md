# Are the two remaining Google ingest jobs additive or duplicative?

> **Read-only investigation.** No code edits, no branch, no PR. Verified against live
> repository files (line numbers re-derived from current source, not docs).
>
> **Scope:** the two paid Google jobs still on schedule after PR #1269 disabled paid
> reviews enrichment:
> - `google-places-grid-search.yml` (quarterly Nearby Search → `restaurant_poi`, `source='google_places'`)
> - `expansion-advisor-data-parking-google.yml` (biannual Nearby Search → `expansion_parking_asset`, `source='google_places'`)
>
> **Goal:** measure how much *unique* coverage each job's Google rows add over the
> free sources already ingested (OSM, Overture, delivery), in the production
> candidate areas. Report numbers + queries only — no recommended action.

---

## ⚠️ Correction to the query assumptions

OSM parking in `expansion_parking_asset` is stored under **`source IN ('osm_polygon','osm_point')`**,
not `'osm'`. The ingest writes those two literals:

- `app/ingest/expansion_advisor_parking.py:139` → `'osm_polygon'`
- `app/ingest/expansion_advisor_parking.py:240` → `'osm_point'`

All queries below therefore use `source <> 'google_places'` for the non-Google side
so neither OSM origin is missed.

---

## How the data actually flows (verified)

- **`restaurant_poi`** is written by:
  - `app/ingest/restaurant_pois.py` → `ingest_overture_restaurants` (`source='overture'`),
    `ingest_osm_restaurants` (`source='osm'`)
  - the delivery upsert (`source` ∈ `hungerstation`/`talabat`/`mrsool`)
  - **grid discovery** `scripts/google_places_grid_search.py` (`source='google_places'`)
  - Google reviews enrich only *updates existing rows* (rating/review_count/price/business_status)
    and is now **manual-only** (scheduled cron commented out).
- **`expansion_parking_asset`** is written by:
  - `app/ingest/expansion_advisor_parking.py` (`source` ∈ `osm_polygon`/`osm_point`)
  - **`app/ingest/expansion_advisor_parking_google.py`** (`source='google_places'`)
- The **live request path makes no Google API calls** — it only counts rows already
  persisted in these two tables.

---

## Part 1 — Grid discovery (`restaurant_poi`)

### 1c. Does scoring distinguish source? **No.**

`_bulk_enrich_competitors` (`app/services/expansion_advisor.py:6310`) is the only
competitor-density read. Its restaurant leg (`:6425-6442`) counts **every**
`restaurant_poi` row in radius with no source predicate — the only filter is the
closed-venue guard:

```sql
FROM restaurant_poi rp
LEFT JOIN expansion_competitor_quality ecq
       ON ecq.restaurant_poi_id = rp.id AND ecq.city = 'riyadh'
WHERE (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
  AND ST_DWithin(rp.geom::geography,
                 ST_SetSRID(ST_MakePoint(i.lon, i.lat), 4326)::geography,
                 :radius_m)
UNION ALL
-- delivery_source_record leg (source-agnostic; chain_strength NULL)
```

A `google_places` row is counted identically to an `osm`/`overture`/delivery row.
**Consequence:** disabling Google discovery only changes `competitor_count` for
candidates where Google is the *only* source that found a nearby venue — i.e.
exactly the "unique" count measured in 1b. The single place Google-derived data
*uniquely* feeds scoring is the `chain_strength` leg via the `ecq` join (`:6426`),
but `expansion_competitor_quality` is built from **all** `restaurant_poi` sources
(`app/ingest/expansion_advisor_competitors.py`), so even that is only
Google-dependent where a chain's POI rows are exclusively Google-sourced.

### 1d. Staleness / net-new openings — **Google is NOT the only source of new venues.**

Free sources that add net-new restaurant rows on a schedule:

| Workflow | Cron | Cadence | Adds |
|---|---|---|---|
| `ingest-restaurant-pois.yml` | `0 3 * * 1` | weekly (Mon) | Overture + OSM restaurants |
| `expansion-advisor-data-delivery-sccc.yml` | `0 5 * * *` | daily | delivery-listed venues → `restaurant_poi` |
| `expansion-advisor-data-competitors.yml` | `0 7 * * 5` | weekly (Fri) | rebuilds `expansion_competitor_quality` |
| `google-places-grid-search.yml` | `0 4 1 1,4,7,10 *` | **quarterly** | Google-only venues |

Grid discovery adds venues that are on Google Maps but absent from OSM/Overture/
delivery — typically very new, or non-delivery dine-in / independents. Trade-off:
free sources already refresh weekly/daily, so the marginal "future openings" Google
adds equals the 1b-unique share, and it is refreshed only quarterly anyway.

### 1a — source distribution

```sql
SELECT source, count(*) FROM restaurant_poi GROUP BY source ORDER BY 2 DESC;
```

### 1b — unique coverage at 1200 m (swap `1200` → `3000` for the 3 km run)

Returns all four numbers in one query:

```sql
WITH prim AS (
  SELECT COALESCE(geom, ST_SetSRID(ST_MakePoint(lon,lat),4326)) AS geom
  FROM candidate_location WHERE source_tier=1 AND is_cluster_primary=TRUE),
inr AS (
  SELECT rp.source, COALESCE(rp.geom, ST_SetSRID(ST_MakePoint(rp.lon,rp.lat),4326)) AS geom
  FROM restaurant_poi rp
  WHERE EXISTS (SELECT 1 FROM prim p
                WHERE ST_DWithin(COALESCE(rp.geom,ST_SetSRID(ST_MakePoint(rp.lon,rp.lat),4326))::geography,
                                 p.geom::geography, 1200)))
SELECT
  count(*) FILTER (WHERE source='google_places') AS google_in_radius,
  count(*) FILTER (WHERE source='google_places' AND NOT EXISTS (
     SELECT 1 FROM restaurant_poi o WHERE o.source<>'google_places'
       AND ST_DWithin(COALESCE(o.geom,ST_SetSRID(ST_MakePoint(o.lon,o.lat),4326))::geography,
                      inr.geom::geography, 50))) AS google_unique,
  count(*) FILTER (WHERE source<>'google_places') AS nongoogle_in_radius,
  count(*) FILTER (WHERE source<>'google_places' AND NOT EXISTS (
     SELECT 1 FROM restaurant_poi g WHERE g.source='google_places'
       AND ST_DWithin(COALESCE(g.geom,ST_SetSRID(ST_MakePoint(g.lon,g.lat),4326))::geography,
                      inr.geom::geography, 50))) AS nongoogle_unique
FROM inr;
```

- `google_unique / google_in_radius` = share of Google rows nobody else found (the decision number).
- `nongoogle_unique` = venues the free sources have that Google discovery misses.

> Optional: add `AND (rp.business_status IS NULL OR rp.business_status='OPERATIONAL')`
> to the `inr` CTE to mirror exactly what scoring counts. Left out above to show raw
> discovery coverage.

---

## Part 2 — Google parking (`expansion_parking_asset`)

### 2c. Scoring sensitivity — **saturates at 6; bands at 0 / 1-2 / 3-5 / 6+.**

`_parking_score` (`app/services/expansion_advisor.py:1797`):

```python
parking_amenity_signal = _clamp((nearby_parking_count / 6.0) * 100.0)   # :1801 linear, =100 at count 6, then flat
return _clamp(area_signal*0.35 + parking_amenity_signal*0.30
              + model_adjustment*0.20 + access_score*0.15)
```

The amenity count carries **0.30 weight**, linear up to 6 (each POI ≈ +5 score pts),
**flat at ≥6**. Evidence band `_parking_evidence_band` (`:1811`):
`0=none_found, 1-2=limited, 3-5=moderate, 6+=strong`.

The read counts **all sources** in `expansion_parking_asset` within **350 m**, no
source filter (`:2153-2158`, bulk path `:8227-8231`), and prefers this table over
OSM `planet_osm_polygon` whenever it has rows (`:2142`, `:1974`). So if a candidate
already has ≥6 OSM parking rows within 350 m, removing Google parking changes nothing.

### 2a — source distribution

```sql
SELECT source, count(*) FROM expansion_parking_asset GROUP BY source ORDER BY 2 DESC;
```

### 2b — unique parking coverage at 350 m (Google-only vs OSM-only near primaries)

```sql
WITH prim AS (
  SELECT COALESCE(geom, ST_SetSRID(ST_MakePoint(lon,lat),4326)) AS geom
  FROM candidate_location WHERE source_tier=1 AND is_cluster_primary=TRUE),
inr AS (
  SELECT epa.source, epa.geom FROM expansion_parking_asset epa
  WHERE epa.geom IS NOT NULL
    AND EXISTS (SELECT 1 FROM prim p WHERE ST_DWithin(epa.geom::geography, p.geom::geography, 350)))
SELECT
  count(*) FILTER (WHERE source='google_places') AS google_in,
  count(*) FILTER (WHERE source='google_places' AND NOT EXISTS (
     SELECT 1 FROM expansion_parking_asset o WHERE o.source<>'google_places' AND o.geom IS NOT NULL
       AND ST_DWithin(o.geom::geography, inr.geom::geography, 50))) AS google_unique,
  count(*) FILTER (WHERE source<>'google_places') AS osm_in,
  count(*) FILTER (WHERE source<>'google_places' AND NOT EXISTS (
     SELECT 1 FROM expansion_parking_asset g WHERE g.source='google_places' AND g.geom IS NOT NULL
       AND ST_DWithin(g.geom::geography, inr.geom::geography, 50))) AS osm_unique
FROM inr;
```

### 2c — how many of the 523 primaries actually change parking band if Google parking is removed

Uses the real saturation/band thresholds via `width_bucket(count, ARRAY[1,3,6])`,
which reproduces 0 / 1-2 / 3-5 / 6+ exactly:

```sql
WITH prim AS (
  SELECT id, COALESCE(geom, ST_SetSRID(ST_MakePoint(lon,lat),4326)) AS geom
  FROM candidate_location WHERE source_tier=1 AND is_cluster_primary=TRUE),
c AS (
  SELECT p.id,
    (SELECT count(*) FROM expansion_parking_asset e WHERE e.geom IS NOT NULL
       AND ST_DWithin(e.geom::geography, p.geom::geography, 350)) AS all_cnt,
    (SELECT count(*) FROM expansion_parking_asset e WHERE e.source<>'google_places' AND e.geom IS NOT NULL
       AND ST_DWithin(e.geom::geography, p.geom::geography, 350)) AS osm_cnt
  FROM prim p)
SELECT
  count(*) AS primaries,
  count(*) FILTER (WHERE all_cnt <> osm_cnt) AS raw_count_changes,
  count(*) FILTER (WHERE all_cnt <> osm_cnt AND all_cnt < 6) AS changes_below_saturation,
  count(*) FILTER (WHERE width_bucket(all_cnt, ARRAY[1,3,6]) <> width_bucket(osm_cnt, ARRAY[1,3,6])) AS evidence_band_changes
FROM c;
```

- `raw_count_changes` = primaries where Google contributes ≥1 parking POI in range.
- `changes_below_saturation` = of those, the ones not already saturated by OSM (where a drop actually moves the linear 0.30-weight signal).
- `evidence_band_changes` = primaries that cross a band boundary (the user-visible / material change). **If ~0, OSM coverage at 350 m is already sufficient and Google parking is duplicative.**

---

## How to read the results

| Job | "Mostly duplicative → safe to disable" if… | "Material unique coverage → keep" if… |
|---|---|---|
| **Grid discovery** (`google_places` → `restaurant_poi`) | `google_unique / google_in_radius` (1b) is small at both 1200 m & 3000 m → free OSM/Overture/delivery already cover the same venues, and 1c confirms scoring counts them equally. | `google_unique` is a large share → Google is the sole discoverer of many in-radius competitors, and (1d) those venues won't otherwise appear until/if OSM/Overture pick them up. |
| **Google parking** (`google_places` → `expansion_parking_asset`) | `evidence_band_changes` (2c) ≈ 0 → removing Google parking doesn't move any candidate's parking band; OSM is sufficient at 350 m. | `evidence_band_changes` is non-trivial → Google parking is the difference between e.g. "limited" and "moderate"/"strong" for real candidates. |

Two caveats when reading the outputs:

1. Parking saturation at count = 6 means `raw_count_changes` overstates impact —
   `evidence_band_changes` is the number that matters.
2. For grid discovery, even a high `google_unique` is refreshed only quarterly, while
   the free sources refresh weekly/daily (1d), so "unique today" and "worth a recurring
   quarterly bill" are slightly different questions.

Run the five queries (1a; 1b at 1200 then 3000; 2a; 2b; 2c) and the four-column
outputs give the unique-coverage fractions and band-change count to decide each job
on its own merits. No action recommended here.

---

### Source references

- `app/services/expansion_advisor.py:1797` — `_parking_score` (0.30 amenity weight, saturates at 6)
- `app/services/expansion_advisor.py:1811` — `_parking_evidence_band` (0 / 1-2 / 3-5 / 6+)
- `app/services/expansion_advisor.py:2142-2158` — live parking read (EA table preferred, all sources, 350 m)
- `app/services/expansion_advisor.py:6310,6425-6459` — `_bulk_enrich_competitors` (source-agnostic count)
- `app/ingest/expansion_advisor_parking.py:139,240` — OSM parking sources `osm_polygon` / `osm_point`
- `app/ingest/expansion_advisor_parking_google.py` — Google parking ingest (`source='google_places'`)
- `scripts/google_places_grid_search.py` — grid discovery ingest
- `.github/workflows/` — cron schedules cited in 1d
