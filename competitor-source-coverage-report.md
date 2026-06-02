# Competitor-source coverage investigation (READ-ONLY)

No edits to repo source, no branch, no PR. Every coverage number below is something **you** run in
Codespace; the exact `psql -c` one-liners are grouped at the end (Q3/Q4).

## TL;DR on the crux question

**The "OSM restaurant data" you'd lean on already lives inside `restaurant_poi` as `source='osm'`
rows** — ingested via the Overpass API (`app/ingest/restaurant_pois.py:95`,
`ingest_osm_restaurants`, querying `amenity~restaurant|fast_food|cafe`). The raw
`planet_osm_point`/`planet_osm_polygon` osm2pgsql tables exist in the DB but are used **only for
roads, parking, landuse, and foot-traffic amenities (school/mosque/park/mall)** — never for
restaurants. So "OSM as a Google substitute" is really two different questions, and the queries
below measure both. Whether it's dense enough is a number you must read off query (c1)/(c2) — not
guessed.

One critical subtlety that changes how you read everything: on `restaurant_poi`,
**`source` = original ingestion origin** (overture / osm / hungerstation / talabat / mrsool), while
**`google_place_id` = whether Google enrichment later matched that row**. They are backfilled onto
existing rows, so a single physical row can be `source='osm'` **and** `google_place_id IS NOT NULL`
at the same time. Dropping paid Google enrichment removes the `google_place_id` / `rating` /
`business_status` *fields*, **not** the underlying overture/osm rows.

---

## Q1 — Source inventory (verified against live schema + ingest)

### (a) Google-enriched POIs -> `restaurant_poi`
- Table: `restaurant_poi` (`app/models/tables.py:302`).
- Geom: `geom geometry(POINT, 4326)`, **NOT NULL**, GiST-indexed, trigger-populated from `lat`/`lon`
  (`alembic/versions/0010_restaurant_location_tables.py:43-68`).
- Category column: `category VARCHAR(64)` (burger/pizza/traditional/...); also `subcategory`.
- Google linkage: `google_place_id TEXT` (indexed), plus `google_fetched_at`, `google_confidence`,
  `business_status` (OPERATIONAL / CLOSED_TEMPORARILY / CLOSED_PERMANENTLY, NULL for non-Google rows).
- `source VARCHAR(32)` enumerated in code as `overture, osm, hungerstation, talabat, mrsool`
  (`tables.py:312`). There is **no `source='google'`** — Google is an enrichment overlay, not a row
  origin. -> distribution query **Q1-a** below.

### (b) OSM amenity data -> **two distinct presences, only one is restaurant data**
1. **`restaurant_poi WHERE source='osm'`** — actual OSM restaurants/cafes/fast_food pulled via
   Overpass (`ingest/restaurant_pois.py:95-149`, ids like `osm:<id>`, category via
   `normalize_osm_cuisine`). Geom 4326, same column as (a). **This is the real OSM restaurant
   coverage.**
2. **`planet_osm_point` / `planet_osm_polygon`** — raw osm2pgsql import. Tag columns `amenity`,
   `cuisine`, `shop`, `leisure`, `building`, `parking`; geometry column `way`. In
   `expansion_advisor.py` these are read for parking (`amenity='parking'`, line 2184/8273) and
   foot-traffic POIs (school/mosque/park/mall, line 8273-8295) — **never
   `amenity IN ('restaurant','cafe','fast_food')`**. Whether these rows even contain restaurants is
   unknown until you run **Q1-b2**.
   - **SRID ambiguity in the repo itself:** the authoritative comment at `expansion_advisor.py:8572`
     says `planet_osm_polygon.way` is **SRID 3857** and every geography cast must be
     `ST_Transform(way,4326)` first (used at 8596/8603). But other live queries cast
     `op.way::geography` / `pt.way::geography` **directly** (lines 2190, 8304), which is only correct
     if the data is 4326. **Verify SRID before trusting any planet_osm coverage number** -> **Q1-b3**.

### (c) Delivery records -> `delivery_source_record`
- Table: `delivery_source_record` (`app/delivery/models.py:29`).
- **Coordinates are optional by design** — docstring: *"never required to have coordinates... valuable
  even with only district, brand, cuisine, or rating data."* Columns: `lat`/`lon` (Numeric, nullable),
  `district_text`, `area_text`, `geocode_method`, `location_confidence`.
- A precomputed `geom geometry(POINT, 4326)` column + GiST index + sync trigger were added later
  (`alembic/versions/20260322_geom_indexes_dsr_pop.py:22-71`), populated from lat/lon (NULL stays NULL).
- Spatial join in scoring is **geom-gated**: `_bulk_enrich_competitors` checks
  `_cached_column_exists(db,"delivery_source_record","geom")`; if present it filters
  `dsr.geom IS NOT NULL`, else falls back to `ST_MakePoint(dsr.lon,dsr.lat)` with
  `lat/lon IS NOT NULL` (`expansion_advisor.py:6379-6387`). **Rows without coords contribute zero to
  spatial competitor counts** — their coverage can only be measured non-spatially (by `district_text`).
  -> row/coords census **Q1-c**.

---

## Q2 — How scoring reads each source *today*

Two competitor reads exist; **neither reads `planet_osm` restaurants**.

**1. Site-B density read — `_bulk_enrich_competitors` (`expansion_advisor.py:6314-6486`).** A single
UNION ALL:
- *Source 1* `restaurant_poi rp` — **all sources, no `source` filter** (so osm/overture/delivery-origin/
  Google-enriched rows are all counted), filtered `business_status IS NULL OR ='OPERATIONAL'`
  (excludes closed venues), `LEFT JOIN expansion_competitor_quality` for `chain_strength_score`.
  Predicate:
  ```sql
  WHERE (rp.business_status IS NULL OR rp.business_status='OPERATIONAL')
    AND ST_DWithin(rp.geom::geography, ST_SetSRID(ST_MakePoint(i.lon,i.lat),4326)::geography, :radius_m)
  ```
- *Source 2* `delivery_source_record dsr` — geom-gated as above; category matched via regex on
  `category_raw`/`cuisine_raw`; `chain_strength` always NULL on this leg.
- Radius = `_catchment_radii(service_model)["competition"]` -> **qsr 1200 / dine_in 3000 /
  delivery_first 2500** (`expansion_advisor.py:818-820`); legacy default 1000.

**2. Comparables read — `comparable_competitors` (`expansion_advisor.py:3455-3581`).** Fallback chain
`expansion_competitor_quality` -> `restaurant_poi`:
- Prefers `expansion_competitor_quality` (`ecq.geom`, `ST_DWithin(...,1500)`, exact `category` match,
  dedup on `canonical_brand_id`). ECQ is itself **derived from `restaurant_poi`**
  (`restaurant_poi_id` FK; built by `ingest/expansion_advisor_competitors.py`).
- Falls back to `restaurant_poi` (all sources, `ST_DWithin(...,1500)`, exact category match).
- **`delivery_source_record` is NOT read here** (density only).

**Is OSM read as a competitor source today?** Only the OSM rows that already sit in
`restaurant_poi`/ECQ (i.e. `source='osm'`). The raw `planet_osm` restaurant amenities are **not** a
competitor source.

**What it would take to add `planet_osm` restaurants (conceptual, not code):** add a third UNION
branch in `_bulk_enrich_competitors` selecting `planet_osm_point`/`planet_osm_polygon WHERE amenity
IN ('restaurant','cafe','fast_food','food_court')`, with (i) **correct SRID** (`ST_Transform(way,4326)`
per the 3857 note — resolve the ambiguity first), (ii) a **category mapping** from OSM
`amenity`/`cuisine` tags into the app taxonomy (`normalize_osm_cuisine` already exists), (iii)
**dedup against `restaurant_poi source='osm'`** — which is sourced from the *same* OSM data via
Overpass, so this is largely duplication, not new coverage, and (iv) no `business_status`/closed
handling (OSM has none). Net: it mostly re-counts what's already there.

---

## Q3 — Coverage comparison (THE deliverable)

Candidate set = `candidate_location WHERE source_tier=1 AND is_cluster_primary=TRUE`, joined on
`cl.geom` (geometry(POINT,4326), GiST-indexed). Each query counts **distinct competitor POIs** that
fall within radius of **any** primary (correlated `EXISTS`, so each POI is counted at most once and
the GiST index on both sides is used). Run the `1200` set, then swap to `3000`.

### Run these first (confirm the 523 and the SRID)
```bash
psql -c "SELECT count(*) FROM candidate_location WHERE source_tier=1 AND is_cluster_primary;"
psql -c "SELECT ST_SRID(way) srid, count(*) FROM planet_osm_point GROUP BY 1;"
```

### Coverage queries — 1200 m (QSR competition radius)

**(a) Google-enriched POIs**
```bash
psql -c "SELECT count(*) FROM restaurant_poi rp WHERE rp.google_place_id IS NOT NULL AND EXISTS (SELECT 1 FROM candidate_location cl WHERE cl.source_tier=1 AND cl.is_cluster_primary AND ST_DWithin(rp.geom::geography, cl.geom::geography, 1200));"
```

**(b) All `restaurant_poi` (any source)**
```bash
psql -c "SELECT count(*) FROM restaurant_poi rp WHERE EXISTS (SELECT 1 FROM candidate_location cl WHERE cl.source_tier=1 AND cl.is_cluster_primary AND ST_DWithin(rp.geom::geography, cl.geom::geography, 1200));"
```

**(c1) OSM restaurants already in `restaurant_poi` (`source='osm'`) — the real OSM coverage**
```bash
psql -c "SELECT count(*) FROM restaurant_poi rp WHERE rp.source='osm' AND EXISTS (SELECT 1 FROM candidate_location cl WHERE cl.source_tier=1 AND cl.is_cluster_primary AND ST_DWithin(rp.geom::geography, cl.geom::geography, 1200));"
```

**(c2) Raw `planet_osm` restaurant amenities (NOT currently used; SRID-sensitive — uses `ST_Transform(way,4326)`)**
```bash
psql -c "SELECT (SELECT count(*) FROM planet_osm_point pt WHERE lower(coalesce(pt.amenity,'')) IN ('restaurant','cafe','fast_food','food_court') AND EXISTS (SELECT 1 FROM candidate_location cl WHERE cl.source_tier=1 AND cl.is_cluster_primary AND ST_DWithin(ST_Transform(pt.way,4326)::geography, cl.geom::geography, 1200))) + (SELECT count(*) FROM planet_osm_polygon op WHERE lower(coalesce(op.amenity,'')) IN ('restaurant','cafe','fast_food','food_court') AND EXISTS (SELECT 1 FROM candidate_location cl WHERE cl.source_tier=1 AND cl.is_cluster_primary AND ST_DWithin(ST_Transform(op.way,4326)::geography, cl.geom::geography, 1200))) AS osm_raw_amenity_pois;"
```
> If Q1-b3 shows `ST_SRID(way)=4326`, `ST_Transform(way,4326)` is a harmless no-op; if it shows 3857
> it's required. If `way` were ever stored as 3857-data-under-a-4326-declaration, neither form is
> reliable — note that and stop.

**(d) `delivery_source_record` (spatially joinable rows only)**
```bash
psql -c "SELECT count(*) FROM delivery_source_record dsr WHERE dsr.geom IS NOT NULL AND EXISTS (SELECT 1 FROM candidate_location cl WHERE cl.source_tier=1 AND cl.is_cluster_primary AND ST_DWithin(dsr.geom::geography, cl.geom::geography, 1200));"
```

For the **3000 m** variant (dine-in max), rerun (a)-(d) with `1200` -> `3000`.

**On sources that lack usable coordinates:** `delivery_source_record` coords are optional, so (d)
undercounts true delivery coverage by exactly the geom-less rows. Measure that gap (and the spatial
vs non-spatial split) with the census below — district-only rows can only be "covered" by matching
`dsr.district_text` to the candidate's `district_ar`/`district_en`, not by radius.

### Supporting census (Q1 numbers)

**Q1-a — source distribution + Google enrichment overlap**
```bash
psql -c "SELECT source, count(*) total, count(*) FILTER (WHERE google_place_id IS NOT NULL) with_google, count(*) FILTER (WHERE business_status='OPERATIONAL') operational FROM restaurant_poi GROUP BY source ORDER BY total DESC;"
```

**Q1-b2 — do raw planet_osm tables contain restaurants at all?**
```bash
psql -c "SELECT 'point' tbl, count(*) FROM planet_osm_point WHERE lower(coalesce(amenity,'')) IN ('restaurant','cafe','fast_food','food_court') UNION ALL SELECT 'polygon', count(*) FROM planet_osm_polygon WHERE lower(coalesce(amenity,'')) IN ('restaurant','cafe','fast_food','food_court');"
```

**Q1-c — delivery coords census (how much of d is spatially measurable)**
```bash
psql -c "SELECT count(*) total, count(geom) with_geom, count(*) FILTER (WHERE lat IS NOT NULL AND lon IS NOT NULL) with_latlon, count(*) FILTER (WHERE geom IS NULL) geomless FROM delivery_source_record;"
```

---

## Q4 — Overlap (OSM adds vs duplicates Google)

Cheap and clean because both live in `restaurant_poi` with geom 4326. "OSM rows in candidate areas
with **no** Google-enriched POI within ~50 m" = net coverage OSM adds; reverse = Google-only venues
OSM misses.

**OSM-only (adds coverage Google lacks), 1200 m candidate areas:**
```bash
psql -c "SELECT count(*) FROM restaurant_poi o WHERE o.source='osm' AND EXISTS (SELECT 1 FROM candidate_location cl WHERE cl.source_tier=1 AND cl.is_cluster_primary AND ST_DWithin(o.geom::geography, cl.geom::geography, 1200)) AND NOT EXISTS (SELECT 1 FROM restaurant_poi g WHERE g.google_place_id IS NOT NULL AND ST_DWithin(g.geom::geography, o.geom::geography, 50));"
```

**Google-only (OSM misses):**
```bash
psql -c "SELECT count(*) FROM restaurant_poi g WHERE g.google_place_id IS NOT NULL AND EXISTS (SELECT 1 FROM candidate_location cl WHERE cl.source_tier=1 AND cl.is_cluster_primary AND ST_DWithin(g.geom::geography, cl.geom::geography, 1200)) AND NOT EXISTS (SELECT 1 FROM restaurant_poi o WHERE o.source='osm' AND ST_DWithin(o.geom::geography, g.geom::geography, 50));"
```
> Caveat: because Google enrichment backfills onto existing rows, a `source='osm'` row can itself
> carry `google_place_id` — the 50 m self-join across the same table still works (it compares
> distinct rows), but if OSM and Google data were merged onto one row, that venue won't appear in
> either bucket. The `with_google` column from Q1-a tells you how much merging has happened.

---

## What these numbers will and won't tell you

- **(a) vs (b)** = how much coverage survives if Google enrichment is dropped but
  overture/osm/delivery rows stay (the rows don't disappear; only the
  `google_place_id`/`rating`/`business_status` fields do).
- **(c1)** = the OSM restaurant coverage that **already feeds scoring today**. If this is close to
  (a), OSM is already carrying the load. If it's a small fraction, OSM is sparse.
- **(c2)** = whether the *unused* raw planet_osm tables hold additional restaurant rows (likely heavy
  overlap with c1 since both originate from OSM).
- **(d)** = delivery spatial coverage, undercounted by the geom-less rows in Q1-c.
- **Q4** = whether OSM is additive or just duplicative of Google.

No swap recommended — these are the numbers to make that call. The accuracy-vs-cost decision hinges
on whether **(c1) at 1200 m** (and the OSM-only count in Q4) is large enough relative to **(a)** to
keep competitor density trustworthy after enrichment is dropped, keeping in mind the live density
read also excludes closed venues (`business_status`) — a signal OSM cannot provide.
