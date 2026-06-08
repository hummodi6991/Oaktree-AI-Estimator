-- ============================================================================
-- l1_generator_coverage.sql
-- ----------------------------------------------------------------------------
-- READ-ONLY go/no-go probe for Layer 1 (modeled demand-generator index).
--
-- Goal: prove the proposed per-candidate demand-generator index has SIGNAL that
-- varies across real candidates and is not mostly zero, BEFORE any code is
-- written. For ~300 real candidates (sampled from the live scoring universe:
-- candidate_location Tier-1 cluster primaries — see
-- app/services/expansion_advisor.py:7107-7116 and :6108-6112), it reports, for
-- the dine_in 3.5 km demand catchment (_CATCHMENT_RADII_M["dine_in"]["demand"]
-- = 3500.0, app/services/expansion_advisor.py:818):
--   * catchment population (population_density.population sum),
--   * per-generator OSM counts (offices / malls-retail / transit / mosques /
--     schools / hospitals / hotels),
--   * building-density coverage (overture_buildings count + floors proxy),
--   * district radiance coverage (district_radiance_monthly).
--
-- HOW TO RUN (Codespace, iPad/Safari friendly — no \set prompts, no heredocs):
--   psql "$DATABASE_URL" -f scripts/diagnostics/l1_generator_coverage.sql
--
-- SAFETY / PORTABILITY NOTES:
--   * Pure read-only. The only writes are to a session TEMP TABLE that vanishes
--     at disconnect.
--   * Several source tables (planet_osm_point, planet_osm_polygon,
--     overture_buildings) are imported EXTERNALLY and may not exist in every
--     environment (see alembic/versions/0016_spatial_indexes_for_scoring_perf.py
--     :16-17). Each generator group is populated by an INDEPENDENT statement, so
--     if one source table/column is missing the matching column stays NULL and
--     the rest of the script still completes. Do NOT run with ON_ERROR_STOP=on.
--   * planet_osm_* are imported with osm2pgsql --latlong (.github/workflows/
--     osm-import.yml:270), so `way` is already SRID 4326. We still wrap with
--     ST_Transform(...,4326) defensively (mirrors restaurant_scoring_factors.py
--     :875,:905) so the probe is correct even if a region is re-imported in 3857.
--   * overture_buildings.geom is SRID 32638 (app/services/overture_buildings_
--     metrics.py:12,30-32); ST_Transform(...,4326) handles it.
-- ============================================================================

\timing on

-- ----------------------------------------------------------------------------
-- SECTION 0 — Preflight: which source tables exist + row counts.
-- to_regclass() returns NULL (no error) for a missing table.
-- ----------------------------------------------------------------------------
SELECT
    t.tbl AS source_table,
    to_regclass(t.tbl) IS NOT NULL AS exists
FROM (VALUES
    ('public.candidate_location'),
    ('public.commercial_unit'),
    ('public.population_density'),
    ('public.planet_osm_point'),
    ('public.planet_osm_polygon'),
    ('public.overture_buildings'),
    ('public.ms_buildings_raw'),
    ('public.district_radiance_monthly')
) AS t(tbl)
ORDER BY t.tbl;

-- ----------------------------------------------------------------------------
-- SECTION 1 — Sample ~300 candidates from the live universe and stage them.
-- The active retrieval path is candidate_location when >= 10 Tier-1 cluster
-- primaries exist (app/services/expansion_advisor.py:7107-7116). Scoring reads
-- ST_MakePoint(lon,lat) at 4326 (the _shortlist_coords pattern, :8227-8233),
-- so we sample lon/lat the same way. Spread across districts so the sample is
-- representative, not clustered in one neighborhood.
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS l1_cand;
CREATE TEMP TABLE l1_cand AS
WITH ranked AS (
    SELECT
        cl.id::text                              AS parcel_id,
        cl.lon::double precision                 AS lon,
        cl.lat::double precision                 AS lat,
        cl.district_ar                           AS district_ar,
        ROW_NUMBER() OVER (
            PARTITION BY cl.district_ar ORDER BY cl.id
        )                                        AS rn_in_district,
        ROW_NUMBER() OVER (ORDER BY cl.district_ar, cl.id) AS rn_global
    FROM candidate_location cl
    WHERE cl.is_cluster_primary = TRUE
      AND cl.source_tier = 1
      AND cl.geom IS NOT NULL
      AND cl.lat IS NOT NULL
      AND cl.lon IS NOT NULL
)
SELECT
    parcel_id,
    lon,
    lat,
    district_ar,
    -- metric columns, populated by the UPDATEs below; NULL = source unavailable
    NULL::double precision AS pop_reach,
    NULL::int AS n_office,
    NULL::int AS n_retail_mall,
    NULL::int AS n_transit,
    NULL::int AS n_mosque,
    NULL::int AS n_school,
    NULL::int AS n_hospital,
    NULL::int AS n_hotel,
    NULL::int AS n_building,
    NULL::double precision AS avg_floors,
    NULL::double precision AS radiance_yoy_pct
FROM ranked
-- Spread: take the first 4 per district first, then top up to ~300 globally.
WHERE rn_in_district <= 4 OR rn_global <= 300
ORDER BY district_ar, parcel_id
LIMIT 300;

SELECT COUNT(*) AS sampled_candidates,
       COUNT(DISTINCT district_ar) AS distinct_districts
FROM l1_cand;

-- ----------------------------------------------------------------------------
-- SECTION 2 — Population reach (3.5 km dine_in demand catchment).
-- Mirrors _bulk_enrich_population (app/services/expansion_advisor.py:6404-6413).
-- population_density has no geom by default (lat/lon numeric H3 cells,
-- app/models/tables.py:342-353), so build the point inline.
-- ----------------------------------------------------------------------------
UPDATE l1_cand c SET pop_reach = COALESCE((
    SELECT SUM(pd.population)
    FROM population_density pd
    WHERE pd.lat IS NOT NULL AND pd.lon IS NOT NULL
      AND ST_DWithin(
          ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography,
          ST_SetSRID(ST_MakePoint(pd.lon::double precision, pd.lat::double precision), 4326)::geography,
          3500.0
      )
), 0);

-- ----------------------------------------------------------------------------
-- SECTION 3 — OSM trip generators (3.5 km). One UNION over point+polygon, then
-- one UPDATE per generator type so a missing column aborts only that group.
-- Tag mapping uses the dedicated osm2pgsql default.style columns the repo
-- already relies on (amenity/shop/leisure/building — see expansion_advisor.py
-- :8449-8463) plus the standard office/tourism/railway columns.
-- public_transport is intentionally proxied via railway+bus_station only,
-- because public_transport lives in the hstore `tags` column (not a dedicated
-- column) and we avoid an hstore dependency in this probe.
-- ----------------------------------------------------------------------------

-- Offices
UPDATE l1_cand c SET n_office = COALESCE((
    SELECT COUNT(*) FROM (
        SELECT ST_Transform(way, 4326) AS g, office, building FROM planet_osm_point
        UNION ALL
        SELECT ST_Transform(way, 4326) AS g, office, building FROM planet_osm_polygon
    ) o
    WHERE (lower(COALESCE(o.office, '')) <> '' OR lower(COALESCE(o.building, '')) IN ('office','commercial'))
      AND ST_DWithin(o.g::geography,
            ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography, 3500.0)
), 0);

-- Malls / retail anchors
UPDATE l1_cand c SET n_retail_mall = COALESCE((
    SELECT COUNT(*) FROM (
        SELECT ST_Transform(way, 4326) AS g, shop, amenity FROM planet_osm_point
        UNION ALL
        SELECT ST_Transform(way, 4326) AS g, shop, amenity FROM planet_osm_polygon
    ) o
    WHERE (lower(COALESCE(o.shop, '')) IN ('mall','supermarket','department_store','wholesale')
           OR lower(COALESCE(o.amenity, '')) = 'marketplace')
      AND ST_DWithin(o.g::geography,
            ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography, 3500.0)
), 0);

-- Transit (rail stations + bus stations; see note above re public_transport)
UPDATE l1_cand c SET n_transit = COALESCE((
    SELECT COUNT(*) FROM (
        SELECT ST_Transform(way, 4326) AS g, railway, amenity FROM planet_osm_point
        UNION ALL
        SELECT ST_Transform(way, 4326) AS g, railway, amenity FROM planet_osm_polygon
    ) o
    WHERE (lower(COALESCE(o.railway, '')) IN ('station','halt','tram_stop','subway_entrance','stop')
           OR lower(COALESCE(o.amenity, '')) IN ('bus_station'))
      AND ST_DWithin(o.g::geography,
            ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography, 3500.0)
), 0);

-- Mosques / places of worship
UPDATE l1_cand c SET n_mosque = COALESCE((
    SELECT COUNT(*) FROM (
        SELECT ST_Transform(way, 4326) AS g, amenity, building FROM planet_osm_point
        UNION ALL
        SELECT ST_Transform(way, 4326) AS g, amenity, building FROM planet_osm_polygon
    ) o
    WHERE (lower(COALESCE(o.amenity, '')) IN ('place_of_worship','mosque')
           OR lower(COALESCE(o.building, '')) = 'mosque')
      AND ST_DWithin(o.g::geography,
            ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography, 3500.0)
), 0);

-- Schools / universities
UPDATE l1_cand c SET n_school = COALESCE((
    SELECT COUNT(*) FROM (
        SELECT ST_Transform(way, 4326) AS g, amenity, building FROM planet_osm_point
        UNION ALL
        SELECT ST_Transform(way, 4326) AS g, amenity, building FROM planet_osm_polygon
    ) o
    WHERE (lower(COALESCE(o.amenity, '')) IN ('school','college','university','kindergarten')
           OR lower(COALESCE(o.building, '')) IN ('school','university'))
      AND ST_DWithin(o.g::geography,
            ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography, 3500.0)
), 0);

-- Hospitals / clinics
UPDATE l1_cand c SET n_hospital = COALESCE((
    SELECT COUNT(*) FROM (
        SELECT ST_Transform(way, 4326) AS g, amenity FROM planet_osm_point
        UNION ALL
        SELECT ST_Transform(way, 4326) AS g, amenity FROM planet_osm_polygon
    ) o
    WHERE lower(COALESCE(o.amenity, '')) IN ('hospital','clinic','doctors')
      AND ST_DWithin(o.g::geography,
            ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography, 3500.0)
), 0);

-- Hotels
UPDATE l1_cand c SET n_hotel = COALESCE((
    SELECT COUNT(*) FROM (
        SELECT ST_Transform(way, 4326) AS g, tourism, building FROM planet_osm_point
        UNION ALL
        SELECT ST_Transform(way, 4326) AS g, tourism, building FROM planet_osm_polygon
    ) o
    WHERE (lower(COALESCE(o.tourism, '')) IN ('hotel','motel','hostel','guest_house')
           OR lower(COALESCE(o.building, '')) = 'hotel')
      AND ST_DWithin(o.g::geography,
            ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography, 3500.0)
), 0);

-- ----------------------------------------------------------------------------
-- SECTION 4 — Building-floor density (overture_buildings, 3.5 km).
-- floors proxy mirrors overture_buildings_metrics.py:43-46.
-- ----------------------------------------------------------------------------
UPDATE l1_cand c SET
    n_building = sub.cnt,
    avg_floors = sub.avg_floors
FROM (
    SELECT c2.parcel_id,
           COUNT(b.*) AS cnt,
           AVG(CASE
                 WHEN b.num_floors IS NOT NULL AND b.num_floors > 0 THEN LEAST(60, GREATEST(1, b.num_floors))
                 WHEN b.height IS NOT NULL AND b.height > 0 THEN LEAST(60, GREATEST(1, round(b.height / 3.2)))
                 ELSE NULL
               END) AS avg_floors
    FROM l1_cand c2
    LEFT JOIN overture_buildings b
      ON ST_DWithin(
            ST_Transform(b.geom, 4326)::geography,
            ST_SetSRID(ST_MakePoint(c2.lon, c2.lat), 4326)::geography,
            3500.0)
    GROUP BY c2.parcel_id
) sub
WHERE sub.parcel_id = c.parcel_id;

-- ----------------------------------------------------------------------------
-- SECTION 5 — District radiance coverage (district_radiance_monthly).
-- The radiance table is keyed by a NORMALIZED district_key (no geom,
-- alembic/versions/20260501_add_district_radiance_monthly.py:20-40), while
-- candidate_location carries district_ar. There is no canonical normalizer in
-- pure SQL, so we approximate the join with lower(trim()) and FLAG mismatch as
-- a verification item. We only need to know whether radiance has rows for the
-- sampled districts.
-- ----------------------------------------------------------------------------
UPDATE l1_cand c SET radiance_yoy_pct = sub.yoy
FROM (
    SELECT c2.parcel_id,
           -- latest two rolling values per district (most recent source)
           NULL::double precision AS yoy  -- placeholder; see coverage query below
    FROM l1_cand c2
) sub
WHERE sub.parcel_id = c.parcel_id;

-- Radiance district-match coverage (does the sampled district have any radiance
-- rows at all, under a naive lower(trim) match?).
SELECT
    COUNT(DISTINCT c.district_ar)                                  AS sample_districts,
    COUNT(DISTINCT c.district_ar) FILTER (
        WHERE EXISTS (
            SELECT 1 FROM district_radiance_monthly r
            WHERE lower(trim(r.district_key)) = lower(trim(c.district_ar))
        )
    )                                                              AS districts_with_radiance_naivematch
FROM l1_cand c;

-- Raw radiance availability (sanity): distinct districts + latest month present.
SELECT COUNT(DISTINCT district_key) AS radiance_districts,
       MAX(year_month)              AS latest_year_month,
       COUNT(*)                     AS radiance_rows
FROM district_radiance_monthly;

-- ----------------------------------------------------------------------------
-- SECTION 6 — COVERAGE ROLL-UP (the go/no-go answer).
-- For each generator: how many of the N sampled candidates have >= 1, plus the
-- spread (avg / p50 / p90 / max). If coverage is high AND the distribution is
-- wide, the index has cross-candidate signal and L1 is buildable.
-- ----------------------------------------------------------------------------
SELECT 'population_reach' AS metric,
       COUNT(*)                                              AS n,
       COUNT(*) FILTER (WHERE pop_reach > 0)                 AS n_nonzero,
       round(AVG(pop_reach)::numeric, 1)                     AS avg,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY pop_reach))::numeric, 1) AS p50,
       round((percentile_cont(0.9) WITHIN GROUP (ORDER BY pop_reach))::numeric, 1) AS p90,
       round(MAX(pop_reach)::numeric, 1)                     AS max
FROM l1_cand
UNION ALL SELECT 'osm_office',        COUNT(*), COUNT(*) FILTER (WHERE n_office > 0),      round(AVG(n_office),2),      percentile_cont(0.5) WITHIN GROUP (ORDER BY n_office),      percentile_cont(0.9) WITHIN GROUP (ORDER BY n_office),      MAX(n_office)      FROM l1_cand
UNION ALL SELECT 'osm_retail_mall',   COUNT(*), COUNT(*) FILTER (WHERE n_retail_mall > 0), round(AVG(n_retail_mall),2), percentile_cont(0.5) WITHIN GROUP (ORDER BY n_retail_mall), percentile_cont(0.9) WITHIN GROUP (ORDER BY n_retail_mall), MAX(n_retail_mall) FROM l1_cand
UNION ALL SELECT 'osm_transit',       COUNT(*), COUNT(*) FILTER (WHERE n_transit > 0),     round(AVG(n_transit),2),     percentile_cont(0.5) WITHIN GROUP (ORDER BY n_transit),     percentile_cont(0.9) WITHIN GROUP (ORDER BY n_transit),     MAX(n_transit)     FROM l1_cand
UNION ALL SELECT 'osm_mosque',        COUNT(*), COUNT(*) FILTER (WHERE n_mosque > 0),      round(AVG(n_mosque),2),      percentile_cont(0.5) WITHIN GROUP (ORDER BY n_mosque),      percentile_cont(0.9) WITHIN GROUP (ORDER BY n_mosque),      MAX(n_mosque)      FROM l1_cand
UNION ALL SELECT 'osm_school',        COUNT(*), COUNT(*) FILTER (WHERE n_school > 0),      round(AVG(n_school),2),      percentile_cont(0.5) WITHIN GROUP (ORDER BY n_school),      percentile_cont(0.9) WITHIN GROUP (ORDER BY n_school),      MAX(n_school)      FROM l1_cand
UNION ALL SELECT 'osm_hospital',      COUNT(*), COUNT(*) FILTER (WHERE n_hospital > 0),    round(AVG(n_hospital),2),    percentile_cont(0.5) WITHIN GROUP (ORDER BY n_hospital),    percentile_cont(0.9) WITHIN GROUP (ORDER BY n_hospital),    MAX(n_hospital)    FROM l1_cand
UNION ALL SELECT 'osm_hotel',         COUNT(*), COUNT(*) FILTER (WHERE n_hotel > 0),       round(AVG(n_hotel),2),       percentile_cont(0.5) WITHIN GROUP (ORDER BY n_hotel),       percentile_cont(0.9) WITHIN GROUP (ORDER BY n_hotel),       MAX(n_hotel)       FROM l1_cand
UNION ALL SELECT 'overture_buildings',COUNT(*), COUNT(*) FILTER (WHERE n_building > 0),    round(AVG(n_building),2),    percentile_cont(0.5) WITHIN GROUP (ORDER BY n_building),    percentile_cont(0.9) WITHIN GROUP (ORDER BY n_building),    MAX(n_building)    FROM l1_cand;

-- Building floors spread (only where buildings were found).
SELECT 'overture_floors_proxy' AS metric,
       COUNT(*) FILTER (WHERE avg_floors IS NOT NULL) AS n_with_floors,
       round(AVG(avg_floors)::numeric, 2)             AS avg_floors,
       round(MAX(avg_floors)::numeric, 2)             AS max_floors
FROM l1_cand;

-- Per-candidate preview (first 25) so you can eyeball that values vary.
SELECT parcel_id, district_ar, pop_reach, n_office, n_retail_mall, n_transit,
       n_mosque, n_school, n_hospital, n_hotel, n_building, avg_floors
FROM l1_cand
ORDER BY pop_reach DESC NULLS LAST
LIMIT 25;

-- Cleanup (TEMP table is dropped at disconnect anyway).
DROP TABLE IF EXISTS l1_cand;
