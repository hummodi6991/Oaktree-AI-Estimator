-- ============================================================================
-- l1_signal_distributions.sql  (PR-1a, Phase A — distribution probe)
-- ----------------------------------------------------------------------------
-- The v1 L1 demand-generator index pinned at the ceiling: every normalization
-- reference sat far below real Riyadh catchment values, so all four sub-scores
-- saturated and the composite barely varied. Before re-anchoring (Phase B) we
-- measure the REAL city-wide distribution of every raw sub-signal so the anchors
-- are set empirically instead of guessed.
--
-- This probe samples Tier-1 cluster-primary candidate_location rows city-wide and,
-- reusing the SAME ST_MakePoint(lon, lat) catchment logic as the enrich blocks in
-- app/services/expansion_advisor.py (the "L1 demand-generator index enrichment"
-- section), reports for each raw sub-signal:
--     p5 / p25 / p50 / p75 / p90 / p95 / p99 / max  and  the count of zeros.
--
-- Signals (mirroring _demand_generator_index inputs):
--   * fnb_review_weighted  — Σ review_count over open F&B venues in radius
--   * building_floors_sum   — Overture building floor-equivalent sum in radius
--   * osm_weighted_total    — Σ(osm_count · weight) using _DEMAND_GENERATOR_OSM_WEIGHTS
--                             (offices 2.0, malls_retail 4.0, transit 2.0, mosques 1.5,
--                              schools 1.75, hospitals 2.0, hotels 2.5) — this is the
--                             exact quantity the OSM sub-score normalizes, so its
--                             percentiles drop straight into the anchor block.
--   * each osm_* generator  — raw per-bucket counts (offices, malls_retail, ...)
--   * pop_reach @ 1000 / 1500 / 3500 m — to see whether a tighter population
--                             catchment actually discriminates (it is near-constant
--                             at 3500 m by construction in dense Riyadh).
--
-- HOW TO RUN (iPad/Safari friendly — psql -f safe, no \set, no heredocs):
--   psql "$DATABASE_URL" -f scripts/diagnostics/l1_signal_distributions.sql
--
-- NOTES
--   * Sampled (LIMIT 1500 candidates) so it runs fast; raise/lower the LIMIT in
--     the `sample` CTE if you want a wider/narrower probe.
--   * F&B filter uses the default dine-in 4-key category set
--     (burger/pizza/chicken/fast_food). If you validate a NARROWER category, the
--     live fnb_review_weighted will be a fraction of these numbers — adjust the
--     fnb p5/p95 anchors down accordingly.
--   * The radius for the OSM / floors / F&B metrics is the 3500 m demand radius
--     (EXPANSION_DEMAND_GENERATOR_RADIUS_M); population is probed at three radii.
-- ============================================================================

\timing on

-- ── Sample of Tier-1 cluster-primary candidates, city-wide ──
DROP TABLE IF EXISTS l1_sample;
CREATE TEMP TABLE l1_sample AS
SELECT
    COALESCE(cl.source_id, cl.id::text) AS parcel_id,
    cl.lon::double precision            AS lon,
    cl.lat::double precision            AS lat
FROM candidate_location cl
WHERE cl.is_cluster_primary = TRUE
  AND cl.source_tier = 1
  AND cl.geom IS NOT NULL
  AND cl.lon IS NOT NULL
  AND cl.lat IS NOT NULL
ORDER BY cl.id
LIMIT 1500;

SELECT COUNT(*) AS sampled_candidates FROM l1_sample;

-- ── Per-candidate raw sub-signals (one bulk pass, LATERAL per source) ──
DROP TABLE IF EXISTS l1_metrics;
CREATE TEMP TABLE l1_metrics AS
SELECT
    s.parcel_id,
    -- OSM trip generators (planet_osm_point ∪ planet_osm_polygon), 3500 m.
    COALESCE(osm.offices, 0)       AS osm_offices,
    COALESCE(osm.malls_retail, 0)  AS osm_malls_retail,
    COALESCE(osm.transit, 0)       AS osm_transit,
    COALESCE(osm.mosques, 0)       AS osm_mosques,
    COALESCE(osm.schools, 0)       AS osm_schools,
    COALESCE(osm.hospitals, 0)     AS osm_hospitals,
    COALESCE(osm.hotels, 0)        AS osm_hotels,
    -- Weighted total = exact quantity the OSM sub-score normalizes.
    ( 2.00 * COALESCE(osm.offices, 0)
    + 4.00 * COALESCE(osm.malls_retail, 0)
    + 2.00 * COALESCE(osm.transit, 0)
    + 1.50 * COALESCE(osm.mosques, 0)
    + 1.75 * COALESCE(osm.schools, 0)
    + 2.00 * COALESCE(osm.hospitals, 0)
    + 2.50 * COALESCE(osm.hotels, 0) )           AS osm_weighted_total,
    COALESCE(fl.floors_sum, 0)     AS building_floors_sum,
    COALESCE(fnb.review_weighted, 0) AS fnb_review_weighted,
    COALESCE(fnb.venue_count, 0)   AS fnb_venue_count,
    COALESCE(p1000.pop, 0)         AS pop_reach_1000,
    COALESCE(p1500.pop, 0)         AS pop_reach_1500,
    COALESCE(p3500.pop, 0)         AS pop_reach_3500
FROM l1_sample s
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE g.kind = 'offices')      AS offices,
        COUNT(*) FILTER (WHERE g.kind = 'malls_retail') AS malls_retail,
        COUNT(*) FILTER (WHERE g.kind = 'transit')      AS transit,
        COUNT(*) FILTER (WHERE g.kind = 'mosques')      AS mosques,
        COUNT(*) FILTER (WHERE g.kind = 'schools')      AS schools,
        COUNT(*) FILTER (WHERE g.kind = 'hospitals')    AS hospitals,
        COUNT(*) FILTER (WHERE g.kind = 'hotels')       AS hotels
    FROM (
        SELECT
            CASE
              WHEN (lower(COALESCE(office,'')) <> '' OR lower(COALESCE(building,'')) IN ('office','commercial')) THEN 'offices'
              WHEN (lower(COALESCE(shop,'')) IN ('mall','supermarket','department_store','wholesale') OR lower(COALESCE(amenity,'')) = 'marketplace') THEN 'malls_retail'
              WHEN (lower(COALESCE(railway,'')) IN ('station','halt','tram_stop','subway_entrance','stop') OR lower(COALESCE(amenity,'')) IN ('bus_station')) THEN 'transit'
              WHEN (lower(COALESCE(amenity,'')) IN ('place_of_worship','mosque') OR lower(COALESCE(building,'')) = 'mosque') THEN 'mosques'
              WHEN (lower(COALESCE(amenity,'')) IN ('school','college','university','kindergarten') OR lower(COALESCE(building,'')) IN ('school','university')) THEN 'schools'
              WHEN (lower(COALESCE(amenity,'')) IN ('hospital','clinic','doctors')) THEN 'hospitals'
              WHEN (lower(COALESCE(tourism,'')) IN ('hotel','motel','hostel','guest_house') OR lower(COALESCE(building,'')) = 'hotel') THEN 'hotels'
              ELSE NULL
            END AS kind
        FROM planet_osm_point p
        WHERE p.way IS NOT NULL
          AND ST_DWithin(ST_Transform(p.way, 4326)::geography,
                         ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography, 3500)
        UNION ALL
        SELECT
            CASE
              WHEN (lower(COALESCE(office,'')) <> '' OR lower(COALESCE(building,'')) IN ('office','commercial')) THEN 'offices'
              WHEN (lower(COALESCE(shop,'')) IN ('mall','supermarket','department_store','wholesale') OR lower(COALESCE(amenity,'')) = 'marketplace') THEN 'malls_retail'
              WHEN (lower(COALESCE(railway,'')) IN ('station','halt','tram_stop','subway_entrance','stop') OR lower(COALESCE(amenity,'')) IN ('bus_station')) THEN 'transit'
              WHEN (lower(COALESCE(amenity,'')) IN ('place_of_worship','mosque') OR lower(COALESCE(building,'')) = 'mosque') THEN 'mosques'
              WHEN (lower(COALESCE(amenity,'')) IN ('school','college','university','kindergarten') OR lower(COALESCE(building,'')) IN ('school','university')) THEN 'schools'
              WHEN (lower(COALESCE(amenity,'')) IN ('hospital','clinic','doctors')) THEN 'hospitals'
              WHEN (lower(COALESCE(tourism,'')) IN ('hotel','motel','hostel','guest_house') OR lower(COALESCE(building,'')) = 'hotel') THEN 'hotels'
              ELSE NULL
            END AS kind
        FROM planet_osm_polygon pg
        WHERE pg.way IS NOT NULL
          AND ST_DWithin(ST_Transform(pg.way, 4326)::geography,
                         ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography, 3500)
    ) g
    WHERE g.kind IS NOT NULL
) osm ON TRUE
LEFT JOIN LATERAL (
    SELECT SUM(
        CASE
          WHEN o.num_floors IS NOT NULL AND o.num_floors > 0 THEN LEAST(60, GREATEST(1, round(o.num_floors)::int))
          WHEN o.height IS NOT NULL AND o.height > 0 THEN LEAST(60, GREATEST(1, round(o.height / 3.2)::int))
          ELSE 1
        END
    ) AS floors_sum
    FROM overture_buildings o
    WHERE o.geom IS NOT NULL
      AND ST_DWithin(ST_Transform(o.geom, 4326)::geography,
                     ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography, 3500)
) fl ON TRUE
LEFT JOIN LATERAL (
    SELECT SUM(COALESCE(rp.review_count, 0)) AS review_weighted,
           COUNT(*) AS venue_count
    FROM restaurant_poi rp
    WHERE rp.geom IS NOT NULL
      AND rp.business_status IS DISTINCT FROM 'CLOSED_PERMANENTLY'
      AND lower(rp.category) = ANY(ARRAY['burger','pizza','chicken','fast_food'])
      AND ST_DWithin(rp.geom::geography,
                     ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography, 3500)
) fnb ON TRUE
LEFT JOIN LATERAL (
    SELECT COALESCE(SUM(pd.population), 0) AS pop
    FROM population_density pd
    WHERE pd.geom IS NOT NULL
      AND ST_DWithin(pd.geom::geography,
                     ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography, 1000)
) p1000 ON TRUE
LEFT JOIN LATERAL (
    SELECT COALESCE(SUM(pd.population), 0) AS pop
    FROM population_density pd
    WHERE pd.geom IS NOT NULL
      AND ST_DWithin(pd.geom::geography,
                     ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography, 1500)
) p1500 ON TRUE
LEFT JOIN LATERAL (
    SELECT COALESCE(SUM(pd.population), 0) AS pop
    FROM population_density pd
    WHERE pd.geom IS NOT NULL
      AND ST_DWithin(pd.geom::geography,
                     ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography, 3500)
) p3500 ON TRUE;

-- NOTE: the population LATERALs above assume population_density has a `geom`
-- column. If your deployment stores lat/lon instead, swap the predicate to
--   pd.lat IS NOT NULL AND pd.lon IS NOT NULL
--   AND ST_DWithin(ST_SetSRID(ST_MakePoint(pd.lon::double precision, pd.lat::double precision),4326)::geography, ..., radius)
-- (this mirrors the runtime fallback in _bulk_enrich_population).

-- ── Percentiles per raw sub-signal (the numbers that set the anchors) ──
SELECT 'fnb_review_weighted' AS signal,
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p5,
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p25,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p50,
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p75,
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p90,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p95,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p99,
       round(MAX(fnb_review_weighted)::numeric, 1) AS max,
       COUNT(*) FILTER (WHERE fnb_review_weighted = 0) AS n_zero
FROM l1_metrics
UNION ALL
SELECT 'building_floors_sum',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(MAX(building_floors_sum)::numeric, 1),
       COUNT(*) FILTER (WHERE building_floors_sum = 0)
FROM l1_metrics
UNION ALL
SELECT 'osm_weighted_total',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(MAX(osm_weighted_total)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_weighted_total = 0)
FROM l1_metrics
UNION ALL
SELECT 'osm_offices',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_offices)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY osm_offices)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_offices)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY osm_offices)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY osm_offices)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_offices)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_offices)::numeric, 1),
       round(MAX(osm_offices)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_offices = 0)
FROM l1_metrics
UNION ALL
SELECT 'osm_malls_retail',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_malls_retail)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY osm_malls_retail)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_malls_retail)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY osm_malls_retail)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY osm_malls_retail)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_malls_retail)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_malls_retail)::numeric, 1),
       round(MAX(osm_malls_retail)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_malls_retail = 0)
FROM l1_metrics
UNION ALL
SELECT 'osm_transit',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_transit)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY osm_transit)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_transit)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY osm_transit)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY osm_transit)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_transit)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_transit)::numeric, 1),
       round(MAX(osm_transit)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_transit = 0)
FROM l1_metrics
UNION ALL
SELECT 'osm_mosques',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_mosques)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY osm_mosques)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_mosques)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY osm_mosques)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY osm_mosques)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_mosques)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_mosques)::numeric, 1),
       round(MAX(osm_mosques)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_mosques = 0)
FROM l1_metrics
UNION ALL
SELECT 'osm_schools',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_schools)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY osm_schools)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_schools)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY osm_schools)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY osm_schools)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_schools)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_schools)::numeric, 1),
       round(MAX(osm_schools)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_schools = 0)
FROM l1_metrics
UNION ALL
SELECT 'osm_hospitals',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_hospitals)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY osm_hospitals)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_hospitals)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY osm_hospitals)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY osm_hospitals)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_hospitals)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_hospitals)::numeric, 1),
       round(MAX(osm_hospitals)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_hospitals = 0)
FROM l1_metrics
UNION ALL
SELECT 'osm_hotels',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_hotels)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY osm_hotels)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_hotels)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY osm_hotels)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY osm_hotels)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_hotels)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_hotels)::numeric, 1),
       round(MAX(osm_hotels)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_hotels = 0)
FROM l1_metrics
UNION ALL
SELECT 'pop_reach_1000',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(MAX(pop_reach_1000)::numeric, 1),
       COUNT(*) FILTER (WHERE pop_reach_1000 = 0)
FROM l1_metrics
UNION ALL
SELECT 'pop_reach_1500',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(MAX(pop_reach_1500)::numeric, 1),
       COUNT(*) FILTER (WHERE pop_reach_1500 = 0)
FROM l1_metrics
UNION ALL
SELECT 'pop_reach_3500',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY pop_reach_3500)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY pop_reach_3500)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY pop_reach_3500)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY pop_reach_3500)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY pop_reach_3500)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY pop_reach_3500)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY pop_reach_3500)::numeric, 1),
       round(MAX(pop_reach_3500)::numeric, 1),
       COUNT(*) FILTER (WHERE pop_reach_3500 = 0)
FROM l1_metrics;

-- ── Population radius discrimination: spread ratio (p95/p50) by radius ──
-- A higher ratio ⇒ the radius discriminates more. Expect 3500 m near 1.0
-- (near-constant) and the tighter radii materially above it.
SELECT
    round((percentile_cont(0.95) WITHIN GROUP (ORDER BY pop_reach_1000)
         / NULLIF(percentile_cont(0.50) WITHIN GROUP (ORDER BY pop_reach_1000), 0))::numeric, 3) AS spread_ratio_1000,
    round((percentile_cont(0.95) WITHIN GROUP (ORDER BY pop_reach_1500)
         / NULLIF(percentile_cont(0.50) WITHIN GROUP (ORDER BY pop_reach_1500), 0))::numeric, 3) AS spread_ratio_1500,
    round((percentile_cont(0.95) WITHIN GROUP (ORDER BY pop_reach_3500)
         / NULLIF(percentile_cont(0.50) WITHIN GROUP (ORDER BY pop_reach_3500), 0))::numeric, 3) AS spread_ratio_3500,
    round(STDDEV_POP(pop_reach_1500)::numeric, 1) AS stddev_1500,
    round(STDDEV_POP(pop_reach_3500)::numeric, 1) AS stddev_3500
FROM l1_metrics;

DROP TABLE IF EXISTS l1_metrics;
DROP TABLE IF EXISTS l1_sample;
