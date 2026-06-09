-- ============================================================================
-- qsr_signal_distributions.sql  (QSR PR-1a, Phase A — distribution probe)
-- ----------------------------------------------------------------------------
-- QSR ANALOGUE of scripts/diagnostics/l1_signal_distributions.sql (the dine-in
-- Phase-A probe that produced the dine-in `l1_v2` anchors). This probe gathers
-- the SAME four L1 demand-generator sub-signals — but at the QSR catchment
-- radii — so a later WIRING PR can set QSR-specific normalization anchors
-- (`l1_v3`). THIS PROBE WIRES NOTHING. It only measures distributions.
--
-- Why a separate probe: the index machinery (_demand_generator_index) is shared,
-- but QSR uses a TIGHTER catchment than dine-in:
--     _CATCHMENT_RADII_M['qsr'] = demand 1500 / competition 1200 / provider 1500
--     (app/services/expansion_advisor.py:830)
-- vs dine-in's demand 3500 (app/services/expansion_advisor.py:822). Reusing the
-- dine-in `l1_v2` anchors (which were read at 3500 m) on QSR's 1500 m counts
-- mis-maps every sub-signal: a 1500 m catchment holds a fraction of the offices,
-- buildings, F&B reviews and population of a 3500 m one, so the dine-in p5/p95
-- band sits far above the QSR distribution and every QSR sub-score floors low.
--
-- This probe reports, for each raw sub-signal, in the SAME units the index
-- normalizes in (raw value PRE-transform — so the anchors read straight off):
--     min / p5 / p25 / p50 / p75 / p90 / p95 / p99 / max  + mean + stddev + n_zero
-- and notes the transform each signal uses in the index (log vs linear).
--
-- Signals (mirroring _demand_generator_index inputs; see expansion_advisor.py
-- "L1 demand-generator index enrichment" block, lines ~8769-9007):
--   * fnb_review_weighted   — Σ review_count over open, in-category F&B venues
--                             in radius.            INDEX TRANSFORM: log
--   * building_floors_sum    — Overture building floor-equivalent sum in radius.
--                                                   INDEX TRANSFORM: log
--   * osm_weighted_total     — Σ(osm_count · weight) using _DEMAND_GENERATOR_OSM_WEIGHTS
--                              (offices 2.0, malls_retail 4.0, transit 2.0,
--                               mosques 1.5, schools 1.75, hospitals 2.0,
--                               hotels 2.5). Exact quantity the OSM sub-score
--                              normalizes.          INDEX TRANSFORM: log
--   * each osm_* generator   — raw per-bucket counts (offices, malls_retail, ...)
--   * population_local       — Σ population in the pop sub-term radius.
--                                                   INDEX TRANSFORM: linear
--
-- RADII USED (literals below mirror the live constants — keep in sync):
--   * QSR demand radius = 1500 m  (_CATCHMENT_RADII_M['qsr']['demand'],
--     expansion_advisor.py:830) — used for OSM / floors / F&B.
--   * Population probed at 1000 / 1200 / 1500 m so the wiring PR can pick the
--     QSR pop sub-term radius. The live pop radius is currently FLAT 1500 m
--     (EXPANSION_DEMAND_GENERATOR_POP_RADIUS_M, app/core/config.py:146 — NOT
--     service-model-aware). The QSR investigation flagged QSR may want tighter
--     than dine-in's 1500 m; the spread_ratio block at the end informs that —
--     it is RECORDED here, not changed.
--
-- HOW TO RUN (iPad/Safari friendly — psql -f safe, no \set, no heredocs):
--   psql "$DATABASE_URL" -f scripts/diagnostics/qsr_signal_distributions.sql
--
-- METHODOLOGY CAVEATS (read before reading anchors off the output):
--   * CITY-WIDE CANDIDATE UNIVERSE. Candidate_location storage is geographic
--     (parcels / cluster primaries), NOT service-model-specific, so the SAME
--     Tier-1 cluster-primary universe the dine-in probe used (538 rows) is the
--     correct city-wide set for QSR. We do NOT filter to one QSR search's
--     shortlist — a single QSR search (~15 candidates) is far too small for
--     stable p5/p95/p99. The LIMIT 1500 in the `qsr_sample` CTE matches the
--     dine-in probe's scale; lower it only to run faster.
--   * F&B CATEGORY SET. The live index filters F&B by the SEARCH's category
--     keys (_cat_expanded["keys"], expansion_advisor.py:8914), not by service
--     model. This probe uses the default fast-food 4-key set
--     ('burger','pizza','chicken','fast_food') — the QSR-shaped default and the
--     same set the dine-in probe used — so the two runs are comparable. If a
--     specific QSR search validates a NARROWER category, the live
--     fnb_review_weighted will be a FRACTION of these numbers — scale the fnb
--     p5/p95 anchors down accordingly (same caveat the dine-in probe carried).
--   * F&B SOURCE. The live L1 F&B enrich (expansion_advisor.py:8910-8953) reads
--     ONLY restaurant_poi — it does NOT union delivery_source_record (that UNION
--     belongs to the delivery-competitor path, not the index numerator). This
--     probe mirrors the index and uses restaurant_poi only, so the percentiles
--     map straight onto the live fnb sub-signal.
--   * OSM simplified category match (dedicated columns only; public_transport in
--     hstore `tags` is excluded, mirroring the runtime TODO at
--     expansion_advisor.py:8800-8802) — transit is mildly under-counted, same as
--     the live enrich and the dine-in probe, so it stays comparable.
--   * geom assumptions mirror the runtime: planet_osm_* via ST_Transform(way,4326),
--     overture_buildings via ST_Transform(geom,4326), restaurant_poi.geom is 4326,
--     population_density supports geom OR lat/lon (handled below, mirroring
--     _bulk_enrich_population / the runtime pop fallback).
-- ============================================================================

\timing on

-- ── Sample of Tier-1 cluster-primary candidates, city-wide ──
-- (Identical universe to the dine-in Phase-A probe so QSR vs dine-in anchor
--  differences are attributable to RADIUS, not to a different candidate set.)
DROP TABLE IF EXISTS qsr_sample;
CREATE TEMP TABLE qsr_sample AS
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

SELECT COUNT(*) AS sampled_candidates FROM qsr_sample;

-- ── Per-candidate raw sub-signals at the QSR radii (one bulk pass) ──
-- OSM / floors / F&B at the QSR DEMAND radius = 1500 m.
-- Population at 1000 / 1200 / 1500 m (pop sub-term radius candidates).
DROP TABLE IF EXISTS qsr_metrics;
CREATE TEMP TABLE qsr_metrics AS
SELECT
    s.parcel_id,
    -- OSM trip generators (planet_osm_point ∪ planet_osm_polygon), 1500 m.
    COALESCE(osm.offices, 0)       AS osm_offices,
    COALESCE(osm.malls_retail, 0)  AS osm_malls_retail,
    COALESCE(osm.transit, 0)       AS osm_transit,
    COALESCE(osm.mosques, 0)       AS osm_mosques,
    COALESCE(osm.schools, 0)       AS osm_schools,
    COALESCE(osm.hospitals, 0)     AS osm_hospitals,
    COALESCE(osm.hotels, 0)        AS osm_hotels,
    -- Weighted total = exact quantity the OSM sub-score normalizes (log in index).
    ( 2.00 * COALESCE(osm.offices, 0)
    + 4.00 * COALESCE(osm.malls_retail, 0)
    + 2.00 * COALESCE(osm.transit, 0)
    + 1.50 * COALESCE(osm.mosques, 0)
    + 1.75 * COALESCE(osm.schools, 0)
    + 2.00 * COALESCE(osm.hospitals, 0)
    + 2.50 * COALESCE(osm.hotels, 0) )           AS osm_weighted_total,
    COALESCE(fl.floors_sum, 0)       AS building_floors_sum,
    COALESCE(fnb.review_weighted, 0) AS fnb_review_weighted,
    COALESCE(fnb.venue_count, 0)     AS fnb_venue_count,
    COALESCE(p1000.pop, 0)           AS pop_reach_1000,
    COALESCE(p1200.pop, 0)           AS pop_reach_1200,
    COALESCE(p1500.pop, 0)           AS pop_reach_1500
FROM qsr_sample s
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
                         ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography, 1500)
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
                         ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography, 1500)
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
                     ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography, 1500)
) fl ON TRUE
LEFT JOIN LATERAL (
    SELECT SUM(COALESCE(rp.review_count, 0)) AS review_weighted,
           COUNT(*) AS venue_count
    FROM restaurant_poi rp
    WHERE rp.geom IS NOT NULL
      AND rp.business_status IS DISTINCT FROM 'CLOSED_PERMANENTLY'
      AND lower(rp.category) = ANY(ARRAY['burger','pizza','chicken','fast_food'])
      AND ST_DWithin(rp.geom::geography,
                     ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography, 1500)
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
                     ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography, 1200)
) p1200 ON TRUE
LEFT JOIN LATERAL (
    SELECT COALESCE(SUM(pd.population), 0) AS pop
    FROM population_density pd
    WHERE pd.geom IS NOT NULL
      AND ST_DWithin(pd.geom::geography,
                     ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography, 1500)
) p1500 ON TRUE;

-- NOTE: the population LATERALs above assume population_density has a `geom`
-- column. If your deployment stores lat/lon instead, swap the predicate to
--   pd.lat IS NOT NULL AND pd.lon IS NOT NULL
--   AND ST_DWithin(ST_SetSRID(ST_MakePoint(pd.lon::double precision, pd.lat::double precision),4326)::geography, ..., radius)
-- (this mirrors the runtime fallback in _bulk_enrich_population and the L1 pop
--  enrich at expansion_advisor.py:8973-8978).

-- ── Percentiles per raw sub-signal (the numbers that set the QSR anchors) ──
-- Reported in raw (PRE-transform) units. The wiring PR reads p5 → low anchor,
-- p95 → high anchor, p99 → winsor cap, then applies the index transform noted
-- in `index_transform` below.
SELECT 'fnb_review_weighted' AS signal, 'log' AS index_transform,
       round(MIN(fnb_review_weighted)::numeric, 1) AS min,
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p5,
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p25,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p50,
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p75,
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p90,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p95,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY fnb_review_weighted)::numeric, 1) AS p99,
       round(MAX(fnb_review_weighted)::numeric, 1) AS max,
       round(AVG(fnb_review_weighted)::numeric, 1) AS mean,
       round(STDDEV_POP(fnb_review_weighted)::numeric, 1) AS stddev,
       COUNT(*) FILTER (WHERE fnb_review_weighted = 0) AS n_zero
FROM qsr_metrics
UNION ALL
SELECT 'building_floors_sum', 'log',
       round(MIN(building_floors_sum)::numeric, 1),
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY building_floors_sum)::numeric, 1),
       round(MAX(building_floors_sum)::numeric, 1),
       round(AVG(building_floors_sum)::numeric, 1),
       round(STDDEV_POP(building_floors_sum)::numeric, 1),
       COUNT(*) FILTER (WHERE building_floors_sum = 0)
FROM qsr_metrics
UNION ALL
SELECT 'osm_weighted_total', 'log',
       round(MIN(osm_weighted_total)::numeric, 1),
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_weighted_total)::numeric, 1),
       round(MAX(osm_weighted_total)::numeric, 1),
       round(AVG(osm_weighted_total)::numeric, 1),
       round(STDDEV_POP(osm_weighted_total)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_weighted_total = 0)
FROM qsr_metrics
UNION ALL
SELECT 'population_local_1500', 'linear',
       round(MIN(pop_reach_1500)::numeric, 1),
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY pop_reach_1500)::numeric, 1),
       round(MAX(pop_reach_1500)::numeric, 1),
       round(AVG(pop_reach_1500)::numeric, 1),
       round(STDDEV_POP(pop_reach_1500)::numeric, 1),
       COUNT(*) FILTER (WHERE pop_reach_1500 = 0)
FROM qsr_metrics
UNION ALL
SELECT 'population_local_1200', 'linear',
       round(MIN(pop_reach_1200)::numeric, 1),
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY pop_reach_1200)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY pop_reach_1200)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY pop_reach_1200)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY pop_reach_1200)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY pop_reach_1200)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY pop_reach_1200)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY pop_reach_1200)::numeric, 1),
       round(MAX(pop_reach_1200)::numeric, 1),
       round(AVG(pop_reach_1200)::numeric, 1),
       round(STDDEV_POP(pop_reach_1200)::numeric, 1),
       COUNT(*) FILTER (WHERE pop_reach_1200 = 0)
FROM qsr_metrics
UNION ALL
SELECT 'population_local_1000', 'linear',
       round(MIN(pop_reach_1000)::numeric, 1),
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(percentile_cont(0.25) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(percentile_cont(0.75) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY pop_reach_1000)::numeric, 1),
       round(MAX(pop_reach_1000)::numeric, 1),
       round(AVG(pop_reach_1000)::numeric, 1),
       round(STDDEV_POP(pop_reach_1000)::numeric, 1),
       COUNT(*) FILTER (WHERE pop_reach_1000 = 0)
FROM qsr_metrics;

-- ── Raw per-bucket OSM counts (context for whether the QSR mix should shift) ──
SELECT 'osm_offices' AS signal,
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_offices)::numeric, 1) AS p5,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_offices)::numeric, 1) AS p50,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_offices)::numeric, 1) AS p95,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_offices)::numeric, 1) AS p99,
       round(MAX(osm_offices)::numeric, 1) AS max,
       COUNT(*) FILTER (WHERE osm_offices = 0) AS n_zero
FROM qsr_metrics
UNION ALL
SELECT 'osm_malls_retail',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_malls_retail)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_malls_retail)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_malls_retail)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_malls_retail)::numeric, 1),
       round(MAX(osm_malls_retail)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_malls_retail = 0)
FROM qsr_metrics
UNION ALL
SELECT 'osm_transit',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_transit)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_transit)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_transit)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_transit)::numeric, 1),
       round(MAX(osm_transit)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_transit = 0)
FROM qsr_metrics
UNION ALL
SELECT 'osm_mosques',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_mosques)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_mosques)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_mosques)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_mosques)::numeric, 1),
       round(MAX(osm_mosques)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_mosques = 0)
FROM qsr_metrics
UNION ALL
SELECT 'osm_schools',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_schools)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_schools)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_schools)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_schools)::numeric, 1),
       round(MAX(osm_schools)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_schools = 0)
FROM qsr_metrics
UNION ALL
SELECT 'osm_hospitals',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_hospitals)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_hospitals)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_hospitals)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_hospitals)::numeric, 1),
       round(MAX(osm_hospitals)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_hospitals = 0)
FROM qsr_metrics
UNION ALL
SELECT 'osm_hotels',
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY osm_hotels)::numeric, 1),
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY osm_hotels)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY osm_hotels)::numeric, 1),
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY osm_hotels)::numeric, 1),
       round(MAX(osm_hotels)::numeric, 1),
       COUNT(*) FILTER (WHERE osm_hotels = 0)
FROM qsr_metrics;

-- ── Population radius discrimination at QSR scale: spread ratio (p95/p50) ──
-- A higher ratio ⇒ the radius discriminates more. This informs whether the QSR
-- pop sub-term should be tightened below dine-in's 1500 m (RECORD-only — the
-- wiring PR decides; this probe changes nothing).
SELECT
    round((percentile_cont(0.95) WITHIN GROUP (ORDER BY pop_reach_1000)
         / NULLIF(percentile_cont(0.50) WITHIN GROUP (ORDER BY pop_reach_1000), 0))::numeric, 3) AS spread_ratio_1000,
    round((percentile_cont(0.95) WITHIN GROUP (ORDER BY pop_reach_1200)
         / NULLIF(percentile_cont(0.50) WITHIN GROUP (ORDER BY pop_reach_1200), 0))::numeric, 3) AS spread_ratio_1200,
    round((percentile_cont(0.95) WITHIN GROUP (ORDER BY pop_reach_1500)
         / NULLIF(percentile_cont(0.50) WITHIN GROUP (ORDER BY pop_reach_1500), 0))::numeric, 3) AS spread_ratio_1500,
    round(STDDEV_POP(pop_reach_1000)::numeric, 1) AS stddev_1000,
    round(STDDEV_POP(pop_reach_1200)::numeric, 1) AS stddev_1200,
    round(STDDEV_POP(pop_reach_1500)::numeric, 1) AS stddev_1500
FROM qsr_metrics;

DROP TABLE IF EXISTS qsr_metrics;
DROP TABLE IF EXISTS qsr_sample;
