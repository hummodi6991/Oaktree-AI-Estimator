-- ============================================================================
-- whitespace_input_distribution.sql
-- ----------------------------------------------------------------------------
-- Diagnose why competition_whitespace is floored at 15.00 for ~all dine-in
-- candidates. This probes the *scored* competitor_count — the exact integer
-- passed to _competition_whitespace_score() — for the most-recent dine-in
-- search, and contrasts it against:
--
--   * the curve's design domain (0-25 same-category competitors),
--   * the floor-crossing count (raw == 15  at count ~= 14.95, i.e. count >= 15
--     floors; raw == 0 at count == 25),
--   * the demand-index 3.5 km fnb_venue_count (snapshot) for scope overlap,
--   * a same-category recompute at TIGHTER radii (500 m / 1000 m vs the
--     dine-in scored radius of 3000 m) to see whether a tighter trade area
--     lands the count back in the 0-25 design range.
--
-- PROVENANCE (verified against app/services/expansion_advisor.py):
--   * scored competitor_count == expansion_candidate.competitor_count
--     (persisted column, migration 20260310_exp_adv_v0.py:63). This is the
--     literal argument fed to _competition_whitespace_score (call site
--     expansion_advisor.py:8088).
--   * It is computed by _bulk_enrich_competitors (expansion_advisor.py:6625)
--     as the SAME-CATEGORY (in_category) COUNT over restaurant_poi UNION ALL
--     delivery_source_record within the *competition* radius.
--   * dine_in competition radius == 3000 m
--     (_CATCHMENT_RADII_M, expansion_advisor.py:818).
--   * demand fnb_venue_count == same-category restaurant_poi count within the
--     demand-generator radius (default 3500 m), restaurant_poi ONLY
--     (expansion_advisor.py:8852, EXPANSION_DEMAND_GENERATOR_RADIUS_M=3500).
--
-- The tighter-radius recompute below approximates the app's same-category
-- filter with `lower(rp.category) = lower(s.category)` plus a delivery
-- category_raw/cuisine_raw ILIKE. The app uses _expand_category() keys/regex
-- (alias expansion), so for multi-alias categories these absolute counts may
-- differ from the scored value; treat them as a RADIUS-SENSITIVITY signal, not
-- an exact reproduction. The 3000 m row is included as a self-check: it should
-- be in the same ballpark as the scored competitor_count.
--
-- HOW TO RUN (iPad/Safari friendly — psql -f safe, no \set, no heredocs):
--   psql "$DATABASE_URL" -f scripts/diagnostics/whitespace_input_distribution.sql
-- ============================================================================

\timing on

-- ── Stage a sample (~500) of the most-recent dine-in search's candidates ──
-- Reads the SCORED competitor_count / whitespace_score straight off
-- expansion_candidate, plus the 3.5 km fnb_venue_count from the snapshot.
DROP TABLE IF EXISTS ws_sample;
CREATE TEMP TABLE ws_sample AS
WITH latest AS (
    SELECT id, category, service_model, created_at
    FROM expansion_search
    WHERE service_model = 'dine_in'
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT
    ec.parcel_id,
    ec.district,
    ec.lat,
    ec.lon,
    l.category                                                            AS search_category,
    ec.competitor_count                                                   AS scored_competitor_count,
    ec.whitespace_score                                                   AS competition_whitespace,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'fnb_venue_count')::int AS fnb_venue_count_3500m
FROM expansion_candidate ec
JOIN latest l ON ec.search_id = l.id
ORDER BY random()
LIMIT 500;

-- Which search are we probing?
SELECT
    (SELECT id         FROM expansion_search WHERE service_model='dine_in' ORDER BY created_at DESC LIMIT 1) AS latest_dine_in_search_id,
    (SELECT category   FROM expansion_search WHERE service_model='dine_in' ORDER BY created_at DESC LIMIT 1) AS category,
    (SELECT created_at FROM expansion_search WHERE service_model='dine_in' ORDER BY created_at DESC LIMIT 1) AS created_at,
    COUNT(*)                          AS sampled_candidates,
    COUNT(DISTINCT district)          AS distinct_districts
FROM ws_sample;

-- ── A) Distribution of the SCORED competitor_count (curve input) ──
-- This is the exact value/radius/filter feeding _competition_whitespace_score.
SELECT
    COUNT(*)                                                                          AS n,
    COUNT(*) FILTER (WHERE scored_competitor_count = 0)                               AS n_zero,
    MIN(scored_competitor_count)                                                      AS min,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY scored_competitor_count)::numeric, 1) AS p5,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY scored_competitor_count)::numeric, 1) AS p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY scored_competitor_count)::numeric, 1) AS p50,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY scored_competitor_count)::numeric, 1) AS p75,
    round(percentile_cont(0.90) WITHIN GROUP (ORDER BY scored_competitor_count)::numeric, 1) AS p90,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY scored_competitor_count)::numeric, 1) AS p95,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY scored_competitor_count)::numeric, 1) AS p99,
    MAX(scored_competitor_count)                                                      AS max
FROM ws_sample;

-- ── B) How floored is the population? ──
-- Curve design ceiling = 25 same-category competitors. Floor (raw<=15) takes
-- over at count >= 15 (raw==15 at ~14.95); raw<=0 at count >= 25.
SELECT
    COUNT(*)                                                          AS n,
    COUNT(*) FILTER (WHERE scored_competitor_count >= 15)             AS n_floored_ge15,
    round(100.0*COUNT(*) FILTER (WHERE scored_competitor_count >= 15)/NULLIF(COUNT(*),0), 1) AS pct_floored_ge15,
    COUNT(*) FILTER (WHERE scored_competitor_count > 25)              AS n_over_design_25,
    round(100.0*COUNT(*) FILTER (WHERE scored_competitor_count > 25)/NULLIF(COUNT(*),0), 1)  AS pct_over_design_25,
    COUNT(*) FILTER (WHERE competition_whitespace <= 15.00)           AS n_whitespace_at_floor,
    round(100.0*COUNT(*) FILTER (WHERE competition_whitespace <= 15.00)/NULLIF(COUNT(*),0), 1) AS pct_whitespace_at_floor
FROM ws_sample;

-- ── C) Per-district: scored competitor_count (3000 m, same-cat, poi+delivery)
--        vs demand-index fnb_venue_count (3500 m, same-cat, poi only) ──
-- Shows how heavily the competition scope overlaps the demand scope.
SELECT
    district,
    COUNT(*)                                       AS n,
    round(AVG(scored_competitor_count)::numeric,1) AS avg_scored_competitor_count_3000m,
    round(AVG(fnb_venue_count_3500m)::numeric,1)   AS avg_fnb_venue_count_3500m,
    round(AVG(competition_whitespace)::numeric,2)  AS avg_whitespace
FROM ws_sample
GROUP BY district
ORDER BY avg_scored_competitor_count_3000m DESC;

-- ── D) Radius sensitivity: same-category recompute at 3000 / 1000 / 500 m ──
-- restaurant_poi (OPERATIONAL-or-null) UNION ALL delivery_source_record,
-- approximating the app's same-category filter (see header caveat). Limited to
-- 150 candidates to bound cost; the delivery join uses lat/lon points and is
-- not index-accelerated, so reduce LIMIT further if it is slow.
DROP TABLE IF EXISTS ws_recompute;
CREATE TEMP TABLE ws_recompute AS
WITH samp AS (
    SELECT parcel_id, district, lat, lon, search_category, scored_competitor_count
    FROM ws_sample
    WHERE lat IS NOT NULL AND lon IS NOT NULL
    LIMIT 150
)
SELECT
    s.parcel_id,
    s.district,
    s.scored_competitor_count,
    r3000.cnt AS recompute_3000m,
    r1000.cnt AS recompute_1000m,
    r500.cnt  AS recompute_500m
FROM samp s
LEFT JOIN LATERAL (
    SELECT COUNT(*) AS cnt FROM (
        SELECT 1 FROM restaurant_poi rp
        WHERE (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
          AND lower(rp.category) = lower(s.search_category)
          AND rp.geom IS NOT NULL
          AND ST_DWithin(rp.geom::geography,
                ST_SetSRID(ST_MakePoint(s.lon, s.lat),4326)::geography, 3000)
        UNION ALL
        SELECT 1 FROM delivery_source_record dsr
        WHERE dsr.lat IS NOT NULL AND dsr.lon IS NOT NULL
          AND (lower(COALESCE(dsr.category_raw,'')) LIKE '%'||lower(s.search_category)||'%'
               OR lower(COALESCE(dsr.cuisine_raw,'')) LIKE '%'||lower(s.search_category)||'%')
          AND ST_DWithin(
                ST_SetSRID(ST_MakePoint(dsr.lon::double precision, dsr.lat::double precision),4326)::geography,
                ST_SetSRID(ST_MakePoint(s.lon, s.lat),4326)::geography, 3000)
    ) u
) r3000 ON TRUE
LEFT JOIN LATERAL (
    SELECT COUNT(*) AS cnt FROM (
        SELECT 1 FROM restaurant_poi rp
        WHERE (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
          AND lower(rp.category) = lower(s.search_category)
          AND rp.geom IS NOT NULL
          AND ST_DWithin(rp.geom::geography,
                ST_SetSRID(ST_MakePoint(s.lon, s.lat),4326)::geography, 1000)
        UNION ALL
        SELECT 1 FROM delivery_source_record dsr
        WHERE dsr.lat IS NOT NULL AND dsr.lon IS NOT NULL
          AND (lower(COALESCE(dsr.category_raw,'')) LIKE '%'||lower(s.search_category)||'%'
               OR lower(COALESCE(dsr.cuisine_raw,'')) LIKE '%'||lower(s.search_category)||'%')
          AND ST_DWithin(
                ST_SetSRID(ST_MakePoint(dsr.lon::double precision, dsr.lat::double precision),4326)::geography,
                ST_SetSRID(ST_MakePoint(s.lon, s.lat),4326)::geography, 1000)
    ) u
) r1000 ON TRUE
LEFT JOIN LATERAL (
    SELECT COUNT(*) AS cnt FROM (
        SELECT 1 FROM restaurant_poi rp
        WHERE (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
          AND lower(rp.category) = lower(s.search_category)
          AND rp.geom IS NOT NULL
          AND ST_DWithin(rp.geom::geography,
                ST_SetSRID(ST_MakePoint(s.lon, s.lat),4326)::geography, 500)
        UNION ALL
        SELECT 1 FROM delivery_source_record dsr
        WHERE dsr.lat IS NOT NULL AND dsr.lon IS NOT NULL
          AND (lower(COALESCE(dsr.category_raw,'')) LIKE '%'||lower(s.search_category)||'%'
               OR lower(COALESCE(dsr.cuisine_raw,'')) LIKE '%'||lower(s.search_category)||'%')
          AND ST_DWithin(
                ST_SetSRID(ST_MakePoint(dsr.lon::double precision, dsr.lat::double precision),4326)::geography,
                ST_SetSRID(ST_MakePoint(s.lon, s.lat),4326)::geography, 500)
    ) u
) r500 ON TRUE;

-- Distribution of the recompute at each radius + how many land in the 0-25
-- design range. If the 500 m / 1000 m columns drop the bulk of candidates to
-- <= 25 while 3000 m stays high, that is fork (a): the radius is the problem.
SELECT
    COUNT(*)                                                                 AS n,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY scored_competitor_count)::numeric,1) AS p50_scored,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY recompute_3000m)::numeric,1)         AS p50_3000m,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1)         AS p50_1000m,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY recompute_500m)::numeric,1)          AS p50_500m,
    round(100.0*COUNT(*) FILTER (WHERE recompute_3000m <= 25)/NULLIF(COUNT(*),0),1)         AS pct_le25_3000m,
    round(100.0*COUNT(*) FILTER (WHERE recompute_1000m <= 25)/NULLIF(COUNT(*),0),1)         AS pct_le25_1000m,
    round(100.0*COUNT(*) FILTER (WHERE recompute_500m  <= 25)/NULLIF(COUNT(*),0),1)         AS pct_le25_500m
FROM ws_recompute;

-- Per-district radius sensitivity (eyeball a handful of districts).
SELECT
    district,
    COUNT(*)                              AS n,
    round(AVG(scored_competitor_count)::numeric,1) AS avg_scored_3000m_app,
    round(AVG(recompute_3000m)::numeric,1)         AS avg_recompute_3000m,
    round(AVG(recompute_1000m)::numeric,1)         AS avg_recompute_1000m,
    round(AVG(recompute_500m)::numeric,1)          AS avg_recompute_500m
FROM ws_recompute
GROUP BY district
ORDER BY avg_recompute_3000m DESC;

-- ── E) City-wide 1000 m percentile distribution (recalibration reference) ──
-- Exists to set the recalibrated _competition_whitespace_score reference: the
-- per-district ladder (block D) showed the same-category count collapses into
-- the curve's 0-25 design range at ~1000 m, so this block reports the city-wide
-- percentile distribution of that SAME recompute_1000m column (same category-
-- match approximation, same restaurant_poi UNION ALL delivery_source_record
-- set, same ST_DWithin(..., 1000)) over the same sampled candidates already in
-- ws_recompute -- no re-sampling. pct_in_design_range = share with count <= 25
-- (the curve's current domain); pct_le_15 = share with count <= 15 (where the
-- current curve still has headroom above the 15.00 floor).
SELECT
    'recompute_1000m_citywide'                                              AS label,
    COUNT(*)                                                                AS n,
    COUNT(*) FILTER (WHERE recompute_1000m = 0)                             AS n_zero,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p5,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p50,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p75,
    round(percentile_cont(0.90) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p90,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p95,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY recompute_1000m)::numeric,1) AS p99,
    MAX(recompute_1000m)                                                    AS max,
    round(100.0*COUNT(*) FILTER (WHERE recompute_1000m <= 25)/NULLIF(COUNT(*),0),1) AS pct_in_design_range,
    round(100.0*COUNT(*) FILTER (WHERE recompute_1000m <= 15)/NULLIF(COUNT(*),0),1) AS pct_le_15
FROM ws_recompute;

DROP TABLE IF EXISTS ws_recompute;
DROP TABLE IF EXISTS ws_sample;
