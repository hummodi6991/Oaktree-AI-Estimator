-- ============================================================================
-- delivery_first_whitespace_probe.sql   (READ-ONLY diagnostic)
-- ----------------------------------------------------------------------------
-- Purpose: confirm and SIZE the competition-whitespace flooring bug for the
-- `delivery_first` service model, the same bug we already fixed for dine_in.
--
-- Background (see docs/fix-dinein-competition-whitespace-report.md):
--   _competition_whitespace_score(count) = 100*(1 - log1p(count)/log1p(REF)),
--   clamped to a 15.00 floor. It floors STRUCTURALLY at count = REF.
--   - delivery_first competition radius = 2500 m
--       (app/services/expansion_advisor.py:823, _CATCHMENT_RADII_M).
--   - delivery_first is NOT in _WHITESPACE_LOG_REF (only dine_in -> 50), so it
--       resolves to _WHITESPACE_LOG_REF_DEFAULT = 25
--       (app/services/expansion_advisor.py:2556-2567, 2608-2609).
--   => any candidate whose same-category competitor_count (measured over 2500 m)
--      is >= 25 is pinned at whitespace_score = 15.00.
--
-- This script reads the MOST RECENT delivery_first search and:
--   (1) lists per-candidate district / parcel_id / competitor_count (the stored
--       2500 m count) / competition_whitespace / final_score;
--   (2) summarises the floor: count + % of candidates at exactly 15.00, the
--       competitor_count distribution (p50/p75/p90/p99/max), and how many rows
--       have competitor_count >= 25 (REF) i.e. are pinned at the floor;
--   (3) recomputes the same-category count SPATIALLY at 2500 m vs 1000 m so the
--       radius effect on the count distribution is visible apples-to-apples;
--   (4) reads the persisted competition_whitespace WEIGHT from
--       score_breakdown_json so the blast radius of any fix is explicit.
--
-- The authoritative "is it floored" answer comes from block (1)/(2), which read
-- the production-accurate stored columns (competitor_count is the value that was
-- actually fed to the scorer, including production category alias-expansion).
--
-- Block (3) RECOMPUTES the count from restaurant_poi + delivery_source_record
-- using a SIMPLIFIED category match (ILIKE on expansion_search.category). It
-- does NOT replicate _expand_category()'s alias expansion, so it UNDER-COUNTS
-- vs production (same caveat the dine_in probe carried). Use block (3) for the
-- RELATIVE 2500 m -> 1000 m shift, not as an absolute count.
--
-- EXPECTED RESULT IF THE BUG IS REAL (mirrors dine_in's 97.4% floored):
--   - pct_whitespace_at_floor is HIGH (most delivery_first candidates at 15.00);
--   - competitor_count p50 >> 25 (REF), so the median candidate is past the knee;
--   - n_pinned_at_or_above_ref ~= the floored count;
--   - block (3) shows the 1000 m distribution materially lower than 2500 m
--     (whether 1000 m alone clears the REF=25 knee is the data question that
--      decides shrink-radius vs raise-REF vs both).
--
-- HOW TO RUN (iPad/Safari friendly -- psql -f safe, no \set, no heredocs):
--   psql "$DATABASE_URL" -f scripts/diagnostics/delivery_first_whitespace_probe.sql
-- ============================================================================

\timing on

-- ── Stage the most-recent delivery_first search's candidates ──
-- competitor_count here is the STORED 2500 m same-category count that was fed to
-- _competition_whitespace_score; whitespace_score is that scorer's output.
DROP TABLE IF EXISTS df_ws;
CREATE TEMP TABLE df_ws AS
WITH latest AS (
    SELECT id, created_at, brand_name, category, service_model
    FROM expansion_search
    WHERE service_model = 'delivery_first'
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT
    l.id                                                              AS search_id,
    l.category                                                        AS search_category,
    ec.parcel_id,
    ec.district,
    ec.lat::double precision                                         AS lat,
    ec.lon::double precision                                         AS lon,
    ec.competitor_count                                              AS competitor_count_2500m,
    ec.whitespace_score                                             AS competition_whitespace,
    ec.final_score,
    -- persisted weight of the competition_whitespace component (blast radius).
    (ec.score_breakdown_json -> 'weights' ->> 'competition_whitespace')::numeric AS competition_whitespace_weight_pct,
    -- F4 confidence flag (count<=0 path): lets us separate true floor (15.00 via
    -- count>=REF) from the neutral-50 / wide-open-100 count<=0 cases.
    (ec.score_breakdown_json -> 'inputs' ->> 'competition_whitespace_confident') AS competition_whitespace_confident
FROM expansion_candidate ec
JOIN latest l ON ec.search_id = l.id;

-- ── Which search are we probing? ──
SELECT
    (SELECT id FROM expansion_search WHERE service_model = 'delivery_first'
       ORDER BY created_at DESC LIMIT 1)               AS latest_delivery_first_search_id,
    (SELECT created_at FROM expansion_search WHERE service_model = 'delivery_first'
       ORDER BY created_at DESC LIMIT 1)               AS created_at,
    (SELECT category FROM expansion_search WHERE service_model = 'delivery_first'
       ORDER BY created_at DESC LIMIT 1)               AS category,
    COUNT(*)                                           AS n_candidates,
    COUNT(DISTINCT district)                           AS distinct_districts,
    MIN(competition_whitespace_weight_pct)             AS ws_weight_pct_min,
    MAX(competition_whitespace_weight_pct)             AS ws_weight_pct_max
FROM df_ws;

-- ============================================================================
-- (2) FLOOR SIZING  -- the headline numbers (production-accurate, REF = 25)
-- ============================================================================
-- pct_whitespace_at_floor    -> % of candidates pinned at exactly 15.00
-- n_pinned_at_or_above_ref   -> count with competitor_count_2500m >= 25 (REF):
--                               these are STRUCTURALLY floored by the curve.
-- The two should agree closely; any gap is the count<=0 F4 rows (50.0 / 100.0).
SELECT
    COUNT(*)                                                          AS n,
    COUNT(*) FILTER (WHERE competition_whitespace = 15.00)            AS n_at_floor,
    round(100.0 * COUNT(*) FILTER (WHERE competition_whitespace = 15.00)
          / NULLIF(COUNT(*), 0), 1)                                  AS pct_whitespace_at_floor,
    COUNT(*) FILTER (WHERE competitor_count_2500m >= 25)              AS n_pinned_at_or_above_ref,
    round(100.0 * COUNT(*) FILTER (WHERE competitor_count_2500m >= 25)
          / NULLIF(COUNT(*), 0), 1)                                  AS pct_at_or_above_ref,
    -- distinct whitespace values: a near-constant component (mostly 15.00)
    -- carries no discriminating signal, exactly the dine_in failure mode.
    COUNT(DISTINCT competition_whitespace)                           AS distinct_whitespace_values,
    -- F4 buckets (count<=0): these are NOT curve-floored, isolate them out.
    COUNT(*) FILTER (WHERE competition_whitespace = 50.00)            AS n_neutral_50,
    COUNT(*) FILTER (WHERE competition_whitespace = 100.00)           AS n_open_100
FROM df_ws;

-- ── competitor_count distribution at the production 2500 m radius ──
-- If p50 >> 25 the median candidate is well past the curve knee -> bug confirmed.
SELECT
    '2500m_stored' AS radius,
    COUNT(*)                                                                       AS n,
    round(AVG(competitor_count_2500m), 1)                                          AS mean,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY competitor_count_2500m)           AS p50,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY competitor_count_2500m)           AS p75,
    percentile_cont(0.90) WITHIN GROUP (ORDER BY competitor_count_2500m)           AS p90,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY competitor_count_2500m)           AS p99,
    MAX(competitor_count_2500m)                                                    AS max,
    25                                                                             AS ref_default,
    round(100.0 * COUNT(*) FILTER (WHERE competitor_count_2500m <= 25)
          / NULLIF(COUNT(*), 0), 1)                                               AS pct_within_ref
FROM df_ws;

-- ============================================================================
-- (3) RADIUS COMPARISON  -- spatial recompute at 2500 m vs 1000 m
-- ----------------------------------------------------------------------------
-- APPROXIMATE same-category count (simplified ILIKE category match; no alias
-- expansion -> under-counts vs production). Mirrors the bulk-count source UNION
-- in _bulk_compute_competitor_count: restaurant_poi (OPERATIONAL/NULL status)
-- UNION ALL delivery_source_record, same ST_DWithin geography predicate.
-- Use these blocks for the RELATIVE 2500 m -> 1000 m shift only.
-- ============================================================================
DROP TABLE IF EXISTS df_ws_recount;
CREATE TEMP TABLE df_ws_recount AS
SELECT
    d.parcel_id,
    d.district,
    -- same-category count within 2500 m (approx)
    (
        SELECT COUNT(*) FROM (
            SELECT 1
            FROM restaurant_poi rp
            WHERE (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
              AND lower(rp.category) LIKE '%' || lower(d.search_category) || '%'
              AND ST_DWithin(
                    rp.geom::geography,
                    ST_SetSRID(ST_MakePoint(d.lon, d.lat), 4326)::geography,
                    2500.0)
            UNION ALL
            SELECT 1
            FROM delivery_source_record dsr
            WHERE dsr.geom IS NOT NULL
              AND (lower(COALESCE(dsr.category_raw, '')) LIKE '%' || lower(d.search_category) || '%'
                   OR lower(COALESCE(dsr.cuisine_raw, '')) LIKE '%' || lower(d.search_category) || '%')
              AND ST_DWithin(
                    dsr.geom::geography,
                    ST_SetSRID(ST_MakePoint(d.lon, d.lat), 4326)::geography,
                    2500.0)
        ) s
    ) AS approx_count_2500m,
    -- same-category count within 1000 m (approx)
    (
        SELECT COUNT(*) FROM (
            SELECT 1
            FROM restaurant_poi rp
            WHERE (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
              AND lower(rp.category) LIKE '%' || lower(d.search_category) || '%'
              AND ST_DWithin(
                    rp.geom::geography,
                    ST_SetSRID(ST_MakePoint(d.lon, d.lat), 4326)::geography,
                    1000.0)
            UNION ALL
            SELECT 1
            FROM delivery_source_record dsr
            WHERE dsr.geom IS NOT NULL
              AND (lower(COALESCE(dsr.category_raw, '')) LIKE '%' || lower(d.search_category) || '%'
                   OR lower(COALESCE(dsr.cuisine_raw, '')) LIKE '%' || lower(d.search_category) || '%')
              AND ST_DWithin(
                    dsr.geom::geography,
                    ST_SetSRID(ST_MakePoint(d.lon, d.lat), 4326)::geography,
                    1000.0)
        ) s
    ) AS approx_count_1000m
FROM df_ws d
WHERE d.lat IS NOT NULL AND d.lon IS NOT NULL;

-- Side-by-side distribution at both radii (approx counts).
-- NOTE: approx_count_2500m will read LOWER than competitor_count_2500m in
-- block (2) because this match has no alias expansion. Compare the SHAPE
-- (2500 m vs 1000 m), not the absolute level.
SELECT '2500m_approx' AS radius,
       COUNT(*)                                                              AS n,
       round(AVG(approx_count_2500m), 1)                                     AS mean,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY approx_count_2500m)      AS p50,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY approx_count_2500m)      AS p75,
       percentile_cont(0.90) WITHIN GROUP (ORDER BY approx_count_2500m)      AS p90,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY approx_count_2500m)      AS p99,
       MAX(approx_count_2500m)                                               AS max,
       round(100.0 * COUNT(*) FILTER (WHERE approx_count_2500m <= 25)
             / NULLIF(COUNT(*), 0), 1)                                       AS pct_within_ref_25
FROM df_ws_recount
UNION ALL
SELECT '1000m_approx' AS radius,
       COUNT(*)                                                              AS n,
       round(AVG(approx_count_1000m), 1)                                     AS mean,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY approx_count_1000m)      AS p50,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY approx_count_1000m)      AS p75,
       percentile_cont(0.90) WITHIN GROUP (ORDER BY approx_count_1000m)      AS p90,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY approx_count_1000m)      AS p99,
       MAX(approx_count_1000m)                                               AS max,
       round(100.0 * COUNT(*) FILTER (WHERE approx_count_1000m <= 25)
             / NULLIF(COUNT(*), 0), 1)                                       AS pct_within_ref_25
FROM df_ws_recount;

-- ============================================================================
-- (1) PER-CANDIDATE LISTING  -- eyeball the floor and the radius effect
-- ============================================================================
SELECT
    d.district,
    d.parcel_id,
    d.competitor_count_2500m,
    r.approx_count_2500m,
    r.approx_count_1000m,
    d.competition_whitespace,
    d.competition_whitespace_confident,
    d.final_score,
    d.competition_whitespace_weight_pct
FROM df_ws d
LEFT JOIN df_ws_recount r USING (parcel_id, district)
ORDER BY d.competitor_count_2500m DESC NULLS LAST, d.final_score DESC
LIMIT 60;

-- ── Top candidate per district (geographic spread) ──
SELECT DISTINCT ON (d.district)
    d.district,
    d.parcel_id,
    d.competitor_count_2500m,
    d.competition_whitespace,
    d.final_score
FROM df_ws d
ORDER BY d.district, d.final_score DESC;

DROP TABLE IF EXISTS df_ws_recount;
DROP TABLE IF EXISTS df_ws;
