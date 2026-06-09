-- ============================================================================
-- qsr_whitespace_probe.sql  (Weight audit Item 1 — QSR competition_whitespace)
-- ----------------------------------------------------------------------------
-- A fresh QSR run (search d4ca314b-f6b6-46ca-86f4-4760b125d618, 2026-06-10)
-- floored 10/15 candidates at 15.00: same-category competitor counts ran 20-45
-- at the 1200 m QSR competition radius (_CATCHMENT_RADII_M['qsr']['competition'],
-- app/services/expansion_advisor.py:830) under the default curve REF=25
-- (_WHITESPACE_LOG_REF_DEFAULT, expansion_advisor.py:2691). Under
-- raw = 100*(1 - ln(1+count)/ln(1+REF)) with the 15.0 floor
-- (expansion_advisor.py:2736-2739), REF=25 floors every count >= 15 — the same
-- dead-signal signature dine_in and delivery_first had before their fixes.
--
-- This probe measures the city-wide QSR same-category count distribution at
-- 800 / 1000 / 1200 m and scores the exact production curve for
-- REF in {25, 50, 75} so the (radius, REF) pair can be settled the same way
-- delivery_first was (spread the p25-p75 band; floor only the saturated tail).
--
-- HOW THE COUNT MIRRORS PRODUCTION (_bulk_enrich_competitors,
-- app/services/expansion_advisor.py:6876-6972):
--   * Source 1: restaurant_poi, lower(category) = ANY(category_keys),
--     business_status NULL or 'OPERATIONAL' (:6930-6947).
--   * Source 2: delivery_source_record, lower(category_raw|cuisine_raw)
--     ~* category_regex, geom required (:6952-6963).
--   * keys/regex from _expand_category (:558-574) / _CATEGORY_ALIAS_MAP
--     (:154-223). Two scopes are probed because the brief category drives them:
--       'burger'    -> keys {burger},
--                      regex 'burger|hamburger|برجر'
--       'fast food' -> keys {burger,pizza,chicken,fast_food},
--                      regex 'fast.food|fast_food|qsr|burger|hamburger|chicken|
--                             broasted|fried.chicken|pizza|pizzeria|
--                             وجبات سريعة|برجر|دجاج|بيتزا|فاست فود'
--   KNOWN BIAS: near-exact for these two categories. Residual deviations:
--   (a) if delivery_source_record.geom is NULL on some rows, production's
--       pre-Patch-5 lat/lon fallback would count them while this probe skips
--       them -> probe UNDER-counts in that case; (b) other QSR brief
--       categories (chicken, pizza) use narrower key sets -> their live counts
--       are a fraction of the 'fast food' scope numbers; (c) the candidate set
--       here is the Phase-A city-wide Tier-1 sample, not one brief's
--       area-filtered pool — distributional, not per-search.
--
-- Candidate set mirrors the Phase-A probe (l1_signal_distributions.sql:46-59):
-- Tier-1 cluster-primary candidate_location rows, LIMIT 1500.
--
-- HOW TO RUN (iPad/Safari friendly — single line, no heredocs):
--   psql -x -f scripts/diagnostics/qsr_whitespace_probe.sql > /tmp/qsr_ws.txt 2>&1
-- ============================================================================

\timing on

-- ── Sample of Tier-1 cluster-primary candidates, city-wide (Phase-A pattern) ──
DROP TABLE IF EXISTS qsr_ws_sample;
CREATE TEMP TABLE qsr_ws_sample AS
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

SELECT COUNT(*) AS sampled_candidates FROM qsr_ws_sample;

-- ── Per-candidate same-category competitor counts at 800/1000/1200 m ──
-- One LATERAL per candidate over the 1200 m envelope; tighter radii are
-- distance-filtered from the same row set (cheaper than three DWithin scans).
DROP TABLE IF EXISTS qsr_ws_metrics;
CREATE TEMP TABLE qsr_ws_metrics AS
SELECT
    s.parcel_id,
    COALESCE(c.b_800, 0)  AS burger_800,
    COALESCE(c.b_1000, 0) AS burger_1000,
    COALESCE(c.b_1200, 0) AS burger_1200,
    COALESCE(c.f_800, 0)  AS fastfood_800,
    COALESCE(c.f_1000, 0) AS fastfood_1000,
    COALESCE(c.f_1200, 0) AS fastfood_1200
FROM qsr_ws_sample s
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE g.is_burger AND g.dist_m <= 800)  AS b_800,
        COUNT(*) FILTER (WHERE g.is_burger AND g.dist_m <= 1000) AS b_1000,
        COUNT(*) FILTER (WHERE g.is_burger)                      AS b_1200,
        COUNT(*) FILTER (WHERE g.is_ff AND g.dist_m <= 800)      AS f_800,
        COUNT(*) FILTER (WHERE g.is_ff AND g.dist_m <= 1000)     AS f_1000,
        COUNT(*) FILTER (WHERE g.is_ff)                          AS f_1200
    FROM (
        -- Source 1: restaurant_poi (mirrors expansion_advisor.py:6930-6947)
        SELECT
            ST_Distance(
                rp.geom::geography,
                ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography
            ) AS dist_m,
            (lower(rp.category) = ANY (ARRAY['burger'])) AS is_burger,
            (lower(rp.category) = ANY (ARRAY['burger','pizza','chicken','fast_food'])) AS is_ff
        FROM restaurant_poi rp
        WHERE (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
          AND rp.geom IS NOT NULL
          AND ST_DWithin(
                rp.geom::geography,
                ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography,
                1200
              )
        UNION ALL
        -- Source 2: delivery_source_record (mirrors expansion_advisor.py:6952-6963)
        SELECT
            ST_Distance(
                dsr.geom::geography,
                ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography
            ) AS dist_m,
            (lower(COALESCE(dsr.category_raw, '')) ~* 'burger|hamburger|برجر'
             OR lower(COALESCE(dsr.cuisine_raw, '')) ~* 'burger|hamburger|برجر') AS is_burger,
            (lower(COALESCE(dsr.category_raw, '')) ~* 'fast.food|fast_food|qsr|burger|hamburger|chicken|broasted|fried.chicken|pizza|pizzeria|وجبات سريعة|برجر|دجاج|بيتزا|فاست فود'
             OR lower(COALESCE(dsr.cuisine_raw, '')) ~* 'fast.food|fast_food|qsr|burger|hamburger|chicken|broasted|fried.chicken|pizza|pizzeria|وجبات سريعة|برجر|دجاج|بيتزا|فاست فود') AS is_ff
        FROM delivery_source_record dsr
        WHERE dsr.geom IS NOT NULL
          AND ST_DWithin(
                dsr.geom::geography,
                ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography,
                1200
              )
    ) g
) c ON TRUE;

-- ── Tall view: (scope, radius, count) per candidate ──
DROP TABLE IF EXISTS qsr_ws_tall;
CREATE TEMP TABLE qsr_ws_tall AS
SELECT parcel_id, 'burger'::text    AS scope,  800 AS radius_m, burger_800    AS cnt FROM qsr_ws_metrics
UNION ALL
SELECT parcel_id, 'burger',                   1000,             burger_1000          FROM qsr_ws_metrics
UNION ALL
SELECT parcel_id, 'burger',                   1200,             burger_1200          FROM qsr_ws_metrics
UNION ALL
SELECT parcel_id, 'fast_food',                 800,             fastfood_800         FROM qsr_ws_metrics
UNION ALL
SELECT parcel_id, 'fast_food',                1000,             fastfood_1000        FROM qsr_ws_metrics
UNION ALL
SELECT parcel_id, 'fast_food',                1200,             fastfood_1200        FROM qsr_ws_metrics;

-- ── Output 1: raw count distribution per scope × radius ──
SELECT
    scope,
    radius_m,
    COUNT(*) AS n,
    round(percentile_cont(0.05) WITHIN GROUP (ORDER BY cnt)::numeric, 1) AS p5,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY cnt)::numeric, 1) AS p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY cnt)::numeric, 1) AS p50,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY cnt)::numeric, 1) AS p75,
    round(percentile_cont(0.90) WITHIN GROUP (ORDER BY cnt)::numeric, 1) AS p90,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY cnt)::numeric, 1) AS p95,
    MAX(cnt) AS max,
    round(100.0 * COUNT(*) FILTER (WHERE cnt = 0) / COUNT(*), 1) AS pct_zero
FROM qsr_ws_tall
GROUP BY scope, radius_m
ORDER BY scope, radius_m;

-- ── Output 2: exact production curve per REF × radius × scope ──
-- Curve (expansion_advisor.py:2736-2739):
--   score = max(15, 100*(1 - ln(1+count)/ln(1+REF)))   for count > 0
--   count = 0 scores 100 only when 'confident' (F4, :2728-2731); zeros here
--   are confident by construction (the scan observed the radius).
-- floored_pct = % of candidates whose unfloored raw is <= 15 (count > 0),
-- i.e. the component is a dead flat 15.00 for them.
SELECT
    x.scope,
    x.radius_m,
    x.ref::int AS ref,
    x.n,
    round(x.floored_pct::numeric, 1) AS floored_pct,
    round(CASE WHEN x.p25 <= 0 THEN 100.0
               ELSE GREATEST(15.0, 100.0 * (1.0 - ln(1.0 + x.p25) / ln(1.0 + x.ref)))
          END::numeric, 1) AS score_at_p25,
    round(CASE WHEN x.p50 <= 0 THEN 100.0
               ELSE GREATEST(15.0, 100.0 * (1.0 - ln(1.0 + x.p50) / ln(1.0 + x.ref)))
          END::numeric, 1) AS score_at_p50,
    round(CASE WHEN x.p75 <= 0 THEN 100.0
               ELSE GREATEST(15.0, 100.0 * (1.0 - ln(1.0 + x.p75) / ln(1.0 + x.ref)))
          END::numeric, 1) AS score_at_p75,
    round(x.p25::numeric, 1) AS p25_cnt,
    round(x.p50::numeric, 1) AS p50_cnt,
    round(x.p75::numeric, 1) AS p75_cnt
FROM (
    SELECT
        t.scope,
        t.radius_m,
        r.ref,
        COUNT(*) AS n,
        100.0 * COUNT(*) FILTER (
            WHERE t.cnt > 0
              AND 100.0 * (1.0 - ln(1.0 + t.cnt) / ln(1.0 + r.ref)) <= 15.0
        ) / COUNT(*) AS floored_pct,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY t.cnt) AS p25,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY t.cnt) AS p50,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY t.cnt) AS p75
    FROM qsr_ws_tall t
    CROSS JOIN (VALUES (25.0), (50.0), (75.0)) AS r(ref)
    GROUP BY t.scope, t.radius_m, r.ref
) x
ORDER BY x.scope, x.radius_m, x.ref;

-- ── Output 3: floor onset reference (exact, for the findings doc) ──
-- Smallest integer count whose unfloored raw is <= 15 per REF:
--   REF=25 -> 15, REF=50 -> 28, REF=75 -> 39  (c* = exp(0.85*ln(1+REF)) - 1)
SELECT
    r.ref::int AS ref,
    MIN(c.c) AS first_floored_count
FROM (VALUES (25.0), (50.0), (75.0)) AS r(ref)
JOIN generate_series(1, 200) AS c(c)
  ON 100.0 * (1.0 - ln(1.0 + c.c) / ln(1.0 + r.ref)) <= 15.0
GROUP BY r.ref
ORDER BY r.ref;

DROP TABLE IF EXISTS qsr_ws_tall;
DROP TABLE IF EXISTS qsr_ws_metrics;
DROP TABLE IF EXISTS qsr_ws_sample;
