-- ============================================================================
-- chain_strength_input_probe.sql  (Weight audit Item 3 — dead-weight check)
-- ----------------------------------------------------------------------------
-- The chain_strength leg input is the strong-chain SHARE computed in
-- _bulk_enrich_competitors (app/services/expansion_advisor.py:6908-6915):
-- among same-category restaurant_poi rows in the competition radius that
-- joined to expansion_competitor_quality (ECQ), the % whose
-- chain_strength_score >= EXPANSION_CHAIN_STRONG_THRESHOLD (60.0,
-- config.py:409-411). NULL when fewer than EXPANSION_CHAIN_MIN_MATCHED (3,
-- config.py:412-414) matched POIs are in radius; NULL -> neutral 50.0 in
-- _chain_strength_score (:2742-2758). The component weight is 3.0%
-- (EXPANSION_CHAIN_STRENGTH_WEIGHT, config.py:395-397).
--
-- Dead-weight question: what fraction of candidates actually resolves to the
-- neutral 50 (insufficient matched POIs) vs a real share?
--
-- NOTE on stored JSON: score_breakdown_json.inputs.chain_strength == 50.0 is
-- AMBIGUOUS — it is either the neutral fallback OR a true 50.0 share.
-- inputs.chain_strength_max IS NULL identifies the "no ECQ match at all"
-- subset (:3520-3524), but "1-2 matched POIs" vs "true 50 share" cannot be
-- told apart from the JSON. Part B therefore RE-DERIVES the share spatially
-- with the exact production predicate over a sample of recent candidates.
--
-- HOW TO RUN (iPad/Safari friendly — single line, no heredocs):
--   psql -x -f scripts/diagnostics/chain_strength_input_probe.sql > /tmp/chain_in.txt 2>&1
-- ============================================================================

\timing on

-- ════════════════════════════════════════════════════════════════════════
-- PART A — stored score_breakdown_json over the last 30 days
-- ════════════════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS cs_json;
CREATE TEMP TABLE cs_json AS
SELECT
    ec.id,
    es.service_model,
    (ec.score_breakdown_json -> 'inputs' ->> 'chain_strength')::double precision AS cs_input,
    (ec.score_breakdown_json -> 'inputs' ->> 'chain_strength_max')::double precision AS cs_max
FROM expansion_candidate ec
JOIN expansion_search es ON es.id = ec.search_id
WHERE es.created_at >= now() - interval '30 days'
  AND ec.score_breakdown_json ? 'inputs';

SELECT
    service_model,
    COUNT(*) AS n,
    COUNT(*) FILTER (WHERE cs_input = 50.0 AND cs_max IS NULL)
        AS n_neutral_no_ecq_match,
    COUNT(*) FILTER (WHERE cs_input = 50.0 AND cs_max IS NOT NULL)
        AS n_fifty_ambiguous,          -- thin (<3 matched) OR a true 50.0 share
    COUNT(*) FILTER (WHERE cs_input IS DISTINCT FROM 50.0)
        AS n_real_share_non50,
    round(100.0 * COUNT(*) FILTER (WHERE cs_input = 50.0) / GREATEST(COUNT(*), 1), 1)
        AS pct_input_eq_50
FROM cs_json
GROUP BY service_model
ORDER BY service_model;

-- Share distribution where the input is demonstrably a real share (<> 50).
SELECT
    service_model,
    COUNT(*) AS n_non50,
    round(percentile_cont(0.10) WITHIN GROUP (ORDER BY cs_input)::numeric, 1) AS p10,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY cs_input)::numeric, 1) AS p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY cs_input)::numeric, 1) AS p50,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY cs_input)::numeric, 1) AS p75,
    round(percentile_cont(0.90) WITHIN GROUP (ORDER BY cs_input)::numeric, 1) AS p90,
    round(100.0 * COUNT(*) FILTER (WHERE cs_input = 0.0)  / GREATEST(COUNT(*), 1), 1) AS pct_zero,
    round(100.0 * COUNT(*) FILTER (WHERE cs_input = 100.0) / GREATEST(COUNT(*), 1), 1) AS pct_hundred
FROM cs_json
WHERE cs_input IS DISTINCT FROM 50.0
GROUP BY service_model
ORDER BY service_model;

-- ════════════════════════════════════════════════════════════════════════
-- PART B — spatial re-derivation with the exact production predicate
-- (restaurant_poi side only — the delivery side contributes NULL
-- chain_strength by construction, expansion_advisor.py:6952-6956, so it never
-- enters the share.)
-- Radius per service model from _CATCHMENT_RADII_M['<model>']['competition']
-- (expansion_advisor.py:817-832); category keys per _CATEGORY_ALIAS_MAP
-- (:154-223), unknown categories fall back to the single normalized key
-- (:566-568).
-- ════════════════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS cs_sample;
CREATE TEMP TABLE cs_sample AS
SELECT
    ec.id,
    ec.lon::double precision AS lon,
    ec.lat::double precision AS lat,
    lower(es.service_model)  AS service_model,
    lower(es.category)       AS category,
    CASE lower(es.service_model)
        WHEN 'dine_in'        THEN 1000.0
        WHEN 'delivery_first' THEN 1000.0
        WHEN 'cafe'           THEN  800.0
        ELSE 1200.0   -- qsr + unknown fall back to qsr (expansion_advisor.py:846-856)
    END AS radius_m,
    CASE lower(es.category)
        WHEN 'fast food'  THEN ARRAY['burger','pizza','chicken','fast_food']
        WHEN 'burger'     THEN ARRAY['burger']
        WHEN 'pizza'      THEN ARRAY['pizza']
        WHEN 'chicken'    THEN ARRAY['chicken']
        WHEN 'cafe'       THEN ARRAY['coffee_bakery']
        WHEN 'coffee'     THEN ARRAY['coffee_bakery']
        WHEN 'shawarma'   THEN ARRAY['shawarma','traditional']
        WHEN 'traditional' THEN ARRAY['traditional']
        WHEN 'indian'     THEN ARRAY['indian','asian']
        WHEN 'asian'      THEN ARRAY['asian']
        WHEN 'seafood'    THEN ARRAY['seafood']
        WHEN 'healthy'    THEN ARRAY['healthy']
        ELSE ARRAY[replace(lower(es.category), ' ', '_')]
    END AS category_keys
FROM expansion_candidate ec
JOIN expansion_search es ON es.id = ec.search_id
WHERE es.created_at >= now() - interval '30 days'
  AND ec.lon IS NOT NULL AND ec.lat IS NOT NULL
ORDER BY es.created_at DESC, ec.id
LIMIT 800;

SELECT COUNT(*) AS rederived_sample FROM cs_sample;

DROP TABLE IF EXISTS cs_derived;
CREATE TEMP TABLE cs_derived AS
SELECT
    s.id,
    s.service_model,
    s.category,
    COALESCE(m.matched, 0) AS matched,
    COALESCE(m.strong, 0)  AS strong,
    CASE WHEN COALESCE(m.matched, 0) >= 3
         THEN 100.0 * m.strong / m.matched
    END AS share
FROM cs_sample s
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE ecq.chain_strength_score IS NOT NULL) AS matched,
        COUNT(*) FILTER (WHERE ecq.chain_strength_score >= 60.0)     AS strong
    FROM restaurant_poi rp
    LEFT JOIN expansion_competitor_quality ecq
           ON ecq.restaurant_poi_id = rp.id
          AND ecq.city = 'riyadh'
    WHERE (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
      AND rp.geom IS NOT NULL
      AND lower(rp.category) = ANY (s.category_keys)
      AND ST_DWithin(
            rp.geom::geography,
            ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326)::geography,
            s.radius_m
          )
) m ON TRUE;

-- Output: neutral vs real-share split, exactly disambiguated.
SELECT
    service_model,
    COUNT(*) AS n,
    round(100.0 * COUNT(*) FILTER (WHERE matched = 0)  / COUNT(*), 1) AS pct_matched_0,
    round(100.0 * COUNT(*) FILTER (WHERE matched IN (1, 2)) / COUNT(*), 1) AS pct_matched_1_2,
    round(100.0 * COUNT(*) FILTER (WHERE matched >= 3) / COUNT(*), 1) AS pct_real_share,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY matched)::numeric, 1) AS matched_p50,
    round(percentile_cont(0.90) WITHIN GROUP (ORDER BY matched)::numeric, 1) AS matched_p90
FROM cs_derived
GROUP BY service_model
ORDER BY service_model;

-- Share distribution where real (matched >= 3).
SELECT
    service_model,
    COUNT(*) AS n_real,
    round(percentile_cont(0.10) WITHIN GROUP (ORDER BY share)::numeric, 1) AS p10,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY share)::numeric, 1) AS p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY share)::numeric, 1) AS p50,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY share)::numeric, 1) AS p75,
    round(percentile_cont(0.90) WITHIN GROUP (ORDER BY share)::numeric, 1) AS p90,
    round(STDDEV_POP(share)::numeric, 1) AS stddev
FROM cs_derived
WHERE share IS NOT NULL
GROUP BY service_model
ORDER BY service_model;

-- ════════════════════════════════════════════════════════════════════════
-- PART C — ECQ canonical coverage + upstream name signal
-- ════════════════════════════════════════════════════════════════════════
-- ECQ rows matched to a canonical brand (brand_alias hit at ingest,
-- app/ingest/expansion_advisor_competitors.py:304-315) vs same-form-only.
SELECT
    COUNT(*) AS ecq_total,
    COUNT(canonical_brand_id) AS ecq_canonical,
    round(100.0 * COUNT(canonical_brand_id) / GREATEST(COUNT(*), 1), 1) AS pct_canonical,
    COUNT(*) FILTER (WHERE chain_strength_score >= 60.0) AS ecq_strong,
    round(100.0 * COUNT(*) FILTER (WHERE chain_strength_score >= 60.0)
          / GREATEST(COUNT(*), 1), 1) AS pct_strong,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY chain_strength_score)::numeric, 1)
        AS chain_strength_p50,
    round(percentile_cont(0.90) WITHIN GROUP (ORDER BY chain_strength_score)::numeric, 1)
        AS chain_strength_p90
FROM expansion_competitor_quality
WHERE city = 'riyadh';

-- Implied chain-size ladder buckets (chain_strength_score = LEAST(100, size*12),
-- app/ingest/expansion_advisor_competitors.py:245): 12=1 (or <5, no CTE row),
-- 60=5, 72=6, ..., 100 = 9+ branches.
SELECT
    round(chain_strength_score::numeric, 0) AS chain_strength_score,
    COUNT(*) AS n
FROM expansion_competitor_quality
WHERE city = 'riyadh'
GROUP BY 1
ORDER BY 1;

-- Upstream: restaurant_poi.chain_name null rate (the reason the ingest derives
-- chain identity from `name`, app/ingest/expansion_advisor_competitors.py:43-44).
SELECT
    COUNT(*) AS poi_total,
    COUNT(chain_name) AS poi_chain_name_nonnull,
    round(100.0 * (COUNT(*) - COUNT(chain_name)) / GREATEST(COUNT(*), 1), 2)
        AS pct_chain_name_null
FROM restaurant_poi;

-- brand_alias coverage (canonicalization layer feeding chain_counts,
-- app/ingest/expansion_advisor_competitors.py:199-218).
SELECT
    COUNT(*) AS alias_rows,
    COUNT(DISTINCT canonical_brand_id) AS canonical_brands
FROM brand_alias;

DROP TABLE IF EXISTS cs_derived;
DROP TABLE IF EXISTS cs_sample;
DROP TABLE IF EXISTS cs_json;
