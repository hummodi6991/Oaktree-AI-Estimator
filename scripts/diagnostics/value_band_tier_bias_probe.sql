-- ============================================================================
-- value_band_tier_bias_probe.sql  (Finding 2 — scoring/ranking audit 2026-06)
--
-- _estimate_revenue_index multiplies its base by a price-tier ticket
-- multiplier (expansion_advisor.py L4996-4999: implied_check / 50, clamped
-- [0.5, 2.5]) before the result feeds _value_score (geometric mean with
-- rent_burden_score, L5476-5490) and _classify_value_band (>=75 best_value,
-- <25 above_market, L5493-5500) — despite the comment at L5330-5335 that
-- the value path "must stay tier-blind".
--
-- Expected fingerprint if the bias is real:
--   * premium-tier briefs: estimated_revenue_index pinned at/near 100 for a
--     large share of candidates; value_band skews heavily to best_value.
--   * value-tier briefs: revenue index capped (~52-66 depending on
--     category); best_value share near zero.
--
-- Run read-only against the production replica (Ahmed: via Codespace psql).
-- ============================================================================

-- Value-band distribution and revenue-index saturation by persisted
-- brand-profile price tier (last 90 days for sample size).
WITH cands AS (
    SELECT
        COALESCE(NULLIF(lower(bp.price_tier), ''), 'unset') AS price_tier,
        lower(s.category) AS category,
        c.estimated_revenue_index::float AS revenue_index,
        c.score_breakdown_json -> 'economics_detail' ->> 'value_band' AS value_band,
        (c.score_breakdown_json -> 'economics_detail' ->> 'value_band_low_confidence')::boolean
            AS value_band_low_confidence,
        (c.score_breakdown_json -> 'bonus_detail' ->> 'value_band_delta')::float
            AS value_band_delta
    FROM expansion_candidate c
    JOIN expansion_search s ON s.id = c.search_id
    LEFT JOIN expansion_brand_profile bp ON bp.search_id = s.id
    WHERE s.created_at >= now() - interval '90 days'
)
SELECT
    price_tier,
    COUNT(*) AS candidates,
    -- value_band distribution
    COUNT(*) FILTER (WHERE value_band = 'best_value')   AS best_value,
    COUNT(*) FILTER (WHERE value_band = 'neutral')      AS neutral,
    COUNT(*) FILTER (WHERE value_band = 'above_market') AS above_market,
    COUNT(*) FILTER (WHERE value_band IS NULL)          AS band_null,
    ROUND(100.0 * COUNT(*) FILTER (WHERE value_band = 'best_value') / NULLIF(COUNT(*), 0), 1)
        AS pct_best_value,
    ROUND(100.0 * COUNT(*) FILTER (WHERE value_band = 'above_market') / NULLIF(COUNT(*), 0), 1)
        AS pct_above_market,
    -- revenue-index saturation (multiplier-driven pinning at the 100 clamp)
    COUNT(*) FILTER (WHERE revenue_index >= 99.5) AS revidx_ge_99_5,
    ROUND(100.0 * COUNT(*) FILTER (WHERE revenue_index >= 99.5) / NULLIF(COUNT(*), 0), 1)
        AS pct_revidx_ge_99_5,
    ROUND(AVG(revenue_index)::numeric, 1) AS avg_revenue_index,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue_index)::numeric, 1)
        AS median_revenue_index,
    -- how often the band actually moved the score (+4 / -6 deltas applied)
    COUNT(*) FILTER (WHERE value_band_delta = 4.0)  AS uprank_applied,
    COUNT(*) FILTER (WHERE value_band_delta = -6.0) AS downrank_applied
FROM cands
GROUP BY price_tier
ORDER BY price_tier;

-- Same cut by (price_tier, category) for the standard categories, to match
-- the multiplier table in the report (burger/coffee/cafe/shawarma).
WITH cands AS (
    SELECT
        COALESCE(NULLIF(lower(bp.price_tier), ''), 'unset') AS price_tier,
        lower(s.category) AS category,
        c.estimated_revenue_index::float AS revenue_index,
        c.score_breakdown_json -> 'economics_detail' ->> 'value_band' AS value_band
    FROM expansion_candidate c
    JOIN expansion_search s ON s.id = c.search_id
    LEFT JOIN expansion_brand_profile bp ON bp.search_id = s.id
    WHERE s.created_at >= now() - interval '90 days'
)
SELECT
    price_tier,
    category,
    COUNT(*) AS candidates,
    ROUND(100.0 * COUNT(*) FILTER (WHERE value_band = 'best_value') / NULLIF(COUNT(*), 0), 1)
        AS pct_best_value,
    ROUND(100.0 * COUNT(*) FILTER (WHERE revenue_index >= 99.5) / NULLIF(COUNT(*), 0), 1)
        AS pct_revidx_ge_99_5,
    ROUND(MAX(revenue_index)::numeric, 1) AS max_revenue_index
FROM cands
WHERE category ~ '(burger|coffee|cafe|shawarma)'
GROUP BY price_tier, category
ORDER BY price_tier, category;
