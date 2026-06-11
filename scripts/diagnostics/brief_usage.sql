\pset footer off
--
-- brief_usage.sql  (Probe E — brand-brief redesign investigation)
--
-- Question: across all historical expansion_search rows, how often is each
-- brand-brief field actually non-default, and which values occur? If users
-- never touch a knob, retiring it (in favour of archetypes) is cheap; if
-- they do, the migration path matters.
--
-- Schema verified against alembic 20260310_exp_adv_v0 (expansion_search:
-- id, created_at, brand_name, category, service_model, target_districts,
-- min_area_m2, max_area_m2, target_area_m2, bbox, request_json, notes) and
-- 20260311_exp_adv_brand_v4 (expansion_brand_profile: search_id UNIQUE,
-- price_tier, average_check_sar, primary_channel, parking_sensitivity,
-- frontage_sensitivity, visibility_sensitivity, target_customer,
-- expansion_goal, cannibalization_tolerance_m, preferred_districts_json,
-- excluded_districts_json).
--
-- Defaults (app/services/expansion_advisor.py:_default_brand_profile):
--   price_tier NULL, average_check_sar NULL, primary_channel 'balanced',
--   parking/frontage/visibility_sensitivity 'medium',
--   expansion_goal 'balanced', cannibalization_tolerance_m 1800.0,
--   preferred/excluded districts [].
--
-- NOTE on coverage: persist_brand_profile only runs when the request body
-- contained a brand_profile object (app/api/expansion_advisor.py:1006-1007).
-- The UI always sends an all-neutral profile (ExpansionBriefForm.defaultBrief),
-- so a missing row means an API-only / scripted caller.

\echo ''
\echo '=== E.0 Coverage: searches vs persisted brand-profile rows ==='
SELECT
  COUNT(*)                                            AS searches_total,
  COUNT(bp.search_id)                                 AS with_profile_row,
  COUNT(*) - COUNT(bp.search_id)                      AS without_profile_row,
  ROUND(100.0 * COUNT(bp.search_id) / NULLIF(COUNT(*), 0), 1) AS pct_with_profile
FROM expansion_search s
LEFT JOIN expansion_brand_profile bp ON bp.search_id = s.id;

\echo ''
\echo '=== E.1 service_model distribution (all searches) ==='
SELECT
  service_model,
  COUNT(*)                          AS n,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
  MIN(created_at)::date             AS first_seen,
  MAX(created_at)::date             AS last_seen
FROM expansion_search
GROUP BY service_model
ORDER BY n DESC;

\echo ''
\echo '=== E.2 Per-knob non-default rate (profile rows only) ==='
WITH bp AS (
  SELECT
    bp.*,
    COALESCE(jsonb_array_length(preferred_districts_json), 0) AS n_pref,
    COALESCE(jsonb_array_length(excluded_districts_json), 0)  AS n_excl
  FROM expansion_brand_profile bp
)
SELECT knob, n_rows, n_non_default,
       ROUND(100.0 * n_non_default / NULLIF(n_rows, 0), 1) AS pct_non_default
FROM (
  SELECT 'price_tier' AS knob, COUNT(*) AS n_rows,
         COUNT(*) FILTER (WHERE price_tier IS NOT NULL) AS n_non_default FROM bp
  UNION ALL
  SELECT 'average_check_sar', COUNT(*),
         COUNT(*) FILTER (WHERE average_check_sar IS NOT NULL) FROM bp
  UNION ALL
  SELECT 'primary_channel', COUNT(*),
         COUNT(*) FILTER (WHERE COALESCE(primary_channel, 'balanced') <> 'balanced') FROM bp
  UNION ALL
  SELECT 'parking_sensitivity', COUNT(*),
         COUNT(*) FILTER (WHERE COALESCE(parking_sensitivity, 'medium') <> 'medium') FROM bp
  UNION ALL
  SELECT 'frontage_sensitivity', COUNT(*),
         COUNT(*) FILTER (WHERE COALESCE(frontage_sensitivity, 'medium') <> 'medium') FROM bp
  UNION ALL
  SELECT 'visibility_sensitivity', COUNT(*),
         COUNT(*) FILTER (WHERE COALESCE(visibility_sensitivity, 'medium') <> 'medium') FROM bp
  UNION ALL
  SELECT 'expansion_goal', COUNT(*),
         COUNT(*) FILTER (WHERE COALESCE(expansion_goal, 'balanced') <> 'balanced') FROM bp
  UNION ALL
  SELECT 'cannibalization_tolerance_m', COUNT(*),
         COUNT(*) FILTER (WHERE cannibalization_tolerance_m IS NOT NULL
                            AND cannibalization_tolerance_m <> 1800.0) FROM bp
  UNION ALL
  SELECT 'preferred_districts', COUNT(*),
         COUNT(*) FILTER (WHERE n_pref > 0) FROM bp
  UNION ALL
  SELECT 'excluded_districts', COUNT(*),
         COUNT(*) FILTER (WHERE n_excl > 0) FROM bp
  UNION ALL
  SELECT 'target_customer (dead col)', COUNT(*),
         COUNT(*) FILTER (WHERE target_customer IS NOT NULL) FROM bp
) t
ORDER BY pct_non_default DESC NULLS LAST;

\echo ''
\echo '=== E.3 Value distributions for the enum knobs ==='
SELECT 'primary_channel' AS knob, COALESCE(primary_channel, '(null)') AS value, COUNT(*) AS n
FROM expansion_brand_profile GROUP BY 2
UNION ALL
SELECT 'expansion_goal', COALESCE(expansion_goal, '(null)'), COUNT(*)
FROM expansion_brand_profile GROUP BY 2
UNION ALL
SELECT 'price_tier', COALESCE(price_tier, '(null)'), COUNT(*)
FROM expansion_brand_profile GROUP BY 2
UNION ALL
SELECT 'parking_sensitivity', COALESCE(parking_sensitivity, '(null)'), COUNT(*)
FROM expansion_brand_profile GROUP BY 2
UNION ALL
SELECT 'frontage_sensitivity', COALESCE(frontage_sensitivity, '(null)'), COUNT(*)
FROM expansion_brand_profile GROUP BY 2
UNION ALL
SELECT 'visibility_sensitivity', COALESCE(visibility_sensitivity, '(null)'), COUNT(*)
FROM expansion_brand_profile GROUP BY 2
ORDER BY 1, n DESC;

\echo ''
\echo '=== E.4 cannibalization_tolerance_m distribution (non-default only) ==='
SELECT
  cannibalization_tolerance_m AS tolerance_m,
  COUNT(*)                    AS n
FROM expansion_brand_profile
WHERE cannibalization_tolerance_m IS NOT NULL
  AND cannibalization_tolerance_m <> 1800.0
GROUP BY 1
ORDER BY n DESC, 1;

\echo ''
\echo '=== E.5 Reweight-active briefs (would have triggered _brand_weight_multipliers) ==='
-- Trigger condition mirrors app/services/expansion_advisor.py:3479-3505:
--   * channel in (delivery, dine_in), OR
--   * goal in (flagship, delivery_led, neighborhood), OR
--   * MAX of the three site sigs != 0. With sig(low)=-0.75, sig(medium)=0,
--     sig(high)=1, the max is non-zero only if (a) any knob is high, or
--     (b) ALL THREE are low. A single 'low' with the others 'medium' is a
--     weight-domain NO-OP today (max picks the medium 0).
WITH bp AS (
  SELECT *,
    (COALESCE(parking_sensitivity,'medium')    = 'high' OR
     COALESCE(frontage_sensitivity,'medium')   = 'high' OR
     COALESCE(visibility_sensitivity,'medium') = 'high') AS any_high,
    (COALESCE(parking_sensitivity,'medium')    = 'low' AND
     COALESCE(frontage_sensitivity,'medium')   = 'low' AND
     COALESCE(visibility_sensitivity,'medium') = 'low') AS all_low
  FROM expansion_brand_profile
)
SELECT
  COUNT(*) AS profile_rows,
  COUNT(*) FILTER (WHERE COALESCE(primary_channel,'balanced') <> 'balanced'
                      OR COALESCE(expansion_goal,'balanced')  <> 'balanced'
                      OR any_high OR all_low)               AS reweight_active,
  COUNT(*) FILTER (WHERE COALESCE(primary_channel,'balanced') <> 'balanced') AS via_channel,
  COUNT(*) FILTER (WHERE COALESCE(expansion_goal,'balanced')  <> 'balanced') AS via_goal,
  COUNT(*) FILTER (WHERE any_high)                          AS via_any_high_sensitivity,
  COUNT(*) FILTER (WHERE all_low)                           AS via_all_low_sensitivity,
  -- single/double 'low' knobs that today silently do nothing in the
  -- weight domain (the max() in _brand_weight_multipliers eats them):
  COUNT(*) FILTER (WHERE NOT any_high AND NOT all_low AND (
     COALESCE(parking_sensitivity,'medium')    = 'low' OR
     COALESCE(frontage_sensitivity,'medium')   = 'low' OR
     COALESCE(visibility_sensitivity,'medium') = 'low')) AS low_knob_noop_rows
FROM bp;

\echo ''
\echo '=== E.6 Reweight-active searches over time (monthly) ==='
WITH bp AS (
  SELECT search_id,
    (COALESCE(primary_channel,'balanced') <> 'balanced'
     OR COALESCE(expansion_goal,'balanced') <> 'balanced'
     OR COALESCE(parking_sensitivity,'medium')    = 'high'
     OR COALESCE(frontage_sensitivity,'medium')   = 'high'
     OR COALESCE(visibility_sensitivity,'medium') = 'high'
     OR (COALESCE(parking_sensitivity,'medium')    = 'low' AND
         COALESCE(frontage_sensitivity,'medium')   = 'low' AND
         COALESCE(visibility_sensitivity,'medium') = 'low')) AS active
  FROM expansion_brand_profile
)
SELECT
  date_trunc('month', s.created_at)::date AS month,
  COUNT(*)                                AS searches,
  COUNT(*) FILTER (WHERE bp.active)       AS reweight_active,
  ROUND(100.0 * COUNT(*) FILTER (WHERE bp.active) / NULLIF(COUNT(*),0), 1) AS pct_active
FROM expansion_search s
LEFT JOIN bp ON bp.search_id = s.id
GROUP BY 1
ORDER BY 1;

\echo ''
\echo '=== E.7 Cross-check: request_json briefs missing a profile row ==='
-- API callers that sent brand_profile in the request but whose persist
-- failed (persist_brand_profile swallows exceptions), or sent none at all.
SELECT
  COUNT(*) FILTER (WHERE s.request_json ? 'brand_profile'
                     AND s.request_json->'brand_profile' IS NOT NULL
                     AND s.request_json->>'brand_profile' IS NOT NULL
                     AND bp.search_id IS NULL)  AS sent_profile_but_no_row,
  COUNT(*) FILTER (WHERE (NOT s.request_json ? 'brand_profile'
                          OR s.request_json->>'brand_profile' IS NULL)) AS sent_no_profile
FROM expansion_search s
LEFT JOIN expansion_brand_profile bp ON bp.search_id = s.id;
