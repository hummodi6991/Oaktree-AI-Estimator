\pset footer off
--
-- delivery_demand_legs_probe.sql
--
-- Per-service-model distribution of the realized-demand delivery leg, used
-- to anchor _REALIZED_DEMAND_REFERENCE in app/services/expansion_advisor.py
-- (delivery_first 307 / dine_in 402 / qsr 327, derived 2026-06-10 from a
-- 1,220-candidate trailing-30d probe). Run in Codespace against the
-- production DB.
--
-- Anchor rule (same as the original 2026-05-15 global calibration in
-- scripts/diagnostics/realized_demand_calibration.sql): take each model's
-- realized_demand_30d p75 from section A as its reference — p75 maps to a
-- score of 100, the median lands in the ~70s, only the top quartile
-- saturates. Counts are measured at the 1200 m
-- EXPANSION_REALIZED_DEMAND_RADIUS_M catchment; re-derive the anchors if
-- that radius ever changes. cafe is deliberately unanchored (no cafe rows
-- in the probe window) and falls back to the env default (263).
--
-- "Populated" = realized_demand_30d present AND realized_demand_branches
-- >= 3, the same gate the snapshot writer applies.
--
\echo ''
\echo '=== A. realized_demand_30d percentiles per service model (trailing 30d) ==='
SELECT
  s.service_model,
  COUNT(*)                                                            AS n,
  ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY rd)::numeric, 1) AS median,
  ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY rd)::numeric, 1) AS p75,
  ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY rd)::numeric, 1) AS p90
FROM (
  SELECT
    c.search_id,
    (c.feature_snapshot_json->>'realized_demand_30d')::numeric AS rd
  FROM expansion_candidate c
  WHERE c.feature_snapshot_json ? 'realized_demand_30d'
    AND (c.feature_snapshot_json->>'realized_demand_30d') IS NOT NULL
    AND COALESCE((c.feature_snapshot_json->>'realized_demand_branches')::numeric, 0) >= 3
) populated
JOIN expansion_search s ON s.id = populated.search_id
WHERE s.created_at >= now() - interval '30 days'
GROUP BY s.service_model
ORDER BY s.service_model;

\echo ''
\echo '=== B. Realized-leg score under the per-model anchors (post-deploy check) ==='
-- After deploying the re-anchor, run on fresh searches only (created_at
-- after the deploy). Healthy shape: realized_p50 in the ~75-90 band and
-- pct_at_ceiling roughly 25% — not the pre-fix 100/62.5% saturation.
SELECT
  s.service_model,
  COUNT(*) AS n,
  ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY score)::numeric, 1) AS realized_p50,
  ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY score)::numeric, 1) AS realized_p75,
  ROUND(100.0 * AVG((score >= 100)::int), 1) AS pct_at_ceiling
FROM (
  SELECT
    c.search_id,
    LEAST(100.0, sqrt(
      (c.feature_snapshot_json->>'realized_demand_30d')::numeric
      / CASE s2.service_model
          WHEN 'delivery_first' THEN 307.0
          WHEN 'dine_in'        THEN 402.0
          WHEN 'qsr'            THEN 327.0
          ELSE 263.0  -- env fallback (cafe / unknown models)
        END
    ) * 100.0) AS score
  FROM expansion_candidate c
  JOIN expansion_search s2 ON s2.id = c.search_id
  WHERE c.feature_snapshot_json ? 'realized_demand_30d'
    AND (c.feature_snapshot_json->>'realized_demand_30d') IS NOT NULL
    AND COALESCE((c.feature_snapshot_json->>'realized_demand_branches')::numeric, 0) >= 3
) scored
JOIN expansion_search s ON s.id = scored.search_id
WHERE s.created_at >= now() - interval '30 days'  -- post-deploy: tighten to the deploy timestamp
GROUP BY s.service_model
ORDER BY s.service_model;
