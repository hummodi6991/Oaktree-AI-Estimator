\pset footer off
--
-- realized_demand_calibration.sql
--
-- Trailing-90d distribution of realized_demand_30d across Riyadh expansion
-- candidates, used to calibrate the _delivery_score realized-demand
-- reference point (settings.EXPANSION_REALIZED_DEMAND_REFERENCE, currently
-- 200.0 as a placeholder). Run this in Codespace against the production
-- DB; feed the p75 from section C into the EXPANSION_REALIZED_DEMAND_REFERENCE
-- default with a dated calibration comment.
--
-- "Populated" means the candidate carries a realized_demand_30d value AND
-- realized_demand_branches >= 3 — the same minimum-branch gate the snapshot
-- writer applies (app/services/expansion_advisor.py). Candidates below that
-- gate never contribute a realized-demand score, so they are excluded from
-- the calibration distribution.
--
-- The product is Riyadh-only, so no city filter is applied. Trailing-90d is
-- derived from expansion_search.created_at (expansion_candidate has no own
-- timestamp).
--
\echo ''
\echo '=== A. Total candidate count (trailing 90d) ==='
SELECT
  COUNT(*) AS total_candidates
FROM expansion_candidate c
JOIN expansion_search s ON s.id = c.search_id
WHERE s.created_at >= now() - interval '90 days';

\echo ''
\echo '=== B. Candidates with realized_demand_30d populated (branches >= 3) ==='
SELECT
  COUNT(*) AS populated_candidates
FROM expansion_candidate c
JOIN expansion_search s ON s.id = c.search_id
WHERE s.created_at >= now() - interval '90 days'
  AND c.feature_snapshot_json ? 'realized_demand_30d'
  AND (c.feature_snapshot_json->>'realized_demand_30d') IS NOT NULL
  AND COALESCE((c.feature_snapshot_json->>'realized_demand_branches')::numeric, 0) >= 3;

\echo ''
\echo '=== C. realized_demand_30d percentiles over populated values ==='
SELECT
  COUNT(*)                                                            AS n,
  ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY rd), 1)           AS median,
  ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY rd), 1)           AS p75,
  ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY rd), 1)           AS p90,
  ROUND(percentile_cont(0.95) WITHIN GROUP (ORDER BY rd), 1)           AS p95
FROM (
  SELECT (c.feature_snapshot_json->>'realized_demand_30d')::numeric AS rd
  FROM expansion_candidate c
  JOIN expansion_search s ON s.id = c.search_id
  WHERE s.created_at >= now() - interval '90 days'
    AND c.feature_snapshot_json ? 'realized_demand_30d'
    AND (c.feature_snapshot_json->>'realized_demand_30d') IS NOT NULL
    AND COALESCE((c.feature_snapshot_json->>'realized_demand_branches')::numeric, 0) >= 3
) populated;
