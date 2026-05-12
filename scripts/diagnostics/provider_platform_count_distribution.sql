-- provider_platform_count_distribution.sql
--
-- Quantitative check of the "every candidate sees provider_platform_count = 1"
-- claim.
--
-- provider_platform_count is the raw count of *distinct delivery platforms*
-- with at least one listing inside the catchment radius around each candidate
-- (currently a hard-coded 1200 m in the bulk-enrichment SQL — see
-- app/services/expansion_advisor.py).  When only one platform ('hungerstation')
-- has rows in expansion_delivery_market, every candidate's
-- provider_platform_count is forced to 0 or 1.
--
-- This query computes the distribution across expansion_candidate rows from
-- the trailing 30 days, sourced from feature_snapshot_json (which is where
-- the field is persisted; it is NOT a top-level column on expansion_candidate).
--
-- Run from Codespace:
--   psql "$DATABASE_URL" -f scripts/diagnostics/provider_platform_count_distribution.sql

\pset format aligned
\pset border 2

\echo === provider_platform_count distribution (last 30d candidates) ===
SELECT
    COALESCE((feature_snapshot_json ->> 'provider_platform_count')::int, -1)
                                            AS provider_platform_count,
    COUNT(*)                                AS candidate_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)
                                            AS pct_of_total
FROM expansion_candidate
WHERE created_at >= now() - interval '30 days'
  AND feature_snapshot_json IS NOT NULL
GROUP BY 1
ORDER BY 1;

\echo === Same, sliced by service_model for context ===
SELECT
    COALESCE(service_model, '(null)')       AS service_model,
    COALESCE((feature_snapshot_json ->> 'provider_platform_count')::int, -1)
                                            AS provider_platform_count,
    COUNT(*)                                AS candidate_count
FROM expansion_candidate
WHERE created_at >= now() - interval '30 days'
  AND feature_snapshot_json IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;
