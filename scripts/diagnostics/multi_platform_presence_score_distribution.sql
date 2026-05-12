-- multi_platform_presence_score_distribution.sql
--
-- Distribution of multi_platform_presence_score across recent
-- expansion_candidate rows.
--
-- Persistence shape (verified in repo):
--   * multi_platform_presence_score IS persisted as a top-level numeric
--     column on expansion_candidate (set around
--     app/services/expansion_advisor.py:8960 and the INSERT block at :9185+).
--   * It is also mirrored under feature_snapshot_json for downstream
--     consumers — but the column read is the authoritative path.
--
-- The score is computed in _delivery_market_signal as:
--     (provider_platform_count / _active_platform_count) * 100
-- so when only HungerStation has rows in expansion_delivery_market,
-- _active_platform_count = 1 and the score saturates to either 0 or 100
-- depending on whether the candidate's catchment overlaps any listing.
-- That bimodal distribution is the smoking-gun visualization for the bug.
--
-- Run from Codespace:
--   psql "$DATABASE_URL" -f scripts/diagnostics/multi_platform_presence_score_distribution.sql

\pset format aligned
\pset border 2

\echo === multi_platform_presence_score binned distribution (30d) ===
SELECT
    width_bucket(multi_platform_presence_score, 0, 100, 10)    AS bucket_1_10,
    COUNT(*)                                                   AS candidate_count,
    MIN(multi_platform_presence_score)                         AS bucket_min,
    MAX(multi_platform_presence_score)                         AS bucket_max,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)         AS pct_of_total
FROM expansion_candidate
WHERE created_at >= now() - interval '30 days'
  AND multi_platform_presence_score IS NOT NULL
GROUP BY 1
ORDER BY 1;

\echo === Same as exact values (top 20 most common) ===
SELECT
    ROUND(multi_platform_presence_score::numeric, 1)           AS score_value,
    COUNT(*)                                                   AS candidate_count
FROM expansion_candidate
WHERE created_at >= now() - interval '30 days'
  AND multi_platform_presence_score IS NOT NULL
GROUP BY 1
ORDER BY candidate_count DESC, score_value
LIMIT 20;

\echo === provider_density_score vs multi_platform_presence_score correlation ===
-- If both saturate together (e.g. both 100 whenever any listing is found)
-- that confirms single-platform-data behavior.
SELECT
    width_bucket(provider_density_score, 0, 100, 5)            AS density_bucket_1_5,
    width_bucket(multi_platform_presence_score, 0, 100, 5)     AS mpp_bucket_1_5,
    COUNT(*)                                                   AS candidate_count
FROM expansion_candidate
WHERE created_at >= now() - interval '30 days'
  AND multi_platform_presence_score IS NOT NULL
  AND provider_density_score IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;
