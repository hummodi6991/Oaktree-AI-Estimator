-- delivery_scraper_health.sql
--
-- Per-platform health summary derived from delivery_ingest_run.
--
-- For each platform, shows:
--   * total run count (any status)
--   * count of runs with status in ('completed', 'completed_with_errors')
--   * count of runs marked 'failed'
--   * most recent successful insert timestamp (last time rows_inserted > 0)
--   * rows_inserted in the trailing 30 / 90 / 365 days
--
-- This is the primary "is the scraper dead?" check.  A platform with
-- runs_30d > 0 AND inserted_30d = 0 AND status_completed_30d > 0 is the
-- failure mode described in the briefing: scraper runs to completion, but
-- inserts no rows.
--
-- Run from Codespace:
--   psql "$DATABASE_URL" -f scripts/diagnostics/delivery_scraper_health.sql
--
-- No psql :var substitutions — windows are baked in to keep iPad-Safari
-- copy/paste safe.

\pset format aligned
\pset border 2

SELECT
    platform,
    COUNT(*)                                                         AS runs_lifetime,
    COUNT(*) FILTER (WHERE status IN ('completed','completed_with_errors'))
                                                                     AS runs_completed_lifetime,
    COUNT(*) FILTER (WHERE status = 'failed')                        AS runs_failed_lifetime,
    SUM(rows_inserted)                                               AS rows_inserted_lifetime,
    MAX(started_at) FILTER (WHERE rows_inserted > 0)                 AS last_successful_insert_at,
    -- 30-day window
    COUNT(*) FILTER (WHERE started_at >= now() - interval '30 days') AS runs_30d,
    COUNT(*) FILTER (WHERE started_at >= now() - interval '30 days'
                       AND status IN ('completed','completed_with_errors'))
                                                                     AS runs_completed_30d,
    SUM(rows_inserted) FILTER (WHERE started_at >= now() - interval '30 days')
                                                                     AS rows_inserted_30d,
    -- 90-day window
    COUNT(*) FILTER (WHERE started_at >= now() - interval '90 days') AS runs_90d,
    SUM(rows_inserted) FILTER (WHERE started_at >= now() - interval '90 days')
                                                                     AS rows_inserted_90d,
    -- 365-day window
    COUNT(*) FILTER (WHERE started_at >= now() - interval '365 days')AS runs_365d,
    SUM(rows_inserted) FILTER (WHERE started_at >= now() - interval '365 days')
                                                                     AS rows_inserted_365d
FROM delivery_ingest_run
GROUP BY platform
ORDER BY rows_inserted_30d DESC NULLS LAST, platform;
