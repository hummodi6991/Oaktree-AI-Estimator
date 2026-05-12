-- delivery_platform_distribution.sql
--
-- Per-platform footprint in the two normalized delivery tables.
--
-- expansion_delivery_market is the table read by the Expansion Advisor
-- bulk-enrichment block (see app/services/expansion_advisor.py around the
-- ST_DWithin LEFT JOIN), so its distribution directly drives
-- provider_platform_count and provider_listing_count in the scoring.
--
-- delivery_source_record is the raw upsert target of the scraper pipeline
-- (app/delivery/pipeline.py), before normalization into expansion_delivery_market.
--
-- If only one platform ('hungerstation') has rows, the field
-- provider_platform_count maxes out at 1 across the city — which makes
-- multi_platform_presence_score collapse to 100/_active_platform_count.
--
-- Run from Codespace:
--   psql "$DATABASE_URL" -f scripts/diagnostics/delivery_platform_distribution.sql

\pset format aligned
\pset border 2

\echo === expansion_delivery_market (normalized, used by EA scoring) ===
SELECT
    platform,
    COUNT(*)                                    AS row_count,
    MIN(scraped_at)                             AS first_seen,
    MAX(scraped_at)                             AS last_seen,
    COUNT(*) FILTER (WHERE geom IS NOT NULL)    AS rows_with_geom,
    COUNT(DISTINCT district)                    AS distinct_districts
FROM expansion_delivery_market
WHERE city = 'riyadh' OR city IS NULL
GROUP BY platform
ORDER BY row_count DESC;

\echo === delivery_source_record (raw upsert target) ===
SELECT
    platform,
    COUNT(*)                                    AS row_count,
    MIN(scraped_at)                             AS first_seen,
    MAX(scraped_at)                             AS last_seen,
    COUNT(*) FILTER (WHERE lat IS NOT NULL AND lon IS NOT NULL)
                                                AS rows_with_coords,
    COUNT(*) FILTER (WHERE location_confidence >= 0.7)
                                                AS rows_high_confidence,
    AVG(location_confidence)::numeric(4,3)      AS avg_location_confidence
FROM delivery_source_record
GROUP BY platform
ORDER BY row_count DESC;
