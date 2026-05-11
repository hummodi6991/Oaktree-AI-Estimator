-- ============================================================================
-- Diagnostic: why does "Provider platform count" render as 1 in production?
-- ============================================================================
--
-- Tables this query inspects (all populated by app/ingest/expansion_advisor_delivery.py
-- and app/ingest/restaurant_pois.py via app.delivery.pipeline):
--
--   * delivery_source_record           -- raw per-platform scraped rows
--   * expansion_delivery_market        -- Riyadh-normalized rows (= _EA_DELIVERY_TABLE)
--   * expansion_delivery_rating_history-- daily rating_count snapshots
--   * delivery_ingest_run              -- per-platform scrape run log
--
-- The production scoring path is
--   app/services/expansion_advisor.py:6929-6952
-- which is the bulk enrichment block that fires whenever
--   _cached_ea_table_has_rows(db, 'expansion_delivery_market')
-- returns true.  It computes provider_platform_count via
--   COUNT(DISTINCT d.platform)
--   FROM expansion_delivery_market d
--   ST_DWithin(d.geom::geography, candidate_point::geography, 1200)   -- HARD-CODED 1200 m
-- (radius is hard-coded 1200 m there, NOT the service-model-aware
--  _catchment_radii(...)['provider'] of 1500/3000/3500/1000 m used by
--  the LATERAL JOIN fallback at lines 6405-6429).
--
-- Run with:
--   psql "$DATABASE_URL" -f scripts/diagnostics/provider_platform_count_audit.sql
--
-- For Part B (point-specific catchment), edit the :lon / :lat / :radius_m values
-- below before running.
-- ============================================================================


\echo
\echo ============================================================================
\echo  PART A1 — Distinct platform values in delivery_source_record
\echo ============================================================================

SELECT
    lower(COALESCE(platform, 'NULL')) AS platform,
    COUNT(*)                                                                AS rows_total,
    COUNT(*) FILTER (WHERE scraped_at >= now() - interval '7  days')        AS rows_last_7d,
    COUNT(*) FILTER (WHERE scraped_at >= now() - interval '30 days')        AS rows_last_30d,
    COUNT(*) FILTER (WHERE lat IS NOT NULL AND lon IS NOT NULL)             AS rows_with_coords,
    MIN(scraped_at)                                                         AS first_scraped_at,
    MAX(scraped_at)                                                         AS last_scraped_at
FROM delivery_source_record
GROUP BY 1
ORDER BY rows_total DESC;


\echo
\echo ============================================================================
\echo  PART A2 — Distinct platform values in expansion_delivery_market
\echo            (this is the table the production scoring path actually reads)
\echo ============================================================================

SELECT
    lower(COALESCE(platform, 'NULL')) AS platform,
    COUNT(*)                                                                AS rows_total,
    COUNT(*) FILTER (WHERE scraped_at >= now() - interval '7  days')        AS rows_last_7d,
    COUNT(*) FILTER (WHERE scraped_at >= now() - interval '30 days')        AS rows_last_30d,
    COUNT(*) FILTER (WHERE geom IS NOT NULL)                                AS rows_with_geom,
    MIN(scraped_at)                                                         AS first_scraped_at,
    MAX(scraped_at)                                                         AS last_scraped_at
FROM expansion_delivery_market
WHERE city = 'riyadh'
GROUP BY 1
ORDER BY rows_total DESC;


\echo
\echo ============================================================================
\echo  PART A3 — Distinct platform values in expansion_delivery_rating_history
\echo ============================================================================

SELECT
    lower(COALESCE(platform, 'NULL')) AS platform,
    COUNT(*)                                                                AS snapshots_total,
    COUNT(DISTINCT source_record_id)                                        AS unique_source_records,
    COUNT(*) FILTER (WHERE captured_at >= now() - interval '7  days')       AS snapshots_last_7d,
    COUNT(*) FILTER (WHERE captured_at >= now() - interval '30 days')       AS snapshots_last_30d,
    MIN(captured_at)                                                        AS first_capture,
    MAX(captured_at)                                                        AS last_capture
FROM expansion_delivery_rating_history
GROUP BY 1
ORDER BY snapshots_total DESC;


\echo
\echo ============================================================================
\echo  PART A4 — Sanity: source column on restaurant_poi (cross-check)
\echo ============================================================================

SELECT
    lower(COALESCE(source, 'NULL')) AS source,
    COUNT(*) AS rows_total
FROM restaurant_poi
WHERE source IS NOT NULL
GROUP BY 1
ORDER BY rows_total DESC
LIMIT 30;


\echo
\echo ============================================================================
\echo  PART B — Platforms within the catchment around a candidate point
\echo            (edit :lon / :lat / :radius_m to your candidate before running)
\echo
\echo            Production scoring path uses radius = 1200 m and reads from
\echo            expansion_delivery_market (the EA-normalized table).
\echo            The LATERAL fallback uses _catchment_radii(service_model)
\echo            = {qsr:1500, cafe:1000, dine_in:3500, delivery_first:3000}.
\echo ============================================================================

-- ───── Edit me ─────────────────────────────────────────────────────────────
\set lon       46.6753   -- e.g. Al Olaya
\set lat       24.7136
\set radius_m  1200       -- match the hard-coded 1200 m in the bulk path
-- ───────────────────────────────────────────────────────────────────────────

-- B1: production-path counterpart (expansion_delivery_market, 1200 m hard-coded)
\echo --- B1: expansion_delivery_market within :radius_m m of (:lon, :lat) ---

SELECT
    lower(COALESCE(platform, 'NULL')) AS platform,
    COUNT(*) AS listings_in_catchment,
    MAX(scraped_at) AS most_recent
FROM expansion_delivery_market
WHERE city = 'riyadh'
  AND geom IS NOT NULL
  AND ST_DWithin(
        geom::geography,
        ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
        :radius_m
      )
GROUP BY 1
ORDER BY listings_in_catchment DESC;

-- B2: LATERAL-fallback counterpart (delivery_source_record, same radius)
\echo --- B2: delivery_source_record within :radius_m m of (:lon, :lat) ---

SELECT
    lower(COALESCE(platform, 'NULL')) AS platform,
    COUNT(*) AS listings_in_catchment,
    COUNT(*) FILTER (WHERE scraped_at >= now() - interval '30 days') AS last_30d,
    MAX(scraped_at) AS most_recent
FROM delivery_source_record
WHERE lat IS NOT NULL AND lon IS NOT NULL
  AND ST_DWithin(
        ST_SetSRID(ST_MakePoint(lon::double precision, lat::double precision), 4326)::geography,
        ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
        :radius_m
      )
GROUP BY 1
ORDER BY listings_in_catchment DESC;

-- B3: exact value the production scoring path would report for this point
\echo --- B3: provider_platform_count as the production path would compute it ---

SELECT
    COUNT(*)                                AS provider_listing_count,
    COUNT(DISTINCT lower(platform))         AS provider_platform_count
FROM expansion_delivery_market
WHERE city = 'riyadh'
  AND geom IS NOT NULL
  AND ST_DWithin(
        geom::geography,
        ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
        :radius_m
      );


\echo
\echo ============================================================================
\echo  PART C — Most recent ingestion timestamp per platform
\echo            (detects scrapers that are wired up but silently stalled)
\echo ============================================================================

-- C1: from delivery_ingest_run (canonical ingest log)
\echo --- C1: delivery_ingest_run ---

SELECT
    lower(COALESCE(platform, 'NULL')) AS platform,
    COUNT(*)                                                  AS runs_total,
    COUNT(*) FILTER (WHERE status = 'completed')              AS runs_ok,
    COUNT(*) FILTER (WHERE status = 'failed')                 AS runs_failed,
    COUNT(*) FILTER (WHERE status = 'completed_with_errors')  AS runs_partial,
    MAX(started_at)                                           AS last_started_at,
    MAX(finished_at)                                          AS last_finished_at,
    SUM(rows_inserted)                                        AS lifetime_rows_inserted,
    SUM(rows_updated)                                         AS lifetime_rows_updated
FROM delivery_ingest_run
GROUP BY 1
ORDER BY last_started_at DESC NULLS LAST;

-- C2: most recent row actually written, per table
\echo --- C2: most recent scraped_at per platform across all tables ---

WITH dsr AS (
    SELECT lower(platform) AS platform, MAX(scraped_at) AS last_seen
    FROM delivery_source_record
    GROUP BY 1
),
edm AS (
    SELECT lower(platform) AS platform, MAX(scraped_at) AS last_seen
    FROM expansion_delivery_market
    WHERE city = 'riyadh'
    GROUP BY 1
),
edrh AS (
    SELECT lower(platform) AS platform, MAX(captured_at) AS last_seen
    FROM expansion_delivery_rating_history
    GROUP BY 1
)
SELECT
    COALESCE(dsr.platform, edm.platform, edrh.platform) AS platform,
    dsr.last_seen  AS last_in_delivery_source_record,
    edm.last_seen  AS last_in_expansion_delivery_market,
    edrh.last_seen AS last_in_rating_history,
    EXTRACT(epoch FROM (now() - GREATEST(
        COALESCE(dsr.last_seen,  'epoch'::timestamptz),
        COALESCE(edm.last_seen,  'epoch'::timestamptz),
        COALESCE(edrh.last_seen, 'epoch'::timestamptz)
    )))::int / 86400 AS days_since_most_recent
FROM dsr
FULL OUTER JOIN edm  USING (platform)
FULL OUTER JOIN edrh USING (platform)
ORDER BY days_since_most_recent ASC NULLS LAST;


\echo
\echo ============================================================================
\echo  PART D — Sanity cross-check: registry vs reality
\echo
\echo  The SCRAPER_REGISTRY in app/connectors/delivery_platforms.py registers
\echo  14 platforms: hungerstation, talabat, mrsool, jahez, toyou, keeta,
\echo  thechefz, lugmety, shgardi, ninja, nana, dailymealz, careemfood, deliveroo.
\echo
\echo  If part A2 (expansion_delivery_market) returns ONE row (hungerstation),
\echo  the scheduled cron in expansion-advisor-data-delivery-sccc.yml
\echo  (default platforms=hungerstation) is the proximate cause — see synthesis.
\echo ============================================================================
