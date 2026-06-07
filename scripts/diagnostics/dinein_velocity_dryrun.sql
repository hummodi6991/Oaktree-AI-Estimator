\pset footer off
--
-- dinein_velocity_dryrun.sql
--
-- QUESTION: Is there ANY latent temporal signal we could mine for dine-in
-- review velocity TODAY, without building a new snapshot table? Expected
-- answer: NO. restaurant_poi holds one row per POI (id PK) with a single
-- review_count + single google_fetched_at — point-in-time only, no history.
-- This proves it rather than assuming it, and sanity-checks that the existing
-- delivery history table carries no dine-in rows.
--
-- HOW TO READ:
--   A. One row per POI? COUNT(*) == COUNT(DISTINCT id) confirms there is no
--      per-POI time-series; each POI carries exactly one review_count.
--   B. Distinct google_fetched_at values = enrichment batch dates, NOT
--      multiple observations of the same POI. Many rows can share a fetch
--      date; none has two.
--   C. raw JSONB top-level keys (top 40). If no key holds an array/history of
--      prior counts, there is no stashed time-series to recover. (Look for
--      anything like *history*, *previous*, *snapshot* — expected: none.)
--   D. Explicit probe for history-shaped keys in raw.
--   E. Sanity: expansion_delivery_rating_history platform breakdown — every
--      row is a delivery platform (hungerstation/jahez/keeta/talabat/mrsool);
--      there is no dine-in-tagged row. Confirms the delivery history table is
--      not already carrying dine-in data.
--
-- Riyadh-only product; no city filter needed.
--
\echo ''
\echo '=== A. One row per POI (no per-POI time-series) ==='
SELECT
  COUNT(*)             AS total_rows,
  COUNT(DISTINCT id)   AS distinct_poi_ids,
  COUNT(*) FILTER (WHERE review_count IS NOT NULL)      AS rows_with_review_count,
  COUNT(DISTINCT google_fetched_at)                     AS distinct_fetch_timestamps
FROM restaurant_poi;

\echo ''
\echo '=== B. Distinct google_fetched_at = batch dates, not repeat observations ==='
SELECT
  (google_fetched_at AT TIME ZONE 'UTC')::date AS fetched_date,
  COUNT(*)                                      AS rows_fetched_that_day
FROM restaurant_poi
WHERE google_fetched_at IS NOT NULL
GROUP BY (google_fetched_at AT TIME ZONE 'UTC')::date
ORDER BY fetched_date DESC
LIMIT 40;

\echo ''
\echo '=== C. raw JSONB top-level keys (top 40 by frequency) ==='
SELECT
  k          AS raw_key,
  COUNT(*)   AS rows_with_key
FROM restaurant_poi rp
CROSS JOIN LATERAL jsonb_object_keys(rp.raw) AS k
WHERE rp.raw IS NOT NULL
  AND jsonb_typeof(rp.raw) = 'object'
GROUP BY k
ORDER BY rows_with_key DESC
LIMIT 40;

\echo ''
\echo '=== D. Probe for any history/previous/snapshot-shaped key in raw ==='
SELECT
  k          AS suspicious_raw_key,
  COUNT(*)   AS rows_with_key
FROM restaurant_poi rp
CROSS JOIN LATERAL jsonb_object_keys(rp.raw) AS k
WHERE rp.raw IS NOT NULL
  AND jsonb_typeof(rp.raw) = 'object'
  AND (
        lower(k) LIKE '%history%'
     OR lower(k) LIKE '%previous%'
     OR lower(k) LIKE '%prior%'
     OR lower(k) LIKE '%snapshot%'
     OR lower(k) LIKE '%series%'
     OR lower(k) LIKE '%review_count%'
     OR lower(k) LIKE '%user_ratings%'
  )
GROUP BY k
ORDER BY rows_with_key DESC;

\echo ''
\echo '=== E. Sanity: delivery rating-history is delivery-only (no dine-in tag) ==='
SELECT
  lower(COALESCE(platform, '(null)')) AS platform,
  COUNT(*)                            AS rows,
  COUNT(DISTINCT source_record_id)    AS distinct_branches,
  MIN(captured_date)                  AS first_capture,
  MAX(captured_date)                  AS last_capture
FROM expansion_delivery_rating_history
GROUP BY lower(COALESCE(platform, '(null)'))
ORDER BY rows DESC;
