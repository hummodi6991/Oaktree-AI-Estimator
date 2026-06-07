\pset footer off
--
-- dinein_poi_coverage.sql
--
-- QUESTION: Do we have enough fresh, populated review_count data on
-- restaurant_poi to even consider a dine-in realized-demand (review-velocity)
-- signal? review_count on restaurant_poi is POINT-IN-TIME ONLY (one column,
-- no history table), so this measures the *base material*, not velocity.
--
-- HOW TO READ:
--   A. Row coverage — how many POIs carry review_count / google_place_id.
--   B. Staleness of google_fetched_at — the only timestamp we have for the
--      review data; paid enrichment cron is DISABLED
--      (.github/workflows/enrich-google-reviews.yml:22-35), so expect drift.
--   C. business_status breakdown — the advisor counts only NULL/OPERATIONAL
--      rows as live competition (app/services/expansion_advisor.py:6582-6583).
--   D/E. review_count distribution overall, then restricted to the same
--      live-venue filter the Expansion Advisor applies to restaurant_poi
--      (business_status IS NULL OR 'OPERATIONAL'). NOTE: restaurant_poi is
--      *entirely* restaurants; there is no fixed "restaurant-suitable category"
--      subset — `category` only distinguishes cuisine (burger/pizza/...). The
--      advisor's same-category filter is per user-search
--      (lower(rp.category)=ANY(keys), app/services/expansion_advisor.py:6571),
--      so the closest reusable global filter is the business_status one.
--   F. review_count by category — shows which cuisines carry usable counts.
--
-- Riyadh-only product; no city filter needed.
--
\echo ''
\echo '=== A. restaurant_poi row coverage ==='
SELECT
  COUNT(*)                                              AS total_rows,
  COUNT(*) FILTER (WHERE review_count IS NOT NULL)      AS rows_with_review_count,
  COUNT(*) FILTER (WHERE google_place_id IS NOT NULL)   AS rows_with_google_place_id,
  COUNT(*) FILTER (WHERE google_fetched_at IS NOT NULL) AS rows_with_google_fetched_at,
  ROUND(100.0 * COUNT(*) FILTER (WHERE review_count IS NOT NULL)
        / NULLIF(COUNT(*), 0), 1)                       AS pct_with_review_count
FROM restaurant_poi;

\echo ''
\echo '=== B. Staleness of google_fetched_at (the only review-data timestamp) ==='
SELECT
  COUNT(*)                                                                       AS n_fetched,
  MIN(google_fetched_at)                                                         AS min_fetched_at,
  percentile_disc(0.10) WITHIN GROUP (ORDER BY google_fetched_at)                AS p10_fetched_at,
  percentile_disc(0.50) WITHIN GROUP (ORDER BY google_fetched_at)                AS p50_fetched_at,
  percentile_disc(0.90) WITHIN GROUP (ORDER BY google_fetched_at)                AS p90_fetched_at,
  MAX(google_fetched_at)                                                         AS max_fetched_at,
  COUNT(*) FILTER (WHERE google_fetched_at >= now() - interval '150 days')       AS fresh_le_150d,
  COUNT(*) FILTER (WHERE google_fetched_at <  now() - interval '150 days')       AS stale_gt_150d
FROM restaurant_poi
WHERE google_fetched_at IS NOT NULL;

\echo ''
\echo '=== C. business_status breakdown ==='
SELECT
  COALESCE(business_status, '(null)') AS business_status,
  COUNT(*)                            AS rows
FROM restaurant_poi
GROUP BY COALESCE(business_status, '(null)')
ORDER BY rows DESC;

\echo ''
\echo '=== D. review_count distribution — ALL rows with review_count ==='
SELECT
  COUNT(*)                                                              AS n,
  ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY review_count)::numeric, 1) AS median,
  ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY review_count)::numeric, 1) AS p75,
  ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY review_count)::numeric, 1) AS p90,
  ROUND(percentile_cont(0.95) WITHIN GROUP (ORDER BY review_count)::numeric, 1) AS p95,
  MAX(review_count)                                                     AS max
FROM restaurant_poi
WHERE review_count IS NOT NULL;

\echo ''
\echo '=== E. review_count distribution — live venues (NULL/OPERATIONAL only) ==='
\echo '    (mirrors the advisor live-competition filter, expansion_advisor.py:6582-6583)'
SELECT
  COUNT(*)                                                              AS n,
  ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY review_count)::numeric, 1) AS median,
  ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY review_count)::numeric, 1) AS p75,
  ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY review_count)::numeric, 1) AS p90,
  ROUND(percentile_cont(0.95) WITHIN GROUP (ORDER BY review_count)::numeric, 1) AS p95,
  MAX(review_count)                                                     AS max
FROM restaurant_poi
WHERE review_count IS NOT NULL
  AND (business_status IS NULL OR business_status = 'OPERATIONAL');

\echo ''
\echo '=== F. review_count coverage by category (top 30 by populated rows) ==='
SELECT
  lower(COALESCE(category, '(null)'))                       AS category,
  COUNT(*)                                                  AS total_rows,
  COUNT(*) FILTER (WHERE review_count IS NOT NULL)          AS rows_with_review_count,
  ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY review_count)
        FILTER (WHERE review_count IS NOT NULL)::numeric, 1) AS median_review_count
FROM restaurant_poi
GROUP BY lower(COALESCE(category, '(null)'))
ORDER BY rows_with_review_count DESC
LIMIT 30;
