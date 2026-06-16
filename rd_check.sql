\pset footer off
\echo ''
\echo '=== 1. TABLE SHAPE ==='
SELECT
  COUNT(*)                                   AS total_rows,
  COUNT(DISTINCT source_record_id)           AS distinct_branches,
  COUNT(DISTINCT captured_date)              AS distinct_days,
  MIN(captured_date)                         AS first_day,
  MAX(captured_date)                         AS last_day,
  (MAX(captured_date) - MIN(captured_date))  AS day_span,
  COUNT(DISTINCT platform)                   AS distinct_platforms
FROM expansion_delivery_rating_history;

\echo ''
\echo '=== 2. PER-DAY CAPTURE ==='
SELECT
  captured_date,
  COUNT(*)                           AS rows,
  COUNT(DISTINCT source_record_id)   AS branches,
  COUNT(DISTINCT platform)           AS platforms,
  SUM(CASE WHEN rating_count IS NULL THEN 1 ELSE 0 END) AS null_rc_rows
FROM expansion_delivery_rating_history
GROUP BY captured_date
ORDER BY captured_date DESC;

\echo ''
\echo '=== 3. PLATFORM SPLIT ==='
SELECT
  platform,
  COUNT(*)                           AS rows,
  COUNT(DISTINCT source_record_id)   AS branches,
  COUNT(DISTINCT captured_date)      AS days_covered,
  AVG(rating_count)::int             AS mean_rating_count,
  SUM(CASE WHEN geom IS NULL THEN 1 ELSE 0 END) AS null_geom_rows
FROM expansion_delivery_rating_history
GROUP BY platform
ORDER BY rows DESC;

\echo ''
\echo '=== 4. DELTA-READY BRANCHES ==='
WITH per_branch AS (
  SELECT source_record_id, COUNT(*) AS snap_count
  FROM expansion_delivery_rating_history
  WHERE captured_at >= now() - interval '30 days'
  GROUP BY source_record_id
)
SELECT
  COUNT(*)                               AS total_branches,
  COUNT(*) FILTER (WHERE snap_count = 1) AS one_snap_only,
  COUNT(*) FILTER (WHERE snap_count >= 2) AS delta_ready,
  ROUND(100.0 * COUNT(*) FILTER (WHERE snap_count >= 2)
        / NULLIF(COUNT(*),0), 1)          AS pct_delta_ready,
  MAX(snap_count)                        AS max_snaps
FROM per_branch;

\echo ''
\echo '=== 5. PER-BRANCH DELTA DISTRIBUTION ==='
WITH branch_delta AS (
  SELECT
    source_record_id,
    MAX(rating_count) - MIN(rating_count) AS delta
  FROM expansion_delivery_rating_history
  WHERE captured_at >= now() - interval '30 days'
    AND rating_count IS NOT NULL
  GROUP BY source_record_id
  HAVING COUNT(*) >= 2
)
SELECT
  COUNT(*)                                                 AS branches,
  COUNT(*) FILTER (WHERE delta = 0)                        AS zero_delta,
  COUNT(*) FILTER (WHERE delta > 0)                        AS positive_delta,
  ROUND(AVG(delta)::numeric, 2)                            AS mean_delta,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY delta)::int AS p50,
  percentile_cont(0.75) WITHIN GROUP (ORDER BY delta)::int AS p75,
  percentile_cont(0.90) WITHIN GROUP (ORDER BY delta)::int AS p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY delta)::int AS p95,
  MAX(delta)                                               AS max_delta
FROM branch_delta;

\echo ''
\echo '=== 6. CATEGORY COVERAGE ==='
WITH branch_delta AS (
  SELECT
    h.source_record_id,
    MAX(h.rating_count) - MIN(h.rating_count) AS delta,
    (ARRAY_AGG(h.category_raw ORDER BY h.captured_at DESC))[1] AS category_raw,
    (ARRAY_AGG(h.cuisine_raw  ORDER BY h.captured_at DESC))[1] AS cuisine_raw
  FROM expansion_delivery_rating_history h
  WHERE h.captured_at >= now() - interval '30 days'
    AND h.rating_count IS NOT NULL
  GROUP BY h.source_record_id
  HAVING COUNT(*) >= 2
)
SELECT
  v.cat_term,
  COUNT(*) FILTER (WHERE lower(COALESCE(bd.category_raw,'')) LIKE '%'||v.cat_term||'%'
                      OR lower(COALESCE(bd.cuisine_raw, '')) LIKE '%'||v.cat_term||'%')
                           AS matched_branches,
  SUM(bd.delta) FILTER (WHERE lower(COALESCE(bd.category_raw,'')) LIKE '%'||v.cat_term||'%'
                           OR lower(COALESCE(bd.cuisine_raw, '')) LIKE '%'||v.cat_term||'%')
                           AS total_delta
FROM branch_delta bd
CROSS JOIN (VALUES
  ('burger'),('shawarma'),('pizza'),('indian'),('arabic'),
  ('cafe'),('coffee'),('chicken'),('broasted'),('asian'),('dessert')
) AS v(cat_term)
GROUP BY v.cat_term
ORDER BY matched_branches DESC NULLS LAST;

\echo ''
\echo '=== 7. CATCHMENT SUM-DELTA ==='
WITH recent_candidates AS (
  SELECT id, geom
  FROM candidate_location
  WHERE geom IS NOT NULL
  ORDER BY created_at DESC NULLS LAST
  LIMIT 200
),
branch_delta_by_cat AS (
  SELECT
    v.cat_term,
    h.source_record_id,
    (ARRAY_AGG(h.geom ORDER BY h.captured_at DESC))[1] AS geom,
    MAX(h.rating_count) - MIN(h.rating_count) AS delta
  FROM expansion_delivery_rating_history h
  CROSS JOIN (VALUES ('burger'),('shawarma'),('cafe')) AS v(cat_term)
  WHERE h.captured_at >= now() - interval '30 days'
    AND h.rating_count IS NOT NULL
    AND h.geom IS NOT NULL
    AND (lower(COALESCE(h.category_raw,'')) LIKE '%'||v.cat_term||'%'
      OR lower(COALESCE(h.cuisine_raw, '')) LIKE '%'||v.cat_term||'%')
  GROUP BY v.cat_term, h.source_record_id
  HAVING COUNT(*) >= 2
),
catchment AS (
  SELECT
    v2.cat_term,
    c.id AS candidate_id,
    COALESCE(SUM(bd.delta), 0) AS sum_delta,
    COUNT(DISTINCT bd.source_record_id) FILTER (WHERE bd.delta IS NOT NULL) AS branch_count
  FROM recent_candidates c
  CROSS JOIN (VALUES ('burger'),('shawarma'),('cafe')) AS v2(cat_term)
  LEFT JOIN branch_delta_by_cat bd
    ON bd.cat_term = v2.cat_term
   AND ST_DWithin(bd.geom::geography, c.geom::geography, 1200)
  GROUP BY v2.cat_term, c.id
)
SELECT
  cat_term,
  COUNT(*)                                                     AS catchments,
  COUNT(*) FILTER (WHERE branch_count >= 3)                    AS passes_3_gate,
  ROUND(100.0 * COUNT(*) FILTER (WHERE branch_count >= 3)
        / NULLIF(COUNT(*),0), 1)                               AS pct_gate_pass,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY sum_delta)::int AS p50,
  percentile_cont(0.75) WITHIN GROUP (ORDER BY sum_delta)::int AS p75,
  percentile_cont(0.90) WITHIN GROUP (ORDER BY sum_delta)::int AS p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY sum_delta)::int AS p95,
  MAX(sum_delta)                                               AS max_sum_delta
FROM catchment
GROUP BY cat_term
ORDER BY cat_term;
