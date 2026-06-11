\pset footer off
--
-- economics_concentration.sql  (Probe D — weight-stack investigation)
--
-- occupancy_economics carries 26.3% of final_score. Is that weight producing
-- genuine candidate separation, or are most candidates clustered in a narrow
-- band (e.g. rent-percentile compressing, revenue_index dominated by
-- near-constant inputs)?
--
-- Sections:
--   A. distribution of the economics CONTRIBUTION (pts) and raw input,
--      pooled and per-search-averaged.
--   B. clustering: share of candidates within +/-2 pts of their search's
--      median economics contribution.
--   C. rent_burden mode/source mix + rent_burden_score and percentile
--      distribution from score_breakdown_json->'economics_detail'.
--   D. rent_burden_weight actually applied (rb_confidence damping) — when
--      this collapses to 0, economics is ~58% revenue_index by construction.
--
\echo ''
\echo '=== A. Economics contribution distribution (recent ~10 searches) ==='
WITH recent_searches AS (
  SELECT s.id FROM expansion_search s
  WHERE EXISTS (SELECT 1 FROM expansion_candidate c WHERE c.search_id = s.id)
  ORDER BY s.created_at DESC LIMIT 10
),
econ AS (
  SELECT
    c.search_id,
    (c.score_breakdown_json->'weighted_components'->>'occupancy_economics')::numeric AS pts,
    (c.score_breakdown_json->'inputs'->>'occupancy_economics')::numeric              AS raw
  FROM expansion_candidate c
  JOIN recent_searches rs ON rs.id = c.search_id
  WHERE c.score_breakdown_json ? 'weighted_components'
)
SELECT
  COUNT(*)                                                          AS n_candidates,
  ROUND(AVG(pts)::numeric, 2)                                       AS mean_pts,
  ROUND(STDDEV_SAMP(pts)::numeric, 2)                               AS sd_pts,
  ROUND(percentile_cont(0.10) WITHIN GROUP (ORDER BY pts)::numeric, 2) AS p10_pts,
  ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY pts)::numeric, 2) AS p50_pts,
  ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY pts)::numeric, 2) AS p90_pts,
  ROUND(AVG(raw)::numeric, 2)                                       AS mean_raw,
  ROUND(STDDEV_SAMP(raw)::numeric, 2)                               AS sd_raw
FROM econ;

\echo ''
\echo '=== B. Clustering: share within +/-2 pts of per-search median ==='
WITH recent_searches AS (
  SELECT s.id FROM expansion_search s
  WHERE EXISTS (SELECT 1 FROM expansion_candidate c WHERE c.search_id = s.id)
  ORDER BY s.created_at DESC LIMIT 10
),
econ AS (
  SELECT c.search_id,
         (c.score_breakdown_json->'weighted_components'->>'occupancy_economics')::numeric AS pts
  FROM expansion_candidate c
  JOIN recent_searches rs ON rs.id = c.search_id
  WHERE c.score_breakdown_json ? 'weighted_components'
),
med AS (
  SELECT search_id,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY pts) AS med_pts,
         COUNT(*) AS n
  FROM econ GROUP BY search_id HAVING COUNT(*) >= 8
)
SELECT
  COUNT(DISTINCT e.search_id)                                              AS n_searches,
  COUNT(*)                                                                 AS n_candidates,
  ROUND(100.0 * SUM(CASE WHEN ABS(e.pts - m.med_pts) <= 2.0 THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                           AS pct_within_2pts_of_median,
  ROUND(100.0 * SUM(CASE WHEN ABS(e.pts - m.med_pts) <= 1.0 THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                           AS pct_within_1pt_of_median
FROM econ e
JOIN med m ON m.search_id = e.search_id;

\echo ''
\echo '=== C. rent_burden mode / source mix + percentile distribution ==='
WITH recent_searches AS (
  SELECT s.id FROM expansion_search s
  WHERE EXISTS (SELECT 1 FROM expansion_candidate c WHERE c.search_id = s.id)
  ORDER BY s.created_at DESC LIMIT 10
),
rb AS (
  SELECT
    c.score_breakdown_json->'economics_detail'->'rent_burden'->>'mode'         AS mode,
    c.score_breakdown_json->'economics_detail'->'rent_burden'->>'source_label' AS source_label,
    (c.score_breakdown_json->'economics_detail'->>'rent_burden_score')::numeric AS rent_burden_score,
    (c.score_breakdown_json->'economics_detail'->'rent_burden'->>'percentile')::numeric AS rent_percentile
  FROM expansion_candidate c
  JOIN recent_searches rs ON rs.id = c.search_id
  WHERE c.score_breakdown_json ? 'economics_detail'
)
SELECT
  COALESCE(mode, '(missing)')          AS mode,
  COALESCE(source_label, '(none)')     AS source_label,
  COUNT(*)                             AS n,
  ROUND(AVG(rent_burden_score)::numeric, 1)                                            AS mean_burden,
  ROUND(STDDEV_SAMP(rent_burden_score)::numeric, 1)                                    AS sd_burden,
  ROUND(percentile_cont(0.10) WITHIN GROUP (ORDER BY rent_percentile)::numeric, 3)     AS pctile_p10,
  ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY rent_percentile)::numeric, 3)     AS pctile_p50,
  ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY rent_percentile)::numeric, 3)     AS pctile_p90
FROM rb
GROUP BY mode, source_label
ORDER BY n DESC;

\echo ''
\echo '=== D. Applied rent_burden_weight (rb_confidence damping) distribution ==='
-- rent_burden_weight = 0.20 * rb_confidence; the deficit is absorbed by
-- revenue_index. 0.20 = full rent signal; 0.05/0.03 = damped citywide comps;
-- 0.00 = rent signal fully dropped (economics ~58% revenue_index).
WITH recent_searches AS (
  SELECT s.id FROM expansion_search s
  WHERE EXISTS (SELECT 1 FROM expansion_candidate c WHERE c.search_id = s.id)
  ORDER BY s.created_at DESC LIMIT 10
)
SELECT
  (c.score_breakdown_json->'economics_detail'->>'rent_burden_weight')::numeric AS rent_burden_weight,
  (c.score_breakdown_json->'economics_detail'->>'revenue_weight')::numeric     AS revenue_weight,
  COUNT(*) AS n
FROM expansion_candidate c
JOIN recent_searches rs ON rs.id = c.search_id
WHERE c.score_breakdown_json ? 'economics_detail'
GROUP BY 1, 2
ORDER BY n DESC;
