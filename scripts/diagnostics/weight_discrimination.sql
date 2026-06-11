\pset footer off
--
-- weight_discrimination.sql  (Probe A — weight-stack investigation)
--
-- Core question: do nominal component weights translate into actual ranking
-- influence? A high-weight component whose contribution barely varies across
-- candidates within a search is dead weight; a low-weight component with a
-- wide spread punches above its class.
--
-- Method: take the most recent ~10 expansion_search rows that actually have
-- candidates, explode score_breakdown_json->'weighted_components' (the
-- per-component contribution in POINTS of final_score), compute per-search
-- stddev / p10 / p90 / (p90-p10), then average those per-search stats across
-- searches. Searches with < 8 candidates are skipped (too few rows for a
-- meaningful spread).
--
-- discrimination_index = mean per-search stddev of the contribution (pts).
--
-- NOTE: persisted final_score = weighted sum + bonus deltas
-- (score_breakdown_json->'bonus_detail'); the weighted_components here are
-- the pre-delta deterministic contributions, which is what we want.
--
-- Weight-stack aware by construction: components are exploded from each
-- row's weighted_components, so v2 rows (EXPANSION_WEIGHT_STACK=v2)
-- contribute district_momentum and simply have no confidence row (weight 0,
-- display-only) without any change here.
--
\echo ''
\echo '=== A. Per-component contribution discrimination (recent ~10 searches) ==='
WITH recent_searches AS (
  SELECT s.id
  FROM expansion_search s
  WHERE EXISTS (SELECT 1 FROM expansion_candidate c WHERE c.search_id = s.id)
  ORDER BY s.created_at DESC
  LIMIT 10
),
contrib AS (
  SELECT
    c.search_id,
    kv.key                                                   AS component,
    (kv.value)::numeric                                      AS pts,
    (c.score_breakdown_json->'weights'->>kv.key)::numeric    AS weight_pct
  FROM expansion_candidate c
  JOIN recent_searches rs ON rs.id = c.search_id
  CROSS JOIN LATERAL jsonb_each_text(c.score_breakdown_json->'weighted_components') AS kv(key, value)
  WHERE c.score_breakdown_json ? 'weighted_components'
),
per_search AS (
  SELECT
    search_id,
    component,
    COUNT(*)                                                       AS n,
    AVG(weight_pct)                                                AS weight_pct,
    AVG(pts)                                                       AS mean_pts,
    STDDEV_SAMP(pts)                                               AS sd_pts,
    percentile_cont(0.10) WITHIN GROUP (ORDER BY pts)              AS p10,
    percentile_cont(0.90) WITHIN GROUP (ORDER BY pts)              AS p90
  FROM contrib
  GROUP BY search_id, component
  HAVING COUNT(*) >= 8
)
SELECT
  component,
  COUNT(*)                                          AS n_searches,
  ROUND(AVG(weight_pct), 2)                         AS nominal_weight_pct,
  ROUND(AVG(mean_pts)::numeric, 2)                  AS mean_contribution,
  ROUND(AVG(p90 - p10)::numeric, 2)                 AS spread_p90_p10,
  ROUND(AVG(sd_pts)::numeric, 2)                    AS stddev,
  ROUND(AVG(sd_pts)::numeric, 2)                    AS discrimination_index,
  -- spread per weight point: how hard each nominal weight pct works
  ROUND((AVG(sd_pts) / NULLIF(AVG(weight_pct), 0))::numeric, 3) AS sd_per_weight_pct
FROM per_search
GROUP BY component
ORDER BY nominal_weight_pct DESC;

\echo ''
\echo '=== B. Rank influence: per-search corr(component pts, deterministic base) ==='
-- Pearson corr between each component contribution and the deterministic
-- base score (bonus_detail.base_deterministic when present, else
-- score_breakdown final_score). A high-weight component with near-zero corr
-- to the score that actually orders candidates is not driving rank.
WITH recent_searches AS (
  SELECT s.id
  FROM expansion_search s
  WHERE EXISTS (SELECT 1 FROM expansion_candidate c WHERE c.search_id = s.id)
  ORDER BY s.created_at DESC
  LIMIT 10
),
contrib AS (
  SELECT
    c.search_id,
    kv.key              AS component,
    (kv.value)::numeric AS pts,
    COALESCE(
      (c.score_breakdown_json->'bonus_detail'->>'base_deterministic')::numeric,
      (c.score_breakdown_json->>'final_score')::numeric,
      c.final_score::numeric
    ) AS base_score
  FROM expansion_candidate c
  JOIN recent_searches rs ON rs.id = c.search_id
  CROSS JOIN LATERAL jsonb_each_text(c.score_breakdown_json->'weighted_components') AS kv(key, value)
  WHERE c.score_breakdown_json ? 'weighted_components'
),
per_search AS (
  SELECT search_id, component,
         corr(pts::float8, base_score::float8) AS r,
         COUNT(*) AS n
  FROM contrib
  GROUP BY search_id, component
  HAVING COUNT(*) >= 8
)
SELECT
  component,
  COUNT(*)                       AS n_searches,
  ROUND(AVG(r)::numeric, 3)      AS mean_corr_with_base_score,
  ROUND(MIN(r)::numeric, 3)      AS min_corr,
  ROUND(MAX(r)::numeric, 3)      AS max_corr
FROM per_search
GROUP BY component
ORDER BY mean_corr_with_base_score DESC;
