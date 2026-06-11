\pset footer off
--
-- contribution_vs_realized_demand.sql  (Probe B — weight-stack investigation)
--
-- For candidates where the realized-demand signal populates
-- (feature_snapshot_json->'realized_demand_30d' present AND
-- realized_demand_branches >= 3, the same gate the snapshot writer applies —
-- in practice mostly QSR runs), compute a per-search Spearman rank
-- correlation between each component's contribution (pts) and
-- realized_demand_30d, then average across searches. Also: final_score vs
-- realized_demand_30d.
--
-- Spearman is computed as Pearson corr over per-search ranks
-- (rank() OVER ...), which matches Spearman up to tie handling.
--
-- CAVEATS (read before interpreting):
--  * realized_demand_30d is itself an INPUT to demand_potential (the
--    realized leg of _delivery_score), so demand_potential's positive
--    correlation here is partly mechanical, not validation.
--  * realized_demand_30d is rating velocity of EXISTING same-category
--    branches in the catchment — an outcome proxy for the AREA, not for the
--    specific listing. It says nothing about rent economics ex post.
--  * The interesting cells are the heavy non-demand components
--    (occupancy_economics 26.3%, listing_quality 22%, landlord_signal 7%
--    under the v1 stack): near-zero or negative mean Spearman at >= 7%
--    weight is the flag.
--
-- Weight-stack aware by construction: components are exploded from each
-- row's weighted_components / weights, so v2 rows (EXPANSION_WEIGHT_STACK=v2)
-- contribute district_momentum and simply have no confidence row (weight 0,
-- display-only) without any change here.
--
\echo ''
\echo '=== A. Per-component Spearman vs realized_demand_30d (last 30 days) ==='
WITH base AS (
  SELECT
    c.search_id,
    c.id AS candidate_id,
    (c.feature_snapshot_json->>'realized_demand_30d')::numeric AS rd,
    c.score_breakdown_json->'weighted_components'              AS wc
  FROM expansion_candidate c
  JOIN expansion_search s ON s.id = c.search_id
  WHERE s.created_at >= now() - interval '30 days'
    AND c.feature_snapshot_json ? 'realized_demand_30d'
    AND (c.feature_snapshot_json->>'realized_demand_30d') IS NOT NULL
    AND COALESCE((c.feature_snapshot_json->>'realized_demand_branches')::numeric, 0) >= 3
    AND c.score_breakdown_json ? 'weighted_components'
),
long AS (
  SELECT b.search_id, b.candidate_id, b.rd,
         kv.key AS component, (kv.value)::numeric AS pts
  FROM base b
  CROSS JOIN LATERAL jsonb_each_text(b.wc) AS kv(key, value)
),
ranked AS (
  SELECT search_id, component,
         rank() OVER (PARTITION BY search_id, component ORDER BY pts) AS r_pts,
         rank() OVER (PARTITION BY search_id, component ORDER BY rd)  AS r_rd
  FROM long
),
per_search AS (
  SELECT search_id, component,
         corr(r_pts::float8, r_rd::float8) AS spearman,
         COUNT(*) AS n
  FROM ranked
  GROUP BY search_id, component
  HAVING COUNT(*) >= 8
),
weights AS (
  SELECT kv.key AS component, AVG((kv.value)::numeric) AS weight_pct
  FROM base b2
  JOIN expansion_candidate c2 ON c2.id = b2.candidate_id
  CROSS JOIN LATERAL jsonb_each_text(c2.score_breakdown_json->'weights') AS kv(key, value)
  GROUP BY kv.key
)
SELECT
  p.component,
  COUNT(*)                                  AS n_searches,
  ROUND(w.weight_pct, 2)                    AS weight_pct,
  ROUND(AVG(p.spearman)::numeric, 3)        AS mean_spearman_vs_rd,
  ROUND(MIN(p.spearman)::numeric, 3)        AS min_spearman,
  ROUND(MAX(p.spearman)::numeric, 3)        AS max_spearman,
  CASE
    WHEN w.weight_pct >= 7.0 AND AVG(p.spearman) < 0.10
      THEN '<< FLAG: >=7% weight, near-zero/negative corr'
    ELSE ''
  END AS flag
FROM per_search p
JOIN weights w ON w.component = p.component
GROUP BY p.component, w.weight_pct
ORDER BY w.weight_pct DESC;

\echo ''
\echo '=== B. final_score (and deterministic base) Spearman vs realized_demand_30d ==='
WITH base AS (
  SELECT
    c.search_id,
    (c.feature_snapshot_json->>'realized_demand_30d')::numeric AS rd,
    c.final_score::numeric                                     AS final_score,
    COALESCE(
      (c.score_breakdown_json->'bonus_detail'->>'base_deterministic')::numeric,
      c.final_score::numeric
    ) AS base_score
  FROM expansion_candidate c
  JOIN expansion_search s ON s.id = c.search_id
  WHERE s.created_at >= now() - interval '30 days'
    AND c.feature_snapshot_json ? 'realized_demand_30d'
    AND (c.feature_snapshot_json->>'realized_demand_30d') IS NOT NULL
    AND COALESCE((c.feature_snapshot_json->>'realized_demand_branches')::numeric, 0) >= 3
),
ranked AS (
  SELECT search_id,
         rank() OVER (PARTITION BY search_id ORDER BY final_score) AS r_final,
         rank() OVER (PARTITION BY search_id ORDER BY base_score)  AS r_base,
         rank() OVER (PARTITION BY search_id ORDER BY rd)          AS r_rd
  FROM base
),
per_search AS (
  SELECT search_id,
         corr(r_final::float8, r_rd::float8) AS sp_final,
         corr(r_base::float8,  r_rd::float8) AS sp_base,
         COUNT(*) AS n
  FROM ranked
  GROUP BY search_id
  HAVING COUNT(*) >= 8
)
SELECT
  COUNT(*)                              AS n_searches,
  ROUND(AVG(sp_final)::numeric, 3)      AS mean_spearman_final_vs_rd,
  ROUND(AVG(sp_base)::numeric, 3)       AS mean_spearman_base_vs_rd,
  ROUND(MIN(sp_final)::numeric, 3)      AS min_final,
  ROUND(MAX(sp_final)::numeric, 3)      AS max_final
FROM per_search;
