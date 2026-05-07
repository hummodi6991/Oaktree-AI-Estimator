\timing on
\pset pager off

\echo '=== 1. RPC distribution across last 30d ==='
SELECT
  PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY rpc) AS p10,
  PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY rpc) AS p25,
  PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY rpc) AS p50,
  PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY rpc) AS p75,
  PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY rpc) AS p90,
  ROUND(AVG(rpc)::numeric, 4) AS mean_rpc,
  COUNT(*) AS n
FROM (
  SELECT
    NULLIF((feature_snapshot_json->>'estimated_annual_rent_sar')::float, 0)
      / NULLIF((feature_snapshot_json->>'population_reach')::float, 0) AS rpc
  FROM expansion_candidate
  WHERE computed_at >= NOW() - INTERVAL '30 days'
    AND (feature_snapshot_json->>'estimated_annual_rent_sar') IS NOT NULL
    AND (feature_snapshot_json->>'population_reach') IS NOT NULL
) sub
WHERE rpc IS NOT NULL;

\echo '=== 2. RPC demote firing rate (last 24h) ==='
SELECT
  COALESCE((score_breakdown_json->'market_viability_flag'->>'rent_per_capita_demote')::text, 'null') AS rpc_demote,
  COUNT(*) AS n,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM expansion_candidate
WHERE computed_at >= NOW() - INTERVAL '24 hours'
GROUP BY 1
ORDER BY n DESC;

\echo '=== 3. Telemetry presence breakdown ==='
SELECT
  CASE
    WHEN (score_breakdown_json->'market_viability_flag' ? 'rent_per_capita_demote') = false
      THEN 'absent (cohort skipped)'
    WHEN (score_breakdown_json->'market_viability_flag'->>'rent_per_capita_sar') IS NULL
      THEN 'null (missing inputs)'
    ELSE 'present (evaluated)'
  END AS telemetry_state,
  COUNT(*) AS n
FROM expansion_candidate
WHERE computed_at >= NOW() - INTERVAL '24 hours'
GROUP BY 1
ORDER BY n DESC;

\echo '=== 4. Single-demote contract check (CRITICAL — must return 0 rows) ==='
SELECT
  (score_breakdown_json->'market_viability_flag'->>'rent_per_capita_demote')::bool AS rpc,
  (score_breakdown_json->'market_viability_flag'->>'population_demote')::bool      AS pop,
  (score_breakdown_json->'market_viability_flag'->>'rent_demote')::bool            AS rent,
  (score_breakdown_json->'market_viability_flag'->>'economics_demote')::bool       AS econ,
  (score_breakdown_json->'market_viability_flag'->>'demand_demote')::bool          AS demand,
  (score_breakdown_json->'market_viability_flag'->>'radiance_growth_demote')::bool AS radiance,
  COUNT(*) AS n
FROM expansion_candidate
WHERE computed_at >= NOW() - INTERVAL '24 hours'
  AND (score_breakdown_json->'market_viability_flag'->>'rent_per_capita_demote')::bool = true
  AND (
       (score_breakdown_json->'market_viability_flag'->>'population_demote')::bool      = true
    OR (score_breakdown_json->'market_viability_flag'->>'rent_demote')::bool            = true
    OR (score_breakdown_json->'market_viability_flag'->>'economics_demote')::bool       = true
    OR (score_breakdown_json->'market_viability_flag'->>'demand_demote')::bool          = true
    OR (score_breakdown_json->'market_viability_flag'->>'radiance_growth_demote')::bool = true
  )
GROUP BY 1,2,3,4,5,6;

\echo '=== 5. Percentile/demote consistency (must return 0 rows) ==='
SELECT
  id,
  (score_breakdown_json->'market_viability_flag'->>'rent_per_capita_sar')::float  AS rpc_sar,
  (score_breakdown_json->'market_viability_flag'->>'rent_per_capita_pct')::float  AS rpc_pct,
  (score_breakdown_json->'market_viability_flag'->>'rent_per_capita_demote')::bool AS demoted
FROM expansion_candidate
WHERE computed_at >= NOW() - INTERVAL '24 hours'
  AND (score_breakdown_json->'market_viability_flag'->>'rent_per_capita_demote')::bool = true
  AND (score_breakdown_json->'market_viability_flag'->>'rent_per_capita_pct')::float < 0.75
LIMIT 50;

\echo '=== 6. Worst rpc candidates and their demote status ==='
SELECT
  id,
  feature_snapshot_json->>'district_display'                         AS district,
  (feature_snapshot_json->>'population_reach')::int                  AS pop_reach,
  (feature_snapshot_json->>'estimated_annual_rent_sar')::float       AS annual_rent_sar,
  ROUND((score_breakdown_json->'market_viability_flag'->>'rent_per_capita_sar')::numeric, 2) AS rpc_sar,
  ROUND((score_breakdown_json->'market_viability_flag'->>'rent_per_capita_pct')::numeric, 4) AS rpc_pct,
  (score_breakdown_json->'market_viability_flag'->>'rent_per_capita_demote')::bool AS rpc_demote,
  (score_breakdown_json->'market_viability_flag'->>'population_demote')::bool      AS pop_demote,
  (score_breakdown_json->'market_viability_flag'->>'rent_demote')::bool            AS rent_demote
FROM expansion_candidate
WHERE computed_at >= NOW() - INTERVAL '24 hours'
  AND (score_breakdown_json->'market_viability_flag'->>'rent_per_capita_sar') IS NOT NULL
ORDER BY (score_breakdown_json->'market_viability_flag'->>'rent_per_capita_sar')::float DESC
LIMIT 30;

\echo '=== 7. Cohort-skip rate (per-search leg_enabled flag) ==='
SELECT
  CASE
    WHEN notes->'viability'->'demote_legs'->'leg_enabled'->>'rent_per_capita' = 'true' THEN 'enabled'
    WHEN notes->'viability'->'demote_legs'->'leg_enabled'->>'rent_per_capita' = 'false' THEN 'disabled (cohort < 10)'
    ELSE 'absent (older search)'
  END AS leg_state,
  COUNT(*) AS n_searches
FROM expansion_search
WHERE created_at >= NOW() - INTERVAL '7 days'
GROUP BY 1
ORDER BY n_searches DESC;

\echo '=== 8. Per-search dropped_rent_per_capita meta vs per-row demote counts (sample) ==='
SELECT
  es.id AS search_id,
  es.created_at,
  (es.notes->'viability'->'demote_legs'->'drops'->>'dropped_rent_per_capita')::int AS meta_dropped,
  COUNT(*) FILTER (
    WHERE (ec.score_breakdown_json->'market_viability_flag'->>'rent_per_capita_demote')::bool = true
  ) AS row_demoted_count,
  COUNT(*) AS row_total
FROM expansion_search es
LEFT JOIN expansion_candidate ec ON ec.search_id = es.id
WHERE es.created_at >= NOW() - INTERVAL '24 hours'
  AND es.notes->'viability'->'demote_legs'->'drops' ? 'dropped_rent_per_capita'
GROUP BY es.id, es.created_at, es.notes
ORDER BY es.created_at DESC
LIMIT 20;
