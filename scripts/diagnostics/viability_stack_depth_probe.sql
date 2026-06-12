-- ============================================================================
-- viability_stack_depth_probe.sql  (Finding 4 — scoring/ranking audit 2026-06)
--
-- _apply_market_viability_pass attaches viability_delta = -10 * len(reasons)
-- (expansion_advisor.py L6274 — stacking), while its docstring (L5717) says
-- "single demote, never compounded". Before deciding which semantics to keep
-- (product question for Ahmed/Faisal), measure how often legs actually
-- co-fire in production: a -10 vs -60 swing is the difference between a
-- nudge and a hard exclusion.
--
-- Persisted location: score_breakdown_json -> 'bonus_detail' ->
-- 'viability_legs_fired' (list) and 'viability_delta' (float), written by
-- _apply_score_deltas_and_sort (L5636-5647).
--
-- Run read-only against the production replica (Ahmed: via Codespace psql).
-- ============================================================================

-- Distribution of stack depth (number of legs co-firing) over 30 days.
WITH cands AS (
    SELECT
        c.id,
        COALESCE(
            jsonb_array_length(c.score_breakdown_json -> 'bonus_detail' -> 'viability_legs_fired'),
            0
        ) AS legs_fired,
        (c.score_breakdown_json -> 'bonus_detail' ->> 'viability_delta')::float AS viability_delta
    FROM expansion_candidate c
    JOIN expansion_search s ON s.id = c.search_id
    WHERE s.created_at >= now() - interval '30 days'
      AND c.score_breakdown_json ? 'bonus_detail'
)
SELECT
    legs_fired,
    COUNT(*) AS candidates,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_all,
    MIN(viability_delta) AS min_delta,
    MAX(viability_delta) AS max_delta
FROM cands
GROUP BY legs_fired
ORDER BY legs_fired;

-- Headline co-fire rates: how often do >=2 and >=3 legs stack in practice?
WITH cands AS (
    SELECT
        COALESCE(
            jsonb_array_length(c.score_breakdown_json -> 'bonus_detail' -> 'viability_legs_fired'),
            0
        ) AS legs_fired
    FROM expansion_candidate c
    JOIN expansion_search s ON s.id = c.search_id
    WHERE s.created_at >= now() - interval '30 days'
      AND c.score_breakdown_json ? 'bonus_detail'
)
SELECT
    COUNT(*) AS candidates_total,
    COUNT(*) FILTER (WHERE legs_fired >= 1) AS fired_ge_1,
    COUNT(*) FILTER (WHERE legs_fired >= 2) AS fired_ge_2,
    COUNT(*) FILTER (WHERE legs_fired >= 3) AS fired_ge_3,
    ROUND(100.0 * COUNT(*) FILTER (WHERE legs_fired >= 2) / NULLIF(COUNT(*), 0), 2) AS pct_ge_2,
    ROUND(100.0 * COUNT(*) FILTER (WHERE legs_fired >= 3) / NULLIF(COUNT(*), 0), 2) AS pct_ge_3
FROM cands;

-- Which leg combinations co-fire (top 20)? Legs are written in stable order
-- (rpc, population, rent, economics, demand, radiance_growth — L6256-6271)
-- so the array text is a stable combination key.
SELECT
    c.score_breakdown_json -> 'bonus_detail' ->> 'viability_legs_fired' AS leg_combination,
    COUNT(*) AS candidates
FROM expansion_candidate c
JOIN expansion_search s ON s.id = c.search_id
WHERE s.created_at >= now() - interval '30 days'
  AND jsonb_array_length(c.score_breakdown_json -> 'bonus_detail' -> 'viability_legs_fired') >= 1
GROUP BY 1
ORDER BY candidates DESC
LIMIT 20;
