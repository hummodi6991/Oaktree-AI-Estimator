\pset footer off
--
-- archetype_backtest.sql  (Probe F — brand-brief redesign investigation)
--
-- Pure-SQL re-weighting backtest: for the v2 validation searches (the 10
-- most recent searches with >= 8 candidates scored under weight stack v2),
-- recompute each candidate's deterministic base score under each proposed
-- archetype weight profile (archetype weights x the persisted
-- score_breakdown_json->'inputs') and compare the resulting ordering to the
-- shipped ranking. No app changes; goal is to verify archetypes produce
-- visibly different, sane orderings before anything is built.
--
-- v2-row detection: score_breakdown_json->'inputs' ? 'district_momentum'
-- (only the v2 stack writes that input key — _score_breakdown:3665-3668).
--
-- Shipped-ranking proxy: ROW_NUMBER over (rank_position NULLS LAST,
-- final_score DESC). rank_position is the true shipped order (incl. LLM
-- rerank); final_score is the fallback for rows persisted without it.
--
-- Bonus deltas (value-band / viability / freshness) are held constant:
--   new_final = new_base + (final_score - bonus_detail.base_deterministic)
-- so the backtest isolates the WEIGHT change. Rows without bonus_detail
-- use new_base directly.
--
-- The 'balanced' archetype is the v2 stack itself and acts as the harness
-- control: its top-5 overlap should be 5/5 and max displacement ~0
-- (modulo 2-dp rounding of persisted inputs). If the control is off, the
-- harness — not the archetypes — is broken.

\echo ''
\echo '=== F.0 Archetype weight profiles (must each sum to 100) ==='
WITH archetype_weights(archetype, component, w) AS (
  VALUES
  -- balanced = shipped v2 stack (control)
  ('balanced', 'occupancy_economics',    20.0),
  ('balanced', 'demand_potential',       18.0),
  ('balanced', 'competition_whitespace', 12.0),
  ('balanced', 'access_visibility',      11.0),
  ('balanced', 'listing_quality',         9.0),
  ('balanced', 'brand_fit',               8.0),
  ('balanced', 'district_momentum',       7.0),
  ('balanced', 'delivery_demand',         6.0),
  ('balanced', 'landlord_signal',         5.0),
  ('balanced', 'chain_strength',          4.0),
  -- delivery_led_qsr: delivery economics dominate; the storefront is a
  -- production node, not a destination.
  ('delivery_led_qsr', 'occupancy_economics',    20.0),
  ('delivery_led_qsr', 'demand_potential',       18.0),
  ('delivery_led_qsr', 'competition_whitespace', 13.0),
  ('delivery_led_qsr', 'access_visibility',       6.0),
  ('delivery_led_qsr', 'listing_quality',         8.0),
  ('delivery_led_qsr', 'brand_fit',               6.0),
  ('delivery_led_qsr', 'district_momentum',       7.0),
  ('delivery_led_qsr', 'delivery_demand',        13.0),
  ('delivery_led_qsr', 'landlord_signal',         5.0),
  ('delivery_led_qsr', 'chain_strength',          4.0),
  -- flagship_dine_in: street presence is the purchase; delivery marginal.
  ('flagship_dine_in', 'occupancy_economics',    19.0),
  ('flagship_dine_in', 'demand_potential',       19.0),
  ('flagship_dine_in', 'competition_whitespace', 11.0),
  ('flagship_dine_in', 'access_visibility',      17.0),
  ('flagship_dine_in', 'listing_quality',         8.0),
  ('flagship_dine_in', 'brand_fit',               8.0),
  ('flagship_dine_in', 'district_momentum',       6.0),
  ('flagship_dine_in', 'delivery_demand',         2.0),
  ('flagship_dine_in', 'landlord_signal',         5.0),
  ('flagship_dine_in', 'chain_strength',          5.0),
  -- neighborhood_cafe: local demand + district trajectory + rent
  -- discipline; chain adjacency and delivery de-emphasized.
  ('neighborhood_cafe', 'occupancy_economics',    22.0),
  ('neighborhood_cafe', 'demand_potential',       21.0),
  ('neighborhood_cafe', 'competition_whitespace', 10.0),
  ('neighborhood_cafe', 'access_visibility',       9.0),
  ('neighborhood_cafe', 'listing_quality',        10.0),
  ('neighborhood_cafe', 'brand_fit',               7.0),
  ('neighborhood_cafe', 'district_momentum',      11.0),
  ('neighborhood_cafe', 'delivery_demand',         4.0),
  ('neighborhood_cafe', 'landlord_signal',         4.0),
  ('neighborhood_cafe', 'chain_strength',          2.0)
)
SELECT archetype, SUM(w) AS weight_sum, COUNT(*) AS n_components
FROM archetype_weights
GROUP BY archetype
ORDER BY archetype;

\echo ''
\echo '=== F.1 Per-search rank movement vs shipped ranking ==='
WITH archetype_weights(archetype, component, w) AS (
  VALUES
  ('balanced', 'occupancy_economics',    20.0),
  ('balanced', 'demand_potential',       18.0),
  ('balanced', 'competition_whitespace', 12.0),
  ('balanced', 'access_visibility',      11.0),
  ('balanced', 'listing_quality',         9.0),
  ('balanced', 'brand_fit',               8.0),
  ('balanced', 'district_momentum',       7.0),
  ('balanced', 'delivery_demand',         6.0),
  ('balanced', 'landlord_signal',         5.0),
  ('balanced', 'chain_strength',          4.0),
  ('delivery_led_qsr', 'occupancy_economics',    20.0),
  ('delivery_led_qsr', 'demand_potential',       18.0),
  ('delivery_led_qsr', 'competition_whitespace', 13.0),
  ('delivery_led_qsr', 'access_visibility',       6.0),
  ('delivery_led_qsr', 'listing_quality',         8.0),
  ('delivery_led_qsr', 'brand_fit',               6.0),
  ('delivery_led_qsr', 'district_momentum',       7.0),
  ('delivery_led_qsr', 'delivery_demand',        13.0),
  ('delivery_led_qsr', 'landlord_signal',         5.0),
  ('delivery_led_qsr', 'chain_strength',          4.0),
  ('flagship_dine_in', 'occupancy_economics',    19.0),
  ('flagship_dine_in', 'demand_potential',       19.0),
  ('flagship_dine_in', 'competition_whitespace', 11.0),
  ('flagship_dine_in', 'access_visibility',      17.0),
  ('flagship_dine_in', 'listing_quality',         8.0),
  ('flagship_dine_in', 'brand_fit',               8.0),
  ('flagship_dine_in', 'district_momentum',       6.0),
  ('flagship_dine_in', 'delivery_demand',         2.0),
  ('flagship_dine_in', 'landlord_signal',         5.0),
  ('flagship_dine_in', 'chain_strength',          5.0),
  ('neighborhood_cafe', 'occupancy_economics',    22.0),
  ('neighborhood_cafe', 'demand_potential',       21.0),
  ('neighborhood_cafe', 'competition_whitespace', 10.0),
  ('neighborhood_cafe', 'access_visibility',       9.0),
  ('neighborhood_cafe', 'listing_quality',        10.0),
  ('neighborhood_cafe', 'brand_fit',               7.0),
  ('neighborhood_cafe', 'district_momentum',      11.0),
  ('neighborhood_cafe', 'delivery_demand',         4.0),
  ('neighborhood_cafe', 'landlord_signal',         4.0),
  ('neighborhood_cafe', 'chain_strength',          2.0)
),
recent_v2_searches AS (
  SELECT s.id, s.created_at, s.service_model
  FROM expansion_search s
  WHERE (
    SELECT COUNT(*)
    FROM expansion_candidate c
    WHERE c.search_id = s.id
      AND c.score_breakdown_json->'inputs' ? 'district_momentum'
  ) >= 8
  ORDER BY s.created_at DESC
  LIMIT 10
),
cand AS (
  SELECT
    c.search_id,
    c.id,
    rs.service_model,
    c.final_score::numeric AS final_score,
    (c.score_breakdown_json->'bonus_detail'->>'base_deterministic')::numeric
      AS base_det,
    c.score_breakdown_json->'inputs' AS inputs,
    ROW_NUMBER() OVER (
      PARTITION BY c.search_id
      ORDER BY c.rank_position ASC NULLS LAST, c.final_score DESC, c.id
    ) AS shipped_rank
  FROM expansion_candidate c
  JOIN recent_v2_searches rs ON rs.id = c.search_id
  WHERE c.score_breakdown_json->'inputs' ? 'district_momentum'
),
recomputed AS (
  SELECT
    cand.search_id,
    cand.service_model,
    cand.id,
    cand.shipped_rank,
    aw.archetype,
    SUM(COALESCE((cand.inputs->>aw.component)::numeric, 0.0) * aw.w) / 100.0
      AS new_base,
    COUNT(*) FILTER (WHERE NOT cand.inputs ? aw.component)
      AS missing_inputs
  FROM cand
  CROSS JOIN LATERAL (
    SELECT archetype, component, w FROM archetype_weights
  ) aw
  GROUP BY cand.search_id, cand.service_model, cand.id,
           cand.shipped_rank, aw.archetype
),
reranked AS (
  SELECT
    r.*,
    -- hold sort-time bonus deltas constant
    r.new_base + COALESCE(c2.final_score - r2.base_det, 0.0) AS new_final,
    ROW_NUMBER() OVER (
      PARTITION BY r.search_id, r.archetype
      ORDER BY r.new_base + COALESCE(c2.final_score - r2.base_det, 0.0) DESC,
               r.id
    ) AS new_rank
  FROM recomputed r
  JOIN cand r2 ON r2.id = r.id AND r2.search_id = r.search_id
  JOIN expansion_candidate c2 ON c2.id = r.id
),
per_search AS (
  SELECT
    search_id,
    service_model,
    archetype,
    COUNT(*) AS n_candidates,
    SUM(missing_inputs) AS missing_input_cells,
    -- top-5 overlap: shipped top5 ids found in archetype top5
    COUNT(*) FILTER (WHERE shipped_rank <= 5 AND new_rank <= 5) AS top5_overlap,
    BOOL_OR(shipped_rank = 1 AND new_rank <> 1)                 AS rank1_changed,
    MAX(ABS(new_rank - shipped_rank)) FILTER (WHERE shipped_rank <= 10)
      AS max_displacement_top10,
    ROUND(CORR(new_rank, shipped_rank)::numeric, 3)             AS rank_corr
  FROM reranked
  GROUP BY search_id, service_model, archetype
)
SELECT * FROM per_search
ORDER BY search_id, archetype;

\echo ''
\echo '=== F.2 Summary across the validation searches, per archetype ==='
\echo '    (balanced row is the harness control: expect overlap ~5, disp ~0)'
WITH archetype_weights(archetype, component, w) AS (
  VALUES
  ('balanced', 'occupancy_economics',    20.0),
  ('balanced', 'demand_potential',       18.0),
  ('balanced', 'competition_whitespace', 12.0),
  ('balanced', 'access_visibility',      11.0),
  ('balanced', 'listing_quality',         9.0),
  ('balanced', 'brand_fit',               8.0),
  ('balanced', 'district_momentum',       7.0),
  ('balanced', 'delivery_demand',         6.0),
  ('balanced', 'landlord_signal',         5.0),
  ('balanced', 'chain_strength',          4.0),
  ('delivery_led_qsr', 'occupancy_economics',    20.0),
  ('delivery_led_qsr', 'demand_potential',       18.0),
  ('delivery_led_qsr', 'competition_whitespace', 13.0),
  ('delivery_led_qsr', 'access_visibility',       6.0),
  ('delivery_led_qsr', 'listing_quality',         8.0),
  ('delivery_led_qsr', 'brand_fit',               6.0),
  ('delivery_led_qsr', 'district_momentum',       7.0),
  ('delivery_led_qsr', 'delivery_demand',        13.0),
  ('delivery_led_qsr', 'landlord_signal',         5.0),
  ('delivery_led_qsr', 'chain_strength',          4.0),
  ('flagship_dine_in', 'occupancy_economics',    19.0),
  ('flagship_dine_in', 'demand_potential',       19.0),
  ('flagship_dine_in', 'competition_whitespace', 11.0),
  ('flagship_dine_in', 'access_visibility',      17.0),
  ('flagship_dine_in', 'listing_quality',         8.0),
  ('flagship_dine_in', 'brand_fit',               8.0),
  ('flagship_dine_in', 'district_momentum',       6.0),
  ('flagship_dine_in', 'delivery_demand',         2.0),
  ('flagship_dine_in', 'landlord_signal',         5.0),
  ('flagship_dine_in', 'chain_strength',          5.0),
  ('neighborhood_cafe', 'occupancy_economics',    22.0),
  ('neighborhood_cafe', 'demand_potential',       21.0),
  ('neighborhood_cafe', 'competition_whitespace', 10.0),
  ('neighborhood_cafe', 'access_visibility',       9.0),
  ('neighborhood_cafe', 'listing_quality',        10.0),
  ('neighborhood_cafe', 'brand_fit',               7.0),
  ('neighborhood_cafe', 'district_momentum',      11.0),
  ('neighborhood_cafe', 'delivery_demand',         4.0),
  ('neighborhood_cafe', 'landlord_signal',         4.0),
  ('neighborhood_cafe', 'chain_strength',          2.0)
),
recent_v2_searches AS (
  SELECT s.id, s.created_at
  FROM expansion_search s
  WHERE (
    SELECT COUNT(*)
    FROM expansion_candidate c
    WHERE c.search_id = s.id
      AND c.score_breakdown_json->'inputs' ? 'district_momentum'
  ) >= 8
  ORDER BY s.created_at DESC
  LIMIT 10
),
cand AS (
  SELECT
    c.search_id,
    c.id,
    c.final_score::numeric AS final_score,
    (c.score_breakdown_json->'bonus_detail'->>'base_deterministic')::numeric
      AS base_det,
    c.score_breakdown_json->'inputs' AS inputs,
    ROW_NUMBER() OVER (
      PARTITION BY c.search_id
      ORDER BY c.rank_position ASC NULLS LAST, c.final_score DESC, c.id
    ) AS shipped_rank
  FROM expansion_candidate c
  JOIN recent_v2_searches rs ON rs.id = c.search_id
  WHERE c.score_breakdown_json->'inputs' ? 'district_momentum'
),
recomputed AS (
  SELECT
    cand.search_id,
    cand.id,
    cand.shipped_rank,
    cand.final_score,
    cand.base_det,
    aw.archetype,
    SUM(COALESCE((cand.inputs->>aw.component)::numeric, 0.0) * aw.w) / 100.0
      AS new_base
  FROM cand
  CROSS JOIN LATERAL (
    SELECT archetype, component, w FROM archetype_weights
  ) aw
  GROUP BY cand.search_id, cand.id, cand.shipped_rank,
           cand.final_score, cand.base_det, aw.archetype
),
reranked AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      PARTITION BY r.search_id, r.archetype
      ORDER BY r.new_base + COALESCE(r.final_score - r.base_det, 0.0) DESC,
               r.id
    ) AS new_rank
  FROM recomputed r
),
per_search AS (
  SELECT
    search_id,
    archetype,
    COUNT(*) FILTER (WHERE shipped_rank <= 5 AND new_rank <= 5) AS top5_overlap,
    BOOL_OR(shipped_rank = 1 AND new_rank <> 1)                 AS rank1_changed,
    MAX(ABS(new_rank - shipped_rank)) FILTER (WHERE shipped_rank <= 10)
      AS max_displacement_top10,
    CORR(new_rank, shipped_rank)                                AS rank_corr
  FROM reranked
  GROUP BY search_id, archetype
)
SELECT
  archetype,
  COUNT(*)                                   AS n_searches,
  ROUND(AVG(top5_overlap), 2)                AS avg_top5_overlap,
  ROUND(AVG(max_displacement_top10), 1)      AS avg_max_disp_top10,
  MAX(max_displacement_top10)                AS worst_disp_top10,
  ROUND(100.0 * COUNT(*) FILTER (WHERE rank1_changed) / COUNT(*), 0)
                                             AS pct_rank1_changed,
  ROUND(AVG(rank_corr)::numeric, 3)          AS avg_rank_corr
FROM per_search
GROUP BY archetype
ORDER BY archetype;
