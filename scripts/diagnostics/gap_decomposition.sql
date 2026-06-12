\pset footer off
--
-- gap_decomposition.sql  (Probe I — weight-stack investigation)
--
-- Question: the v2+archetypes BASE score sits at mean per-search Spearman
-- ≈ −0.50 vs realized_demand_30d (18 archetype-era searches). Probe H
-- showed neutralizing brand_fit alone buys only +0.037. How does the
-- remaining base-vs-rd gap apportion across ALL weighted components, and
-- how much of it is fixable defect vs deliberate strategy vs a density
-- confound in rd itself?
--
-- Conventions (carried over from the brand_fit counterfactual, Probe H):
--   * Archetype-era detection: score_breakdown_json ? 'brand_archetype'
--     (the key is written only when EXPANSION_ARCHETYPE_PROFILES is on
--     under the v2 stack — see _score_breakdown).
--   * Per-row persisted weights: score_breakdown_json->'weights' and raw
--     inputs score_breakdown_json->'inputs' are read per candidate, so
--     per-archetype weight profiles are honored without any hardcoding.
--   * rd gate: feature_snapshot_json realized_demand_30d present AND
--     realized_demand_branches >= 3 (the snapshot writer's own gate).
--   * Search gate: >= 8 gated candidates per search.
--   * Deterministic ranks: every rank() tie-breaks parcel_id ASC, on the
--     rd side too. Spearman = Pearson corr over those ranks.
--   * NEUTRAL counterfactual: a component is neutralized by pinning its
--     raw input at the constant 60 (Probe H's NEUTRAL), i.e.
--       base' = base − w_c·(raw_c − 60)/100.
--
-- Base score is bonus_detail.base_deterministic (the _score_breakdown
-- output before bonus deltas), falling back to the breakdown/Numeric
-- final_score for any row that predates bonus_detail.
--
-- CAVEATS (read before interpreting):
--   * demand_potential and delivery_demand both ingest realized/delivery
--     legs, so their LOO deltas are partly mechanical (removing them
--     removes rd's own echo), not evidence of defect.
--   * realized_demand_30d is rating velocity of existing same-category
--     branches in the catchment — an AREA outcome proxy that is
--     structurally a dense-urban signal. Section D tests exactly that.
--   * delta > 0 in section B means the component drags base away from rd;
--     it apportions the gap, it does not by itself say "remove it" —
--     see the reading-guide note for the strategy split.
--

-- ════════════════════════════════════════════════════════════════════
\echo ''
\echo '=== A. Cohort sanity: archetype-era searches passing the rd gate ==='
-- Expect ~18 searches. If this drifts, the LOO numbers are not
-- comparable with the Probe G/H runs.
WITH gated AS (
  SELECT
    c.search_id,
    c.parcel_id
  FROM expansion_candidate c
  WHERE c.score_breakdown_json ? 'brand_archetype'
    AND c.feature_snapshot_json ? 'realized_demand_30d'
    AND (c.feature_snapshot_json->>'realized_demand_30d') IS NOT NULL
    AND COALESCE((c.feature_snapshot_json->>'realized_demand_branches')::numeric, 0) >= 3
    AND c.score_breakdown_json ? 'weights'
    AND c.score_breakdown_json ? 'inputs'
)
SELECT
  COUNT(DISTINCT search_id) FILTER (WHERE n >= 8) AS n_searches_gated,
  COUNT(DISTINCT search_id)                       AS n_searches_any_rd,
  SUM(n) FILTER (WHERE n >= 8)                    AS n_candidates_gated
FROM (
  SELECT search_id, COUNT(*) AS n FROM gated GROUP BY search_id
) t;

-- ════════════════════════════════════════════════════════════════════
\echo ''
\echo '=== B. Leave-one-out: neutralize each component at 60, Spearman vs rd ==='
-- One row per weighted component. delta = mean_sp_neutralized −
-- mean_sp_shipped; delta > 0 means the component pulls base AWAY from rd
-- (neutralizing it closes that much of the gap, in Spearman points).
WITH base AS (
  SELECT
    c.search_id,
    c.parcel_id,
    (c.feature_snapshot_json->>'realized_demand_30d')::numeric AS rd,
    COALESCE(
      (c.score_breakdown_json->'bonus_detail'->>'base_deterministic')::numeric,
      (c.score_breakdown_json->>'final_score')::numeric,
      c.final_score::numeric
    ) AS base_score,
    c.score_breakdown_json->'weights' AS weights,
    c.score_breakdown_json->'inputs'  AS inputs
  FROM expansion_candidate c
  WHERE c.score_breakdown_json ? 'brand_archetype'
    AND c.feature_snapshot_json ? 'realized_demand_30d'
    AND (c.feature_snapshot_json->>'realized_demand_30d') IS NOT NULL
    AND COALESCE((c.feature_snapshot_json->>'realized_demand_branches')::numeric, 0) >= 3
    AND c.score_breakdown_json ? 'weights'
    AND c.score_breakdown_json ? 'inputs'
),
gated AS (
  SELECT b.*
  FROM base b
  JOIN (
    SELECT search_id FROM base GROUP BY search_id HAVING COUNT(*) >= 8
  ) ok USING (search_id)
),
loo AS (
  -- One row per (candidate, component): base with that component pinned
  -- at the NEUTRAL constant 60. Components are exploded from the row's
  -- own persisted weights, so archetype profiles are honored per row.
  SELECT
    g.search_id,
    g.parcel_id,
    g.rd,
    g.base_score,
    kv.key AS component,
    (kv.value)::numeric AS weight_pct,
    g.base_score
      - (kv.value)::numeric * ((g.inputs->>kv.key)::numeric - 60.0) / 100.0
      AS base_neutralized
  FROM gated g
  CROSS JOIN LATERAL jsonb_each_text(g.weights) AS kv(key, value)
  WHERE g.inputs ? kv.key
),
ranked AS (
  SELECT
    search_id,
    component,
    weight_pct,
    rank() OVER (PARTITION BY search_id, component
                 ORDER BY base_score, parcel_id ASC)       AS r_ship,
    rank() OVER (PARTITION BY search_id, component
                 ORDER BY base_neutralized, parcel_id ASC) AS r_neut,
    rank() OVER (PARTITION BY search_id, component
                 ORDER BY rd, parcel_id ASC)               AS r_rd
  FROM loo
),
per_search AS (
  SELECT
    search_id,
    component,
    AVG(weight_pct)                       AS weight_pct,
    corr(r_ship::float8, r_rd::float8)    AS sp_ship,
    corr(r_neut::float8, r_rd::float8)    AS sp_neut
  FROM ranked
  GROUP BY search_id, component
  HAVING COUNT(*) >= 8
)
SELECT
  component,
  COUNT(*)                                            AS n_searches,
  ROUND(AVG(weight_pct)::numeric, 2)                  AS mean_weight_pct,
  ROUND(AVG(sp_ship)::numeric, 3)                     AS mean_sp_shipped,
  ROUND(AVG(sp_neut)::numeric, 3)                     AS mean_sp_neutralized,
  ROUND((AVG(sp_neut) - AVG(sp_ship))::numeric, 3)    AS delta
FROM per_search
GROUP BY component
ORDER BY (AVG(sp_neut) - AVG(sp_ship)) DESC, component;

-- ════════════════════════════════════════════════════════════════════
\echo ''
\echo '=== C. Cumulative greedy: neutralize top-k draggers (k = 0..4) ==='
-- k = 0 is the shipped base (the −0.50 anchor). k = 1..4 neutralizes the
-- k components with the largest LOO delta from section B, cumulatively.
-- A fast climb that flattens early means the gap concentrates in one or
-- two components; a slow steady climb means it is spread thin.
WITH base AS (
  SELECT
    c.search_id,
    c.parcel_id,
    (c.feature_snapshot_json->>'realized_demand_30d')::numeric AS rd,
    COALESCE(
      (c.score_breakdown_json->'bonus_detail'->>'base_deterministic')::numeric,
      (c.score_breakdown_json->>'final_score')::numeric,
      c.final_score::numeric
    ) AS base_score,
    c.score_breakdown_json->'weights' AS weights,
    c.score_breakdown_json->'inputs'  AS inputs
  FROM expansion_candidate c
  WHERE c.score_breakdown_json ? 'brand_archetype'
    AND c.feature_snapshot_json ? 'realized_demand_30d'
    AND (c.feature_snapshot_json->>'realized_demand_30d') IS NOT NULL
    AND COALESCE((c.feature_snapshot_json->>'realized_demand_branches')::numeric, 0) >= 3
    AND c.score_breakdown_json ? 'weights'
    AND c.score_breakdown_json ? 'inputs'
),
gated AS (
  SELECT b.*
  FROM base b
  JOIN (
    SELECT search_id FROM base GROUP BY search_id HAVING COUNT(*) >= 8
  ) ok USING (search_id)
),
-- Recompute the section-B LOO delta to rank the draggers (each section
-- of this probe is self-contained, matching the Probe A/B file style).
loo AS (
  SELECT
    g.search_id,
    g.parcel_id,
    g.rd,
    g.base_score,
    kv.key AS component,
    g.base_score
      - (kv.value)::numeric * ((g.inputs->>kv.key)::numeric - 60.0) / 100.0
      AS base_neutralized
  FROM gated g
  CROSS JOIN LATERAL jsonb_each_text(g.weights) AS kv(key, value)
  WHERE g.inputs ? kv.key
),
loo_ranked AS (
  SELECT
    search_id,
    component,
    rank() OVER (PARTITION BY search_id, component
                 ORDER BY base_score, parcel_id ASC)       AS r_ship,
    rank() OVER (PARTITION BY search_id, component
                 ORDER BY base_neutralized, parcel_id ASC) AS r_neut,
    rank() OVER (PARTITION BY search_id, component
                 ORDER BY rd, parcel_id ASC)               AS r_rd
  FROM loo
),
loo_per_search AS (
  SELECT
    search_id,
    component,
    corr(r_ship::float8, r_rd::float8) AS sp_ship,
    corr(r_neut::float8, r_rd::float8) AS sp_neut
  FROM loo_ranked
  GROUP BY search_id, component
  HAVING COUNT(*) >= 8
),
drag_rank AS (
  SELECT
    component,
    AVG(sp_neut) - AVG(sp_ship)                                       AS delta,
    row_number() OVER (ORDER BY AVG(sp_neut) - AVG(sp_ship) DESC,
                                component)                            AS rk
  FROM loo_per_search
  GROUP BY component
),
-- base_k: base with the top-k draggers pinned at 60, k = 0..4.
cum AS (
  SELECT
    ks.k,
    g.search_id,
    g.parcel_id,
    g.rd,
    g.base_score
      - COALESCE((
          SELECT SUM(
            (g.weights->>dr.component)::numeric
            * ((g.inputs->>dr.component)::numeric - 60.0) / 100.0
          )
          FROM drag_rank dr
          WHERE dr.rk <= ks.k
            AND g.weights ? dr.component
            AND g.inputs  ? dr.component
        ), 0) AS base_k
  FROM gated g
  CROSS JOIN generate_series(0, 4) AS ks(k)
),
cum_ranked AS (
  SELECT
    k,
    search_id,
    rank() OVER (PARTITION BY k, search_id
                 ORDER BY base_k, parcel_id ASC) AS r_k,
    rank() OVER (PARTITION BY k, search_id
                 ORDER BY rd, parcel_id ASC)     AS r_rd
  FROM cum
),
cum_per_search AS (
  SELECT k, search_id, corr(r_k::float8, r_rd::float8) AS sp_k
  FROM cum_ranked
  GROUP BY k, search_id
  HAVING COUNT(*) >= 8
)
SELECT
  p.k,
  COALESCE((
    SELECT string_agg(dr.component, ' + ' ORDER BY dr.rk)
    FROM drag_rank dr WHERE dr.rk <= p.k
  ), '(none — shipped base)')                 AS components_neutralized,
  COUNT(*)                                    AS n_searches,
  ROUND(AVG(p.sp_k)::numeric, 3)              AS mean_sp_vs_rd,
  ROUND((AVG(p.sp_k) - first_value_k0.sp0)::numeric, 3) AS gain_vs_shipped
FROM cum_per_search p
CROSS JOIN (
  SELECT AVG(sp_k) AS sp0 FROM cum_per_search WHERE k = 0
) AS first_value_k0
GROUP BY p.k, first_value_k0.sp0
ORDER BY p.k;

-- ════════════════════════════════════════════════════════════════════
\echo ''
\echo '=== D. Confound panel: rd vs raw context signals (NOT score components) ==='
-- Per-search Spearman of realized_demand_30d against persisted raw
-- context values that are NOT themselves weighted components. If rd
-- correlates with raw urban-density context (parking supply, plot size,
-- street width, delivery-provider density, district listing activity)
-- about as strongly as with the misaligned components, then rd is
-- structurally a dense-urban signal and the section-B deltas are a
-- density confound, not component defects.
--
-- Persistence check against the live schema (all verified):
--   parking_score            expansion_candidate.parking_score   (20260313_exp_adv_v6_features)
--   area_m2                  expansion_candidate.area_m2         (20260310_exp_adv_v0)
--   street_width_m           expansion_candidate.unit_street_width_m (20260330_exp_adv_commercial_units;
--                            NULL for parcel-source candidates — coverage_pct shows how thin it is)
--   provider_density_score   expansion_candidate.provider_density_score (20260311_exp_adv_brand_v4)
--   district_momentum raw    feature_snapshot_json->'district_momentum'->>'activity_30d'
--                            (raw 30-day district activity count; deliberately NOT
--                            momentum_score, which is the component's own input)
-- No other candidates were dropped — everything requested is persisted.
WITH base AS (
  SELECT
    c.search_id,
    c.parcel_id,
    (c.feature_snapshot_json->>'realized_demand_30d')::numeric AS rd,
    c.parking_score::numeric                                   AS parking_score,
    c.area_m2::numeric                                         AS area_m2,
    c.unit_street_width_m::numeric                             AS street_width_m,
    c.provider_density_score::numeric                          AS provider_density_score,
    (c.feature_snapshot_json->'district_momentum'->>'activity_30d')::numeric
                                                               AS district_momentum_activity_30d
  FROM expansion_candidate c
  WHERE c.score_breakdown_json ? 'brand_archetype'
    AND c.feature_snapshot_json ? 'realized_demand_30d'
    AND (c.feature_snapshot_json->>'realized_demand_30d') IS NOT NULL
    AND COALESCE((c.feature_snapshot_json->>'realized_demand_branches')::numeric, 0) >= 3
    AND c.score_breakdown_json ? 'weights'
    AND c.score_breakdown_json ? 'inputs'
),
gated AS (
  SELECT b.*
  FROM base b
  JOIN (
    SELECT search_id FROM base GROUP BY search_id HAVING COUNT(*) >= 8
  ) ok USING (search_id)
),
totals AS (
  SELECT COUNT(*) AS n_rows FROM gated
),
long AS (
  SELECT g.search_id, g.parcel_id, g.rd, s.signal, s.val
  FROM gated g
  CROSS JOIN LATERAL (VALUES
    ('parking_score',                   g.parking_score),
    ('area_m2',                         g.area_m2),
    ('street_width_m',                  g.street_width_m),
    ('provider_density_score',          g.provider_density_score),
    ('district_momentum_activity_30d',  g.district_momentum_activity_30d)
  ) AS s(signal, val)
  WHERE s.val IS NOT NULL
),
ranked AS (
  SELECT
    search_id,
    signal,
    rank() OVER (PARTITION BY search_id, signal
                 ORDER BY val, parcel_id ASC) AS r_val,
    rank() OVER (PARTITION BY search_id, signal
                 ORDER BY rd, parcel_id ASC)  AS r_rd
  FROM long
),
per_search AS (
  SELECT
    search_id,
    signal,
    corr(r_val::float8, r_rd::float8) AS spearman,
    COUNT(*)                          AS n
  FROM ranked
  GROUP BY search_id, signal
  HAVING COUNT(*) >= 8
),
-- Anchor on the full signal list so a thin signal (street_width_m is NULL
-- for parcel-source candidates) still reports its coverage instead of
-- silently vanishing when no search clears the per-search n >= 8 gate.
signals AS (
  SELECT unnest(ARRAY[
    'parking_score',
    'area_m2',
    'street_width_m',
    'provider_density_score',
    'district_momentum_activity_30d'
  ]) AS signal
)
SELECT
  s.signal,
  COUNT(p.search_id)                                AS n_searches,
  ROUND(AVG(p.spearman)::numeric, 3)                AS mean_sp_vs_rd,
  ROUND(MIN(p.spearman)::numeric, 3)                AS min_sp,
  ROUND(MAX(p.spearman)::numeric, 3)                AS max_sp,
  ROUND(AVG(p.n)::numeric, 1)                       AS mean_n_candidates,
  ROUND(100.0 * (SELECT COUNT(*) FROM long l WHERE l.signal = s.signal)
        / NULLIF((SELECT n_rows FROM totals), 0), 1) AS coverage_pct
FROM signals s
LEFT JOIN per_search p ON p.signal = s.signal
GROUP BY s.signal
ORDER BY AVG(p.spearman) DESC NULLS LAST;
