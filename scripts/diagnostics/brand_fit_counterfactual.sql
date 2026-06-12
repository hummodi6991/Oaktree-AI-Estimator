\pset footer off
--
-- brand_fit_counterfactual.sql  (Probe H — brand_fit de-dup investigation)
--
-- Recomputes the deterministic base score with brand_fit replaced by:
--   (i)  variant NEUTRAL:  a constant 60 (the component contributes weight
--        but zero rank information — upper bound on what ANY brand_fit-only
--        fix can achieve at unchanged weights);
--   (ii) variant GENUINE:  only the class-(a) genuine brand<->site match
--        legs from the Part-1 classification (district preference,
--        overlap/spacing fit, area+zoning fit leg, parking leg, and the
--        genuine slice of goal_component), renormalized — this previews
--        fix option A.
--
-- Pure SQL reweighting on persisted inputs:
--   base'  =  base  +  w_brand_fit * (bf_variant - bf_persisted) / 100
-- where base = score_breakdown_json.bonus_detail.base_deterministic
-- (fallback final_score for rows persisted before the delta refactor) and
-- w_brand_fit = score_breakdown_json.weights.brand_fit (per-row, so
-- archetype profiles with brand_fit 6/7/8 are honored automatically).
--
-- Reports:
--   A. Reconstruction fidelity (validity gate for the counterfactual).
--   B. Per-search Spearman vs realized_demand_30d: shipped base vs both
--      variants (rd-gated rows, >=8 per search).
--   C. Ranking churn vs shipped: top-5 overlap and rank-1 changes, over ALL
--      candidates of each search (>=5 candidates), tie-broken by parcel_id
--      ASC to mirror _apply_score_deltas_and_sort.
--
-- Verdict thresholds (agreed in the findings report):
--   * If mean Spearman(base_neutral, rd) improves on Spearman(base, rd) by
--     >= +0.10, a brand_fit fix is worth shipping (options A/B).
--   * If the improvement is < +0.05, brand_fit is not the binding
--     constraint on the base-vs-rd gap; redirect to the next suspect.
--   * GENUINE should capture most of NEUTRAL's improvement while keeping
--     mean top-5 overlap >= 4/5; large extra churn from GENUINE vs NEUTRAL
--     means the retained legs still carry market signal.
--
-- Same reconstruction chain and caveats as brand_fit_decomposition.sql
-- (Probe G) — keep the two files in sync with _brand_fit_score
-- (app/services/expansion_advisor.py:1635). Uses a session-local TEMP
-- table only; no application table is written.
--

DROP TABLE IF EXISTS _bf_cf;
CREATE TEMP TABLE _bf_cf AS
WITH cand AS (
  SELECT
    c.id                                         AS candidate_id,
    c.search_id,
    c.parcel_id,
    lower(btrim(COALESCE(s.service_model, '')))  AS service_model,
    lower(btrim(COALESCE(bp.price_tier, 'mid')))           AS price_tier,
    lower(btrim(COALESCE(bp.primary_channel, 'balanced'))) AS primary_channel,
    lower(btrim(COALESCE(bp.parking_sensitivity, 'medium')))    AS parking_sens,
    lower(btrim(COALESCE(bp.frontage_sensitivity, 'medium')))   AS frontage_sens,
    lower(btrim(COALESCE(bp.visibility_sensitivity, 'medium'))) AS visibility_sens,
    lower(btrim(COALESCE(bp.expansion_goal, 'balanced')))  AS expansion_goal,
    COALESCE(bp.cannibalization_tolerance_m::float8, 1800.0) AS tolerance_m,
    COALESCE(bp.preferred_districts_json, '[]'::jsonb)     AS preferred_districts,
    COALESCE(bp.excluded_districts_json, '[]'::jsonb)      AS excluded_districts,
    c.district,
    COALESCE(c.demand_score::float8, 0.0)        AS demand,
    COALESCE(c.fit_score::float8, 0.0)           AS fit,
    COALESCE(c.cannibalization_score::float8, 0.0)          AS cann,
    COALESCE(c.provider_density_score::float8, 0.0)         AS pd,
    COALESCE(c.provider_whitespace_score::float8, 0.0)      AS pw,
    COALESCE(c.multi_platform_presence_score::float8, 0.0)  AS mpp,
    COALESCE(c.delivery_competition_score::float8, 0.0)     AS dc,
    COALESCE(c.access_visibility_score::float8, 0.0)        AS av,
    COALESCE(c.parking_score::float8, 0.0)       AS parking,
    c.brand_fit_score::float8                    AS bf_persisted,
    (c.score_breakdown_json->'weights'->>'brand_fit')::float8 AS w_bf,
    COALESCE(
      (c.score_breakdown_json->'bonus_detail'->>'base_deterministic')::float8,
      c.final_score::float8
    )                                            AS base_score,
    (c.feature_snapshot_json->>'realized_demand_30d')::float8 AS rd,
    COALESCE((c.feature_snapshot_json->>'realized_demand_branches')::float8, 0) AS rd_branches,
    c.score_breakdown_json->>'brand_archetype'   AS sb_archetype,
    COALESCE(c.area_m2::float8, 0.0)
      / (CASE WHEN s.target_area_m2 IS NOT NULL AND s.target_area_m2 > 0
              THEN s.target_area_m2::float8 ELSE 350.0 END) AS area_ratio
  FROM expansion_candidate c
  JOIN expansion_search s        ON s.id = c.search_id
  LEFT JOIN expansion_brand_profile bp ON bp.search_id = s.id
  WHERE s.created_at >= now() - interval '30 days'
    AND c.score_breakdown_json->'weights' ? 'district_momentum'  -- v2-era only
    AND c.brand_fit_score IS NOT NULL
    AND (c.score_breakdown_json->'weights'->>'brand_fit') IS NOT NULL
),
resolved AS (
  SELECT
    cand.*,
    CASE
      WHEN sb_archetype IS NOT NULL THEN
        CASE sb_archetype
          WHEN 'street_flagship'    THEN 'flagship'
          WHEN 'neighborhood_local' THEN 'neighborhood'
          WHEN 'delivery_led'       THEN 'delivery_led'
          ELSE 'balanced'
        END
      WHEN expansion_goal IN ('flagship', 'neighborhood', 'delivery_led')
        THEN expansion_goal
      ELSE 'balanced'
    END AS goal_key,
    CASE parking_sens    WHEN 'low' THEN 0.3 WHEN 'high' THEN 1.0 ELSE 0.6 END AS pk_w,
    CASE frontage_sens   WHEN 'low' THEN 0.3 WHEN 'high' THEN 1.0 ELSE 0.6 END AS fr_w,
    CASE
      WHEN district IS NOT NULL AND EXISTS (
        SELECT 1 FROM jsonb_array_elements_text(excluded_districts) e
        WHERE lower(btrim(e)) = lower(btrim(district))
      ) THEN 20.0
      WHEN district IS NOT NULL AND EXISTS (
        SELECT 1 FROM jsonb_array_elements_text(preferred_districts) p
        WHERE lower(btrim(p)) = lower(btrim(district))
      ) THEN 88.0
      ELSE 60.0
    END AS district_comp,
    LEAST(GREATEST(
      100.0 - abs(cann - LEAST(GREATEST((2500.0 - tolerance_m) / 25.0, 0.0), 100.0)) * 0.8,
    0.0), 100.0) AS overlap_fit,
    CASE
      WHEN area_ratio BETWEEN 0.80 AND 1.20 THEN 100.0
      WHEN area_ratio BETWEEN 0.60 AND 1.50 THEN 80.0
      WHEN area_ratio BETWEEN 0.40 AND 2.00 THEN 55.0
      ELSE 30.0
    END AS flagship_area_comp
  FROM cand
),
variants AS (
  SELECT
    r.*,
    (0.10 + r.pk_w * 0.06) AS parking_coef,
    (0.12 + r.fr_w * 0.03) AS fit_coef,
    CASE WHEN r.price_tier = 'premium'
         THEN GREATEST(0.0, 65.0 - r.av) * 0.35 + GREATEST(0.0, 60.0 - r.district_comp) * 0.25
         ELSE 0.0
    END AS premium_penalty,
    -- Genuine slice of goal_component per the Part-1 classification:
    --   flagship      -> the area-format sub-leg only
    --   neighborhood  -> kept as-is (fit + spacing + parking, all class (a))
    --   balanced      -> the fit third only (drop demand + whitespace thirds)
    --   delivery_led  -> dropped entirely (pure delivery-market re-blend)
    CASE r.goal_key
      WHEN 'flagship'     THEN r.flagship_area_comp
      WHEN 'neighborhood' THEN LEAST(GREATEST(
          r.fit * 0.45 + (100.0 - abs(r.cann - 45.0)) * 0.25 + r.parking * 0.3, 0.0), 100.0)
      WHEN 'delivery_led' THEN NULL
      ELSE r.fit
    END AS goal_genuine
  FROM resolved r
),
scored AS (
  SELECT
    v.*,
    -- GENUINE variant: class-(a) legs only, coefficients renormalized.
    LEAST(GREATEST(
      (   v.district_comp * 0.18
        + v.overlap_fit   * 0.14
        + v.fit           * v.fit_coef
        + v.parking       * v.parking_coef
        + COALESCE(v.goal_genuine, 0.0) * (CASE WHEN v.goal_genuine IS NULL THEN 0.0 ELSE 0.20 END)
      ) / (0.18 + 0.14 + v.fit_coef + v.parking_coef
           + CASE WHEN v.goal_genuine IS NULL THEN 0.0 ELSE 0.20 END)
      - v.premium_penalty,
    0.0), 100.0) AS bf_genuine
  FROM variants v
)
SELECT
  s.*,
  s.base_score + s.w_bf * (60.0       - s.bf_persisted) / 100.0 AS base_neutral,
  s.base_score + s.w_bf * (s.bf_genuine - s.bf_persisted) / 100.0 AS base_genuine
FROM scored s;

\echo ''
\echo '=== A. Validity gate: candidate volume and weight sanity ==='
SELECT
  COUNT(*)                                   AS n_candidates,
  COUNT(DISTINCT search_id)                  AS n_searches,
  ROUND(AVG(w_bf)::numeric, 2)               AS mean_brand_fit_weight,
  ROUND(AVG(bf_persisted)::numeric, 2)       AS mean_bf_shipped,
  ROUND(AVG(bf_genuine)::numeric, 2)         AS mean_bf_genuine
FROM _bf_cf;

\echo ''
\echo '=== B. Spearman(base'', realized_demand_30d) per variant (rd-gated, >=8/search) ==='
WITH ranked AS (
  SELECT search_id,
         rank() OVER (PARTITION BY search_id ORDER BY base_score)   AS r_base,
         rank() OVER (PARTITION BY search_id ORDER BY base_neutral) AS r_neutral,
         rank() OVER (PARTITION BY search_id ORDER BY base_genuine) AS r_genuine,
         rank() OVER (PARTITION BY search_id ORDER BY rd)           AS r_rd
  FROM _bf_cf
  WHERE rd IS NOT NULL AND rd_branches >= 3
),
per_search AS (
  SELECT search_id,
         corr(r_base::float8,    r_rd::float8) AS sp_base,
         corr(r_neutral::float8, r_rd::float8) AS sp_neutral,
         corr(r_genuine::float8, r_rd::float8) AS sp_genuine,
         COUNT(*) AS n
  FROM ranked
  GROUP BY search_id
  HAVING COUNT(*) >= 8
)
SELECT
  COUNT(*)                                AS n_searches,
  ROUND(AVG(sp_base)::numeric, 3)         AS mean_sp_base_shipped,
  ROUND(AVG(sp_neutral)::numeric, 3)      AS mean_sp_base_neutral,
  ROUND(AVG(sp_genuine)::numeric, 3)      AS mean_sp_base_genuine,
  ROUND((AVG(sp_neutral) - AVG(sp_base))::numeric, 3) AS delta_neutral,
  ROUND((AVG(sp_genuine) - AVG(sp_base))::numeric, 3) AS delta_genuine,
  ROUND(MIN(sp_genuine)::numeric, 3)      AS min_sp_genuine,
  ROUND(MAX(sp_genuine)::numeric, 3)      AS max_sp_genuine
FROM per_search;

\echo ''
\echo '=== C. Ranking churn vs shipped base order (all candidates, >=5/search) ==='
WITH ranked AS (
  SELECT search_id, candidate_id,
         row_number() OVER (PARTITION BY search_id ORDER BY base_score   DESC, parcel_id ASC) AS rk_base,
         row_number() OVER (PARTITION BY search_id ORDER BY base_neutral DESC, parcel_id ASC) AS rk_neutral,
         row_number() OVER (PARTITION BY search_id ORDER BY base_genuine DESC, parcel_id ASC) AS rk_genuine
  FROM _bf_cf
),
eligible AS (
  SELECT search_id FROM ranked GROUP BY search_id HAVING COUNT(*) >= 5
),
per_search AS (
  SELECT r.search_id,
         SUM(((r.rk_base <= 5) AND (r.rk_neutral <= 5))::int) AS top5_overlap_neutral,
         SUM(((r.rk_base <= 5) AND (r.rk_genuine <= 5))::int) AS top5_overlap_genuine,
         MAX(CASE WHEN r.rk_base = 1 THEN (r.rk_neutral <> 1)::int END) AS rank1_changed_neutral,
         MAX(CASE WHEN r.rk_base = 1 THEN (r.rk_genuine <> 1)::int END) AS rank1_changed_genuine
  FROM ranked r
  JOIN eligible e USING (search_id)
  GROUP BY r.search_id
)
SELECT
  COUNT(*)                                          AS n_searches,
  ROUND(AVG(top5_overlap_neutral)::numeric, 2)      AS avg_top5_overlap_neutral,
  ROUND(AVG(top5_overlap_genuine)::numeric, 2)      AS avg_top5_overlap_genuine,
  SUM(rank1_changed_neutral)                        AS n_rank1_changed_neutral,
  SUM(rank1_changed_genuine)                        AS n_rank1_changed_genuine
FROM per_search;
