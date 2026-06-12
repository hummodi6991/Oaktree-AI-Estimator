\pset footer off
--
-- brand_fit_decomposition.sql  (Probe G — brand_fit de-dup investigation)
--
-- Reconstructs every leg of _brand_fit_score (app/services/expansion_advisor.py:1635)
-- from PERSISTED per-candidate columns + the search's expansion_brand_profile row,
-- then reports, for recent v2-era searches:
--   A. Reconstruction fidelity (reconstructed vs persisted brand_fit_score) —
--      read this FIRST; sections B-D are only as trustworthy as this residual.
--   B. Per-leg mean contribution (pts within brand_fit) and variance share.
--   C. Per-leg Spearman vs the deterministic base score (all candidates).
--   D. Per-leg Spearman vs realized_demand_30d (rd-gated rows only).
--
-- Persistence note: brand_fit leg values are NOT persisted anywhere
-- (score_breakdown_json.inputs carries only the top-level component raws),
-- so this probe reconstructs them from the leg INPUTS, all of which ARE
-- persisted: demand_score, fit_score, cannibalization_score,
-- provider_density_score, provider_whitespace_score,
-- multi_platform_presence_score, delivery_competition_score,
-- access_visibility_score, parking_score, area_m2, district on
-- expansion_candidate; service_model / target_area_m2 on expansion_search;
-- brand knobs on expansion_brand_profile (1:1 by search_id).
--
-- CAVEATS (read before interpreting):
--  * District-list matching uses lower(btrim(...)) equality; the service
--    uses normalize_district_key (Arabic prefix stripping etc.). Searches
--    with non-empty preferred/excluded district lists are therefore
--    approximate — section A breaks fidelity out for them separately.
--  * The persisted brand_fit_score comes from the second (shortlist) pass,
--    which uses the same access_visibility_score / parking_score that are
--    persisted as columns, so the reconstruction should match to rounding.
--  * v2-era rows are detected via weights ? 'district_momentum'. Archetype-
--    era rows additionally carry score_breakdown_json->>'brand_archetype';
--    for pre-archetype v2 rows the goal branch falls back to the legacy
--    expansion_goal knob, mirroring _brand_fit_score's flag-off path.
--  * Spearman is Pearson corr over per-search ranks (ties averaged by
--    rank()), matching the convention of contribution_vs_realized_demand.sql.
--  * The reconstruction is materialized into a session-local TEMP table
--    (_bf_recon) so sections A-D can share it; no application table is
--    written. The table vanishes when the psql session ends.
--

DROP TABLE IF EXISTS _bf_recon;
CREATE TEMP TABLE _bf_recon AS
WITH cand AS (
  SELECT
    c.id                                         AS candidate_id,
    c.search_id,
    c.parcel_id,
    lower(btrim(COALESCE(s.service_model, '')))  AS service_model,
    s.target_area_m2::float8                     AS target_area_m2,
    lower(btrim(COALESCE(bp.price_tier, 'mid')))           AS price_tier,
    lower(btrim(COALESCE(bp.primary_channel, 'balanced'))) AS primary_channel,
    lower(btrim(COALESCE(bp.parking_sensitivity, 'medium')))    AS parking_sens,
    lower(btrim(COALESCE(bp.frontage_sensitivity, 'medium')))   AS frontage_sens,
    lower(btrim(COALESCE(bp.visibility_sensitivity, 'medium'))) AS visibility_sens,
    lower(btrim(COALESCE(bp.expansion_goal, 'balanced')))  AS expansion_goal,
    bp.brand_archetype,
    COALESCE(bp.cannibalization_tolerance_m::float8, 1800.0) AS tolerance_m,
    COALESCE(bp.preferred_districts_json, '[]'::jsonb)     AS preferred_districts,
    COALESCE(bp.excluded_districts_json, '[]'::jsonb)      AS excluded_districts,
    (jsonb_array_length(COALESCE(bp.preferred_districts_json, '[]'::jsonb)) > 0
     OR jsonb_array_length(COALESCE(bp.excluded_districts_json, '[]'::jsonb)) > 0)
                                                 AS has_district_lists,
    c.district,
    COALESCE(c.area_m2::float8, 0.0)             AS area_m2,
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
    COALESCE(
      (c.score_breakdown_json->'bonus_detail'->>'base_deterministic')::float8,
      c.final_score::float8
    )                                            AS base_score,
    (c.feature_snapshot_json->>'realized_demand_30d')::float8 AS rd,
    COALESCE((c.feature_snapshot_json->>'realized_demand_branches')::float8, 0) AS rd_branches,
    c.score_breakdown_json->>'brand_archetype'   AS sb_archetype,
    -- flagship area ratio vs target (falls back to 350 m², :1666-1669)
    COALESCE(c.area_m2::float8, 0.0)
      / (CASE WHEN s.target_area_m2 IS NOT NULL AND s.target_area_m2 > 0
              THEN s.target_area_m2::float8 ELSE 350.0 END) AS area_ratio
  FROM expansion_candidate c
  JOIN expansion_search s        ON s.id = c.search_id
  LEFT JOIN expansion_brand_profile bp ON bp.search_id = s.id
  WHERE s.created_at >= now() - interval '30 days'
    AND c.score_breakdown_json->'weights' ? 'district_momentum'  -- v2-era only
    AND c.brand_fit_score IS NOT NULL
),
resolved AS (
  SELECT
    cand.*,
    -- Goal branch key. Archetype-era rows persist the resolved archetype in
    -- the breakdown JSON; otherwise mirror resolve_brand_archetype /
    -- legacy-goal fallback (:1651-1660).
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
    -- _sensitivity_weight: low 0.3 / medium 0.6 / high 1.0 (:1621)
    CASE parking_sens    WHEN 'low' THEN 0.3 WHEN 'high' THEN 1.0 ELSE 0.6 END AS pk_w,
    CASE frontage_sens   WHEN 'low' THEN 0.3 WHEN 'high' THEN 1.0 ELSE 0.6 END AS fr_w,
    CASE visibility_sens WHEN 'low' THEN 0.3 WHEN 'high' THEN 1.0 ELSE 0.6 END AS vi_w,
    -- district_component (:1642-1646); excluded overrides preferred
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
    -- overlap_fit (:1648-1649)
    LEAST(GREATEST(
      100.0 - abs(cann - LEAST(GREATEST((2500.0 - tolerance_m) / 25.0, 0.0), 100.0)) * 0.8,
    0.0), 100.0) AS overlap_fit,
    -- flagship area sub-leg (:1670-1677)
    CASE
      WHEN area_ratio BETWEEN 0.80 AND 1.20 THEN 100.0
      WHEN area_ratio BETWEEN 0.60 AND 1.50 THEN 80.0
      WHEN area_ratio BETWEEN 0.40 AND 2.00 THEN 55.0
      ELSE 30.0
    END AS flagship_area_comp
  FROM cand
),
legs AS (
  SELECT
    r.*,
    -- goal_component (:1661-1685)
    CASE r.goal_key
      WHEN 'flagship' THEN
        LEAST(GREATEST(r.flagship_area_comp * 0.6 + r.av * 0.4 + r.demand * 0.2, 0.0), 100.0)
      WHEN 'neighborhood' THEN
        LEAST(GREATEST(r.fit * 0.45 + (100.0 - abs(r.cann - 45.0)) * 0.25 + r.parking * 0.3, 0.0), 100.0)
      WHEN 'delivery_led' THEN
        LEAST(GREATEST(r.pd * 0.35 + r.pw * 0.35 + (100.0 - r.dc) * 0.3, 0.0), 100.0)
      ELSE
        LEAST(GREATEST((r.demand + r.fit + r.pw) / 3.0, 0.0), 100.0)
    END AS goal_comp,
    -- channel_component (_channel_fit_score :1625-1632)
    CASE r.primary_channel
      WHEN 'delivery' THEN
        LEAST(GREATEST(r.pd * 0.7 + r.mpp * 0.3, 0.0), 100.0)
      WHEN 'dine_in' THEN
        LEAST(GREATEST(
          (CASE WHEN r.service_model = 'dine_in' THEN 65.0 ELSE 50.0 END)
          + (100.0 - r.pd) * 0.2, 0.0), 100.0)
      ELSE
        LEAST(GREATEST(55.0 + (r.mpp - 50.0) * 0.2, 0.0), 100.0)
    END AS channel_comp,
    -- premium_penalty (:1698-1700); needs district_comp, computed in `resolved`
    CASE WHEN r.price_tier = 'premium'
         THEN GREATEST(0.0, 65.0 - r.av) * 0.35 + GREATEST(0.0, 60.0 - r.district_comp) * 0.25
         ELSE 0.0
    END AS premium_penalty
  FROM resolved r
)
SELECT
  l.*,
  (0.10 + l.pk_w * 0.06) AS parking_coef,
  (0.12 + l.fr_w * 0.03) AS fit_coef,
  (0.08 + l.vi_w * 0.05) AS vis_coef,
  LEAST(GREATEST(
      l.district_comp * 0.18
    + l.goal_comp     * 0.20
    + l.channel_comp  * 0.14
    + l.overlap_fit   * 0.14
    + l.parking * (0.10 + l.pk_w * 0.06)
    + l.fit     * (0.12 + l.fr_w * 0.03)
    + l.av      * (0.08 + l.vi_w * 0.05)
    + l.pw      * 0.08
    - l.premium_penalty,
  0.0), 100.0) AS bf_recon
FROM legs l;

\echo ''
\echo '=== A. Reconstruction fidelity (bf_recon vs persisted brand_fit_score) ==='
SELECT
  has_district_lists,
  COUNT(*)                                            AS n_candidates,
  COUNT(DISTINCT search_id)                           AS n_searches,
  ROUND(AVG(abs(bf_recon - bf_persisted))::numeric, 3)        AS mean_abs_err,
  ROUND((percentile_cont(0.95) WITHIN GROUP (ORDER BY abs(bf_recon - bf_persisted)))::numeric, 3)
                                                      AS p95_abs_err,
  ROUND(100.0 * AVG((abs(bf_recon - bf_persisted) <= 1.0)::int)::numeric, 1)
                                                      AS pct_within_1pt
FROM _bf_recon
GROUP BY has_district_lists
ORDER BY has_district_lists;

\echo ''
\echo '=== B. Per-leg mean contribution (pts within brand_fit 0-100) and variance share ==='
WITH leg_long AS (
  SELECT
    r.search_id, r.candidate_id, r.base_score, r.rd, r.rd_branches,
    v.leg, v.pts
  FROM _bf_recon r
  CROSS JOIN LATERAL (VALUES
    ('district_pref',      r.district_comp * 0.18),
    ('goal_component',     r.goal_comp     * 0.20),
    ('channel',            r.channel_comp  * 0.14),
    ('overlap_fit',        r.overlap_fit   * 0.14),
    ('parking',            r.parking * r.parking_coef),
    ('fit_area_zoning',    r.fit     * r.fit_coef),
    ('visibility',         r.av      * r.vis_coef),
    ('provider_whitespace',r.pw      * 0.08),
    ('premium_penalty',    -r.premium_penalty)
  ) AS v(leg, pts)
),
per_search_var AS (
  SELECT search_id, leg,
         var_samp(pts) AS leg_var,
         AVG(pts)      AS leg_mean,
         COUNT(*)      AS n
  FROM leg_long
  GROUP BY search_id, leg
  HAVING COUNT(*) >= 8
),
search_totals AS (
  SELECT search_id, SUM(leg_var) AS total_var
  FROM per_search_var
  GROUP BY search_id
)
SELECT
  v.leg,
  COUNT(*)                                       AS n_searches,
  ROUND(AVG(v.leg_mean)::numeric, 2)             AS mean_pts,
  ROUND(AVG(sqrt(v.leg_var))::numeric, 2)        AS mean_stddev_pts,
  ROUND(AVG(CASE WHEN t.total_var > 0 THEN v.leg_var / t.total_var END)::numeric, 3)
                                                 AS mean_variance_share
FROM per_search_var v
JOIN search_totals t USING (search_id)
GROUP BY v.leg
ORDER BY mean_variance_share DESC NULLS LAST;

\echo ''
\echo '=== C. Per-leg Spearman vs deterministic base score (all v2-era candidates) ==='
WITH leg_long AS (
  SELECT
    r.search_id, r.candidate_id, r.base_score,
    v.leg, v.pts
  FROM _bf_recon r
  CROSS JOIN LATERAL (VALUES
    ('district_pref',      r.district_comp * 0.18),
    ('goal_component',     r.goal_comp     * 0.20),
    ('channel',            r.channel_comp  * 0.14),
    ('overlap_fit',        r.overlap_fit   * 0.14),
    ('parking',            r.parking * r.parking_coef),
    ('fit_area_zoning',    r.fit     * r.fit_coef),
    ('visibility',         r.av      * r.vis_coef),
    ('provider_whitespace',r.pw      * 0.08),
    ('premium_penalty',    -r.premium_penalty)
  ) AS v(leg, pts)
),
ranked AS (
  SELECT search_id, leg,
         rank() OVER (PARTITION BY search_id, leg ORDER BY pts)        AS r_pts,
         rank() OVER (PARTITION BY search_id, leg ORDER BY base_score) AS r_base
  FROM leg_long
),
per_search AS (
  SELECT search_id, leg,
         corr(r_pts::float8, r_base::float8) AS spearman,
         COUNT(*) AS n
  FROM ranked
  GROUP BY search_id, leg
  HAVING COUNT(*) >= 8
)
SELECT
  leg,
  COUNT(spearman)                       AS n_searches,
  ROUND(AVG(spearman)::numeric, 3)      AS mean_spearman_vs_base,
  ROUND(MIN(spearman)::numeric, 3)      AS min_spearman,
  ROUND(MAX(spearman)::numeric, 3)      AS max_spearman
FROM per_search
GROUP BY leg
ORDER BY mean_spearman_vs_base DESC NULLS LAST;

\echo ''
\echo '=== D. Per-leg Spearman vs realized_demand_30d (rd-gated rows) ==='
WITH leg_long AS (
  SELECT
    r.search_id, r.candidate_id, r.rd,
    v.leg, v.pts
  FROM _bf_recon r
  CROSS JOIN LATERAL (VALUES
    ('district_pref',      r.district_comp * 0.18),
    ('goal_component',     r.goal_comp     * 0.20),
    ('channel',            r.channel_comp  * 0.14),
    ('overlap_fit',        r.overlap_fit   * 0.14),
    ('parking',            r.parking * r.parking_coef),
    ('fit_area_zoning',    r.fit     * r.fit_coef),
    ('visibility',         r.av      * r.vis_coef),
    ('provider_whitespace',r.pw      * 0.08),
    ('premium_penalty',    -r.premium_penalty)
  ) AS v(leg, pts)
  WHERE r.rd IS NOT NULL AND r.rd_branches >= 3
),
ranked AS (
  SELECT search_id, leg,
         rank() OVER (PARTITION BY search_id, leg ORDER BY pts) AS r_pts,
         rank() OVER (PARTITION BY search_id, leg ORDER BY rd)  AS r_rd
  FROM leg_long
),
per_search AS (
  SELECT search_id, leg,
         corr(r_pts::float8, r_rd::float8) AS spearman,
         COUNT(*) AS n
  FROM ranked
  GROUP BY search_id, leg
  HAVING COUNT(*) >= 8
)
SELECT
  leg,
  -- COUNT(spearman): legs that are CONSTANT within a search (e.g.
  -- district_pref under a default brief) yield NULL corr and drop out;
  -- counting rows would overstate the evidence base for those legs.
  COUNT(spearman)                       AS n_searches,
  ROUND(AVG(spearman)::numeric, 3)      AS mean_spearman_vs_rd,
  ROUND(MIN(spearman)::numeric, 3)      AS min_spearman,
  ROUND(MAX(spearman)::numeric, 3)      AS max_spearman,
  CASE WHEN AVG(spearman) < -0.15 THEN '<< rd-inverse leg' ELSE '' END AS flag
FROM per_search
GROUP BY leg
ORDER BY mean_spearman_vs_rd ASC NULLS LAST;
