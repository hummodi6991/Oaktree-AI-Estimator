\pset footer off
--
-- listing_quality_decomposition.sql  (Probe C — weight-stack investigation)
--
-- score_breakdown_json does NOT persist listing_quality sub-signals (only
-- the 0-100 composite under inputs->listing_quality), so this probe
-- RECONSTRUCTS them from feature_snapshot_json + the commercial_unit row,
-- exactly mirroring _listing_quality_score in app/services/expansion_advisor.py.
--
-- Weight-stack aware: rows scored under EXPANSION_WEIGHT_STACK=v2 are
-- detected via score_breakdown_json->'weights' ? 'district_momentum' and use
-- the v2 renormalized sub-weights (freshness 0.30/0.65, suitability
-- 0.20/0.65, image 0.10/0.65, furnished 0.05/0.65, momentum EXCLUDED —
-- it is paid once as the top-level district_momentum component). v1 rows
-- use the momentum-enabled tuple (0.30 / 0.20 / 0.10 / 0.05 / 0.35).
-- Both stacks keep the +5 drive-thru bonus.
--
-- Join key: commercial_unit.aqar_id = expansion_candidate.parcel_id.
-- Verified against the live schema: commercial_unit has no integer PK —
-- aqar_id varchar(64) is the key — and expansion_candidate.parcel_id holds
-- the Aqar listing ID for listing candidates (the candidate query emits
-- COALESCE(source_id, id::text) AS parcel_id). No status filter on the
-- join — the unit may have gone inactive since the search; cu.status is
-- emitted for transparency in section C.
--
-- Classification used in the report:
--   freshness  -> listing-artifact   (posting/refresh behavior on Aqar)
--   image      -> listing-artifact   (photo / LLM listing-quality read)
--   suitability-> mixed              (LLM read of the listing describing the site)
--   furnished  -> unit-intrinsic
--   momentum   -> district market signal (neither site nor listing; v1 rows only)
--
-- Section A reports per-sub-signal weighted-contribution variance (within
-- listing_quality, in listing-quality points). Variance shares ignore
-- covariance between sub-signals — they are a first-order attribution, not
-- an exact decomposition; section A also emits the recomposed composite vs
-- the stored input as a reconstruction check (mean_abs_diff should be ~0;
-- small drift = momentum entry changed between passes).
--
\echo ''
\echo '=== A. Sub-signal contribution stats across recent listing candidates ==='
WITH recent_searches AS (
  SELECT s.id
  FROM expansion_search s
  WHERE EXISTS (SELECT 1 FROM expansion_candidate c WHERE c.search_id = s.id)
  ORDER BY s.created_at DESC
  LIMIT 10
),
sub AS (
  SELECT
    c.search_id,
    c.id AS candidate_id,
    (c.score_breakdown_json->'inputs'->>'listing_quality')::numeric AS stored_lq,
    -- weight-stack flag: v2 rows carry district_momentum as a component
    (c.score_breakdown_json->'weights' ? 'district_momentum') AS is_v2,
    -- freshness band, from the persisted effective age
    CASE
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days') IS NULL THEN 50.0
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days')::numeric <= 14  THEN 100.0
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days')::numeric <= 30  THEN 92.0
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days')::numeric <= 60  THEN 80.0
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days')::numeric <= 120 THEN 65.0
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days')::numeric <= 240 THEN 45.0
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days')::numeric <= 365 THEN 28.0
      ELSE 15.0
    END AS freshness,
    -- suitability: LLM verdict, else structural restaurant_score * 2, else 50
    COALESCE(
      LEAST(100.0, GREATEST(0.0, cu.llm_suitability_score::numeric)),
      CASE WHEN cu.restaurant_score IS NOT NULL AND cu.restaurant_score > 0
           THEN LEAST(100.0, GREATEST(0.0, cu.restaurant_score::numeric * 2.0)) END,
      50.0
    ) AS suitability,
    -- image signal: LLM listing-quality, else binary image presence
    COALESCE(
      LEAST(100.0, GREATEST(0.0, cu.llm_listing_quality_score::numeric)),
      CASE WHEN c.image_url IS NOT NULL AND c.image_url <> '' THEN 100.0 ELSE 30.0 END
    ) AS image_signal,
    CASE WHEN cu.is_furnished IS TRUE THEN 100.0 ELSE 50.0 END AS furnished_signal,
    COALESCE(
      (c.feature_snapshot_json->'district_momentum'->>'momentum_score')::numeric,
      50.0
    ) AS momentum_signal,
    CASE WHEN cu.has_drive_thru IS TRUE THEN 5.0 ELSE 0.0 END AS drive_thru_bonus
  FROM expansion_candidate c
  JOIN recent_searches rs ON rs.id = c.search_id
  LEFT JOIN commercial_unit cu ON cu.aqar_id = c.parcel_id
  WHERE c.commercial_unit_id IS NOT NULL          -- listings only
    AND c.score_breakdown_json ? 'inputs'
),
weighted AS (
  SELECT *,
    freshness        * CASE WHEN is_v2 THEN 0.30/0.65 ELSE 0.30 END AS w_freshness,
    suitability      * CASE WHEN is_v2 THEN 0.20/0.65 ELSE 0.20 END AS w_suitability,
    image_signal     * CASE WHEN is_v2 THEN 0.10/0.65 ELSE 0.10 END AS w_image,
    furnished_signal * CASE WHEN is_v2 THEN 0.05/0.65 ELSE 0.05 END AS w_furnished,
    momentum_signal  * CASE WHEN is_v2 THEN 0.0       ELSE 0.35 END AS w_momentum,
    (  freshness        * CASE WHEN is_v2 THEN 0.30/0.65 ELSE 0.30 END
     + suitability      * CASE WHEN is_v2 THEN 0.20/0.65 ELSE 0.20 END
     + image_signal     * CASE WHEN is_v2 THEN 0.10/0.65 ELSE 0.10 END
     + furnished_signal * CASE WHEN is_v2 THEN 0.05/0.65 ELSE 0.05 END
     + momentum_signal  * CASE WHEN is_v2 THEN 0.0       ELSE 0.35 END
     + drive_thru_bonus) AS recomposed
  FROM sub
)
SELECT * FROM (
  SELECT 'freshness (artifact, v1 0.30 / v2 0.4615)' AS sub_signal, ROUND(AVG(w_freshness)::numeric,2)  AS mean_pts, ROUND(VAR_SAMP(w_freshness)::numeric,2)  AS variance, ROUND(STDDEV_SAMP(w_freshness)::numeric,2)  AS stddev FROM weighted
  UNION ALL
  SELECT 'suitability (mixed, v1 0.20 / v2 0.3077)',    ROUND(AVG(w_suitability)::numeric,2), ROUND(VAR_SAMP(w_suitability)::numeric,2), ROUND(STDDEV_SAMP(w_suitability)::numeric,2) FROM weighted
  UNION ALL
  SELECT 'image (artifact, v1 0.10 / v2 0.1538)',       ROUND(AVG(w_image)::numeric,2),       ROUND(VAR_SAMP(w_image)::numeric,2),       ROUND(STDDEV_SAMP(w_image)::numeric,2)       FROM weighted
  UNION ALL
  SELECT 'furnished (unit, v1 0.05 / v2 0.0769)',       ROUND(AVG(w_furnished)::numeric,2),   ROUND(VAR_SAMP(w_furnished)::numeric,2),   ROUND(STDDEV_SAMP(w_furnished)::numeric,2)   FROM weighted
  UNION ALL
  SELECT 'momentum (market, v1 0.35 / v2 EXCLUDED)',    ROUND(AVG(w_momentum)::numeric,2),    ROUND(VAR_SAMP(w_momentum)::numeric,2),    ROUND(STDDEV_SAMP(w_momentum)::numeric,2)    FROM weighted
  UNION ALL
  SELECT 'RECONSTRUCTION CHECK (abs diff)', ROUND(AVG(ABS(recomposed - stored_lq))::numeric,2), NULL, NULL FROM weighted
) t;

\echo ''
\echo '=== B. Variance share: listing-artifact vs site/unit vs market ==='
-- First-order attribution (covariances ignored): share of summed sub-signal
-- variances. artifact = freshness + image; mixed = suitability;
-- unit = furnished; market = momentum (0 by construction for v2 rows).
WITH recent_searches AS (
  SELECT s.id FROM expansion_search s
  WHERE EXISTS (SELECT 1 FROM expansion_candidate c WHERE c.search_id = s.id)
  ORDER BY s.created_at DESC LIMIT 10
),
sub AS (
  SELECT
    CASE
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days') IS NULL THEN 50.0
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days')::numeric <= 14  THEN 100.0
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days')::numeric <= 30  THEN 92.0
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days')::numeric <= 60  THEN 80.0
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days')::numeric <= 120 THEN 65.0
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days')::numeric <= 240 THEN 45.0
      WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days')::numeric <= 365 THEN 28.0
      ELSE 15.0
    END * CASE WHEN (c.score_breakdown_json->'weights' ? 'district_momentum') THEN 0.30/0.65 ELSE 0.30 END AS w_freshness,
    COALESCE(
      LEAST(100.0, GREATEST(0.0, cu.llm_suitability_score::numeric)),
      CASE WHEN cu.restaurant_score IS NOT NULL AND cu.restaurant_score > 0
           THEN LEAST(100.0, GREATEST(0.0, cu.restaurant_score::numeric * 2.0)) END,
      50.0
    ) * CASE WHEN (c.score_breakdown_json->'weights' ? 'district_momentum') THEN 0.20/0.65 ELSE 0.20 END AS w_suitability,
    COALESCE(
      LEAST(100.0, GREATEST(0.0, cu.llm_listing_quality_score::numeric)),
      CASE WHEN c.image_url IS NOT NULL AND c.image_url <> '' THEN 100.0 ELSE 30.0 END
    ) * CASE WHEN (c.score_breakdown_json->'weights' ? 'district_momentum') THEN 0.10/0.65 ELSE 0.10 END AS w_image,
    CASE WHEN cu.is_furnished IS TRUE THEN 100.0 ELSE 50.0 END
      * CASE WHEN (c.score_breakdown_json->'weights' ? 'district_momentum') THEN 0.05/0.65 ELSE 0.05 END AS w_furnished,
    COALESCE((c.feature_snapshot_json->'district_momentum'->>'momentum_score')::numeric, 50.0)
      * CASE WHEN (c.score_breakdown_json->'weights' ? 'district_momentum') THEN 0.0 ELSE 0.35 END AS w_momentum
  FROM expansion_candidate c
  JOIN recent_searches rs ON rs.id = c.search_id
  LEFT JOIN commercial_unit cu ON cu.aqar_id = c.parcel_id
  WHERE c.commercial_unit_id IS NOT NULL
),
v AS (
  SELECT
    VAR_SAMP(w_freshness)  AS v_fresh,
    VAR_SAMP(w_suitability) AS v_suit,
    VAR_SAMP(w_image)      AS v_img,
    VAR_SAMP(w_furnished)  AS v_furn,
    VAR_SAMP(w_momentum)   AS v_mom
  FROM sub
)
SELECT
  ROUND(((v_fresh + v_img) / NULLIF(v_fresh+v_suit+v_img+v_furn+v_mom,0) * 100)::numeric, 1) AS artifact_pct,
  ROUND((v_suit            / NULLIF(v_fresh+v_suit+v_img+v_furn+v_mom,0) * 100)::numeric, 1) AS mixed_llm_pct,
  ROUND((v_furn            / NULLIF(v_fresh+v_suit+v_img+v_furn+v_mom,0) * 100)::numeric, 1) AS unit_intrinsic_pct,
  ROUND((v_mom             / NULLIF(v_fresh+v_suit+v_img+v_furn+v_mom,0) * 100)::numeric, 1) AS market_momentum_pct
FROM v;

\echo ''
\echo '=== C. Coverage: how often each sub-signal is real vs neutral fallback ==='
WITH recent_searches AS (
  SELECT s.id FROM expansion_search s
  WHERE EXISTS (SELECT 1 FROM expansion_candidate c WHERE c.search_id = s.id)
  ORDER BY s.created_at DESC LIMIT 10
)
SELECT
  COUNT(*)                                                                          AS n_listing_candidates,
  SUM(CASE WHEN cu.aqar_id IS NULL THEN 1 ELSE 0 END)                               AS unit_join_misses,
  SUM(CASE WHEN cu.status IS DISTINCT FROM 'active' THEN 1 ELSE 0 END)              AS unit_not_active_now,
  SUM(CASE WHEN (c.feature_snapshot_json->'listing_age'->>'effective_age_days') IS NULL THEN 1 ELSE 0 END) AS age_unknown,
  SUM(CASE WHEN cu.llm_suitability_score IS NULL THEN 1 ELSE 0 END)                 AS llm_suitability_null,
  SUM(CASE WHEN cu.llm_listing_quality_score IS NULL THEN 1 ELSE 0 END)             AS llm_quality_null,
  SUM(CASE WHEN cu.llm_landlord_signal_score IS NULL THEN 1 ELSE 0 END)             AS llm_landlord_null,
  SUM(CASE WHEN COALESCE((c.feature_snapshot_json->'district_momentum'->>'sample_floor_applied')::boolean, true) THEN 1 ELSE 0 END) AS momentum_neutral
FROM expansion_candidate c
JOIN recent_searches rs ON rs.id = c.search_id
LEFT JOIN commercial_unit cu ON cu.aqar_id = c.parcel_id
WHERE c.commercial_unit_id IS NOT NULL;
