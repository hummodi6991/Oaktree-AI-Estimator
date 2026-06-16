-- Radiance YoY distribution diagnostic for calibrating
-- EXPANSION_VIABILITY_RADIANCE_YOY_THRESHOLD (rescue side) and reviewing
-- the existing EXPANSION_VIABILITY_RADIANCE_YOY_DEMOTE_THRESHOLD leg.
--
-- Schema sources of truth (no guessed column names):
--   * district_radiance_monthly
--       app/models/tables.py:351-367
--       alembic/versions/20260501_add_district_radiance_monthly.py:20-46
--     Columns used: district_key, year_month, radiance_mean,
--                   pixel_count_valid, quality_filter, source
--   * external_feature (layer_name='aqar_district_hulls')
--       referenced by app/ingest/black_marble_radiance.py:128-148 and
--       app/services/expansion_advisor.py:7119-7148 (district polygons +
--       district_en in jsonb properties; see
--       app/ingest/aqar_district_hulls.py:67-141).
--     Columns used: layer_name, geometry,
--                   properties->>'district_raw',
--                   properties->>'district',
--                   properties->>'district_en'
--   * expansion_candidate
--       alembic/versions/20260310_exp_adv_v0.py:47-77 (id, search_id,
--       computed_at)
--       alembic/versions/20260313_exp_adv_v6_features.py:28
--       (feature_snapshot_json JSONB)
--     Reads: feature_snapshot_json->'radiance_growth'->>'value_yoy_pct'
--            feature_snapshot_json->'radiance_growth'->>'confident'
--            feature_snapshot_json->'radiance_growth'->>'confidence_reason'
--            feature_snapshot_json->>'district_display'
--
-- Quality filter label and source constants (cited verbatim against the
-- production runtime, not invented):
--   * QUALITY_FILTER_LABEL = 'lenient_qa_lt_2'   app/connectors/blackmarble.py:29
--   * SOURCE_LABEL         = 'nasa_blackmarble_vnp46a3_c2'
--                            app/connectors/blackmarble.py:30
--
-- Confidence rules (mirrors app/connectors/blackmarble.py:47-87):
--   * pixel_count_valid >= 10                  (PIXEL_COUNT_FLOOR)
--   * district polygon area >= 0.5 km^2         (SMALL_DISTRICT_FLOOR_KM2)
--   * district polygon area <= 500 km^2         (LARGE_DISTRICT_OUTLIER_KM2)
-- A district is "low-confidence" when ANY of these fail. There is no
-- hardcoded permanent exclusion list in the repo (see report Part C2).
--
-- Window: the production radiance pipeline uses a pixel-weighted
-- rolling-6-month YoY (current rn-5..rn vs rn-17..rn-12) — see
-- app/services/expansion_advisor.py:7158-7232. This script reproduces
-- the same windowed view so the buckets here match what the live gate
-- evaluates per candidate. The candidate-side cross-tab in section 7
-- reads the already-computed value_yoy_pct from feature_snapshot_json
-- so it never recomputes inside the per-search loop.
--
-- Run with:
--   psql -f scripts/diagnostics/radiance_yoy_distribution.sql | tee /tmp/radiance.txt

\timing on
\pset pager off

-- ---------------------------------------------------------------------------
-- Reusable CTE: latest rolling-6 pixel-weighted YoY per district_key.
-- Mirrors app/services/expansion_advisor.py:7158-7232 verbatim.
-- ---------------------------------------------------------------------------
CREATE TEMP TABLE _district_areas ON COMMIT DROP AS
SELECT
    TRIM(COALESCE(ef.properties->>'district_raw',
                  ef.properties->>'district')) AS district_label,
    MAX(ef.properties->>'district_en')         AS district_en,
    MAX(
      ST_Area(
        ST_SetSRID(ST_GeomFromGeoJSON(ef.geometry::text), 4326)::geography
      ) / 1e6
    )                                          AS area_km2
FROM external_feature ef
WHERE ef.layer_name = 'aqar_district_hulls'
  AND ef.geometry IS NOT NULL
  AND jsonb_typeof(ef.geometry) = 'object'
  AND COALESCE(ef.properties->>'district_raw',
               ef.properties->>'district') IS NOT NULL
  AND TRIM(COALESCE(ef.properties->>'district_raw',
                    ef.properties->>'district')) <> ''
GROUP BY 1;

-- Bridge district_label (Arabic raw, as in external_feature.properties)
-- to the normalized district_key used at radiance-ingest time. The python
-- helper norm_district() is not directly portable to SQL; we approximate
-- via TRIM + alef/yaa fold + "حي " prefix strip (mirrors
-- normalize_district_key_sql in app/services/aqar_district_match.py:113).
-- Caveat: english fallbacks in district_key are lowercased by
-- norm_district() but we do not attempt to invert; non-Arabic
-- district_key rows that fail to match _district_areas will surface
-- under "district_en IS NULL" in sections 4-6 and should be inspected.
CREATE TEMP TABLE _district_xwalk ON COMMIT DROP AS
SELECT
    TRIM(REGEXP_REPLACE(
        TRANSLATE(district_label,
            E'أإآىـ',
            E'اااي'
        ),
        E'^حي\\s+', '', 'g'
    )) AS district_key,
    district_label AS district_label_ar,
    district_en,
    area_km2
FROM _district_areas;

CREATE TEMP TABLE _radiance_yoy ON COMMIT DROP AS
WITH ordered AS (
    SELECT
        district_key,
        year_month,
        radiance_mean,
        pixel_count_valid,
        ROW_NUMBER() OVER (
            PARTITION BY district_key ORDER BY year_month
        ) AS rn
    FROM district_radiance_monthly
    WHERE source = 'nasa_blackmarble_vnp46a3_c2'
      AND quality_filter = 'lenient_qa_lt_2'
),
windowed AS (
    SELECT
        district_key,
        year_month,
        rn,
        pixel_count_valid,
        SUM(radiance_mean * pixel_count_valid)
            FILTER (WHERE radiance_mean IS NOT NULL AND pixel_count_valid > 0)
            OVER w_cur
        / NULLIF(
            SUM(pixel_count_valid)
                FILTER (WHERE radiance_mean IS NOT NULL AND pixel_count_valid > 0)
                OVER w_cur,
            0
        ) AS rad_cur6,
        SUM(radiance_mean * pixel_count_valid)
            FILTER (WHERE radiance_mean IS NOT NULL AND pixel_count_valid > 0)
            OVER w_prev
        / NULLIF(
            SUM(pixel_count_valid)
                FILTER (WHERE radiance_mean IS NOT NULL AND pixel_count_valid > 0)
                OVER w_prev,
            0
        ) AS rad_prev6,
        MIN(pixel_count_valid) OVER w_cur  AS min_pixels_cur6,
        MIN(pixel_count_valid) OVER w_prev AS min_pixels_prev6,
        COUNT(*) OVER w_cur  AS rows_cur6,
        COUNT(*) OVER w_prev AS rows_prev6
    FROM ordered
    WINDOW
        w_cur  AS (PARTITION BY district_key ORDER BY year_month
                   ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
        w_prev AS (PARTITION BY district_key ORDER BY year_month
                   ROWS BETWEEN 17 PRECEDING AND 12 PRECEDING)
),
latest_per_district AS (
    SELECT *
    FROM windowed
    WHERE (district_key, rn) IN (
        SELECT district_key, MAX(rn) FROM windowed GROUP BY district_key
    )
)
SELECT
    lpd.district_key,
    xw.district_label_ar,
    xw.district_en,
    xw.area_km2,
    lpd.year_month                       AS ym_cur,
    lpd.rad_cur6                         AS rad_cur,
    lpd.rad_prev6                        AS rad_prev,
    lpd.min_pixels_cur6                  AS pixels_cur,
    lpd.min_pixels_prev6                 AS pixels_prev,
    lpd.rows_cur6,
    lpd.rows_prev6,
    -- YoY value_yoy_pct on the 0..100 scale (matches the python branch in
    -- app/services/expansion_advisor.py:7250-7253).
    CASE
      WHEN lpd.rad_prev6 IS NOT NULL AND lpd.rad_prev6 > 0
      THEN (lpd.rad_cur6 - lpd.rad_prev6) / lpd.rad_prev6 * 100.0
      ELSE NULL
    END AS value_yoy_pct,
    -- Confidence flags mirroring evaluate_confidence() in
    -- app/connectors/blackmarble.py:47-87. low_confidence = any of these
    -- is true; gate ABSTAINs (treats as no signal) when low_confidence.
    (lpd.min_pixels_cur6 < 10  OR lpd.min_pixels_prev6 < 10) AS fail_pixel_floor,
    (xw.area_km2 IS NOT NULL AND xw.area_km2 < 0.5)          AS fail_small_district,
    (xw.area_km2 IS NOT NULL AND xw.area_km2 > 500.0)        AS fail_large_district,
    (lpd.rows_cur6 <> 6 OR lpd.rows_prev6 <> 6)              AS fail_partial_window
FROM latest_per_district lpd
LEFT JOIN _district_xwalk xw USING (district_key);

CREATE INDEX ON _radiance_yoy (district_key);

-- ---------------------------------------------------------------------------
-- 1. Total district count with any radiance_yoy value vs total Riyadh
--    district count.
-- ---------------------------------------------------------------------------
\echo
\echo '=== 1. District coverage (radiance vs total Riyadh districts) ==='
SELECT
  (SELECT COUNT(DISTINCT district_key) FROM district_radiance_monthly
    WHERE source = 'nasa_blackmarble_vnp46a3_c2'
      AND quality_filter = 'lenient_qa_lt_2')                AS ingested_districts,
  (SELECT COUNT(*) FROM _radiance_yoy
    WHERE value_yoy_pct IS NOT NULL)                          AS with_yoy_value,
  (SELECT COUNT(*) FROM _radiance_yoy
    WHERE value_yoy_pct IS NOT NULL
      AND fail_pixel_floor    = false
      AND fail_small_district = false
      AND fail_large_district = false
      AND fail_partial_window = false)                        AS confident_yoy,
  (SELECT COUNT(*) FROM _district_xwalk)                      AS riyadh_district_polygons;

-- ---------------------------------------------------------------------------
-- 2. Distribution of radiance_yoy: bucket counts.
-- Buckets (value_yoy_pct on the 0..100 percent scale): <-10, -10..0, 0..2,
-- 2..5, 5..10, >=10. Edges per the task spec; "-0.10..0" interpreted as
-- -10..0 percent because value_yoy_pct is stored as percent in
-- feature_snapshot_json (rounded to 2dp at write, see
-- app/services/expansion_advisor.py:7255). If the calibration narrative
-- expects fractional 0.02 = 2%, the buckets line up identically.
-- ---------------------------------------------------------------------------
\echo
\echo '=== 2. radiance_yoy bucket distribution (all districts with a value) ==='
SELECT
  CASE
    WHEN value_yoy_pct < -10               THEN '1. <-10%%'
    WHEN value_yoy_pct < 0                 THEN '2. -10..0%%'
    WHEN value_yoy_pct < 2                 THEN '3. 0..2%%'
    WHEN value_yoy_pct < 5                 THEN '4. 2..5%%'
    WHEN value_yoy_pct < 10                THEN '5. 5..10%%'
    ELSE                                        '6. >=10%%'
  END                                                          AS bucket,
  COUNT(*)                                                     AS n_districts,
  COUNT(*) FILTER (
    WHERE fail_pixel_floor = false
      AND fail_small_district = false
      AND fail_large_district = false
      AND fail_partial_window = false
  )                                                            AS n_confident
FROM _radiance_yoy
WHERE value_yoy_pct IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- ---------------------------------------------------------------------------
-- 3. Per-bucket mean of the worst-case pixel coverage across the YoY window
--    (proxy for confidence; the live gate uses MIN over each 6-month window).
--    "quality" is not a per-row column on district_radiance_monthly — the
--    raw QA mask is collapsed at ingest into pixel_count_valid (see
--    QUALITY_FILTER_LABEL='lenient_qa_lt_2', app/connectors/blackmarble.py:29).
-- ---------------------------------------------------------------------------
\echo
\echo '=== 3. Per-bucket mean pixel coverage (proxy for confidence) ==='
SELECT
  CASE
    WHEN value_yoy_pct < -10               THEN '1. <-10%%'
    WHEN value_yoy_pct < 0                 THEN '2. -10..0%%'
    WHEN value_yoy_pct < 2                 THEN '3. 0..2%%'
    WHEN value_yoy_pct < 5                 THEN '4. 2..5%%'
    WHEN value_yoy_pct < 10                THEN '5. 5..10%%'
    ELSE                                        '6. >=10%%'
  END                                                          AS bucket,
  ROUND(AVG(pixels_cur)::numeric, 1)                           AS mean_min_pixels_cur6,
  ROUND(AVG(pixels_prev)::numeric, 1)                          AS mean_min_pixels_prev6,
  ROUND(AVG(area_km2)::numeric, 3)                             AS mean_area_km2
FROM _radiance_yoy
WHERE value_yoy_pct IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- ---------------------------------------------------------------------------
-- 4. Top 20 districts by radiance_yoy.
-- ---------------------------------------------------------------------------
\echo
\echo '=== 4. Top 20 districts by value_yoy_pct ==='
SELECT
  district_en,
  district_label_ar                                  AS district_ar,
  ROUND(value_yoy_pct::numeric, 2)                   AS yoy_pct,
  pixels_cur,
  pixels_prev,
  ROUND(area_km2::numeric, 3)                        AS area_km2,
  CASE
    WHEN fail_pixel_floor    THEN 'pixel_floor'
    WHEN fail_small_district THEN 'small_district'
    WHEN fail_large_district THEN 'large_district'
    WHEN fail_partial_window THEN 'partial_window'
    ELSE                          'confident'
  END                                                AS quality_outcome
FROM _radiance_yoy
WHERE value_yoy_pct IS NOT NULL
ORDER BY value_yoy_pct DESC NULLS LAST
LIMIT 20;

-- ---------------------------------------------------------------------------
-- 5. Bottom 20 districts by radiance_yoy.
-- ---------------------------------------------------------------------------
\echo
\echo '=== 5. Bottom 20 districts by value_yoy_pct ==='
SELECT
  district_en,
  district_label_ar                                  AS district_ar,
  ROUND(value_yoy_pct::numeric, 2)                   AS yoy_pct,
  pixels_cur,
  pixels_prev,
  ROUND(area_km2::numeric, 3)                        AS area_km2,
  CASE
    WHEN fail_pixel_floor    THEN 'pixel_floor'
    WHEN fail_small_district THEN 'small_district'
    WHEN fail_large_district THEN 'large_district'
    WHEN fail_partial_window THEN 'partial_window'
    ELSE                          'confident'
  END                                                AS quality_outcome
FROM _radiance_yoy
WHERE value_yoy_pct IS NOT NULL
ORDER BY value_yoy_pct ASC NULLS LAST
LIMIT 20;

-- ---------------------------------------------------------------------------
-- 6. Full low-confidence list — gate should ABSTAIN (not demote) on these.
--    Mirrors the (confident == false) branch of evaluate_confidence().
-- ---------------------------------------------------------------------------
\echo
\echo '=== 6. Low-confidence districts (gate should ABSTAIN) ==='
SELECT
  district_key,
  district_en,
  district_label_ar                                  AS district_ar,
  ROUND(value_yoy_pct::numeric, 2)                   AS yoy_pct,
  pixels_cur,
  pixels_prev,
  ROUND(area_km2::numeric, 3)                        AS area_km2,
  CASE
    WHEN fail_pixel_floor    THEN 'pixel_floor'
    WHEN fail_small_district THEN 'small_district'
    WHEN fail_large_district THEN 'large_district'
    WHEN fail_partial_window THEN 'partial_window'
    ELSE                          'confident'
  END                                                AS reason
FROM _radiance_yoy
WHERE fail_pixel_floor
   OR fail_small_district
   OR fail_large_district
   OR fail_partial_window
ORDER BY reason, district_key;

-- ---------------------------------------------------------------------------
-- 7. Candidate-side cross-tab — how many candidates from the last 30 days
--    of expansion_search would each candidate threshold value haircut.
--    Reads the already-evaluated radiance_growth snapshot persisted on the
--    candidate row (feature_snapshot_json->'radiance_growth') rather than
--    re-joining to district_radiance_monthly so the result matches what the
--    live demote leg saw at search time.
-- ---------------------------------------------------------------------------
\echo
\echo '=== 7a. Candidate count by radiance_yoy bucket (last 30 days) ==='
WITH cand AS (
    SELECT
      ec.id,
      (ec.feature_snapshot_json->'radiance_growth'->>'value_yoy_pct')::float AS yoy,
      (ec.feature_snapshot_json->'radiance_growth'->>'confident')::bool      AS confident
    FROM expansion_candidate ec
    WHERE ec.computed_at >= NOW() - INTERVAL '30 days'
)
SELECT
  CASE
    WHEN yoy IS NULL                       THEN '0. no signal (NULL / absent)'
    WHEN confident IS NOT TRUE             THEN '0b. low-confidence (abstain)'
    WHEN yoy < -10                         THEN '1. <-10%%'
    WHEN yoy < 0                           THEN '2. -10..0%%'
    WHEN yoy < 2                           THEN '3. 0..2%%'
    WHEN yoy < 5                           THEN '4. 2..5%%'
    WHEN yoy < 10                          THEN '5. 5..10%%'
    ELSE                                        '6. >=10%%'
  END                                                          AS bucket,
  COUNT(*)                                                     AS n_candidates,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)           AS pct_of_total
FROM cand
GROUP BY 1
ORDER BY 1;

\echo
\echo '=== 7b. Haircut sweep: how many candidates each rescue/demote threshold would affect ==='
WITH cand AS (
    SELECT
      (ec.feature_snapshot_json->'radiance_growth'->>'value_yoy_pct')::float AS yoy,
      (ec.feature_snapshot_json->'radiance_growth'->>'confident')::bool      AS confident
    FROM expansion_candidate ec
    WHERE ec.computed_at >= NOW() - INTERVAL '30 days'
), thresholds(t) AS (
    VALUES (0.0::float), (0.5), (1.0), (1.5), (2.0), (2.5), (3.0), (4.0), (5.0)
)
SELECT
  thresholds.t AS threshold_pct,
  -- Rescue side (operator >=, EXPANSION_VIABILITY_RADIANCE_YOY_THRESHOLD):
  -- rescues pop+rent legs when confident AND yoy >= t.
  COUNT(*) FILTER (
    WHERE confident IS TRUE AND yoy IS NOT NULL AND yoy >= thresholds.t
  )                                              AS n_would_rescue,
  -- Demote side (operator <, EXPANSION_VIABILITY_RADIANCE_YOY_DEMOTE_THRESHOLD):
  -- demotes when confident AND yoy < t.
  COUNT(*) FILTER (
    WHERE confident IS TRUE AND yoy IS NOT NULL AND yoy < thresholds.t
  )                                              AS n_would_demote,
  COUNT(*) FILTER (
    WHERE confident IS NOT TRUE OR yoy IS NULL
  )                                              AS n_abstain
FROM cand, thresholds
GROUP BY thresholds.t
ORDER BY thresholds.t;
