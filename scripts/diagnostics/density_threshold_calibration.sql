-- density_threshold_calibration.sql
--
-- Diagnostic: calibrate a threshold X for the new delivery_market_pass
-- gate of the form `provider_density_score >= X` (replacing the current
-- composite `0.6 * density + 0.4 * multi_platform >= 45.0`).
--
-- Window: trailing 90 days of expansion_candidate rows (by computed_at).
--
-- Column sources (verified against migrations):
--   * expansion_candidate.provider_density_score  — top-level column
--       (alembic/versions/20260311_exp_adv_brand_v4.py:41)
--   * expansion_candidate.final_rank              — top-level column
--       (alembic/versions/20260418_ea_rerank_persistence.py:49)
--   * expansion_candidate.computed_at             — top-level column
--       (alembic/versions/20260310_exp_adv_v0.py:72)
--   * expansion_candidate.search_id  -> expansion_search.id (FK)
--   * expansion_search.service_model              — top-level column
--       (alembic/versions/20260310_exp_adv_v0.py:32)
--
-- NOTE on rent_burden_score: it is NOT a top-level column on
-- expansion_candidate. It is a computed value (see
-- app/services/expansion_advisor.py:4237) that lives inside score
-- breakdowns rather than as a queryable column. Section E therefore
-- omits the secondary rent_burden_score >= 50 filter and shows the raw
-- pass-rate at each threshold only.
--
-- Read-only. iPad-Safari-safe. No psql :var substitutions; the 90-day
-- window is hardcoded as (now() - interval '90 days').

\pset border 2
\pset format aligned

\echo
\echo ============================================================
\echo Section A - Density distribution overall (trailing 90 days)
\echo 5-point buckets, all candidates
\echo ============================================================
\echo

WITH base AS (
  SELECT provider_density_score
  FROM expansion_candidate
  WHERE computed_at >= now() - interval '90 days'
    AND provider_density_score IS NOT NULL
),
bucketed AS (
  SELECT
    LEAST(FLOOR(provider_density_score / 5.0)::int, 19) AS bucket_idx
  FROM base
),
totals AS (SELECT COUNT(*) AS n FROM base)
SELECT
  (b.bucket_idx * 5)            AS bucket_low,
  (b.bucket_idx * 5 + 5)        AS bucket_high,
  COUNT(*)                      AS candidate_count,
  ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM totals), 0), 2) AS pct_of_total
FROM bucketed b
GROUP BY b.bucket_idx
ORDER BY b.bucket_idx;

\echo
\echo ============================================================
\echo Section B - Density distribution among top-ranked candidates
\echo final_rank <= 5 within each search (trailing 90 days)
\echo ============================================================
\echo

WITH base AS (
  SELECT provider_density_score
  FROM expansion_candidate
  WHERE computed_at >= now() - interval '90 days'
    AND provider_density_score IS NOT NULL
    AND final_rank IS NOT NULL
    AND final_rank <= 5
),
bucketed AS (
  SELECT
    LEAST(FLOOR(provider_density_score / 5.0)::int, 19) AS bucket_idx
  FROM base
),
totals AS (SELECT COUNT(*) AS n FROM base)
SELECT
  (b.bucket_idx * 5)            AS bucket_low,
  (b.bucket_idx * 5 + 5)        AS bucket_high,
  COUNT(*)                      AS candidate_count,
  ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM totals), 0), 2) AS pct_of_total
FROM bucketed b
GROUP BY b.bucket_idx
ORDER BY b.bucket_idx;

\echo
\echo ============================================================
\echo Section C - Top-5 density distribution sliced by service_model
\echo service_model from expansion_search (JOIN on search_id)
\echo Delivery slice is the most diagnostic; qsr is next-best proxy.
\echo ============================================================
\echo

WITH base AS (
  SELECT
    s.service_model               AS service_model,
    c.provider_density_score      AS provider_density_score
  FROM expansion_candidate c
  JOIN expansion_search    s ON s.id = c.search_id
  WHERE c.computed_at >= now() - interval '90 days'
    AND c.provider_density_score IS NOT NULL
    AND c.final_rank IS NOT NULL
    AND c.final_rank <= 5
),
bucketed AS (
  SELECT
    service_model,
    LEAST(FLOOR(provider_density_score / 5.0)::int, 19) AS bucket_idx
  FROM base
),
totals AS (
  SELECT service_model, COUNT(*) AS n
  FROM base
  GROUP BY service_model
)
SELECT
  b.service_model               AS service_model,
  (b.bucket_idx * 5)            AS bucket_low,
  (b.bucket_idx * 5 + 5)        AS bucket_high,
  COUNT(*)                      AS candidate_count,
  ROUND(100.0 * COUNT(*) / NULLIF(t.n, 0), 2) AS pct_of_service_model
FROM bucketed b
JOIN totals   t ON t.service_model = b.service_model
GROUP BY b.service_model, b.bucket_idx, t.n
ORDER BY b.service_model, b.bucket_idx;

\echo
\echo ============================================================
\echo Section D - Quantiles of provider_density_score
\echo Two rows: full candidate set, then top-5 only.
\echo ============================================================
\echo

SELECT
  'all_candidates'                                                                    AS scope,
  COUNT(*)                                                                            AS n,
  ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY provider_density_score)::numeric, 2) AS p25,
  ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY provider_density_score)::numeric, 2) AS p50_median,
  ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY provider_density_score)::numeric, 2) AS p75,
  ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY provider_density_score)::numeric, 2) AS p90
FROM expansion_candidate
WHERE computed_at >= now() - interval '90 days'
  AND provider_density_score IS NOT NULL
UNION ALL
SELECT
  'top5_candidates'                                                                   AS scope,
  COUNT(*)                                                                            AS n,
  ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY provider_density_score)::numeric, 2) AS p25,
  ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY provider_density_score)::numeric, 2) AS p50_median,
  ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY provider_density_score)::numeric, 2) AS p75,
  ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY provider_density_score)::numeric, 2) AS p90
FROM expansion_candidate
WHERE computed_at >= now() - interval '90 days'
  AND provider_density_score IS NOT NULL
  AND final_rank IS NOT NULL
  AND final_rank <= 5;

\echo
\echo ============================================================
\echo Section E - Pass-rate simulation per threshold
\echo For each candidate threshold X, how many candidates would
\echo pass `provider_density_score >= X` in the trailing 90 days.
\echo NOTE: secondary rent_burden_score filter omitted because
\echo rent_burden_score is not a top-level column on
\echo expansion_candidate (computed in services layer; see
\echo app/services/expansion_advisor.py:4237). Raw pass-rate only.
\echo ============================================================
\echo

WITH base AS (
  SELECT provider_density_score
  FROM expansion_candidate
  WHERE computed_at >= now() - interval '90 days'
    AND provider_density_score IS NOT NULL
),
totals AS (SELECT COUNT(*) AS n FROM base),
thresholds(threshold) AS (
  VALUES (5), (8), (10), (15), (20), (25), (30), (40), (50)
)
SELECT
  t.threshold                                                                AS threshold,
  COUNT(*) FILTER (WHERE b.provider_density_score >= t.threshold)            AS pass_count,
  ROUND(100.0 * COUNT(*) FILTER (WHERE b.provider_density_score >= t.threshold)
              / NULLIF((SELECT n FROM totals), 0), 2)                        AS pass_pct_of_total
FROM thresholds t
CROSS JOIN base b
GROUP BY t.threshold
ORDER BY t.threshold;
