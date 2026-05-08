-- =============================================================================
-- Expansion Advisor — Ranking Quality SQL Runbook
-- Date: 2026-05-08
-- Companion to: findings.md
--
-- HOW TO USE
-- ----------
-- 1. Open the production DB (Codespace) on the read replica.
-- 2. Run section A (parameters) once — set @search_id_a / @search_id_b /
--    @search_size_min. The runbook below uses psql-style parameters via
--    \set. If your client is not psql, replace the :param refs inline.
-- 3. Sections B (Line 2 — single-cohort discriminative power) and
--    C (Line 3 — counterfactual / cross-search) can be run independently.
-- 4. The final query (D — scorecard) is the one-row summary.
--
-- Every query has a -- WHY: header so the question it answers is explicit.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Section A — Parameters
-- -----------------------------------------------------------------------------
-- Pick the search to analyze. Default = the most recent search of size >= 25.
-- Override by uncommenting the explicit \set lines and pasting search ids.

\set search_size_min 25

-- Default: most recent meaningful search.
-- (If you know the search_id, replace this block with `\set search_id_a 'xxx'`.)
SELECT id AS recent_search_id, brand_name, category, created_at,
       (SELECT count(*) FROM expansion_candidate ec WHERE ec.search_id = es.id)
         AS cohort_size
FROM expansion_search es
WHERE (SELECT count(*) FROM expansion_candidate ec WHERE ec.search_id = es.id)
      >= :search_size_min
ORDER BY es.created_at DESC
LIMIT 5;

-- Pick the top one (or override). Replace 'PASTE_SEARCH_ID_HERE' before running.
\set search_id_a 'PASTE_SEARCH_ID_HERE'

-- For Line 3 (counterfactuals): a SECOND search to compare against.
--  - For Line 3 #1 (stability under same inputs): pick another recent search
--    with the SAME brand_name + same target_area_m2 + overlapping bbox.
--  - For Line 3 #2 (brand-vertical sensitivity): pick a recent search with a
--    DIFFERENT brand_name but matching area window.
\set search_id_b 'PASTE_SECOND_SEARCH_ID_HERE'


-- =============================================================================
-- Section B — Line 2: Cohort discriminative-power analysis
--
-- All queries in this section run against a single :search_id_a.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- B1. Final-score distribution
-- -----------------------------------------------------------------------------
-- WHY: Tells us whether final_score is a wide, useful axis or a narrow band.
--      If the p10..p90 spread is < 8 points, the score is barely separating
--      candidates and the ranking is loosely structured noise. If stddev > 5
--      and p90 - p10 > 15, the deterministic scorer has real headroom.
SELECT
    count(*)                                                   AS cohort_size,
    min(final_score)::numeric(6,2)                             AS min_score,
    max(final_score)::numeric(6,2)                             AS max_score,
    (max(final_score) - min(final_score))::numeric(6,2)        AS score_range,
    percentile_cont(0.10) WITHIN GROUP (ORDER BY final_score)::numeric(6,2) AS p10,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY final_score)::numeric(6,2) AS p25,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY final_score)::numeric(6,2) AS p50,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY final_score)::numeric(6,2) AS p75,
    percentile_cont(0.90) WITHIN GROUP (ORDER BY final_score)::numeric(6,2) AS p90,
    stddev_samp(final_score)::numeric(6,3)                     AS score_stddev
FROM expansion_candidate
WHERE search_id = :'search_id_a';


-- -----------------------------------------------------------------------------
-- B2. Tying / score uniqueness
-- -----------------------------------------------------------------------------
-- WHY: How many distinct final_score values exist vs total candidates? A high
--      degree of tying (distinct_pct < 60%) means the scorer is producing
--      collisions and the ranking is decided by tiebreakers / late passes
--      more than by the score itself.
WITH c AS (
    SELECT final_score
    FROM expansion_candidate
    WHERE search_id = :'search_id_a'
)
SELECT
    count(*)                                            AS cohort_size,
    count(DISTINCT final_score)                         AS distinct_scores,
    round(100.0 * count(DISTINCT final_score) / NULLIF(count(*), 0), 1)
                                                        AS distinct_scores_pct
FROM c;


-- -----------------------------------------------------------------------------
-- B3. Near-tie pair count
-- -----------------------------------------------------------------------------
-- WHY: How many candidate pairs are within < 1.0 final_score points of each
--      other? A high near-tie ratio means a 0.5-point noise perturbation
--      could flip a large fraction of the rank order — i.e. the ranking is
--      sensitive to score noise rather than driven by signal.
WITH c AS (
    SELECT id, final_score
    FROM expansion_candidate
    WHERE search_id = :'search_id_a'
)
SELECT
    count(*)                                            AS near_tie_pairs,
    (SELECT count(*) FROM c) * ((SELECT count(*) FROM c) - 1) / 2
                                                        AS total_pairs,
    round(
        100.0 * count(*)::numeric / NULLIF(
            (SELECT count(*) FROM c) * ((SELECT count(*) FROM c) - 1) / 2.0, 0),
        2)                                              AS near_tie_pct
FROM c c1
JOIN c c2
  ON c1.id < c2.id
 AND abs(c1.final_score - c2.final_score) < 1.0;


-- -----------------------------------------------------------------------------
-- B4. Per-component RAW input spread (10 components from inputs.*)
-- -----------------------------------------------------------------------------
-- WHY: How much does each raw 0-100 input vary across the cohort? A component
--      whose raw stddev is < 3 (range < 10) is effectively constant for this
--      search and contributes nothing to ranking regardless of its weight.
--
-- Reads score_breakdown_json -> 'inputs' -> '<component>'. Each input is the
-- 0-100 value before the weight multiplication.
WITH inp AS (
    SELECT
        (score_breakdown_json -> 'inputs' ->> 'occupancy_economics')::numeric    AS occupancy_economics,
        (score_breakdown_json -> 'inputs' ->> 'listing_quality')::numeric        AS listing_quality,
        (score_breakdown_json -> 'inputs' ->> 'brand_fit')::numeric              AS brand_fit,
        (score_breakdown_json -> 'inputs' ->> 'landlord_signal')::numeric        AS landlord_signal,
        (score_breakdown_json -> 'inputs' ->> 'competition_whitespace')::numeric AS competition_whitespace,
        (score_breakdown_json -> 'inputs' ->> 'chain_strength')::numeric         AS chain_strength,
        (score_breakdown_json -> 'inputs' ->> 'demand_potential')::numeric       AS demand_potential,
        (score_breakdown_json -> 'inputs' ->> 'access_visibility')::numeric      AS access_visibility,
        (score_breakdown_json -> 'inputs' ->> 'delivery_demand')::numeric        AS delivery_demand,
        (score_breakdown_json -> 'inputs' ->> 'confidence')::numeric             AS confidence
    FROM expansion_candidate
    WHERE search_id = :'search_id_a'
)
SELECT 'occupancy_economics'    AS component,
       min(occupancy_economics)::numeric(6,2)  AS min,
       max(occupancy_economics)::numeric(6,2)  AS max,
       (max(occupancy_economics) - min(occupancy_economics))::numeric(6,2) AS range,
       stddev_samp(occupancy_economics)::numeric(6,3) AS stddev
FROM inp UNION ALL
SELECT 'listing_quality',
       min(listing_quality)::numeric(6,2),
       max(listing_quality)::numeric(6,2),
       (max(listing_quality) - min(listing_quality))::numeric(6,2),
       stddev_samp(listing_quality)::numeric(6,3)
FROM inp UNION ALL
SELECT 'brand_fit',
       min(brand_fit)::numeric(6,2),
       max(brand_fit)::numeric(6,2),
       (max(brand_fit) - min(brand_fit))::numeric(6,2),
       stddev_samp(brand_fit)::numeric(6,3)
FROM inp UNION ALL
SELECT 'landlord_signal',
       min(landlord_signal)::numeric(6,2),
       max(landlord_signal)::numeric(6,2),
       (max(landlord_signal) - min(landlord_signal))::numeric(6,2),
       stddev_samp(landlord_signal)::numeric(6,3)
FROM inp UNION ALL
SELECT 'competition_whitespace',
       min(competition_whitespace)::numeric(6,2),
       max(competition_whitespace)::numeric(6,2),
       (max(competition_whitespace) - min(competition_whitespace))::numeric(6,2),
       stddev_samp(competition_whitespace)::numeric(6,3)
FROM inp UNION ALL
SELECT 'chain_strength',
       min(chain_strength)::numeric(6,2),
       max(chain_strength)::numeric(6,2),
       (max(chain_strength) - min(chain_strength))::numeric(6,2),
       stddev_samp(chain_strength)::numeric(6,3)
FROM inp UNION ALL
SELECT 'demand_potential',
       min(demand_potential)::numeric(6,2),
       max(demand_potential)::numeric(6,2),
       (max(demand_potential) - min(demand_potential))::numeric(6,2),
       stddev_samp(demand_potential)::numeric(6,3)
FROM inp UNION ALL
SELECT 'access_visibility',
       min(access_visibility)::numeric(6,2),
       max(access_visibility)::numeric(6,2),
       (max(access_visibility) - min(access_visibility))::numeric(6,2),
       stddev_samp(access_visibility)::numeric(6,3)
FROM inp UNION ALL
SELECT 'delivery_demand',
       min(delivery_demand)::numeric(6,2),
       max(delivery_demand)::numeric(6,2),
       (max(delivery_demand) - min(delivery_demand))::numeric(6,2),
       stddev_samp(delivery_demand)::numeric(6,3)
FROM inp UNION ALL
SELECT 'confidence',
       min(confidence)::numeric(6,2),
       max(confidence)::numeric(6,2),
       (max(confidence) - min(confidence))::numeric(6,2),
       stddev_samp(confidence)::numeric(6,3)
FROM inp
ORDER BY range DESC;


-- -----------------------------------------------------------------------------
-- B5. Per-component WEIGHTED contribution spread (10 components)
-- -----------------------------------------------------------------------------
-- WHY: Multiply each raw input by its weight and measure stddev/range across
--      the cohort. This is the empirical answer to "which components are
--      doing the discriminative work." A component whose weighted-stddev is
--      < 0.5 is dead weight. A component whose weighted-range is > 5 is a
--      live driver. The component with the largest weighted-stddev is the
--      single most discriminative signal in this cohort.
--
-- Reads score_breakdown_json -> 'weighted_components' -> '<component>'.
WITH wc AS (
    SELECT
        (score_breakdown_json -> 'weighted_components' ->> 'occupancy_economics')::numeric    AS occupancy_economics,
        (score_breakdown_json -> 'weighted_components' ->> 'listing_quality')::numeric        AS listing_quality,
        (score_breakdown_json -> 'weighted_components' ->> 'brand_fit')::numeric              AS brand_fit,
        (score_breakdown_json -> 'weighted_components' ->> 'landlord_signal')::numeric        AS landlord_signal,
        (score_breakdown_json -> 'weighted_components' ->> 'competition_whitespace')::numeric AS competition_whitespace,
        (score_breakdown_json -> 'weighted_components' ->> 'chain_strength')::numeric         AS chain_strength,
        (score_breakdown_json -> 'weighted_components' ->> 'demand_potential')::numeric       AS demand_potential,
        (score_breakdown_json -> 'weighted_components' ->> 'access_visibility')::numeric      AS access_visibility,
        (score_breakdown_json -> 'weighted_components' ->> 'delivery_demand')::numeric        AS delivery_demand,
        (score_breakdown_json -> 'weighted_components' ->> 'confidence')::numeric             AS confidence
    FROM expansion_candidate
    WHERE search_id = :'search_id_a'
)
SELECT 'occupancy_economics' AS component,
       min(occupancy_economics)::numeric(6,3) AS min,
       max(occupancy_economics)::numeric(6,3) AS max,
       (max(occupancy_economics) - min(occupancy_economics))::numeric(6,3) AS range,
       stddev_samp(occupancy_economics)::numeric(6,3) AS stddev
FROM wc UNION ALL
SELECT 'listing_quality',
       min(listing_quality)::numeric(6,3),
       max(listing_quality)::numeric(6,3),
       (max(listing_quality) - min(listing_quality))::numeric(6,3),
       stddev_samp(listing_quality)::numeric(6,3)
FROM wc UNION ALL
SELECT 'brand_fit',
       min(brand_fit)::numeric(6,3),
       max(brand_fit)::numeric(6,3),
       (max(brand_fit) - min(brand_fit))::numeric(6,3),
       stddev_samp(brand_fit)::numeric(6,3)
FROM wc UNION ALL
SELECT 'landlord_signal',
       min(landlord_signal)::numeric(6,3),
       max(landlord_signal)::numeric(6,3),
       (max(landlord_signal) - min(landlord_signal))::numeric(6,3),
       stddev_samp(landlord_signal)::numeric(6,3)
FROM wc UNION ALL
SELECT 'competition_whitespace',
       min(competition_whitespace)::numeric(6,3),
       max(competition_whitespace)::numeric(6,3),
       (max(competition_whitespace) - min(competition_whitespace))::numeric(6,3),
       stddev_samp(competition_whitespace)::numeric(6,3)
FROM wc UNION ALL
SELECT 'chain_strength',
       min(chain_strength)::numeric(6,3),
       max(chain_strength)::numeric(6,3),
       (max(chain_strength) - min(chain_strength))::numeric(6,3),
       stddev_samp(chain_strength)::numeric(6,3)
FROM wc UNION ALL
SELECT 'demand_potential',
       min(demand_potential)::numeric(6,3),
       max(demand_potential)::numeric(6,3),
       (max(demand_potential) - min(demand_potential))::numeric(6,3),
       stddev_samp(demand_potential)::numeric(6,3)
FROM wc UNION ALL
SELECT 'access_visibility',
       min(access_visibility)::numeric(6,3),
       max(access_visibility)::numeric(6,3),
       (max(access_visibility) - min(access_visibility))::numeric(6,3),
       stddev_samp(access_visibility)::numeric(6,3)
FROM wc UNION ALL
SELECT 'delivery_demand',
       min(delivery_demand)::numeric(6,3),
       max(delivery_demand)::numeric(6,3),
       (max(delivery_demand) - min(delivery_demand))::numeric(6,3),
       stddev_samp(delivery_demand)::numeric(6,3)
FROM wc UNION ALL
SELECT 'confidence',
       min(confidence)::numeric(6,3),
       max(confidence)::numeric(6,3),
       (max(confidence) - min(confidence))::numeric(6,3),
       stddev_samp(confidence)::numeric(6,3)
FROM wc
ORDER BY stddev DESC;


-- -----------------------------------------------------------------------------
-- B6. Most / least discriminative components
-- -----------------------------------------------------------------------------
-- WHY: One row each: which weighted component spread the most across the
--      cohort (true ranking driver) and which spread the least (dead weight).
--      Read these together with B5 to understand the shape of the score.
WITH wc AS (
    SELECT
        unnest(ARRAY[
            'occupancy_economics','listing_quality','brand_fit','landlord_signal',
            'competition_whitespace','chain_strength','demand_potential',
            'access_visibility','delivery_demand','confidence'
        ]) AS component,
        unnest(ARRAY[
            (score_breakdown_json -> 'weighted_components' ->> 'occupancy_economics')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'listing_quality')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'brand_fit')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'landlord_signal')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'competition_whitespace')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'chain_strength')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'demand_potential')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'access_visibility')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'delivery_demand')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'confidence')::numeric
        ]) AS w_value
    FROM expansion_candidate
    WHERE search_id = :'search_id_a'
)
SELECT component,
       stddev_samp(w_value)::numeric(6,3)         AS weighted_stddev,
       (max(w_value) - min(w_value))::numeric(6,3) AS weighted_range,
       CASE
         WHEN stddev_samp(w_value) IS NULL THEN 'singleton_cohort'
         WHEN stddev_samp(w_value) = (
             SELECT max(s) FROM (
                 SELECT stddev_samp(w_value) AS s FROM (SELECT component AS c, w_value FROM wc) x
                 GROUP BY c
             ) z
         ) THEN 'most_discriminative'
         WHEN stddev_samp(w_value) = (
             SELECT min(s) FROM (
                 SELECT stddev_samp(w_value) AS s FROM (SELECT component AS c, w_value FROM wc) x
                 GROUP BY c
             ) z
         ) THEN 'least_discriminative'
         ELSE 'mid'
       END AS role
FROM wc
GROUP BY component
ORDER BY weighted_stddev DESC NULLS LAST;


-- -----------------------------------------------------------------------------
-- B7. Top-15 rank-vs-final_score audit
-- -----------------------------------------------------------------------------
-- WHY: For the top 15 candidates, list rank_position alongside the
--      deterministic_rank, value_uprank/downrank deltas, and the
--      market_viability "demoted" flag. Any row where rank_position differs
--      from dense_rank() OVER (ORDER BY final_score DESC) WITHOUT a labelled
--      uprank / downrank / demote / rerank delta is a "hidden disagreement"
--      caused by dedupe / fuzzy tiebreak / district balance — log those
--      explicitly so the user can decide whether the residual is acceptable.
WITH ranked AS (
    SELECT
        rank_position,
        deterministic_rank,
        final_rank,
        rerank_delta,
        rerank_status,
        final_score,
        COALESCE(
            (score_breakdown_json -> 'value_pass' ->> 'value_uprank_delta')::int, 0
        ) AS value_uprank_delta,
        COALESCE(
            (score_breakdown_json -> 'value_pass' ->> 'value_downrank_delta')::int, 0
        ) AS value_downrank_delta,
        COALESCE(
            (score_breakdown_json -> 'market_viability_flag' ->> 'demoted')::boolean,
            false
        ) AS market_viability_demoted,
        (score_breakdown_json -> 'market_viability_flag' ->> 'reason') AS demote_reason,
        parcel_id,
        district,
        dense_rank() OVER (ORDER BY final_score DESC) AS rank_by_final_score
    FROM expansion_candidate
    WHERE search_id = :'search_id_a'
)
SELECT
    rank_position,
    rank_by_final_score,
    deterministic_rank,
    final_rank,
    rerank_delta,
    rerank_status,
    value_uprank_delta,
    value_downrank_delta,
    market_viability_demoted,
    demote_reason,
    final_score,
    parcel_id,
    district,
    CASE
      WHEN rank_position = rank_by_final_score THEN 'aligned'
      WHEN rerank_delta IS NOT NULL AND rerank_delta <> 0 THEN 'llm_rerank_move'
      WHEN value_uprank_delta > 0 THEN 'value_uprank'
      WHEN value_downrank_delta > 0 THEN 'value_downrank'
      WHEN market_viability_demoted THEN 'viability_demote'
      ELSE 'unlabelled_disagreement'  -- dedupe / fuzzy / district balance / unknown
    END AS divergence_class
FROM ranked
WHERE rank_position <= 15
ORDER BY rank_position;


-- =============================================================================
-- Section C — Line 3: Counterfactual / cross-search comparisons
--
-- All queries in this section join :search_id_a with :search_id_b.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- C1. Stability under rerun (Spearman over common parcel_ids)
-- -----------------------------------------------------------------------------
-- WHY: For two searches with the same brand_name + same area_window +
--      same target_area, restrict to candidates whose parcel_id appears in
--      BOTH cohorts and compute the Spearman rank correlation between their
--      rank_position values. ~1.0 means deterministic; < 0.7 means the
--      ranking is materially unstable across reruns even though the inputs
--      are nominally identical (likely from per-search percentile cutoffs in
--      the value_pass and market_viability_pass).
--
--      Spearman = Pearson on the ranks. We compute ranks WITHIN each
--      restricted cohort (i.e. dense_rank() over the intersection only) so
--      the correlation is not distorted by candidates present in only one
--      search.
WITH a AS (
    SELECT parcel_id, rank_position
    FROM expansion_candidate
    WHERE search_id = :'search_id_a'
), b AS (
    SELECT parcel_id, rank_position
    FROM expansion_candidate
    WHERE search_id = :'search_id_b'
), j AS (
    SELECT a.parcel_id,
           a.rank_position AS rank_a,
           b.rank_position AS rank_b
    FROM a JOIN b USING (parcel_id)
), r AS (
    SELECT
        parcel_id,
        rank() OVER (ORDER BY rank_a) AS r_a,
        rank() OVER (ORDER BY rank_b) AS r_b
    FROM j
)
SELECT
    (SELECT count(*) FROM j)        AS common_parcels,
    corr(r_a::numeric, r_b::numeric)::numeric(6,4) AS spearman_rho,
    CASE
      WHEN corr(r_a::numeric, r_b::numeric) >= 0.95 THEN 'fully_stable'
      WHEN corr(r_a::numeric, r_b::numeric) >= 0.70 THEN 'mostly_stable'
      ELSE 'materially_unstable'
    END                              AS verdict
FROM r;


-- -----------------------------------------------------------------------------
-- C2. Brand-vertical sensitivity (Spearman across two brands, same area)
-- -----------------------------------------------------------------------------
-- WHY: For two searches with DIFFERENT brand_name but same area window
--      (e.g. Burger vs Coffee), compute the Spearman rank correlation over
--      the intersection of common parcel_ids. If rho is near 1.0 the ranking
--      is brand-insensitive — the brand_profile inputs aren't actually
--      moving the order, and the model is just sorting parcels by
--      brand-agnostic signals (econ + listing_quality + spatial). A healthy
--      ranking shows rho meaningfully below 1.0 here.
--
--      Use this on :search_id_a vs :search_id_b after pointing them at two
--      different-brand searches in the same area.
WITH a AS (
    SELECT parcel_id, rank_position
    FROM expansion_candidate
    WHERE search_id = :'search_id_a'
), b AS (
    SELECT parcel_id, rank_position
    FROM expansion_candidate
    WHERE search_id = :'search_id_b'
), j AS (
    SELECT a.parcel_id,
           a.rank_position AS rank_a,
           b.rank_position AS rank_b
    FROM a JOIN b USING (parcel_id)
), r AS (
    SELECT
        parcel_id,
        rank() OVER (ORDER BY rank_a) AS r_a,
        rank() OVER (ORDER BY rank_b) AS r_b
    FROM j
)
SELECT
    (SELECT brand_name FROM expansion_search WHERE id = :'search_id_a') AS brand_a,
    (SELECT brand_name FROM expansion_search WHERE id = :'search_id_b') AS brand_b,
    (SELECT count(*) FROM j)        AS common_parcels,
    corr(r_a::numeric, r_b::numeric)::numeric(6,4) AS spearman_rho,
    CASE
      WHEN corr(r_a::numeric, r_b::numeric) >= 0.95
        THEN 'red_flag_brand_insensitive'
      WHEN corr(r_a::numeric, r_b::numeric) >= 0.70
        THEN 'weakly_brand_sensitive'
      ELSE 'brand_sensitive_ok'
    END                              AS verdict
FROM r;


-- -----------------------------------------------------------------------------
-- C3. Top-5 rows with a weak component (mask test)
-- -----------------------------------------------------------------------------
-- WHY: Identify candidates that ranked top-5 despite having at least one
--      raw component score < 50. These are cases where the weighted sum is
--      masking a real weakness. Lists the offending component(s).
WITH r AS (
    SELECT
        rank_position, parcel_id, district, final_score,
        score_breakdown_json -> 'inputs' AS inp
    FROM expansion_candidate
    WHERE search_id = :'search_id_a'
      AND rank_position <= 5
), kv AS (
    SELECT r.rank_position, r.parcel_id, r.district, r.final_score,
           k AS component, v::numeric AS raw_score
    FROM r,
         LATERAL jsonb_each_text(inp) AS j(k, v)
    WHERE k IN (
        'occupancy_economics','listing_quality','brand_fit','landlord_signal',
        'competition_whitespace','chain_strength','demand_potential',
        'access_visibility','delivery_demand','confidence'
    )
      AND v ~ '^-?[0-9]+(\.[0-9]+)?$'
)
SELECT rank_position, parcel_id, district, final_score, component, raw_score
FROM kv
WHERE raw_score < 50
ORDER BY rank_position, raw_score;


-- -----------------------------------------------------------------------------
-- C4. Outside-top-10 rows that are uniformly strong (suppression test)
-- -----------------------------------------------------------------------------
-- WHY: Candidates that ranked outside the top 10 but had every component
--      score >= 70. These are cases where the model is suppressing
--      genuinely strong candidates — usually via the value_pass downrank,
--      the market_viability demote, or the unlabelled dedupe / district
--      balance reordering. Surface them so the user can decide whether the
--      suppression mechanism is the right call.
WITH r AS (
    SELECT
        rank_position, parcel_id, district, final_score,
        score_breakdown_json -> 'inputs' AS inp,
        COALESCE(
            (score_breakdown_json -> 'value_pass' ->> 'value_downrank_delta')::int, 0
        ) AS value_downrank_delta,
        COALESCE(
            (score_breakdown_json -> 'market_viability_flag' ->> 'demoted')::boolean,
            false
        ) AS viability_demoted,
        (score_breakdown_json -> 'market_viability_flag' ->> 'reason') AS demote_reason
    FROM expansion_candidate
    WHERE search_id = :'search_id_a'
      AND rank_position > 10
), strong AS (
    SELECT *
    FROM r
    WHERE (inp ->> 'occupancy_economics')::numeric    >= 70
      AND (inp ->> 'listing_quality')::numeric        >= 70
      AND (inp ->> 'brand_fit')::numeric              >= 70
      AND (inp ->> 'landlord_signal')::numeric        >= 70
      AND (inp ->> 'competition_whitespace')::numeric >= 70
      AND (inp ->> 'chain_strength')::numeric         >= 70
      AND (inp ->> 'demand_potential')::numeric       >= 70
      AND (inp ->> 'access_visibility')::numeric      >= 70
      AND (inp ->> 'delivery_demand')::numeric        >= 70
      AND (inp ->> 'confidence')::numeric             >= 70
)
SELECT rank_position, parcel_id, district, final_score,
       value_downrank_delta, viability_demoted, demote_reason,
       inp -> 'occupancy_economics' AS occ_econ,
       inp -> 'listing_quality'     AS list_qual,
       inp -> 'brand_fit'           AS brand_fit,
       inp -> 'landlord_signal'     AS landlord,
       inp -> 'competition_whitespace' AS whitespace,
       inp -> 'chain_strength'      AS chain,
       inp -> 'demand_potential'    AS demand,
       inp -> 'access_visibility'   AS access,
       inp -> 'delivery_demand'     AS delivery,
       inp -> 'confidence'          AS confidence
FROM strong
ORDER BY rank_position;


-- =============================================================================
-- Section D — Ranking quality scorecard (one row)
--
-- Run this against :search_id_a only. It folds the most important signals
-- from sections B and C into a single row.
-- =============================================================================

WITH cohort AS (
    SELECT *
    FROM expansion_candidate
    WHERE search_id = :'search_id_a'
), final_score_stats AS (
    SELECT
        count(*)                                        AS cohort_size,
        (max(final_score) - min(final_score))::numeric(6,2) AS score_range,
        round(100.0 * count(DISTINCT final_score) / NULLIF(count(*), 0), 1)
                                                        AS distinct_scores_pct
    FROM cohort
), near_ties AS (
    SELECT count(*) AS near_tie_pairs
    FROM cohort c1 JOIN cohort c2
      ON c1.id < c2.id
     AND abs(c1.final_score - c2.final_score) < 1.0
), wc AS (
    SELECT
        unnest(ARRAY[
            'occupancy_economics','listing_quality','brand_fit','landlord_signal',
            'competition_whitespace','chain_strength','demand_potential',
            'access_visibility','delivery_demand','confidence'
        ]) AS component,
        unnest(ARRAY[
            (score_breakdown_json -> 'weighted_components' ->> 'occupancy_economics')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'listing_quality')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'brand_fit')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'landlord_signal')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'competition_whitespace')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'chain_strength')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'demand_potential')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'access_visibility')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'delivery_demand')::numeric,
            (score_breakdown_json -> 'weighted_components' ->> 'confidence')::numeric
        ]) AS w_value
    FROM cohort
), wc_stats AS (
    SELECT component, stddev_samp(w_value) AS s
    FROM wc GROUP BY component
), top3 AS (
    SELECT string_agg(component, ',' ORDER BY s DESC) AS comps
    FROM (SELECT component, s FROM wc_stats ORDER BY s DESC NULLS LAST LIMIT 3) z
), bottom3 AS (
    SELECT string_agg(component, ',' ORDER BY s ASC) AS comps
    FROM (SELECT component, s FROM wc_stats ORDER BY s ASC NULLS LAST LIMIT 3) z
), rerank_disagreement AS (
    -- Rows whose rank_position deviates from rank-by-final_score for any
    -- reason: LLM rerank, value_pass, viability demote, or unlabelled
    -- (dedupe / fuzzy / district balance). Counts every such row once.
    SELECT count(*) AS n
    FROM (
        SELECT rank_position,
               dense_rank() OVER (ORDER BY final_score DESC) AS rank_by_score
        FROM cohort
    ) z
    WHERE rank_position <> rank_by_score
)
SELECT
    fs.cohort_size,
    fs.score_range,
    fs.distinct_scores_pct,
    round(
        100.0 * nt.near_tie_pairs::numeric
              / NULLIF(fs.cohort_size * (fs.cohort_size - 1) / 2.0, 0),
        2)                                  AS near_tie_pct,
    t3.comps                                AS top3_dominant_components,
    b3.comps                                AS weakest_dominant_components,
    rd.n                                    AS rerank_disagreement_count
FROM final_score_stats fs
CROSS JOIN near_ties nt
CROSS JOIN top3 t3
CROSS JOIN bottom3 b3
CROSS JOIN rerank_disagreement rd;
