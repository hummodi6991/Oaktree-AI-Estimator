-- ============================================================================
-- qsr_demand_probe.sql   (READ-ONLY diagnostic — no DDL on app tables)
-- ----------------------------------------------------------------------------
-- Inspect the demand path for the MOST RECENT *QSR* expansion search.
--
-- Context (verified against the synced tree, 2026-06):
--   * The L1 demand-generator index is computed for EVERY service model whenever
--     EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED is on (prod=on). The compute block
--     is gated ONLY on the index flag, NOT on service_model
--     (app/services/expansion_advisor.py:9337). So `feature_snapshot_json ->
--     'demand_generator_index'` IS emitted for QSR candidates (computed-but-unused).
--   * The PR-2 scoring SWAP (pop_score -> dg_composite) is gated on
--     `EXPANSION_DEMAND_GENERATOR_SCORING_ENABLED and service_model == "dine_in"`
--     (expansion_advisor.py:9358-9361). QSR therefore falls through to the
--     pop_score blend, and `feature_snapshot_json ->> 'demand_score_source'`
--     reads 'pop_score' for QSR (the field is emitted whenever the scoring flag
--     is on — expansion_advisor.py:9485 — regardless of service_model).
--   * So for QSR we expect: dg_composite POPULATED, demand_score_source =
--     'pop_score' for every candidate (n_dg_index = 0). That is the headline the
--     summary tally below proves.
--
-- This mirrors the joins and JSON paths of l1_index_validation.sql exactly
-- (same expansion_search -> expansion_candidate join, same JSON traversal of
-- feature_snapshot_json -> 'demand_generator_index'); it only swaps the
-- service_model filter from 'dine_in' to 'qsr' and adds a derived
-- pop_score-equivalent so we can compare the two engines side by side.
--
-- HOW TO RUN (iPad/Safari friendly — psql -f safe, no \set, no heredocs):
--   psql "$DATABASE_URL" -f scripts/diagnostics/qsr_demand_probe.sql
-- ============================================================================

\timing on

-- Stage the most-recent QSR search's candidates with the index flattened.
-- NOTE on the LEFT-vs-INNER join to the index: l1_index_validation.sql filters
-- to rows WHERE the index IS NOT NULL (it only cares about dine-in candidates
-- that carry it). Here we KEEP every candidate of the latest QSR search even if
-- the index is absent — that way, if A2's expectation is ever wrong on a given
-- pod (index flag off, old snapshot, etc.), the probe still prints the demand
-- fields that ARE present instead of silently dropping the whole search.
DROP TABLE IF EXISTS qsr_probe;
CREATE TEMP TABLE qsr_probe AS
WITH latest AS (
    SELECT id, created_at
    FROM expansion_search
    WHERE service_model = 'qsr'
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT
    ec.parcel_id,
    ec.district,
    ec.final_score,
    ec.competitor_count,
    ec.whitespace_score                                                   AS competition_whitespace,
    (ec.feature_snapshot_json ->> 'demand_score_source')                  AS demand_score_source,
    -- L1 index composite + sub-components (present for QSR per A2; NULL only if
    -- the index flag is off on the serving pod or this is a pre-deploy snapshot).
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'composite_0_100')::numeric          AS dg_composite,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'radius_m')::int                     AS dg_radius_m,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'pop_radius_m')::int                 AS dg_pop_radius_m,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'weights_version')                   AS weights_version,
    -- population_reach: prefer the index's copy, fall back to the top-level
    -- snapshot key that actually feeds the pop_score blend (expansion_advisor.py:9947).
    COALESCE(
        (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'population_reach')::numeric,
        (ec.feature_snapshot_json ->> 'population_reach')::numeric
    )                                                                                              AS pop_reach,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'population_local_reach')::numeric   AS pop_local_reach,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'fnb_review_weighted_density')::numeric AS fnb_review_weighted,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'fnb_venue_count')::int              AS fnb_venue_count,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'building_floors_proxy_sum')::numeric AS building_floors_sum,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'offices')::int      AS osm_offices,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'malls_retail')::int AS osm_malls_retail,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'transit')::int      AS osm_transit,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'mosques')::int      AS osm_mosques,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'schools')::int      AS osm_schools,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'hospitals')::int    AS osm_hospitals,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'hotels')::int       AS osm_hotels,
    -- pop_score-EQUIVALENT: what the QSR demand numerator actually uses today.
    -- Mirrors _population_score(reach, service_model='qsr') =
    --   clamp( sqrt(reach / 80000) * 100 )   (ref = _POPULATION_SCORE_REFERENCE['qsr']
    --   = 80000, expansion_advisor.py:835; sqrt scaling at :2493).
    -- This is the leg dg_composite would REPLACE if the index were extended to QSR.
    LEAST(100.0, sqrt(GREATEST(
        COALESCE(
            (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'population_reach')::numeric,
            (ec.feature_snapshot_json ->> 'population_reach')::numeric,
            0
        ), 0) / 80000.0) * 100.0)                                                                  AS pop_score_equiv_qsr,
    -- Delivery-side inputs the blend's OTHER leg uses (for context; QSR _del_w = 0.40).
    (ec.feature_snapshot_json ->> 'realized_demand_30d')::numeric                                  AS realized_demand_30d,
    (ec.feature_snapshot_json ->> 'realized_demand_branches')::int                                 AS realized_demand_branches
FROM expansion_candidate ec
JOIN latest l ON ec.search_id = l.id;

-- Which search are we probing, and how many candidates carry the index?
SELECT
    (SELECT id FROM expansion_search WHERE service_model = 'qsr'
      ORDER BY created_at DESC LIMIT 1)                          AS latest_qsr_search_id,
    (SELECT created_at FROM expansion_search WHERE service_model = 'qsr'
      ORDER BY created_at DESC LIMIT 1)                          AS created_at,
    COUNT(*)                                                     AS n_candidates,
    COUNT(*) FILTER (WHERE dg_composite IS NOT NULL)            AS candidates_with_index,
    COUNT(DISTINCT district)                                     AS distinct_districts,
    MIN(dg_radius_m)                                             AS dg_radius_m,
    MIN(dg_pop_radius_m)                                         AS dg_pop_radius_m,
    MIN(weights_version)                                         AS weights_version
FROM qsr_probe;

-- ── HEADLINE: which numerator fed the QSR demand blend? ──
-- Expectation (A1/A3): n_dg_index = 0, n_pop_score = all (the PR-2 swap is
-- dine_in-only), n_source_null = 0 (scoring flag is on in prod, so the field is
-- emitted). If n_dg_index > 0 here, something scoped the swap to QSR — investigate.
SELECT
    count(*) FILTER (WHERE demand_score_source = 'dg_index')   AS n_dg_index,
    count(*) FILTER (WHERE demand_score_source = 'pop_score')  AS n_pop_score,
    count(*) FILTER (WHERE demand_score_source IS NULL)        AS n_source_null
FROM qsr_probe;

-- ── Is the (unused) composite even populated for QSR? (A2) ──
-- Confirms a QSR probe can read composites with NO code change. If have_composite
-- = 0 the index flag is off on this pod / snapshot is pre-deploy; fall back to the
-- pop_score-equivalent + delivery columns below for the demand read.
SELECT
    COUNT(*)                                              AS n,
    COUNT(*) FILTER (WHERE dg_composite        IS NOT NULL) AS have_composite,
    COUNT(*) FILTER (WHERE pop_reach           IS NOT NULL) AS have_population,
    COUNT(*) FILTER (WHERE pop_local_reach     IS NOT NULL) AS have_population_local,
    COUNT(*) FILTER (WHERE fnb_review_weighted IS NOT NULL) AS have_fnb,
    COUNT(*) FILTER (WHERE building_floors_sum IS NOT NULL) AS have_building_floors,
    COUNT(*) FILTER (WHERE osm_offices         IS NOT NULL) AS have_osm
FROM qsr_probe;

-- ── Does the composite carry distinct signal at QSR catchments? ──
-- The index inputs were gathered at the FLAT EXPANSION_DEMAND_GENERATOR_RADIUS_M
-- (3500 m demand / 1500 m pop) regardless of service_model — i.e. the dine-in
-- catchment, NOT QSR's _CATCHMENT_RADII_M['qsr'] (1500 m demand / 1200 m comp).
-- So dg_radius_m above should read 3500 even on a QSR search. Spread stats let us
-- judge whether the composite still discriminates on the (dine-in-shaped) inputs.
SELECT
    COUNT(*)                                         AS n,
    round(MIN(dg_composite), 2)                      AS min_composite,
    round(AVG(dg_composite), 2)                      AS avg_composite,
    round((percentile_cont(0.5) WITHIN GROUP (ORDER BY dg_composite))::numeric, 2) AS p50,
    round(MAX(dg_composite), 2)                      AS max_composite,
    round(STDDEV_POP(dg_composite), 2)               AS stddev,
    COUNT(*) FILTER (WHERE dg_composite > 0)          AS n_nonzero,
    COUNT(DISTINCT round(dg_composite, 1))            AS distinct_rounded_values
FROM qsr_probe;

-- ── Correlations (mirror of l1_index_validation.sql Criterion 3) ──
-- corr(composite, final_score): how much the composite WOULD move QSR ranking if
--   swapped in (today it does not feed scoring, so this is the "what-if").
-- corr(composite, competition_whitespace): independence from the competitor proxy.
-- corr(composite, pop_score_equiv_qsr): how much NEW signal the composite adds
--   over the population leg it would replace (low corr ⇒ genuine recalibration,
--   not a relabel).
SELECT
    round(corr(dg_composite, final_score::double precision)::numeric, 3)            AS corr_composite_vs_final_score,
    round(corr(dg_composite, competition_whitespace::double precision)::numeric, 3) AS corr_composite_vs_whitespace,
    round(corr(dg_composite, competitor_count::double precision)::numeric, 3)       AS corr_composite_vs_competitor_count,
    round(corr(dg_composite, pop_score_equiv_qsr::double precision)::numeric, 3)    AS corr_composite_vs_pop_score_equiv
FROM qsr_probe
WHERE dg_composite IS NOT NULL;

-- ── Eyeball: top candidate per district (geographic spread) ──
SELECT DISTINCT ON (district)
    district,
    parcel_id,
    demand_score_source,
    final_score,
    dg_composite,
    pop_score_equiv_qsr,
    competitor_count,
    competition_whitespace,
    pop_reach,
    pop_local_reach,
    fnb_review_weighted,
    fnb_venue_count,
    building_floors_sum,
    osm_offices, osm_malls_retail, osm_transit, osm_mosques,
    osm_schools, osm_hospitals, osm_hotels
FROM qsr_probe
ORDER BY district, final_score DESC NULLS LAST;

-- ── Eyeball: overall top 25 by final_score (full breakdown) ──
-- Ordered by final_score (not dg_composite) because for QSR the composite is NOT
-- what ranked these — final_score reflects the live pop_score engine. Both columns
-- are shown so the divergence between "what ranked" and "what the index would say"
-- is visible per row.
SELECT
    parcel_id,
    district,
    demand_score_source,
    final_score,
    dg_composite,
    pop_score_equiv_qsr,
    competitor_count,
    competition_whitespace,
    pop_reach,
    pop_local_reach,
    fnb_review_weighted,
    fnb_venue_count,
    building_floors_sum,
    realized_demand_30d,
    realized_demand_branches,
    osm_offices, osm_malls_retail, osm_transit, osm_mosques,
    osm_schools, osm_hospitals, osm_hotels
FROM qsr_probe
ORDER BY final_score DESC NULLS LAST
LIMIT 25;

DROP TABLE IF EXISTS qsr_probe;
