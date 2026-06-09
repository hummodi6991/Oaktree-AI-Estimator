-- ============================================================================
-- qsr_index_validation.sql
-- ----------------------------------------------------------------------------
-- QSR analogue of l1_index_validation.sql. Validate the l1_v3 QSR demand-
-- generator re-anchor AFTER a fresh QSR search was run with BOTH
--   EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED=true   (index computed) and
--   EXPANSION_DEMAND_GENERATOR_SCORING_QSR_ENABLED=true (index scored).
--
-- It reads expansion_candidate.feature_snapshot_json->'demand_generator_index'
-- for the MOST RECENT qsr search and surfaces the composite + every
-- sub-component alongside each candidate's district, competitor_count, and
-- whitespace_score, so we can confirm:
--
--   1. the index + all sub-components are populated for QSR candidates, the
--      weights_version is 'l1_v3_qsr_2026-06', and the radius_m is 1500 (QSR's
--      demand catchment — the whole point of Change-1; NOT 3500),
--   2. demand_score_source = 'dg_index' for QSR candidates with a composite
--      (n_dg_index ≈ all) — the proof QSR now scores off the index,
--   3. the composite SPREADS sensibly at 1500 m (NOT bunched near 0 — confirms
--      the l1_v3 anchors fit the 1500 m counts, vs reusing l1_v2 at 3500 m), and
--   4. corr(composite, final_score) jumps materially from its flag-off baseline
--      (analogous to dine-in's 0.06 → 0.51).
--
-- IMPORTANT: validation requires a FRESH qsr search after deploy with the flags
-- ON. Old searches won't backfill the snapshot, so their candidates have a NULL
-- demand_generator_index and are filtered out below.
--
-- HOW TO RUN (iPad/Safari friendly — psql -f safe, no \set, no heredocs):
--   psql "$DATABASE_URL" -f scripts/diagnostics/qsr_index_validation.sql
-- ============================================================================

\timing on

-- Stage the most-recent qsr search's candidates with the index flattened.
DROP TABLE IF EXISTS qsr_val;
CREATE TEMP TABLE qsr_val AS
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
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'composite_0_100')::numeric AS dg_composite,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'population_reach')::numeric AS pop_reach,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'population_local_reach')::numeric AS pop_local_reach,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'fnb_review_weighted_density')::numeric AS fnb_review_weighted,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'fnb_venue_count')::int      AS fnb_venue_count,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'building_floors_proxy_sum')::numeric AS building_floors_sum,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'offices')::int      AS osm_offices,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'malls_retail')::int AS osm_malls_retail,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'transit')::int      AS osm_transit,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'mosques')::int      AS osm_mosques,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'schools')::int      AS osm_schools,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'hospitals')::int    AS osm_hospitals,
    (ec.feature_snapshot_json -> 'demand_generator_index' -> 'osm_generators' ->> 'hotels')::int       AS osm_hotels,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'weights_version') AS weights_version,
    (ec.feature_snapshot_json -> 'demand_generator_index' ->> 'radius_m')::int   AS radius_m
FROM expansion_candidate ec
JOIN latest l ON ec.search_id = l.id
WHERE ec.feature_snapshot_json -> 'demand_generator_index' IS NOT NULL;

-- Which search are we validating, and how many candidates carry the index?
SELECT
    (SELECT id FROM expansion_search WHERE service_model = 'qsr'
      ORDER BY created_at DESC LIMIT 1)                       AS latest_qsr_search_id,
    (SELECT created_at FROM expansion_search WHERE service_model = 'qsr'
      ORDER BY created_at DESC LIMIT 1)                       AS created_at,
    COUNT(*)                                                  AS candidates_with_index,
    COUNT(DISTINCT district)                                  AS distinct_districts
FROM qsr_val;

-- ── Criterion 1: index populated, l1_v3 tag, and radius_m = 1500 (Change-1) ──
-- weights_version must be 'l1_v3_qsr_2026-06' and radius_m must be 1500 (QSR's
-- demand catchment). A radius_m of 3500 here means the enrich-radius fix (E.2)
-- did NOT land and the 1500 m anchors are being applied to 3500 m counts.
SELECT
    COUNT(*)                                              AS n,
    COUNT(*) FILTER (WHERE dg_composite        IS NOT NULL) AS have_composite,
    COUNT(*) FILTER (WHERE pop_reach           IS NOT NULL) AS have_population,
    COUNT(*) FILTER (WHERE fnb_review_weighted IS NOT NULL) AS have_fnb,
    COUNT(*) FILTER (WHERE building_floors_sum IS NOT NULL) AS have_building_floors,
    COUNT(*) FILTER (WHERE osm_offices         IS NOT NULL) AS have_osm,
    MIN(weights_version)                                  AS weights_version,
    MIN(radius_m)                                         AS radius_m,
    MAX(radius_m)                                         AS radius_m_max
FROM qsr_val;

-- ── Criterion 2: which numerator fed the QSR demand blend? ──
-- With the QSR scoring flag ON and a composite present, every QSR candidate
-- should score off 'dg_index'.
--   n_dg_index    -> scored off the demand-generator composite (expected: all)
--   n_pop_score   -> fell back to pop_score (flag off on serving pod, or no composite)
--   n_source_null -> emit not landing in the read snapshot (real wiring bug)
SELECT
    count(*) FILTER (WHERE demand_score_source = 'dg_index')   AS n_dg_index,
    count(*) FILTER (WHERE demand_score_source = 'pop_score')  AS n_pop_score,
    count(*) FILTER (WHERE demand_score_source IS NULL)        AS n_source_null
FROM qsr_val;

-- ── Criterion 3: the composite SPREADS at 1500 m (NOT bunched near 0) ──
-- The whole point of l1_v3 vs reusing l1_v2: at 1500 m the counts are smaller,
-- so l1_v2's 3500 m anchors would peg nearly every QSR candidate near 0. A
-- healthy spread here (wide min→max, many distinct values, p50 well above 0)
-- confirms the l1_v3 anchors fit the 1500 m distribution.
SELECT
    COUNT(*)                                         AS n,
    round(MIN(dg_composite), 2)                      AS min_composite,
    round(AVG(dg_composite), 2)                      AS avg_composite,
    round((percentile_cont(0.25) WITHIN GROUP (ORDER BY dg_composite))::numeric, 2) AS p25,
    round((percentile_cont(0.5)  WITHIN GROUP (ORDER BY dg_composite))::numeric, 2) AS p50,
    round((percentile_cont(0.75) WITHIN GROUP (ORDER BY dg_composite))::numeric, 2) AS p75,
    round(MAX(dg_composite), 2)                      AS max_composite,
    round(STDDEV_POP(dg_composite), 2)               AS stddev,
    COUNT(*) FILTER (WHERE dg_composite > 0)          AS n_nonzero,
    COUNT(*) FILTER (WHERE dg_composite < 1.0)        AS n_near_zero,
    COUNT(DISTINCT round(dg_composite, 1))            AS distinct_rounded_values
FROM qsr_val;

-- ── Criterion 4: corr(composite, final_score) (jumps from flag-off baseline) ──
-- Run this script ONCE with the QSR scoring flag OFF (baseline) and again with
-- it ON; corr_composite_vs_final_score should jump materially (analogous to
-- dine-in's 0.06 → 0.51) — the proof QSR now scores off the index. The
-- vs-competitor / vs-whitespace corrs show the index is not just a competitor
-- proxy (|corr| well below 1).
SELECT
    round(corr(dg_composite, competitor_count::double precision)::numeric, 3)        AS corr_composite_vs_competitor_count,
    round(corr(dg_composite, competition_whitespace::double precision)::numeric, 3)  AS corr_composite_vs_whitespace,
    round(corr(dg_composite, final_score::double precision)::numeric, 3)             AS corr_composite_vs_final_score
FROM qsr_val
WHERE dg_composite IS NOT NULL;

-- ── Eyeball: top candidate per district (geographic spread, 3+ districts) ──
SELECT DISTINCT ON (district)
    district,
    parcel_id,
    demand_score_source,
    final_score,
    dg_composite,
    competitor_count,
    competition_whitespace,
    pop_reach,
    pop_local_reach,
    fnb_review_weighted,
    fnb_venue_count,
    building_floors_sum,
    osm_offices, osm_malls_retail, osm_transit, osm_mosques,
    osm_schools, osm_hospitals, osm_hotels
FROM qsr_val
ORDER BY district, dg_composite DESC NULLS LAST;

-- ── Eyeball: overall top 25 by composite (full sub-component breakdown) ──
SELECT
    parcel_id,
    district,
    demand_score_source,
    final_score,
    dg_composite,
    competitor_count,
    competition_whitespace,
    pop_reach,
    pop_local_reach,
    fnb_review_weighted,
    fnb_venue_count,
    building_floors_sum,
    osm_offices, osm_malls_retail, osm_transit, osm_mosques,
    osm_schools, osm_hospitals, osm_hotels
FROM qsr_val
ORDER BY dg_composite DESC NULLS LAST
LIMIT 25;

DROP TABLE IF EXISTS qsr_val;
