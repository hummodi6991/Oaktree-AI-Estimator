-- ============================================================================
-- l1_index_validation.sql
-- ----------------------------------------------------------------------------
-- Validate the PR-1 L1 demand-generator index AFTER a fresh dine-in search was
-- run with EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED=true.
--
-- It reads expansion_candidate.feature_snapshot_json->'demand_generator_index'
-- for the MOST RECENT dine-in search and surfaces the composite + every
-- sub-component alongside each candidate's district, competitor_count, and
-- whitespace_score (the competition_whitespace input), so we can eyeball that:
--
--   1. the index + all sub-components are populated (non-null) for dine-in candidates,
--   2. the composite VARIES across candidates (not mostly-zero, not constant), and
--   3. the composite DIVERGES from competitor density — proving it carries
--      independent demand signal, not just a competitor proxy.
--
-- IMPORTANT: validation requires a FRESH dine-in search after deploy with the
-- flag ON. Old searches won't backfill the snapshot, so their candidates will
-- have a NULL demand_generator_index and are filtered out below.
--
-- HOW TO RUN (iPad/Safari friendly — psql -f safe, no \set, no heredocs):
--   psql "$DATABASE_URL" -f scripts/diagnostics/l1_index_validation.sql
-- ============================================================================

\timing on

-- Stage the most-recent dine-in search's candidates with the index flattened.
DROP TABLE IF EXISTS l1_val;
CREATE TEMP TABLE l1_val AS
WITH latest AS (
    SELECT id, created_at
    FROM expansion_search
    WHERE service_model = 'dine_in'
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
    (SELECT id FROM expansion_search WHERE service_model = 'dine_in'
      ORDER BY created_at DESC LIMIT 1)                       AS latest_dine_in_search_id,
    (SELECT created_at FROM expansion_search WHERE service_model = 'dine_in'
      ORDER BY created_at DESC LIMIT 1)                       AS created_at,
    COUNT(*)                                                  AS candidates_with_index,
    COUNT(DISTINCT district)                                  AS distinct_districts
FROM l1_val;

-- ── Criterion 1: index + all sub-components populated (non-null) ──
SELECT
    COUNT(*)                                              AS n,
    COUNT(*) FILTER (WHERE dg_composite        IS NOT NULL) AS have_composite,
    COUNT(*) FILTER (WHERE pop_reach           IS NOT NULL) AS have_population,
    COUNT(*) FILTER (WHERE fnb_review_weighted IS NOT NULL) AS have_fnb,
    COUNT(*) FILTER (WHERE building_floors_sum IS NOT NULL) AS have_building_floors,
    COUNT(*) FILTER (WHERE osm_offices         IS NOT NULL) AS have_osm,
    MIN(weights_version)                                  AS weights_version,
    MIN(radius_m)                                         AS radius_m
FROM l1_val;

-- ── Criterion 2: the composite VARIES (spread, not constant / not mostly-zero) ──
SELECT
    COUNT(*)                                         AS n,
    round(MIN(dg_composite), 2)                      AS min_composite,
    round(AVG(dg_composite), 2)                      AS avg_composite,
    round((percentile_cont(0.5) WITHIN GROUP (ORDER BY dg_composite))::numeric, 2) AS p50,
    round(MAX(dg_composite), 2)                      AS max_composite,
    round(STDDEV_POP(dg_composite), 2)              AS stddev,
    COUNT(*) FILTER (WHERE dg_composite > 0)         AS n_nonzero,
    COUNT(DISTINCT round(dg_composite, 1))           AS distinct_rounded_values
FROM l1_val;

-- ── Criterion 3: composite DIVERGES from competition (independent signal) ──
-- Pearson correlation of the composite vs competitor_count and vs the
-- competition_whitespace score. |corr| well below 1 ⇒ the index is not just a
-- competitor proxy. (whitespace is inverse to competitor density, so a positive
-- corr there is expected but should still be far from +1.)
SELECT
    round(corr(dg_composite, competitor_count::double precision)::numeric, 3)        AS corr_composite_vs_competitor_count,
    round(corr(dg_composite, competition_whitespace::double precision)::numeric, 3)  AS corr_composite_vs_whitespace,
    round(corr(dg_composite, final_score::double precision)::numeric, 3)             AS corr_composite_vs_final_score
FROM l1_val
WHERE dg_composite IS NOT NULL;

-- ── PR-2: which numerator fed the dine-in demand blend? ──
-- One-line tally of feature_snapshot_json->>'demand_score_source' across the
-- candidates of the latest dine-in search. With the PR-2 scoring flag ON and a
-- composite present, every dine-in candidate should score off 'dg_index'.
--   n_dg_index    -> scored off the demand-generator composite (expected: all)
--   n_pop_score   -> fell back to pop_score (flag off on serving pod, or no composite)
--   n_source_null -> emit not landing in the read snapshot (real wiring bug)
SELECT
    count(*) FILTER (WHERE demand_score_source = 'dg_index')   AS n_dg_index,
    count(*) FILTER (WHERE demand_score_source = 'pop_score')  AS n_pop_score,
    count(*) FILTER (WHERE demand_score_source IS NULL)        AS n_source_null
FROM l1_val;

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
    fnb_review_weighted,
    fnb_venue_count,
    building_floors_sum,
    osm_offices, osm_malls_retail, osm_transit, osm_mosques,
    osm_schools, osm_hospitals, osm_hotels
FROM l1_val
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
    fnb_review_weighted,
    fnb_venue_count,
    building_floors_sum,
    osm_offices, osm_malls_retail, osm_transit, osm_mosques,
    osm_schools, osm_hospitals, osm_hotels
FROM l1_val
ORDER BY dg_composite DESC NULLS LAST
LIMIT 25;

DROP TABLE IF EXISTS l1_val;
