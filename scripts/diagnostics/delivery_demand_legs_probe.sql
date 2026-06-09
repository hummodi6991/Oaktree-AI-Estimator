-- ============================================================================
-- delivery_demand_legs_probe.sql  (Weight audit Item 2 — supply-as-demand)
-- ----------------------------------------------------------------------------
-- _delivery_score (app/services/expansion_advisor.py:2610-2650) blends two
-- legs when realized demand is available:
--   listing_leg  = clamp(sqrt(delivery_listing_count / 40) * 100)   (:2634-2638)
--   realized_leg = clamp(sqrt(realized_demand / 263) * 100)         (:2645-2648,
--                  REF = EXPANSION_REALIZED_DEMAND_REFERENCE, config.py:118-120)
--   score = listing_leg*(1-w) + realized_leg*w,
--   w = EXPANSION_REALIZED_DEMAND_BLEND (default 0.5, config.py:107-109).
-- The realized leg is only active when the snapshot found >= 3 contributing
-- branches with >= 2 rating_count snapshots in the window (:8238-8239,
-- :7886-7900); otherwise the score is the listing leg alone (:2639-2640).
--
-- Hypothesis under test: listing_leg is competitor SUPPLY (delivery branches
-- in radius), so half (or all, when realized is missing) of the delivery term
-- inside demand_potential rewards saturation and partially cancels
-- competition_whitespace.
--
-- This probe runs over the last 30 days of persisted expansion_candidate rows
-- (joined to expansion_search for service_model) and reports:
--   1) realized-demand coverage by service_model,
--   2) per-candidate leg scores recomputed exactly per the code formulas,
--      their correlation, and each leg's correlation vs whitespace /
--      competitor_count / provider_whitespace / delivery_competition,
--   3) the realized_demand distribution vs the REF=263 anchor,
--   4) what a 0.5 -> 0.7 blend shift changes per candidate
--      (delta = 0.2 * (realized_leg - listing_leg), only where realized is
--      active).
--
-- Snapshot field notes:
--   feature_snapshot_json.realized_demand_30d / realized_demand_branches are
--   only written when branches >= 3 (expansion_advisor.py:9829-9834);
--   context_sources.realized_demand_source distinguishes
--   'expansion_delivery_rating_history' (active) from 'insufficient_history'
--   / 'history_unavailable' (:9835-9841).
--   delivery_listing_count is the persisted column (and snapshot field,
--   :10115-10116): the same-category delivery count over the bulk-enrichment
--   1200 m radius (:7812-7818, applied :8230-8233).
--
-- HOW TO RUN (iPad/Safari friendly — single line, no heredocs):
--   psql -x -f scripts/diagnostics/delivery_demand_legs_probe.sql > /tmp/dd_legs.txt 2>&1
-- ============================================================================

\timing on

DROP TABLE IF EXISTS dd_legs;
CREATE TEMP TABLE dd_legs AS
SELECT
    ec.id,
    es.service_model,
    lower(es.category)                            AS category,
    es.created_at,
    COALESCE(ec.delivery_listing_count, 0)::int   AS dlc,
    (ec.feature_snapshot_json ->> 'realized_demand_30d')::double precision AS rd,
    COALESCE((ec.feature_snapshot_json ->> 'realized_demand_branches')::int, 0) AS rd_branches,
    ec.feature_snapshot_json -> 'context_sources' ->> 'realized_demand_source' AS rd_source,
    ec.whitespace_score::double precision          AS whitespace_score,
    COALESCE(ec.competitor_count, 0)::int          AS competitor_count,
    ec.provider_whitespace_score::double precision AS provider_whitespace_score,
    ec.delivery_competition_score::double precision AS delivery_competition_score,
    ec.demand_score::double precision              AS demand_score
FROM expansion_candidate ec
JOIN expansion_search es ON es.id = ec.search_id
WHERE es.created_at >= now() - interval '30 days';

SELECT COUNT(*) AS candidates_30d FROM dd_legs;

-- Exact leg recomputation per the code formulas.
DROP TABLE IF EXISTS dd_scored;
CREATE TEMP TABLE dd_scored AS
SELECT
    *,
    CASE WHEN dlc <= 0 THEN 0.0
         ELSE LEAST(100.0, sqrt(dlc / 40.0) * 100.0)
    END AS listing_leg,
    CASE WHEN rd IS NOT NULL AND rd > 0 AND rd_branches >= 3
         THEN LEAST(100.0, sqrt(rd / 263.0) * 100.0)
    END AS realized_leg
FROM dd_legs;

-- ── Output 1: realized-demand coverage by service_model ──
SELECT
    service_model,
    COUNT(*) AS n,
    COUNT(*) FILTER (WHERE rd IS NOT NULL) AS n_rd_emitted,
    COUNT(*) FILTER (WHERE realized_leg IS NOT NULL) AS n_realized_active,
    round(100.0 * COUNT(*) FILTER (WHERE realized_leg IS NOT NULL) / COUNT(*), 1)
        AS pct_realized_active,
    COUNT(*) FILTER (WHERE rd_source = 'insufficient_history') AS n_insufficient,
    COUNT(*) FILTER (WHERE rd_source = 'history_unavailable')  AS n_flag_off
FROM dd_scored
GROUP BY service_model
ORDER BY service_model;

-- ── Output 2: leg distributions by service_model ──
SELECT
    service_model,
    round(AVG(listing_leg)::numeric, 1)  AS listing_mean,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY listing_leg)::numeric, 1) AS listing_p50,
    round(AVG(realized_leg)::numeric, 1) AS realized_mean,
    -- NULL realized_leg rows are ignored by AVG / percentile_cont.
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY realized_leg)::numeric, 1)
        AS realized_p50,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY dlc)::numeric, 1) AS dlc_p50,
    round(percentile_cont(0.90) WITHIN GROUP (ORDER BY dlc)::numeric, 1) AS dlc_p90,
    round(100.0 * COUNT(*) FILTER (WHERE dlc >= 40) / COUNT(*), 1) AS pct_listing_saturated
FROM dd_scored
GROUP BY service_model
ORDER BY service_model;

-- ── Output 3: correlations (rows where realized leg is active) ──
SELECT
    service_model,
    COUNT(*) AS n_active,
    round(corr(listing_leg, realized_leg)::numeric, 3)        AS corr_listing_realized,
    round(corr(listing_leg, whitespace_score)::numeric, 3)    AS corr_listing_whitespace,
    round(corr(listing_leg, competitor_count)::numeric, 3)    AS corr_listing_compcount,
    round(corr(realized_leg, whitespace_score)::numeric, 3)   AS corr_realized_whitespace,
    round(corr(realized_leg, competitor_count)::numeric, 3)   AS corr_realized_compcount,
    round(corr(listing_leg, provider_whitespace_score)::numeric, 3)  AS corr_listing_provws,
    round(corr(listing_leg, delivery_competition_score)::numeric, 3) AS corr_listing_delcomp
FROM dd_scored
WHERE realized_leg IS NOT NULL
GROUP BY service_model
ORDER BY service_model;

-- Same correlations over ALL rows (listing leg vs the competition stack),
-- since the listing leg drives the whole delivery term when realized is off.
SELECT
    'ALL_ROWS' AS scope,
    COUNT(*) AS n,
    round(corr(listing_leg, whitespace_score)::numeric, 3)    AS corr_listing_whitespace,
    round(corr(listing_leg, competitor_count)::numeric, 3)    AS corr_listing_compcount,
    round(corr(listing_leg, provider_whitespace_score)::numeric, 3)  AS corr_listing_provws,
    round(corr(listing_leg, delivery_competition_score)::numeric, 3) AS corr_listing_delcomp
FROM dd_scored;

-- ── Output 4: realized_demand distribution vs the REF=263 anchor ──
-- The anchor was calibrated as a p75 (scripts/diagnostics/
-- realized_demand_calibration.sql); check whether p75 still sits near 263.
SELECT
    service_model,
    COUNT(*) AS n_active,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY rd)::numeric, 1) AS rd_p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY rd)::numeric, 1) AS rd_p50,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY rd)::numeric, 1) AS rd_p75,
    round(percentile_cont(0.90) WITHIN GROUP (ORDER BY rd)::numeric, 1) AS rd_p90,
    round(MAX(rd)::numeric, 1) AS rd_max,
    round(100.0 * COUNT(*) FILTER (WHERE rd >= 263.0) / COUNT(*), 1) AS pct_at_or_over_ref
FROM dd_scored
WHERE realized_leg IS NOT NULL
GROUP BY service_model
ORDER BY service_model;

-- ── Output 5: blend-shift impact 0.5 -> 0.7 (realized-active rows only) ──
-- delivery_score(w) = listing*(1-w) + realized*w, so
-- delta(0.5 -> 0.7) = 0.2 * (realized_leg - listing_leg).
-- The final-score impact is delta * del_w * demand_potential_weight, where
-- del_w = _demand_blend_weights(service_model)[1] (expansion_advisor.py:
-- 2661-2666: delivery_first 0.60, qsr 0.40, cafe 0.45, dine_in 0.25) and
-- demand_potential weight = 8.7640% (:3457).
SELECT
    service_model,
    COUNT(*) AS n_active,
    round(AVG(realized_leg - listing_leg)::numeric, 1) AS leg_gap_mean,
    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY realized_leg - listing_leg)::numeric, 1) AS leg_gap_p25,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY realized_leg - listing_leg)::numeric, 1) AS leg_gap_p50,
    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY realized_leg - listing_leg)::numeric, 1) AS leg_gap_p75,
    round(AVG(0.2 * (realized_leg - listing_leg))::numeric, 2) AS delivery_score_delta_mean,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY 0.2 * (realized_leg - listing_leg))::numeric, 2)
        AS delivery_score_delta_p50
FROM dd_scored
WHERE realized_leg IS NOT NULL
GROUP BY service_model
ORDER BY service_model;

DROP TABLE IF EXISTS dd_scored;
DROP TABLE IF EXISTS dd_legs;
