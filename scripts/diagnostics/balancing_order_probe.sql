-- ============================================================================
-- balancing_order_probe.sql  (Finding 1 — scoring/ranking audit 2026-06)
--
-- Pipeline order at HEAD (app/services/expansion_advisor.py):
--   _rank_sort_key sort (L10865) -> _dedupe_candidates (L10867)
--   -> _dedupe_score_clones (L10870)
--   -> district balancing, truncates to ~limit (L10881-10908)
--   -> _apply_market_viability_pass hard-floor DROPS, no backfill (L10918)
--   -> _apply_score_deltas_and_sort (L10924) -> candidates[:limit] (L10926)
--
-- Because the hard floors run AFTER the balancing truncation, any candidate
-- dropped by a floor leaves an unfilled slot: failures never appear in
-- persisted rows (they are dropped, not flagged), so the production
-- fingerprint of this ordering bug is an UNDER-FILLED result set on
-- multi-district searches, optionally combined with target districts
-- losing representation.
--
-- Run read-only against the production replica (Ahmed: via Codespace psql).
-- Do NOT run from CI or the app pods.
-- ============================================================================

-- Per multi-district search (last 30 days): persisted count vs requested
-- limit, district representation, and floor-gate failures among persisted
-- rows (expected 0 — failures are dropped before persistence).
WITH multi_district_searches AS (
    SELECT
        s.id AS search_id,
        s.created_at,
        s.brand_name,
        jsonb_array_length(s.target_districts) AS n_target_districts,
        COALESCE((s.request_json ->> 'limit')::int, 15) AS requested_limit
    FROM expansion_search s
    WHERE s.created_at >= now() - interval '30 days'
      AND s.target_districts IS NOT NULL
      AND jsonb_typeof(s.target_districts) = 'array'
      AND jsonb_array_length(s.target_districts) >= 2
),
per_search AS (
    SELECT
        m.search_id,
        m.created_at,
        m.brand_name,
        m.n_target_districts,
        m.requested_limit,
        COUNT(c.id) AS persisted_candidates,
        COUNT(DISTINCT c.district) AS distinct_districts_persisted,
        -- Floor-gate failures among PERSISTED rows. Should always be 0:
        -- _apply_market_viability_pass drops these with `continue` before
        -- survivors.append (expansion_advisor.py L5903-5912).
        COUNT(*) FILTER (
            WHERE (c.gate_status_json ->> 'population_floor_pass') = 'false'
        ) AS persisted_population_floor_failures,
        COUNT(*) FILTER (
            WHERE (c.gate_status_json ->> 'commercial_floor_pass') = 'false'
        ) AS persisted_commercial_floor_failures,
        COUNT(*) FILTER (
            WHERE (c.gate_status_json ->> 'construction_proximity_pass') = 'false'
        ) AS persisted_construction_floor_failures
    FROM multi_district_searches m
    LEFT JOIN expansion_candidate c ON c.search_id = m.search_id
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    search_id,
    created_at,
    brand_name,
    n_target_districts,
    requested_limit,
    persisted_candidates,
    (persisted_candidates < requested_limit) AS underfilled,
    requested_limit - persisted_candidates AS missing_slots,
    distinct_districts_persisted,
    (distinct_districts_persisted < n_target_districts) AS districts_lost,
    persisted_population_floor_failures,
    persisted_commercial_floor_failures,
    persisted_construction_floor_failures
FROM per_search
ORDER BY underfilled DESC, missing_slots DESC, created_at DESC;

-- Summary: how prevalent is the under-fill fingerprint on multi-district
-- searches vs city-wide searches over the same window?
WITH searches AS (
    SELECT
        s.id AS search_id,
        COALESCE((s.request_json ->> 'limit')::int, 15) AS requested_limit,
        (s.target_districts IS NOT NULL
         AND jsonb_typeof(s.target_districts) = 'array'
         AND jsonb_array_length(s.target_districts) >= 2) AS is_multi_district,
        COUNT(c.id) AS persisted_candidates
    FROM expansion_search s
    LEFT JOIN expansion_candidate c ON c.search_id = s.id
    WHERE s.created_at >= now() - interval '30 days'
    GROUP BY 1, 2, 3
)
SELECT
    is_multi_district,
    COUNT(*) AS searches,
    COUNT(*) FILTER (WHERE persisted_candidates < requested_limit) AS underfilled_searches,
    ROUND(AVG(requested_limit - persisted_candidates), 2) AS avg_missing_slots,
    MAX(requested_limit - persisted_candidates) AS max_missing_slots
FROM searches
GROUP BY 1
ORDER BY 1 DESC;
