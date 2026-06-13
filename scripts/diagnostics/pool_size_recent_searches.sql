-- F6 — Scoring-pool size probe for the _percentile_rent_burden N+1
-- ============================================================================
-- The first scoring pass (app/services/expansion_advisor.py, `for row in rows:`
-- at ~line 8750) calls _economics_score(db=db, is_listing=...) for the FULL
-- candidate pool BEFORE shortlisting (shortlist_size set at ~line 9193). For
-- listing rows (source_type='commercial_unit'), _economics_score ->
-- _percentile_rent_burden runs up to 5 aggregate queries (chain rungs:
-- district_band_type, district_type, district, city_band_type, city) over the
-- envelope-filtered commercial_unit set, with NO per-search caching.
--
-- The TRUE first-pass pool size (`len(rows)`) is NOT persisted to
-- expansion_search. Read it from pod logs:
--     "expansion_search timing: candidate_query=...s search_id=... rows=N"
-- and the pool-cap context from:
--     "expansion_advisor.candidate_pool_cap search_id=... candidate_pool_limit=..."
-- For per-search latency, pull p50/p95 from:
--     "expansion_report timing: total=...s ..."  /  "expansion_memo timing: ..."
--
-- This probe gives the persisted candidate counts per recent search (a floor
-- on the pool, since the pool is capped/limited before persistence) plus the
-- global Tier-1 (Aqar) upper bound and the comparable population each
-- _percentile_rent_burden aggregate scans.
--
-- Run:  psql -x -f scripts/diagnostics/pool_size_recent_searches.sql > /tmp/out.txt 2>&1
-- ============================================================================

-- §1 — last 20 searches: persisted candidate counts (post-cap/post-limit), and
-- how many are listing rows (the rows that trigger the rent-burden N+1).
SELECT
    s.id                                                            AS search_id,
    s.created_at,
    s.service_model,
    s.category,
    COUNT(c.id)                                                     AS persisted_candidates,
    COUNT(c.id) FILTER (WHERE c.source_type = 'commercial_unit')    AS listing_candidates
FROM expansion_search s
LEFT JOIN expansion_candidate c ON c.search_id = s.id
GROUP BY s.id, s.created_at, s.service_model, s.category
ORDER BY s.created_at DESC
LIMIT 20;

-- §2 — global upper bound: active restaurant-suitable Tier-1 (Aqar) listings.
-- This is the maximum number of listing rows that can enter the first-pass
-- loop; each can fire up to 5 _percentile_rent_burden aggregates.
SELECT
    COUNT(*)                                                                AS active_restaurant_suitable_listings,
    COUNT(*) FILTER (WHERE aqar_created_at IS NOT NULL OR first_seen_at IS NOT NULL) AS with_age_basis
FROM commercial_unit
WHERE restaurant_suitable = true
  AND status = 'active';

-- §3 — the comparable population each _percentile_rent_burden aggregate scans
-- (envelope filter mirrors the function's base_where: MIN=15, MAX=350 SAR/m2/mo,
-- MAX_AREA=1000 m2, excluded property types warehouse/building/land/rest_house/farm).
-- One scan per chain rung; up to 5 rungs per listing, uncached.
SELECT
    COUNT(*) AS rent_comparable_population
FROM commercial_unit
WHERE restaurant_suitable = true
  AND price_sar_annual IS NOT NULL AND price_sar_annual > 0
  AND area_sqm IS NOT NULL AND area_sqm > 0
  AND status = 'active'
  AND (price_sar_annual / area_sqm / 12.0) BETWEEN 15 AND 350
  AND area_sqm <= 1000
  AND (property_type IS NULL
       OR lower(property_type) NOT IN ('warehouse', 'building', 'land', 'rest_house', 'farm'));

-- §4 — distinct comparable CELLS actually exercised (neighborhood x area-band x
-- listing_type). A per-search cache keyed on (label, neighborhood, band_lo,
-- band_hi, ltype) would collapse repeated listings sharing a cell to ONE scan;
-- this counts how much sharing exists in the live data.
SELECT
    COUNT(*)                                              AS n_listings_in_envelope,
    COUNT(DISTINCT (lower(neighborhood), listing_type))   AS distinct_neighborhood_type_cells
FROM commercial_unit
WHERE restaurant_suitable = true
  AND price_sar_annual IS NOT NULL AND price_sar_annual > 0
  AND area_sqm IS NOT NULL AND area_sqm > 0
  AND status = 'active'
  AND (price_sar_annual / area_sqm / 12.0) BETWEEN 15 AND 350
  AND area_sqm <= 1000
  AND (property_type IS NULL
       OR lower(property_type) NOT IN ('warehouse', 'building', 'land', 'rest_house', 'farm'));
