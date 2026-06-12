-- ============================================================================
-- repost_duplicate_probe.sql  (Finding 7 — scoring/ranking audit 2026-06)
--
-- _dedupe_candidates short-circuits on parcel_id (expansion_advisor.py
-- L939-946): pid-bearing candidates never reach the spatial key, and every
-- production candidate carries one (cu.aqar_id AS parcel_id, L7108;
-- commercial_unit.aqar_id is the PK). A re-posted listing (same physical
-- unit, new aqar_id) is therefore only catchable by _dedupe_score_clones,
-- which requires EXACT estimated_rent_sar_m2_year equality and
-- |delta score| <= 0.3 (L993-999) — both defeated by the rent micro-location
-- multiplier and any score jitter.
--
-- Fingerprint: pairs of PERSISTED candidates in the same search within ~30 m
-- of each other with area within 5% but different parcel_id. These survived
-- both dedupe passes.
--
-- Run read-only against the production replica (Ahmed: via Codespace psql).
-- ============================================================================

-- Pairwise survivor duplicates within each recent search (last 30 days).
WITH cands AS (
    SELECT
        c.search_id,
        c.id,
        c.parcel_id,
        c.district,
        c.area_m2::float AS area_m2,
        c.final_score::float AS final_score,
        c.estimated_rent_sar_m2_year::float AS rent_m2_year,
        ST_SetSRID(ST_MakePoint(c.lon::float, c.lat::float), 4326)::geography AS g
    FROM expansion_candidate c
    JOIN expansion_search s ON s.id = c.search_id
    WHERE s.created_at >= now() - interval '30 days'
),
pairs AS (
    SELECT
        a.search_id,
        a.parcel_id  AS parcel_id_a,
        b.parcel_id  AS parcel_id_b,
        a.district,
        ROUND(ST_Distance(a.g, b.g)::numeric, 1) AS distance_m,
        a.area_m2    AS area_a,
        b.area_m2    AS area_b,
        a.rent_m2_year AS rent_a,
        b.rent_m2_year AS rent_b,
        a.final_score AS score_a,
        b.final_score AS score_b
    FROM cands a
    JOIN cands b
      ON b.search_id = a.search_id
     AND b.id > a.id                          -- each pair once
     AND b.parcel_id <> a.parcel_id           -- distinct listings only
     AND ST_DWithin(a.g, b.g, 30)             -- ~30 m
     AND a.area_m2 > 0
     AND abs(a.area_m2 - b.area_m2) / a.area_m2 <= 0.05  -- area within 5%
)
SELECT * FROM pairs
ORDER BY distance_m ASC
LIMIT 200;

-- Summary: searches affected and why _dedupe_score_clones missed each pair
-- (different rent after micro-adjustment, or score gap > 0.3).
WITH cands AS (
    SELECT
        c.search_id, c.id, c.parcel_id,
        c.area_m2::float AS area_m2,
        c.final_score::float AS final_score,
        c.estimated_rent_sar_m2_year::float AS rent_m2_year,
        ST_SetSRID(ST_MakePoint(c.lon::float, c.lat::float), 4326)::geography AS g
    FROM expansion_candidate c
    JOIN expansion_search s ON s.id = c.search_id
    WHERE s.created_at >= now() - interval '30 days'
),
pairs AS (
    SELECT
        a.search_id,
        (a.rent_m2_year IS DISTINCT FROM b.rent_m2_year) AS rent_differs,
        (abs(a.final_score - b.final_score) > 0.3)       AS score_gap_gt_0_3
    FROM cands a
    JOIN cands b
      ON b.search_id = a.search_id
     AND b.id > a.id
     AND b.parcel_id <> a.parcel_id
     AND ST_DWithin(a.g, b.g, 30)
     AND a.area_m2 > 0
     AND abs(a.area_m2 - b.area_m2) / a.area_m2 <= 0.05
)
SELECT
    COUNT(*) AS duplicate_pairs_surviving,
    COUNT(DISTINCT search_id) AS searches_affected,
    COUNT(*) FILTER (WHERE rent_differs) AS missed_due_to_rent_inequality,
    COUNT(*) FILTER (WHERE score_gap_gt_0_3) AS missed_due_to_score_gap,
    COUNT(*) FILTER (WHERE NOT rent_differs AND NOT score_gap_gt_0_3)
        AS unexplained_pairs
FROM pairs;
