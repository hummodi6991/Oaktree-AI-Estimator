-- F2 — Cross-source competitor double-count probe
-- ============================================================================
-- Quantifies the overlap between the two sources UNION ALL'd (no cross-source
-- dedupe) inside _bulk_enrich_competitors (app/services/expansion_advisor.py
-- ~line 7498-7538):
--   Source 1: restaurant_poi      (Google Places)
--   Source 2: delivery_source_record (HungerStation / delivery marketplaces)
-- A venue listed on BOTH Google and a delivery platform is counted twice in
-- competitor_count, inflating the same-category competition signal that feeds
-- provider_whitespace_score / delivery_competition_score and the
-- _WHITESPACE_LOG_REF anchors (dine_in/delivery_first 50, qsr 75).
--
-- Measures, per recent qsr/burger candidate centroid, inside the qsr/burger
-- competition radius (1000 m, _catchment_radii("qsr")["competition"]):
--   (a) same-category restaurant_poi count
--   (b) same-category delivery_source_record count
--   (c) estimated cross-source overlap via:
--         - normalized-name match (inline mirror of _CHAIN_NAME_NORM_SQL), and
--         - 75 m spatial proximity between cross-source same-category rows
--   Reports the overlap-share distribution (p50/p75/p90) and the deduped
--   count distribution, so the _WHITESPACE_LOG_REF anchor shift can be sized.
--
-- CAVEAT: simplified single-token category match ('burger') — under-counts vs
-- the production alias-expanded match (_expand_category), same approximation
-- as scripts/diagnostics/qsr_whitespace_probe.sql. Treat counts as a lower
-- bound; the overlap SHARE is the robust output.
--
-- Run:  psql -x -f scripts/diagnostics/competitor_cross_source_overlap.sql > /tmp/out.txt 2>&1
-- ============================================================================

WITH recent_search AS (
    SELECT id, category, service_model
    FROM expansion_search
    WHERE lower(service_model) = 'qsr' OR lower(category) LIKE '%burger%'
    ORDER BY created_at DESC
    LIMIT 5
),
centroids AS (
    SELECT
        c.id  AS candidate_id,
        c.lat::double precision AS lat,
        c.lon::double precision AS lon
    FROM expansion_candidate c
    JOIN recent_search s ON s.id = c.search_id
    WHERE c.lat IS NOT NULL AND c.lon IS NOT NULL
    LIMIT 120
),
-- Source 1: same-category Google POIs in radius, with normalized name.
poi AS (
    SELECT
        ce.candidate_id,
        rp.id AS poi_id,
        TRIM(regexp_replace(
            regexp_replace(
                TRANSLATE(
                    LOWER(COALESCE(rp.name, '')),
                    E'أإآىـ',
                    E'اااي'
                ),
                '[^a-z0-9\s؀-ۿ]', ' ', 'g'
            ),
            '\s+', ' ', 'g'
        )) AS norm_name,
        rp.lat::double precision AS lat,
        rp.lon::double precision AS lon
    FROM centroids ce
    JOIN restaurant_poi rp
      ON lower(rp.category) = 'burger'
     AND (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
     AND ST_DWithin(
            ST_SetSRID(ST_MakePoint(rp.lon::double precision, rp.lat::double precision), 4326)::geography,
            ST_SetSRID(ST_MakePoint(ce.lon, ce.lat), 4326)::geography,
            1000)
),
-- Source 2: same-category delivery rows in radius, with normalized name.
dsr AS (
    SELECT
        ce.candidate_id,
        d.id AS dsr_id,
        TRIM(regexp_replace(
            regexp_replace(
                TRANSLATE(
                    LOWER(COALESCE(d.restaurant_name_raw, '')),
                    E'أإآىـ',
                    E'اااي'
                ),
                '[^a-z0-9\s؀-ۿ]', ' ', 'g'
            ),
            '\s+', ' ', 'g'
        )) AS norm_name,
        d.lat::double precision AS lat,
        d.lon::double precision AS lon
    FROM centroids ce
    JOIN delivery_source_record d
      ON (lower(COALESCE(d.category_raw, '')) ~* 'burger'
          OR lower(COALESCE(d.cuisine_raw, '')) ~* 'burger')
     AND d.lat IS NOT NULL AND d.lon IS NOT NULL
     AND ST_DWithin(
            ST_SetSRID(ST_MakePoint(d.lon::double precision, d.lat::double precision), 4326)::geography,
            ST_SetSRID(ST_MakePoint(ce.lon, ce.lat), 4326)::geography,
            1000)
),
per_candidate AS (
    SELECT
        ce.candidate_id,
        (SELECT COUNT(*) FROM poi p WHERE p.candidate_id = ce.candidate_id) AS poi_count,
        (SELECT COUNT(*) FROM dsr d WHERE d.candidate_id = ce.candidate_id) AS dsr_count,
        -- name-match overlap: DSR rows whose normalized name equals a POI's.
        (SELECT COUNT(DISTINCT d.dsr_id)
           FROM dsr d
          WHERE d.candidate_id = ce.candidate_id
            AND d.norm_name <> ''
            AND EXISTS (
                SELECT 1 FROM poi p
                 WHERE p.candidate_id = ce.candidate_id
                   AND p.norm_name = d.norm_name)
        ) AS name_overlap_dsr,
        -- spatial overlap: DSR rows within 75 m of any same-category POI.
        (SELECT COUNT(DISTINCT d.dsr_id)
           FROM dsr d
          WHERE d.candidate_id = ce.candidate_id
            AND EXISTS (
                SELECT 1 FROM poi p
                 WHERE p.candidate_id = ce.candidate_id
                   AND ST_DWithin(
                        ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326)::geography,
                        ST_SetSRID(ST_MakePoint(d.lon, d.lat), 4326)::geography,
                        75))
        ) AS spatial_overlap_dsr
    FROM centroids ce
),
overlap_share AS (
    SELECT
        candidate_id,
        poi_count,
        dsr_count,
        (poi_count + dsr_count) AS union_all_count,
        name_overlap_dsr,
        spatial_overlap_dsr,
        GREATEST(name_overlap_dsr, spatial_overlap_dsr) AS est_overlap,
        CASE WHEN (poi_count + dsr_count) > 0
             THEN GREATEST(name_overlap_dsr, spatial_overlap_dsr)::numeric / (poi_count + dsr_count)
             ELSE 0 END AS overlap_share
    FROM per_candidate
)
SELECT
    COUNT(*)                                                                              AS n_candidates,
    ROUND(AVG(poi_count), 1)                                                              AS avg_poi_count,
    ROUND(AVG(dsr_count), 1)                                                              AS avg_dsr_count,
    ROUND(AVG(union_all_count), 1)                                                        AS avg_union_all_count,
    -- overlap share = est_overlap / union_all_count
    ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY overlap_share)::numeric, 3)        AS overlap_share_p50,
    ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY overlap_share)::numeric, 3)        AS overlap_share_p75,
    ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY overlap_share)::numeric, 3)        AS overlap_share_p90,
    -- current (inflated, UNION ALL) competitor count distribution
    ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY union_all_count)::numeric, 1)      AS union_count_p50,
    ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY union_all_count)::numeric, 1)      AS union_count_p75,
    ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY union_all_count)::numeric, 1)      AS union_count_p90,
    -- deduped count = union - est_overlap; use to re-anchor _WHITESPACE_LOG_REF
    ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY union_all_count - est_overlap)::numeric, 1) AS deduped_count_p50,
    ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY union_all_count - est_overlap)::numeric, 1) AS deduped_count_p75,
    ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY union_all_count - est_overlap)::numeric, 1) AS deduped_count_p90
FROM overlap_share;
