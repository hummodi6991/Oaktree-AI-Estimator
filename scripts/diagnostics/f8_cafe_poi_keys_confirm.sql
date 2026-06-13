-- F8 confirmation probe: cafe/coffee dark POI competitor leg
-- ---------------------------------------------------------------------------
-- Proves the same-category brick-and-mortar competitor leg for cafe/coffee
-- searches is no longer dark after correcting
-- _CATEGORY_ALIAS_MAP["cafe"]/["coffee"]["keys"] from the meta-bucket
-- {coffee_bakery} (never stored in restaurant_poi.category) to the real
-- granular set {cafe, coffee, bakery, dessert}.
--
-- For a sample of recent cafe/coffee candidate centroids (1000 m radius),
-- this shows same-category restaurant_poi counts under BOTH key sets in a
-- single query so the before/after is directly comparable.
--
--   poi_count_old = lower(category) = ANY('{coffee_bakery}')           -> ~0
--   poi_count_new = lower(category) = ANY('{cafe,coffee,bakery,dessert}') -> dozens..hundreds
--
-- Non-interactive. Run with:
--   psql -x -f scripts/diagnostics/f8_cafe_poi_keys_confirm.sql "$DATABASE_URL"
--
-- Matches the live competitor query: restaurant_poi joined by geography
-- ST_DWithin at the 1000 m competition radius, excluding non-operational
-- venues (business_status NULL or 'OPERATIONAL').
-- ---------------------------------------------------------------------------

\set ON_ERROR_STOP on

-- Tunables (overridable via -v): radius, candidate sample size, lookback.
\if :{?radius_m}
\else
  \set radius_m 1000
\endif
\if :{?sample_limit}
\else
  \set sample_limit 200
\endif
\if :{?lookback_days}
\else
  \set lookback_days 90
\endif

-- ---------------------------------------------------------------------------
-- [1] Per-candidate before/after counts (head of the sample).
-- ---------------------------------------------------------------------------
WITH cand AS (
    SELECT c.id AS candidate_id,
           c.search_id,
           s.category,
           c.lon,
           c.lat
    FROM expansion_candidate c
    JOIN expansion_search s ON s.id = c.search_id
    WHERE lower(s.category) IN ('cafe', 'coffee')
      AND c.lon IS NOT NULL
      AND c.lat IS NOT NULL
      AND s.created_at >= now() - make_interval(days => :lookback_days)
    ORDER BY s.created_at DESC, c.id
    LIMIT :sample_limit
),
counts AS (
    SELECT cand.candidate_id,
           cand.category,
           (
             SELECT count(*)
             FROM restaurant_poi rp
             WHERE (rp.business_status IS NULL
                    OR rp.business_status = 'OPERATIONAL')
               AND lower(rp.category) = ANY('{coffee_bakery}')
               AND ST_DWithin(
                     rp.geom::geography,
                     ST_SetSRID(ST_MakePoint(cand.lon, cand.lat), 4326)::geography,
                     :radius_m
                   )
           ) AS poi_count_old,
           (
             SELECT count(*)
             FROM restaurant_poi rp
             WHERE (rp.business_status IS NULL
                    OR rp.business_status = 'OPERATIONAL')
               AND lower(rp.category) = ANY('{cafe,coffee,bakery,dessert}')
               AND ST_DWithin(
                     rp.geom::geography,
                     ST_SetSRID(ST_MakePoint(cand.lon, cand.lat), 4326)::geography,
                     :radius_m
                   )
           ) AS poi_count_new
    FROM cand
)
SELECT candidate_id,
       category,
       poi_count_old,
       poi_count_new,
       (poi_count_new - poi_count_old) AS delta,
       CASE WHEN poi_count_old > 0
            THEN round(poi_count_new::numeric / poi_count_old, 1)
            ELSE NULL END AS ratio
FROM counts
ORDER BY poi_count_new DESC
LIMIT 25;

-- ---------------------------------------------------------------------------
-- [2] Aggregate summary: p50/p90 of each key set + dark-leg confirmation.
-- Expected: p50/p90 old ~ 0; p50/p90 new in the dozens-to-hundreds.
-- ---------------------------------------------------------------------------
WITH cand AS (
    SELECT c.id AS candidate_id,
           s.category,
           c.lon,
           c.lat
    FROM expansion_candidate c
    JOIN expansion_search s ON s.id = c.search_id
    WHERE lower(s.category) IN ('cafe', 'coffee')
      AND c.lon IS NOT NULL
      AND c.lat IS NOT NULL
      AND s.created_at >= now() - make_interval(days => :lookback_days)
    ORDER BY s.created_at DESC, c.id
    LIMIT :sample_limit
),
counts AS (
    SELECT cand.category,
           (
             SELECT count(*)
             FROM restaurant_poi rp
             WHERE (rp.business_status IS NULL
                    OR rp.business_status = 'OPERATIONAL')
               AND lower(rp.category) = ANY('{coffee_bakery}')
               AND ST_DWithin(
                     rp.geom::geography,
                     ST_SetSRID(ST_MakePoint(cand.lon, cand.lat), 4326)::geography,
                     :radius_m
                   )
           ) AS poi_count_old,
           (
             SELECT count(*)
             FROM restaurant_poi rp
             WHERE (rp.business_status IS NULL
                    OR rp.business_status = 'OPERATIONAL')
               AND lower(rp.category) = ANY('{cafe,coffee,bakery,dessert}')
               AND ST_DWithin(
                     rp.geom::geography,
                     ST_SetSRID(ST_MakePoint(cand.lon, cand.lat), 4326)::geography,
                     :radius_m
                   )
           ) AS poi_count_new
    FROM cand
)
SELECT category,
       count(*)                                                       AS candidates,
       round(avg(poi_count_old), 2)                                   AS old_avg,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY poi_count_old)     AS old_p50,
       percentile_cont(0.9) WITHIN GROUP (ORDER BY poi_count_old)     AS old_p90,
       round(avg(poi_count_new), 2)                                   AS new_avg,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY poi_count_new)     AS new_p50,
       percentile_cont(0.9) WITHIN GROUP (ORDER BY poi_count_new)     AS new_p90
FROM counts
GROUP BY category
ORDER BY category;

-- ---------------------------------------------------------------------------
-- [3] Ground truth: confirm restaurant_poi never stores 'coffee_bakery' and
-- the four granular cafe categories carry the volume the leg should count.
-- ---------------------------------------------------------------------------
SELECT lower(category) AS category,
       count(*)        AS poi_rows
FROM restaurant_poi
WHERE lower(category) IN ('cafe', 'coffee', 'bakery', 'dessert', 'coffee_bakery')
GROUP BY lower(category)
ORDER BY poi_rows DESC;
