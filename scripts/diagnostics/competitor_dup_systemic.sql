\pset footer off
--
-- competitor_dup_systemic.sql
--
-- READ-ONLY. Run in Codespace against the production DB:
--     psql -x -f scripts/diagnostics/competitor_dup_systemic.sql
--
-- Companion to competitor_dup_audit.sql. That probe inspects ONE candidate;
-- this one runs the same raw-vs-distinct-vs-clustered audit across EVERY
-- candidate of the most recent qsr expansion search, then aggregates, so we
-- can tell whether the brand_presence inflation is systemic or a single bad
-- POI cluster in Al Olaya.
--
-- Inflation is measured exactly as the memo would surface it: per
-- (candidate, brand_group) within 500 m, raw_branch_count = COUNT(*) of ecq
-- rows (the _bulk_brand_presence COUNT(*)), cluster_count = stores remaining
-- after collapsing rows within :cluster_m. brand_group = canonical_brand_id
-- when present, else the raw brand_name (mirrors the canonical leg that
-- collapses EN/AR variants).
--
-- Tables / columns (alembic 20260310_exp_adv_v0, d4e5f6a1b2c3,
-- 20260426_ecq_canonical_cols):
--   expansion_search(id, created_at, service_model, category)
--   expansion_candidate(id, search_id, parcel_id, lat, lon, final_score)
--   expansion_competitor_quality(geom Point4326, brand_name, category,
--       canonical_brand_id, ...)   -- coords via ST_X/ST_Y(geom)
-- Candidate point built with ST_MakePoint(lon, lat) per the codebase.
--
\set service_model 'qsr'
\set radius_m 500
\set cluster_m 25

\echo ''
\echo '=== Target search (most recent of the chosen service_model) ==='
SELECT id AS search_id, created_at, brand_name, category, service_model
FROM expansion_search
WHERE service_model = :'service_model'
ORDER BY created_at DESC
LIMIT 1;

\echo ''
\echo '=== A. Per-(candidate,brand) inflation across the whole search ==='
\echo '    One row per brand that has >1 raw ecq row inside 500m of a candidate.'
\echo '    Sorted by the worst inflation first. Surfaces whether multiple'
\echo '    candidates / brands inflate, not just Domino in Al Olaya.'
WITH latest AS (
  SELECT id AS search_id
  FROM expansion_search
  WHERE service_model = :'service_model'
  ORDER BY created_at DESC
  LIMIT 1
),
cand AS (
  SELECT ec.id AS candidate_id, ec.parcel_id,
         ec.lat::float8 AS lat, ec.lon::float8 AS lon
  FROM expansion_candidate ec
  JOIN latest l ON l.search_id = ec.search_id
),
rows_in_radius AS (
  SELECT
    c.candidate_id, c.parcel_id,
    ecq.id AS ecq_id,
    COALESCE(ecq.canonical_brand_id, 'name:' || ecq.brand_name) AS brand_group,
    ecq.geom,
    ST_Distance(
      ecq.geom::geography,
      ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography
    ) AS dist_m
  FROM cand c
  JOIN expansion_competitor_quality ecq
    ON ecq.geom IS NOT NULL
   AND ST_DWithin(
         ecq.geom::geography,
         ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography,
         :radius_m)
),
clustered AS (
  SELECT a.candidate_id, a.brand_group, a.ecq_id,
         NOT EXISTS (
           SELECT 1 FROM rows_in_radius b
           WHERE b.candidate_id = a.candidate_id
             AND b.brand_group  = a.brand_group
             AND b.ecq_id <> a.ecq_id
             AND (b.dist_m < a.dist_m OR (b.dist_m = a.dist_m AND b.ecq_id < a.ecq_id))
             AND ST_DWithin(a.geom::geography, b.geom::geography, :cluster_m)
         ) AS is_cluster_head
  FROM rows_in_radius a
),
per_brand AS (
  SELECT
    r.candidate_id, r.parcel_id, r.brand_group,
    COUNT(*) AS raw_branch_count,
    SUM(CASE WHEN cl.is_cluster_head THEN 1 ELSE 0 END) AS cluster_count,
    ROUND(MIN(r.dist_m)::numeric, 1) AS nearest_m
  FROM rows_in_radius r
  JOIN clustered cl USING (candidate_id, brand_group, ecq_id)
  GROUP BY r.candidate_id, r.parcel_id, r.brand_group
)
SELECT
  parcel_id, brand_group, raw_branch_count, cluster_count, nearest_m,
  ROUND(raw_branch_count::numeric / NULLIF(cluster_count, 0), 2) AS inflation_ratio
FROM per_brand
WHERE raw_branch_count > 1
ORDER BY inflation_ratio DESC NULLS FIRST, raw_branch_count DESC
LIMIT 50;

\echo ''
\echo '=== B. Search-wide aggregate: how systemic is the gap? ==='
\echo '    multi_row_brands       = (candidate,brand) pairs with raw count > 1'
\echo '    inflated_brands        = those where raw > cluster (real over-count)'
\echo '    avg_inflation_ratio    = mean raw/cluster over multi-row brands'
\echo '    total_raw / total_clustered = global branch_count if dedup applied'
WITH latest AS (
  SELECT id AS search_id
  FROM expansion_search
  WHERE service_model = :'service_model'
  ORDER BY created_at DESC
  LIMIT 1
),
cand AS (
  SELECT ec.id AS candidate_id, ec.lat::float8 AS lat, ec.lon::float8 AS lon
  FROM expansion_candidate ec
  JOIN latest l ON l.search_id = ec.search_id
),
rows_in_radius AS (
  SELECT
    c.candidate_id,
    ecq.id AS ecq_id,
    COALESCE(ecq.canonical_brand_id, 'name:' || ecq.brand_name) AS brand_group,
    ecq.geom,
    ST_Distance(
      ecq.geom::geography,
      ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography
    ) AS dist_m
  FROM cand c
  JOIN expansion_competitor_quality ecq
    ON ecq.geom IS NOT NULL
   AND ST_DWithin(
         ecq.geom::geography,
         ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography,
         :radius_m)
),
clustered AS (
  SELECT a.candidate_id, a.brand_group, a.ecq_id,
         NOT EXISTS (
           SELECT 1 FROM rows_in_radius b
           WHERE b.candidate_id = a.candidate_id
             AND b.brand_group  = a.brand_group
             AND b.ecq_id <> a.ecq_id
             AND (b.dist_m < a.dist_m OR (b.dist_m = a.dist_m AND b.ecq_id < a.ecq_id))
             AND ST_DWithin(a.geom::geography, b.geom::geography, :cluster_m)
         ) AS is_cluster_head
  FROM rows_in_radius a
),
per_brand AS (
  SELECT
    r.candidate_id, r.brand_group,
    COUNT(*) AS raw_branch_count,
    SUM(CASE WHEN cl.is_cluster_head THEN 1 ELSE 0 END) AS cluster_count
  FROM rows_in_radius r
  JOIN clustered cl USING (candidate_id, brand_group, ecq_id)
  GROUP BY r.candidate_id, r.brand_group
)
SELECT
  COUNT(*) FILTER (WHERE raw_branch_count > 1)                       AS multi_row_brands,
  COUNT(*) FILTER (WHERE raw_branch_count > cluster_count)           AS inflated_brands,
  ROUND(AVG(raw_branch_count::numeric / NULLIF(cluster_count, 0))
        FILTER (WHERE raw_branch_count > 1), 3)                      AS avg_inflation_ratio,
  MAX(raw_branch_count)                                              AS worst_raw_count,
  SUM(raw_branch_count)                                              AS total_raw,
  SUM(cluster_count)                                                 AS total_clustered,
  ROUND(SUM(raw_branch_count)::numeric / NULLIF(SUM(cluster_count), 0), 3)
                                                                     AS global_ratio
FROM per_brand;
