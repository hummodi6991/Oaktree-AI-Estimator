\pset footer off
--
-- competitor_dup_audit.sql
--
-- READ-ONLY. Run in Codespace against the production DB:
--     psql -x -f scripts/diagnostics/competitor_dup_audit.sql
--
-- Purpose (investigation Q1): characterize the "Domino's Pizza has 6 branches
-- within 138 m" inflation in the decision-memo "HOW IT COMPARES" block.
--
-- The bulk brand-presence query in app/services/expansion_advisor.py
-- (_bulk_brand_presence, ~L9852-9955) emits, per brand within 500 m of a
-- candidate:
--     branch_count        = COUNT(*) of raw expansion_competitor_quality rows
--     nearest_distance_m  = MIN(distance) of those rows
-- These are SEPARATE values. The "6 within 138 m" reading is an LLM conflation
-- of the two (count=6 with nearest=138). Crucially, COUNT(*) counts RAW ecq
-- rows with NO spatial de-duplication: two POIs at byte-identical coordinates
-- both add to branch_count. This probe quantifies how far the raw count drifts
-- from the true distinct-store count.
--
-- The score-card count (brand_presence, 500 m, GROUP BY canonical_brand_id)
-- is a DIFFERENT query from the comparison narrative's competitor distances
-- (comparable_competitors / _bulk_competitors, 1500 m, DISTINCT ON dedup_key,
-- category-filtered). Section D below contrasts the two so the KFC "401 m vs
-- 388 m" discrepancy can be attributed to the two-source split rather than a
-- bug in either query.
--
-- ecq column reference (alembic d4e5f6a1b2c3 + 20260426_ecq_canonical_cols):
--   geom geometry(Point,4326), brand_name, category, district,
--   restaurant_poi_id, canonical_brand_id, display_name_en, display_name_ar,
--   review_score, review_count, overall_quality_score, city.
-- ecq has NO lat/lon columns — coordinates come from geom (ST_X/ST_Y).
-- The candidate point is built with ST_MakePoint(lon, lat) per the codebase
-- pattern. Edit :p_lon / :p_lat / :p_category below for the candidate of
-- interest. Defaults are the reported case: parcel_id 6706340, Al Olaya, qsr.
--
\set p_parcel_id 6706340
\set p_lat 24.6543170
\set p_lon 46.7107299
\set p_category 'qsr'
\set radius_m 500
\set cluster_m 25

\echo ''
\echo '=== Candidate under audit ==='
SELECT :'p_parcel_id' AS parcel_id, :p_lat AS lat, :p_lon AS lon,
       :'p_category' AS category, :radius_m AS brand_presence_radius_m,
       :cluster_m AS cluster_collapse_m;

\echo ''
\echo '=== A. Per-brand inflation: raw branch_count vs distinct-coord vs 25m-cluster ==='
\echo '    raw_branch_count   = what the memo score card shows (COUNT(*), no dedup)'
\echo '    distinct_coords    = COUNT(DISTINCT exact lat/lon)'
\echo '    cluster_count      = stores after collapsing rows within :cluster_m of each other'
\echo '    inflation_ratio    = raw_branch_count / cluster_count (1.0 = clean, >1 = inflated)'
WITH pt AS (
  SELECT ST_SetSRID(ST_MakePoint(:p_lon, :p_lat), 4326)::geography AS g
),
in_radius AS (
  SELECT
    ecq.id,
    ecq.restaurant_poi_id,
    -- group label: canonical_brand_id when present, else the raw brand_name.
    -- This mirrors the canonical leg of _bulk_brand_presence (the leg that
    -- collapses Domino's EN/AR variants into ONE branch_count when they share
    -- a canonical_brand_id).
    COALESCE(ecq.canonical_brand_id, 'name:' || ecq.brand_name) AS brand_group,
    ecq.canonical_brand_id,
    ecq.brand_name,
    ecq.display_name_en,
    ecq.category,
    ST_Y(ecq.geom) AS lat,
    ST_X(ecq.geom) AS lon,
    ecq.geom,
    ST_Distance(ecq.geom::geography, pt.g) AS dist_m
  FROM expansion_competitor_quality ecq, pt
  WHERE ecq.geom IS NOT NULL
    AND ST_DWithin(ecq.geom::geography, pt.g, :radius_m)
),
-- Greedy spatial clustering: order rows by distance to candidate, assign each
-- row to the first earlier row of the same brand_group within :cluster_m.
-- cluster_count = number of rows that are NOT absorbed by an earlier row.
clustered AS (
  SELECT a.id, a.brand_group,
         NOT EXISTS (
           SELECT 1 FROM in_radius b
           WHERE b.brand_group = a.brand_group
             AND b.id <> a.id
             AND (b.dist_m < a.dist_m OR (b.dist_m = a.dist_m AND b.id < a.id))
             AND ST_DWithin(a.geom::geography, b.geom::geography, :cluster_m)
         ) AS is_cluster_head
  FROM in_radius a
)
SELECT
  ir.brand_group,
  MAX(ir.display_name_en)                               AS sample_name_en,
  MAX(ir.brand_name)                                    AS sample_brand_name,
  bool_or(ir.canonical_brand_id IS NOT NULL)            AS has_canonical,
  COUNT(*)                                              AS raw_branch_count,
  COUNT(DISTINCT (round(ir.lat::numeric, 6) || ',' || round(ir.lon::numeric, 6)))
                                                        AS distinct_coords,
  SUM(CASE WHEN c.is_cluster_head THEN 1 ELSE 0 END)    AS cluster_count,
  ROUND(MIN(ir.dist_m)::numeric, 1)                     AS nearest_m,
  ROUND(MAX(ir.dist_m)::numeric, 1)                     AS farthest_m,
  ROUND(
    COUNT(*)::numeric
    / NULLIF(SUM(CASE WHEN c.is_cluster_head THEN 1 ELSE 0 END), 0)
  , 2)                                                  AS inflation_ratio
FROM in_radius ir
JOIN clustered c USING (id)
GROUP BY ir.brand_group
ORDER BY raw_branch_count DESC, nearest_m ASC;

\echo ''
\echo '=== B. Row-level dump for the most-inflated brand (default: Domino) ==='
\echo '    Shows the byte-identical-coordinate pairs and the ~15x27m cluster box.'
\echo '    Adjust the brand_name ILIKE filter if the inflated brand differs.'
WITH pt AS (
  SELECT ST_SetSRID(ST_MakePoint(:p_lon, :p_lat), 4326)::geography AS g
)
SELECT
  ecq.id,
  ecq.restaurant_poi_id,
  ecq.brand_name,
  ecq.display_name_en,
  ecq.display_name_ar,
  ecq.canonical_brand_id,
  ecq.category,
  ROUND(ST_Y(ecq.geom)::numeric, 6) AS lat,
  ROUND(ST_X(ecq.geom)::numeric, 6) AS lon,
  ROUND(ST_Distance(ecq.geom::geography, pt.g)::numeric, 1) AS dist_m
FROM expansion_competitor_quality ecq, pt
WHERE ecq.geom IS NOT NULL
  AND ST_DWithin(ecq.geom::geography, pt.g, :radius_m)
  AND ecq.brand_name ILIKE '%domino%'
ORDER BY dist_m ASC;

\echo ''
\echo '=== C. Do the Domino EN/AR variants share a canonical_brand_id? (Q1.2) ==='
\echo '    If has_canonical=t and a single canonical_brand_id covers all variant'
\echo '    spellings, the canonical leg of _bulk_brand_presence collapses them to'
\echo '    one branch_count row — confirming the "6" is one brand, not a name bug.'
\echo '    norm_name_key would NOT merge them ("domino s" != "domino s pizza"),'
\echo '    so canonicalization, not name-normalization, is doing the grouping.'
SELECT
  ecq.canonical_brand_id,
  ecq.brand_name,
  COUNT(*) AS rows,
  MAX(ecq.display_name_en) AS display_name_en,
  MAX(ecq.display_name_ar) AS display_name_ar
FROM expansion_competitor_quality ecq
WHERE ecq.brand_name ILIKE '%domino%'
  AND ecq.geom IS NOT NULL
  AND ST_DWithin(
        ecq.geom::geography,
        ST_SetSRID(ST_MakePoint(:p_lon, :p_lat), 4326)::geography,
        :radius_m)
GROUP BY ecq.canonical_brand_id, ecq.brand_name
ORDER BY rows DESC;

\echo ''
\echo '=== D. Two-source contrast: score-card (500m) vs comparison (1500m, category) ==='
\echo '    Reproduces the KFC "401m vs 388m" discrepancy. Left side = the'
\echo '    brand_presence path (500m, all categories). Right side = the'
\echo '    comparable_competitors path (1500m, category-filtered, DISTINCT ON'
\echo '    canonical_brand_id keeping nearest). A category mismatch on the'
\echo '    nearest KFC row is the prime suspect: it is visible to brand_presence'
\echo '    (no category filter) but excluded from comparable_competitors.'
WITH pt AS (
  SELECT ST_SetSRID(ST_MakePoint(:p_lon, :p_lat), 4326)::geography AS g
),
score_card AS (   -- brand_presence semantics: 500 m, NO category filter
  SELECT
    COALESCE(ecq.canonical_brand_id, 'name:' || ecq.brand_name) AS brand_group,
    COUNT(*) AS bp_branch_count,
    ROUND(MIN(ST_Distance(ecq.geom::geography, pt.g))::numeric, 1) AS bp_nearest_m,
    array_agg(DISTINCT ecq.category) AS bp_categories
  FROM expansion_competitor_quality ecq, pt
  WHERE ecq.geom IS NOT NULL
    AND ST_DWithin(ecq.geom::geography, pt.g, 500)
  GROUP BY 1
),
comparison AS ( -- comparable_competitors semantics: 1500 m, category-filtered
  SELECT
    COALESCE(ecq.canonical_brand_id, 'name:' || ecq.brand_name) AS brand_group,
    ROUND(MIN(ST_Distance(ecq.geom::geography, pt.g))::numeric, 1) AS cc_nearest_m
  FROM expansion_competitor_quality ecq, pt
  WHERE ecq.geom IS NOT NULL
    AND lower(COALESCE(ecq.category, '')) = lower(:'p_category')
    AND ST_DWithin(ecq.geom::geography, pt.g, 1500)
  GROUP BY 1
)
SELECT
  COALESCE(s.brand_group, c.brand_group)        AS brand_group,
  s.bp_branch_count,
  s.bp_nearest_m       AS score_card_nearest_m,
  c.cc_nearest_m       AS comparison_nearest_m,
  (s.bp_nearest_m - c.cc_nearest_m) AS nearest_m_delta,
  s.bp_categories
FROM score_card s
FULL OUTER JOIN comparison c USING (brand_group)
ORDER BY COALESCE(s.bp_branch_count, 0) DESC, brand_group;

\echo ''
\echo '=== E. brand_alias coverage for the Domino keys (Q1.2 supporting) ==='
\echo '    Confirms whether the norm_name_key UNION leg would have aliased the'
\echo '    non-canonical Domino spellings. alias_key is keyed on the same'
\echo '    normalization (_CHAIN_NAME_NORM_SQL) used at query time.'
SELECT alias_key, canonical_brand_id, display_name_en, display_name_ar
FROM brand_alias
WHERE alias_key ILIKE '%domino%'
ORDER BY alias_key;
