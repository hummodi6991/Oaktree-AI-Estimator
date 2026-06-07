\pset footer off
--
-- dinein_catchment_density.sql
--
-- QUESTION: If we built a dine-in realized-demand signal by aggregating
-- review-velocity across nearby restaurant_poi branches (mirroring the
-- delivery branch_delta CTE, app/services/expansion_advisor.py:7521-7552),
-- would a typical candidate even HAVE enough branches in its catchment for
-- the signal to be non-trivial? Delivery requires >=2 snapshots per branch
-- and the candidate carries the signal only at >=3 contributing branches
-- (expansion_advisor.py:7873). This measures the upstream venue density.
--
-- HOW TO READ:
--   A. Sample size actually drawn (~300 live candidates).
--   B. Per-candidate count of live restaurant_poi venues (business_status
--      NULL/OPERATIONAL — the advisor's live filter, expansion_advisor.py:6582)
--      within the dine_in DEMAND radius 3500 m (_CATCHMENT_RADII_M["dine_in"],
--      expansion_advisor.py:818) AND a tighter 1200 m
--      (EXPANSION_REALIZED_DEMAND_RADIUS_M default, config.py:103).
--      Distribution buckets >=3 / >=5 / >=10 tell us how many candidates would
--      clear a plausible minimum-branch gate. ANY-category count is the
--      OPTIMISTIC upper bound (same-category would be smaller).
--
-- Candidate pool = candidate_location Tier-1 cluster primaries, the live
-- scoring retrieval path when >=10 Tier-1 rows exist
-- (app/services/expansion_advisor.py:7101-7142, 6108-6134). Riyadh-only.
--
\echo ''
\echo '=== A. Candidate sample drawn ==='
WITH cands AS (
  SELECT cl.id, cl.geom
  FROM candidate_location cl
  WHERE cl.is_cluster_primary = TRUE
    AND cl.source_tier = 1
    AND cl.geom IS NOT NULL
  ORDER BY cl.id
  LIMIT 300
)
SELECT COUNT(*) AS sampled_candidates FROM cands;

\echo ''
\echo '=== B. Per-candidate live dine-in branch counts (any category) ==='
\echo '    n_3500 = live restaurant_poi within 3.5 km; n_1200 = within 1.2 km'
WITH cands AS (
  SELECT cl.id, cl.geom
  FROM candidate_location cl
  WHERE cl.is_cluster_primary = TRUE
    AND cl.source_tier = 1
    AND cl.geom IS NOT NULL
  ORDER BY cl.id
  LIMIT 300
),
per_cand AS (
  SELECT
    c.id,
    COUNT(rp.id)                                                         AS n_3500,
    COUNT(rp.id) FILTER (
      WHERE ST_DWithin(rp.geom::geography, c.geom::geography, 1200)
    )                                                                    AS n_1200
  FROM cands c
  LEFT JOIN restaurant_poi rp
    ON rp.geom IS NOT NULL
   AND (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
   AND ST_DWithin(rp.geom::geography, c.geom::geography, 3500)
  GROUP BY c.id
)
SELECT
  COUNT(*)                                                       AS candidates,
  ROUND(AVG(n_3500), 1)                                          AS avg_branches_3500m,
  percentile_disc(0.50) WITHIN GROUP (ORDER BY n_3500)           AS median_3500m,
  percentile_disc(0.90) WITHIN GROUP (ORDER BY n_3500)           AS p90_3500m,
  COUNT(*) FILTER (WHERE n_3500 >= 3)                            AS ge3_at_3500m,
  COUNT(*) FILTER (WHERE n_3500 >= 5)                            AS ge5_at_3500m,
  COUNT(*) FILTER (WHERE n_3500 >= 10)                           AS ge10_at_3500m,
  ROUND(AVG(n_1200), 1)                                          AS avg_branches_1200m,
  percentile_disc(0.50) WITHIN GROUP (ORDER BY n_1200)           AS median_1200m,
  COUNT(*) FILTER (WHERE n_1200 >= 3)                            AS ge3_at_1200m,
  COUNT(*) FILTER (WHERE n_1200 >= 5)                            AS ge5_at_1200m,
  COUNT(*) FILTER (WHERE n_1200 >= 10)                           AS ge10_at_1200m
FROM per_cand;
