-- ============================================================================
-- qsr_whitespace_probe.sql — search-scoped post-rollout check (PR-B, Item 1)
-- ----------------------------------------------------------------------------
-- Validates the qsr competition_whitespace recalibration (competition radius
-- 1200 -> 1000 m, REF 25 -> 75) on the LATEST qsr search. Run AFTER deploy and
-- a fresh city-wide qsr search (old searches won't backfill). Expected vs the
-- pre-fix probe: pct_floored drops from ~67% (burger scope) toward ~5%,
-- distinct_whitespace_values >= 6 in a 15-candidate shortlist, and
-- competitor_count_p50 reflects 1000 m (~0.6–0.7x the 1200 m counts).
--
-- HOW TO RUN (psql -f safe, no \set):
--   psql "$DATABASE_URL" -f scripts/diagnostics/qsr_whitespace_probe.sql
-- ============================================================================
WITH s AS (
    SELECT id FROM expansion_search
    WHERE service_model = 'qsr'
    ORDER BY created_at DESC LIMIT 1
)
SELECT
    ec.search_id,
    COUNT(*)                                              AS candidates,
    COUNT(*) FILTER (WHERE ec.whitespace_score <= 15.0)   AS floored,
    ROUND(100.0 * COUNT(*) FILTER (WHERE ec.whitespace_score <= 15.0)
          / COUNT(*), 1)                                  AS pct_floored,
    COUNT(DISTINCT ec.whitespace_score)                   AS distinct_whitespace_values,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY ec.competitor_count)  AS competitor_count_p50,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY ec.whitespace_score)  AS whitespace_p50,
    -- Monotonic score-vs-count sanity: must be <= 0 (more competitors,
    -- lower whitespace) for every confident candidate.
    corr(ec.competitor_count::float, ec.whitespace_score::float)      AS count_score_corr
FROM expansion_candidate ec
JOIN s ON ec.search_id = s.id
GROUP BY ec.search_id;
