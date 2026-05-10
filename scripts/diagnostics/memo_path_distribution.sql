-- ============================================================================
-- Decision-memo path distribution diagnostic
-- ============================================================================
--
-- Production decision memos render in one of two structurally distinct shapes
-- (see expansion_advisor.py:1535-1665 / DecisionMemoNarrative.tsx:319-332):
--
--   * STRUCTURED  (KEY EVIDENCE / RISKS TO WATCH / HOW IT COMPARES + verdict
--                  box) — written when generate_structured_memo() returns a
--                  parsed dict that survives shape validation. Persisted as
--                  expansion_candidate.decision_memo_json (JSONB, non-null).
--   * FALLBACK    (WHY PURSUE / RISKS TO WEIGH / Recommended next action +
--                  italic rent_context tail) — written when the structured
--                  path returns None (flag off, cost ceiling, LLM error, JSON
--                  parse fail, missing required key, empty key_evidence,
--                  invalid advisory section, etc.). Persisted with
--                  decision_memo_json = NULL and decision_memo (TEXT) populated
--                  by _legacy_memo_to_text().
--
-- Discriminator (confirmed in expansion_advisor.py:1632-1657):
--   structured   ⇔  decision_memo_json IS NOT NULL
--   fallback     ⇔  decision_memo_json IS NULL AND decision_memo IS NOT NULL
--
-- Schema confirmation:
--   * expansion_candidate.computed_at                  alembic 20260310_exp_adv_v0:71-76
--   * expansion_candidate.decision_memo                alembic 20260414_memo_json:34
--   * expansion_candidate.decision_memo_json           alembic 20260414_memo_json:38
--   * expansion_candidate.decision_memo_prompt_version alembic 20260425_memo_prompt_version:29-36
--   * expansion_candidate.score_breakdown_json         (JSONB column on expansion_candidate)
--
-- JSON paths confirmed in app/services/expansion_advisor.py:5002-5013 and
-- app/services/llm_decision_memo.py:1144-1146:
--   score_breakdown_json -> economics_detail -> rent_burden -> source_label
--   score_breakdown_json -> economics_detail -> rent_burden -> percentile
--
-- The MEMO_PROMPT_VERSION stamp is NOT embedded inside decision_memo_json — it
-- lives in the dedicated decision_memo_prompt_version TEXT column.  Section 6
-- below therefore reads the column, not a JSON path.
-- ============================================================================

\pset border 2
\pset null '∅'

-- ---------------------------------------------------------------------------
-- 1. Total candidates with decision_memo_json populated, last 30 days.
--    (Also breaks down memo presence so the denominator for %s is explicit.)
-- ---------------------------------------------------------------------------
\echo
\echo === 1. Memo population over the last 30 days ===
SELECT
    COUNT(*)                                                       AS candidates_total,
    COUNT(*) FILTER (WHERE decision_memo IS NOT NULL
                       OR  decision_memo_json IS NOT NULL)         AS with_any_memo,
    COUNT(*) FILTER (WHERE decision_memo_json IS NOT NULL)         AS with_structured_json,
    COUNT(*) FILTER (WHERE decision_memo_json IS NULL
                      AND  decision_memo IS NOT NULL)              AS with_fallback_text_only
FROM expansion_candidate
WHERE computed_at >= now() - interval '30 days';


-- ---------------------------------------------------------------------------
-- 2. Structured (primary) path count + share of candidates with ANY memo.
--    Discriminator: decision_memo_json IS NOT NULL  (see header).
-- ---------------------------------------------------------------------------
\echo
\echo === 2. Structured (primary) path share ===
WITH base AS (
    SELECT *
    FROM expansion_candidate
    WHERE computed_at >= now() - interval '30 days'
      AND (decision_memo IS NOT NULL OR decision_memo_json IS NOT NULL)
)
SELECT
    COUNT(*) FILTER (WHERE decision_memo_json IS NOT NULL)               AS structured_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE decision_memo_json IS NOT NULL)
        / NULLIF(COUNT(*), 0),
        2
    )                                                                    AS structured_pct,
    COUNT(*)                                                             AS denom_with_any_memo
FROM base;


-- ---------------------------------------------------------------------------
-- 3. Fallback (legacy) path count + share. Discriminator:
--    decision_memo_json IS NULL AND decision_memo IS NOT NULL.
-- ---------------------------------------------------------------------------
\echo
\echo === 3. Fallback (legacy) path share ===
WITH base AS (
    SELECT *
    FROM expansion_candidate
    WHERE computed_at >= now() - interval '30 days'
      AND (decision_memo IS NOT NULL OR decision_memo_json IS NOT NULL)
)
SELECT
    COUNT(*) FILTER (WHERE decision_memo_json IS NULL
                      AND  decision_memo IS NOT NULL)                    AS fallback_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE decision_memo_json IS NULL
                                  AND  decision_memo IS NOT NULL)
        / NULLIF(COUNT(*), 0),
        2
    )                                                                    AS fallback_pct,
    COUNT(*)                                                             AS denom_with_any_memo
FROM base;


-- ---------------------------------------------------------------------------
-- 4. Cross-tab: memo path × rent_scope (rent_burden.source_label).
--    Path: score_breakdown_json -> economics_detail -> rent_burden -> source_label.
--    If this returns zero rows, alternate path to try is
--      score_breakdown_json -> rent_scope
--    (top-level alias) — confirmed unused in the live codebase but worth a fallback check.
-- ---------------------------------------------------------------------------
\echo
\echo === 4. Memo path x rent_scope ===
WITH base AS (
    SELECT
        CASE
            WHEN decision_memo_json IS NOT NULL                          THEN 'structured'
            WHEN decision_memo_json IS NULL AND decision_memo IS NOT NULL THEN 'fallback'
            ELSE 'no_memo'
        END                                                              AS memo_path,
        score_breakdown_json
            #>> '{economics_detail,rent_burden,source_label}'            AS rent_scope
    FROM expansion_candidate
    WHERE computed_at >= now() - interval '30 days'
      AND (decision_memo IS NOT NULL OR decision_memo_json IS NOT NULL)
)
SELECT
    memo_path,
    COALESCE(rent_scope, 'NULL')                                         AS rent_scope,
    COUNT(*)                                                             AS n
FROM base
GROUP BY memo_path, COALESCE(rent_scope, 'NULL')
ORDER BY memo_path, n DESC;


-- ---------------------------------------------------------------------------
-- 5. Cross-tab: memo path × rent_burden.percentile presence (NULL vs populated).
--    Path: score_breakdown_json -> economics_detail -> rent_burden -> percentile.
-- ---------------------------------------------------------------------------
\echo
\echo === 5. Memo path x rent_burden.percentile presence ===
WITH base AS (
    SELECT
        CASE
            WHEN decision_memo_json IS NOT NULL                          THEN 'structured'
            WHEN decision_memo_json IS NULL AND decision_memo IS NOT NULL THEN 'fallback'
            ELSE 'no_memo'
        END                                                              AS memo_path,
        (score_breakdown_json
            #> '{economics_detail,rent_burden,percentile}') IS NOT NULL  AS percentile_present
    FROM expansion_candidate
    WHERE computed_at >= now() - interval '30 days'
      AND (decision_memo IS NOT NULL OR decision_memo_json IS NOT NULL)
)
SELECT
    memo_path,
    percentile_present,
    COUNT(*)                                                             AS n
FROM base
GROUP BY memo_path, percentile_present
ORDER BY memo_path, percentile_present DESC;


-- ---------------------------------------------------------------------------
-- 6. Cross-tab: memo path × decision_memo_prompt_version.
--    The prompt-version stamp is the COLUMN, not a JSON key. Pre-versioning
--    rows hold NULL.
-- ---------------------------------------------------------------------------
\echo
\echo === 6. Memo path x decision_memo_prompt_version ===
WITH base AS (
    SELECT
        CASE
            WHEN decision_memo_json IS NOT NULL                          THEN 'structured'
            WHEN decision_memo_json IS NULL AND decision_memo IS NOT NULL THEN 'fallback'
            ELSE 'no_memo'
        END                                                              AS memo_path,
        decision_memo_prompt_version
    FROM expansion_candidate
    WHERE computed_at >= now() - interval '30 days'
      AND (decision_memo IS NOT NULL OR decision_memo_json IS NOT NULL)
)
SELECT
    memo_path,
    COALESCE(decision_memo_prompt_version, 'NULL')                       AS prompt_version,
    COUNT(*)                                                             AS n
FROM base
GROUP BY memo_path, COALESCE(decision_memo_prompt_version, 'NULL')
ORDER BY memo_path, n DESC;


-- ---------------------------------------------------------------------------
-- 7. Twenty sample candidate ids per path with their headline.
--    Structured headline lives at decision_memo_json -> headline_recommendation
--    (see llm_decision_memo.py:1257).  Fallback headline is the first ~120
--    chars of the legacy memo dict's "headline" — but since that path persists
--    only the rendered text, we surface the first 200 chars of decision_memo
--    instead so the spot-check still has something to read.
-- ---------------------------------------------------------------------------
\echo
\echo === 7a. Structured-path samples (n=20) ===
SELECT
    id,
    parcel_id,
    decision_memo_json ->> 'headline_recommendation'                     AS headline
FROM expansion_candidate
WHERE computed_at >= now() - interval '30 days'
  AND decision_memo_json IS NOT NULL
ORDER BY computed_at DESC
LIMIT 20;

\echo
\echo === 7b. Fallback-path samples (n=20) ===
SELECT
    id,
    parcel_id,
    LEFT(decision_memo, 200)                                             AS headline_text
FROM expansion_candidate
WHERE computed_at >= now() - interval '30 days'
  AND decision_memo_json IS NULL
  AND decision_memo IS NOT NULL
ORDER BY computed_at DESC
LIMIT 20;
