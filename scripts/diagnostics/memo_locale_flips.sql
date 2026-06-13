-- F3 — Locale ping-pong memo regeneration probe
-- ============================================================================
-- get_candidate_memo() -> _regenerate_candidate_memo_in_locale()
-- (app/services/expansion_advisor.py ~line 12009-12071, fired at ~12325)
-- overwrites a SINGLE storage slot on every requested-vs-stored locale
-- mismatch:
--     UPDATE expansion_candidate
--        SET decision_memo = ..., decision_memo_json = ...,
--            decision_memo_prompt_version = ..., decision_memo_lang = :lang
--      WHERE id = :cid
-- So an AR view then an EN view then an AR view re-generates the memo each
-- time (one EXPANSION_MEMO_MODEL call per flip) and flips the single slot back
-- and forth — there is no per-locale storage.
--
-- NOTE: there is NO request/audit log table in the schema. decision_memo_lang
-- records only the CURRENT resting locale of each row, so historical flip
-- COUNT cannot be derived from the DB — only the current state is observable.
-- For flip frequency, pull GET /candidates/{id}/memo access patterns from API
-- logs separately.
--
-- Cost per flip: EXPANSION_MEMO_MODEL (config default "gpt-4o-mini";
-- production value via kubectl/secret) at EXPANSION_MEMO_MAX_TOKENS (default
-- 2400) completion tokens + the memo-context prompt.
--
-- Run:  psql -x -f scripts/diagnostics/memo_locale_flips.sql > /tmp/out.txt 2>&1
-- ============================================================================

-- §1 — current resting locale of every memo-bearing candidate.
SELECT
    COALESCE(decision_memo_lang, '(null=en-legacy)') AS memo_lang,
    COUNT(*)                                                  AS n_rows,
    COUNT(*) FILTER (WHERE decision_memo_json IS NOT NULL)    AS n_with_json,
    COUNT(*) FILTER (WHERE decision_memo IS NOT NULL)         AS n_with_text
FROM expansion_candidate
WHERE decision_memo IS NOT NULL OR decision_memo_json IS NOT NULL
GROUP BY 1
ORDER BY n_rows DESC;

-- §2 — prompt-version x locale distribution among memo-bearing rows.
SELECT
    COALESCE(decision_memo_prompt_version, '(null)')         AS prompt_version,
    COALESCE(decision_memo_lang, '(null=en-legacy)')         AS memo_lang,
    COUNT(*)                                                  AS n_rows
FROM expansion_candidate
WHERE decision_memo IS NOT NULL OR decision_memo_json IS NOT NULL
GROUP BY 1, 2
ORDER BY n_rows DESC;

-- §3 — AR-resting rows specifically. Each of these paid at least one
-- regeneration on first AR view; any later EN view re-pays and flips it back.
SELECT
    COUNT(*) AS ar_resting_rows,
    COUNT(*) FILTER (WHERE decision_memo_json IS NOT NULL) AS ar_resting_with_json
FROM expansion_candidate
WHERE decision_memo_lang = 'ar';

-- §4 — total memo-bearing rows (regeneration-eligible population). Any of
-- these, when viewed in the non-stored locale, triggers one model call.
SELECT
    COUNT(*) AS total_memo_rows
FROM expansion_candidate
WHERE decision_memo IS NOT NULL OR decision_memo_json IS NOT NULL;
