# PR-F — Relative listing-age signal + hedged negotiating-leverage memo reframe

Branch: `claude/listing-age-leverage-pr-f-s794xg`. Commit + push, **no merge** —
Ahmed reviews (Arabic byte-check on the new Rule 7/8b terms) and approves.

## What changed (path:line inventory)

### Change 1 — backend rider: relative listing-age percentile
`app/services/expansion_advisor.py`
- **`_created_basis_age_days(row)`** (new helper, ~:2925) — listing age on the
  ORIGINAL-listing-date basis: `aqar_created_at` with `first_seen_at` as a
  COALESCE null-guard floor ONLY. Deliberately NOT the GREATEST-of-three basis
  of `_effective_listing_age_days` (a re-posted long-vacant listing must stay
  stale, not reset to the re-post date).
- **`_percentile_rent_burden(...)`** (~:4829) — added `cand_age_days` param;
  added one term to the SAME comparable aggregate:
  `SUM(CASE WHEN EXTRACT(DAY FROM now() - COALESCE(aqar_created_at, first_seen_at)) >= :cand_age_days THEN 1 ELSE 0 END) AS n_older`; derived
  `age_percentile = n_older / n` (share of comparables AS OLD OR OLDER →
  HIGH = old vs peers), rounded 3dp; returns `age_percentile` + `n_comparables`
  (the `n` it was computed over). `age_percentile` is null when
  `cand_age_days` is None; the whole function still returns None below the
  rent percentile's min-N gate, so a percentile is never emitted off an
  under-supported set.
- **`_economics_score(...)`** (~:5045) — threads `cand_age_days` through to
  `_percentile_rent_burden`.
- Call site (~:9712) passes `cand_age_days=_created_basis_age_days(row)`.
- Persistence (~:9800) — added `age_percentile` and `n_comparables` to the
  existing whitelisted `feature_snapshot_json["listing_age"]` dict, sourced
  from `economics_meta["rent_burden"]` only when `mode == "percentile"`
  (else null). `_effective_listing_age_days`, `listing_age.created_days`,
  and `listing_age.updated_days` are **unchanged** — this is a parallel
  created-basis signal.

### Change 2 — memo: internal boolean + zone-conditioned reframe
`app/services/llm_decision_memo.py`
- `MEMO_PROMPT_VERSION` (:53) → `v12.2-listing-age-leverage-2026-06`.
- `_build_advisory_sections` (~:1252) — computes
  `listing_old_relative_to_peers = age_percentile is not None and age_percentile >= 0.75`
  and exposes it on `property_overview` as an **internal boolean** (never a
  spoken number). 0.75 mirrors the rent p75 convention. The rent zone is
  already on `financial_framing.rent_positioning.zone`.
- Preamble "Risk signals" doc (~:1556) — documents the new boolean and that
  the age percentile is internal.
- HARD RULES (~:1593) — risks rule notes staleness + leverage are ONE folded
  item; new **listing-staleness & relative-age discipline** rule:
  data-conditional relativity + zone-conditioned hedged leverage
  (mid / high / low / null branches).
- Few-shot examples C / D / F staleness lines rewritten to model the
  data-conditional + hedged form (Ex F's 64-day "longer than typical"
  unbacked claim removed).

### Change 3 — Arabic Rules 7/8b
`app/services/llm_decision_memo.py` (`_CRITICAL_BLOCK_AR`, ~:2084)
- New **Rule 8b** with fixed AR terms (no English leak); AR Example AR-1
  staleness line rewritten to mirror new Example C.

### Version + fixtures + tests
- `tests/test_llm_decision_memo.py:2769` version-pin updated.
- `tests/data/pr4a_structured_memo_system_prompt_en_head.txt` regenerated.
- New backend tests in `tests/test_expansion_advisor_regression.py`
  (age-percentile compute / null-without-cand-age / null-below-min-N /
  created-basis-not-greatest).
- New memo tests in `tests/test_llm_decision_memo.py`
  (`TestListingOldRelativeToPeersBoolean`, `TestListingAgeLeveragePromptRules`).

## Column-basis confirmation

Staleness rests on **ORIGINAL listing date**: `aqar_created_at` (the TRUE-AGE
column, max 450d), with `first_seen_at` (crawler-truncated at ~73d) used as a
COALESCE null-guard floor ONLY. NOT `aqar_updated_at` (re-post/re-price) and
NOT the GREATEST-of-three `_effective_listing_age_days`. Candidate and
comparables share the created-at basis, so the percentile is meaningful.

## Reframed risk wording

### EN — data-conditional relativity (false / fresh case, Example F)
> "Listing has been live for 64 days; comparable-listing age context does not
> flag it as older than peers, so this is a routine freshness note rather than
> a market-clearing concern."

### EN — old vs peers, LOW zone (diligence caveat, Example C)
> "Listing has been live for 102 days — longer than comparable listings in this
> district, yet the ask already sits below about 72% of peers; pressure-test why
> a below-market corner has not cleared (access, latent fit-out, landlord terms)
> before reading the long vacancy as soft pricing."

### EN — old vs peers, HIGH zone (leverage compounds over-priced finding, Example D)
> "Listing has been on market for 147 days — longer than comparable listings,
> and at an ask already more expensive than about 88% of peers the market has
> plainly not cleared; this compounds the over-priced finding and may hand a
> tenant real negotiating leverage, but only if the structural mispricing can be
> closed."

### AR — old vs peers, LOW zone (Example AR-1)
> "الإعلان منشور منذ 102 يوماً — أطول من المعتاد مقارنةً بالإعلانات المماثلة في
> هذا الحي، ومع ذلك فإن السعر المطلوب أقل من حوالي 72% من النظراء؛ تحقّق من سبب
> عدم تأجير وحدة أقل من السوق (الوصول، تجهيزات خفية، شروط المالك) قبل اعتبار
> الشغور الطويل تسعيراً ليّناً، فقد يمنح ذلك قوة تفاوضية للمستأجر إن لم يكن هناك
> سبب هيكلي."

### New AR terms (for Ahmed's byte-check)
- "negotiating leverage" (tenant-side) → **قوة تفاوضية للمستأجر**
- "longer than comparable listings / longer than peers" →
  **أطول من المعتاد مقارنةً بالإعلانات المماثلة**
- Reused existing tokens: "<N> يوماً", "عمر الإعلان (تاريخ النشر)".
- Age percentile is INTERNAL — never spoken, no new percentile unit token.

## Validation psql — find a high-age-percentile candidate for the screenshot

```sql
-- High relative-age candidate (old vs peers) in percentile mode, with a
-- known rent zone so the zone-conditioned leverage line can be screenshotted.
SELECT
    ec.id,
    ec.parcel_id,
    ec.district,
    (ec.feature_snapshot_json->'listing_age'->>'age_percentile')::float  AS age_percentile,
    (ec.feature_snapshot_json->'listing_age'->>'n_comparables')::int       AS n_comparables,
    (ec.feature_snapshot_json->'listing_age'->>'created_days')::int        AS created_days,
    (ec.score_breakdown_json->'economics_detail'->'rent_burden'->>'percentile')::float AS rent_percentile,
    (ec.score_breakdown_json->'economics_detail'->'rent_burden'->>'source_label')      AS rent_scope
FROM expansion_candidate ec
WHERE ec.feature_snapshot_json->'listing_age'->>'age_percentile' IS NOT NULL
  AND (ec.feature_snapshot_json->'listing_age'->>'age_percentile')::float >= 0.75
  AND ec.score_breakdown_json->'economics_detail'->'rent_burden'->>'mode' = 'percentile'
ORDER BY age_percentile DESC, n_comparables DESC
LIMIT 20;
```

For a MID-zone screenshot (cleanest leverage line) add to the WHERE:
`AND (ec.score_breakdown_json->'economics_detail'->'rent_burden'->>'percentile')::float BETWEEN 0.40 AND 0.60`.
For a LOW-zone diligence-caveat screenshot use `< 0.40`; for HIGH use `> 0.60`.

## Test results

- New backend tests: 6 passed.
- New memo tests + version pin: 9 passed.
- `test_llm_decision_memo.py`: 104 → 117 passed.
- `test_pr4a_arabic_structured_memo.py` + `test_pr4c_arabic_key_evidence.py`:
  46 passed (EN head fixture re-asserted; AR canon intact).
- Full suite: 2361 passed, 24 skipped, 1 pre-existing concurrency flake
  (`test_prewarm_concurrency.py::test_prewarm_per_candidate_exception_does_not_crash_batch`,
  passes in isolation; unrelated to this PR).

## Risk / tradeoff

Low risk, additive. New JSONB keys on an already-whitelisted dict; no schema
change. Scoring/rankings are byte-unchanged (the percentile is display/memo
only, never fed to a score). The prompt reframe replaces an invented
peer-relative hedge with a data-conditional one — strictly more honest.
