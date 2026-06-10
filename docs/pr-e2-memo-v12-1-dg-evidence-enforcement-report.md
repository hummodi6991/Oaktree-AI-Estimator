# PATCH PR-E2 — Memo v12.1: dg-index composite evidence enforcement

**Branch:** `claude/memo-v12-1-dg-index-composite-mq0suo` · **Commit:** `55dda77c0` · **Status:** pushed, no PR, no merge — awaiting Ahmed's review.

## 1. What was wrong

Two production v12 regenerations on confirmed `dg_index` candidates produced
`key_evidence` with **no demand-generator composite row** (rows were rent /
rent percentile / access / realized demand; body prose still used population
reach as demand support). The v12 prompt mandate (Market-context block + the
Example C inline caveat) is insufficient on its own. The PR-E observation
held: the deterministic pre-rendered rent phrase (copy-verbatim) complied on
both samples — **deterministic beats mandate**.

## 2. The fix — two layers, both on existing machinery

Both layers ride the existing one-retry corrective loop in
`generate_structured_memo`. No validator shape-keys, rent logic, or
whitelist entries were touched; all Rule 7/8 vocabulary is reused verbatim.

### Layer 1 — soft validation + one corrective retry

Applies only when `feature_snapshot.demand_score_source == "dg_index"` AND
`demand_generator_index.composite_0_100` is numeric (absent/malformed block
→ both layers skip, defensively).

A memo is **compliant** when any `key_evidence` row's signal or value
mentions:

- the EN signal phrase `demand-generator composite`, or
- the AR Rule-7 term `مركب مولدات الطلب`, or
- a `/100` value equal to the rounded composite (e.g. `74/100`).

A miss triggers the existing single retry. The new corrective preamble
(EN + AR variants, same `.format()` semantics as the headline preamble):

1. restates the dg_index mandate,
2. embeds the **exact required row as JSON with this candidate's composite
   value filled in** ("copy signal and value exactly; you may sharpen the
   implication"),
3. instructs that population reach is supporting context only and MUST NOT
   be presented as the demand anchor in `key_evidence` or body prose.

If the headline check failed on the same response, both corrective
preambles are concatenated into the one retry turn (existing one-retry
semantics preserved).

### Layer 2 — deterministic injection fallback

If the retry still lacks the row, it is injected server-side into the memo
JSON before persistence:

- **Position 2 (index 1), after the rent anchor.** Verified
  `DecisionMemoNarrative.tsx` renders `key_evidence.slice(0, 4)` — position
  2 sits inside the top-4 render window.
- **Marker field** `"source": "deterministic_injection"` — confirmed
  harmless: the frontend reads only signal/value/implication/polarity,
  `isValidStructuredMemo` tolerates extra keys, persistence is raw JSONB,
  and the legacy text renderer reads signal/value/implication only.
- **Idempotent** — re-running the detector before insert means it never
  double-injects.
- **Skips** when the dg block is absent, and skips the headline
  local-rewrite null-out path (empty `key_evidence` only occurs there;
  injecting a lone row would undo the null-out invariant).
- **Polarity is banded** on the composite so implication and polarity never
  contradict the rendered value: ≥ 60 → `positive`, 40–59 → `neutral`,
  < 40 → `negative`.

**Edge case handled:** a dg-only retry whose headline *regresses* (first
response had a valid headline, retry doesn't) keeps the first response and
falls through to deterministic injection — the retry can never make the
memo worse.

## 3. Exact injected row text (composite 74 example)

**EN:**

```json
{"signal": "demand-generator composite", "value": "74/100", "implication": "venue activity and trip generators are the measured demand evidence for this catchment; population reach is supporting context only", "polarity": "positive", "source": "deterministic_injection"}
```

**AR** (Latin digits per Rule 8; implication reuses the Rule-7 terms
مولدات الرحلات and عدد السكان القابلين للوصول):

```json
{"signal": "مركب مولدات الطلب", "value": "74/100", "implication": "نشاط المرافق ومولدات الرحلات هما دليل الطلب المقاس لهذا النطاق؛ عدد السكان القابلين للوصول سياق داعم فقط", "polarity": "positive", "source": "deterministic_injection"}
```

The implication template is identical across polarity bands (engine
attribution, truthful at any value — the `value` field carries the
magnitude); only the `polarity` field varies.

## 4. Version

`MEMO_PROMPT_VERSION` bumped `v12-demand-engine-evidence-2026-06` →
`v12.1-demand-evidence-enforced-2026-06` (`llm_decision_memo.py:53`). The
two non-compliant memos are cached at v12; the mismatch regenerates them on
next view. **System prompt text is unchanged**, so the pinned prompt-head
fixture `tests/data/pr4a_structured_memo_system_prompt_en_head.txt` needed
no regeneration — the byte-identity test still passes.

## 5. Diff stat

```
app/services/llm_decision_memo.py | 276 ++++++++++++++++++++++++++--
tests/test_llm_decision_memo.py   | 378 ++++++++++++++++++++++++++++++++++++++
2 files changed, 644 insertions(+), 12 deletions(-)
```

## 6. path:line inventory (`app/services/llm_decision_memo.py`)

| Location | Change |
|---|---|
| `:53` | `MEMO_PROMPT_VERSION = "v12.1-demand-evidence-enforced-2026-06"` |
| `:2661–2671` | v12.1 section header comment (evidence + design) |
| `:2673–2674` | `_DG_COMPOSITE_SIGNAL_EN` / `_DG_COMPOSITE_SIGNAL_AR` (Rule-7 terms, verbatim) |
| `:2681–2692` | `_DG_INJECTED_IMPLICATION_EN` / `_AR` templates |
| `:2695` | `_dg_required_composite` — mandate gating + rounded composite |
| `:2716` | `_dg_evidence_invalid_reason` — the detector |
| `:2745` | `_dg_required_evidence_row` — locale-correct row builder, banded polarity |
| `:2774` | `_inject_dg_evidence_row` — position-2 insert, idempotence, null-out guard |
| `:2806–2838` | `_DG_CORRECTIVE_RETRY_PREAMBLE_EN` / `_AR` |
| `:2841–2852` | `_dg_corrective_retry_preamble` |
| `:2933–2940` | first-pass dg detection in `generate_structured_memo` |
| `:2942–2977` | combined retry trigger + preamble composition |
| `:3009–3045` | retry re-verification (incl. regressed-headline edge) |
| `:3077–3090` | layer-2 deterministic injection before return/persistence |

Tests: `tests/test_llm_decision_memo.py:2305–2682` (new v12.1 section).

## 7. Tests

New (23 tests):

- **Gating** (`TestDgRequiredComposite`, 5): dg_index+block → 74; pop_score
  → None; absent source → None; dg_index without block → None; non-numeric
  composite → None.
- **Detector** (`TestDgEvidenceDetector`, 5): compliant by EN phrase, by AR
  term, by value-match alone → no reason; non-compliant → reason; another
  row's `80/100` score does not satisfy a composite of 74.
- **Retry** (`TestDgEvidenceRetry`, 5): missing row → retry fires and the
  LLM-authored row is kept (no marker); string-pin on the retry preamble
  (mandate, `"dg_index"`, exact row JSON with `74/100`, population-reach
  non-anchor line); pop_score never triggers; dg_index-without-block skips
  both layers; compliant first response → single call.
- **Injection fallback** (`TestDgEvidenceInjectionFallback`, 3): retry
  still missing → EN row injected at index 1 with marker, exactly once;
  AR injection uses the Rule-7 term with Latin-digit `/100`; regressed-
  headline edge keeps first response and injects.
- **Injection primitive** (`TestDgInjectionHelper`, 4): index-1 position,
  idempotence, empty-list (null-out) guard, polarity bands.
- **Version pin** (`TestMemoPromptVersionBumpedForV121`, 1).

Existing retry-loop tests: **unchanged** (the new reason is additive; all
pre-existing headline-retry tests pass untouched).

Results:

- memo suites (`test_llm_decision_memo`, `test_pr4a_arabic_structured_memo`,
  `test_pr4c_arabic_key_evidence`, `test_sample_regression_memos`):
  **152 passed**
- full backend suite: **2346 passed, 24 skipped**

Lint note: one 80-char line introduced by the patch was fixed; both touched
files were already non-black/flake8-compliant at HEAD and CI runs neither,
so surrounding style was matched rather than reformatting.

## 8. Validation for Ahmed

1. Re-open the **same two dg_index candidates**: the v12 → v12.1 prompt-
   version mismatch regenerates on view. Both must now show the composite
   row in KEY EVIDENCE — LLM-authored after one retry, or deterministic at
   position 2 (an injected row carries `"source": "deterministic_injection"`
   in `decision_memo_json`, visible via DB/telemetry, invisible in UI).
2. **Cafe memo re-check:** unchanged — pop_score candidates never trigger
   either layer (single LLM call, no injection).
3. **Arabic dg_index memo:** shows مركب مولدات الطلب with a Latin-digit
   `/100` value.

**Risk:** low. Both layers are no-ops for every non-dg_index candidate; the
worst-case added cost is one extra LLM call for non-compliant dg_index
generations (bounded by existing one-retry semantics and the daily cost
ceiling); the version bump's only cost is one-time regeneration of cached
v12 memos on next view.
