# Patch — Finding 4: Block fabricated competitor economics in the decision memo

**Branch:** `claude/finding4-memo-competitor-economics-guardrail-Gay3E` (pushed, not merged, no PR)
**Commit:** `acc055a28`
**Status:** ⛔ STOP for Ahmed's review. Prompt-text + version-constant change only. Riyadh-only. No scoring math, no API contract change.

---

## 1. What is wrong now

`comparable_competitors` reaches the memo LLM as only `{id, name, score}` (see `_build_*` →
`MemoNamedCompetitor`, surfaced at `app/services/llm_decision_memo.py:1251`). No rent, revenue,
sales, traffic, or any financial/operational figure is ever sent.

Yet the prompt's HARD RULE (`:1458`) requires the `comparison` field to name a competitor and to "be
specific about what beats what," and a separate rule requires citing numbers with units. The only
relevant guard was a general "do not invent facts" line — there was **no explicit ban on attaching a
financial/operational metric to a named competitor**, leaving room for plausible-sounding
fabrications (e.g. "outsells X", "X pays SAR Y/yr").

## 2. Why it happens

The prompt simultaneously:

- (a) demands specificity in the competitor comparison,
- (b) demands unit-bearing numbers, and
- (c) never tells the model which competitor facts it actually possesses.

The model fills the gap (c) with invented economics to satisfy (a) and (b).

## 3. Smallest safe fix

Three edits, all in `app/services/llm_decision_memo.py`, all inside the shared
`_STRUCTURED_MEMO_PREAMBLE` — so EN **and** AR both inherit the rule. (Confirmed
`_compose_structured_system_prompt(locale)` returns `preamble + "\n\n" + critical_block`, and the AR
test asserts the preamble half is shared verbatim: `ar.split("══════")[0] == en.split("══════")[0]`.)

1. **Tightened the `comparable_competitors` field description** — states each entry has ONLY a name +
   internal quality score (0–100); no financial/operational figure is known.
2. **New HARD RULE** — inserted immediately after the existing `Comparison MUST reference…` rule,
   forbidding any financial/operational figure attributed to a named competitor; the comparison may
   use only name, category, proximity/presence, and the provided quality score.
3. **`MEMO_PROMPT_VERSION` bumped** `v9-lay-friendly-percentile-2026-05` →
   `v10-competitor-econ-guardrail-2026-06`, so cached memos regenerate lazily on next view.

The EN byte-identity fixture (`tests/data/pr4a_structured_memo_system_prompt_en_head.txt`, the
reference for the `STRUCTURED_MEMO_SYSTEM_PROMPT == head` checks in `test_pr4a_*` and `test_pr4c_*`)
was regenerated from the new EN composition so the byte-identity guard reflects the intended new
text. No test pins the version string literally.

### Unified diff — `app/services/llm_decision_memo.py`

```diff
@@ -49,7 +49,7 @@ TEMPERATURE = 0.3
 # Bumped whenever STRUCTURED_MEMO_SYSTEM_PROMPT changes meaningfully.
 # Cached memos with a different version are treated as cache-miss and
 # regenerated lazily on next view.
-MEMO_PROMPT_VERSION = "v9-lay-friendly-percentile-2026-05"
+MEMO_PROMPT_VERSION = "v10-competitor-econ-guardrail-2026-06"
 
@@ -1414,7 +1414,9 @@ Competitive landscape:
 - brand_presence.top_chains: named chains within 500m with branch_count and
   nearest_distance_m. Use display_name_en in English memos and display_name_ar
   in Arabic memos.
-- comparable_competitors: rated peer restaurants for the operator's brand.
+- comparable_competitors: rated peer restaurants for the operator's brand. Each entry has ONLY a
+  name and an internal quality score (0-100). No rent, revenue, sales, turnover, foot-traffic, order
+  volume, payback, or any financial/operational figure is known for these competitors.
 - next_candidate_summary: the rank-2 site in this search, for explicit
   alternative comparison. Reference it by rank in the comparison field.
 
@@ -1456,6 +1458,13 @@ HARD RULES:
 - Comparison MUST reference at least one named competitor from comparable_competitors. [...] A comparison that names neither a real competitor nor a real rank-2 candidate is a defect.
+- NEVER attribute any financial or operational figure to a named competitor. You are given only a
+  competitor's name and an internal quality score — you do NOT know their rent, revenue, sales,
+  turnover, average check, foot traffic, order volume, payback, number of seats, or staff. Do NOT
+  state, estimate, imply, or compare any such figure for a named competitor (e.g. "outsells X",
+  "X pays more rent", "higher revenue than Y"). The comparison may reference a competitor only by
+  name, category, proximity/presence, and the provided quality score. Any number attached to a
+  named competitor that is not its provided score is a hard error.
 - Banned openers: 'Overall,', 'Generally speaking,', 'It appears that', 'consider due to', 'This candidate could potentially'.
```

The same two prose blocks are mirrored byte-for-byte into
`tests/data/pr4a_structured_memo_system_prompt_en_head.txt` (the regenerated EN byte-identity
snapshot).

### Files changed

```
 app/services/llm_decision_memo.py                         | 13 +++++++++++--
 tests/data/pr4a_structured_memo_system_prompt_en_head.txt | 11 ++++++++++-
 2 files changed, 21 insertions(+), 3 deletions(-)
```

## 4. Validation

### 4.1 Tests

Full `make test` is blocked in this sandbox only by unrelated optional dependencies during pytest
collection (e.g. ingestion/geo modules) — not by any of the touched files. Targeted runs:

| Selection | Result |
| --- | --- |
| `tests/test_pr4a_arabic_structured_memo.py`, `tests/test_pr4c_arabic_key_evidence.py`, `tests/services/test_llm_decision_memo_grounding.py` | **54 passed, 5 skipped** |
| `tests/test_llm_decision_memo.py` + `tests/test_pr2_english_byte_identity.py` (`-k "memo or decision or prompt or byte"`) | **316 passed** |

- EN byte-identity (`_compose_structured_system_prompt("en") == head` and
  `STRUCTURED_MEMO_SYSTEM_PROMPT == head`) passes against the regenerated snapshot.
- AR composition still differs from EN, carries the Arabic headline triad, and shares the preamble
  verbatim — so it inherits the new rule.
- No test pins `MEMO_PROMPT_VERSION`. `RERANK_PROMPT_VERSION` (in `app/services/expansion_rerank.py`)
  is independent and untouched.

### 4.2 Rule present in both locales (direct check)

```
version          : v10-competitor-econ-guardrail-2026-06
EN has rule      : True      AR has rule      : True
EN has desc      : True      AR has desc      : True
EN == constant   : True      shared preamble  : True
```

### 4.3 Live memo regeneration — NOT RUN ⚠️

This sandbox has no OpenAI key and no production PostGIS/DB, so a fresh QSR/Burger Riyadh search +
live LLM call could not be exercised here. **Ahmed should run this in a credentialed environment
before merge.**

The behavior the new rule enforces is already modeled by the prompt's own worked examples, which
carry a named competitor but **zero** competitor economics — e.g. the in-prompt golden `comparison`:

> "Peer Chain A operates 2 branches within 180 m and Peer Chain B holds a single branch at 320 m of
> this site — established category demand at this corner, not a greenfield. Against rank 2 in this
> search, this site pulls ahead on rent positioning (cheaper than ~72% vs cheaper than ~53%) and
> access/visibility (82/100 vs 71/100); rank 2 carries a marginally larger footprint but no
> comparable corner exposure."

Every number there belongs to *this site* or the rank-2 candidate — never to Peer Chain A/B. That is
exactly the contract the new rule hardens.

## 5. Risk / tradeoff

- **Low risk.** Prompt text + one cache-busting constant only; no code paths, payload builders,
  scoring, or API fields touched.
- The version bump invalidates cached memos → they regenerate lazily on next view (intended, mild
  one-time LLM cost).
- The new rule is additive and consistent with the existing competitor-fabrication ban already
  present in the CRITICAL tail (`:1640–1645`); no contradiction introduced.

## 6. Merge recommendation

Ready for Ahmed's review — **do not merge yet** per the gate. Recommend running validation step 4.3
against a credentialed environment before merge. **Risk level: low.**
