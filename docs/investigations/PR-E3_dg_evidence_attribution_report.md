# PR-E3 — Tighten the dg-evidence detector: value-only matches require engine attribution

**Branch:** `claude/dg-evidence-engine-attribution-6wbnja`
**Status:** Committed + pushed. **No merge** — awaiting Ahmed's review.
**Scope:** Single purpose — the detector function `_dg_evidence_invalid_reason` and its tests.
**Prompt impact:** None. System prompt bytes verified unchanged; **no `MEMO_PROMPT_VERSION` bump**, no fixture regen.

---

## 1. The bug (confirmed structural in v12.1)

`_dg_evidence_invalid_reason` (`app/services/llm_decision_memo.py:2716`) treated a bare
`f"{composite_rounded}/100"` substring in **any** `key_evidence` row's signal/value as
proof that the demand-generator composite was evidenced.

Failure chain:

1. Composite `59.82` rounds to `60` → detector needle becomes `"60/100"`.
2. Any other composite-dominated score rendered at the same rounded value collides with
   that needle. `dine_in` `demand_score` is composite-dominated, so these collisions are
   **correlated, not rare**.
3. A row like `{"signal": "Demand Strength", "value": "60/100"}` satisfied the needle even
   though it never attributed the number to the generator engine.
4. The first LLM response was accepted as compliant → **no corrective retry, no
   deterministic injection, no `dg-evidence rejected` log line**.

The test at `tests/test_llm_decision_memo.py:2427-2434` pinned this false-accept as
*intended* behavior — i.e. the test encoded the spec bug.

This is the **parcel-6706340** shape from production.

---

## 2. The fix

In `_dg_evidence_invalid_reason`:

- **Unchanged:** the two phrase needles — the EN signal phrase (`_DG_COMPOSITE_SIGNAL_EN`,
  `"demand-generator composite"`) and the AR Rule-7 term (`_DG_COMPOSITE_SIGNAL_AR`,
  `"مركب مولدات الطلب"`) — still matched against the lowercased `"{signal} {value}"`
  haystack exactly as before.
- **Tightened:** a value-only `"<composite>/100"` match now counts as compliance **only
  when the same row's signal attributes the number to the generator engine** — i.e. the
  lowercased signal contains `"generator"` or `"مولدات"`.
- Bare `"<N>/100"` with no generator attribution no longer satisfies the mandate.

Everything downstream — the gate, the one-retry corrective loop, the deterministic
injector, and the logging — inherits the new behavior through this shared detector. No
other code was touched. The function's **return-string contract is identical** (same
reason string, same `None`-when-compliant semantics).

### Source diff

```diff
diff --git a/app/services/llm_decision_memo.py b/app/services/llm_decision_memo.py
@@ -2719,22 +2719,26 @@ def _dg_evidence_invalid_reason(
     """Return a short reason string when no key_evidence row in ``parsed``
     references the demand-generator composite; None when compliant.

-    A row matches when its signal or value mentions the EN signal phrase,
-    the AR Rule-7 term, or a "/100" value equal to the rounded composite."""
-    needles = (
-        _DG_COMPOSITE_SIGNAL_EN,
-        _DG_COMPOSITE_SIGNAL_AR,
-        f"{composite_rounded}/100",
-    )
+    A row matches when its signal or value mentions the EN signal phrase or
+    the AR Rule-7 term. A bare ``"<composite>/100"`` value counts ONLY when
+    the same row's signal attributes the number to the generator engine
+    (signal contains "generator" or "مولدات"); without that attribution the
+    value collides with composite-dominated scores rendered at the same
+    rounded value (e.g. dine_in demand_score) and must not be accepted."""
+    phrase_needles = (_DG_COMPOSITE_SIGNAL_EN, _DG_COMPOSITE_SIGNAL_AR)
+    value_needle = f"{composite_rounded}/100"
     rows = parsed.get("key_evidence")
     if isinstance(rows, list):
         for row in rows:
             if not isinstance(row, dict):
                 continue
-            haystack = (
-                f"{row.get('signal') or ''} {row.get('value') or ''}".lower()
-            )
-            if any(n.lower() in haystack for n in needles):
+            signal = (row.get("signal") or "").lower()
+            haystack = f"{signal} {(row.get('value') or '').lower()}"
+            if any(n.lower() in haystack for n in phrase_needles):
+                return None
+            if value_needle in haystack and (
+                "generator" in signal or "مولدات" in signal
+            ):
                 return None
     return (
         "demand_score_source is dg_index but key_evidence has no "
```

---

## 3. Diff stat

```
 app/services/llm_decision_memo.py | 26 +++++-----   (detector only; net +4 lines)
 tests/test_llm_decision_memo.py   | 99 +++++++++++++++++++++++++++++--
 2 files changed, 109 insertions(+), 16 deletions(-)
```

---

## 4. Test deltas (`tests/test_llm_decision_memo.py`)

| Test | Change | Asserts |
|------|--------|---------|
| `test_other_score_value_does_not_match` (`:2427`) | **Flipped pin** | `80/100` `final_score` row vs composite 80 now returns a **non-None** reason (no generator attribution). |
| `test_compliant_by_value_match_alone` | **Removed / replaced** | Old pin that value-only match alone = compliant (the spec bug). |
| `test_value_match_requires_generator_attribution_en` | **Added** | EN signal containing `"generator"` + `74/100` ⇒ compliant. |
| `test_value_match_requires_generator_attribution_ar` | **Added** | AR signal containing `"مولدات"` + `74/100` ⇒ compliant. |
| `test_value_match_without_generator_attribution_is_invalid` | **Added** | Bare `74/100`, signal `"demand composite"` ⇒ invalid. |
| `test_production_collision_demand_strength_row_is_invalid` | **Added** | Composite 60 + `{"signal": "Demand Strength", "value": "60/100"}` ⇒ invalid (parcel-6706340 shape). |
| `test_colliding_value_row_still_retries_then_injects` | **Added (e2e)** | New `_DG_COMPOSITE_60_CANDIDATE` (composite 59.82 → 60): first response carries only the colliding `60/100` row ⇒ retry fires (`call_count == 2`); retry still non-compliant ⇒ deterministic injection at index 1 with `source == "deterministic_injection"`. |
| `test_compliant_by_en_signal_phrase`, `test_compliant_by_ar_signal_term` | **Unchanged** | Phrase-needle matches still compliant. |
| All existing retry / injection tests | **Unchanged** | Pass as-is. |

New test fixture added:

```python
# Composite rounds to 60 — the production collision shape: a non-generator
# score (e.g. dine_in demand_score) can also render "60/100".
_DG_COMPOSITE_60_CANDIDATE = {
    **_RANK1_ALL_PASS_CANDIDATE,
    "id": "dg-comp60",
    "parcel_id": "dg-comp60",
    "feature_snapshot_json": {
        **_RANK1_ALL_PASS_CANDIDATE["feature_snapshot_json"],
        **_DG_INDEX_SNAPSHOT_FIELDS,
        "demand_generator_index": {
            **_DG_INDEX_SNAPSHOT_FIELDS["demand_generator_index"],
            "composite_0_100": 59.82,
        },
    },
}
```

---

## 5. Validation

| Check | Result |
|-------|--------|
| `pytest tests/test_llm_decision_memo.py -k "Dg or dg"` | **35 passed** |
| `pytest tests/test_llm_decision_memo.py` (full file) | **100 passed, 4 failed** |
| The 4 failures | Pre-existing **import errors** in the FastAPI endpoint-integration tests (missing `shapefile` / `pandas` / etc. in this container). Confirmed identical on baseline via `git stash` → unrelated to this change. |
| System prompt bytes | **Unchanged.** `sha256(_compose_structured_system_prompt("en"))` and `"ar"` are byte-identical before and after the change. |
| `MEMO_PROMPT_VERSION` | Still `v12.1-demand-evidence-enforced-2026-06` (no bump). |

System-prompt hash evidence (identical before and after the edit):

```
en  de9e4adc8e482eea9fc6a38d1f2da2eb2313767c423927b71ddf7194edfc8de1
ar  8be3acd87dc10d4237949715307a8a4de90d493ac4d8bd6f7c388f2e771f0403
```

---

## 6. Risk & tradeoffs

- **Risk: low.** Single-function change with an identical return contract; all downstream
  consumers (gate / retry / injector / logging) are untouched and inherit the behavior.
- **Behavioral tightening, not loosening.** The detector now rejects more first responses
  (the false-accept path), which can only *increase* corrective retries / injections — it
  never suppresses a previously-firing safeguard.
- **Compliant memos already carrying the EN/AR phrase row are unaffected** (phrase needles
  unchanged), and the deterministic injection row itself uses the phrase signal, so the
  injected row always satisfies the detector (no infinite-loop / re-rejection risk).
- No prompt, schema, or API-contract changes ⇒ no frontend/backend drift.

---

## 7. Explicitly OUT of scope (parked as separate decisions)

- The render-window pin (a compliant row at index ≥ 4 is invisible in the UI).
- The dead persistence plumbing (`search_id` missing from the drawer POST; pre-warm
  disabled).
- The "9 components" preamble drift.

---

## 8. Validation steps for Ahmed

Re-open the drawer for **parcel 6706340** (or any `dg_index` candidate whose rounded
composite collides with another rendered score):

1. The memo must now carry a **generator-attributed composite row** — either retry-authored
   by the LLM or deterministically injected.
2. The `kubectl` log grep for that request should show **either**:
   - `dg-evidence rejected` (the corrective retry path fired), **or**
   - the deterministic injection line (retry still non-compliant → row injected at index 1).

---

## 9. Merge recommendation

**Recommend merge — risk level: low.** Targeted, single-purpose fix; contract-preserving;
prompt bytes verified unchanged; tests flip the false-accept pin and add the production
regression plus an end-to-end retry→injection flow.
