# Fix: `competition_whitespace` fabrication on the unknown-confidence path

**Branch:** `claude/fix-whitespace-unknown-confidence-gjF9T`
**Status:** Pushed — not merged (awaiting review approval)
**Risk:** Low — corrects one scoring input; weights and `final_score` math untouched.

## Summary

When a candidate had **zero observed competitors**, the whitespace score only
deserved the wide-open `100` on a *confirmed* empty market. The ArcGIS-fallback
candidate pool bypasses bulk competitor enrichment and passes `confident=None`,
which previously fell through to the legacy `100` branch — scoring *"we don't
know"* as *"we know it's empty"* and pushing thin-coverage candidates up the
ranking. This was a fabrication of evidence, independent of how often it fired.

The fix makes **unknown confidence behave conservatively** (neutral `~50`),
while genuinely-confirmed-empty markets still earn `100` on real evidence.

## Step 1 — Verification

### Q1. Parameter meaning

Signature (`app/services/expansion_advisor.py:2340`):

```python
def _competition_whitespace_score(competitor_count: int, *, confident: bool | None = None) -> float:
```

Three states of `confident` (producer: `_bulk_enrich_competitors`,
`:6326` / `:6467` → `"confident": int(r["broader_count"]) > 0`):

| State   | Meaning |
|---------|---------|
| `True`  | Scan observed **broader** POI/delivery presence in radius → a zero same-category count is trustworthy evidence of a real greenfield. |
| `False` | Scan ran but both tables returned zero rows (thin POI coverage) → zero can't be trusted. |
| `None`  | No flag supplied: row bypassed bulk enrichment (ArcGIS-fallback pool path, comment at `:7649-7651`). |

### Q2. Genuine confirmed-zero path?

Yes — `confident=True` with `competitor_count=0` (broader presence observed,
none in category). **Already handled correctly:** `_bulk_enrich_competitors`
sets `confident=True`, and both enrichment call sites (`:7113` candidate-location
path, `:7157` commercial_unit path) propagate the real bool. **No caller needed
an explicit `confident=True` added.**

### Q3. Is `None→100` reachable on the candidate/listings path?

`None` is **not** tied to a specific product surface — it survives for any row
that bypasses bulk enrichment (missing lat/lon, or the ArcGIS-fallback pool SQL
that never sets the key). The single call site (`:7720` — grep confirms no other
callers) passes `confident=competitor_count_confident`, which is `None`
(`:7652-7655`) whenever the row wasn't enriched. So unknown-data rows were scored
as confirmed-empty markets. **This is the fabrication — confirmed, not
intentional → patched.**

## Step 2 — The fix

`app/services/expansion_advisor.py:2369`:

```diff
-    if confident is False and competitor_count <= 0:
+    if not confident and competitor_count <= 0:
         return 50.0
     if competitor_count <= 0:
         return 100.0
```

Behavior after the fix:

| Input                          | Score |
|--------------------------------|-------|
| `count=0`, `confident=None`    | `50.0` (was `100.0`) |
| `count=0`, `confident=False`   | `50.0` |
| `count=0`, `confident=True`    | `100.0` |
| `count>0` (any flag)           | unchanged log-decay |

The docstring was updated to document the unknown→neutral rule. No separate or
shadow score path was introduced.

## Step 3 — Tests

Added `test_competition_whitespace_unknown_confidence_is_neutral_not_open`
(`tests/test_expansion_advisor_service.py`), asserting on the **executed
function's returned value**:

- `count=0, confident=None` → `50.0` (not `100`)
- `count=0, confident=False` → `50.0`
- `count=0, confident=True` → `100.0`
- `count=3` across all flags → `15.0 ≤ score < 100.0`

Results:

- `tests/test_expansion_advisor_service.py` — **152 passed**
- `-k expansion` (full suite) — **684 passed, 8 skipped**
- golden memo regression (`test_sample_regression_memos.py`) — **6 passed**

## Step 4 — Confirmation

`final_score` math is **unchanged** — this only corrects one input (the `count=0`
branch of the existing `competition_whitespace` component); weights, `final_score`
composition, and all other component logic are untouched.

**Not merged.** Awaiting review approval.
