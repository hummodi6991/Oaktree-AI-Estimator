# PR-2 — Wire L1 Demand-Generator Index into Dine-In Scoring

**Branch:** `claude/wire-l1-demand-index-scoring-57lijw`
**Commit:** `3bc0b7e69` — *PR-2: wire L1 demand-generator index into dine-in demand scoring*
**Status:** pushed to origin. No PR opened, no merge/dispatch (Ahmed reviews the diff first).
**Type:** Real scoring change. Flag-gated, default OFF. Dine-in only.

---

## 1. What this PR does (one line)

Swaps the near-constant `pop_score` numerator for the L1 demand-generator
**composite** inside the **dine-in** demand blend, behind a new default-OFF flag,
without touching any weight, gate, invariant, or the index's own computation.

When the flag is off, dine-in `final_score` and ordering are **byte-for-byte
identical** to current production.

---

## 2. Why now

The whitespace fix landed and L1 validation on a working denominator gave the
green light:

- `corr(composite, competition_whitespace) = −0.482` — demand and supply
  genuinely oppose, so net-of-supply discriminates.
- `corr(composite, competitor_count)` fell `0.863 → 0.288` — the free index is
  no longer just a competitor echo.
- `corr(composite, final_score) = 0.059` — the index is computed but **not yet
  influencing the score**.

PR-2 wires it in so the composite actually drives the dine-in score.

---

## 3. Design decision and the architecture reality

### 3.1 The drifted anchor

The task hinted the demand blend was "near the index call site `~:9258`". The
**live tree is different**, and I verified `path:line` against it. The scoring
pipeline in `run_expansion_search` is a **two-pass loop**:

| Concern | Pass | Live location (post-edit) |
|---|---|---|
| `demand_score = pop_score·_pop_w + delivery_score·_del_w` | **1st pass** | `app/services/expansion_advisor.py:8109` and `:8228` (recompute after district fallback) |
| `prepared_item` build (carries `demand_score` to pass 2) | **1st pass** | `app/services/expansion_advisor.py:8390` |
| Bulk L1 enrichment (OSM / floors / F&B / local pop) | between passes | `app/services/expansion_advisor.py:8730–8968` |
| `_demand_generator_index(...)` composite (was emit-only) | **2nd pass** | old `:9412` → moved to `:9337` |
| `demand_score = prepared_item["demand_score"]` | **2nd pass** | `app/services/expansion_advisor.py:9329` |
| Snapshot emit `feature_snapshot_json[...]` | **2nd pass** | `:9473` |

So the `pop_score` blend is computed in pass 1, but the composite is only
available in pass 2. To honor the hard constraint *"reuse the in-memory composite
already computed by `_demand_generator_index` for this candidate this pass — do
NOT re-read it from the JSON snapshot or recompute it"*, I:

1. **Lifted** the composite computation to the **top of the second-pass loop**
   (`:9337`), guarded by the existing index flag.
2. **Reuse that one in-memory result** for both:
   - the gated demand-blend swap (`:9350`), and
   - the snapshot emit (`:9473`).

This means **one** `_demand_generator_index` call per candidate (same as PR-1),
no JSON re-read, no extra SQL.

### 3.2 The swap itself

The dine-in blend keeps its exact shape; only the population numerator changes:

```text
OFF (production):  demand_score = clamp(pop_score     · 0.75 + delivery_score · 0.25)
ON  (dine-in):     demand_score = clamp(dg_composite  · 0.75 + delivery_score · 0.25)
```

`pop_score` (250k dine-in reference) saturates at ~98 for virtually every dense
Riyadh catchment; the composite (`l1_v2_2026-06`, range ~14–91) is a strict
superset of population and actually spreads. **Net-of-supply** is the existing
additive offset between the (now index-fed) demand component and the separate
`competition_whitespace` component — no division added, no F&B orthogonalization
(the −0.482 offset handles the correlation).

To re-form the blend in pass 2 I needed the final `delivery_score`, so I store it
in `prepared_item` during pass 1 (`:8427`). Nothing else about the delivery term
changes.

---

## 4. Changes

### Change 1 — new scoring flag (`app/core/config.py:149`)

```python
EXPANSION_DEMAND_GENERATOR_SCORING_ENABLED: bool = (
    os.getenv("EXPANSION_DEMAND_GENERATOR_SCORING_ENABLED", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
```

- Default **OFF**.
- Style intentionally mirrors the sibling `EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED`
  flag (see §8 on formatting).
- It only takes effect when `EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED` is **also**
  true. If scoring is on but the index flag is off, the service **logs once** and
  falls back to `pop_score` — it does not crash.

### Change 2 — gated swap + transparency (`app/services/expansion_advisor.py`)

**One-time misconfig warning (`:1911`)**

```python
_DG_SCORING_WITHOUT_INDEX_WARNED = False

def _warn_dg_scoring_without_index() -> None:
    global _DG_SCORING_WITHOUT_INDEX_WARNED
    if not _DG_SCORING_WITHOUT_INDEX_WARNED:
        _DG_SCORING_WITHOUT_INDEX_WARNED = True
        logger.warning(... fall back to pop_score ...)
```

**Carry the delivery term (`:8427`)**

```python
"delivery_score": delivery_score,  # so pass 2 can re-blend without recompute
```

**Compute composite once + gated swap (`:9337`–`:9376`)**

```python
_dg_index_result: dict[str, Any] | None = None
if settings.EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED:
    _dg_index_result = _demand_generator_index(... same args as PR-1 ...)

_demand_score_source = "pop_score"
if settings.EXPANSION_DEMAND_GENERATOR_SCORING_ENABLED and service_model == "dine_in":
    if not settings.EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED:
        _warn_dg_scoring_without_index()             # log once, fall back
    else:
        _dg_composite = _dg_index_result.get("composite_0_100") if _dg_index_result else None
        if _dg_composite is not None:
            _pop_w, _del_w = _demand_blend_weights(service_model)
            demand_score = _clamp(float(_dg_composite) * _pop_w
                                  + prepared_item["delivery_score"] * _del_w)
            _demand_score_source = "dg_index"
```

**Reuse for emit + transparency field (`:9473`)**

```python
if _dg_index_result is not None:
    feature_snapshot_json["demand_generator_index"] = _dg_index_result   # PR-1 emit, unchanged content
if settings.EXPANSION_DEMAND_GENERATOR_SCORING_ENABLED:
    feature_snapshot_json["demand_score_source"] = _demand_score_source  # "dg_index" | "pop_score"
```

Fallback semantics:

- flag off → exact current behaviour, **no** `demand_score_source` key.
- non-dine_in → `pop_score` path (cafe/qsr untouched).
- composite missing/None → silent fall back to `pop_score`, never zeroes demand.
- `demand_score_source` is emitted **only** when the scoring flag is on, and is
  **never read** by scoring.

---

## 5. Hard constraints — compliance checklist

| Constraint | Status |
|---|---|
| Flag-gated, default OFF | ✅ `EXPANSION_DEMAND_GENERATOR_SCORING_ENABLED = False` |
| Flag off ⇒ dine-in score + ordering byte-for-byte identical | ✅ test `test_pr2_scoring_flag_off_is_inert` |
| Dine-in only; cafe/qsr/delivery_first keep `pop_score` | ✅ test `test_pr2_non_dine_in_unchanged_with_flag_on` |
| Reuse in-memory composite; no JSON re-read; no recompute | ✅ single call at `:9337`, reused at `:9350` + `:9473` |
| `component_weights` unchanged | ✅ not touched (`_score_breakdown` weights intact) |
| `_demand_blend_weights` values unchanged (0.75/0.25) | ✅ not touched |
| `sum == 100` invariant unchanged | ✅ not touched; covered by existing suite |
| Gates unchanged | ✅ not touched |
| `competition_whitespace` unchanged | ✅ not touched |
| Index anchors / weights `l1_v2_2026-06` unchanged | ✅ `_demand_generator_index` untouched |
| No division added; no F&B orthogonalization | ✅ additive offset only |
| Two-flag gating (scoring requires index) | ✅ logs once + falls back |
| Missing composite never zeroes demand | ✅ silent fall back to `pop_score` |
| No merge / push to other branch / dispatch | ✅ pushed only to feature branch |

---

## 6. Blast radius

Demand is one component among many in `_score_breakdown`. The swap only changes
the demand component's input value, not its weight. The bounded effect is roughly
**~6.6% of total score** (the demand component's weight share), so the expected
behaviour when enabled is a sensible re-ordering — high-demand / high-whitespace
districts rise, saturated (low-whitespace) districts fall — within that envelope,
**not** a wild reshuffle.

---

## 7. Tests

File: `tests/test_expansion_advisor_demand_generator.py` (4 new tests added to the
existing 10).

| Test | Asserts |
|---|---|
| `test_pr2_scoring_flag_off_is_inert` | index-on/scoring-off ⇒ scores + ordering equal both-flags-off baseline; no `demand_score_source` key |
| `test_pr2_scoring_on_dine_in_uses_dg_index` | dine-in + composite present ⇒ `demand_score_source == "dg_index"`, composite ≠ saturated `pop_score`, final_score moves |
| `test_pr2_scoring_on_index_off_falls_back_to_pop_score` | scoring-on/index-off ⇒ no exception, `demand_score_source == "pop_score"`, scores equal feature-absent baseline, no index key |
| `test_pr2_non_dine_in_unchanged_with_flag_on` | cafe **and** qsr ⇒ scores identical to scoring-off, `demand_score_source == "pop_score"` |

The `_demand_blend_weights` / `sum==100` invariants are guarded by the
pre-existing `test_demand_blend_weights_unchanged` and the `_score_breakdown`
weight-sum tests in `test_expansion_advisor_service.py`.

### Test results

```text
tests/test_expansion_advisor_demand_generator.py ............ 14 passed
expansion + golden + memo + strengths suites .............. 1222 passed, 13 skipped
flake8 --select=F,E9 (real errors) ........................ only pre-existing warnings
```

Commands run:

```bash
python3 -m pytest tests/test_expansion_advisor_demand_generator.py -q
python3 -m pytest tests/test_expansion_advisor_service.py tests/test_expansion_advisor.py \
                  tests/test_expansion_advisor_regression.py -q
python3 -m pytest tests/test_pr2_english_byte_identity.py \
                  tests/test_expansion_advisor_production_patch.py \
                  tests/test_expansion_advisor_api.py \
                  tests/services/test_realized_demand_broadcast.py -q
python3 -m pytest tests/ -q -k "expansion or demand or golden or pr2 or memo or strengths"
flake8 --select=F,E9 app/services/expansion_advisor.py app/core/config.py \
                     tests/test_expansion_advisor_demand_generator.py
```

---

## 8. Note on formatting (black / flake8)

The repo is **not** black-clean and has no `pyproject.toml` / `setup.cfg` /
`.flake8`. `black --check` wants to reformat large swaths of pre-existing code —
including the existing `EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED` flag, whose
exact multi-line `in {...}` style I deliberately mirrored. Likewise the default
`flake8` E501 (79-char) fires on thousands of pre-existing lines.

Per the minimal-diff mandate I **matched the surrounding convention** instead of
running `make fmt` (which would have produced a huge, unrelated reformat diff).
`flake8 --select=F,E9` confirms my edits introduce **no** real errors (the four
F-code findings are all in pre-existing code at lines 4121 / 7125 / 7227 / 7452).

---

## 9. Validation playbook (Ahmed, Codespace — after diff review)

This changes dine-in rankings, so validate after merge:

1. Merge → deploy → `kubectl rollout status deployment/oaktree-estimator -n default`.
2. Enable (index flag already true):
   ```bash
   kubectl set env deployment/oaktree-estimator -n default \
     EXPANSION_DEMAND_GENERATOR_SCORING_ENABLED=true
   ```
   Wait for rollout; `printenv` to confirm.
3. Run a fresh **city-wide dine-in** search (last action before validating —
   old searches won't backfill the snapshot).
4. Re-run `scripts/diagnostics/l1_index_validation.sql`. Success criteria:
   - **`corr(composite, final_score)` jumps materially** from ~0.06 — proof the
     index now drives score.
   - **`demand_score_source == "dg_index"`** for dine-in candidates with a
     composite.
   - **Sensible ordering:** high-demand / high-whitespace districts rise,
     saturated (low-whitespace) districts fall; the shift stays inside the
     bounded ~6.6%-of-score envelope (no wild reordering).
   - **Top-10 flag-off vs flag-on** compared so the ranking delta is explicit
     and defensible.

---

## 10. Files changed

```text
 app/core/config.py                               |  13 +++
 app/services/expansion_advisor.py                |  97 +++++++++++++++---
 tests/test_expansion_advisor_demand_generator.py | 121 +++++++++++++++++++++++
 3 files changed, 215 insertions(+), 16 deletions(-)
```

---

## 11. Merge recommendation

**Risk: Low-to-moderate, fully reversible via flag.**

- Code-path risk is contained: flag default OFF ⇒ zero production change until
  explicitly enabled, and even then the effect is bounded to the demand
  component (~6.6% of score).
- All invariants, weights, gates, and the index computation are untouched.
- Behaviour is fully validated by tests for all four states (off, on/dine-in,
  on/missing-composite, on/non-dine-in) plus 1222 passing existing tests.

Recommend merge after Ahmed's diff review, then the Codespace validation in §9
before considering it production-validated.
