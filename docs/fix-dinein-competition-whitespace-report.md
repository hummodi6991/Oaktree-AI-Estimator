# FIX: dine-in competition radius + whitespace curve recalibration

**Branch:** `claude/dinein-competition-whitespace-ow96n9`
**Commit:** `3d35342b9`
**Status:** Pushed to origin. No PR opened — Ahmed reviews the diff (especially the curve constant) before merge.
**Type:** Real scoring change (not emit-only, not flag-gated). **Changes dine-in rankings on deploy.**

---

## 1. What is wrong now

`_competition_whitespace_score` floors at **15.00 for 97.4% of dine-in candidates**. Two
Codespace probes + investigation established the cause:

- The function is fed **same-category competitor counts over a 3000 m radius** (`dine_in`
  `"competition"` radius). At 3000 m those counts are huge: **p50 ~230, max ~339**.
- The log-decay curve `raw = 100·(1 − log1p(count)/log1p(REF))` has its domain effectively
  ending at `count = REF`, and `REF = 25` (shared across all service models). Any count ≥ 25
  hits the 15.0 floor. With p50 ~230, essentially every dine-in candidate floors.

A constant component output means `competition_whitespace` carries **zero discriminating signal**
for dine-in. This blocks PR-2 (net-of-supply) and the L2 decision, both of which depend on this
component varying across districts.

## 2. Why it happens

The radius and the curve reference were never co-calibrated for dine-in:

- The per-district radius ladder shows that at **1000 m** the same-category count collapses back
  into the curve's usable range: **p50 16, p75 24, p90 32, p95 40, p99/max 69; 76.3% ≤ 25.**
- Even at 1000 m, the dine-in **p50 (count 16) still sits at the 15.0 floor under REF=25**
  (count 16 → raw 13.0 → floored 15.0). So tightening the radius alone is necessary but not
  sufficient — the curve knee must also move, **for dine-in only**.

## 3. The fix (smallest safe change, dine-in blast radius only)

Three scoped source changes + tests. No changes to `_demand_blend_weights`, `component_weights`,
the demand-generator index, or the `sum == 100` invariant.

### Change 1 — dine-in competition radius 3000 → 1000 m

`app/services/expansion_advisor.py:817` — `_CATCHMENT_RADII_M`:

```python
# dine_in competition tightened to 1000 m: a direct-competition trade area
# distinct from the 3500 m demand catchment, so net-of-supply differencing
# spans two genuinely different scopes (same-category counts at 3000 m
# saturated the whitespace curve — p50 ~230 vs a domain ending at 25).
"dine_in":        {"demand": 3500.0, "competition": 1000.0, "provider": 3500.0},
```

- `dine_in` demand radius (3500) unchanged.
- **All** `cafe` / `qsr` / `delivery_first` values unchanged.

### Change 2 — service-model-scoped curve reference

`app/services/expansion_advisor.py` (~2537) — new named constant + signature change:

```python
_WHITESPACE_LOG_REF: dict[str, float] = {
    # ... dine_in -> 50; tunable/reviewable per model.
    "dine_in": 50.0,
}
_WHITESPACE_LOG_REF_DEFAULT: float = 25.0


def _competition_whitespace_score(
    competitor_count: int,
    *,
    confident: bool | None = None,
    service_model: str | None = None,   # NEW
) -> float:
    ...
    ref = _WHITESPACE_LOG_REF.get(
        (service_model or "").lower(), _WHITESPACE_LOG_REF_DEFAULT
    )
    raw = 100.0 * (1.0 - (math.log1p(competitor_count) / math.log1p(ref)))
    return _clamp(max(15.0, raw))
```

- The shared `25` is **NOT bumped globally** — that would shift cafe/qsr. Only `dine_in`
  resolves to `REF=50`; every other model (and unknown/`None`) keeps `25`.
- The call site at `expansion_advisor.py:8088` now passes `service_model=service_model`.
- The **15.0 floor** and the **F4 `confident`/`count<=0` → {100.0, 50.0}** logic are unchanged.

### Change 3 — docstring fix

The old docstring advertised a targets table (1→88, 8→40, 20→15) that did **not** match the
actual formula and claimed it floored at "20+" when it floors at `count = REF`. Replaced with the
real log-decay formula, the per-model reference behaviour, and a verified count→score table.

---

## Reference = 50 rationale (verified in code)

`scripts`-free verification (`python3`, exact formula):

| count    | REF=25 (old / cafe・qsr) | REF=50 (dine_in) |
|----------|--------------------------|------------------|
| 1        | 78.7                     | 82.4             |
| 6        | 40.3                     | 50.5             |
| 16 (p50) | **15.0 (floored)**       | **27.9**         |
| 24 (p75) | 15.0                     | 18.1             |
| 32 (p90) | 15.0                     | 15.0\*           |
| 40 (p95) | 15.0                     | 15.0             |
| 50       | 15.0                     | 15.0 (floor)     |

\* count 32 lands right at the knee. Because the probe under-counts vs production
alias-expansion, flooring the genuinely-saturated p90+ tail is acceptable/correct. REF=50 keeps
the p50–p75 band spread and floors only true saturation (REF=50 floors at count 50; the dense
tail ≥ ~40 floors — intended).

---

## 4. Validation steps

### Already run (this patch)

```bash
python -m pytest tests/test_expansion_advisor_service.py -k whitespace -q   # 4 passed
python -m pytest tests/test_expansion_advisor_service.py -q                 # 159 passed
python -m pytest tests/test_expansion_advisor_demand_generator.py -q        # 10 passed
```

New tests added in `tests/test_expansion_advisor_service.py`:

- `test_competition_whitespace_dine_in_reference_varies_off_floor` — dine_in whitespace
  **varies** across counts 0/6/16/24/40 (not constant 15); asserts count 16 → ~27.9 (off the
  floor), count 40 → 15.0 (floored).
- `test_competition_whitespace_cafe_qsr_reference_unchanged` — **blast-radius guard**:
  cafe / qsr / delivery_first / `None` outputs pinned **bit-for-bit** to the legacy REF=25 curve
  across counts 1/3/6/8/12/16/25/40.
- `test_competition_whitespace_f4_path_unchanged_per_model` — count 0 + confident → 100;
  count 0 + not confident / unknown → 50, across all models.

No change to `component_weights` / `_demand_blend_weights` / `sum == 100`.

### Post-deploy (Ahmed — this DOES change dine-in rankings)

1. Merge → deploy to SCCC → `kubectl rollout status deployment/oaktree-estimator -n default`.
2. Run a fresh city-wide **dine-in** search.
3. `psql -f scripts/diagnostics/whitespace_input_distribution.sql` — confirm scored
   `competitor_count` now reflects the 1000 m radius and `pct_whitespace_at_floor` drops sharply
   from 97.4%.
4. Re-run `scripts/diagnostics/l1_index_validation.sql` — confirm `competition_whitespace` now
   **varies** across districts, and read **`corr(composite, competition_whitespace)`**. This is
   the gate:
   - **strongly negative** ⇒ demand-net-of-supply discriminates (free signal suffices →
     proceed to PR-2, lean skip-L2);
   - **weak** ⇒ revisit.

---

## 5. Risk / tradeoff

- **Intended ranking shift, dine-in only.** Saturated dine-in districts that previously all read
  15.0 now spread across the p50–p75 band; the genuinely saturated ≥ ~40 tail still floors. This
  is the corrected behaviour, not a regression.
- **Blast radius contained.** cafe / qsr / delivery_first keep REF=25 and their existing radii;
  pinned by a bit-for-bit test.
- **Curve constant is the review point.** REF=50 is the tunable knee — surfaced in a named dict
  (`_WHITESPACE_LOG_REF`) precisely so Ahmed can adjust it without touching the formula.

---

## 6. Verified anchors (live tree vs brief)

| Symbol | Live `path:line` | Brief anchor | Drift |
|--------|------------------|--------------|-------|
| `_CATCHMENT_RADII_M` | `app/services/expansion_advisor.py:817` | ~817 | none |
| `_competition_whitespace_score` | `expansion_advisor.py:2533` | ~2340 | **+193** |
| call site | `expansion_advisor.py:8088` | — | — |

---

## 7. Out of scope — follow-up flag

`delivery_first` competition radius is **2500 m** and almost certainly has the same flooring
problem. **Not changed here** — flag it for a follow-up once dine-in is confirmed post-deploy.

---

## 8. Files changed

```
app/services/expansion_advisor.py       | 61 +++++++++++++++---------- (43 +, 18 -)
tests/test_expansion_advisor_service.py | 67 +++++++++++++++++++++++++  (67 +)
2 files changed, 110 insertions(+), 18 deletions(-)
```

## 9. Merge recommendation

**Low risk, high signal.** Targeted, well-tested, behaviour-corrupting input is fixed at the
source. The only judgement call is the REF=50 constant — surfaced in a named dict for review.
Recommend merge after Ahmed signs off on the constant, then run the post-deploy validation
above before proceeding to PR-2.
