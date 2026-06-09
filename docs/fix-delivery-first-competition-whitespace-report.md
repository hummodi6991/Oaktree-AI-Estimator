# FIX: delivery_first competition radius + whitespace curve recalibration

**Branch:** `claude/delivery-first-whitespace-fix-ax3ezq`
**Status:** On branch — no PR opened, no push/merge/dispatch. Ahmed reviews the diff (especially the
REF=50 constant and the 1000 m radius) before merge, and validates on a fresh delivery_first search.
**Type:** Real scoring change (not emit-only, not flag-gated). **Changes delivery_first rankings on
deploy** — same class as the dine_in whitespace fix (`docs/fix-dinein-competition-whitespace-report.md`).
This is the follow-up that report's §7 explicitly flagged.

---

## 1. What is wrong now

A live delivery_first burger search (15 candidates) probed **100% floored**:
`competition_whitespace = 15.00` for all 15 — **one distinct value**, every site pinned at/above
the default curve REF=25. The component is **dead signal** for delivery_first, exactly as it was for
dine_in before its fix.

Root cause is the same two-part issue:

- `_competition_whitespace_score` is fed **same-category competitor counts over the delivery_first
  `"competition"` radius of 2500 m**. At 2500 m those counts are both **huge and nearly constant**:
  probe p50/p75/p90 all ≈ **145**, max **149**, **0% within REF**.
- The log-decay curve `raw = 100·(1 − log1p(count)/log1p(REF))` floors structurally at `count = REF`,
  and delivery_first fell through to `REF = 25` (the shared default). Any count ≥ 25 floors — with
  p50 ≈ 145, every candidate floors.

A constant component output means `competition_whitespace` carries **zero discriminating signal** for
delivery_first.

## 2. Why it happens — both levers are required

The radius and the curve reference were never co-calibrated for delivery_first:

- **Why not REF-only.** At 2500 m the counts don't merely overflow the curve, they **barely vary**
  (p50/p75/p90 all 145). Raising REF would un-floor them but could not *spread* a flat distribution —
  the discriminating variation simply isn't present at 2500 m.
- **Why not radius-only.** The discriminating signal lives at **1000 m** (probe recompute: p50 **16**,
  p90 **29**, max **32**, with real per-candidate variation). But even at 1000 m the p50 (count 16)
  **still floors under REF=25** (16 → raw 13.0 → floored 15.0). Tightening the radius is necessary but
  not sufficient.

dine_in needed **both** radius→1000 and REF→50 for this exact signature; delivery_first shows the same
signature, so it takes the same two coupled levers.

## 3. The fix (two coupled changes + a test update — delivery_first blast radius only)

### Change 1 — delivery_first competition radius 2500 → 1000 m

`app/services/expansion_advisor.py:829` — `_CATCHMENT_RADII_M['delivery_first']['competition']`:
`2500.0 → 1000.0`. The model's **`demand` (3000) and `provider` (3000) radii are unchanged** (platform
delivery radius). dine_in / qsr / cafe rows untouched.

### Change 2 — add a delivery_first curve reference

`app/services/expansion_advisor.py:2581` — `_WHITESPACE_LOG_REF`: add `"delivery_first": 50.0`.
`dine_in: 50.0` and `_WHITESPACE_LOG_REF_DEFAULT = 25.0` are unchanged; qsr / cafe still resolve to the
25 default. No call-site change needed — the scorer is already called with `service_model=service_model`
(`expansion_advisor.py:8146`).

### Change 3 — update the blast-radius guard test

The dine_in fix had pinned **delivery_first** bit-for-bit to the legacy REF=25 curve
(`test_competition_whitespace_cafe_qsr_reference_unchanged`). That guard now encoded the bug. It is
updated to:

- **drop delivery_first** from the legacy REF=25 loop (now guards qsr / cafe / `None` only), broadened
  to representative counts `1/3/6/8/12/16/24/25/40/50/145`;
- a new `test_competition_whitespace_delivery_first_reference_varies_off_floor` pins delivery_first
  **bit-for-bit to the REF=50 curve** (via a `_ref50` helper), asserts the p50 (count 16) is off the
  floor (~27.9) and that genuine saturation (50, 145) still floors at 15.0;
- `test_competition_whitespace_dine_in_reference_varies_off_floor` is strengthened with an explicit
  REF=50 representative-count guard (6/16/25/50/145) so the delivery_first change cannot perturb
  dine_in;
- `test_competition_whitespace_f4_path_unchanged_per_model` now includes delivery_first.

The point of the guard is unchanged — pin per-model whitespace behaviour — it just now encodes the
fixed delivery_first curve instead of the buggy one. It is **not** weakened to a no-op.

---

## REF = 50 rationale (verified in code, exact formula)

At 1000 m the probe shows **p50 16 / p90 29 / max 32**. REF=50 spreads that band off the floor:

| count    | REF=25 (default / qsr・cafe) | REF=50 (dine_in + delivery_first) |
|----------|------------------------------|-----------------------------------|
| 1        | 78.7                         | 82.4                              |
| 6        | 40.3                         | 50.5                              |
| 16 (p50) | **15.0 (floored)**           | **27.9**                          |
| 24       | 15.0                         | 18.1                              |
| 29 (p90) | 15.0                         | 15.0\*                            |
| 32 (max) | 15.0                         | 15.0                              |
| 50       | 15.0                         | 15.0 (floor)                      |
| 145      | 15.0                         | 15.0                              |

\* The 1000 m probe counts are the **under-counting approximation** (simplified category match, no
alias expansion), so true production counts at 1000 m run somewhat higher. REF=50 (matching dine_in) is
chosen deliberately **not lower**, to avoid re-flooring on the genuine alias-expanded counts. If
post-deploy validation shows genuinely-competitive sites still bunched low, REF can be nudged up — do
not pre-optimize.

> Note: the brief's loose parentheticals (e.g. "29 → ~17") were estimates; the tests assert the actual
> computed REF=50 curve (29 → 15.0 under the exact formula), so they encode true behaviour rather than
> the approximation.

---

## 4. Validation

### Already run (this patch)

```bash
python -m pytest tests/test_expansion_advisor_service.py -k whitespace -q   # 5 passed
python -m pytest tests/test_expansion_advisor_service.py \
                tests/test_expansion_advisor.py \
                tests/test_expansion_advisor_demand_generator.py -q          # 280 passed
python -m pytest tests/ -k "golden or memo" -q                              # 128 passed, 6 skipped
```

### Post-deploy (Ahmed — this DOES change delivery_first rankings)

1. Merge → deploy → `kubectl rollout status deployment/oaktree-estimator -n default`.
2. Fresh city-wide **delivery_first** search (old searches won't backfill).
3. Re-run `scripts/diagnostics/delivery_first_whitespace_probe.sql`
   (`psql -f … > /tmp/df_val2.txt`). Success criteria:
   - `pct_whitespace_at_floor` drops from **100%** to only the genuinely saturated few;
   - `distinct_whitespace_values` rises from 1 to many (signal restored);
   - `competition_whitespace` spreads roughly **15–82** across the shortlist;
   - `competitor_count` now reflects the **1000 m** radius (p50 ~16, not ~145);
   - the shortlist reorders sensibly (low-local-competition sites rise).

---

## 5. Risk / tradeoff — blast radius

- **Intended ranking shift, delivery_first only.** Saturated delivery_first sites that previously all
  read 15.0 now spread across the p50–p75 band; the genuinely saturated ≥ ~40 tail still floors. This
  is the corrected behaviour, not a regression.
- **Blast radius** is the whitespace component's weight (**~5.764%**), larger in effect under a
  delivery-led brief where delivery weighting dominates the demand blend.
- **Contained.** dine_in (REF 50), qsr (REF 25), cafe (REF 25) keep their existing radii and REFs,
  pinned bit-for-bit by guard tests. No change to `_competition_whitespace_score`'s formula, the 15.00
  floor, the F4 `count<=0` branches (50/100), `component_weights`, `_demand_blend_weights`, the
  `sum == 100` invariant, gates, or any other model's radii.
- **Curve constant is the review point.** REF=50 lives in the named `_WHITESPACE_LOG_REF` dict so Ahmed
  can tune the knee without touching the formula.

---

## 6. Verified anchors (live tree vs brief)

| Symbol | Live `path:line` | Brief anchor | Drift |
|--------|------------------|--------------|-------|
| `_CATCHMENT_RADII_M['delivery_first']` | `expansion_advisor.py:829` | :823 | +6 |
| `_WHITESPACE_LOG_REF['delivery_first']` | `expansion_advisor.py:2581` | :2556 | +25 |
| call site (already passes `service_model`) | `expansion_advisor.py:8146` | — | — |

---

## 7. Merge recommendation

**Low risk, high signal.** Targeted, well-tested; the behaviour-corrupting input is fixed at the
source and the only judgement call — the REF=50 constant — is surfaced in a named dict for review.
Recommend merge after Ahmed signs off on the constant and the 1000 m radius, then run the post-deploy
validation above.
