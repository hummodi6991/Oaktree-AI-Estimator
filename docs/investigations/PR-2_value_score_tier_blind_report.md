# PR-2 Report — value_score uses the tier-blind revenue basis (ticket-multiplier leak)

| | |
|---|---|
| **PR** | [#1307 — fix(expansion): value_score uses tier-blind revenue basis](https://github.com/hummodi6991/Oaktree-Atlas/pull/1307) |
| **Branch** | `claude/value-score-tier-blind-uugb9z` (from `main` @ `5ba564f8a`, post PR-1 merge) |
| **Commit** | `fe850349d` |
| **Scope** | Finding 2 of the 2026-06 scoring/ranking audit (§2.5) ONLY. The ranking-side `estimated_revenue_index` semantics are explicitly out of scope. |
| **Status** | Pushed and PR open. **Not merged** — awaiting review, per task scope. |
| **Files** | `app/services/expansion_advisor.py` (+42/−5), `tests/test_expansion_advisor_regression.py` (+256) |
| **Validation** | Full backend suite: **2474 passed, 24 skipped** (one pre-existing flake deselected, see §5) |

---

## 1. What was wrong

`_estimate_revenue_index` (`app/services/expansion_advisor.py:4895` pre-fix)
returns

```
clamp(base × category_factor × ticket_multiplier)
```

where `ticket_multiplier = clamp(implied_check / 50, 0.5, 2.5)` is derived
from the brief's **price tier** via `_IMPLIED_CHECK_SAR`. That
tier-multiplied index was the first argument to
`_value_score(estimated_revenue_index, rent_burden_score)` inside
`_economics_score` — violating the in-code contract documented next to
`_RENT_CEILING_TIER_MULT`:

> "The percentile path is intentionally NOT tier-adjusted — it is
> peer-relative and feeds value_score, **which must stay tier-blind**."

The rent-burden half of value_score was kept tier-blind (the percentile
path takes no tier input); the revenue half leaked the tier multiplier.

### §2.5 math, concretely

`value_score = √(revenue_basis × rent_burden)`, banded at ≥ 75 →
`best_value` (+4 uprank) and < 25 → `above_market` (−6 downrank):

| Brief | Ticket multiplier | Effect on revenue basis | Consequence for value_score |
|---|---|---|---|
| Premium burger | 95/50 = **1.9×** | Any base ≥ 47.9 (100 / (1.10 × 1.9)) pins at the 100 clamp | Collapses to `√(100 × rb)` — best_value becomes **rent-burden-only** (any burden ≥ 56.25 qualifies, regardless of location quality) |
| Premium cafe | 80/50 = **1.6×** | Pins at clamp for base ≥ 59.5 | Same rent-burden-only degeneration |
| Mid pizza | 50/50 = **1.0×** | No change | value_score identical pre/post fix (pinned in tests) |
| Value cafe | 25/50 = **0.5×** | Capped at `clamp(100 × 1.05 × 0.5) = 52.5` even for a **perfect** base | `√(52.5 × 100) ≈ 72.5 < 75` — best_value **mathematically unreachable** |
| Value coffee / shawarma | 0.36 / 0.44, both **floor at 0.5×** | Capped ≤ ~54–56 | best_value unreachable (√(54 × 100) ≈ 73.5) |

So the +4 best_value uprank was systematically awarded to premium briefs on
rent burden alone and systematically withheld from value-tier briefs no
matter how good the site — the exact opposite of a "strong location at a
fair price" chip.

## 2. Why it happened

`_estimate_revenue_index` serves two consumers with different contracts:

1. The **ranking-side** economics component, where a tier-driven revenue
   expectation is an explicit product choice (premium brands monetize the
   same site harder).
2. The **value_score** chip, which is defined as peer-relative and
   tier-blind.

Only one scalar crossed the function boundary, so consumer 2 silently
inherited consumer 1's tier semantics. The contract comment was written on
the rent-burden side of the equation and never enforced on the revenue
side.

## 3. The fix

### `_estimate_revenue_index` — `return_detail=True` path

The function gains a keyword `return_detail: bool = False`. When set, it
returns `(index, detail)` where:

- `detail["tier_blind_index"] = clamp(base × category_factor)` —
  everything **except** the ticket multiplier. Category throughput stays:
  it is category-driven, constant within a search, and not the tier leak.
- `detail["ticket_multiplier"]` — the multiplier that was excluded.

The legacy scalar return path is **byte-identical** (same expression,
assigned to a local before the branch). Any caller not updated — e.g.
`scripts/diagnostics/score_component_probe.py` and all existing tests —
keeps the old behavior with zero change.

### `_economics_score` — tier-blind basis for value_score only

New keyword `revenue_index_detail: dict | None = None`:

- `value_revenue_basis = detail["tier_blind_index"]`, falling back to
  `estimated_revenue_index` when the detail is absent (legacy callers).
- `value_score = _value_score(value_revenue_basis, rent_burden_score)` —
  the **only** semantic change.
- Everything else — revenue_weight blending, rent burden (including the
  Finding-2 tier-aware absolute ceilings, untouched), fitout,
  cannibalization, fit — continues to use the tier-multiplied
  `estimated_revenue_index`. Ranking semantics outside value_score are
  unchanged.

### `economics_detail` transparency fields

Two additive keys (no existing key removed or renamed):

- `value_revenue_basis` — the tier-blind number value_score actually used.
- `ticket_multiplier` — the excluded multiplier (`null` for legacy callers
  that did not supply the detail).

### Call sites

Both scoring passes thread the detail consistently:

- **First pass** (`run_expansion_search`, `:8984` / `:9008`)
- **Second pass** (`:10135` / `:10177`) — this is the pass whose
  `economics_detail` is persisted into `score_breakdown_json`, i.e. the
  source of truth `_candidate_value_band` reads for the ±4/−6 deltas.

### Deliberately unchanged

- `_value_score` (geometric mean), band thresholds (75 / 25), and
  `_value_band_score_delta` (+4/−6).
- The persisted `estimated_revenue_index` column and its input to
  `_score_breakdown` economics: **byte-identical** (pinned by a
  fixed-input-matrix test).
- The ranking-side tier multiplier itself — whether it belongs in the
  economics component is a **separate product decision**, explicitly out
  of scope per the task.
- **No flag gating** — bug-fix correction restoring the documented
  contract (same precedent as PR-1 and the whitespace fix).

## 4. Tests

Six new tests in `tests/test_expansion_advisor_regression.py` (after the
existing Finding-2 tier-ceiling test), driven through the real
`_estimate_revenue_index → _economics_score` composition with
`_percentile_rent_burden` stubbed to a fixed peer-relative burden (the
established pattern in that file):

| Test | Pins |
|---|---|
| `test_value_score_premium_multiplier_no_longer_pins_best_value` | Premium burger, mediocre tier-blind base (≈55) + burden 58: scalar index is pinned at 100, but value_band ≠ best_value and value_score < 75; the pre-fix basis (`√(100 × 58) ≈ 76.2`) would have crossed the line. |
| `test_value_score_value_tier_cafe_can_reach_best_value` | Value cafe, tier-blind base ≥ 85 + burden 72 → best_value. Also pins the old impossibility: `_value_score(clamp(100×1.05×0.5), 100) < 75`. |
| `test_value_score_tier_invariant_for_same_listing` | Same synthetic listing under value/mid/premium → identical `(tier_blind_index, value_score, value_band)` triplets, while the ranking-side index still strictly increases with tier (out of scope, unchanged). |
| `test_value_score_mid_tier_near_identical_pre_post_fix` | Mid pizza (multiplier exactly 1.0): `tier_blind_index == index`, so value_score is identical pre/post fix — the required mid-tier invariance. |
| `test_economics_detail_value_transparency_fields` | Both new keys present and correct; all ten pre-existing economics_detail keys still present; legacy fallback (no detail) → basis = tier-multiplied index, `ticket_multiplier` = None. |
| `test_estimated_revenue_index_scalar_unchanged_over_matrix` | Pinned golden values (neutral 60.0; premium burger clamped 100.0; value coffee floored 32.4) + scalar ≡ detail-path index across a 4-tier × 6-category × 3-area matrix. |

No existing test pinned the leaky behavior — none were modified. The
pre-existing `test_economics_score_tier_aware_absolute_ceiling` (which
already asserts percentile-path value_score is tier-invariant under a
**fixed** revenue index) still passes unchanged.

## 5. Validation performed

```bash
python -m pytest tests/test_expansion_advisor_regression.py -k "value_score or revenue_index or economics" -q
# 14 passed

python -m pytest tests/test_expansion_advisor_regression.py tests/test_expansion_advisor_service.py \
    tests/test_expansion_weight_stack.py tests/test_expansion_rerank.py -q
# 321 passed

python -m pytest tests -q \
  --deselect tests/test_prewarm_concurrency.py::test_prewarm_per_candidate_exception_does_not_crash_batch
# 2474 passed, 24 skipped, 1 deselected
```

Full-suite note: the deselected
`test_prewarm_per_candidate_exception_does_not_crash_batch` is a
**pre-existing flake** — measured failing 3/8 runs on unpatched `main`
(`git stash` baseline) and ~1/5 with the patch; it exercises prewarm
thread-pool exception handling and is unrelated to this change.

Lint note: `black --check` / `flake8` already fail on `main` for both
touched files (~1,320 pre-existing flake8 hits; CI runs neither tool).
New code matches the file's existing style; the diff adds 5 E501s in the
test file consistent with surrounding lines.

## 6. Risk assessment

| Risk | Level | Mitigation |
|---|---|---|
| Ranking regression | **Very low** | The composite economics score, `_score_breakdown` input, and persisted `estimated_revenue_index` are byte-identical (matrix-pinned). Only the value_score input changes. |
| value_band churn | **Low–medium (intended)** | This is the fix: premium briefs lose unearned best_value badges (and their +4); value-tier briefs become eligible. Mid-tier briefs with multiplier ≈ 1.0 are near-identical (exactly identical at 1.0). |
| Contract drift | **Very low** | `economics_detail` changes are strictly additive; frontend reads of `value_score` / `value_band` / `value_band_low_confidence` are untouched. |
| Legacy callers | **Very low** | Scalar return path unchanged; `_economics_score` without the detail falls back to pre-fix behavior. |
| Performance | **Negligible** | One extra `_clamp` and a two-key dict per scored candidate. |

## 7. Acceptance / rollout

- Post-deploy acceptance metric:
  `scripts/diagnostics/value_band_tier_bias_probe.sql` (pre-merge baseline
  run by Ahmed) — the premium-brief best_value share should normalize
  post-deploy, and value-tier briefs should begin to register non-zero
  best_value shares.
- Sanity checks after deploy (per repo playbook): scores internally
  consistent; `economics_detail.value_revenue_basis × rent_burden_score`
  squares to `value_score²`; `ticket_multiplier` matches the brief's tier
  × category; ranking order for a fixed brief unchanged except where ±4/−6
  band deltas legitimately moved.

> Note: `docs/investigations/scoring_ranking_audit_2026-06.md` and
> `scripts/diagnostics/value_band_tier_bias_probe.sql` referenced by the
> task are not present in the repo at `5ba564f8a`; the implementation was
> grounded on the task's code anchors and §2.5 math, which matched that
> HEAD exactly.

## 8. Merge recommendation

**Merge after review — low risk.** Ranking-side outputs are provably
unchanged; the value_score correction restores the documented contract and
is fully pinned by tests in both directions (premium no longer pins,
value-tier now reachable, mid-tier invariant); rollback is a single revert
of `fe850349d`.
