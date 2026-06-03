# Patch — Finding 1: Brand-Brief knobs re-weight top-level components

**Branch:** `claude/finding1-brand-weight-reweight-5vmWD` (pushed, **no PR opened** per GATE)
**Commit:** `599a72e45`
**Scope:** Riyadh-only; targeted, additive, backward-compatible.

---

## Diagnosis

Brand-brief soft knobs (`parking_sensitivity`, `frontage_sensitivity`, `visibility_sensitivity`,
`primary_channel`, `expansion_goal`) fed **only** `_brand_fit_score` — 9.6404% of `final_score` —
and nudged sub-coefficients inside it. Max final-score swing from flipping any knob low→high was
below noise (<~0.5 pts), so the knobs were effectively inert.

## Why it happens

The knobs never touched the top-level `component_weights` distribution in `_score_breakdown`. Their
entire influence was diluted twice: once by being confined to `brand_fit`, and again by `brand_fit`
being only ~9.64% of the final score. Even a full low→high flip on every knob could not move ranking.

## Fix

Let the knobs re-weight the **top-level** `component_weights` in `_score_breakdown`, then renormalize
to sum 100. Existing `brand_fit` behavior is untouched (the patch is purely additive at the
weight-distribution layer). A neutral/empty profile (all `medium`/`balanced`) or
`EXPANSION_BRAND_WEIGHT_GAIN=0` yields all-1.0 multipliers → byte-identical weights and scores.

---

## Diff

### 1. `app/core/config.py` — env-tunable gain (0 = kill switch)

```python
    # --- Expansion Advisor brand-weight reweighting (Finding 1) ---
    # Brand-brief soft knobs (parking/frontage/visibility sensitivity, primary_channel,
    # expansion_goal) re-weight the top-level component_weights in _score_breakdown
    # instead of only nudging terms inside brand_fit (9.64% of final_score). The gain
    # scales how strongly a "high"/"low" knob moves its target component weight before
    # renormalization to 100. 0.0 disables the reweighting entirely (every multiplier
    # becomes 1.0 → byte-identical to the pre-Finding-1 static weights).
    EXPANSION_BRAND_WEIGHT_GAIN: float = float(
        os.getenv("EXPANSION_BRAND_WEIGHT_GAIN", "0.35")
    )
```

### 2. `app/services/expansion_advisor.py` — helper, signature, dynamic weights, call sites

- New `_REWEIGHTABLE_COMPONENTS` tuple + `_brand_weight_multipliers(...)` helper (returns
  per-component multipliers; all-1.0 when neutral or gain ≤ 0).
- `_score_breakdown` gains two optional kwargs (`brand_profile`, `service_model`, both default `None`).
- After `component_weights` is built and **before** the sum-to-100 assertion: apply multipliers,
  renormalize to 100, absorb rounding residual into the largest weight.
- `weighted_components` now derived from the (possibly reweighted) live weights via a comprehension
  instead of hardcoded literals.
- Both call sites in `run_expansion_search` (listings branch + candidate_location branch) pass
  `brand_profile=effective_brand_profile, service_model=service_model`.

**Mapping (product choice):**

| Knob | Effect on top-level weights |
|---|---|
| parking / frontage / visibility sensitivity | `access_visibility` lifted by the **strongest** (max) of the three |
| `primary_channel = delivery` | `delivery_demand` (+g), `competition_whitespace` (+0.5g) |
| `primary_channel = dine_in` | `access_visibility` (+0.6g), `delivery_demand` (−0.5g) |
| `expansion_goal = flagship` | `access_visibility` (+0.5g), `brand_fit` (+0.5g) |
| `expansion_goal = delivery_led` | `delivery_demand` (+g), `competition_whitespace` (+0.5g) |
| `expansion_goal = neighborhood` | `demand_potential` (+0.5g) |
| `cannibalization_tolerance_m` | no clean top-level target — keeps flowing through `brand_fit`/`occupancy_economics` unchanged |

### 3. `tests/test_expansion_advisor_service.py` — four unit tests

- neutral profile (no profile / all-medium) → weights == static baseline, sum == 100
- `EXPANSION_BRAND_WEIGHT_GAIN=0` → weights == baseline even with aggressive knobs
- `parking_sensitivity="high"` → `access_visibility` strictly greater, sum == 100
- `primary_channel="delivery"` → `delivery_demand` strictly greater, sum == 100

---

## Full unified diff

```diff
diff --git a/app/core/config.py b/app/core/config.py
index 90bc0d8e2..81bf88c86 100644
--- a/app/core/config.py
+++ b/app/core/config.py
@@ -336,6 +336,17 @@ class Settings:
         os.getenv("EXPANSION_CHAIN_STRENGTH_WEIGHT", "3.0")
     )
 
+    # --- Expansion Advisor brand-weight reweighting (Finding 1) ---
+    # Brand-brief soft knobs (parking/frontage/visibility sensitivity, primary_channel,
+    # expansion_goal) re-weight the top-level component_weights in _score_breakdown
+    # instead of only nudging terms inside brand_fit (9.64% of final_score). The gain
+    # scales how strongly a "high"/"low" knob moves its target component weight before
+    # renormalization to 100. 0.0 disables the reweighting entirely (every multiplier
+    # becomes 1.0 → byte-identical to the pre-Finding-1 static weights).
+    EXPANSION_BRAND_WEIGHT_GAIN: float = float(
+        os.getenv("EXPANSION_BRAND_WEIGHT_GAIN", "0.35")
+    )
+
     # --- Expansion Advisor decision-memo pre-warm (Phase 3) ---
     # After POST /searches returns, schedule a background task that
     # generates structured decision memos for the top-N candidates so the
diff --git a/app/services/expansion_advisor.py b/app/services/expansion_advisor.py
index 07d2061ce..e333b0577 100644
--- a/app/services/expansion_advisor.py
+++ b/app/services/expansion_advisor.py
@@ -2943,6 +2943,83 @@ def _landlord_signal_component(landlord_signal_score: int | float | None) -> flo
     return _clamp(float(landlord_signal_score))
 
 
+# Components whose weights brand-brief knobs may re-weight (Finding 1).
+_REWEIGHTABLE_COMPONENTS: tuple[str, ...] = (
+    "occupancy_economics",
+    "listing_quality",
+    "brand_fit",
+    "landlord_signal",
+    "competition_whitespace",
+    "chain_strength",
+    "demand_potential",
+    "access_visibility",
+    "delivery_demand",
+    "confidence",
+)
+
+
+def _brand_weight_multipliers(
+    brand_profile: dict[str, Any] | None,
+    service_model: str | None,
+) -> dict[str, float]:
+    """Per-component weight multipliers derived from brand-brief knobs.
+
+    Returns a multiplier (default 1.0) for each top-level scoring component.
+    A neutral/empty profile (all "medium"/"balanced") returns all 1.0, so the
+    reweighting is a no-op and scores stay byte-identical to the static-weight
+    behavior. Gain is env-tunable; 0.0 disables.
+
+    Mapping (product choice — see PR header):
+      * physical-site knobs (parking/frontage/visibility sensitivity) -> access_visibility,
+        using the strongest of the three (max), so caring about ANY of them lifts the
+        measured-access weight.
+      * primary_channel: "delivery" lifts delivery_demand (+g) and competition_whitespace
+        (+0.5g); "dine_in" lifts access_visibility (+0.6g) and trims delivery_demand (-0.5g).
+      * expansion_goal: "flagship" lifts access_visibility (+0.5g) and brand_fit (+0.5g);
+        "delivery_led" lifts delivery_demand (+g) and competition_whitespace (+0.5g);
+        "neighborhood" lifts demand_potential (+0.5g).
+      * cannibalization_tolerance_m has no clean top-level target; it keeps flowing
+        through brand_fit/occupancy_economics unchanged.
+    """
+    mult = {name: 1.0 for name in _REWEIGHTABLE_COMPONENTS}
+    g = float(getattr(settings, "EXPANSION_BRAND_WEIGHT_GAIN", 0.0) or 0.0)
+    if not brand_profile or g <= 0.0:
+        return mult
+
+    # _sensitivity_weight maps low->0.3, medium->0.6, high->1.0 (0.6 neutral).
+    # Normalize to a [-0.75, +1.0] signal around the medium baseline.
+    def _sig(level: str | None) -> float:
+        return (_sensitivity_weight(level) - 0.6) / 0.4
+
+    site_sig = max(
+        _sig(brand_profile.get("parking_sensitivity")),
+        _sig(brand_profile.get("frontage_sensitivity")),
+        _sig(brand_profile.get("visibility_sensitivity")),
+    )
+    mult["access_visibility"] *= 1.0 + g * site_sig
+
+    channel = str(brand_profile.get("primary_channel") or "balanced").lower()
+    if channel == "delivery":
+        mult["delivery_demand"] *= 1.0 + g
+        mult["competition_whitespace"] *= 1.0 + g * 0.5
+    elif channel == "dine_in":
+        mult["access_visibility"] *= 1.0 + g * 0.6
+        mult["delivery_demand"] *= max(0.0, 1.0 - g * 0.5)
+
+    goal = str(brand_profile.get("expansion_goal") or "balanced").lower()
+    if goal == "flagship":
+        mult["access_visibility"] *= 1.0 + g * 0.5
+        mult["brand_fit"] *= 1.0 + g * 0.5
+    elif goal == "delivery_led":
+        mult["delivery_demand"] *= 1.0 + g
+        mult["competition_whitespace"] *= 1.0 + g * 0.5
+    elif goal == "neighborhood":
+        mult["demand_potential"] *= 1.0 + g * 0.5
+
+    # Guard against any negative weight from stacked trims.
+    return {k: max(0.0, v) for k, v in mult.items()}
+
+
 def _score_breakdown(
     *,
     demand_score: float,
@@ -2956,6 +3033,8 @@ def _score_breakdown(
     landlord_signal_score: int | float | None = None,
     chain_strength_score: float = 50.0,
     chain_strength_max: float | None = None,
+    brand_profile: dict[str, Any] | None = None,
+    service_model: str | None = None,
 ) -> dict[str, Any]:
     """Listings-first weight distribution.
 
@@ -3021,6 +3100,25 @@ def _score_breakdown(
         "delivery_demand": 4.3820,
         "confidence": 4.3820,
     }
+    # Finding 1: brand-brief knobs re-weight components, then renormalize to 100.
+    _w_mult = _brand_weight_multipliers(brand_profile, service_model)
+    if any(abs(m - 1.0) > 1e-9 for m in _w_mult.values()):
+        _reweighted = {
+            name: component_weights[name] * _w_mult.get(name, 1.0)
+            for name in component_weights
+        }
+        _total = sum(_reweighted.values())
+        if _total > 0:
+            component_weights = {
+                name: round(w * 100.0 / _total, 4) for name, w in _reweighted.items()
+            }
+            # Absorb the rounding residual into the largest weight so the sum
+            # is exactly 100 and the assertion below holds.
+            _residual = round(100.0 - sum(component_weights.values()), 4)
+            _largest = max(component_weights, key=component_weights.get)
+            component_weights[_largest] = round(
+                component_weights[_largest] + _residual, 4
+            )
     # Invariant: weights must sum to 100 so final_score stays on a 0-100 scale.
     # Tolerance accommodates IEEE-754 rounding of 4-decimal float weights.
     # Catches misconfigured EXPANSION_CHAIN_STRENGTH_WEIGHT at startup
@@ -3044,20 +3142,8 @@ def _score_breakdown(
         "confidence": round(_safe_float(confidence_score), 2),
     }
     weighted_components = {
-        "occupancy_economics": round(_safe_float(economics_score) * 0.262924, 2),
-        "listing_quality": round(_safe_float(listing_quality_score) * 0.22, 2),
-        "brand_fit": round(_safe_float(brand_fit_score) * 0.096404, 2),
-        "landlord_signal": round(landlord_input * 0.070112, 2),
-        "competition_whitespace": round(
-            _safe_float(whitespace_score) * (_competition_whitespace_weight / 100.0), 2
-        ),
-        "chain_strength": round(
-            chain_strength_input * (_chain_strength_weight / 100.0), 2
-        ),
-        "demand_potential": round(_safe_float(demand_score) * 0.087640, 2),
-        "access_visibility": round(_safe_float(access_visibility_score) * 0.087640, 2),
-        "delivery_demand": round(_safe_float(provider_intelligence_composite) * 0.043820, 2),
-        "confidence": round(_safe_float(confidence_score) * 0.043820, 2),
+        name: round(_safe_float(raw_inputs[name]) * component_weights[name] / 100.0, 2)
+        for name in component_weights
     }
     final_score = round(sum(weighted_components.values()), 2)
     display = {
@@ -7993,6 +8079,8 @@ def run_expansion_search(
             landlord_signal_score=row.get("unit_llm_landlord_signal_score"),
             chain_strength_score=chain_strength_score,
             chain_strength_max=max_chain_strength,
+            brand_profile=effective_brand_profile,
+            service_model=service_model,
         )
         prepared.append(
             {
@@ -9044,6 +9132,8 @@ def run_expansion_search(
             landlord_signal_score=row.get("unit_llm_landlord_signal_score"),
             chain_strength_score=chain_strength_score,
             chain_strength_max=max_chain_strength,
+            brand_profile=effective_brand_profile,
+            service_model=service_model,
         )
         score_breakdown_json["inputs"]["rent_fallback_used"] = rent_fallback_used
         # F4: surface the whitespace confidence flag so the API response
diff --git a/tests/test_expansion_advisor_service.py b/tests/test_expansion_advisor_service.py
index 3eaef1a56..518e10289 100644
--- a/tests/test_expansion_advisor_service.py
+++ b/tests/test_expansion_advisor_service.py
@@ -618,6 +618,91 @@ def test_score_breakdown_matches_final_score():
     assert 0.0 <= breakdown["final_score"] <= 100.0
 
 
+def _baseline_breakdown_kwargs():
+    return dict(
+        demand_score=80,
+        whitespace_score=70,
+        brand_fit_score=75,
+        economics_score=60,
+        provider_intelligence_composite=65,
+        access_visibility_score=55,
+        confidence_score=50,
+        listing_quality_score=60,
+    )
+
+
+def test_brand_weight_reweight_neutral_profile_is_noop():
+    """Neutral / empty / all-medium profiles must leave weights byte-identical."""
+    baseline = expansion_service._score_breakdown(**_baseline_breakdown_kwargs())
+    static_weights = baseline["weights"]
+
+    # No profile at all.
+    assert (
+        expansion_service._score_breakdown(
+            **_baseline_breakdown_kwargs(), brand_profile=None, service_model="qsr"
+        )["weights"]
+        == static_weights
+    )
+
+    # Explicitly neutral knobs.
+    neutral = {
+        "parking_sensitivity": "medium",
+        "frontage_sensitivity": "medium",
+        "visibility_sensitivity": "medium",
+        "primary_channel": "balanced",
+        "expansion_goal": "balanced",
+    }
+    neutral_weights = expansion_service._score_breakdown(
+        **_baseline_breakdown_kwargs(), brand_profile=neutral, service_model="qsr"
+    )["weights"]
+    assert neutral_weights == static_weights
+    assert abs(sum(neutral_weights.values()) - 100) < 1e-3
+
+
+def test_brand_weight_reweight_gain_zero_disables(monkeypatch):
+    baseline = expansion_service._score_breakdown(**_baseline_breakdown_kwargs())
+    monkeypatch.setattr(
+        expansion_service.settings, "EXPANSION_BRAND_WEIGHT_GAIN", 0.0, raising=False
+    )
+    aggressive = {
+        "parking_sensitivity": "high",
+        "primary_channel": "delivery",
+        "expansion_goal": "delivery_led",
+    }
+    weights = expansion_service._score_breakdown(
+        **_baseline_breakdown_kwargs(), brand_profile=aggressive, service_model="qsr"
+    )["weights"]
+    assert weights == baseline["weights"]
+
+
+def test_brand_weight_reweight_high_parking_lifts_access_visibility(monkeypatch):
+    monkeypatch.setattr(
+        expansion_service.settings, "EXPANSION_BRAND_WEIGHT_GAIN", 0.35, raising=False
+    )
+    baseline = expansion_service._score_breakdown(**_baseline_breakdown_kwargs())
+    weights = expansion_service._score_breakdown(
+        **_baseline_breakdown_kwargs(),
+        brand_profile={"parking_sensitivity": "high"},
+        service_model="qsr",
+    )["weights"]
+    assert weights["access_visibility"] > baseline["weights"]["access_visibility"]
+    assert abs(sum(weights.values()) - 100) < 1e-3
+
+
+def test_brand_weight_reweight_delivery_channel_lifts_delivery_demand(monkeypatch):
+    monkeypatch.setattr(
+        expansion_service.settings, "EXPANSION_BRAND_WEIGHT_GAIN", 0.35, raising=False
+    )
+    baseline = expansion_service._score_breakdown(**_baseline_breakdown_kwargs())
+    weights = expansion_service._score_breakdown(
+        **_baseline_breakdown_kwargs(),
+        brand_profile={"primary_channel": "delivery"},
+        service_model="qsr",
+    )["weights"]
+    assert weights["delivery_demand"] > baseline["weights"]["delivery_demand"]
+    assert abs(sum(weights.values()) - 100) < 1e-3
+
+
 def test_compare_includes_v61_fields():
     db = FakeDB(
         compare_rows=[
```

---

## Implementation note for review

The new `weighted_components` comprehension reads `raw_inputs[name]` (rounded to 2dp) where the old
literals read the unrounded raw scores. This follows the patch spec ("Replace the entire
`weighted_components` block … reads the live weights"). **Verified safe:** the golden-memo regression
(`test_sample_regression_memos.py`) passes unchanged, and `test_score_breakdown_matches_final_score`
still holds — the 2dp pre-rounding does not move any golden output. Flagged explicitly because it is
the one place neutral-profile behavior could theoretically drift; it does not.

---

## Invariants preserved

- Neutral profile (no knobs / all-medium / `EXPANSION_BRAND_WEIGHT_GAIN=0`) → `component_weights`
  identical to today → scores byte-identical. **Verified via golden-memo regression.**
- `sum(component_weights.values()) == 100` within the existing 1e-3 tolerance for every profile.
- `EXPANSION_CHAIN_STRENGTH_WEIGHT` env split still works: multipliers apply on top of whatever the
  env produces.
- Frontend reads weights from `score_breakdown_json.weights` — `DecisionLogicCard.tsx` reads
  `breakdown.weights` dynamically (not hardcoded), so **no frontend change is needed**.

---

## Validation results

| Check | Result |
|---|---|
| `pytest -k expansion -q` | **688 passed, 8 skipped** (684 prior + 4 new) |
| Golden memo regression (`test_sample_regression_memos.py`) | **6 passed, 1 skipped** — neutral profiles byte-identical |
| New unit tests (neutral no-op / gain=0 / parking / delivery) | **5 passed** |
| New code black-clean (88-col, checked in isolation) | clean |
| Frontend invariant — `DecisionLogicCard.tsx` reads `breakdown.weights` dynamically | confirmed, no FE change |

### Step 3 — weight movement (direct `_score_breakdown` sim, GAIN=0.35)

Rank-1 inputs, **neutral** vs **delivery-first + high-parking** brief
(`parking_sensitivity=high, frontage_sensitivity=high, visibility_sensitivity=medium,
primary_channel=delivery, expansion_goal=delivery_led`):

| component | neutral | brief | Δ |
|---|---|---|---|
| access_visibility | 8.764 | 10.868 | **+2.10** |
| delivery_demand | 4.382 | 7.336 | **+2.95** |
| competition_whitespace | 5.764 | 7.310 | **+1.55** |
| occupancy_economics | 26.292 | 24.151 | −2.14 |
| listing_quality | 22.000 | 20.208 | −1.79 |
| brand_fit | 9.640 | 8.855 | −0.79 |
| landlord_signal | 7.011 | 6.440 | −0.57 |
| demand_potential | 8.764 | 8.050 | −0.71 |
| chain_strength | 3.000 | 2.756 | −0.24 |
| confidence | 4.382 | 4.025 | −0.36 |
| **sum** | **100.0** | **100.0** | balanced |

The knobs now move measured-access and delivery weights by **2–3 points each** (vs the prior <0.5-pt
noise), with the sum held exactly at 100. The lift correctly targets `access_visibility` (physical-site
knobs), `delivery_demand` + `competition_whitespace` (delivery channel + `delivery_led` goal), and
renormalization absorbs the opposite move across the rest.

> **Note on the "fresh production search twice" sub-item:** that requires a live Riyadh DB + ArcGIS
> proxy, which is not reachable from this sandbox. The equivalent was validated at the scoring layer
> (the deterministic core of `run_expansion_search`) with realistic rank-1 inputs above. The remaining
> piece — confirming end-to-end ordering responds on a real city-wide QSR/Burger search — is the one
> validation that could not be run here and is worth a quick manual pass before merge.

---

## Risk / tradeoff — **Low**

- Fully gated: `EXPANSION_BRAND_WEIGHT_GAIN=0.0` reverts to byte-identical static weights. Default
  0.35 is a deliberate, reviewable gain.
- Multipliers apply *on top of* whatever `EXPANSION_CHAIN_STRENGTH_WEIGHT` produces, so the existing
  env split still works.
- `cannibalization_tolerance_m` intentionally keeps flowing through `brand_fit`/`occupancy_economics`
  (no clean top-level target).
- Diff is intentionally targeted (no blanket `black` reformat of the pre-existing, non-black-clean
  files — only the touched regions were kept black-style to preserve a high-signal, reviewable diff).

## Merge recommendation

**Hold for Ahmed's review (per GATE — not merged, no PR opened).** Recommended to merge after a single
manual end-to-end production search confirms ranking responds. If Finding 2 (price-tier rent ceiling)
is in the same cycle, merge this first then rebase Finding 2, since both touch the
`_score_breakdown`/`_economics_score` call sites in the same `run_expansion_search` loop.
