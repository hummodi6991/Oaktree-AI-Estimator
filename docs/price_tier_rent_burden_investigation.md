# READ-ONLY INVESTIGATION: threading `price_tier` into the rent-burden ceiling

All references below are to `app/services/expansion_advisor.py` (live repo, verified line numbers).

## 1. `_economics_score` (lines 4487–4588)

### Full function body (verbatim)

```python
4487 def _economics_score(
4488     *,
4489     estimated_revenue_index: float,
4490     estimated_annual_rent_sar: float,
4491     estimated_fitout_cost_sar: float,
4492     area_m2: float,
4493     cannibalization_score: float,
4494     fit_score: float,
4495     db: Session | None = None,
4496     is_listing: bool = False,
4497     district: str | None = None,
4498     listing_type: str | None = None,
4499     unit_neighborhood_raw: str | None = None,
4500 ) -> tuple[float, dict[str, Any]]:
4501     monthly_rent_per_m2 = estimated_annual_rent_sar / max(area_m2 * 12.0, 1.0)
4502
4503     rent_burden_meta: dict[str, Any] = {"mode": "absolute_legacy"}
4504     rent_burden_score: float
4505
4506     if is_listing and db is not None:
4507         comp = _percentile_rent_burden(
4508             db,
4509             listing_monthly_rent_per_m2=monthly_rent_per_m2,
4510             district=district,
4511             area_m2=area_m2,
4512             listing_type=listing_type,
4513             unit_neighborhood_raw=unit_neighborhood_raw,
4514         )
4515         if comp is not None:
4516             rent_burden_score = comp["burden_score"]
4517             rent_burden_meta = {"mode": "percentile", **comp}
4518         else:
4519             rent_burden_score = _clamp(100.0 - (monthly_rent_per_m2 / 220.0) * 100.0)
4520             rent_burden_meta = {
4521                 "mode": "absolute_fallback",
4522                 "listing_monthly_rent_per_m2": round(monthly_rent_per_m2, 2),
4523                 "ceiling": 220.0,
4524             }
4525     else:
4526         rent_burden_score = _clamp(100.0 - (monthly_rent_per_m2 / 180.0) * 100.0)
4527         rent_burden_meta = {
4528             "mode": "absolute_legacy",
4529             "monthly_rent_per_m2": round(monthly_rent_per_m2, 2),
4530             "ceiling": 180.0,
4531         }
4532
4533     fitout_cost_per_m2 = estimated_fitout_cost_sar / max(area_m2, 1.0)
4534     fitout_burden_score = _clamp(100.0 - ((fitout_cost_per_m2 - 1800.0) / 2600.0) * 100.0)
4535     cannibalization_component = 100.0 - cannibalization_score
4536
4537     rb_confidence = _rent_burden_confidence(
4538         rent_burden_meta.get("source_label") if isinstance(rent_burden_meta, dict) else None,
4539         rent_burden_meta.get("n_comparable") if isinstance(rent_burden_meta, dict) else None,
4540     )
4541     rb_weight = 0.20 * rb_confidence
4542     revenue_weight = 0.38 + (0.20 - rb_weight)  # absorb deficit into most reliable component
4543     score = _clamp(
4544         estimated_revenue_index * revenue_weight
4545         + rent_burden_score * rb_weight
4546         + fitout_burden_score * 0.14
4547         + cannibalization_component * 0.13
4548         + fit_score * 0.15
4549     )
4550
4551     # Derived "best price-to-value" chip. Computed inside _economics_score so
4552     # the inputs (revenue_index, rent_burden_score, source_label,
4553     # n_comparable) are all in scope without re-reading score_breakdown_json.
4554     # Only published when rent_burden ran in percentile mode — the
4555     # absolute_legacy / absolute_fallback / envelope paths produce a
4556     # rent_burden_score that isn't peer-relative, so a value_score derived
4557     # from them would mis-classify candidates and the UI would badge them
4558     # incorrectly. value_score == None propagates as "value not available".
4559     value_score: float | None
4560     value_band: str | None
4561     value_band_low_confidence = False
4562     if (
4563         settings.EXPANSION_VALUE_SCORE_ENABLED
4564         and isinstance(rent_burden_meta, dict)
4565         and rent_burden_meta.get("mode") == "percentile"
4566     ):
4567         value_score = _value_score(estimated_revenue_index, rent_burden_score)
4568         value_band = _classify_value_band(value_score)
4569         value_band_low_confidence = _value_band_is_low_confidence(
4570             rent_burden_meta.get("source_label"),
4571             rent_burden_meta.get("n_comparable"),
4572         )
4573     else:
4574         value_score = None
4575         value_band = None
4576
4577     return score, {
4578         "rent_burden_score": round(rent_burden_score, 2),
4579         "rent_burden": rent_burden_meta,
4580         "rent_burden_confidence": round(rb_confidence, 3),
4581         "rent_burden_weight": round(rb_weight, 4),
4582         "revenue_weight": round(revenue_weight, 4),
4583         "fitout_burden_score": round(fitout_burden_score, 2),
4584         "monthly_rent_per_m2": round(monthly_rent_per_m2, 2),
4585         "value_score": round(value_score, 2) if value_score is not None else None,
4586         "value_band": value_band,
4587         "value_band_low_confidence": value_band_low_confidence,
4588     }
```

### Signature confirmation

The function is **keyword-only** (note the bare `*` at line 4488) and currently takes **NO `brand_profile` / `price_tier` / `category` argument**. Its eleven parameters end at `unit_neighborhood_raw` (line 4499). Confirmed.

### The two rent-burden ceiling constants and their branch logic

There are **three** rent-burden code paths; two of them use a hardcoded absolute ceiling:

| Mode | Line | Ceiling | Branch condition |
|------|------|---------|------------------|
| `percentile` | 4515–4517 | n/a (peer-relative) | `is_listing and db is not None` **and** `_percentile_rent_burden` returned non-None |
| `absolute_fallback` | **4519** (`/ 220.0`), declared at **4523** (`"ceiling": 220.0`) | **220.0** | `is_listing and db is not None` but `_percentile_rent_burden` returned `None` |
| `absolute_legacy` | **4526** (`/ 180.0`), declared at **4530** (`"ceiling": 180.0`) | **180.0** | the `else` — i.e. not a listing, or no DB session (the parcel path) |

The selecting branch is the `if is_listing and db is not None:` at line 4506 with its inner `if comp is not None: … else:` (4515 vs 4518) and the outer `else:` at 4525.

### How rent burden is computed and scored

- Line 4501: `monthly_rent_per_m2 = estimated_annual_rent_sar / max(area_m2 * 12.0, 1.0)`.
- In the absolute paths the sub-score is a linear decay against the ceiling:
  - fallback (4519): `rent_burden_score = _clamp(100.0 - (monthly_rent_per_m2 / 220.0) * 100.0)`
  - legacy (4526): `rent_burden_score = _clamp(100.0 - (monthly_rent_per_m2 / 180.0) * 100.0)`
  - i.e. rent == ceiling → score 0; rent == 0 → score 100; clamped to [0,100].
- In percentile mode the sub-score is `comp["burden_score"]` from `_percentile_rent_burden` (no ceiling involved).
- The sub-score then enters the composite at line 4545 weighted by `rb_weight`.

## 2. `_rent_burden_confidence` / `rb_weight` (lines 4243–4268)

### Body (verbatim)

```python
4243 def _rent_burden_confidence(source_label: str | None, n_comparable: int | None) -> float:
4244     """Confidence multiplier for rent_burden's 20% weight in the economics composite.
4245
4246     Narrow fix: only damp the specific pathology where _percentile_rent_burden
4247     silently falls back to a citywide comp pool but the caller treats it as a
4248     real district hit. All other paths (district hits, envelope flags,
4249     absolute_legacy, absolute_fallback, unknown labels, missing metadata)
4250     preserve full weight to avoid unintended behavior changes.
4251     """
4252     if source_label is None:
4253         return 1.0  # preserve legacy behavior — no damping
4254
4255     n = int(n_comparable) if n_comparable is not None else 0
4256
4257     if source_label in ("district_band_type", "district_type", "district"):
4258         # District tiers self-enforce min_n inside _percentile_rent_burden;
4259         # if one of these labels is present, n should already be >= 8.
4260         return 1.0
4261
4262     if source_label == "city_band_type":
4263         return 0.25 if n >= 12 else 0.0
4264     if source_label == "city":
4265         return 0.15 if n >= 20 else 0.0
4266
4267     # Unknown / envelope / absolute paths: preserve full weight.
4268     return 1.0
```

**Inputs read:** only `source_label: str | None` and `n_comparable: int | None` (both pulled from `rent_burden_meta` at the call site, lines 4538–4539).

**Weights returned per level:**

- `source_label is None` → **1.0** (line 4253)
- `source_label in ("district_band_type", "district_type", "district")` → **1.0** (4257–4260)
- `"city_band_type"` → **0.25 if n>=12 else 0.0** (4262–4263)
- `"city"` → **0.15 if n>=20 else 0.0** (4264–4265)
- anything else (envelope / absolute / unknown) → **1.0** (4268)

`rb_weight = 0.20 * rb_confidence` (4541); the deficit `(0.20 - rb_weight)` is added to `revenue_weight` (4542) so the five weights always sum to 1.0.

**Confirmed:** `rb_weight` depends **ONLY on data confidence** (`source_label`, `n_comparable`). It reads no brief/brand field. `price_tier` is not an input.

**Landmine flag:** A price-tier ceiling adjustment must touch **only `rent_burden_score`** (the value at line 4516/4519/4526), and must **not** alter `source_label`/`n_comparable` or `rent_burden_meta["mode"]`. If you mutate the mode or labels you'll silently re-route `rb_weight`. The cleanest insertion (a multiplier on the ceiling itself) leaves `_rent_burden_confidence` and `rb_weight` completely untouched — there is no interaction as long as you keep the `mode`/`source_label`/`n_comparable` keys identical.

## 3. `_estimate_revenue_index` ticket multiplier (lines 4045–4189)

### `_implied_average_check` (4045–4057)

```python
4045 def _implied_average_check(price_tier: str | None, category: str | None) -> float:
4046     """Return implied average check SAR from price tier and category."""
4047     tier = (price_tier or "").lower().strip()
4048     cat = (category or "").lower().strip()
4049     tier_map = _IMPLIED_CHECK_SAR.get(tier)
4050     if not tier_map:
4051         return _IMPLIED_CHECK_BASELINE_SAR
4052     if cat in tier_map:
4053         return tier_map[cat]
4054     for key, val in tier_map.items():
4055         if key != "_default" and key in cat:
4056             return val
4057     return tier_map.get("_default", _IMPLIED_CHECK_BASELINE_SAR)
```

- Normalizes tier via `(price_tier or "").lower().strip()` (4047).
- Falls back to `_IMPLIED_CHECK_BASELINE_SAR` (= **50.0**, line 4042) when tier is unset/unknown (4051), or `tier_map["_default"]` for unknown category within a known tier (4057).

### Ticket multiplier (4186–4189)

```python
4186     implied_check = _implied_average_check(price_tier, category)
4187     ticket_multiplier = max(0.5, min(2.5, implied_check / _IMPLIED_CHECK_BASELINE_SAR))
4188
4189     return _clamp(base * factor * ticket_multiplier)
```

- **Baseline divided by:** `_IMPLIED_CHECK_BASELINE_SAR = 50.0` (line 4042).
- **Clamp range:** `[0.5, 2.5]` (line 4187).

**Confirmed:** `price_tier`'s only current economic effect is this **revenue-side** multiplier. It is read in exactly two places in the economic chain: (a) here via `_estimate_revenue_index(price_tier=...)`, and (b) the brand-fit `premium_penalty` at lines 1566–1569 (a fit-score effect, not economics). It does **not** touch rent anywhere — the rent ceilings at 4519/4526 are hardcoded and tier-blind today.

## 4. Call sites + plumbing

`_economics_score` has exactly **two** call sites:

| Call site | Call line | `effective_brand_profile` in scope? | `price_tier` already passed to neighboring `_estimate_revenue_index`? |
|-----------|-----------|--------------------------------------|------------------------------------------------------------------------|
| First pass | **7899** | **Yes** | **Yes** — line 7897: `price_tier=effective_brand_profile.get("price_tier")` |
| Final recompute | **8731** | **Yes** | **Yes** — line 8712 |

At **both** sites, `effective_brand_profile.get("price_tier")` is already evaluated in the immediately-preceding `_estimate_revenue_index(...)` call (lines 7897 and 8712). So **no new plumbing is required** — `price_tier` is already in scope at both call sites and can be passed directly into a new keyword-only `_economics_score` argument.

### price_tier normalization elsewhere (canonical literals)

- `_implied_average_check` (4047): `(price_tier or "").lower().strip()` → keys are `"value"`, `"mid"`, `"premium"` (the `_IMPLIED_CHECK_SAR` dict keys, lines 3999/4013/4027).
- Brand-fit branch (1566): `price_tier = (brand_profile.get("price_tier") or "mid").lower()` then compares `== "premium"`.

→ **Canonical form:** lowercase `"value"` / `"mid"` / `"premium"`, with `"mid"` as the implicit default for missing values. A new ceiling-adjustment branch should normalize identically (`(price_tier or "mid").lower().strip()`) to stay consistent.

## 5. Tests

| File | Test | What it asserts |
|------|------|-----------------|
| `tests/test_expansion_advisor_regression.py` | `test_economics_score_damps_rent_burden_on_city_fallback` (line **1843**) | `_rent_burden_confidence` return values per label + the 5-weight sum-to-1.0 arithmetic (lines 1871–1874). Will **not** break from a ceiling multiplier as long as you don't change `rb_weight`/labels. |
| `tests/test_expansion_advisor_regression.py` | `test_score_breakdown_economics_weight_is_30` (line **1889**) | top-level breakdown weights — unrelated to the ceiling. |
| `tests/test_expansion_advisor_service.py` | `test_value_band_score_delta_reads_economics_detail_first` (2527) and the value_score block (~2411, fixture `rent_burden` at 2567) | reads `economics_meta` / `rent_burden` meta shape — keep the meta keys stable. |

**No existing test asserts the literal `180.0` / `220.0` ceiling constants directly.** The many `180`/`220` hits in `test_expansion_advisor_service.py` are `area_m2`/`target_area_m2` values, not the ceiling. The `220.0` in `tests/test_revenue.py` (lines 55, 74) is an unrelated `rent_per_m2` field. So a ceiling change has **no golden-value test to update** — though you should add one to lock the new behavior.

---

## Deliverable

### Cleanest insertion point

Add a keyword-only param to `_economics_score` (after line 4499), e.g. `price_tier: str | None = None`, and apply a **tier multiplier to the selected absolute ceiling** in the two absolute branches — the percentile branch should be left alone (it's peer-relative and already prices the tier implicitly through the local comp pool; multiplying it would double-count and corrupt `value_score`).

Concretely, define a small map near the other rent constants:

```python
# premium tenants tolerate higher rent/m² before it's a "burden"; value tenants less.
_RENT_BURDEN_TIER_CEILING_MULT = {"value": 0.85, "mid": 1.0, "premium": 1.25}
```

Then in the two absolute branches:

- Line **4519/4523** (`absolute_fallback`): replace the literal `220.0` with `220.0 * mult` where `mult = _RENT_BURDEN_TIER_CEILING_MULT.get((price_tier or "mid").lower().strip(), 1.0)`, and store the **effective** ceiling in meta (keep the `"ceiling"` key, optionally add `"ceiling_base": 220.0`, `"price_tier": ...`).
- Line **4526/4530** (`absolute_legacy`): same treatment on `180.0`.

This is the minimal, review-friendly change: it touches only `rent_burden_score` and its meta, premium > mid > value as requested, and leaves the percentile path, `_rent_burden_confidence`, `rb_weight`, and the revenue-side `ticket_multiplier` completely untouched.

### Plumbing statement

**No new plumbing is required.** `effective_brand_profile` (and thus `price_tier`) is already in scope at both `_economics_score` call sites (lines 7899 and 8731), and `effective_brand_profile.get("price_tier")` is already being read two lines above each call (7897, 8712). You only add the keyword argument to those two calls and to the signature.

### Landmines

1. **Null / missing / legacy `price_tier`.** Many rows have no brief tier. Normalize with `(price_tier or "mid").lower().strip()` and default the multiplier to `1.0` via `.get(..., 1.0)` so legacy/parcel rows (which hit the `absolute_legacy` path) keep today's exact 180.0 behavior when tier is absent. Do **not** let a `None` tier change the score.
2. **Percentile path must stay tier-blind.** Applying the multiplier inside the `percentile` branch would change `rent_burden_score` and therefore `value_score` (line 4567), which is explicitly only published in percentile mode and is peer-relative — adjusting it for tier would mis-classify the value badge. Restrict the change to the two absolute branches.
3. **`rb_weight` interaction.** `rb_weight` is derived purely from `source_label`/`n_comparable`. As long as you don't alter `mode`, `source_label`, or `n_comparable` in the meta dict, the confidence/weight logic and the sum-to-1.0 invariant (tested at regression line 1871–1874) are unaffected.
4. **Meta-shape consumers.** `value_band`/`rent_burden` meta is read by tests (`test_value_band_score_delta_reads_economics_detail_first`) and the UI; keep existing keys, add new ones additively.

_Investigation-only: no source files were edited._
