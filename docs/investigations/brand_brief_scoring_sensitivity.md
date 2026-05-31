# Brand Brief → final_score sensitivity map (READ-ONLY findings)

All line numbers refer to `app/services/expansion_advisor.py` unless noted. No
files were edited as part of this investigation.

## 1. `component_weights` / `_score_breakdown`

Defined at **lines 3012–3023**, verbatim:

```python
3010    _chain_strength_weight = float(settings.EXPANSION_CHAIN_STRENGTH_WEIGHT)
3011    _competition_whitespace_weight = round(8.7640 - _chain_strength_weight, 4)
3012    component_weights = {
3013        "occupancy_economics": 26.2924,
3014        "listing_quality": 22.0,
3015        "brand_fit": 9.6404,
3016        "landlord_signal": 7.0112,
3017        "competition_whitespace": _competition_whitespace_weight,
3018        "chain_strength": _chain_strength_weight,
3019        "demand_potential": 8.7640,
3020        "access_visibility": 8.7640,
3021        "delivery_demand": 4.3820,
3022        "confidence": 4.3820,
3023    }
```

**Sum = 100 ✓** (enforced by `assert` at line 3028). `competition_whitespace +
chain_strength` is a fixed split of `8.7640` (env-driven via
`EXPANSION_CHAIN_STRENGTH_WEIGHT`), so the total is invariant. Exact %:

| Component | % of final_score |
|---|---|
| occupancy_economics | 26.2924 |
| listing_quality | 22.0 |
| **brand_fit** | **9.6404** |
| landlord_signal | 7.0112 |
| competition_whitespace | 8.7640 − chain (default chain 3.0 → 5.7640) |
| chain_strength | default 3.0 |
| demand_potential | 8.7640 |
| access_visibility | 8.7640 |
| delivery_demand | 4.3820 |
| confidence | 4.3820 |

**Crucial:** only **`brand_fit` (9.6404%)** and **`access_visibility`
(8.7640%)** ingest any brand_profile field directly. Everything else is
location/listing-intrinsic. So the *entire* Brand Brief touches at most ~18.4%
of the deterministic score, plus the economics path indirectly via
`price_tier`.

## 2. `_brand_fit_score` (lines 1513–1581)

Final return, verbatim (**lines 1571–1581**):

```python
    return _clamp(
        district_component * 0.18
        + goal_component * 0.2
        + channel_component * 0.14
        + overlap_fit * 0.14
        + parking_signal * (0.1 + parking_weight * 0.06)
        + fit_score * (0.12 + frontage_weight * 0.03)
        + visibility_signal * (0.08 + visibility_weight * 0.05)
        + provider_whitespace_score * 0.08
        - premium_penalty
    )
```

`_sensitivity_weight` (line 1500): **`low → 0.3, medium → 0.6, high → 1.0`**
(default 0.6 for unknown/None).

| Term | Multiplier | Sensitivity entry |
|---|---|---|
| district_component | `0.18` (fixed) | none (driven by preferred/excluded districts; 60 base, 88 preferred, 20 excluded) |
| goal_component | `0.2` (fixed) | none directly (driven by `expansion_goal` + target_area_m2/visibility/demand) |
| channel_component | `0.14` (fixed) | none (driven by `primary_channel` via `_channel_fit_score`) |
| overlap_fit | `0.14` (fixed) | none (driven by `cannibalization_tolerance_m`) |
| **parking_signal** | `0.1 + parking_weight*0.06` | `parking_sensitivity` → low **0.118** / med **0.136** / high **0.16** |
| **fit_score** | `0.12 + frontage_weight*0.03` | `frontage_sensitivity` → low **0.129** / med **0.138** / high **0.15** |
| **visibility_signal** | `0.08 + visibility_weight*0.05` | `visibility_sensitivity` → low **0.095** / med **0.11** / high **0.13** |
| provider_whitespace_score | `0.08` (fixed) | none |
| premium_penalty | subtracted | `price_tier=="premium"` → `max(0,65−visibility_signal)*0.35 + max(0,60−district_component)*0.25` (lines 1567–1569) |

Max sensitivity swing **inside brand_fit** (signal=100, low↔high): parking
±4.2, fit ±2.1, visibility ±3.5 brand_fit-points. After the 0.096404 weight
these are **≤0.40 / 0.20 / 0.34 final-score points** respectively. Tiny.

## 3. `_access_visibility_score` (lines 1894–1900) — full body

```python
def _access_visibility_score(*, frontage_score: float, access_score: float, brand_profile: dict[str, Any]) -> float:
    visibility_weight = _sensitivity_weight(brand_profile.get("visibility_sensitivity"))
    frontage_weight = _sensitivity_weight(brand_profile.get("frontage_sensitivity"))
    blend = 0.5 + frontage_weight * 0.2
    access_blend = 1.0 - blend
    weighted = frontage_score * blend + access_score * access_blend
    return _clamp(weighted * (0.75 + visibility_weight * 0.25))
```

**Answer to the critical question:** it *does* read brand_profile — but **only
two fields: `visibility_sensitivity` and `frontage_sensitivity`**. It does
**NOT** read `parking_sensitivity`, frontage/visibility *values*, or any
street/parking measurement from the brief.

- `visibility_sensitivity` scales the whole output by `0.75 + w*0.25` → low
  **0.825** / med **0.90** / high **1.0**.
- `frontage_sensitivity` only re-weights the frontage-vs-access blend (`blend`
  0.56→0.70); it moves the score **only when `frontage_score != access_score`**.

This is the **largest single sensitivity lever**: visibility_sensitivity
low↔high scales an access_visibility score of 100 by up to 17.5 points; at the
0.08764 weight that's up to **~1.53 final-score points**.

## 4. Hard filters / gates

**SQL area filters:**

- **candidate_location (parcel) path** — `BETWEEN` is a hard WHERE clause in
  both CTE builders:
  - Line 6657: `AND p.area_m2 BETWEEN :min_area_m2 AND :max_area_m2`
  - Line 6910: `AND p.area_m2 BETWEEN :min_area_m2 AND :max_area_m2`
- **commercial_unit path** — conditional, separate min/max (lines 6172–6178):

  ```python
  if min_area_m2 and min_area_m2 > 0:
      filters.append("cu.area_sqm >= :min_area")
      params["min_area"] = min_area_m2
  if max_area_m2 and max_area_m2 < 999999:
      filters.append("cu.area_sqm <= :max_area")
      params["max_area"] = max_area_m2
  ```

So `min_area_m2`/`max_area_m2` are **hard SQL pre-filters** — out-of-range
candidates never enter scoring.

**Hard-fail gates** (`HARD_FAIL_GATES`, lines 92–103): base set is
**`zoning_fit_pass` + `area_fit_pass`** only. `population_floor_pass`,
`commercial_floor_pass`, `construction_proximity_pass` join the set *only* when
their settings knob is > 0 (default 0 → not blocking).

In `_apply_market_viability_pass` (lines 4818+) the only **absolute drops**
(`continue`) are the three hard floors — population (4964), commercial/
brand-presence (4982), construction-proximity (4999) — all disabled by default
(`<= 0` → pass). The five "directive legs" (population/rent/economics/demand/
radiance) are **soft demotes**: `viability_delta = -10.0 * len(reasons)`
(line 5389), folded into final_score once.

`area_fit_pass` is a **hard-fail gate but a pure range re-check** (line 2732:
`min_area_m2 <= area_m2 <= max_area_m2`) — redundant with the SQL filter, so in
practice it always passes survivors. `zoning_fit_pass` is the only
*substantive* hard-fail (line 2742: `fail` verdict → False); `unknown`/no-data
→ `None` (indeterminate, not fail).

**Is `target_area_m2` a filter?** **No — soft signal only.** Comment at lines
2728–2731 explicitly states it no longer hard-constrains. Call sites that read
it:

- SQL `ORDER BY ABS(area_m2 - target_area_m2)` ranking + `area_distance` column
  (lines 6652, 6906, 6917) — ordering only, within the candidate pool.
- `_area_fit(...)` (line 7728) → feeds `fit_score`.
- `_estimate_revenue_index(target_area_m2=…)` (lines 7891, 8706) → `area_signal`.
- `_brand_fit_score(target_area_m2=…)` (lines 7943, 9008) → only used when
  `expansion_goal=="flagship"` (line 1535).
- Shortlist diversity tie-break `area_dist` (line 9455).

## 5. Economics path

**`_estimate_revenue_index` (lines 4085–4189):**

- `target_area_m2` → `_target` (line 4132, fallback 225). Drives `area_signal`
  (20% of base) via the ratio bands at lines 4137–4155 (full credit ±20% of
  target, tapering out).
- `price_tier` → `_implied_average_check(price_tier, category)` →
  `ticket_multiplier = clamp(implied_check / baseline, 0.5, 2.5)` (lines
  4186–4187), applied as a **final multiplier**: `return _clamp(base * factor *
  ticket_multiplier)` (line 4189). This is the single biggest brief-driven
  economic lever — premium vs value can swing the revenue index by up to ~2–5×
  ratio (clamped 0.5–2.5), which then flows into `occupancy_economics`
  (26.29%) at `revenue_weight ≈ 0.38`.

**`_economics_score` (lines 4487–4588):** signature takes **no
`brand_profile`** and reads **no** brief field. Rent-burden ceilings are
**fixed constants** — 220 (percentile fallback, line 4519) / 180 (legacy, line
4526) SAR/m²/mo — **not** modulated by `price_tier`, `channel`, or any brief
field. `rb_weight` varies only by data confidence (`_rent_burden_confidence`),
not by the brief. **Confirmed: no brand_profile field influences rent-burden
tolerance.** `price_tier`'s only economic effect is upstream, via the
revenue-side ticket multiplier.

## 6. Frontend form coverage (`frontend/src/features/expansion-advisor/ExpansionBriefForm.tsx`)

Rendered inputs: `brand_name`, `category`, `service_model`, `min_area_m2`,
`max_area_m2`, `target_area_m2`, `target_districts`, `existing_branches`,
`limit`. Advanced (collapsed): `price_tier`, `primary_channel`,
`expansion_goal`, `cannibalization_tolerance_m`, `parking_sensitivity`,
`frontage_sensitivity`, `visibility_sensitivity`.

**`preferred_districts` / `excluded_districts`: NOT exposed in the UI.** Gated
behind `SHOW_ADVANCED_GEOGRAPHY_SECTION = false` (line 15). The comment (lines
9–14) states they contribute "only a ~±0.55 overall-score nudge" and are
redundant with `target_districts`. Only **`target_districts`** is user-facing —
and it is the **hard pool filter** (SQL `district_filter_sql`, e.g. lines 5887,
6112), *not* a scoring input. So the soft district-preference signal in
`_brand_fit_score` is effectively always at its 60.0 baseline in production.

---

## Field → mechanism → component → max swing

| Brief field | Mechanism | Component(s) | Est. max final_score swing |
|---|---|---|---|
| `min_area_m2`/`max_area_m2` | Hard SQL filter + `area_fit_pass` hard gate | inclusion/exclusion | **binary** (in or out of results) |
| `target_area_m2` | SQL ORDER BY; `area_fit`→fit_score; revenue area_signal; flagship goal | brand_fit, occupancy_economics, ordering | **~3–6 pts** (mostly via economics area_signal × ticket) |
| `price_tier` | `ticket_multiplier` (0.5–2.5×) on revenue index; premium_penalty | occupancy_economics (26.29%), brand_fit | **largest — many pts** via revenue×0.38 in 26.29% leg |
| `expansion_goal` | selects goal_component formula | brand_fit `goal_component ×0.2` | ~±1.9 final (×0.0964) |
| `primary_channel` | `_channel_fit_score` | brand_fit `channel_component ×0.14` | ~±1.3 final |
| `cannibalization_tolerance_m` | `overlap_fit` + gate distance threshold | brand_fit `×0.14` + cannibalization_pass (advisory) | ~±1.3 final |
| `visibility_sensitivity` | scales access_visibility (0.825–1.0) + brand_fit visibility term | access_visibility (8.76%), brand_fit | **~1.5 + 0.34 ≈ 1.9 pts** |
| `frontage_sensitivity` | access_visibility blend; brand_fit fit_score term | access_visibility, brand_fit | ≤~0.5 pts (only if frontage≠access) |
| `parking_sensitivity` | brand_fit parking_signal term only | brand_fit | **≤0.40 pts** |
| `preferred_districts` | district_component 60→88 | brand_fit `×0.18` | ~+0.49 pts (**UI-hidden**) |
| `excluded_districts` | district_component 60→20 | brand_fit `×0.18` | ~−0.69 pts (**UI-hidden**) |
| `target_districts` | hard SQL district filter | inclusion/exclusion | **binary** (pool membership) |
| `service_model` | dine_in baseline in `_channel_fit_score`; population/other helpers | brand_fit channel + elsewhere | small |
| `category` | throughput factor + implied check | occupancy_economics | moderate (via revenue) |

## Dead / near-dead knobs

- **No truly dead knob** — every accepted `brand_profile`/brief field has
  *some* measurable path to `final_score`.
- **`parking_sensitivity`** is the weakest real knob: ≤0.40 final-score points
  end-to-end, and it is **the only sensitivity field NOT read by
  `_access_visibility_score`** — it lives solely in one brand_fit term (9.64% ×
  ~4-point internal swing). Effectively noise-floor.
- **`preferred_districts` / `excluded_districts`** are wired (±0.5–0.7 pts) but
  **unreachable from the UI** (`SHOW_ADVANCED_GEOGRAPHY_SECTION=false`) —
  de-facto dead in production because they always default to `[]`, pinning
  `district_component` at 60.
- **`frontage_sensitivity`** is a near-dead knob in the common case: its
  access_visibility effect is zero whenever `frontage_score == access_score`,
  leaving only a ≤0.2-pt brand_fit contribution.
- **Rent-burden tolerance** ignores the brief entirely — operators expecting
  `price_tier`/`channel` to relax the rent ceiling (180/220) will see no
  effect; `price_tier` only moves economics through the revenue-side ticket
  multiplier.

If the goal is to make sensitivity sliders feel responsive, the leverage
problem is structural: brand-profile sensitivity touches only ~18.4% of the
deterministic score, and within that the per-term increments (`*0.06`, `*0.03`,
`*0.05`) are small relative to the fixed-weight terms.
