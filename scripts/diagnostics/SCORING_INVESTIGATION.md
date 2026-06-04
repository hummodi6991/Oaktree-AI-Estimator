# Expansion Advisor — Scoring-Component & Ranking-Integrity Investigation

**Scope:** READ-ONLY audit of the live scoring core in
`app/services/expansion_advisor.py`. No production code was edited. A throwaway
harness (`scripts/diagnostics/score_component_probe.py`, uncommitted) imports
and runs the REAL functions.

> **Important architecture note up front.** The task brief references
> `_apply_value_band_pass` as a live positional-reorder pass. **That function
> no longer exists in the checked-out repo.** It was removed in the
> "score-delta refactor" and replaced by `_value_band_score_delta` (a pure
> additive delta) folded inside `_apply_score_deltas_and_sort`. The value-band
> and viability effects are now **summed into a single `final_score` delta and
> applied with one deterministic re-sort** (see Part A.3/A.4). This materially
> changes the suspected anomaly. All findings below describe the **live** code.

---

## Part A.1 — Weight table (`_score_breakdown`, `app/services/expansion_advisor.py:3023`)

`component_weights` dict: **`expansion_advisor.py:3091-3102`**.
The `assert sum == 100` invariant: **`expansion_advisor.py:3126-3129`** (present, tolerance `1e-3`).
Env-driven equal-and-opposite move: **`expansion_advisor.py:3089-3090`**.
Brand-knob reweight + renormalize-to-100: **`expansion_advisor.py:3104-3121`**.

| Component (key)          | Weight % | Source line | Notes |
|-------------------------|---------:|-------------|-------|
| `occupancy_economics`   | 26.2924  | :3092 | absorbs rounding residual in both reweight paths |
| `listing_quality`       | 22.0     | :3093 | CEO-directive elevation (11 → 22) |
| `brand_fit`             | 9.6404   | :3094 | |
| `landlord_signal`       | 7.0112   | :3095 | |
| `competition_whitespace`| 5.7640   | :3096 | `= round(8.7640 - EXPANSION_CHAIN_STRENGTH_WEIGHT, 4)` |
| `chain_strength`        | 3.0      | :3097 | `= EXPANSION_CHAIN_STRENGTH_WEIGHT` (env, default `3.0`) |
| `demand_potential`      | 8.7640   | :3098 | |
| `access_visibility`     | 8.7640   | :3099 | |
| `delivery_demand`       | 4.3820   | :3100 | |
| `confidence`            | 4.3820   | :3101 | |
| **SUM**                 | **100.0**| — | verified at runtime (harness Part B.2) |

**Equal-and-opposite verification.** `competition_whitespace` and
`chain_strength` are derived from the single env knob
`EXPANSION_CHAIN_STRENGTH_WEIGHT` (`config.py:335-336`, default `3.0`):
`competition_whitespace = 8.7640 − knob`, `chain_strength = knob`. Their sum is
always `8.7640`, so the total stays at 100 for any knob value. ✔

**`assert ... sum == 100` invariant** — present at `:3126-3129`, tolerance
`1e-3`, fires at call time (catches a misconfigured env knob before producing
silently-wrong scores).

**Brand-knob reweighting path** (`:3104-3121`) — multiplies each weight by
`_brand_weight_multipliers(...)` (`:2961`), then renormalizes:
`w * 100 / total`, rounds to 4 dp, and absorbs the rounding residual into the
largest weight (`:3117-3121`). The assert then re-validates. **Verified
live:** with a non-neutral profile (`primary_channel=delivery`,
`expansion_goal=delivery_led`, parking/visibility `high`) and the default gain
`EXPANSION_BRAND_WEIGHT_GAIN=0.35`, the reweighted dict sums to exactly
`100.0`. ✔

> **Production reality:** `EXPANSION_BRAND_WEIGHT_GAIN` defaults to **0.35
> (not 0)**, so the reweight branch *is live* for any brand profile that
> carries a non-neutral knob; it is a no-op only for empty/all-"medium"
> profiles. `EXPANSION_LLM_RERANK_ENABLED` defaults to **False** and
> `EXPANSION_VALUE_SCORE_ENABLED` defaults to **True**.

---

## Part A.2 — Per-leg data source + output range + neutral-default table

Each leg's raw input is computed once per candidate in the enrichment loop and
passed by keyword into `_score_breakdown` (`:9153-9167`; an earlier coarse pass
at `:8099-8113`).

| Leg (weight key)        | Producing fn (line) | Inputs / DB columns / JSON keys | Output range | Neutral default & exact trigger |
|------------------------|---------------------|---------------------------------|-------------|---------------------------------|
| `occupancy_economics`  | `_economics_score` (:4590) | `estimated_revenue_index`, `estimated_annual_rent_sar`, `estimated_fitout_cost_sar`, `area_m2`, `cannibalization_score`, `fit_score`; percentile-rent uses `commercial_unit` rents (district scope) when `is_listing and db` | 0–100 | No single neutral constant. Rent-burden falls back from `percentile` → `absolute_fallback`/`absolute_legacy` when `_percentile_rent_burden` returns `None` (no comparable pool). `rb_weight` scales with `_rent_burden_confidence`; deficit shifts to `revenue_weight` (:4655-4656). |
| `listing_quality`      | `_listing_quality_score` (:2536) | `unit_aqar_updated_at/created_at`, `unit_first_seen_at` (→ effective age), `unit_is_furnished`, `unit_restaurant_score`, `image_url`, `unit_has_drive_thru`, `unit_llm_suitability_score`, `unit_llm_listing_quality_score`, district momentum | 0–100 (+5 drive-thru, clamped) | **50.0** when `is_listing=False` (parcel) → whole leg neutral (:2591-2592). Sub-defaults: freshness **50** when `effective_age_days is None` (:2597-2598); suitability **50** when both LLM & `unit_restaurant_score` missing (:2627-2628); momentum **50** when `district_momentum_score is None` (:2658-2659). |
| `brand_fit`            | `_brand_fit_score` (:1513) | `district`, `area_m2`, `target_area_m2`, `demand_score`, `fit_score`, `cannibalization_score`, provider_* scores, `visibility_signal`, `parking_signal`, `brand_profile` knobs | 0–100 | District component defaults to **60.0** when district neither preferred nor excluded (:1520); goal component defaults **60.0** for unknown goal (:1530). No single "all-missing" constant. |
| `landlord_signal`      | `_landlord_signal_component` (:2932) | `unit_llm_landlord_signal_score` (commercial_unit LLM column) | 0–100 | **50.0** when `landlord_signal_score is None` (:2941-2942) — structural-fallback rows w/o LLM landlord signal. |
| `competition_whitespace`| `_competition_whitespace_score` (:2340) | `competitor_count`, `competitor_count_confident` | 15–100, **or 50** | **50.0** when `not confident and competitor_count <= 0` (:2372-2373) — unknown/unscanned zero count. `100.0` only when `confident and count==0`. |
| `chain_strength`       | `_chain_strength_score` (:2383) | `max_chain_strength` (max `chain_strength_score` from `expansion_competitor_quality` over same-category POIs in radius) | 0–100 | **50.0** when `max_chain_strength is None` (:2397-2398) — no same-category competitor in radius. |
| `demand_potential`     | blend at :7828-7835 / :7944-7952 of `_population_score` (:2254) + `_delivery_score` (:2280); weights `_demand_blend_weights` (:2323) | `population_reach`; `delivery_listing_count`; optional `realized_demand_30d` (gated by flag + `realized_demand_branches >= 3`) | 0–100 | `_population_score` returns **0.0** when `population_reach <= 0` (:2274-2275, a *penalty*, not neutral). `_delivery_score` returns **0.0** at count 0. Delivery branch fallback sets `provider_whitespace_score=50` "unknown ≠ excellent" (:7940). |
| `access_visibility`    | `_access_visibility_score` (:1894) | `frontage_score` (← `unit_street_width_m` or parcel geometry), `access_score`, `brand_profile` visibility/frontage sensitivity | 0–100 | No neutral constant — blends two already-defaulted inputs. Returns 0 only when both inputs are 0. |
| `delivery_demand`      | inline composite at :8076-8080 / equivalent in final pass | `provider_density_score`, `provider_whitespace_score`, `delivery_competition_score` (HungerStation et al.) | 0–100 | When `_delivery_observed` is False AND no district fallback: `provider_density=0`, `provider_whitespace=50`, `delivery_competition=0` (:7938-7942) → composite ≈ 19, **not** a clean neutral. |
| `confidence`           | `_confidence_score` (:2402) | listing path: `rent_confidence`, `area_confidence`, `unit_street_width_m`, `image_url`, `landuse_label`, `population_reach`; parcel path: `landuse_label`, `population_reach`, `delivery_listing_count` | 0–100 | Listing base **30**; **parcel path capped at 70** (`min(70, ...)`, :2448). No `None` short-circuit — accumulates from a base. |

### Legs that can SILENTLY default to a neutral 50 (the audit hot-list)

1. `chain_strength` → **50.0** when `max_chain_strength is None` (no same-category POI in radius). `_chain_strength_score:2397`.
2. `landlord_signal` → **50.0** when `unit_llm_landlord_signal_score` is NULL. `_landlord_signal_component:2941`.
3. `competition_whitespace` → **50.0** when `competitor_count<=0 and not confident` (unknown, not greenfield). `_competition_whitespace_score:2372`.
4. `listing_quality` → **50.0** whole-leg when candidate is a **parcel** (`is_listing=False`); plus per-sub neutral 50 for freshness (no dates), suitability (no LLM + no structural score), momentum (district below sample floor). `_listing_quality_score:2592/2598/2628/2659`.

### Caps / asymmetric defaults (not 50, but worth the DB check)

- `confidence` **parcel cap = 70** (`min(70.0, ...)`, `_confidence_score:2448`).
- `demand_potential.population` → **0.0** (penalty) when `population_reach<=0` — a *missing-data penalty*, not neutral (`_population_score:2274`).
- `delivery_demand` "no-data" floor composite ≈ 19 (density 0 + whitespace 50 + competition 0), `:7938-7942`.

---

## Part A.3 — Ordering pipeline, in call order

The full ordering pipeline (inside the main search routine, after enrichment):

| # | Step (line) | What it reorders | Mechanism | Fields it WRITES |
|---|-------------|------------------|-----------|------------------|
| 0 | coarse `prepared.sort` (:8164) | shortlist gate | re-sort by `preliminary_final_score` DESC | `preliminary_final_score` |
| 1 | `_rank_sort_key` + `candidates.sort` (:9559-9588) | full candidate list | re-sort by `(-final_score, gate_rank, zoning_rank, area_dist, -economics, cannib, parcel_id)` | (reads `final_score` from `_score_breakdown`) |
| 2 | `_dedupe_candidates` / `_dedupe_score_clones` (:9590-9593) | drops near-clones | filter | — |
| 3 | district balancing (:9604-9631) | multi-district representation | rebuild list, min-per-district then fill by rank | (positional only) |
| 4 | `_apply_market_viability_pass` (:4932, called :9641) | **nothing — no reorder** | per-candidate hard-floor *drops* + stashes legs/delta | `viability_legs_fired`, `viability_delta` (transient); `score_breakdown_json["market_viability_flag"]` |
| 5 | `_apply_score_deltas_and_sort` (:4779, called :9647) | full list | folds Σdeltas into `final_score`, **single** re-sort by `(-final_score, parcel_id)` | `final_score`, `score_breakdown_json["final_score"]`, `score_breakdown_json["bonus_detail"]`, legacy `value_pass.*` / `value_uprank_*` / `value_downrank_*`; pops the transient viability fields |
| 6 | `candidates[:limit]` (:9649) | truncates | slice | — |
| 7 | `_apply_rerank_to_candidates` (:1017, called :9660) | shortlist (no-op in prod) | assigns ranks; reorders by `final_rank` only if LLM enabled | **`deterministic_rank`**, **`final_rank`**, **`rerank_applied`**, `rerank_reason`, **`rerank_delta`**, `rerank_status` |
| 8 | enumerate (:9662-9664) | — | — | `compare_rank`, `rank_position` |

**The three functions the brief asked about:**

- **`_apply_value_band_pass` — DOES NOT EXIST.** Replaced by
  `_value_band_score_delta` (:4914), a pure function returning `+4.0` for
  high-confidence `best_value`, `-6.0` for high-confidence `above_market`,
  `0.0` otherwise (low-confidence / neutral / missing are inert). It does **no
  reordering** — it only contributes a number to the additive delta in step 5.

- **`_apply_market_viability_pass` (:4932).** Per the "score-delta refactor"
  docstring (:5002-5010): it **no longer reorders**. It applies hard-floor
  *drops* (population / brand-presence / construction-buffer, :5046+) and, for
  each survivor, computes which of the 6 legs fired (rpc, population, rent,
  economics, demand, radiance_growth) and stashes `viability_legs_fired` +
  `viability_delta = -10.0 * len(reasons)` (:5503-5508). It writes
  `market_viability_flag` into `score_breakdown_json` (:5510+).

- **`_apply_score_deltas_and_sort` (:4779).** `base = final_score` from
  `_score_breakdown` (:4808). Computes `value_band_delta` (:4810),
  `viability_delta` (:4813), `freshness_bonus` (0/1/2, :4833-4841),
  `momentum_bonus` (0/2, :4845-4853). `total_delta = sum` (:4855-4857),
  `final_score = clamp(base + total_delta, 0, 100)` (:4858-4860). Writes
  `bonus_detail` (:4866-4876), mirrors `score_breakdown_json["final_score"]`
  and top-level `final_score` (:4877-4878), then **one** strict re-sort by
  `(-final_score, parcel_id)` (:4905-4910).

- **`_apply_rerank_to_candidates` (:1017).** Step 1 (:1043-1049) assigns
  `deterministic_rank = idx` (1-based) **in the current list order** — i.e.
  AFTER step 5's `final_score`-DESC sort. With the flag off (production
  default), `final_rank == deterministic_rank`, `rerank_delta == 0`,
  `rerank_status = "flag_off"`, and the list is unchanged. With the flag on, it
  reorders the shortlist by LLM `new_rank` and re-sorts by `final_rank` (:1109).

### When is `deterministic_rank` assigned, and can it be non-monotonic in `base+bonus`?

`deterministic_rank` is assigned at **`_apply_rerank_to_candidates` step 1
(:1044)**, which runs **after** `_apply_score_deltas_and_sort` has already
sorted the list by `final_score = base + Σdeltas` descending (`:4905`).
Therefore:

- `deterministic_rank` **is monotonic in `final_score` (= `base + bonus`)** —
  it is literally the 1-based index of that exact sort. ✔
- `deterministic_rank` **is NOT necessarily monotonic in `base` alone, BY
  DESIGN** — the additive deltas (`value_band ±`, `viability −10/leg`,
  `freshness +1/+2`, `momentum +2`) re-order candidates relative to their bare
  `_score_breakdown` score before the rank is assigned. A `best_value +4`
  candidate can legitimately out-rank a higher-`base` neighbor. This is
  intended and is recorded transparently in `bonus_detail`.

---

## Part A.4 — Suspected anomaly: can value-band up-rank + viability demote on the SAME candidate produce an order no single pass intended?

**In the live (refactored) code: NO.** The class of bug the brief suspects —
two *independent positional passes* (a value-band swap, then a viability swap)
fighting over the same candidate and yielding an order neither intended — was
**structurally eliminated** by the score-delta refactor.

Why it cannot happen now:

1. Neither value-band nor viability reorders the list. Value-band contributes
   a scalar via `_value_band_score_delta` (:4810); viability contributes a
   scalar via `viability_delta` (:4813).
2. Both scalars are **summed once** into a single `total_delta` per candidate
   (`_apply_score_deltas_and_sort:4855-4857`), `final_score` is recomputed
   once (:4858-4860), and the list is re-sorted exactly once by
   `(-final_score, parcel_id)` (:4905-4910).
3. With a single total-order sort key, the result is well-defined and
   path-independent: there is no sequence of partial positional moves that can
   leave the list in an order "no pass intended." A `best_value +4` on a
   candidate that also trips one viability leg `-10` simply nets to `-6` on its
   own `final_score`; the candidate moves down by however much `-6` warrants
   relative to peers, and `bonus_detail` records both legs.

**Residual subtlety (not a bug, worth noting):** the *base* re-sort in step 1
(`_rank_sort_key`, :9559) uses rich tie-breakers (gate verdict, zoning, area
distance, economics, cannibalization) while the *post-delta* re-sort in step 5
uses only `(-final_score, parcel_id)`. So two candidates that were ordered by a
tie-breaker in step 1 can have that tie-breaker discarded in step 5 if their
post-delta `final_score`s differ — but that is the deltas legitimately
changing the score, not a positional conflict. If `final_score`s tie after
deltas, step 5 falls back to `parcel_id` only (it does **not** re-apply the
gate/zoning tie-breakers), so the *secondary* ordering of exact-score ties can
differ between the pre-delta and post-delta lists. This is the only ordering
behavior that is "less rich" than a naive reader might expect; it is
deterministic and stable, just `parcel_id`-keyed.

---

## Part B — Offline component harness results

Harness: `scripts/diagnostics/score_component_probe.py` (uncommitted). It
imports the real functions; no DB needed (every leg exercised in its
`db=None` / `is_listing=False` / `absolute_legacy` path). Run:
`python scripts/diagnostics/score_component_probe.py`.

### Part B.1 — Component sweeps (range ⊆ [0,100], monotonic in score-improving direction, not flat)

```
component                    driver->output                                              range_ok mono  flat
occupancy_economics          0->32.3, 25->41.8, 50->51.3, 75->60.8, 100->70.3 | None->32.3   True   True  False
listing_quality              0->46.5, 25->55.2, 50->64.0, 75->72.8, 100->81.5 | None->50.0   True   True  False
brand_fit                    0->28.6, 25->45.9, 50->63.2, 75->80.5, 100->97.8 | None->28.6   True   True  False
landlord_signal              0->0.0,  25->25.0, 50->50.0, 75->75.0, 100->100.0 | None->50.0  True   True  False
competition_whitespace       0->100,  1->78.7, 3->57.5, 8->32.6, 20->15.0 | None(unconf)->50 True   True  False
chain_strength               0->0.0,  25->25.0, 50->50.0, 75->75.0, 100->100.0 | None->50.0  True   True  False
demand_potential.population  0->0.0,  5k->25.0, 30k->61.2, 80k->100, 200k->100 | None->0.0   True   True  False
demand_potential.delivery    0->0.0,  5->35.4, 15->61.2, 40->100, 120->100 | None->0.0       True   True  False
access_visibility            0->0.0,  25->22.5, 50->45.0, 75->67.5, 100->90.0 | None->0.0    True   True  False
confidence                   0sig->30, 1->50, 2->65, 3->80, 5->95 | parcel(None)->40         True   True  False
PART B.1 RESULT: PASS
```

Notes:
- `competition_whitespace` is an **inverse** driver (input = competitor_count);
  monotonicity is checked in the score-improving direction (more competitors →
  lower whitespace). Its unknown/unconfident case returns the neutral **50**.
- No component is degenerate/flat. All outputs ⊆ [0,100]. The neutral-default
  values match Part A.2 exactly (chain_strength/landlord/whitespace → 50,
  listing_quality parcel → 50, confidence parcel → 40 ≤ cap 70).

### Part B.2 — `_score_breakdown` invariants (synthetic candidate)

```
sum(weights) = 100.0  -> PASS

component                     raw    weight%     wpts   expect   ok
occupancy_economics          70.0    26.2924     18.4     18.4 True
listing_quality              61.0       22.0    13.42    13.42 True
brand_fit                    58.0     9.6404     5.59     5.59 True
landlord_signal              75.0     7.0112     5.26     5.26 True
competition_whitespace       64.0      5.764     3.69     3.69 True
chain_strength               50.0        3.0      1.5      1.5 True
demand_potential             72.0      8.764     6.31     6.31 True
access_visibility            66.0      8.764     5.78     5.78 True
delivery_demand              55.0      4.382     2.41     2.41 True
confidence                   80.0      4.382     3.51     3.51 True
sum(weighted_points) = 65.87   final_score = 65.87  -> PASS

PART B.2 RESULT: PASS
OVERALL: ALL PASS
```

Asserted and verified:
- `sum(weights) == 100` (±1e-3). ✔
- `weighted_points == round(raw * weight / 100, 2)` for every component. ✔
- `final_score == round(sum(weighted_points), 2)` (clamped). ✔
- **Reweight path:** with a non-neutral brand profile + default gain `0.35`,
  the renormalized weights still sum to exactly `100.0`. ✔

---

## Part C — DB checks for Codespace (exact live names — verified, not from memory)

You have no DB access; run these in Codespace. **All column names below are
taken from the live `INSERT INTO expansion_candidate` (:9684-9756) and the
migrations**; all JSON paths from the live `_score_breakdown` /
`_apply_score_deltas_and_sort` / viability writers.

### Table: `public.expansion_candidate`

**Scalar columns that mirror a leg's *final* numbers** (use these for spot
checks; types from `alembic/versions/20260310_exp_adv_v0.py` + later adds):

| Column | Type | Meaning |
|--------|------|---------|
| `final_score` | `NUMERIC(6,2)` | post-delta score (after step 5) |
| `demand_score` | `NUMERIC(6,2)` | demand leg raw input |
| `whitespace_score` | `NUMERIC(6,2)` | competition_whitespace raw input |
| `confidence_score` | `NUMERIC(6,2)` | confidence raw input |
| `economics_score` | `NUMERIC` | occupancy_economics raw input |
| `brand_fit_score` | `NUMERIC` | brand_fit raw input |
| `access_visibility_score` | `NUMERIC` | access_visibility raw input |
| `provider_density_score`, `provider_whitespace_score`, `multi_platform_presence_score`, `delivery_competition_score` | `NUMERIC` | delivery_demand composite inputs |
| `fit_score`, `zoning_fit_score`, `frontage_score`, `access_score`, `parking_score`, `cannibalization_score` | `NUMERIC` | upstream sub-signals |
| `deterministic_rank`, `final_rank` | `INTEGER` | rank before/after rerank (20260418 migration) |
| `rerank_applied` | `BOOLEAN` | |
| `rerank_delta` | `INTEGER` | `final_rank - deterministic_rank` |
| `rerank_status` | `VARCHAR(32)` | `flag_off` in prod |
| `compare_rank`, `rank_position` | `INTEGER` | display ranks |

> **There is NO column for `landlord_signal`, `chain_strength`,
> `listing_quality`, or `delivery_demand` raw inputs.** Those live ONLY inside
> `score_breakdown_json` (see JSON paths below). Don't expect scalar columns.

**JSONB columns:** `score_breakdown_json`, `feature_snapshot_json`,
`gate_status_json`, `gate_reasons_json`, plus the memo/strengths/risks JSONB.

### `score_breakdown_json` key paths (jsonb `->` / `->>`)

| Path | Set by (line) | Use |
|------|---------------|-----|
| `weights -> '<component>'` | `_score_breakdown:3158` | per-component weight; verify Σ == 100 |
| `inputs -> '<component>'` | `:3159` | raw 0-100 leg input (incl. `landlord_signal`, `chain_strength`, `listing_quality`, `delivery_demand`) |
| `inputs -> 'chain_strength_max'` | `:3161` | null when no same-category POI in radius |
| `inputs ->> 'competition_whitespace_confident'` | `:9171` | F4 flag — was the count trusted |
| `inputs ->> 'rent_fallback_used'` | `:9168` | |
| `weighted_components -> '<component>'` | `:3167` | `round(raw*weight/100,2)` |
| `display -> '<component>' -> {raw_input_score,weight_percent,weighted_points}` | `:3149-3156` | UI breakdown |
| `final_score` | `:3169` then overwritten `:4877` | base, then post-delta |
| `bonus_detail -> {base_deterministic, value_band_delta, viability_delta, viability_legs_fired, freshness_bonus, freshness_label, momentum_bonus, total_delta, final_score_clamped}` | `_apply_score_deltas_and_sort:4866-4876` | the full delta ledger |
| `market_viability_flag -> ...` | `_apply_market_viability_pass:5510+` | which legs fired (rpc/population/rent/economics/demand/radiance_growth) |
| `economics_detail -> {rent_burden_score, value_score, value_band, value_band_low_confidence, rent_burden{...}}` | merged at `:9180` from `_economics_score` meta (:4691+) | value-band source of truth |
| `value_pass -> {value_uprank_applied/delta, value_downrank_applied/delta}` | `:4887-4898` | **deprecated** legacy mirror |
| `display_score` | `:9680` | clamped 1-99 UI score |

**Suggested SQL spot-checks (Codespace):**

```sql
-- 1. Weights always sum to 100 (per stored candidate).
SELECT id,
       (SELECT round(sum((v.value)::numeric), 4)
          FROM jsonb_each_text(score_breakdown_json->'weights') v) AS weight_sum
FROM expansion_candidate
WHERE score_breakdown_json ? 'weights'
  AND (SELECT round(sum((v.value)::numeric),4)
         FROM jsonb_each_text(score_breakdown_json->'weights') v) <> 100.0;
-- expect 0 rows

-- 2. final_score column == score_breakdown_json.final_score (post-delta mirror).
SELECT id, final_score, (score_breakdown_json->>'final_score')::numeric AS sb_final
FROM expansion_candidate
WHERE round(final_score,2) <> round((score_breakdown_json->>'final_score')::numeric,2);
-- expect 0 rows

-- 3. base + total_delta == final_score (bonus ledger consistency).
SELECT id,
       (score_breakdown_json->'bonus_detail'->>'base_deterministic')::numeric AS base,
       (score_breakdown_json->'bonus_detail'->>'total_delta')::numeric        AS delta,
       final_score
FROM expansion_candidate
WHERE score_breakdown_json ? 'bonus_detail'
  AND (score_breakdown_json->'bonus_detail'->>'final_score_clamped')::boolean IS DISTINCT FROM true
  AND round(final_score,2) <> round(
        (score_breakdown_json->'bonus_detail'->>'base_deterministic')::numeric
      + (score_breakdown_json->'bonus_detail'->>'total_delta')::numeric, 2);
-- expect 0 rows (clamped candidates excluded since clamp breaks the equality)

-- 4. How often each leg silently defaults to the neutral 50.
SELECT
  count(*) FILTER (WHERE (score_breakdown_json->'inputs'->>'chain_strength')::numeric = 50) AS chain_neutral,
  count(*) FILTER (WHERE (score_breakdown_json->'inputs'->>'landlord_signal')::numeric = 50) AS landlord_neutral,
  count(*) FILTER (WHERE (score_breakdown_json->'inputs'->>'competition_whitespace')::numeric = 50) AS whitespace_neutral,
  count(*) FILTER (WHERE (score_breakdown_json->'inputs'->>'listing_quality')::numeric = 50) AS listing_neutral,
  count(*) AS total
FROM expansion_candidate;

-- 5. deterministic_rank monotonic in post-delta final_score within a search.
SELECT search_id, count(*) AS inversions
FROM (
  SELECT search_id, deterministic_rank, final_score,
         lag(final_score) OVER (PARTITION BY search_id ORDER BY deterministic_rank) AS prev_score
  FROM expansion_candidate
) t
WHERE prev_score IS NOT NULL AND final_score > prev_score + 0.001  -- higher score at a LATER rank
GROUP BY search_id;
-- expect 0 rows (ties on final_score broken by parcel_id are fine; this only
-- flags a strictly-higher score appearing below a strictly-lower one)
```

> Note for check #5: `deterministic_rank` is monotonic in **final_score**
> (`base+bonus`), NOT in the bare `_score_breakdown` base — see Part A.3. Do
> not assert monotonicity against a recomputed base.

---

## Summary of findings

1. **Weight integrity: clean.** 10 components sum to exactly 100; the
   `chain_strength`/`competition_whitespace` env split is equal-and-opposite;
   the assert is present; the brand-knob reweight renormalizes to 100
   (verified live with default gain 0.35).
2. **Component math: clean.** All 10 legs are in-range, monotonic in the
   score-improving direction, and non-degenerate. `weighted_points` and
   `final_score` arithmetic hold to 2 dp.
3. **Neutral-default exposure:** 4 legs can silently resolve to **50**
   (`chain_strength`, `landlord_signal`, `competition_whitespace`,
   `listing_quality`/sub-signals) and `confidence` caps parcels at **70**;
   `demand_potential` *penalizes* missing population to 0. These are by design
   but are the highest-value DB audit targets (SQL #4).
4. **The brief's suspected two-pass ordering anomaly does NOT exist in the live
   code.** `_apply_value_band_pass` was removed; value-band and viability are
   now additive deltas folded into a single deterministic `final_score`
   re-sort, which is path-independent. `deterministic_rank` is assigned after
   that sort and is monotonic in `base+bonus` by construction (and
   intentionally non-monotonic in bare base).

---

## Part D — `chain_strength` leg root-cause trace (READ-ONLY)

### D.1 — Caller → how `max_chain_strength` is computed

**Caller chain:** `_bulk_enrich_competitors` (bulk SQL) → stored per row as
`max_chain_strength` → read at `expansion_advisor.py:7778-7781` →
`_chain_strength_score(max_chain_strength)` (`:7840`) → `_score_breakdown(...,
chain_strength_score=...)` (`:8109` / `:9163`).

**The aggregate** (`expansion_advisor.py:6520-6574`):

```sql
LEFT JOIN LATERAL (
  SELECT
    COUNT(*) FILTER (WHERE in_category) AS competitor_count,
    MAX(chain_strength) FILTER (WHERE in_category) AS max_chain_strength,   -- :6524
    ...
  FROM (
    SELECT (lower(rp.category) = ANY(:category_keys)) AS in_category,
           ecq.chain_strength_score AS chain_strength,                       -- :6540
           rp.name AS brand_name
    FROM restaurant_poi rp
    LEFT JOIN expansion_competitor_quality ecq
           ON ecq.restaurant_poi_id = rp.id                                  -- :6543-6545
          AND ecq.city = 'riyadh'
    WHERE (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')
      AND ST_DWithin(rp.geom::geography,
                     ST_SetSRID(ST_MakePoint(i.lon, i.lat),4326)::geography,
                     :radius_m)                                              -- :6552-6556
    UNION ALL
    SELECT (... category match ...) AS in_category,
           NULL::double precision AS chain_strength,   -- delivery side: always NULL  :6564
           NULL::text AS brand_name
    FROM delivery_source_record dsr
    WHERE {geom present} AND ST_DWithin(... :radius_m)
  ) combined
) comp ON TRUE
```

- **Source table/column:** `expansion_competitor_quality.chain_strength_score`.
- **Join keys:** `ecq.restaurant_poi_id = rp.id AND ecq.city = 'riyadh'` (POI side only).
- **Same-category filter:** `lower(rp.category) = ANY(:category_keys)` via `FILTER (WHERE in_category)`.
- **Radius:** `ST_DWithin(..., :radius_m)`; radius = `_catchment_radii(service_model)["competition"]` (`:6463-6467`).
- **Aggregate:** `MAX(...) FILTER (WHERE in_category)`.
- **COALESCE/default:** **None** on `max_chain_strength` (raw passthrough at `:6517`; contrast `competitor_count` which is `COALESCE(...,0)`). NULL → Python `None` (`:6585-6589`) → `_chain_strength_score(None)` → **50.0** (`:2397-2398`). Delivery rows never contribute (NULL chain_strength).

### D.2 — How the ECQ `chain_strength_score` column is computed

`app/ingest/expansion_advisor_competitors.py:245`:

```sql
-- chain_strength_score (0-100): more locations = stronger chain
LEAST(100.0, COALESCE(cc.chain_size, 1) * 12.0),
```

`cc.chain_size` from the `chain_counts` CTE (`:199-218`):

```sql
chain_counts AS (
  SELECT COALESCE(ba.canonical_brand_id, raw.chain_key) AS chain_group,
         COUNT(*) AS chain_size
  FROM ( SELECT <normalized rp.name> AS chain_key
         FROM restaurant_poi
         WHERE name IS NOT NULL AND name != ''
           AND <name has a real token>
           AND <name NOT IN denylist>
           AND (business_status IS NULL OR business_status='OPERATIONAL') ) raw
  LEFT JOIN brand_alias ba ON ba.alias_key = raw.chain_key
  GROUP BY COALESCE(ba.canonical_brand_id, raw.chain_key)
  HAVING COUNT(*) >= 5          -- :216
)
```

**Depends on:** the count of OPERATIONAL POIs sharing the same canonical brand
(`brand_alias.canonical_brand_id`) or, absent an alias, the same normalized
`restaurant_poi.name` — **not** `chain_name`. The `_CHAIN_KEY_DENYLIST`
(`:101-118`: `restaurant, cafe, burger, pizza, shawarma, …`) blocks generic
words from forming bogus mega-chains.

Value ladder (smallest non-NULL `chain_size` is 5 due to `HAVING >= 5`):

| brand citywide POI count | `chain_size` | `chain_strength_score` |
|---|---|---|
| < 5 (or unmatched) | NULL → COALESCE 1 | **12** |
| 5 / 6 / 7 / 8 | 5/6/7/8 | 60 / 72 / 84 / 96 |
| **≥ 9** | ≥9 | **100** (LEAST cap) |

**Can it be 100 when `chain_name` is empty?** `chain_name` is irrelevant to the
score — grouping keys off normalized `name` + `brand_alias`; `brand_name` is a
display-only field (`COALESCE(rp.chain_name, rp.name)`, `:234`). An empty/NULL
`chain_name` neither forces 100 nor breaks grouping. Score is 100 iff the
POI's name/alias maps to a brand with **≥9 operational Riyadh locations** —
earned by count, never a literal default.

### D.3 — The two explicit questions

**(a) Can MAX-over-radius saturate at 100 citywide for QSR/Burger? → YES — the
root cause.** The leg input is a `MAX` over same-category POIs in radius, so a
single ≥9-branch chain neighbor pins it to 100. Riyadh QSR/burger is saturated
with such chains (McDonald's, Burger King, Hardee's, Herfy, Kudu — each dozens
of branches → each POI's `chain_strength_score = 100`). Virtually every
populated QSR/burger radius returns `max_chain_strength = 100` → leg input
`100`. The leg becomes **near-constant / non-discriminative** for chain-dense
categories: a flat ~3 points (3.0% weight × 100) on nearly every candidate, no
ranking spread. Cafés / niche cuisines with few ≥9-branch operators spread more.

**(b) Is there any path that hard-defaults the leg input to 100? → NO.**
- No `COALESCE(..., 100)` on `max_chain_strength` or in `_chain_strength_score`.
- The only hard default is the **neutral 50** (`_chain_strength_score(None)`,
  `:2398`) when no same-category ECQ-matched POI is in radius.
- `_clamp` returns 100 only when the data value ≥100 — a genuine ≥9-branch
  chain in radius.
- A soft per-POI floor of **12** exists in ECQ (`COALESCE(chain_size,1)*12`),
  but it is a per-row value, almost always overridden by the `MAX`.

### D.4 — Tables/columns + SQL for Codespace

| Table | Columns relevant to this leg |
|---|---|
| `expansion_competitor_quality` | `chain_strength_score`, `restaurant_poi_id`, `city` (=`'riyadh'`), `category`, `brand_name`, `canonical_brand_id`, `district`, `geom`, `overall_quality_score` |
| `restaurant_poi` | `id`, `name`, `chain_name` (display only), `category`, `business_status`, `geom`, `lon`, `lat`, `rating` |
| `brand_alias` | `alias_key`, `canonical_brand_id`, `display_name_en/ar` |
| `delivery_source_record` | `category_raw`, `cuisine_raw`, `platform`, `geom`/`lat`/`lon` — contributes NULL chain_strength |

```sql
-- A. Distribution of ECQ chain_strength_score (expect spikes at 12 and 100).
SELECT chain_strength_score, count(*)
FROM expansion_competitor_quality
WHERE city='riyadh'
GROUP BY 1 ORDER BY 1;

-- B. Same-category POIs at the 100 ceiling (saturation driver), per category.
SELECT category,
       count(*) FILTER (WHERE chain_strength_score = 100) AS at_100,
       count(*) AS total
FROM expansion_competitor_quality
WHERE city='riyadh'
GROUP BY category ORDER BY at_100 DESC;

-- C. For a burger/QSR search center, does one big chain pin MAX to 100?
SELECT max(ecq.chain_strength_score) AS max_chain_strength
FROM restaurant_poi rp
JOIN expansion_competitor_quality ecq
  ON ecq.restaurant_poi_id = rp.id AND ecq.city='riyadh'
WHERE lower(rp.category) = ANY(ARRAY['burger','fast food','...'])   -- category_keys
  AND (rp.business_status IS NULL OR rp.business_status='OPERATIONAL')
  AND ST_DWithin(rp.geom::geography,
                 ST_SetSRID(ST_MakePoint(:lon,:lat),4326)::geography, :radius_m);
```

**Bottom line:** the leg input is not *hard-defaulted* to 100 — it is
*MAX-saturated* to 100 for chain-dense categories because one ubiquitous
≥9-branch chain in any radius caps the aggregate. The genuine missing-data path
yields **50**, not 100.

