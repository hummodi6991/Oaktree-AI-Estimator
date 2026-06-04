# Scoring-component & Ranking-integrity Investigation (READ-ONLY)

Scope: Expansion Advisor scoring core in `app/services/expansion_advisor.py`.
No production code was modified. All line numbers below are against the live
checked-out repo (verified, not from memory). A throwaway harness lives at
`scripts/diagnostics/score_component_probe.py` (uncommitted).

> **Headline finding (read this first).** The architecture the task brief
> describes — three sequential *positional* passes
> (`_apply_value_band_pass` → `_apply_market_viability_pass` →
> `_apply_rerank_to_candidates`), where a value-band up-rank and a viability
> demote fight over list positions — **no longer exists in the live code.**
> `_apply_value_band_pass` has been **deleted**. Reordering now happens in
> exactly **one** place via a single total-order re-sort, so the suspected
> "order no single pass intended" anomaly is **architecturally impossible**
> in the current code (see Part A.3 / A.4).

---

## Part A.1 — The scoring core: `_score_breakdown` and `component_weights`

* `_score_breakdown` — `app/services/expansion_advisor.py:3023`
* `component_weights` dict — `app/services/expansion_advisor.py:3091-3102`
* Env-driven chain_strength weight — `:3089`
  (`_chain_strength_weight = float(settings.EXPANSION_CHAIN_STRENGTH_WEIGHT)`)
* Equal-and-opposite competition_whitespace move — `:3090`
  (`_competition_whitespace_weight = round(8.7640 - _chain_strength_weight, 4)`)
* Brand-knob reweight + renormalize-to-100 — `:3104-3121`
* `assert ... sum == 100` invariant — `:3126-3129`
* Config default `EXPANSION_CHAIN_STRENGTH_WEIGHT = "3.0"` — `app/core/config.py:335-337`

### Live weight table (default, `EXPANSION_CHAIN_STRENGTH_WEIGHT=3.0`)

| Component (exact key)     | Weight %  | Source                                    |
|--------------------------|-----------|-------------------------------------------|
| `occupancy_economics`    | 26.2924   | static                                    |
| `listing_quality`        | 22.0      | static                                    |
| `brand_fit`              | 9.6404    | static                                    |
| `landlord_signal`        | 7.0112    | static                                    |
| `competition_whitespace` | 5.7640    | `round(8.7640 − chain_strength_wt, 4)`    |
| `chain_strength`         | 3.0       | env `EXPANSION_CHAIN_STRENGTH_WEIGHT`     |
| `demand_potential`       | 8.7640    | static                                    |
| `access_visibility`      | 8.7640    | static                                    |
| `delivery_demand`        | 4.3820    | static                                    |
| `confidence`             | 4.3820    | static                                    |
| **Sum**                  | **100.0** | verified                                  |

**Equal-and-opposite move confirmed:** `chain_strength` (3.0) and
`competition_whitespace` (8.7640 − 3.0 = 5.7640) are the only two weights that
move with the env knob; their sum is constant at 8.7640, so the total stays at
100 for any `EXPANSION_CHAIN_STRENGTH_WEIGHT ∈ [0, 8.7640]`.

**`assert` invariant present:** Yes — `:3126-3129`, tolerance `1e-3`. It runs
on *every* `_score_breakdown` call (per-candidate), so a misconfigured env knob
that breaks the sum fails fast rather than silently mis-scoring.

**Brand-knob reweight renormalizes to 100:** Yes — `:3104-3121`. When any
`_brand_weight_multipliers` (`:2961`) value ≠ 1.0 (only when
`EXPANSION_BRAND_WEIGHT_GAIN > 0` and a non-neutral brand profile is supplied),
weights are multiplied, re-divided by their new total ×100, and the rounding
residual is absorbed into the largest weight (`:3117-3121`) so the sum is
exactly 100 and the assertion holds. **Verified empirically** (gain=0.5,
delivery channel + delivery_led goal + high visibility): reweighted sum = 100.0.

---

## Part A.2 — The 10 legs: producer, inputs, range, neutral-default

Each leg's *raw input* is rounded and multiplied by its weight in
`_score_breakdown` (`raw_inputs` `:3132-3143`, `weighted_components` `:3144-3147`).
All leg producers clamp to **[0, 100]** via `_clamp`.

| # | Leg key (`component`)    | Producer fn (line)                               | Driving DB columns / JSON keys                                                                                  | Output range | Neutral / missing-data default |
|---|--------------------------|--------------------------------------------------|----------------------------------------------------------------------------------------------------------------|--------------|--------------------------------|
| 1 | `occupancy_economics`    | `_economics_score` (`:4590`) → `economics_score` | revenue_index, `estimated_annual_rent_sar`, `estimated_fitout_cost_sar`, `area_m2`, cannib, fit; `is_listing`, `district`, `price_tier` | 0–100 | **No flat neutral.** Rent-burden confidence (`rb_weight = 0.20·confidence`, `:4655`) shifts weight to revenue when comparables thin; not a 50.0 midpoint. |
| 2 | `listing_quality`        | `_listing_quality_score` (`:2536`)               | `is_listing`; `unit_aqar_*`/`first_seen_at`→`effective_age_days`; `unit_is_furnished`; `unit_restaurant_score`; `image_url`; `unit_llm_suitability_score`; `unit_llm_listing_quality_score`; `district_momentum.momentum_score` | 0–100 | **Multiple silent 50.0 defaults** (see below). |
| 3 | `brand_fit`              | `_brand_fit_score` (`:1513`)                     | `district` vs brand_profile preferred/excluded; `area_m2`/`target_area_m2`; demand/fit/cannib/provider signals; brand_profile knobs | 0–100 | District not in preferred/excluded → `district_component = 60.0` base (`:1520`). |
| 4 | `landlord_signal`        | `_landlord_signal_component` (`:2932`)           | `unit_llm_landlord_signal_score`                                                                                | 0–100 | **`None` → 50.0** (`:2941-2942`). Trigger: LLM landlord-signal not yet backfilled / returned NULL. |
| 5 | `competition_whitespace` | `_competition_whitespace_score` (`:2340`)        | `competitor_count` (int), `competitor_count_confident` (bool)                                                   | 15–100 | **`count≤0 AND not confident` → 50.0** (`:2372-2373`). `count≤0 AND confident` → 100.0. Floor 15. |
| 6 | `chain_strength`         | `_chain_strength_score` (`:2383`)                | `max_chain_strength` (max `chain_strength_score` over same-category POIs in radius)                             | 0–100 | **`None` → 50.0** (`:2397-2398`). Trigger: no same-category competitor POI in the competition radius. |
| 7 | `demand_potential`       | blend of `_population_score` (`:2254`) + `_delivery_score` (`:2280`) at `:7835` | `population_reach`; `delivery_listing_count`; `realized_demand_30d`; `service_model` blend weights | 0–100 | **Zero, not neutral.** `population_reach ≤ 0 → 0.0` (`:2274-2275`); `delivery_listing_count ≤ 0 → 0.0` (`:2304-2308`). Missing demand data **penalizes**. |
| 8 | `access_visibility`      | `_access_visibility_score` (`:1894`)             | `frontage_score`, `access_score` (from `_frontage_score`/`_access_score`), brand visibility/frontage sensitivity | 0–100 | No own neutral; inherits upstream frontage/access defaults. |
| 9 | `delivery_demand`        | `provider_intelligence_composite` computed at `:8076-8080` | `provider_density_score`·0.36 + `provider_whitespace_score`·0.38 + (100−`delivery_competition_score`)·0.26 | 0–100 | `provider_whitespace_score` **unknown → 50.0** (`:7940`, "unknown ≠ excellent"). |
| 10 | `confidence`            | `_confidence_score` (`:2402`)                    | listing: `rent_confidence`,`area_confidence`,`unit_street_width_m`,`image_url`,`landuse_label`,`population_reach`,`delivery_listing_count`; parcel: `landuse_label`,`population_reach`,`delivery_listing_count` | 0–100 | Listing base 30 (`:2419`); **parcel base 40, hard cap 70** (`:2441,2448`). Missing signals = low score, not midpoint. |

### Legs that can SILENTLY default to the neutral midpoint (50.0)

| Leg | Exact missing-data condition | Line |
|-----|------------------------------|------|
| `landlord_signal` | `unit_llm_landlord_signal_score IS NULL` | `:2941` |
| `chain_strength` | no same-category competitor POI in radius → `max_chain_strength IS None` | `:2397` |
| `competition_whitespace` | `competitor_count ≤ 0` **and** `confident` falsy (scan thin / no flag) | `:2372` |
| `listing_quality` (whole) | `is_listing=False` (parcel candidate) → returns 50.0 | `:2591-2592` |
| `listing_quality` · freshness | `effective_age_days IS None` (all aqar/first_seen dates NULL) | `:2597-2598` |
| `listing_quality` · suitability | `llm_suitability_score IS None` **and** `unit_restaurant_score` NULL/≤0 | `:2627-2628` |
| `listing_quality` · momentum | `district_momentum_score IS None` or district below `_MOMENTUM_SAMPLE_FLOOR` (20) | `:2589`, `:2642+` |
| `delivery_demand` · provider_whitespace | delivery whitespace unknown | `:7940` |

### Legs that default to ZERO (penalizing) on missing data — NOT neutral

* `demand_potential`: `population_reach ≤ 0 → 0.0`; `delivery_listing_count ≤ 0 → 0.0`.

### Leg with a capped default

* `confidence` (parcel path): base 40.0, **hard cap 70.0** (`:2448`). A parcel
  can never reach the confidence ceiling a measured listing can.

---

## Part A.3 — The full ordering pipeline (in call order)

Call site: `expansion_search`, `app/services/expansion_advisor.py:9633-9664`.

| Step | Function (line) | Reorders? | Mechanism | Fields it WRITES |
|------|-----------------|-----------|-----------|------------------|
| 0 | preliminary sort `:8164` | yes | sort by `preliminary_final_score` DESC (shortlisting only) | — |
| 1 | `_apply_market_viability_pass` (`:4932`) | **No positional reorder.** Only **removes** hard-floor failures (order preserved among survivors). | hard floors filter (`:5067-5141`); soft legs stash deltas | `gate_status_json.{population_floor_pass,commercial_floor_pass,construction_proximity_pass}`; transient `viability_legs_fired`, `viability_delta` (−10 per fired leg, `:5503-5508`); `score_breakdown_json.market_viability_flag` (`:5550`) |
| 2 | `_apply_score_deltas_and_sort` (`:4777`) | **Yes — the only score-driven reorder.** | folds 4 deltas into `final_score`, then **one strict re-sort** `(-final_score, parcel_id)` (`:4905-4910`) | `final_score` (overwritten, `:4878`); `score_breakdown_json.final_score` (`:4877`); `score_breakdown_json.bonus_detail.*` (`:4866-4876`); legacy `score_breakdown_json.value_pass.*` + top-level `value_uprank_*/value_downrank_*` (`:4887-4898`); drops transient viability fields (`:4902-4903`) |
| 3 | `candidates[:limit]` (`:9649`) | truncation | slice | — |
| 4 | `_apply_rerank_to_candidates` (`:1017`) | **No reorder in production** (`EXPANSION_LLM_RERANK_ENABLED=False`). | with flag on: re-sort by `final_rank` (`:1109`) | `deterministic_rank` (`:1044`), `final_rank` (`:1045`), `rerank_applied` (`:1046`), `rerank_reason` (`:1047`), `rerank_delta` (`:1048`), `rerank_status` (`:1049`) |
| 5 | enumerate `:9662-9664` | positional | assigns by current order | `compare_rank`, `rank_position` |

### The deltas folded by step 2 (`_apply_score_deltas_and_sort`)

* `value_band_delta` — `_value_band_score_delta` (`:4914`): high-confidence
  `best_value` **+4**, high-confidence `above_market` **−6**, else 0
  (low-confidence pools and neutral/missing bands are inert).
* `viability_delta` — **−10 per fired soft leg**, stacking (`:5503`).
* `freshness_bonus` — `+2` if "new" (created ≤7d), else `+1` if "updated"
  (refreshed ≤7d); mutually exclusive (`:4822-4841`).
* `momentum_bonus` — `+2` when district `momentum_score ≥ 70` and sample floor
  not applied (`:4845-4853`).

### Where `deterministic_rank` is assigned, and monotonicity

`deterministic_rank` is assigned at **`:1044`**, inside
`_apply_rerank_to_candidates`, by a 1-based `enumerate` over the candidate
list **after** step 2 already sorted it strictly by `(-final_score,
parcel_id)`. Therefore:

* `deterministic_rank` **is, by construction, monotonic in the final
  (base + Σbonus) score** — it is just the 1-based index of an already
  sorted list. It cannot be non-monotonic in `base+bonus`.
* It **can** be non-monotonic in the **pre-bonus base** score: a candidate
  with a lower base but a favorable bonus (best_value +4 / freshness +2 /
  momentum +2, or simply not demoted while a higher-base peer ate −10) can
  legitimately out-rank a higher-base candidate. **This is by design** — the
  bonuses are folded *before* ranking precisely so they move rank.

---

## Part A.4 — The suspected anomaly: does NOT exist in the live code

**Suspicion (per brief):** a `value_band` up-rank followed by a `viability`
demote (or vice-versa) on the same candidate could produce an order no single
pass intended.

**Verdict: not possible in the current architecture.**

* In the **old** design (the stale chat copy), value-band and viability were
  *separate positional passes* that each swapped list positions. Two
  independent positional nudges acting on overlapping candidates can indeed
  yield a net order that neither pass alone "intended" (the result depends on
  pass order and can violate the global `final_score` total order).
* In the **live** design, `_apply_value_band_pass` is **deleted**
  (confirmed by the residual comments at `:4919-4920`
  — *"same skip semantics as the deleted `_apply_value_band_pass` positional
  nudge"* — and `app/core/config.py:188`). `_apply_market_viability_pass`
  **no longer reorders** (`:5002-5006`, *"this function no longer reorders the
  candidate list"*). Both value-band and viability now contribute **additive
  scalar deltas** to `final_score`, which are summed in a single fold and
  resolved by **one** strict total-order sort `(-final_score, parcel_id)`
  (`:4855-4910`).
* A single total-order sort, with a deterministic `parcel_id` tie-break,
  produces exactly one well-defined ordering. There is no second positional
  operation to disagree with it. A candidate that is `best_value` (+4) **and**
  viability-demoted (−10) simply nets to **−6** and is ranked by its resulting
  `final_score` — the correct, intended behavior, not a positional tug-of-war.

**Code path that *would* have shown the anomaly** (now absent): there is no
longer any function that performs a positional swap keyed on `value_band`
after `_apply_market_viability_pass`. The only sort after the viability pass is
the single strict re-sort at `:4905-4910`.

**Doc-hygiene note (non-blocking, READ-ONLY — not fixed):** stale comments at
`:5307-5310` still describe *"a single positional swap per candidate"* and
*"the existing demote loop applies a single positional swap"*. These describe
the removed mechanism; the code below them stashes a delta and does not swap.
Worth a comment cleanup in a future targeted patch.

---

## Part B — Offline component harness results

Harness: `scripts/diagnostics/score_component_probe.py` (uncommitted). Imports
the real functions; sweeps each single-input leg, asserts range/monotonicity,
flags degenerate (flat) outputs; builds a synthetic candidate and checks the
`_score_breakdown` arithmetic invariants.

Run (deps `sqlalchemy fastapi pydantic pydantic-settings` required):

```
PYTHONPATH=. python scripts/diagnostics/score_component_probe.py
```

### Part 1 — component monotonicity sweep (PASS = in-range; monotonic; neutral-default correct)

| Component (driver swept)                                  | range | mono | flat | neutral default observed |
|----------------------------------------------------------|-------|------|------|--------------------------|
| `chain_strength` (input 0→100)                           | PASS  | PASS | vary | None → **50.0** ✓ |
| `landlord_signal` (input 0→100)                          | PASS  | PASS | vary | None → **50.0** ✓ |
| `competition_whitespace` (count 20→0, confident=True)    | PASS  | PASS | vary | count≤0 & unconfident → **50.0** ✓; sweep 20→15.0 … 0→100.0 |
| `demand:population` (reach 0→160k, qsr)                   | PASS  | PASS | vary | reach 0 → **0.0** (penalizing, by design) |
| `delivery` (listing_count 0→40)                          | PASS  | PASS | vary | count 0 → **0.0** |
| `confidence:parcel` (pop driver, cap 70)                 | PASS  | PASS | vary | saturates at **70.0** cap ✓ |
| `listing_quality` (age 400→10 d, listing)                | PASS  | PASS | vary | age None → 48.0; `is_listing=False` → **50.0** ✓ |

No component was flagged FLAT/degenerate across its driving sweep. (The
`confidence:parcel` curve correctly plateaus at the 70.0 cap, which is the
intended ceiling, not a degeneracy.)

### Part 2 — `_score_breakdown` invariants (synthetic candidate)

```
[PASS] sum(weights) == 100                              (sum=100.000000)
[PASS] each weighted_points == round(raw*weight/100, 2)
[PASS] final_score == round(sum(weighted_points), 2)    (final_score=62.63 sum(wp)=62.63)
```

Per-leg weighted_points all matched `round(raw·weight/100, 2)` exactly
(occupancy_economics 17.48, listing_quality 12.10, brand_fit 5.59,
landlord_signal 4.35, competition_whitespace 3.69, chain_strength 1.32,
demand_potential 6.31, access_visibility 6.13, delivery_demand 2.15,
confidence 3.51 → Σ = 62.63).

**Brand-knob renormalization (separate check):** with
`EXPANSION_BRAND_WEIGHT_GAIN=0.5` and a delivery/delivery_led/high-visibility
profile, the reweighted weights still sum to **100.0** and the assertion holds.

**OVERALL: ALL CHECKS PASS.**

---

## Part C — DB checks for Codespace (exact live names)

> ⚠ **Key gotcha for the SQL author.** `_apply_score_deltas_and_sort`
> **overwrites** both the `final_score` column and
> `score_breakdown_json->'final_score'` with the **post-bonus** value
> (`:4877-4878`), but the `weighted_components` / `display` blocks still hold
> the **pre-bonus base**. Therefore:
> * `Σ weighted_components == score_breakdown_json->'bonus_detail'->>'base_deterministic'`
>   (NOT the top-level `final_score`).
> * `final_score (column) == clamp(base_deterministic + total_delta, 0, 100) == score_breakdown_json->>'final_score'`.

### Table: `expansion_candidate` (verified live columns, from the INSERT at `:9684-9756`)

Scalar leg columns persisted directly:
`demand_score`, `whitespace_score`, `fit_score`, `confidence_score`,
`zoning_fit_score`, `frontage_score`, `access_score`, `parking_score`,
`access_visibility_score`, `cannibalization_score`, `economics_score`,
`brand_fit_score`, `provider_density_score`, `provider_whitespace_score`,
`multi_platform_presence_score`, `delivery_competition_score`, `final_score`.

> Note: `landlord_signal`, `chain_strength`, `listing_quality`,
> `delivery_demand` (composite), and `demand_potential` do **not** have their
> own scalar columns — their authoritative raw values live inside
> `score_breakdown_json` (see key paths). `whitespace_score` (column) is the
> raw input to the `competition_whitespace` leg; `demand_score` (column) is the
> raw input to the `demand_potential` leg.

Rank / rerank columns (verified `:9751-9756`, `:9824-9829`):
`compare_rank`, `rank_position`, `deterministic_rank`, `final_rank`,
`rerank_applied`, `rerank_reason` (jsonb), `rerank_delta`, `rerank_status`.

JSON columns: `gate_status_json`, `gate_reasons_json`,
`feature_snapshot_json`, `score_breakdown_json` (all jsonb).

### `score_breakdown_json` key paths (verified against `_score_breakdown` `:3157-3170`, `_apply_score_deltas_and_sort` `:4866-4898`, `_apply_market_viability_pass` `:5515-5550`)

The 10 exact component keys: `occupancy_economics`, `listing_quality`,
`brand_fit`, `landlord_signal`, `competition_whitespace`, `chain_strength`,
`demand_potential`, `access_visibility`, `delivery_demand`, `confidence`.

```
score_breakdown_json->'weights'->>'<component>'                       -- weight %
score_breakdown_json->'inputs'->>'<component>'                        -- raw input (0..100)
score_breakdown_json->'inputs'->>'chain_strength_max'                 -- nullable
score_breakdown_json->'weighted_components'->>'<component>'           -- raw*weight/100
score_breakdown_json->'display'-><component>->>'raw_input_score'
score_breakdown_json->'display'-><component>->>'weight_percent'
score_breakdown_json->'display'-><component>->>'weighted_points'
score_breakdown_json->>'final_score'                                  -- POST-bonus (overwritten)
score_breakdown_json->'bonus_detail'->>'base_deterministic'          -- == Σ weighted_components
score_breakdown_json->'bonus_detail'->>'value_band_delta'            -- +4 / -6 / 0
score_breakdown_json->'bonus_detail'->'viability_legs_fired'         -- jsonb array[str]
score_breakdown_json->'bonus_detail'->>'viability_delta'             -- -10 * legs
score_breakdown_json->'bonus_detail'->>'freshness_bonus'             -- 0/1/2
score_breakdown_json->'bonus_detail'->>'freshness_label'             -- new/updated/null
score_breakdown_json->'bonus_detail'->>'momentum_bonus'              -- 0/2
score_breakdown_json->'bonus_detail'->>'total_delta'
score_breakdown_json->'bonus_detail'->>'final_score_clamped'         -- bool
score_breakdown_json->'market_viability_flag'->>'demoted'           -- when any leg fired
score_breakdown_json->'market_viability_flag'->>'reason'            -- legs '_and_'-joined
score_breakdown_json->'market_viability_flag'->>'population_demote'
score_breakdown_json->'market_viability_flag'->>'rent_demote'
score_breakdown_json->'market_viability_flag'->>'economics_demote'
score_breakdown_json->'market_viability_flag'->>'demand_demote'
score_breakdown_json->'market_viability_flag'->>'radiance_growth_demote'
score_breakdown_json->'market_viability_flag'->>'rent_per_capita_demote'
score_breakdown_json->'value_pass'->>'value_uprank_applied'         -- legacy, deprecated
score_breakdown_json->'value_pass'->>'value_downrank_applied'       -- legacy, deprecated
```

### Suggested validation queries (run in Codespace; READ-ONLY SELECTs)

1. **Weight-sum integrity (per candidate):**
   ```sql
   SELECT id,
     (SELECT round(sum(v::numeric), 4)
        FROM jsonb_each_text(score_breakdown_json->'weights') AS t(k, v)) AS wsum
   FROM expansion_candidate
   WHERE search_id = :sid
   HAVING abs((SELECT sum(v::numeric)
                 FROM jsonb_each_text(score_breakdown_json->'weights') AS t(k,v)) - 100) > 0.001;
   -- expect 0 rows
   ```

2. **weighted_points == round(raw·weight/100, 2)** — iterate the `display`
   keys and compare `weighted_points` to `round(raw_input_score *
   weight_percent / 100, 2)`; expect 0 mismatches.

3. **Σ weighted_components == bonus_detail.base_deterministic:**
   ```sql
   SELECT id,
     (SELECT round(sum(v::numeric), 2)
        FROM jsonb_each_text(score_breakdown_json->'weighted_components') AS t(k,v)) AS wc_sum,
     (score_breakdown_json->'bonus_detail'->>'base_deterministic')::numeric AS base
   FROM expansion_candidate WHERE search_id = :sid
   HAVING abs((SELECT sum(v::numeric)
                 FROM jsonb_each_text(score_breakdown_json->'weighted_components') AS t(k,v))
              - (score_breakdown_json->'bonus_detail'->>'base_deterministic')::numeric) > 0.02;
   ```

4. **final_score column == post-bonus json final_score == clamp(base+total_delta):**
   ```sql
   SELECT id, final_score,
     (score_breakdown_json->>'final_score')::numeric AS json_final,
     (score_breakdown_json->'bonus_detail'->>'base_deterministic')::numeric
       + (score_breakdown_json->'bonus_detail'->>'total_delta')::numeric AS base_plus_delta
   FROM expansion_candidate WHERE search_id = :sid
   HAVING abs(final_score - (score_breakdown_json->>'final_score')::numeric) > 0.02;
   ```
   (Where `final_score_clamped = false`, `base_plus_delta` should equal
   `final_score` within rounding; where clamped, it is pinned to [0,100].)

5. **deterministic_rank monotonic in post-bonus final_score (flag-off):**
   ```sql
   SELECT id, deterministic_rank, final_score,
     row_number() OVER (ORDER BY final_score DESC, parcel_id ASC) AS expected_rank
   FROM expansion_candidate WHERE search_id = :sid
   QUALIFY deterministic_rank <> expected_rank;   -- or wrap as subquery in PG
   -- expect 0 rows: deterministic_rank must equal the (final_score DESC, parcel_id ASC) order
   ```

6. **Rerank flag-off invariant (production default):**
   ```sql
   SELECT id FROM expansion_candidate
   WHERE search_id = :sid
     AND NOT (deterministic_rank = final_rank
              AND deterministic_rank = rank_position
              AND rerank_applied = false
              AND rerank_delta = 0
              AND coalesce(rerank_status, 'flag_off') = 'flag_off');
   -- expect 0 rows while EXPANSION_LLM_RERANK_ENABLED=False
   ```

7. **Neutral-default audit** (how often each silent 50.0 fires) — count rows
   where `score_breakdown_json->'inputs'->>'<leg>' = '50'` for
   `landlord_signal`, `chain_strength`, `competition_whitespace`, to size the
   thin-data cohort.
