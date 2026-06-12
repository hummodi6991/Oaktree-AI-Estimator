# Scoring/ranking audit verification — 2026-06

**Scope:** Verify 10 candidate defects (from a chat-side audit of a possibly
stale bundled copy of `app/services/expansion_advisor.py`) against the live
repo at HEAD (`154296c35`, merge of PR #1305). Read-only investigation —
findings only, no patches.

**File under audit:** `app/services/expansion_advisor.py` (12,482 lines at
HEAD). All line anchors below refer to HEAD unless another file is named.

**Production config relevant to several findings** (verified, not assumed):

- `EXPANSION_WEIGHT_STACK=v2` in `k8s/deployment.yaml:43-44` (config default
  is `v1`, `app/core/config.py:427`).
- Hard floors enabled by default: population 20,000
  (`app/core/config.py:375-377`), brand presence 1 (`:378-380`),
  construction buffer 75 m (`:390-392`).
- Radiance rescue threshold 2.0 (`app/core/config.py:335-337`) and radiance
  demote threshold 2.0 (default in `expansion_advisor.py:5810-5812`).

**Probes committed** (read-only, for Ahmed to run via Codespace — see
`scripts/diagnostics/README_audit_probes_2026-06.md`):
`balancing_order_probe.sql`, `value_band_tier_bias_probe.sql`,
`viability_stack_depth_probe.sql`, `repost_duplicate_probe.sql`.

| # | Verdict | Severity | Pinning tests |
| --- | --- | --- | --- |
| 1 | CONFIRMED | ranking-integrity | none |
| 2 | CONFIRMED | ranking-integrity | none on the tier interaction |
| 3 | CONFIRMED | display-only | none (tests check pass-through only) |
| 4 | CONFIRMED | doc-drift + product question | tests pin **stacking** |
| 5 | PARTIAL | ranking-integrity (narrowed) | none |
| 6 | CONFIRMED | ranking-integrity (minor) | none |
| 7 | CONFIRMED | ranking-integrity | none |
| 8 | CONFIRMED | ranking-integrity | none |
| 9 | CONFIRMED | ranking-integrity (weight inflation) | none |
| 10a–h | see per-item | mixed | see per-item |

---

## Finding 1 — District balancing truncates before hard floors and score deltas

**Verdict: CONFIRMED** · Severity: **ranking-integrity** · Pinning tests: none
(no test exercises the balancing block or the balancing→floor ordering).

### 1.1 Pipeline order at HEAD (exact)

All inside `run_expansion_search`:

| Step | Anchor |
| --- | --- |
| `candidates.sort(key=_rank_sort_key)` | `expansion_advisor.py:10865` |
| `candidates = _dedupe_candidates(candidates)` | `:10867` |
| `candidates = _dedupe_score_clones(candidates, max_results=max(limit * 3, len(candidates)))` | `:10870` |
| District balancing block (`if len(target_districts) >= 2 ...`) | `:10881-10908` |
| `candidates = _apply_market_viability_pass(...)` | `:10918-10922` |
| `candidates = _apply_score_deltas_and_sort(candidates)` | `:10924` |
| `candidates = candidates[:limit]` | `:10926` |
| `candidates = _apply_rerank_to_candidates(...)` (ordering no-op in prod, flag off) | `:10937` |

Matches the claimed order exactly.

### 1.2 Balancing's second fill pass truncates at `limit`

`expansion_advisor.py:10899-10906`:

```python
# Second pass: fill remaining slots from the global ranked list
for c in candidates:
    if len(_balanced) >= limit:
        break
```

Confirmed: for multi-district searches the candidate list is cut to ~`limit`
**here**, before any hard floor or score delta runs. (The first pass,
`:10891-10897`, takes `min_per_district` from every district **without a
global cap**, so `_balanced` can also *exceed* `limit` when
`districts × min_per_district > limit` — see 1.5 and "new observations".)

### 1.3 Hard floors DROP with no backfill

`_apply_market_viability_pass`, `expansion_advisor.py:5903-5912`:

```python
if not population_floor_pass:
    dropped_population += 1
    continue
if not commercial_floor_pass:
    dropped_commercial += 1
    continue
if not construction_proximity_pass:
    dropped_construction += 1
    continue
survivors.append(c)
```

then `candidates = survivors` at `:5948`. There is no backfill from the
pre-balancing pool anywhere afterwards; the dropped slots are simply lost.
With all three floors enabled by default in production (pop 20,000 / brands 1
/ construction 75 m), a multi-district search that balanced down to 15
candidates and then loses N to floors returns 15−N rows.

### 1.4 Soft-leg percentile cohorts computed over the post-balancing list

All three cohorts are built over `out` = the floor-survivors of whatever list
reached the function:

- Population cohort `pop_values`: `:5957-5967` (quantile cut `:5972-5977`).
- Demand cohort `demand_values`: `:5990-6001` (cut `:6010-6016`).
- Rent-per-capita cohort `valid_rpc`: `:6033-6047` (cut `:6050-6058`).

Confirmed: for multi-district searches these "per-search bottom-quartile"
thresholds are computed over a ≤`limit`-sized list (typically 15), versus the
full deduped shortlist for city-wide searches (which skip the balancing
block, `:10881`). The statistical meaning of the same env-var thresholds
therefore differs by search type. Note the small-cohort guards (`< 4`
candidates → return, `:5949`, `:5969`; rpc skipped below
`EXPANSION_VIABILITY_RPC_MIN_COHORT=10`, `:6030-6048`) — quartiles over a
~15-candidate cohort are extremely coarse but do run.

### 1.5 Final `[:limit]` can void the balancing guarantee

`:10926` truncates after `_apply_score_deltas_and_sort` re-sorts strictly by
`(-final_score, parcel_id)` (`:5676-5681`). Two ways a district's guaranteed
representation dies after the balancing block:

1. Hard floors drop its only representatives (1.3) — no backfill.
2. When the first balancing pass overshoots `limit` (no global cap,
   `:10891-10897`), or viability deltas (−10/leg) push a district's
   representatives to the bottom, the final `[:limit]` evicts them.

The balancing comment promises "guarantee at least min_per_district
candidates from each district" (`:10878-10880`); nothing downstream preserves
that invariant.

**Blast radius:** scoring/result composition (which candidates exist at all),
plus the under-filled response sets surfaced to the UI. The API meta does
carry floor-drop diagnostics (`hard_floor_drops`,
`app/api/expansion_advisor.py:177-182`; populated at
`expansion_advisor.py:5933-5946`) so the under-fill is explainable, but not
prevented.

**Probe:** `scripts/diagnostics/balancing_order_probe.sql` — per
multi-district search: persisted count vs requested limit, distinct districts,
and floor-gate failures among persisted rows (expected 0 since failures are
dropped — under-fill is the fingerprint).

---

## Finding 2 — value_band is not tier-blind (ticket multiplier leaks into value_score)

**Verdict: CONFIRMED** · Severity: **ranking-integrity** (value_band drives a
±4/−6 final-score delta and UI badges) · Pinning tests: none pin the
tier→band interaction.

### 2.1 The multiplier is applied to the final return

`_estimate_revenue_index`, `expansion_advisor.py:4995-4999`:

```python
implied_check = _implied_average_check(price_tier, category)
ticket_multiplier = max(0.5, min(2.5, implied_check / _IMPLIED_CHECK_BASELINE_SAR))

return _clamp(base * factor * ticket_multiplier)
```

`_IMPLIED_CHECK_BASELINE_SAR = 50.0` (`:4852`); table at `:4808-4851`;
category throughput `factor` clamped to [0.88, 1.12] (`:4993`,
table `:4872-4882`).

### 2.2 Effective multiplier per (tier, category)

`ticket_multiplier = clamp(check/50, 0.5, 2.5)`; "combined" includes the
category throughput factor:

| Tier | Category | Implied check (SAR) | Ticket mult | × category factor | Combined |
| --- | --- | --- | --- | --- | --- |
| value | burger | 30 | 0.60 | 1.10 | **0.66** |
| value | coffee | 18 | 0.36 → clamp **0.50** | 1.08 | **0.54** |
| value | cafe | 25 | 0.50 | 1.05 | **0.53** |
| value | shawarma | 22 | 0.44 → clamp **0.50** | 1.12 | **0.56** |
| mid | burger | 55 | 1.10 | 1.10 | 1.21 |
| mid | coffee | 35 | 0.70 | 1.08 | 0.76 |
| mid | cafe | 48 | 0.96 | 1.05 | 1.01 |
| mid | shawarma | 38 | 0.76 | 1.12 | 0.85 |
| premium | burger | 95 | 1.90 | 1.10 | **2.09** |
| premium | coffee | 60 | 1.20 | 1.08 | **1.30** |
| premium | cafe | 80 | 1.60 | 1.05 | **1.68** |
| premium | shawarma | 65 | 1.30 | 1.12 | **1.46** |

### 2.3 value_score and band thresholds

- `_value_score` is the geometric mean `clamp(sqrt(rev * rb))` with an
  eps=1.0 floor: `:5476-5490`.
- Bands: `best_value` at ≥ 75 (`_VALUE_BAND_BEST_VALUE_MIN`, `:5472`,
  `:5496`), `above_market` at < 25 (`:5473`, `:5498`). Computed and persisted
  in `economics_detail` by `_economics_score` (`:5432-5460`), only in
  percentile rent-burden mode.
- The band feeds `_value_band_score_delta`: **+4** for high-confidence
  `best_value`, **−6** for `above_market` (`:5685-5700`), folded into
  `final_score` at `:5575`/`:5625-5630`.

### 2.4 The "tier-blind" contract comment

`expansion_advisor.py:5330-5335`:

> "Finding 2: price-tier multiplier applied to the ABSOLUTE rent ceilings only
> (absolute_fallback 220 / absolute_legacy 180). … The percentile path is
> intentionally NOT tier-adjusted — it is peer-relative and feeds
> value_score, **which must stay tier-blind.**"

The rent leg honors this; the revenue leg violates it: `estimated_revenue_index`
arrives at `_value_score` already multiplied by tier (`:5440`).

### 2.5 Math check

`revenue_index = clamp(base × factor × ticket_mult, 0, 100)` where
`base ≤ 100` by construction (`:4984-4990`).

**Premium tier — minimum base to pin revenue_index at 100:**

| Category | Combined mult | base needed for 100 |
| --- | --- | --- |
| burger | 2.09 | **47.9** |
| cafe | 1.68 | **59.5** |
| shawarma | 1.46 | **68.7** |
| coffee | 1.30 | **77.2** |

A premium burger brief pins revenue at 100 for any site with base ≥ 47.9 —
i.e. a thoroughly mediocre site. With rev = 100, `best_value` requires only
`sqrt(100 × rb) ≥ 75` → **rent_burden ≥ 56.25**. So for premium
burger/cafe briefs, the +4 `best_value` uprank is effectively conditional on
rent burden alone (≥ ~56, which is *better-than-median* rent — p50 maps to
burden 60, `:5297-5298`), exactly as claimed. Premium coffee is less
automatic (needs base ≥ 77).

**Value tier — maximum reachable value_score (base = 100, rb = 100):**

| Category | Max revenue_index | Max value_score | best_value (≥75) reachable? |
| --- | --- | --- | --- |
| burger | 66.0 | 81.2 | only if base ≥ 85.2 **and** rb ≥ 5625/rev — vanishingly narrow |
| shawarma | 56.0 | 74.8 | **never** (74.8 < 75 even at perfect inputs) |
| coffee | 54.0 | 73.5 | **never** |
| cafe | 52.5 | 72.5 | **never** |

Confirmed: `best_value` (+4) is mathematically unreachable for value-tier
coffee/cafe/shawarma briefs and near-unreachable for value-tier burger, while
being near-automatic (rent-burden-only) for premium-tier burger/cafe briefs.
The same listing gets a different "value for money" badge depending on the
searcher's price tier — the opposite of the documented intent.

**Blast radius:** final_score (±4/−6 delta), shortlist composition, the
value badge in the UI, decision memos and reports that surface
`value_band`/`value_score` (`economics_detail` is projected into the
recommendation report at `:12382-12386`).

**Probe:** `scripts/diagnostics/value_band_tier_bias_probe.sql` — value_band
distribution and `estimated_revenue_index ≥ 99.5` share grouped by persisted
`expansion_brand_profile.price_tier` (and by tier × category).

---

## Finding 3 — Stale loop-variable leakage: `_dd_used` / `cs["delivery_source"]`

**Verdict: CONFIRMED** · Severity: **display-only** (provenance label;
scoring confirmed untouched) · Pinning tests: none —
`tests/test_expansion_advisor_data_pipeline.py:419-461` only checks that
`_normalize_feature_snapshot` passes `delivery_source` through; nothing pins
the per-candidate value.

### 3.1 The stale bindings

First scoring pass (`for row in rows:` at `:8633`) binds, per iteration:

- `district_norm = normalize_district_key(district)` — `:8714`
- `_delivery_observed = (provider_listing_count >= 5 or provider_platform_count >= 2 or delivery_competition_count >= 2)` — `:8754-8758`

Second pass (`for prepared_item in prepared[:shortlist_size]:` at `:9899`)
reads both at `:10310-10320`:

```python
_dd_used = (
    not _delivery_observed
    and district_norm
    and district_norm in _district_delivery_stats
    and _district_delivery_stats[district_norm].get("total", 0) >= 5
)
cs["delivery_source"] = (
    "district_fallback" if _dd_used
    else "expansion_delivery_market" if ea_delivery_populated
    else "delivery_source_record"
)
```

Verified by scan: neither `_delivery_observed` nor `district_norm` is
re-assigned anywhere inside the second-pass loop body (`:9899-10835`). Both
hold the values from the **last iteration of the first pass** — `_dd_used`
and the `district_fallback` label are computed from an unrelated candidate's
delivery observation and district for every row in the shortlist.

### 3.2 The pattern was known — this site was missed

`expansion_advisor.py:9928-9932`:

```python
# Final-pass district_norm. The first scoring pass assigns
# district_norm at line ~5757 but that binding leaks across
# iterations of the per-row loop and does NOT track this
# shortlist iteration's candidate. Recompute locally.
district_norm_final = normalize_district_key(district) if district else None
```

The momentum consumer was fixed (`district_norm_final` used at `:10199`);
the rent recompute also re-derives its own `_district_norm_2` (`:10058`). The
`_dd_used` site at `:10310-10314` reads the leaked bindings 380 lines later.

### 3.3 Blast radius — every consumer of `context_sources.delivery_source`

Backend reads: **none**. The only writes are the `"legacy"` default
(`:2399`) and the stale site (`:10316`); no backend code reads the key back
(`grep get("delivery_source")` — no scoring/gating consumer). The memo
builder `app/services/llm_decision_memo.py` reads `context_sources` only for
`parking_evidence_band` (`llm_decision_memo.py:1341-1345`) — memos
unaffected.

Display consumers (wrong provenance label possible):

- Frontend score-component table:
  `frontend/src/features/expansion-advisor/scoreComponentMeta.ts:392, 399,
  500, 507, 599` — `delivery_source` overrides the displayed source for the
  delivery/demand evidence rows (tested in `scoreComponentMeta.test.ts:149-153`).
- Recommendation report: `context_sources` projected wholesale at
  `expansion_advisor.py:12374`, so report payloads carry the wrong label too.
- Docs contract: `docs/expansion_advisor_data_ingest.md:76` documents the
  field's values.

Scoring is untouched: the second pass takes all delivery-derived scores from
`prepared_item` (`:10025-10028`), which were computed in the first pass with
the correct per-row bindings. Also note `cs["delivery_observed"]` at `:10322`
uses the correctly-rebound `provider_listing_count` (`:9914`) — only the
`delivery_source` label is stale.

---

## Finding 4 — Viability delta stacking vs documented "single demote"

**Verdict: CONFIRMED (doc-drift; behavior is intentional-looking and
test-pinned)** · Severity: **doc-drift + open product question** · Pinning
tests: **tests pin the stacking semantics**, not the docstring.

### 4.1 Code vs docstring vs comment

Stacking code, `expansion_advisor.py:6274`:

```python
viability_delta = -10.0 * len(reasons)
```

with the refactor comment at `:6240-6242`: "attach the legs that fired and
the resulting delta (**-10 each, stacking**) to every candidate."

Docstring, `:5715-5717`:

> "Demote candidates that are confidently bad on the CEO-directive legs.
> Five independent legs, soft-demote on any (**single demote, never
> compounded**):"

(Also stale: the function now has **six** legs including rpc.)

rpc-leg comment, `:6078-6081`:

> "At-most-once demote: ``rpc`` runs first, so the already-demoted check is a
> defensive no-op (the existing demote loop applies a **single positional
> swap** per candidate regardless of how many legs fire)."

Both prose artifacts describe the pre-refactor positional-swap semantics; the
implementation stacks.

### 4.2 Maximum stack

All six legs can co-fire on one candidate: `rent_per_capita_high`,
`population_below_quartile`, `rent_high`, `economics_below_threshold`,
`demand_low`, `radiance_growth_low` (assembly `:6259-6271`). Feasibility:
the growth rescue (yoy ≥ 2.0, `:6145-6149`) and the radiance demote
(yoy < 2.0, `:6224-6229`) are mutually exclusive at the default thresholds,
so when the radiance leg fires the rescue is off and the pop/rent legs are
free to fire; rpc requires rent>0 ∧ pop>0 which is compatible with all
others. **Maximum stack = 6 × −10 = −60**, enough to send a 90-score
candidate to ~30 and (combined with `[:limit]`) effectively hard-drop it.

### 4.3 Tests pin stacking

- `tests/test_expansion_advisor_service.py:3009`:
  `assert target["viability_delta"] == -10.0 * len(target["viability_legs_fired"])`
- `tests/test_expansion_advisor_service.py:3172-3175`: two legs co-firing →
  `assert target_out["viability_delta"] == -20.0`

So a "fix" toward the docstring's single-demote would break tests; a fix
toward the code requires updating the docstring/rpc comment. **This ends in a
product question for Ahmed/Faisal:** is a confidently-bad-on-3-legs candidate
meant to be −30 (compounding evidence) or −10 (a flag is a flag)?

**Probe:** `scripts/diagnostics/viability_stack_depth_probe.sql` —
distribution of `jsonb_array_length(bonus_detail->'viability_legs_fired')`
and `bonus_detail->>'viability_delta'` over 30 days; how often ≥2 / ≥3 legs
co-fire, and which combinations.

---

## Finding 5 — Measured-zero population immune to population logic

**Verdict: PARTIAL (core mechanics confirmed; "immune to ALL population
logic" is overbroad)** · Severity: **ranking-integrity** (zero-pop sites
bypass the hard floor that drops 19,999-pop sites) · Pinning tests: none.

### 5.1 Hard floor: zero passes

`expansion_advisor.py:5858-5859`:

```python
if pop_val <= 0:
    population_floor_pass = True
```

A site with measured population 0 passes the 20,000 floor that drops a site
with 19,999.

### 5.2 Soft leg: zero is never "confident", never demotes

`:6180-6184`: `pop_confident = pop_reach > 0` → `pop_demote` stays False at
exactly 0. The rpc leg likewise skips it (`rent_v > 0 and pop_v > 0`,
`:6043`).

### 5.3 Measured zero IS produced by construction — do not downgrade

`_bulk_enrich_population` (`:7138-7231`) returns
`COALESCE(SUM(pd.population), 0)` via LEFT JOIN LATERAL (`:7212-7215`): any
candidate whose catchment contains **no `population_density` rows** gets a
literal `0.0`, indistinguishable from a true zero. Downstream:

- First pass: `population_reach = _safe_float(row.get("population_reach"))`
  (`:8643`) — so even a missing value becomes `0.0`, and the listing-pool SQL
  itself defaults `0 AS population_reach` (`:7126`).
- Snapshot write: `feature_snapshot_json["population_reach"] = population_reach`
  (`:10623-10624`) — always a float for listing candidates, never NULL.

Consequence: the defensive `pop_raw is None` branches in the floor (`:5851-5852`)
and soft leg (`:6177-6178`) are effectively dead for listing candidates; the
"missing data" cohort and the "measured zero" cohort are merged at 0.0, and
both sail through every viability-pass population check.

### 5.4 Why PARTIAL

Zero population is **not** immune to *all* population logic:
`_population_score(0)` returns 0.0 (`:2713-2714`), so the demand component
does penalize it (population is 60–80% of the demand blend,
`_demand_blend_weights` `:2771+`). The defect is precisely scoped to the
viability pass: the hard floor and both demote legs treat 0 as
unmeasurable-therefore-innocent while the enrichment pipeline guarantees 0 is
a real, common value.

**Blast radius:** scoring/result composition (floor bypass), gate display
(`population_floor_pass: true` persisted for zero-pop candidates).

---

## Finding 6 — Final sort drops rich tie-breakers

**Verdict: CONFIRMED** · Severity: **ranking-integrity (minor /
determinism-preserving)** · Pinning tests: none on the final key's richness
(determinism itself is asserted in weight-stack tests).

`_apply_score_deltas_and_sort` final sort key, `expansion_advisor.py:5676-5681`:

```python
candidates.sort(
    key=lambda _c: (
        -_safe_float(_c.get("final_score"), 0.0),
        str(_c.get("parcel_id", "")),
    )
)
```

exactly `(-final_score, parcel_id)`, as documented at `:5563-5564`.

`_rank_sort_key` with its 7-level key (gate verdict → zoning class → area
distance → economics → cannibalization → parcel_id; `:10836-10861`) is
applied only once, at `:10865`, **before** dedupe/balancing/viability/deltas.
After deltas, ties re-sort on `parcel_id` alone — i.e., effectively random
with respect to quality.

Tie likelihood: `final_score` is rounded to 2dp at `:5648-5649` on a 0–100
scale, and the delta legs add coarse quanta (±4/−6, −10·n, +2/+1). Scores are
also clamped at 0/100 (`:5630`), so the clamp boundaries are natural
accumulation points (e.g. several heavily-demoted candidates pinned at 0
order purely by parcel_id). Exact tie frequency left to a probe if needed —
not committed, since the mechanism is fully determined by the code above
(probe would only size it, and rank-adjacent ties among 2dp-rounded scores
within a 15-row response are visibly plausible).

**Blast radius:** ordering among equal-scored candidates only; deterministic
across re-runs (the parcel_id tie-break guarantees that), but a gate-failing
candidate can now outrank a gate-passing one at the same score, which
`_rank_sort_key` explicitly prevented earlier in the pipeline.

---

## Finding 7 — Dedupe effectively parcel_id-only for listings

**Verdict: CONFIRMED** · Severity: **ranking-integrity** (repost duplicates
can occupy multiple shortlist slots) · Pinning tests: none for the repost
case.

### 7.1 pid-bearing candidates never reach the spatial key

`_dedupe_candidates`, `expansion_advisor.py:939-946`:

```python
if parcel_id:
    if parcel_id in seen_pid:
        continue
    seen_pid.add(parcel_id)
    # Candidates with a real parcel_id skip spatial dedupe —
    # different parcels at nearby locations are genuinely distinct.
    result.append(c)
    continue
```

All production candidates carry a parcel_id:

- Listing pool: `cu.aqar_id AS parcel_id` (`:7108`), and
  `commercial_unit.aqar_id` is the **primary key**
  (`app/models/tables.py:404`) — non-null by construction. The
  candidate_location pool path likewise carries listing ids.
- Parcel pool: `COALESCE(source_id, id::text) AS parcel_id` (`:6950`) —
  non-null.

So the spatial+attribute key (`:948-970`) is dead in practice for the main
ranked list. An Aqar **re-post** (same physical unit, new `aqar_id`) is two
"genuinely distinct" candidates to this function.

### 7.2 `_dedupe_score_clones` requires exact rent equality and |Δscore| ≤ 0.3

`:993-999`: same district AND `abs(Δfinal_score) <= 0.3` AND
`estimated_rent_sar_m2_year` **exactly equal** AND area within 5%. The rent
micro-location multiplier (`round(base × mult, 2)`, `:8892`, recomputed with
road context at `:10068`) makes exact rent equality across two scrape rows
unlikely unless every input signal is byte-identical; reposts that changed
the asking price at all are guaranteed to pass.

**Probe (optional, committed):**
`scripts/diagnostics/repost_duplicate_probe.sql` — persisted same-search
pairs within ~30 m, area within 5%, different parcel_id, plus a breakdown of
why `_dedupe_score_clones` missed each pair (rent inequality vs score gap).

---

## Finding 8 — Shortlist-selection bias (preliminary vs final scoring)

**Verdict: CONFIRMED** · Severity: **ranking-integrity** · Pinning tests:
none.

### 8.1 Selection

`expansion_advisor.py:9064-9065`:

```python
prepared.sort(key=lambda item: item["preliminary_final_score"], reverse=True)
shortlist_size = min(len(prepared), max(limit, 25))
```

Only `prepared[:shortlist_size]` enters the second pass (`:9899`). Items
beyond the cut are **never** appended to `candidates` (the only
`candidates.append` in the search is inside the second-pass loop, `:10706`)
— they are not persisted, not re-scorable, gone.

### 8.2 Inputs that differ between preliminary and final scoring at HEAD

| Input | Preliminary (pass 1) | Final (pass 2, shortlist only) |
| --- | --- | --- |
| Demand: dg-index swap | pop_score blend only | `dg_composite` swap for dine_in (`:9966-9984`) and qsr (`:9994-10012`) when flags on |
| Demand: café foot-traffic bonus | absent | ±6-point bonus from `_bulk_foot_traffic` (`:10014-10019`) |
| Frontage/access/parking | zeroed road inputs, `road_context_available=False` (`:8922-8943`) | `_bulk_roads`/`_bulk_parking`/`_bulk_perimeter` enrichment (`:10300-10307`, `:10354+`) |
| Rent | micro-multiplier **without** road_context (`:8884-8891`) | recomputed **with** road_context (`:10059-10068`), changing rent, annual rent |
| Revenue index | `:8897-8906` | recomputed with the updated demand_score (`:10046-10055`) |
| Economics | `:8907-8920`, **no `cand_age_days`** | recomputed with new rent/revenue **plus `cand_age_days`** (`:10073-10087`) — also changes the rent-burden meta (age percentile) |
| Realized demand in snapshot | last-iteration broadcast risk handled | per-parcel lookup (`:10329-10343`) |

So a candidate's final score can move materially in either direction between
passes (road context alone swings frontage/access from neutral-unknown to
measured; the rent recompute changes economics and value_band inputs).

### 8.3 Worst case

A candidate at preliminary rank `shortlist_size + 1` whose pass-2 score
(with road context, dg-index, foot traffic, corrected rent) would exceed the
persisted rank-`limit` candidate **can never appear** — the preliminary score
is a hard, unrecoverable gate. With the default `limit=15`
(`app/api/expansion_advisor.py:161`), `shortlist_size = max(15, 25) = 25`, so
the bias window is everything from preliminary rank 26 to pool size (≤3500).
The bias systematically favors candidates that look good **without** road
context and penalizes candidates whose strength is road-context-dependent
(strong frontage on a measured major road, etc.).

---

## Finding 9 — Freshness double-paid in both weight stacks

**Verdict: CONFIRMED** · Severity: **ranking-integrity (weight inflation)** ·
Pinning tests: none assert single-payment of freshness; weight-stack tests
(`tests/test_expansion_weight_stack.py`) pin component weights but not the
bonus interaction.

### 9.1 Payment 1 — inside `_listing_quality_score`

- v1 (`_MOMENTUM_ENABLED` path): `freshness * 0.30` —
  `expansion_advisor.py:3242`.
- v2: `freshness * (0.30 / 0.65)` ≈ **0.4615** — `:3219` (renormalized after
  removing momentum from the sub-weights).
- Freshness bands from effective listing age at `:3166-3183` (≤14 d → 100,
  ≤30 d → 92, …).

`listing_quality` is then a top-level weighted component of the deterministic
score in `_score_breakdown`.

### 9.2 Payment 2 — flat bonus in `_apply_score_deltas_and_sort`

`:5598-5606`: +2.0 ("new", created ≤ 7 d) / +1.0 ("updated", refreshed ≤ 7 d)
added directly to `final_score`. This branch is **outside** the
`_stack_v2` conditional — only the momentum bonus is stack-gated
(`:5608-5623`). Both stacks pay freshness twice.

### 9.3 Contrast: v2 explicitly removed momentum's double-pay

Docstring `:5566-5569`:

> "Weight stack v2: the +2 momentum bonus is removed (**momentum is paid
> once**, via the district_momentum component in ``_score_breakdown``) and
> ``bonus_detail`` omits the ``momentum_bonus`` key."

Inline `:5608-5611`:

> "v2: momentum is already paid via the district_momentum component; the +2
> bonus **would double-pay it**."

And `_listing_quality_score`'s v2 comment `:3211-3214`: "momentum is paid
once, as the top-level district_momentum component … it no longer contributes
here."

The exact rationale that removed momentum's flat bonus applies verbatim to
freshness — which v2 not only kept double-paying but whose sub-weight share
v2 *increased* (0.30 → 0.4615 of listing_quality). No comment anywhere
acknowledges or justifies the asymmetry.

---

## Finding 10 — Minor items

**a. CONFIRMED** — `_brand_fit_score` flagship goal weights sum to 1.2:
`goal_component = _clamp(_area_component * 0.6 + visibility_signal * 0.4 + demand_score * 0.2)`
(`expansion_advisor.py:1678`). Other goal branches sum to 1.0
(`:1681`, `:1683`, `:1685`); flagship briefs get an inflated (often
clamp-saturated) goal component. Severity: ranking-integrity (flagship
archetype only).

**b. CONFIRMED** — `overlap_fit` ideal point: with default
`cannibalization_tolerance_m = 1800` (`:1648`),
`overlap_fit = clamp(100 − |cs − clamp((2500−1800)/25)| × 0.8)` (`:1649`)
peaks at `cannibalization_score = 28`. No-branch candidates get
`_cannibalization_score(None, …) = 0.0` (`:4418-4420`) →
`overlap_fit = 100 − 22.4 = 77.6`: zero cannibalization risk is *penalized*
22.4 points relative to a "just right" overlap of 28. Severity:
ranking-integrity (counterintuitive but bounded; 14% weight, `:1706`).

**c. CONFIRMED** — dead line in `_rent_micro_location_multiplier`:
`district_avg_per_parcel = district_delivery_stats["total"] / max(1, district_delivery_stats.get("total", 1))`
(`:4610`) always evaluates to ~1.0 and is never read. Severity: doc-drift
(dead code only; the real signal is computed at `:4614-4623`).

**d. CONFIRMED** — competitor double-count: `_bulk_enrich_competitors` counts
`restaurant_poi` (`:7387-7404`) `UNION ALL` (`:7405`)
`delivery_source_record` (`:7409-7420`) with no cross-source identity
matching, so a venue present in both (Google Places + HungerStation) counts
twice in `competitor_count` (`:7344`). Note `delivery_source_record` carries
a `matched_poi` column (index `ix_dsr_matched_poi`,
`alembic/versions/0014_delivery_source_tables.py:120-127`) that could
de-overlap this but is unused here. Severity: ranking-integrity (inflates
competition; the docstring `:7249-7252` even documents the union as
intentional for category coverage, not the double-count).

**e. CONFIRMED** — `_delivery_observed` comment/code drift: comment says
"Thresholds: **≥3 total listings OR ≥2 platforms OR ≥1 same-category
competitor**" (`:8752-8753`); code is `>= 5 / >= 2 / >= 2` (`:8754-8758`).
Severity: doc-drift.

**f. CONFIRMED** — floor gates write `True` when disabled rather than `None`:
`pop_floor <= 0 → population_floor_pass = True` (`:5849-5850`), same for
commercial (`:5867-5868`) and construction (`:5884-5885`), all written
unconditionally into `gate_status_json` (`:5899-5901`). The gate vocabulary
elsewhere uses tri-state `True/None/False` with `None` = not evaluated
(e.g. `_rank_sort_key` gate_rank `:10848-10849`); a disabled floor displayed
as "passed" misrepresents that it never ran. Severity: display-only.

**g. CONFIRMED** — `_chain_strength_score` docstring says MAX:
"Higher ``max_chain_strength`` (**max chain_strength_score** from
``expansion_competitor_quality`` …)" with parameter named
`max_chain_strength` (`:2911-2917`), but both call sites pass the
strong-chain **share** (`chain_strength_share`, `:8733` and `:9910-9912` —
the latter's comment says so explicitly: "Leg input is the strong-chain
SHARE"). The SQL comment also confirms share "replaces the MAX above"
(`:7347-7357`). Severity: doc-drift.

**h. CONFIRMED** — `_percentile_rent_burden` runs up to **5 sequential SQL
queries per candidate**: the fallback chain holds up to 5 entries (3
neighborhood tiers `:5208-5226` + `city_band_type` `:5228-5233` + `city`
`:5234-5239`), executed sequentially at `:5241-5274`. It is invoked from
`_economics_score` (`:5372-5373`) which the FIRST pass calls per row
(`:8907-8920`) over the **full candidate pool** — bounded by
`_CANDIDATE_POOL_LIMIT = 3500` (`:42`, passed at `:8056`; 600 on the
commercial_unit fallback path `:8112`) — and the second pass repeats for the
shortlist (`:10073-10087`). Worst case ≈ 3500 × 5 + 25 × 5 sequential
percentile aggregations per search. Severity: **performance** (it is the
dominant per-candidate DB cost in the scoring loop; note each query is a
full-scan-ish aggregate over `commercial_unit` with non-trivial predicates).

---

## New observations (NOT investigated — listed only)

1. **First-pass `_economics_score` omits `cand_age_days`** (`:8907-8920` vs
   `:10073-10087`): the preliminary economics (and hence shortlist selection,
   Finding 8) never sees the listing-age percentile inputs.
2. **`cs["delivery_observed"]` threshold inconsistency** (`:10322`):
   `provider_listing_count > 0` vs `_delivery_observed`'s ≥5/≥2/≥2 — two
   different "observed" definitions in the same `context_sources` block.
3. **Balancing first pass has no global cap** (`:10891-10897`): `_balanced`
   can exceed `limit` when `districts × min_per_district > limit`, making the
   final `[:limit]` eviction (Finding 1.5) more likely; the `_unknown`
   district bucket also competes for guaranteed slots.
4. **`delivery_source_record.matched_poi` is unused by
   `_bulk_enrich_competitors`** — an existing column that could fix 10d
   without new matching infrastructure.
5. **`_dedupe_candidates` spatial-key arithmetic** uses float floor-division
   (`round(lat, 4) // 0.0005 * 0.0005`, `:950`) — bucket-boundary jitter for
   the (currently dead) spatial path.
6. **Docstring leg count drift** in `_apply_market_viability_pass`: "Five
   independent legs" (`:5717`) — six legs exist (rpc added later).

---

## Recommended patch order (independent PRs, probe-before-patch)

1. **PR-1 (Finding 1 + obs 3)** — pipeline-order fix: run hard floors before
   (or backfill after) the district-balancing truncation; re-assert district
   representation after the final sort/limit. Run
   `balancing_order_probe.sql` first to size production impact; its
   under-fill counts are also the post-patch acceptance metric.
2. **PR-2 (Finding 2)** — make the value_score revenue leg tier-blind (feed
   the pre-multiplier base, or divide the multiplier back out) while leaving
   the persisted `estimated_revenue_index` semantics for ranking unchanged
   pending a separate decision. Run `value_band_tier_bias_probe.sql` first;
   expect the premium best_value share to normalize after the patch.
3. **PR-3 (Finding 5)** — distinguish "no population data in catchment" from
   measured zero in `_bulk_enrich_population` (NULL instead of COALESCE-0, or
   an explicit coverage flag), then let the hard floor treat genuine zeros as
   failures. Small migration-free change but touches the snapshot contract.
4. **PR-4 (Findings 8 + 9 + 6, scoring-loop consistency)** — widen or
   re-rank-aware the shortlist cut (8), single-pay freshness in v2 mirroring
   the momentum rationale (9), and reuse the rich tie-breaker key in the
   final sort (6). These interact (all change final ordering) so they should
   ship together behind one validation run, but each is a small diff.
5. **PR-5 (Finding 7 + 10d)** — repost dedupe (spatial key as a *secondary*
   check even for pid-bearing candidates, or aqar repost detection upstream)
   and competitor de-overlap via `matched_poi`. Run
   `repost_duplicate_probe.sql` first — if survivor pairs are rare, deprioritize.
6. **PR-6 (Finding 3 + 10f, display)** — recompute `_dd_used` from the
   current shortlist item (mirroring the `district_norm_final` fix) and write
   `None` for disabled floor gates. No ranking impact; safe to ship anytime.
7. **PR-7 (Finding 10h, performance)** — cache/batch the rent-percentile
   aggregates (the comparable cohorts are keyed by
   (neighborhood, band, listing_type), not by candidate, so one query per
   distinct cohort per search suffices). Independent of all scoring decisions.
8. **PR-8 (doc-only)** — Finding 4 docstring/rpc-comment vs code **after**
   Ahmed/Faisal decide stacking vs single-demote (probe:
   `viability_stack_depth_probe.sql`), plus 10c dead line, 10e threshold
   comment, 10g MAX→share docstring, and the "Five legs"→six count.

Probe-before-patch applies to PR-1, PR-2, PR-5, and the Finding-4 decision;
the rest are fully determined by code inspection above.
