# Weight-stack audit — 6 findings: evidence, code map, decision framings

**Scope:** investigation only (no application code changed). All citations are
against the current synced tree (branch `claude/investigate-weight-audit-yoa74w`,
base `42331328e`). Probes for Ahmed to run in Codespace:

```
psql -x -f scripts/diagnostics/qsr_whitespace_probe.sql > /tmp/qsr_ws.txt 2>&1
psql -x -f scripts/diagnostics/delivery_demand_legs_probe.sql > /tmp/dd_legs.txt 2>&1
psql -x -f scripts/diagnostics/chain_strength_input_probe.sql > /tmp/chain_in.txt 2>&1
```

**Top-level weights** (verified, `app/services/expansion_advisor.py:3450-3461`;
chain weight env-driven, `app/core/config.py:395-397`):
occupancy_economics 26.2924 / listing_quality 22.0 / brand_fit 9.6404 /
landlord_signal 7.0112 / competition_whitespace 5.7640 (= 8.7640 − 3.0,
`:3448-3449`) / chain_strength 3.0 / demand_potential 8.7640 /
access_visibility 8.7640 / delivery_demand 4.3820 / confidence 4.3820.
Brand-brief multipliers then re-weight and renormalize to 100
(`:3462-3480`, gain `EXPANSION_BRAND_WEIGHT_GAIN` default 0.35,
`app/core/config.py:423-425`).

---

## Item 1 — QSR competition_whitespace flooring

### How production counts QSR same-category competitors (verified)

- Radius: `_CATCHMENT_RADII_M["qsr"]["competition"] = 1200.0`
  (`app/services/expansion_advisor.py:830`), resolved in
  `_bulk_enrich_competitors` via `_catchment_radii(service_model)["competition"]`
  (`:6833-6837`), called with `service_model` for both the candidate_location
  pool (`:7621`) and the commercial_unit fallback pool (`:7666`).
- Count = two sources UNION'd inside one LATERAL (`:6924-6964`):
  1. `restaurant_poi` where `lower(rp.category) = ANY(:category_keys)` and
     `business_status` is NULL or `'OPERATIONAL'` (`:6930-6947`);
  2. `delivery_source_record` where `lower(category_raw|cuisine_raw) ~*
     :category_regex` (`:6952-6963`), geography `ST_DWithin` at `:radius_m`
     (`:6943-6947`, `:6959-6963`).
- Alias expansion: `category_keys` / `category_regex` from `_expand_category`
  (`:558-574`) backed by `_CATEGORY_ALIAS_MAP` (`:154-223`). For a `burger`
  brief: keys `{burger}`, regex `burger|hamburger|برجر`. For `fast food`:
  keys `{burger, pizza, chicken, fast_food}` and a 15-pattern regex.
- Fallback only: if the bulk enrichment raises, the pool-SQL count stands —
  `restaurant_poi` only, **no business_status filter, no delivery side**
  (`:7249-7259`). The probe mirrors the primary (bulk) path.
- Curve: `_competition_whitespace_score` (`:2694-2739`):
  `raw = 100·(1 − log1p(count)/log1p(REF))`, floored at 15.0 (`:2736-2739`).
  `qsr` is absent from `_WHITESPACE_LOG_REF` (`:2670-2690`) so it uses the
  default REF = 25 (`:2691`, `:2732-2734`). F4: count 0 scores 100 only when
  `confident` (`:2728-2731`).

**Exact floor onsets** (recomputed: first integer count with raw ≤ 15, i.e.
`c* = e^(0.85·ln(1+REF)) − 1`): REF=25 → count ≥ 15 (c\*=14.95);
REF=50 → count ≥ 28 (c\*=27.27); REF=75 → count ≥ 39 (c\*=38.70).
The fresh-run evidence (counts 20–45 at 1200 m) is fully consistent: every
count ≥ 15 floors under REF=25, hence 10/15 candidates flat at 15.00.

### Probe

`scripts/diagnostics/qsr_whitespace_probe.sql` — Phase-A candidate CTE
(Tier-1 cluster primaries, mirroring
`scripts/diagnostics/l1_signal_distributions.sql:46-59`), counts at
800/1000/1200 m for two category scopes (`burger` narrow, `fast food` broad),
full percentile set, and the exact curve grid for REF ∈ {25, 50, 75} ×
radius × scope with floored-% and score at the p25/p50/p75 counts.

**Approximation bias (stated per the brief):** unlike the old delivery_first
probe (simplified match, no alias expansion → systematic under-count), this
probe uses the exact production keys/regex and both sources, so it is
near-exact for `burger` / `fast food` briefs. Residual bias: rows of
`delivery_source_record` with NULL `geom` are skipped (production's lat/lon
fallback mode would count them) → **mild under-count**; other QSR categories
(chicken, pizza) have narrower key sets than the `fast food` scope → read the
scope matching Ahmed's brief.

### Recommendation — **PENDING PROBE OUTPUT**

Settle (radius, REF) the same way delivery_first was settled
(`docs/fix-delivery-first-competition-whitespace-report.md`): pick the radius
where per-candidate variation lives, then set REF so the p25–p75 count band
maps to a spread score range and only the genuinely saturated tail (≥ p90-ish)
floors. Expected shape given the 20–45 @ 1200 m evidence: either
(1200 m, REF=75) — floors only count ≥ 39 — or (1000 m, REF=50) matching the
dine_in/delivery_first pattern if the discriminating variation lives at
1000 m. Finalize only after the probe returns; note the whitespace component's
*effective* weight is ~7.4 pts, not its nominal 5.764 (see Item 6), so this
fix moves more than its weight suggests.

---

## Item 2 — delivery_demand: supply-as-demand

### Code map (verified, with `path:line`)

| What | Where | Notes |
|---|---|---|
| `delivery_listing_count` (initial) | pool SQL `:7272-7297` | `delivery_source_record`, regex match, at `:demand_radius_m` (qsr 1500 m) |
| `delivery_listing_count` (effective) | bulk enrichment `:7800-7828`, applied `:8230-8233` | `cat_count` from `expansion_delivery_market` (`EXPANSION_DELIVERY_TABLE`, `config.py:72`), category via `_expand_category_terms` (`:527-555`, broad buckets), **hardcoded 1200 m radius** (`:7817`) for every service model |
| `delivery_competition_count` | `:8233` | set equal to `delivery_listing_count` post-enrichment — same number |
| realized_demand | `:7843-7939` | Σ per-branch `GREATEST(0, MAX−MIN)` of `rating_count` over ≥2 snapshots in `expansion_delivery_rating_history`, window 30 d (`config.py:99-101`), radius `EXPANSION_REALIZED_DEMAND_RADIUS_M` = 1200 m (`config.py:102-104`), category regex on `category_raw|cuisine_raw` (`:7857-7862`) |
| realized gate | `:8236-8239` | leg active only when value present AND ≥ 3 contributing branches |
| blend | `_delivery_score` `:2610-2650` | `listing = clamp(√(count/40)·100)` (`:2634-2638`); `realized = clamp(√(rd/263)·100)` (`:2645-2648`, REF `config.py:118-120`); `score = listing·(1−w) + realized·w` (`:2649-2650`), `w = EXPANSION_REALIZED_DEMAND_BLEND` default 0.5 (`config.py:107-109`), wired at `:8249` and `:8369` |
| demand blend | `:8251-8252`, recompute `:8366-8371` | `demand_score = pop·w_pop + delivery·w_del`; (w_pop, w_del) per `_demand_blend_weights` (`:2661-2666`): qsr (0.60, 0.40), delivery_first (0.40, 0.60), cafe (0.55, 0.45), dine_in (0.75, 0.25) |
| L1 second pass | `:9504-9507` (dine_in), `:9530-9535` (qsr) | when the dg-index scoring flags are on (defaults false in code, `config.py:158-161`, `:175-179`), `pop` is swapped for the L1 composite but the **delivery term is kept unchanged** (`prepared_item["delivery_score"]`, `:9506`, `:9534`) |
| `delivery_demand` component | `:8490-8499` → `:3500` | = `provider_intelligence_composite` = `density·0.36 + provider_whitespace·0.38 + (100−delivery_competition)·0.26` — **`_delivery_score` does NOT feed it** |

**Hypothesis verdict:** confirmed in substance, mislocated in name.
`_delivery_score` feeds **`demand_potential`'s delivery leg only** — not the
`delivery_demand` component. The listing leg (supply) carries, for qsr:
`8.7640% × 0.40 × (1−0.5) = 1.7528` final-score points when realized is
active, and `8.7640% × 0.40 = 3.5056` points when it is not (the leg falls
back to listing-only, `:2639-2640`). Via the Item-6 leakage
(demand → economics + brand_fit), multiply by ≈1.30: ~2.3 / ~4.6 effective
points rewarding delivery branch density — against competition_whitespace's
~7.4 effective points penalizing it, computed from a *different* count
(POI + DSR at the competition radius) that the probe correlates.

Also note the same count is double-booked with opposite signs elsewhere:
`delivery_competition_count = delivery_listing_count` (`:8233`) drives
`provider_whitespace_score` down and `delivery_competition_score` up
(`:8290-8318`), both inside `delivery_demand` — so supply raises
demand_potential while lowering delivery_demand simultaneously.

### Blend shift 0.3/0.7 — env-only or code?

**Env-only is sufficient for the blend itself:** `EXPANSION_REALIZED_DEMAND_BLEND=0.7`
gives `listing·0.3 + realized·0.7` exactly (`:2649-2650`; no other consumer of
that setting). Two caveats the probe must size:

1. It changes nothing for candidates where the realized leg is inactive
   (< 3 branches / no history) — they stay 100% listing-count. Coverage
   (probe Output 1) decides whether an env flip alone moves the needle.
2. "Listing-leg-as-floor-only" (e.g. `score = max(realized, floor(listing))`)
   **requires a code change** in `_delivery_score`.

Final number **PENDING PROBE OUTPUT** (`delivery_demand_legs_probe.sql`:
coverage, leg correlations vs whitespace/competitor_count, realized
distribution vs the 263 anchor, and the per-candidate delta of a 0.5→0.7
shift).

---

## Item 3 — chain_strength: dead-weight check

### Leg mechanics (verified)

Input = strong-chain share from `_bulk_enrich_competitors` (`:6908-6915`):
share of in-category, ECQ-matched POIs with `chain_strength_score ≥ 60`
(`EXPANSION_CHAIN_STRONG_THRESHOLD`, `config.py:409-411`); NULL when matched
POIs < 3 (`EXPANSION_CHAIN_MIN_MATCHED`, `config.py:412-414`); NULL → neutral
50.0 in `_chain_strength_score` (`:2742-2758`). Only the `restaurant_poi`
side can match — the delivery side contributes NULL `chain_strength` by
construction (`:6952-6956`). Weight 3.0% (`:3448`, `config.py:395-397`).

`scripts/diagnostics/chain_strength_input_probe.sql` measures: % neutral-50
(disambiguated — stored JSON cannot tell "thin <3 matched" from a true 50.0
share, so Part B re-derives the share spatially with the exact production
predicate), the real-share distribution, ECQ canonical coverage, the implied
chain-size ladder, `restaurant_poi.chain_name` null rate, and `brand_alias`
size.

### Scoping the "queued fix" — ⚠ already landed

The framing described a queued fix: *"compute chain_size from `name` with
normalization"* because `restaurant_poi.chain_name` is ~99.9% null. **That fix
is already implemented** in `app/ingest/expansion_advisor_competitors.py`:

- The `chain_counts` CTE (`app/ingest/expansion_advisor_competitors.py:199-218`)
  groups **normalized `name`** — `_CHAIN_NAME_NORM_SQL` (`:54-66`: case-fold,
  Arabic Alef-variant collapse, Ya-Maksura → ي, tatweel strip,
  non-alphanumeric→space, whitespace squeeze), with a Python mirror
  `_normalize_chain_name` (`:69-92`) kept in lockstep by unit tests
  (`:50-52`), a generic-name denylist (`:101-118`), an OPERATIONAL filter
  (`:210-212`), and `HAVING COUNT(*) >= 5` (`:216`).
- Names are further collapsed to `brand_alias.canonical_brand_id` when an
  alias row exists (`:202`, `:214`), so bilingual/casing variants aggregate
  (post-#1157). `chain_strength_score = LEAST(100, chain_size·12)` (`:245`).
- `chain_name` is used **only** as a display fallback for `brand_name`
  (`:234`) — it never feeds `chain_size`. The ~99.9% null rate (comment
  `:43-44`; probe Part C verifies the number) is the *reason* the
  normalization exists, not a current starvation.

**What actually remains for a future single-purpose PR** (if the probe shows
the leg is still mostly neutral):

1. **`brand_alias` coverage expansion** — the bilingual merge ("Starbucks" vs
   "ستاربكس") is explicitly out of scope of the normalizer (`:46-48`); the
   candidate generator already exists
   (`scripts/diagnostics/generate_brand_alias_candidates.py`). Touches: data
   only (brand_alias rows) + an ECQ ingest re-run.
2. **Threshold/min-matched calibration** (`EXPANSION_CHAIN_STRONG_THRESHOLD`,
   `EXPANSION_CHAIN_MIN_MATCHED`) — env-only.
3. Backfill implication for either: re-run the competitors ingest
   (`_build_competitor_quality` does a city-scoped DELETE + re-INSERT,
   `:140-142`, `:220-334`); no schema change, no index work (ECQ indexes
   already cover `restaurant_poi_id`, `geom`, `category`;
   `alembic/versions/d4e5f6a1b2c3_create_expansion_advisor_tables.py:146-150`).

---

## Item 4 — access_visibility sensitivity double-count (code facts, no patch)

### The two (actually three) applications — confirmed

1. **Score domain** (`:2224-2230`): `_access_visibility_score` returns
   `clamp((frontage·blend + access·(1−blend)) · (0.75 + vw·0.25))` where
   `vw = _sensitivity_weight(visibility_sensitivity)` (low 0.3 / medium 0.6 /
   high 1.0, `:1530-1531`) and `blend = 0.5 + fw·0.2` (frontage sensitivity,
   `:2227`). Only **visibility** sensitivity scales the magnitude.
2. **Weight domain** (`:3353-3358`): `_brand_weight_multipliers` lifts the
   access_visibility *weight* by `1 + g·site_sig` with
   `site_sig = max(σ(parking), σ(frontage), σ(visibility))`,
   `σ(level) = (w−0.6)/0.4` (low −0.75 / medium 0 / high +1.0, `:3350-3351`),
   `g = 0.35` (`config.py:423-425`); weights then renormalize to 100
   (`:3462-3480`). Channel/goal add further multipliers (`:3360-3376`).
3. **Third application** (missed by the framing): `_brand_fit_score` weighs
   `visibility_signal · (0.08 + vw·0.05)` (`:1609`) — and `visibility_signal`
   *is* the already-multiplied `access_visibility_score` (`:8485`), so
   visibility sensitivity is applied a third time inside brand_fit.

### Exact combined effect (visibility-only knob, others medium, g=0.35)

| brand sensitivity | score-domain cap (max raw) | weight multiplier | effective weight after renorm | max weighted points |
|---|---|---|---|---|
| low (all three site knobs low) | 100×0.825 = **82.5** | 0.7375 | 8.7640×0.7375 / 97.69945 ×100 = **6.6156%** | 0.825 × 6.6156 = **5.458** |
| medium | 100×0.90 = **90.0** | 1.0 | **8.7640%** | 0.90 × 8.7640 = **7.888** |
| high | **100.0** | 1.35 | 8.7640×1.35 / 103.0674 ×100 = **11.4793%** | 1.00 × 11.4793 = **11.479** |

High-to-low ratio in achievable points: **2.103×** — the same preference is
compounded across two domains (plus the brand_fit slope: low 0.095 / medium
0.110 / high 0.130 × 9.6404% = 0.916 / 1.060 / 1.253 pts per 100 raw).
Asymmetry: the weight domain keys on the **max** of three knobs; the score
domain keys on visibility alone — a parking-high/visibility-low brand gets
weight 11.4793% but a raw cap of 82.5.

### Test coverage

The weight-domain behavior is pinned
(`tests/test_expansion_advisor_service.py:764-833`: neutral no-op `:764`,
gain-zero disable `:792`, high-parking lift `:808`, delivery-channel lift
`:822`). **No test pins the score-domain `0.75 + vw·0.25` multiplier** —
removing it would break no existing assertion.

### Decision framing for Ahmed

- **(a) Keep as-is.** Sensitivity acts in both domains; a medium brand can
  never exceed 90 raw, so cross-brand score comparability is permanently
  skewed and the component's spread is compressed ×0.825–0.90 for non-high
  brands.
- **(b) Remove the score-domain multiplier; keep Finding-1 weight
  multipliers.** *(Recommended.)* One domain (weights) expresses brand
  preference; raw inputs become a pure site measurement; medium brands reach
  100. Rank-shift blast radius: within a search the multiplier is a constant
  (one brand profile per search), so removal preserves the component's rank
  order and only **widens its spread** — ×1/0.9 = +11.1% for medium-sensitivity
  briefs, ×1/0.825 = +21.2% for all-low briefs, 0% for visibility-high briefs.
  Max per-candidate final-score movement: medium ≤ 10 raw × 8.7640% ≈ 0.88 pts
  (+ ≤0.11 pts via the brand_fit term); low ≤ 17.5 × 6.6156% ≈ 1.16 pts.
  Only near-ties (final scores within ~1 pt) can flip, and only on
  non-visibility-high briefs.
- **(c) Renormalize so medium reaches 100** (divide by 0.9, i.e. multiplier
  → `0.8333 + vw·0.2778`). Keeps dual-domain coupling and the low-brand cap
  (91.7); more code for less clarity than (b).

---

## Item 5 — cafe demand-blend docstring drift (history check, no patch)

- **Live behavior:** dict says `"cafe": (0.55, 0.45)`
  (`app/services/expansion_advisor.py:2664`); the docstring says
  "cafe: moderate population bias (0.70 / 0.30)" (`:2658`). The dict IS the
  scored path: `_demand_blend_weights(service_model)` → `demand_score =
  clamp(pop_score·0.55 + delivery_score·0.45)` for cafe (`:8251-8252`,
  recompute `:8371`; the cafe-specific second-pass foot-traffic bonus at
  `:9540-9543` adds on top and doesn't touch the blend).
- **History** (full clone fetched for this check):
  - `aa8da394e6` (2026-03-28, "Add service-model-aware demand blend weights
    for delivery score") introduced both the docstring and the dict at
    **(0.70, 0.30)** — consistent at birth.
  - `92268f741c` (2026-04-01, "fix: café category mapping, demand blend, and
    foot-traffic scoring") **deliberately** changed the dict to
    **(0.55, 0.45)** with explicit rationale in the commit message — "P4:
    Shift café demand blend from 70/30 to 55/45 pop/delivery ratio to reduce
    score compression from uniform population signals" — and did not update
    the docstring.
- **Verdict: fix-docstring-to-code.** The 0.55/0.45 dict is the deliberate,
  reasoned decision; the 0.70/0.30 docstring line is drift. One-line comment
  fix, zero behavior change.

---

## Item 6 — component orthogonality (leakage) map

Every path where one top-level component's input feeds another component.
Exact extra weight = source-slope × intermediate weights × component weight
(all at balanced-goal / medium-sensitivity / default-confidence values;
points are per 100 units of the leaked input).

| # | Leaked signal | Destination path | Exact extra weight (pts) |
|---|---|---|---|
| 1 | `demand_score` | brand_fit default goal `(demand+fit+whitespace)/3` (`:1585`) × goal 0.20 (`:1604`) | 9.6404 × 0.20 × ⅓ = **0.6427** |
| 2 | `demand_score` | `_estimate_revenue_index` demand·0.20 (`:4614`, `:4623`) → economics revenue·0.38 (`:5015-5017`) | 26.2924 × 0.38 × 0.20 = **1.9982** (rises to 3.0499 when rent-burden confidence is 0, `:5014-5015`) |
| 3 | `whitespace_score` | revenue_index whitespace·0.10 (`:4617`, `:4624`) → economics | 26.2924 × 0.38 × 0.10 = **0.9991** (→1.5249) |
| 4 | `whitespace_score` | brand_fit default goal (`:1585`) | **0.6427** |
| 5 | `provider_whitespace_score` | delivery_demand ×0.38 (`:8497`) | 4.3820 × 0.38 = **1.6652** |
| 6 | `provider_whitespace_score` | brand_fit direct ×0.08 (`:1610`); default goal (`:1585`); delivery_led goal ×0.35 (`:1583`) | **0.7712**; **0.6427**; (0.6748 when delivery_led) |
| 7 | `provider_density_score` | delivery_demand ×0.36 (`:8496`); delivery-channel fit ×0.7×0.14 (`:1537`, `:1605`); delivery_led goal ×0.35×0.20 (`:1583`) | **1.5775**; **0.9448**; **0.6748** |
| 8 | `delivery_competition_score` | delivery_demand (100−x)·0.26 (`:8498`); delivery_led goal (100−x)·0.3·0.2 (`:1583`) | **1.1393** (inverted); **0.5784** (inverted) |
| 9 | `access_visibility_score` | brand_fit ×(0.08+vw·0.05) (`:1609`, input wiring `:8485`); flagship goal ×0.4×0.2 (`:1578`) | medium **1.0604**; flagship +**0.7712** |
| 10 | image signal | listing_quality image ×0.10 (`:2989-2996`, `:3024`) at weight 22 (`:3452`); confidence +10/100 listing path (`:2789-2790`) | **2.2000**; **0.4382** — image counts ~2.64 pts total |
| 11 | `delivery_listing_count` | demand delivery leg (Item 2): qsr 8.7640×0.40 via `_delivery_score`; parcel-path confidence +15/100 (`:2805-2806`) | **3.5056** (listing share 1.7528 at blend 0.5); **0.6573** (parcels only — production pool is listings, `:799`) |
| 12 | `population_reach` | demand pop leg; confidence +5/100 listing path (`:2794-2795`) | 8.7640×w_pop; **0.2191** |

Non-top-level cross-feeds for completeness: `fit_score` → brand_fit
×(0.12+fw·0.03) (`:1608`) **and** economics ×0.15 (`:5021`);
`cannibalization_score` → economics ×0.13 (`:5020`) and brand_fit overlap
×0.14 (`:1606`). `chain_strength`, `landlord_signal`, `confidence` feed
nothing else (verified: breakdown-only consumers, `:3489-3501`).

**Effective (nominal + leaked) weights for the audit's main actors:**

- `demand_potential`: 8.7640 + 0.6427 + 1.9982 ≈ **11.40 pts** (+30.1%)
- `competition_whitespace`: 5.7640 + 0.9991 + 0.6427 ≈ **7.41 pts** (+28.5%)

**Do the leaks change Items 1–4?** Direction: no. Magnitude: yes —
(i) the Item-1 REF/radius fix moves ~1.29× its nominal weight, strengthening
the case; (ii) the Item-2 supply-as-demand leg is amplified the same way
(~2.3 / ~4.6 effective pts), strengthening the blend-shift case; (iii) Item-3
is unaffected (chain_strength is leak-free); (iv) Item-4's option (b) must
account for the brand_fit third application (quantified above — it survives
the score-domain removal and is small, ≤ 0.13 pts per 10 raw).

---

## Discrepancies & framing

1. **Item 3's "queued fix" is already merged.** The framing treats "compute
   chain_size from `name` with normalization" as future work starved by
   `chain_name` nulls. The `chain_counts` CTE has computed chain_size from
   normalized `name` (+ `brand_alias` canonicalization) since #1157
   (`app/ingest/expansion_advisor_competitors.py:199-218`); `chain_name` is
   display-only (`:234`). The remaining lever is `brand_alias` coverage
   (bilingual merge, `:46-48`) plus env calibration — see Item 3.
2. **The referenced delivery_first probe file does not exist.**
   `docs/fix-delivery-first-competition-whitespace-report.md:125` points to
   `scripts/diagnostics/delivery_first_whitespace_probe.sql`, which was never
   committed (verified across full history). The QSR probe therefore mirrors
   the committed Phase-A CTE (`l1_signal_distributions.sql:46-59`) instead of
   "reusing its CTE", and improves on the old probe's documented
   under-counting approximation by using the exact production keys/regex and
   both sources.
3. **`_delivery_score` does not feed `delivery_demand`.** The framing asked
   which of `delivery_demand` / `demand_potential`'s delivery leg it feeds:
   it feeds **only** `demand_potential`'s delivery leg (`:8246-8252`);
   `delivery_demand` = `provider_intelligence_composite` (`:8490-8499`,
   `:3500`). The supply-cancellation hypothesis still holds — relocated.
4. **Bulk delivery enrichment radius is hardcoded 1200 m for all service
   models** (`:7817`), overriding the pool SQL's model-aware
   `:demand_radius_m` count (`:7281`) at `:8232`. Demand radii are otherwise
   model-aware (qsr 1500, `:830`). Worth folding into any Item-2 patch.
5. **Same count, opposite signs:** `delivery_competition_count` is set equal
   to `delivery_listing_count` post-enrichment (`:8233`), so one number
   simultaneously raises demand_potential and lowers delivery_demand
   (Item 2 / Item 6 #8, #11).
6. **A third sensitivity application** exists inside `_brand_fit_score`
   (`:1609`) on top of the two the framing named (Item 4).
7. **Flagship goal weights sum to 1.2**, not 1.0
   (`area·0.6 + visibility·0.4 + demand·0.2`, `:1578`, clamped) — flagship
   briefs structurally inflate `goal_component` relative to other goals.
8. **Stored-JSON ambiguity for the chain leg:** `inputs.chain_strength = 50`
   cannot distinguish thin-evidence neutral from a true 50.0 share;
   `inputs.chain_strength_max IS NULL` only identifies the zero-match subset
   (`:3520-3524`). The probe re-derives spatially (Item 3, Part B).
9. **Recomputed loose numbers (all exact):**
   - Floor onsets: REF=25 → count ≥ 15; REF=50 → ≥ 28; REF=75 → ≥ 39.
   - Curve docstring examples at `:2710-2714` check out (REF=25: 1→78.7,
     3→57.4, 6→40.3, 15+→floor; REF=50: 1→82.4, 6→50.5, 16→27.9, 24→18.1,
     32→floor).
   - Item-4 table: caps 82.5/90.0/100.0; effective weights
     6.6156/8.7640/11.4793%; max points 5.458/7.888/11.479 (ratio 2.103).
   - Item-2 listing-leg weight (qsr): 1.7528 pts (blend 0.5, realized
     active) / 3.5056 pts (realized inactive); ≈2.3 / ≈4.6 pts effective
     after Item-6 leakage.
   - Weights at `:3450-3461` sum to exactly 100.0000 with
     `EXPANSION_CHAIN_STRENGTH_WEIGHT=3.0`.
10. **Framing claims verified as stated:** top-level weights at `:3450`;
    qsr competition 1200 m (`:830`); `_WHITESPACE_LOG_REF` dine_in/
    delivery_first 50, default 25 (`:2688-2691`); blend env default 0.5 and
    REF 263 (`config.py:107-120`); `EXPANSION_CHAIN_MIN_MATCHED=3` with
    neutral-50 fallback (`config.py:412-414`, `:2742-2758`); the
    `0.75 + vw·0.25` multiplier (`:2230`); cafe docstring/dict mismatch
    (`:2658` vs `:2664`). L1 scoring flags default **false in code**
    (`config.py:158-161`, `:175-179`) — if they are enabled via env in
    production, the qsr/dine_in pop leg is the L1 composite, but the delivery
    leg (Item 2) is unchanged either way (`:9506`, `:9534`).
