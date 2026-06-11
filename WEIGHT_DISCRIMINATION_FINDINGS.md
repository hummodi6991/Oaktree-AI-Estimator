# Weight-stack investigation — are the weights earning their keep?

**Mode:** read-only investigation. No app-code changes in this branch — deliverables are
this report plus four SQL probes under `scripts/diagnostics/`. Final verdicts await probe
results (Ahmed runs the probes in Codespace against production).

All line numbers reference the repo at commit `74bd115e8` (merge of PR #1300 / PR-F).

---

## 1. Static vs effective weights (Part 1.1)

### 1.1.1 Static baseline

Defined in `_score_breakdown`, `app/services/expansion_advisor.py:3555-3566`. The only
env-movable member of the dict is `chain_strength`
(`EXPANSION_CHAIN_STRENGTH_WEIGHT`, default `3.0`, `app/core/config.py:400-402`);
`competition_whitespace` absorbs the equal-and-opposite move
(`round(8.7640 - chain, 4)`, line 3554).

| Component | Static weight % | Source line |
|---|---|---|
| occupancy_economics | 26.2924 | expansion_advisor.py:3556 |
| listing_quality | 22.0000 | :3557 |
| brand_fit | 9.6404 | :3558 |
| demand_potential | 8.7640 | :3562 |
| access_visibility | 8.7640 | :3563 |
| landlord_signal | 7.0112 | :3559 |
| competition_whitespace | 5.7640 (= 8.7640 − 3.0) | :3554, :3560 |
| delivery_demand | 4.3820 | :3564 |
| confidence | 4.3820 | :3565 |
| chain_strength | 3.0000 (env) | :3561, config.py:400 |
| **Sum** | **100.0000** (runtime assertion :3590) | |

### 1.1.2 Reweighting mechanics

`_brand_weight_multipliers` (`expansion_advisor.py:3425-3484`) multiplies the static
weights, then `_score_breakdown` renormalizes to 100 with the rounding residual absorbed
into the largest weight (:3567-3585). Gain `g = EXPANSION_BRAND_WEIGHT_GAIN`, default
**0.35** (config.py:428-430). Knob → multiplier map:

- max of parking/frontage/visibility sensitivity → `access_visibility × (1 + g·sig)` where
  `sig = (sensitivity_weight − 0.6)/0.4`; **medium ⇒ sig = 0 ⇒ no-op** (:3455-3463).
- `primary_channel="delivery"` → delivery_demand ×(1+g), whitespace ×(1+0.5g) (:3466-3468).
- `primary_channel="dine_in"` → access ×(1+0.6g), delivery_demand ×(1−0.5g) (:3469-3471).
- `expansion_goal` flagship/delivery_led/neighborhood → access+brand_fit / delivery+whitespace / demand (:3473-3481).

Two notable facts:

1. **`service_model` is a dead parameter** in `_brand_weight_multipliers` — accepted at
   :3427 but never read. Only the brand-brief knobs move weights; choosing QSR vs dine_in
   as a *service model* changes scoring inputs, never weights.
2. `_default_brand_profile` (:1518-1533) fills every knob to medium/balanced, so **a run
   with no explicit brief always gets multipliers = 1.0** and the reweighting is a no-op
   even with the 0.35 gain live.

### 1.1.3 Effective weights, three profiles

**(a) Neutral profile (all medium/balanced):** every multiplier is 1.0 → effective =
static table above. Sum = 100.0000 exactly (no renormalization branch entered).

**(b) Standard QSR/Burger regression profile (`service_model="qsr"` + default brief):**
identical to (a). The default brief is all-medium/balanced, and service_model never
touches the multipliers, so the QSR run carries the **static** weights.

**(c) dine_in profile.** Two readings:
- `service_model="dine_in"` + default brief → static weights again (same reason as (b)).
- Brief with `primary_channel="dine_in"`, everything else default (the only way "dine_in"
  moves weights): access ×1.21, delivery_demand ×0.825, pre-norm sum 101.0736,
  renormalize ×100/101.0736:

| Component | (a)/(b) neutral & QSR | (c) channel=dine_in |
|---|---|---|
| occupancy_economics | 26.2924 | 26.0131 |
| listing_quality | 22.0000 | 21.7663 |
| brand_fit | 9.6404 | 9.5380 |
| demand_potential | 8.7640 | 8.6709 |
| access_visibility | 8.7640 | **10.4918** |
| landlord_signal | 7.0112 | 6.9367 |
| competition_whitespace | 5.7640 | 5.7028 |
| delivery_demand | 4.3820 | **3.5768** |
| confidence | 4.3820 | 4.3355 |
| chain_strength | 3.0000 | 2.9681 |
| **Sum** | **100.0000** | **100.0000** |

(Arithmetic mechanically replicated from the code path; residual-absorption step lands on
0.0000 for this profile.)

### 1.1.4 Reconciliation against the production UI table

The UI's effective weights (26.3 / 22.0 / 9.6 / 8.8 / 8.8 / 7.0 / 5.8 / 4.4 / 4.4 / 3.0)
are **exactly the static baseline rounded to one decimal**. Nothing else moved weights:
no env override beyond the default `EXPANSION_CHAIN_STRENGTH_WEIGHT=3.0`, and the brief
was neutral. Cross-checks:

- Implied raw inputs (pts ÷ weight × 100): economics 84.8, listing 71.4, brand_fit 85.1,
  demand 84.4, access 92.4, landlord 79.9, whitespace 34.7, delivery 66.2,
  **confidence 100.4 (= saturated 100 + display rounding)**, chain 73.3 — all in-range.
- UI pts sum to **78.8**, but final is **82.73** → bonus delta ≈ **+3.93 ≈ +4.0**, which
  matches a `value_band_delta = +4` best-value uprank in `_apply_score_deltas_and_sort`
  (:5309, :5386). The UI weight table shows the deterministic decomposition; the final
  score additionally carries post-hoc deltas (value band ±4/−6, viability demotions,
  freshness +2/+1, momentum +2; recorded in `score_breakdown_json["bonus_detail"]`,
  :5365-5376).

**Conclusion (1.1):** the brand-weight reweighting feature is live but inert for every
default-brief run, including the standard QSR regression. Effective = static unless the
user explicitly sets a non-medium sensitivity, a non-balanced channel, or a non-balanced
goal. The headline concern (Economics + Listing Quality ≈ 48.3%) is a property of the
static stack itself.

**Complete enumeration of env-tunable weight movers** (everything else in the weight path
is hardcoded): `EXPANSION_CHAIN_STRENGTH_WEIGHT` (default 3.0) and
`EXPANSION_BRAND_WEIGHT_GAIN` (default 0.35). The comment at config.py:398 references an
`EXPANSION_COMPETITION_WHITESPACE_WEIGHT` env var that **does not exist** — whitespace is
derived, not settable (parked item P6). Other `EXPANSION_*` settings move component
*inputs* (e.g. `EXPANSION_REALIZED_DEMAND_BLEND=0.5`, `EXPANSION_REALIZED_DEMAND_REFERENCE=263`,
`EXPANSION_DEMAND_GENERATOR_SCORING[_QSR]_ENABLED=false`, viability thresholds) or
post-hoc deltas, not weights.

---

## 2. Component semantics audit (Part 1.2)

Classification: **SITE** = measures the physical site/market opportunity; **ARTIFACT** =
measures the listing object or our data pipeline; **MARKET** = district/area market
signal.

| # | Component (weight) | Scorer | Inputs & sub-weights | Class | Floors/caps/anchors |
|---|---|---|---|---|---|
| 1 | occupancy_economics (26.29%) | `_economics_score` :5087 | revenue_index ×(0.38→0.58, absorbs rent deficit :5154-5155); rent_burden ×(0.20·rb_confidence); fitout 0.14; (100−cannibalization) 0.13; fit_score 0.15 | SITE + brief-relative, demand-contaminated | burden envelope fixed 50/15 (:4870-4901); percentile anchors p10→92 / p50→60 / p90→18 (:5033-5042); composite clamped |
| 2 | listing_quality (22.0%) | `_listing_quality_score` :3000 | freshness 0.30 (bands :3065-3078); suitability 0.20 (LLM, fallback restaurant_score×2); image 0.10 (LLM, fallback binary); furnished 0.05; **district momentum 0.35**; +5 drive-thru | mixed — see §2.1 | parcels → flat 50 (:3055); freshness floor 15; unknowns → 50 |
| 3 | brand_fit (9.64%) | `_brand_fit_score` :1550 | district 0.18 (60 const unless preferred/excluded); goal 0.20 (balanced ⇒ mean(demand,fit,whitespace)); channel 0.14 (balanced ⇒ ~55±10); overlap_fit 0.14; parking 0.136; fit 0.138; visibility 0.11; provider_whitespace 0.08; −premium penalty | SITE/brief, heavy overlap | default brief makes ~32% of it near-constant (district 60 + channel ~55) |
| 4 | demand_potential (8.76%) | blend :8409/:8529 | qsr: pop_score 0.60 + delivery_score 0.40 (`_demand_blend_weights` :2677). pop = √(reach/80k) (:2599); delivery = 0.5·√(cat_count/40) + 0.5·√(rd/327) (:2625, qsr anchor :2750). DG-index swap for qsr exists but default-off (:9676, config :180) | MARKET | √ saturation at refs; rd gated on ≥3 branches (:8394) |
| 5 | access_visibility (8.76%) | `_access_visibility_score` :2230 | frontage 0.62 / access 0.38 at medium frontage sensitivity; raw deliberately NOT sensitivity-scaled (post weight-audit 4b) | SITE (measured street width) | clamp only |
| 6 | landlord_signal (7.01%) | `_landlord_signal_component` :3396 | single input: `commercial_unit.llm_landlord_signal_score` (LLM read of landlord intent in listing copy); None → 50 | **ARTIFACT** | neutral-50 fallback |
| 7 | competition_whitespace (5.76%) | `_competition_whitespace_score` :2766 | log decay of competitor_count; REF qsr=75 / dine_in & delivery_first=50 / default 25 (:2722-2726); unknown → 50 (F4 :2803) | MARKET | **floor 15** at count ≥ ~39 (qsr); ~4.9% floored per QSR-anchor probe, ~32% on broad fast_food briefs (:2716-2718) |
| 8 | delivery_demand (4.38%) | composite :8653-8657 | provider_density 0.36 + provider_whitespace 0.38 + (100−delivery_competition) 0.26; multi_platform excluded (1-of-14 scrapers live, :8648-8651) | MARKET/pipeline mix | provider_whitespace floor 10 (:8460); thin-data damping to 50 (:8453-8454); no-data → 0/50/0/0 (:8515-8518) |
| 9 | confidence (4.38%) | `_confidence_score` :2836 | listings: 30 base +20 rent-actual +15 area-actual +15 street-width +10 image +5 landuse +5 pop; parcels: legacy, **capped 70** | **ARTIFACT/pipeline** | listing path saturates at 100 with full data |
| 10 | chain_strength (3.0%) | `_chain_strength_score` :2817 | share of strong (ECQ ≥ 60) same-category chains in radius; < 3 matched POIs → None → 50 (config :414-419) | MARKET | neutral-50 fallback |

### 2.1 listing_quality — sub-signal classification (the 22% question)

Share of final_score = sub-weight × 22%:

| Sub-signal | Sub-wt | % of final | Classification |
|---|---|---|---|
| district momentum (:3122-3131) | 0.35 | **7.70%** | **MARKET** — district-level 30-day Aqar activity percentile. Neither site- nor listing-specific; every candidate in the same district gets the same value. |
| freshness (:3061-3078) | 0.30 | **6.60%** | **ARTIFACT** — posting/refresh recency on Aqar. |
| suitability (:3087-3092) | 0.20 | 4.40% | **MIXED** — LLM verdict *about the site/unit*, but evidence is the listing artifact (copy + photos). |
| image / LLM listing quality (:3098-3101) | 0.10 | 2.20% | **ARTIFACT** — photo count/quality, fit-out read. |
| furnished (:3104) | 0.05 | 1.10% | unit-intrinsic. |

**Artifact share of the 22 pts: ~8.8 pts hard (freshness + image) + ~4.4 pts mixed-LLM —
i.e. 40–60% of the component, 8.8–13.2% of final_score.** Pure site/unit content is
~1.1 pts. The single largest sub-signal (momentum, 7.7% of final) isn't listing quality
at all — it's a district market signal living under the listing_quality label, AND it is
additionally paid via the +2 momentum bonus in `_apply_score_deltas_and_sort` (:5342-5352)
— a double payment.

### 2.2 occupancy_economics — what drives the 26.3%?

`revenue_index` is the dominant leg (weight 0.38, rising to **0.58** whenever
rb_confidence collapses — `_rent_burden_confidence` :4793 returns 0.25/0.15 for citywide
comps and **0.0** below min-N, with the deficit absorbed into revenue, :5154-5155).
`_estimate_revenue_index` (:4635) decomposes as: street width 0.35 + area-vs-target 0.20
+ listing_type 0.15 + demand 0.20 + whitespace 0.10, × category factor × ticket
multiplier. So inside the "Economics" 26.3%:

- street width duplicates **access_visibility**'s primary input (~3.5 pts of final);
- area-fit duplicates `fit_score`, which is *also* a direct 0.15 leg of economics *and*
  a leg of brand_fit;
- demand/whitespace leak ≈ 2.0–3.0 pts of final into economics (see §3);
- the rent signal — the thing the component name promises — is at most
  0.20 × 26.29 ≈ **5.3 pts of final**, and less wherever comps are thin.

**Rent-percentile path (post PR-C era):** the percentile mode (:5112-5124) compares
against active `commercial_unit` comps in a district→city fallback chain (:4926-4979),
with envelope guards 15–350 SAR/m²/mo (:4757-4758). PR-C itself re-anchored
*realized demand* (delivery leg), not rent — rent anchors are the p10→92/p50→60/p90→18
interpolation (:5033-5042), unchanged.

**PR-E percentile-semantics flag (not fixed here):** `percentile` is `n_below / n`
(:4989, :5021) — share of comparables **at or below** the listing's rate, i.e. HIGH
percentile = EXPENSIVE. Burden maps it inversely (high percentile → low burden score), and
`value_score = √(revenue·burden)` (:5216) inherits it. Any consumer (memo copy, UI label)
reading `economics_detail.rent_burden.percentile` as "cheaper than X%" inverts the
meaning. Since the same number feeds value-band deltas (±4/−6 on final_score) and the
economics contribution, a semantics error there contaminates ranking, not just copy.
Probe D section C surfaces the live percentile distribution to check for compression as
well.

### 2.3 confidence — additive points, not a dampener

Confirmed **purely additive** at the top level: `_confidence_score` output enters
`_score_breakdown` as a 4.38%-weighted component (:3606) and is used nowhere as a
multiplier on other components. Dampening exists *separately and locally*:
rb_confidence inside economics (:4793), the F4 whitespace `confident` gate (:2803),
delivery thin-data damping (:8453). Consequences of the additive design:

- For well-populated listings the component sits at/near 100 (the UI run: raw 100 →
  flat 4.4 pts) → **near-zero discrimination among the candidates that matter** (top
  listings all have rent+area+image+width) while adding a systematic ~1.5–3 pt wedge
  against parcels (capped at 70 ⇒ ≤ 3.07 pts).
- Data richness is paid twice: confidence points + listing_quality image/LLM legs +
  gates/grades.

The UI "Data Quality" alias is known-intentional and not at issue; the additive-points
design is the question, and Probe A will quantify how dead this weight is.

### 2.4 Floors compressing real variance (post PR-A/B/C state)

- whitespace floor 15 with qsr REF=75: floors count ≥ ~39; acceptable for narrow briefs,
  ~32% floored on broad `fast_food` scope (known limitation, :2716-2718).
- provider_whitespace floor 10 (:8460) inside delivery_demand.
- freshness floor 15 (> 365 d) — fine.
- neutral-50 fallbacks (landlord None, chain < 3 matched, momentum below sample floor,
  whitespace unknown): each converts missing coverage into zero spread; with thin LLM
  coverage, landlord_signal's 7% weight is structurally inert (Probe C section C
  measures coverage).
- confidence parcel cap 70 — by design.

---

## 3. Overlap / orthogonality refresh (Part 1.3)

Shared-input map across the demand-side components (QSR, default flags:
`EXPANSION_DEMAND_GENERATOR_SCORING_QSR_ENABLED=false`, realized blend 0.5):

| Input signal | Appears in |
|---|---|
| same-category delivery count (`cat_count`) | demand_potential (delivery leg, +), delivery_demand (`delivery_competition`, −, :8473), brand_fit (delivery_competition in delivery_led goal only) |
| realized_demand_30d | demand_potential only (delivery leg ×0.5 blend) |
| population_reach | demand_potential (pop leg ×0.6), confidence (+5) |
| provider_listing_count (all-category) | delivery_demand (density 0.36), brand_fit (channel component when non-balanced) |
| provider_whitespace_score | delivery_demand (0.38), brand_fit (0.08 direct + 1/3 of goal ×0.20 ≈ 0.107 total) — and provider_whitespace itself correlates −0.94…−0.96 with density (prior audit, `docs/investigations/weight_audit_6_findings.md`) |
| demand_score (whole composite) | brand_fit goal leg (×0.0667), **economics via revenue_index (×0.20 of base ×0.38–0.58 wt)** |
| whitespace_score | economics via revenue_index (×0.10 of base) |
| competitor_count (POI scan) | competition_whitespace only (separate source from delivery cat_count, correlated in practice) |
| cannibalization | economics (0.13), brand_fit overlap_fit (0.14) |
| fit_score (area+zoning) | economics (0.15), brand_fit (0.138), gates |
| street width | access_visibility (primary), economics revenue_index (0.35 of base), confidence (+15) |

**Updated effective demand-side estimate (nominal + spillover, pts of final):**

- demand_potential direct: 8.76
- demand inside brand_fit: 0.20×(1/3)×9.64 ≈ **0.64**
- demand inside economics: 0.20×(0.38–0.58)×26.29 ≈ **2.0–3.0**
- → **core demand ≈ 11.4–12.4 pts** (consistent with the prior audit's ~11.4; the QSR
  realized-demand re-anchor restored variance to the realized leg but did not change
  weights).
- Adding delivery_demand (4.38, though it is closer to supply/saturation than demand) and
  whitespace spillovers (whitespace-in-economics ≈ 1.1, in-brand_fit ≈ 1.0):
  **broad demand-side ≈ 16–18 pts**, vs whitespace-side ≈ 7.9 pts effective
  (5.76 + ~2.1 spillover), vs economics+listing = 48.3 nominal — of which ≈ 3–4 pts
  secretly *are* demand/whitespace via revenue_index, and ≈ 7.7 pts are district
  momentum. The orthogonal "pure economics" content of the 26.3% is roughly half its
  nominal weight.

---

## 4. SQL probes (Part 2)

Committed under `scripts/diagnostics/`, `psql -f`-ready, verified against the live schema
(`expansion_search.created_at` indexed; `expansion_candidate.computed_at`,
`score_breakdown_json`, `feature_snapshot_json`, `final_score`, `commercial_unit_id` →
`commercial_unit.aqar_id`; `commercial_unit.status='active'` used only where comp-set
semantics require it):

| File | Question it answers |
|---|---|
| `weight_discrimination.sql` (Probe A) | Per component: nominal weight vs contribution stddev / p90−p10 across the last ~10 searches (discrimination index), plus corr(contribution, deterministic base). |
| `contribution_vs_realized_demand.sql` (Probe B) | Per-search Spearman of each component's pts vs `realized_demand_30d`; flags ≥7%-weight components with near-zero/negative corr; final_score vs rd. Includes circularity caveats. |
| `listing_quality_decomposition.sql` (Probe C) | Reconstructs the five sub-signals (breakdown JSON doesn't persist them), reports artifact vs unit vs market variance shares + LLM/momentum coverage, with a reconstruction check against the stored composite. |
| `economics_concentration.sql` (Probe D) | Economics contribution spread + clustering (% within ±2/±1 pts of search median), rent_burden mode/source/percentile distribution, applied rb-weight (damping) mix. |

---

## 5. Preliminary verdicts (code-only; final after probes)

| Component | Verdict | One-line rationale |
|---|---|---|
| occupancy_economics (26.3%) | **SUSPECT-OVERWEIGHTED / RESTRUCTURE** | Nominally 26.3% but ≈ half is duplicated street-width/area-fit/demand/whitespace via revenue_index; the actual rent signal is ≤ 5.3 pts and confidence-damped — Probe D decides. |
| listing_quality (22.0%) | **RESTRUCTURE** | 35% of it is district momentum (a market signal, also paid +2 bonus), ~40% is listing-artifact; "listing quality" names ≤ ~5.5 pts of genuine listing content. |
| brand_fit (9.6%) | **SUSPECT-OVERWEIGHTED** | With default briefs ~32% of the raw score is constants (district 60, channel ~55) and the rest re-blends demand/fit/whitespace/parking already paid elsewhere. |
| demand_potential (8.8%) | **SUSPECT-UNDERWEIGHTED** | The only carrier of population + realized demand; core market-demand axis holds ~11–12 effective pts vs 48 for economics+listing — Probe B tests whether it earns more. |
| access_visibility (8.8%) | **KEEP** | Clean, measured, site-intrinsic, decompressed post-audit-4b; UI raw 92 with real spread expected. |
| landlord_signal (7.0%) | **SUSPECT-OVERWEIGHTED** | 7% on a single LLM read of listing copy; neutral-50 on missing coverage makes its effective spread a function of LLM backfill, not the market — Probe A/C coverage decides. |
| competition_whitespace (5.8%) | **KEEP** | Freshly recalibrated REF=75 for QSR; floor behavior known and bounded for narrow briefs. |
| delivery_demand (4.4%) | **RESTRUCTURE** | Internally conflicted composite (density + dampened-whitespace whose legs correlate −0.94, + inverse competition); mostly duplicates demand/whitespace with pipeline-coverage noise. |
| confidence (4.4%) | **RESTRUCTURE** | Additive points for data richness saturate at 100 for exactly the candidates being compared → near-dead weight at the top, plus a structural listing-vs-parcel wedge; trust belongs in dampeners/grades, not the weighted sum. |
| chain_strength (3.0%) | **KEEP (provisional)** | Small, env-tunable, recently share-calibrated, neutral on thin data; no code-level red flags — confirm spread in Probe A. |

---

## 6. Parked items (encountered, deliberately not touched)

1. **P1 — dead `service_model` parameter** in `_brand_weight_multipliers` (:3427): accepted, documented in the docstring mapping, never read.
2. **P2 — PR-E rent-percentile semantics** (high = expensive; consumers may read it inverted) — flagged in §2.2, affects memo/UI copy and potentially value-band deltas; needs its own pass.
3. **P3 — momentum double-payment**: 0.35 sub-weight inside listing_quality (≈7.7% of final) *plus* the +2 momentum bonus in `_apply_score_deltas_and_sort` (:5342-5352); freshness similarly double-paid (0.30 sub-weight + new/updated +2/+1 bonus :5314-5340).
4. **P4 — comment drift** at :8435 ("≥3 total listings OR ≥2 platforms OR ≥1 same-category") vs code `>= 5 / >= 2 / >= 2` (:8437-8441).
5. **P5 — UI "Data Quality" label** for `confidence` — known-intentional alias, out of scope.
6. **P6 — phantom env var**: config.py:398 instructs adjusting `EXPANSION_COMPETITION_WHITESPACE_WEIGHT` "in lockstep", but no such setting exists; whitespace weight is derived from the chain weight.
7. **P7 — `EXPANSION_REALIZED_DEMAND_BLEND` step-2**: the weight-audit log prescribes raising it to 0.7 after PR-C verification; repo default remains 0.5 — whether prod env was bumped is not knowable from the repo.
8. **P8 — QSR DG-index scoring flag** (`EXPANSION_DEMAND_GENERATOR_SCORING_QSR_ENABLED`) ships default-off; demand_potential for QSR still runs on pop_score unless prod env flips it.

---

*Stop point per brief: no patches proposed. Merge/patch decisions follow probe review.*
