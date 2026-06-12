# brand_fit de-duplication investigation — findings

> Investigation only — **no behavior patch in this branch**. Deliverables: this
> report + two SQL probes (`scripts/diagnostics/brand_fit_decomposition.sql`,
> `scripts/diagnostics/brand_fit_counterfactual.sql`) for Ahmed to run in the
> Codespace against production data.
>
> Motivating evidence (production, June 2026): brand_fit corr with the
> deterministic base score **0.719** under the v2 stack (0.804 under v1)
> despite an 8% weight — the strongest rank-driver in the stack; mean
> per-search Spearman vs `realized_demand_30d` **−0.619** (max −0.464,
> negative in all 10 v2 searches) — the most demand-inverse component.
> v2 Spearman(base, rd) = −0.561 vs target ≥ −0.2. Archetype profiles were
> capped at brand_fit ≤ 8 pending this work
> (`app/services/expansion_advisor.py:3546`).

## TL;DR

`_brand_fit_score` is not a brand↔site match signal. Under the default brief
(which is what every search gets unless the operator edits the brand panel),
**~73% of its nominal coefficient mass is either constant or a re-blend of
market signals already paid elsewhere in the weighted sum**. The variable part
is dominated by five inputs — `fit_score`, `provider_whitespace_score`,
`parking_score`, `access_visibility_score`, `demand_score` — four of which are
direct inputs to other top-level components. That makes brand_fit a positively
weighted miniature of the base score (mechanism of the 0.72 correlation), and
its whitespace-type legs are anti-competition by construction, which makes it
anti-realized-demand (mechanism of the −0.62). The recommended fix is
**option A** (strip the duplicated legs, renormalize the genuine match legs,
keep the 8 weight), previewed empirically by Probe H before any patch.

---

## Part 1.1 — Full decomposition of `_brand_fit_score`

Function: `app/services/expansion_advisor.py:1635-1712`. Final blend at
`:1702-1712`. All leg inputs are computed in the scoring loop at
`:8681-8937` (first pass) and recomputed with full road/parking context in
the shortlist pass at `:10317-10377`, which is what gets persisted.

Class key — **(a)** genuine brand↔site/listing match signal, **(b)** duplicated
market signal already paid elsewhere, **(c)** constant / near-constant under
default briefs. "Default brief" = `_default_brand_profile` (`:1518`): channel
balanced, all sensitivities medium, price tier → "mid", tolerance 1800 m, no
district lists. "Standard QSR profile" = default brief + `service_model=qsr`
→ archetype **balanced** (`:1559-1564`).

| # | Leg | Formula (file:line) | Coefficient (default brief) | Input source | Class | Default-brief behavior |
|---|-----|---------------------|------------------------------|--------------|-------|------------------------|
| 1 | `district_component` | 60 base; 88 preferred / 20 excluded (`:1639-1646`) | 0.18 | brief district lists | **(a)**, but **(c)** in practice | Constant 60 → 10.8 pts. Only varies when the brief carries district lists. |
| 2 | `goal_component` — balanced | `(demand + fit + pw)/3` (`:1685`) | 0.20 | demand_score, fit_score, provider_whitespace_score | **(b)** ⅔, (a) ⅓ | Pure re-blend: demand and whitespace-type signal re-paid; only the fit third is format-ish. |
| 2f | `goal_component` — flagship (street_flagship archetype) | `area_comp·0.6 + visibility·0.4 + demand·0.2` (`:1662-1678`) | 0.20 | area vs target, access_visibility_score, demand_score | (a) area; **(b)** visibility + demand | Pre-clamp weights sum to **1.2** — saturates at 100, compressing top-end spread. |
| 2n | `goal_component` — neighborhood (neighborhood_local) | `fit·0.45 + (100−\|cann−45\|)·0.25 + parking·0.3` (`:1679-1681`) | 0.20 | fit_score, cannibalization_score, parking_score | (a) mostly; cann partially **(b)** | cannibalization also paid (inverse) inside economics. |
| 2d | `goal_component` — delivery_led | `pd·0.35 + pw·0.35 + (100−dc)·0.3` (`:1682-1683`) | 0.20 | provider density / whitespace / delivery competition | **(b)** entirely | Same shape as `provider_intelligence_composite` (0.36/0.38/0.26, `:8933-8937`) = the `delivery_demand` component, weighted **13** in this archetype. |
| 3 | `channel_component` | `_channel_fit_score` (`:1625-1632`) | 0.14 | provider_density, multi_platform_presence | balanced: **(c)**; delivery/dine_in: **(b)** | Balanced: `55 + (mpp−50)·0.2` — mpp collapses to ~100 for ~99.85% of candidates (1 of 14 scrapers live, `:8928-8932`) → near-constant ≈ 65 → 9.1 pts. Delivery: `pd·0.7 + mpp·0.3` re-pays delivery_demand inputs. Dine-in: `+(100−pd)·0.2` is **anti-density**. |
| 4 | `overlap_fit` | `100 − \|cann − clamp((2500−tol)/25)\|·0.8` (`:1648-1649`) | 0.14 | cannibalization_score, brief tolerance | **(a)** spacing preference; **(c)** for first-store searches; partial **(b)** | No existing branches → cann = 0 → constant 77.6 → 10.86 pts. cannibalization also paid at 2.6 final pts inside economics (`:5408,5420`). |
| 5 | parking leg | `parking_signal` (`:1707`) | 0.10 + 0.06·sens = **0.136** | parking_score | **(a)** | Single-paid in the weighted sum (parking otherwise only feeds gates). |
| 6 | fit leg | `fit_score` (`:1708`) | 0.12 + 0.03·sens = **0.138** | area_fit·0.55 + zoning·0.45 (`:8698-8700`) | (a) area-format; partial **(b)** | fit_score is also paid at 3.0 final pts inside economics (`fit_score · 0.15` × 20 wt, `:5421`). |
| 7 | visibility leg | `visibility_signal` (`:1709`) | 0.08 + 0.05·sens = **0.11** | access_visibility_score | **(b)** exact | The *identical value* paid at weight 11 as the `access_visibility` component. |
| 8 | provider whitespace leg | `provider_whitespace_score` (`:1710`) | 0.08 | pw | **(b)** | Also paid at 0.38 × 6 wt = 2.28 final pts inside `delivery_demand`. |
| 9 | `premium_penalty` | `max(0,65−vis)·0.35 + max(0,60−district)·0.25`, premium tier only (`:1698-1700`) | −1.0 | visibility, district_component | **(a)** intent, but visibility-keyed | 0 under default ("mid") tier. |

Nominal coefficient sum (default brief): 0.18 + 0.20 + 0.14 + 0.14 + 0.136 +
0.138 + 0.11 + 0.08 = **1.124** (pre-clamp; the score can pin at 100).

Typical standard-QSR candidate (demand 55, fit 70, pw 55, parking 60, av 50,
mpp 100, cann 0): district 10.8 + goal 12.0 + channel 9.1 + overlap 10.86 +
parking 8.16 + fit 9.66 + visibility 5.5 + pw 4.4 = **brand_fit ≈ 70.5**
(verified against the live function: 70.484), of which **30.76 pts are
constants** (legs 1, 3, 4) and the rest re-blends.

## Part 1.2 — Duplication map (standard QSR profile, brand_fit weight 8)

Effective final-score points per 100 units of underlying signal, paid via
brand_fit vs paid at the signal's primary home:

| Underlying signal | Via brand_fit (coeff × 8%) | Primary payment | Double-pay ratio |
|---|---|---|---|
| `demand_score` | (0.20/3) × 8 = **0.53 pts** | `demand_potential` 18.0 pts (+ indirectly via `revenue_index` inside economics, `:8860-8869`) | +3% — small |
| `provider_whitespace_score` | (0.20/3 + 0.08) × 8 = **1.17 pts** | `delivery_demand` 0.38 × 6 = 2.28 pts | **+51%** |
| `access_visibility_score` | 0.11 × 8 = **0.88 pts** | `access_visibility` 11.0 pts | +8% |
| `fit_score` | (0.20/3 + 0.138) × 8 = **1.64 pts** | economics `fit_score·0.15` × 20 = 3.0 pts | **+55%** |
| `cannibalization_score` | overlap slope 0.8 × 0.14 × 8 = **0.90 pts** (signed) | economics `(100−cann)·0.13` × 20 = 2.6 pts | +35% |
| `parking_score` | 0.136 × 8 = **1.09 pts** | none (gates only) | single-paid here |
| `multi_platform_presence` | 0.2 × 0.14 × 8 = **0.22 pts** | none — *deliberately excluded* from delivery_demand as noise (`:8928-8932`); brand_fit still pays it | n/a |

Archetype-specific aggravation — **delivery_led** (brand_fit wt 6, channel
usually `delivery`): goal + channel legs together re-pay provider density at
(0.35·0.2 + 0.7·0.14) × 6 ≈ **1.01 pts**, whitespace at ≈ **0.90 pts**, and
anti-delivery-competition at ≈ **0.36 pts**, on top of `delivery_demand`'s
4.68 / 4.94 / 3.38 pts at weight 13 — a ~20% surcharge on the exact same
composite.

## Part 1.3 — Mechanism of rank dominance (why 8% weight ⇒ 0.72 corr with base)

**Verdict: code-supported; the leading hypothesis (brand_fit ≈ weighted
re-blend of other components ⇒ proxies the total) is confirmed in the leg
math.**

Within a search, the constant legs (district 10.8, channel ≈9.1, overlap
10.86 under default first-store briefs ⇒ ~30.8 pts) contribute zero rank
information. What remains is:

```
brand_fit_variable ≈ 0.205·fit + 0.147·pw + 0.136·parking + 0.11·av + 0.067·demand (+ 0.028·mpp)
```

Every term is either a top-level component raw input (`av` →
access_visibility, `demand` → demand_potential) or a direct input to one
(`fit` → economics at 15% internal weight; `pw` → delivery_demand at 38%
internal weight; `demand` additionally → revenue_index → economics). All
coefficients are positive. brand_fit is therefore a positively weighted
linear combination of roughly half the base-score stack — a miniature of the
total — so it *ranks* like the total regardless of its 8% weight. Correlation
is scale-free; the 8% weight bounds its point contribution (±~5.6 pts), not
its correlation.

The v1→v2 drop (0.804 → 0.719) is consistent with this: v2 moved mass toward
demand_potential and district_momentum, which brand_fit re-blends only weakly,
and away from listing_quality/economics whose inputs it shares more.

Legs creating the coupling, in order of coefficient: fit leg (+goal fit
third), pw leg (+goal pw third), parking leg, visibility leg, demand third.
Probe G section C measures each leg's Spearman vs base to rank them
empirically; section B gives their variance shares.

## Part 1.4 — Mechanism of demand inversion (the −0.62 vs realized_demand_30d)

`realized_demand_30d` is rating-velocity of existing same-category branches in
the candidate's catchment — it populates (≥3 tracked branches) only where
delivery competition is already established, and is high where many active
branches accumulate ratings.

| Hypothesis | Verdict | Reasoning |
|---|---|---|
| (i) whitespace-type legs reward low-competition = low-rd areas | **Code-supported** | `provider_whitespace_score` is anti-competition by construction (`:8727-8740`: `100 − (dc_count−6)·6 − density·0.2`, district fallback `50 + (1−cat_ratio)·30`). It enters brand_fit at effective coefficient 0.147 (balanced) — the largest *systematically signed* variable mass. The weight-audit already measured the supply leg's corr with provider_whitespace at −0.94…−0.96. Under delivery_led the goal leg adds 0.35·pw + 0.3·(100−dc) — much stronger. |
| (ii) rent/price-tier legs reward cheap = low-demand districts | **Code-refuted** | There is **no rent input anywhere** in `_brand_fit_score`. price_tier appears only as `premium_penalty` (`:1698-1700`), inactive under the default "mid" tier and keyed to visibility/district — not rent. Whatever rent-cheapness inversion exists in the base score lives in economics' rent_burden, not here. |
| (iii) area-fit legs are rd-orthogonal and dilute | **Code-supported (as dilution), needs-probe (magnitude)** | area_fit/zoning have no rd linkage; at 0.205 effective coefficient they shrink the share of the lone rd-positive leg (demand at 0.067) without flipping sign themselves. Probe G section D, `fit_area_zoning` row, quantifies. |
| (iv) overlap_fit penalizes own-branch proximity ⇒ penalizes proven markets | **Needs-probe** (new, found in leg math) | For searches with existing branches, high-rd areas are disproportionately where the operator already operates → cann high → `100 − \|cann−28\|·0.8` low. For first-store searches the leg is constant and contributes nothing. Probe G section D, `overlap_fit` row. |
| (v) dine_in channel leg is anti-density | **Code-supported, scope-limited** | `(100 − pd)·0.2` (`:1631`) fires only for `primary_channel=dine_in` briefs. Prevalence needs-probe (Probe G splits nothing by channel, but the channel row in section D will absorb it). |

Net: the only rd-positive leg is the demand third of the balanced goal
(coefficient 0.067). The anti-rd mass (pw legs ≈ 0.147, plus conditional
overlap/dine-in legs) plus the rd-orthogonal dilution mass (fit, parking,
constants) structurally outweigh it. The code analysis ranks suspects; it
cannot apportion −0.62 among them — that is exactly what Probe G section D
does empirically.

## Part 1.5 — Fix options (design, not implementation)

### Option A — strip class-(b) legs, renormalize genuine legs (keep weight 8)

Retain: `district_component` (0.18), `overlap_fit` (0.14), fit leg (0.138),
parking leg (0.136), premium_penalty, and a *genuine slice* of
goal_component (0.20): flagship → area sub-leg only; neighborhood → unchanged
(all class-a); balanced → fit third only; delivery_led → dropped. Strip:
channel leg (market-signal-based in every branch, near-constant in the
default branch), visibility leg, pw leg, and the demand/pw thirds of the
balanced goal. Renormalize retained coefficients to 1.0.

- **Rank dominance:** the re-blend coupling collapses — remaining shared
  factor is fit (also inside economics) only. Expect corr(brand_fit, base) to
  drop from ~0.72 toward the 0.2–0.4 band.
- **Demand inversion:** the systematically anti-rd legs are removed; retained
  legs are rd-orthogonal ⇒ expect Spearman vs rd ≈ 0. Base-level effect
  previewed by Probe H variant GENUINE.
- **Blast radius:** `_brand_fit_score` body + docstring only. Tests: flagship
  target-area tests (`tests/test_expansion_advisor_regression.py:1722-1765`)
  and the archetype goal-branch test
  (`tests/test_expansion_archetype_profiles.py:332`) survive structurally
  (flagship vs balanced still differ via the area leg) but exact values
  shift; PR-2 golden fixtures (`tests/fixtures/pr2_golden/`, incl.
  `tpr_P4_brand_fit_aligned` whose positive fires at brand_fit ≥ 70,
  `:3932-3934`) regenerate via `scripts/gen_pr2_golden.py`. Memo and UI are
  data-driven (weights/inputs JSON) — no contract change. i18n labels
  ("Brand fit") stay accurate.
- **Coverage sensitivity:** zero — every retained leg derives from the brief
  + persisted site columns; no chain/ECQ/brand_alias dependency, so the ~81%
  of candidates without canonical chain data are unaffected.
- **Known tradeoff:** under a fully default brief, brand_fit becomes
  near-constant (district 60, overlap 77.6, area-fit + parking the only
  variance). That is the *correct* semantics — the component should pay only
  when the brief says something — but it means the 8 weight is de-facto
  neutral for default searches, and the component's UI bar will show little
  spread. State this in the PR.

### Option B — Option A + demote weight to ~5, reallocate per archetype

Reallocate the freed 3 pts (balanced) to **demand_potential (+2) and
district_momentum (+1)** — *not* competition_whitespace, which is itself
anti-rd by design (boosting it would re-widen the very gap this fixes).
Mirror per archetype (delivery_led 6→5 frees 1 → delivery_demand or
demand_potential; street_flagship 8→5 frees 3 → access_visibility/demand;
neighborhood_local 7→5 frees 2 → demand/momentum).

- **Effects:** everything A does, plus directly shrinks whatever residual
  inversion the retained legs carry. Largest expected closure of the −0.561
  base-vs-rd gap among the three options.
- **Blast radius:** A's, plus `_ARCHETYPE_WEIGHT_PROFILES` (`:3550-3599`),
  the static v2 dict (`:3778-3789`), the memo's v2 weight map
  (`app/services/llm_decision_memo.py:420`), exact-dict assertions in
  `tests/test_expansion_weight_stack.py:143` /
  `tests/test_expansion_archetype_profiles.py:63`, sum-to-100 guards, and the
  Probe-F-style backtest the archetype PR ran (re-run it).
- **Coverage:** same as A.

### Option C — replace with a thin "brand-format fit" (area + price tier + channel)

Area-ratio leg (exists — the flagship sub-leg), a *new* price-tier vs
district-rent-band match leg (district medians available via the
`_estimate_rent_sar_m2_year` / percentile-rent machinery), and a
*reformulated* channel leg (the current one is market-signal-based; a format
leg would need site attributes — street width, drive-thru — instead).

- **Effects:** orthogonal by construction; comparable to A on both failure
  modes.
- **Blast radius:** largest — full function rewrite, every brand_fit unit
  test, golden fixtures, brief-form semantics (preferred/excluded districts
  lose their scoring home; partially backstopped by the district gate at
  `:3363`, but the preference side disappears), memo wording, docs.
- **Risks:** a tier-vs-rent-band leg can *reintroduce* hypothesis (ii) by
  design (budget tiers scoring cheap districts up) unless built symmetric
  (match = good, mismatch in either direction = bad); the rent-band leg
  inherits rent-estimate confidence problems.
- **Coverage:** fine (no chain dependency), but strictly more moving parts.

### Recommendation

**A now, B as a fast-follow calibration gated on Probe H.** A is the smallest
diff that fixes both mechanisms at their root, keeps the archetype weight
profiles untouched, and is fully previewable by Probe H variant GENUINE
before a single line of app code changes. If archetype-era probes after A
still show base-vs-rd materially short of target, apply B's reallocation
(profile dicts are one diff + test updates). Park C: highest effort, marginal
incremental benefit, and a built-in path to re-creating the inversion.

## Part 1.6 — Interim-guard audit (brand_fit ≤ 8)

All four archetype profiles hold the cap — `_ARCHETYPE_WEIGHT_PROFILES`,
`app/services/expansion_advisor.py:3550-3599`:

| Profile | brand_fit | line |
|---|---|---|
| balanced | 8.0 | `:3557` |
| delivery_led | 6.0 | `:3569` |
| street_flagship | 8.0 | `:3581` |
| neighborhood_local | 7.0 | `:3593` |

Guard test: `tests/test_expansion_archetype_profiles.py:74-81`. Consistent
mirrors: static v2 dict `:3784` (8.0) and the memo's v2 weight map
`app/services/llm_decision_memo.py:420` (0.08). These dicts are where option
B's rebalance lands.

**Edge found:** the cap holds on the *base* profiles, but
`_brand_weight_multipliers` renormalization (`:3808-3826`) can push the
*effective* brand_fit weight slightly above 8 in archetype mode: an all-low
site-sensitivity brief trims access_visibility ×(1 − 0.35·0.75) = 0.7375
(gain default 0.35, `app/core/config.py:441-442`) → av 11 → 8.11 → all other
weights renormalize ×100/97.09 → brand_fit 8 → **8.24**. Magnitude ≤ ~0.3
pts and requires a deliberate all-low brief, so it is not urgent — but the
de-dup PR should either cap brand_fit post-renormalization or exclude it from
renormalization gains.

---

## Part 2 — SQL probes

Both probes are committed under `scripts/diagnostics/` and follow the
conventions of `contribution_vs_realized_demand.sql` (per-search rank
Spearman, rd gate `realized_demand_branches ≥ 3`, ≥8 candidates per search,
30-day window, v2-era detection via `weights ? 'district_momentum'`).

**Persistence finding (explicit, per the task):** brand_fit **leg values are
not persisted** — `score_breakdown_json.inputs` carries only top-level
component raws. The probes therefore *reconstruct* the legs from inputs that
ARE persisted: `demand_score, fit_score, cannibalization_score,
provider_density_score, provider_whitespace_score,
multi_platform_presence_score, delivery_competition_score,
access_visibility_score, parking_score, area_m2, district` on
`expansion_candidate`; `service_model, target_area_m2, created_at` on
`expansion_search`; all brief knobs on `expansion_brand_profile` (1:1 by
`search_id`, `alembic/versions/20260311_exp_adv_brand_v4.py` +
`20260611_brand_archetype.py`); resolved archetype from
`score_breakdown_json->>'brand_archetype'`; base score from
`score_breakdown_json.bonus_detail.base_deterministic`. No field is
fabricated.

**Probe G — `brand_fit_decomposition.sql`.** Reconstructs all nine legs per
candidate, then reports: (A) reconstruction fidelity vs the persisted
`brand_fit_score` — the validity gate, split by whether the brief carries
district lists (the one approximate path: SQL `lower(btrim())` vs Python
`normalize_district_key`); (B) per-leg mean pts and per-search variance
share; (C) per-leg Spearman vs the deterministic base; (D) per-leg Spearman
vs `realized_demand_30d`. Section D's most-negative rows are the empirical
answer to "which legs cause the inversion"; section B × C answer dominance.

**Probe H — `brand_fit_counterfactual.sql`.** Recomputes
`base′ = base + w_bf·(bf_variant − bf_shipped)/100` per candidate
(per-row persisted `weights.brand_fit`, so archetype weights 6/7/8 are
honored) for two variants: **NEUTRAL** (constant 60 — the upper bound on what
*any* brand_fit-only fix can achieve at unchanged weights) and **GENUINE**
(the Part-1 class-(a) legs only, renormalized — a faithful preview of option
A). Reports Spearman(base′, rd) per variant, top-5 overlap, and rank-1
changes vs the shipped base order (tie-broken by `parcel_id ASC`, mirroring
`_apply_score_deltas_and_sort` `:5563`).

**Validation performed in this branch:** both probes were executed against a
local PostgreSQL 16 scratch schema seeded with 60 synthetic candidates whose
`brand_fit_score` was computed by the **real** `_brand_fit_score` (v2 +
archetype flags on) across all four archetypes, a premium-tier brief, Arabic
preferred/excluded district lists, and delivery/dine-in channels.
Reconstruction fidelity: mean abs error **0.003 pts**, p95 0.005, 100% within
1 pt. (Fixture correlations are noise by construction; only the fidelity and
the mechanics were being validated.)

### Verdict thresholds (agree before running)

- If **NEUTRAL** improves mean Spearman(base′, rd) by **≥ +0.10** over
  shipped, a brand_fit fix is worth shipping (A/B). If **< +0.05**, brand_fit
  is *not* the binding constraint on the −0.561 gap — redirect to the next
  suspect (see parked items) before patching brand_fit for hygiene only.
- **GENUINE** should capture **≥ ~70% of NEUTRAL's improvement** while
  keeping mean top-5 overlap **≥ 4/5**. If GENUINE churns much more than
  NEUTRAL, the retained legs still carry market signal and the
  classification needs revisiting.
- Do **not** expect this alone to close −0.561 → ≥ −0.2. An 8%-weight
  component bounds the arithmetic; other components (competition_whitespace
  by design, economics rent legs) carry their own anti-rd mass. The probe
  tells us brand_fit's *share* of the gap.

## What n = 10–20 searches can and cannot conclude

- Per-search Spearman over 8–20 rd-gated candidates has SE ≈ 1/√(n−1) ≈
  0.23–0.38; the mean over ~10 searches has SE ≈ 0.07–0.12. **Sign
  consistency across all searches (as in Probe B) is robust evidence;
  magnitudes carry ±0.1.**
- Probe G's per-leg correlations split the same candidates across nine legs
  whose inputs are mutually correlated (pd/pw/dc especially). Treat section D
  as a *ranking of suspects*, not a causal attribution — Probe H is the
  attribution instrument because it re-scores directly instead of
  apportioning.
- Top-5 overlap yields one observation of 0–5 per search; over 10–20 searches
  it supports direction ("little churn" vs "lots"), not fine thresholds.
- **Selection bias:** rd populates only in catchments with ≥3 history-tracked
  branches — established delivery markets. Conclusions transfer to "ranking
  inside already-active markets", not to greenfield whitespace areas. A
  demand-inverse brand_fit is still wrong: the whitespace thesis is
  competition_whitespace's job (deliberate, weighted, explainable), not an
  accident inside a component labeled "brand fit".
- rd is partially mechanical with demand_potential (the realized leg feeds
  `_delivery_score`). brand_fit touches rd only via the demand third of the
  balanced goal (effective coefficient ≈ 0.067·0.08 ≈ 0.5% of final), so the
  counterfactual deltas are essentially uncontaminated.

## Parked items

1. **economics' revenue_index re-blends demand & whitespace**
   (`:8860-8869` → `_economics_score` at 20 wt) — the same duplication
   pattern at 2.5× brand_fit's weight; likely the next-largest contributor to
   the base-vs-rd gap. Separate investigation.
2. **competition_whitespace's own rd-inversion is by design** — decide
   explicitly how much anti-rd (whitespace-seeking) mass the product wants,
   rather than paying part of it accidentally through brand_fit.
3. **mpp leg pays a signal deliberately excluded from delivery_demand as
   noise** (`:8928-8932` vs `:1632`) — dies automatically with option A.
4. **Flagship goal pre-clamp sum 1.2** (`:1678`) saturates at 100 and
   compresses top-end differentiation for street_flagship searches.
5. **≤8 guard renormalization edge** (Part 1.6) — fold the post-renorm cap
   into the de-dup PR.
6. **Probe district-leg approximation** — SQL `lower(btrim())` vs
   `normalize_district_key`; Probe G section A quantifies the impact per run.
7. **Chain-data coverage** (~19% ECQ, `brand_alias` 163 rows / 93 brands) —
   brand_fit consumes none of it today; any future chain-derived format leg
   is coverage-blocked. Keep the component brief-driven.
