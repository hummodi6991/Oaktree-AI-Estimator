# Probe I — Base-vs-rd Gap Decomposition: Full Report

**Branch:** `claude/probe-gap-decomposition-oshtig` · **Commit:** `f945f3e11` · **Date:** 2026-06-12
**Mode:** Read-only, SQL-only. No app code touched.
**Deliverables:** `scripts/diagnostics/gap_decomposition.sql` + `scripts/diagnostics/gap_decomposition_reading_guide.md`

---

## 1. What this probe is for

The v2+archetypes **base** score (the deterministic weighted sum, before bonus
deltas) has a mean per-search Spearman of **≈ −0.50** against
`realized_demand_30d` (rd) over the 18 archetype-era searches; the final score
recovers to ≈ −0.21 after deltas. Two earlier probes narrowed but did not close
the question:

- **Probe H** (brand_fit counterfactual): neutralizing brand_fit buys only
  **+0.037** — brand_fit is not the binding constraint.
- **Probe G**: the rd-inverse mass sits in the **parking** (−0.557) and
  **area/zoning fit** (−0.383) raw signals; whitespace is empirically rd-flat.

Open question this probe answers: **how does the remaining gap apportion across
all 10 weighted components, and how much of it is (a) fixable defect, (b)
deliberate product strategy, or (c) a density confound in rd itself?** The
answer decides whether any further scoring patch is warranted before the
re-measure window closes.

---

## 2. Probe design

One file, `scripts/diagnostics/gap_decomposition.sql`, four self-contained
sections. All sections share the same cohort and conventions, carried over
from Probe H:

| Convention | Implementation |
|---|---|
| Archetype-era detection | `score_breakdown_json ? 'brand_archetype'` — the key is written only when `EXPANSION_ARCHETYPE_PROFILES` is on under the v2 stack (see `_score_breakdown`) |
| Weights / raw inputs | Per-row persisted `score_breakdown_json->'weights'` and `->'inputs'` — per-archetype weight profiles honored without hardcoding |
| rd gate | `feature_snapshot_json` has `realized_demand_30d` AND `realized_demand_branches >= 3` (the snapshot writer's own gate) |
| Search gate | ≥ 8 gated candidates per search |
| Ranks | Every `rank()` tie-breaks `parcel_id ASC`, on the rd side too; Spearman = Pearson `corr()` over those ranks |
| Base score | `bonus_detail.base_deterministic`, falling back to breakdown/numeric `final_score` for pre-bonus-detail rows |
| NEUTRAL counterfactual | Pin a component's raw input at the constant **60** (Probe H's NEUTRAL): `base′ = base − w_c·(raw_c − 60)/100` |

### Section A — Cohort sanity
Counts gated searches/candidates. Expected ≈ 18 searches in production; if it
drifts, the numbers are not comparable with the Probe G/H runs and
interpretation should stop there.

### Section B — Leave-one-out per component
For each of the 10 weighted components, recompute the base with that component
neutralized at 60 and report, per component:
`mean_sp_shipped | mean_sp_neutralized | delta`, sorted by delta descending.
**delta > 0 means the component drags base away from rd**; the column
apportions the gap in Spearman points. Built-in self-check: `mean_sp_shipped`
must be identical on every row (same shipped base each time) and should
reproduce the −0.50 anchor.

### Section C — Cumulative greedy top-k
k = 0 is the shipped base; k = 1..4 neutralizes the top-k draggers from B
cumulatively, reporting `mean_sp_vs_rd` and `gain_vs_shipped` at each k.
A fast climb that flattens by k = 2 means the gap **concentrates** (a targeted
patch is plausible); a slow steady climb means it is **spread thin** and no
single-component patch will move the headline number. The curve can be
non-monotonic — components interact through the shared rank ordering; that is
signal, not a bug.

### Section D — Density-confound panel
Per-search Spearman of rd against persisted raw context values that are **not**
weighted components. This distinguishes "rd is structurally a dense-urban
signal" from "specific components are misbuilt."

**Persistence verification against the live schema — nothing requested had to
be dropped:**

| Panel signal | Source | Migration |
|---|---|---|
| `parking_score` | `expansion_candidate.parking_score` | `20260313_exp_adv_v6_features` |
| `area_m2` | `expansion_candidate.area_m2` | `20260310_exp_adv_v0` |
| `street_width_m` | `expansion_candidate.unit_street_width_m` | `20260330_exp_adv_commercial_units` |
| `provider_density_score` | `expansion_candidate.provider_density_score` | `20260311_exp_adv_brand_v4` |
| district momentum raw | `feature_snapshot_json->'district_momentum'->>'activity_30d'` | snapshot writer (Phase 3b) |

Two deliberate choices:

1. **"District momentum raw" is `activity_30d`, not `momentum_score`** — the
   latter is literally the district_momentum component's own input and would
   contaminate a panel meant to be component-free.
2. **`street_width_m` is NULL for parcel-source candidates**, so it may report
   zero qualifying searches. The panel anchors on the full signal list and
   reports a `coverage_pct` column, so a thin signal surfaces with its coverage
   instead of silently vanishing.

---

## 3. Interpretation framework (pre-registered)

### Strategy split — which deltas are "by design"

For any component with **delta > +0.03** in section B, apply this
classification before calling it a defect:

| Component | Anti-rd by design? | Would neutralizing betray a product thesis? |
|---|---|---|
| competition_whitespace | **Yes.** Whitespace deliberately rewards low same-category presence; rd is measured *from* same-category branches. The anti-correlation is the contrarian-entry thesis itself (though Probe G found it empirically rd-flat). | **Yes** — do not patch on this evidence. |
| district_momentum | **Arguably.** The Aqar listing-activity percentile is a leading indicator; rd is a trailing outcome of incumbents. Divergence is partly the point. | **Yes, with caveat** — a large delta *plus* a strongly anti-rd `activity_30d` row in panel D would weaken the leading-indicator defense. |
| occupancy_economics | **Partly.** Rent burden penalizes expensive dense districts — exactly where rd is high. It is a cost gate, not a demand proxy. | **Mostly yes** — tension with rd is priced in. |
| access_visibility | **No — incidental.** Street width / parking / frontage measure site quality; nothing in the thesis wants them anti-demand. Check panel D first: if parking/street width are themselves anti-rd, this is the density confound wearing a component costume. | No thesis betrayed — but a patch only makes sense if panel D does *not* explain it. |
| brand_fit | No — incidental (Probe H: not binding, +0.037). | No. |
| listing_quality | No — incidental. | No. |
| landlord_signal | No — incidental. | No. |
| chain_strength | No — the pro-presence leg should lean *pro*-rd; a positive delta here is a genuine defect flag. | No. |
| demand_potential / delivery_demand | **Mechanical.** rd feeds their realized/delivery legs, so negative LOO deltas are expected (neutralizing removes rd's own echo). Uninformative about defects. | n/a. |

### Decision rule for the re-measure window

- **No patch** if the gap is spread thin (no single *incidental* component with
  delta > ~+0.10 and cumulative top-4 gain < ~+0.15), **or** if panel D shows
  rd tracking raw density context at Probe-G-like magnitudes — then base-vs-rd
  is dominated by strategy + confound, and a scoring patch would be chasing the
  validation target rather than fixing the product.
- **Targeted patch warranted** only if an incidental component
  (access_visibility, listing_quality, landlord_signal, chain_strength,
  brand_fit) carries delta > ~+0.10 that panel D does not explain.

### Caveats baked into the SQL header

- demand_potential and delivery_demand ingest realized/delivery legs, so their
  LOO deltas are partly mechanical, not validation.
- rd is rating velocity of **existing same-category branches** in the catchment
  — an *area* outcome proxy, structurally a dense-urban signal. Section D
  tests exactly that.
- delta > 0 apportions the gap; it does not by itself say "remove the
  component" — the strategy split above governs.

---

## 4. Validation performed (scratch schema)

The SQL shape was validated against a **scratch PostgreSQL 16.13 cluster** with
a synthetic schema mirroring the touched columns of `expansion_search` /
`expansion_candidate`. Test fixture: 4 searches × 12 candidates —
3 archetype-era + 1 pre-archetype; per-search rd-gate violations (one row with
no rd key, one with `branches = 2`); one archetype search held to 6 gated rows
(below the ≥ 8 gate); ~50% NULL street width; `base_deterministic` constructed
as the exact weighted sum of the balanced-profile inputs.

**All structural checks passed:**

| Check | Expected | Observed |
|---|---|---|
| Gated searches | 2 | 2 ✓ |
| Any-rd archetype searches | 3 | 3 ✓ |
| Gated candidates | 20 | 20 ✓ |
| `mean_sp_shipped` identical across all 10 LOO rows | yes | 0.261 on every row ✓ |
| Greedy k = 1 equals top LOO delta | yes | +0.097 both ✓ |
| Thin street width surfaces instead of vanishing | yes | 0 searches, 35.0% coverage ✓ |

One real bug was caught and fixed during validation: the confound panel
originally dropped `street_width_m` entirely when no search cleared the
per-search n ≥ 8 gate; the final version anchors on the signal list and
reports coverage.

### Scratch-run output (synthetic data — numbers below are NOT production findings)

```
=== A. Cohort sanity: archetype-era searches passing the rd gate ===
 n_searches_gated | n_searches_any_rd | n_candidates_gated
------------------+-------------------+--------------------
                2 |                 3 |                 20

=== B. Leave-one-out: neutralize each component at 60, Spearman vs rd ===
       component        | n_searches | mean_weight_pct | mean_sp_shipped | mean_sp_neutralized | delta
------------------------+------------+-----------------+-----------------+---------------------+--------
 district_momentum      |          2 |            7.00 |           0.261 |               0.358 |  0.097
 brand_fit              |          2 |            8.00 |           0.261 |               0.309 |  0.048
 delivery_demand        |          2 |            6.00 |           0.261 |               0.303 |  0.042
 competition_whitespace |          2 |           12.00 |           0.261 |               0.297 |  0.036
 listing_quality        |          2 |            9.00 |           0.261 |               0.291 |  0.030
 occupancy_economics    |          2 |           20.00 |           0.261 |               0.279 |  0.018
 chain_strength         |          2 |            4.00 |           0.261 |               0.261 |  0.000
 demand_potential       |          2 |           18.00 |           0.261 |               0.224 | -0.036
 landlord_signal        |          2 |            5.00 |           0.261 |               0.200 | -0.061
 access_visibility      |          2 |           11.00 |           0.261 |               0.121 | -0.139

=== C. Cumulative greedy: neutralize top-k draggers (k = 0..4) ===
 k |                          components_neutralized                          | n_searches | mean_sp_vs_rd | gain_vs_shipped
---+--------------------------------------------------------------------------+------------+---------------+-----------------
 0 | (none — shipped base)                                                    |          2 |         0.261 |           0.000
 1 | district_momentum                                                        |          2 |         0.358 |           0.097
 2 | district_momentum + brand_fit                                            |          2 |         0.297 |           0.036
 3 | district_momentum + brand_fit + delivery_demand                          |          2 |         0.388 |           0.127
 4 | district_momentum + brand_fit + delivery_demand + competition_whitespace |          2 |         0.485 |           0.224

=== D. Confound panel: rd vs raw context signals (NOT score components) ===
             signal             | n_searches | mean_sp_vs_rd | min_sp | max_sp | mean_n_candidates | coverage_pct
--------------------------------+------------+---------------+--------+--------+-------------------+--------------
 provider_density_score         |          2 |         0.036 |  0.018 |  0.055 |              10.0 |        100.0
 area_m2                        |          2 |        -0.073 | -0.188 |  0.042 |              10.0 |        100.0
 parking_score                  |          2 |        -0.127 | -0.200 | -0.055 |              10.0 |        100.0
 district_momentum_activity_30d |          2 |        -0.412 | -0.430 | -0.394 |              10.0 |        100.0
 street_width_m                 |          0 |               |        |        |                   |         35.0
```

---

## 5. How to run against production

```bash
psql "$DATABASE_URL" -f scripts/diagnostics/gap_decomposition.sql
```

Read-only — no writes, no temp objects. First check section A reports
≈ 18 gated searches; then read B/C/D through the strategy split and decision
rule above.

## 6. Notes and provenance

- **Probe H's file is not in the repo.** `brand_fit_counterfactual.sql` exists
  on no remote branch (checked git refs, PR search, and GitHub code search).
  Its conventions were reconstructed from the live `_score_breakdown` code
  (`app/services/expansion_advisor.py`) and the merged Probe A/B diagnostics
  files (`weight_discrimination.sql`, `contribution_vs_realized_demand.sql`).
- Archetype weight profiles (balanced / delivery_led / street_flagship /
  neighborhood_local) all share the same 10-component keyset, so the per-row
  `weights` explosion covers every profile uniformly.
- Risk level: **none to production** — the probe is a read-only psql script;
  the only repo change is two new files under `scripts/diagnostics/`.
