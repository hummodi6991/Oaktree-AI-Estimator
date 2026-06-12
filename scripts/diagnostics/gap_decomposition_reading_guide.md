# Probe I — base-vs-rd gap decomposition: reading guide

`gap_decomposition.sql` decides whether any further scoring patch is
warranted before the re-measure window closes. Context: the v2+archetypes
**base** score sits at mean per-search Spearman ≈ **−0.50** vs
`realized_demand_30d` (rd) over the 18 archetype-era searches (final score
≈ −0.21 after bonus deltas). Probe H showed neutralizing brand_fit alone
buys only +0.037; Probe G located the rd-inverse mass in parking (−0.557)
and area/zoning-fit (−0.383) raw signals. This probe apportions the whole
gap across all 10 weighted components and tests the density-confound
hypothesis.

## Running it

```bash
psql "$DATABASE_URL" -f scripts/diagnostics/gap_decomposition.sql
```

Read-only; one file; no app code. Conventions match Probe H: archetype-era
rows detected via `score_breakdown_json ? 'brand_archetype'`, per-row
persisted `weights`/`inputs`, rd gate `realized_demand_branches >= 3`,
search gate ≥ 8 gated candidates, all ranks tie-broken `parcel_id ASC`,
NEUTRAL counterfactual pins a component's raw input at the constant 60.

## How to read each section

**A — cohort sanity.** `n_searches_gated` should be ≈ 18. If it drifts,
the numbers are not comparable with the Probe G/H runs; stop and find out
why before interpreting anything else.

**B — leave-one-out.** One row per component;
`delta = mean_sp_neutralized − mean_sp_shipped`. `delta > 0` means the
component drags base away from rd, and its magnitude is that component's
share of the gap (in Spearman points). Structural self-check: the
`mean_sp_shipped` column must be identical on every row (it is the same
shipped base each time) and should reproduce the ≈ −0.50 anchor.

**C — cumulative greedy.** k = 0 is the shipped base; k = 1..4 neutralizes
the top-k draggers from B cumulatively. A fast climb that flattens by k = 2
means the gap concentrates (a targeted patch is plausible); a slow steady
climb means it is spread thin across many components (a patch that moves
one component will not move the headline number). Note the greedy curve
can be non-monotonic — components interact through the shared rank
ordering; that is signal, not a bug.

**D — confound panel.** Per-search Spearman of rd against persisted raw
context values that are *not* weighted components: `parking_score`,
`area_m2`, `unit_street_width_m` (as `street_width_m`),
`provider_density_score`, and the raw district activity count
`feature_snapshot_json->'district_momentum'->>'activity_30d'`.

Persistence was verified against the live schema — **nothing requested had
to be dropped**: parking_score (migration `20260313_exp_adv_v6_features`),
area_m2 (`20260310_exp_adv_v0`), unit_street_width_m
(`20260330_exp_adv_commercial_units`), provider_density_score
(`20260311_exp_adv_brand_v4`) are all real columns. Two deliberate choices:
"district momentum raw" is the raw `activity_30d` count, *not*
`momentum_score` — the latter is literally the district_momentum
component's input and would contaminate a panel meant to be
component-free. And `street_width_m` is NULL for parcel-source candidates,
so it may report 0 qualifying searches; the `coverage_pct` column shows
how thin it is rather than dropping the row.

If these context signals correlate with rd at magnitudes comparable to the
component numbers from Probe G (parking −0.557, area/zoning −0.383), the
conclusion is that **rd is structurally a dense-urban signal**: dense
districts have many same-category branches reviewing fast, small parcels,
narrow streets, scarce parking. In that world the section-B deltas are
mostly a confound of the validation target, not component defects, and the
fix (if any) is in how we use rd for validation — not in the components.

## Strategy split — which deltas are "by design"

For any component with **delta > +0.03** in section B, apply this
pre-registered classification before calling it a defect:

| Component | Anti-rd by design? | Would neutralizing betray a thesis? |
|---|---|---|
| competition_whitespace | **Yes.** Whitespace deliberately rewards low same-category presence; rd is measured *from* same-category branches. Anti-correlation is the contrarian-entry thesis itself (though Probe G found it empirically rd-flat). | Yes — do not patch on this evidence. |
| district_momentum | **Arguably.** Aqar listing-activity percentile is a leading indicator; rd is a trailing outcome of incumbents. Divergence is partly the point. | Yes, with caveat — a large delta plus a strongly anti-rd `activity_30d` row in panel D would weaken the leading-indicator defense. |
| occupancy_economics | **Partly.** Rent burden penalizes expensive dense districts, which are exactly where rd is high. It is a cost gate, not a demand proxy. | Mostly yes — tension with rd is priced in. |
| access_visibility | **No — incidental.** Street width/parking/frontage measure site quality; nothing in the thesis wants them anti-demand. But check panel D first: if parking/street width are themselves anti-rd, this is the density confound wearing a component costume. | No thesis betrayed, but a patch only makes sense if panel D does *not* explain it. |
| brand_fit | No — incidental (Probe H: not binding, +0.037). | No. |
| listing_quality | No — incidental. | No. |
| landlord_signal | No — incidental. | No. |
| chain_strength | No — it is the pro-presence leg and should lean *pro*-rd; a positive delta here is a genuine defect flag. | No. |
| demand_potential / delivery_demand | **Mechanical.** rd feeds their realized/delivery legs, so their LOO deltas are expected to be negative (neutralizing removes rd's own echo). Treat any value as uninformative about defects. | n/a. |

## Decision rule for the re-measure window

- **No patch** if the gap is spread thin (no single *incidental* component
  with delta > ~+0.10, cumulative top-4 gain < ~+0.15) or if panel D shows
  rd tracking raw density context at Probe-G-like magnitudes — then base
  vs rd is dominated by strategy + confound, and a scoring patch would be
  chasing the validation target, not fixing the product.
- **Targeted patch warranted** only if an incidental component
  (access_visibility, listing_quality, landlord_signal, chain_strength,
  brand_fit) carries delta > ~+0.10 that panel D does not explain.

## Validation performed

The SQL shape was validated against a scratch PostgreSQL 16 schema
(mirroring the touched columns of `expansion_search` /
`expansion_candidate`) with 4 synthetic searches: 3 archetype-era, 1
pre-archetype, per-search rd-gate violations (missing key, branches < 3),
one archetype search held under the 8-candidate gate, and ~50% NULL
street width. Observed: cohort counts exactly as constructed (2 gated / 3
any-rd / 20 candidates), `mean_sp_shipped` identical across all 10 LOO
rows, greedy k = 1 equal to the top LOO delta, and `street_width_m`
surfacing with 0 qualifying searches + 35% coverage instead of vanishing.
