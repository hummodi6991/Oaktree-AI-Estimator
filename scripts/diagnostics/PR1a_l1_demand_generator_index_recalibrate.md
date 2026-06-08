# PR-1a: Recalibrate L1 Demand-Generator Index (still emit-only)

Branch: `feat/l1-demand-generator-index-recalibrate` (off the merged PR-1 work)

## Why

PR-1 deployed and emitted cleanly (15/15 candidates, 8 districts, flag live), but the composite
**failed the "varies" check**: min 91.18, avg 97.99, p50 98.75, max 98.75, **stddev 1.92, only 5
distinct values**. المربع (competitor_count 60, low everything) scored the **same 98.75** as الورود
(competitor_count 270, near-max everything). The index was pinned at the ceiling.

**Root cause = normalization, not data.** The raw sub-values vary hugely and correctly; every v1
normalization reference sat far below real Riyadh catchment values, so all four sub-scores saturated:

- `fnb_review_weighted` ~12,998 → 151,145 (≈12×) vs a ~5,000 ref → all max out.
- `building_floors_sum` ~17,146 → 32,259 vs a ~2,000 ref → all max out.
- `pop_reach` ~210,936 → 279,192 (tight) vs the 250k dine-in ref → all near-max. At 3.5 km in dense
  Riyadh **every** catchment holds ~250k+ people, so population barely discriminates by construction.
- The OSM weighted-count sigmoid (`/20` ref) saturated to ~95 for everyone — offices alone run into
  the hundreds, so `1 - exp(-total/20)` ≈ 1 for every candidate. The strongest raw discriminators
  were flattened.

This is calibration, not a data failure. Still **emit-only** — nothing wired into `final_score`.

## What changed (all additive / in-place tuning; still flag-gated, default OFF)

`weights_version` bumped `l1_v1_2026-06` → **`l1_v2_2026-06`** so runs are distinguishable in the snapshot.

### 1. Re-anchored every sub-signal to the real distribution
New shared helper `_demand_generator_normalize(signal, value)` (`app/services/expansion_advisor.py`):
**winsorize at p99, then map the city-wide p5→p95 band onto 0–100**, log-transforming the
wide-spread signals. Anchors live in one clearly-marked, versioned block
`_DEMAND_GENERATOR_NORM_ANCHORS` — `(p5, p95, p99, log?)` per signal. Mapping the top anchor to p95
(not the max) leaves headroom so dense areas land ~80–90 instead of pegging at 100.

| signal | p5 | p95 | p99 | transform |
|---|---|---|---|---|
| `fnb_review_weighted` | 4,210 | 224,576 | 241,965 | log |
| `building_floors` | 6,805 | 35,612 | 40,102 | log |
| `osm_weighted_total` | 3.4 | 3,351 | 3,943 | log |
| `population_local` (1500 m) | 8,281 | 52,010 | 53,393 | linear |

**These anchors are SET from Phase A** — `scripts/diagnostics/l1_signal_distributions.sql` run over
**538 city-wide Tier-1 primary candidates**. The OSM anchors are on the *same* `Σ(count·weight)` the
Phase A probe reports, so they dropped in directly.

The OSM sub-score (`_demand_generator_osm_subscore`) now log-normalizes the weighted total against
those anchors instead of the saturating `/20` sigmoid. Per-generator weights are unchanged
(offices 2.0, malls_retail 4.0, transit 2.0, mosques 1.5, schools 1.75, hospitals 2.0, hotels 2.5).

### 2. Population term — implemented **(a) tighter radius**, then Phase A demoted it
Implemented **(a)** (recompute population at a tighter radius) because the data already exists and
re-deriving it is cheap: the L1 enrich block already has the candidate coords, and
`population_density` is the same table `_bulk_enrich_population` uses.

- New setting `EXPANSION_DEMAND_GENERATOR_POP_RADIUS_M` (default **1500**).
- New 4th bulk-enrich block (block "4)" in the L1 section) computes catchment population at the
  tighter radius via the shared coord VALUES list, guarded by `_cached_table_available` and a
  geom-vs-lat/lon column probe (mirrors `_bulk_enrich_population`). One bulk query, no N+1.
- The population **sub-score** normalizes this tighter `population_local_reach`; the full 3500 m
  `population_reach` is **still retained raw** in the snapshot for continuity.

**Phase A verdict: 1500 m did NOT help.** The probe's spread ratios came back
`spread_ratio_1500 = 1.108 ≈ spread_ratio_3500 = 1.109` — population is near-constant at *both*
radii in dense Riyadh, so the tighter catchment buys no discrimination. We keep computing it (the
plumbing is cheap and the raw value stays in the snapshot), but in the weight rebalance below
population is cut to a **token 0.05** and that weight redistributed to the signals that actually
discriminate (effectively the fall-back of option **b**, now justified by data rather than assumed).

### 3. Rebalanced composite weights onto the discriminators
Same versioned constants block; the *number* of sub-signals is unchanged.

| sub-signal | v1 | v2 |
|---|---|---|
| osm_generators | 0.25 | **0.40** |
| fnb_review_weighted | 0.25 | **0.35** |
| building_floors | 0.20 | 0.20 |
| population | **0.30** | **0.05** |

OSM trip generators + free F&B review density (the strong raw discriminators) drive the spread;
population — shown by Phase A to be near-constant at every radius in dense Riyadh — carries a token
weight only. Sum still 1.0 (`test_composite_weights_sum_to_one`).

### 4. Raw sub-values still fully retained
Every raw sub-value stays in the snapshot (unchanged) **plus** the new `population_local_reach` and
`pop_radius_m`, so the next calibration never needs a re-enrich.

## Files changed
- `app/core/config.py` — `EXPANSION_DEMAND_GENERATOR_POP_RADIUS_M` (default 1500).
- `app/services/expansion_advisor.py`:
  - `_DEMAND_GENERATOR_WEIGHTS_VERSION` → `l1_v2_2026-06`; new `_DEMAND_GENERATOR_NORM_ANCHORS`
    block; rebalanced `_DEMAND_GENERATOR_COMPOSITE_WEIGHTS`; new `_demand_generator_normalize`;
    `_demand_generator_osm_subscore` re-anchored; `_demand_generator_index` gains
    `population_local_reach` / `pop_radius_m` params + emits both.
  - L1 enrich section: new `_bulk_dg_pop_local` dict + 4th bulk-enrich block; call site passes the
    tighter-radius population and `pop_radius_m`.
- `scripts/diagnostics/l1_signal_distributions.sql` — **new** Phase A probe.
- `tests/test_expansion_advisor_demand_generator.py` — fixtures rescaled to the calibrated band;
  new `test_recalibrated_index_spreads_across_a_fixture` (asserts ≥30-pt spread, all-distinct
  rounded values, low-activity << high-activity, ordered); flag-off / emit-only / weights-unchanged
  assertions retained.

## Still emit-only — rankings byte-for-byte unchanged when OFF
- No edits to `component_weights`, `_demand_blend_weights`, the `sum==100` invariant, any gate, or
  `final_score`. `test_demand_blend_weights_unchanged` and `test_flag_on_emits_index_without_changing_scores`
  (same search off→on, identical `final_score` + ordering) both still pass.
- When the flag is OFF the whole L1 path (including the new population block) is inert and no snapshot
  key is emitted (`test_flag_off_emits_no_index_key`).
- Local: `tests/test_expansion_advisor_demand_generator.py` **10 passed**;
  `tests/test_expansion_advisor_service.py` **156 passed**.

## Validation (Ahmed, Codespace, after diff review)
1. **Phase A is done** — `scripts/diagnostics/l1_signal_distributions.sql` was run over 538 city-wide
   Tier-1 primaries and `_DEMAND_GENERATOR_NORM_ANCHORS` is now set from those p5/p95/p99 (the
   `population_local` anchors come from the `pop_reach_1500` column). Re-run it only if the candidate
   universe shifts materially.
2. Re-run `scripts/diagnostics/l1_index_validation.sql` against a **fresh dine-in search** (flag on;
   old searches won't backfill). Success criteria:
   - **Spread:** composite stddev materially up; uses most of 0–100; **≥10 distinct rounded values**
     across 15 candidates; lowest-activity district (e.g. طويق) clearly below highest (e.g. الورود).
   - **Ordering sanity:** high-activity districts rank above low; المربع must no longer tie الورود.
   - **Divergence:** report `corr(composite, competitor_count)` — low corr ⇒ the free F&B term is
     independent demand signal (lean skip-L2); high corr ⇒ it echoes competitor density (L2 may earn
     its cost).

## Out of scope — flagged for later
- **`competition_whitespace = 15.00` constant** observed in the PR-1 run is a **separate denominator
  issue (pre-existing, not introduced by PR-1/PR-1a)** — a stuck/clamped denominator, unrelated to
  this index's normalization. **Not touched here.** Worth a dedicated look afterward.

## Drift vs the prompt's `path:line`
- The prompt's references were against the read-only report; against the live tree the L1 enrich
  block sits after the foot-traffic block (~`:8629`), the call site is ~`:9258`, and the constants
  block is ~`:1813`. The F&B filter uses `_cat_expanded["keys"]` (no `category_keys` in scope), as in
  PR-1. Phase A reuses the candidate_location Tier-1 primary fields (`is_cluster_primary`,
  `source_tier=1`, `lon`/`lat`, `district_ar`) from `_query_candidate_location_pool`.

## Risk
**Low.** Tuning-only + flag-gated + degrade-silently on missing tables; default-OFF means production
is byte-for-byte unchanged until the flag is flipped for a validation search. The one behavioral
addition (4th bulk population query) only runs under the flag and no-ops if `population_density` is
absent.
