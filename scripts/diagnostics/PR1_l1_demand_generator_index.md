# PR-1: L1 Demand-Generator Index (enrich, additive, emit-only)

Branch: `feat/l1-demand-generator-index-enrich`

## What this does
Builds a per-candidate **`demand_generator_index`** from data we already own and **emits it into `feature_snapshot_json` for validation — without wiring it into the score** (that's PR-2). For `dine_in` this is the signal that replaces the effectively-absent `foot_traffic` proxy (which is a cafe-only nudge today). It deliberately includes a **free `review_count`-weighted F&B-density term** so we can measure how much observed dine-in activity it captures *before* deciding whether to pay for BestTime (L2).

## Feature flag (default OFF)
- `EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED` (default `false`) — entire path inert when off.
- `EXPANSION_DEMAND_GENERATOR_RADIUS_M` (default `3500`) — single configurable dine-in catchment radius.

## Files changed (all additive — 0 deletions in `expansion_advisor.py`)
- `app/core/config.py:122-141` — the two settings above.
- `app/services/expansion_advisor.py`:
  - `:1797-1916` — `_DEMAND_GENERATOR_*` weight constants (seeded from `_ANCHOR_WEIGHTS`, regrouped), `_demand_generator_osm_subscore`, and `_demand_generator_index` (composite; reuses `_population_score` + the `_demand_anchor_score` sigmoid; retains every raw sub-value; documents net-of-supply + `review_count` staleness).
  - `:7416-7421` — three new bulk dicts declared beside `_bulk_foot_traffic`.
  - `:8649-8830` (after the foot-traffic block) — three coordinate-based bulk-enrich blocks (OSM generators / Overture floor-density / free F&B review-weighted density), flag-gated, each guarded by `_cached_table_available` and an independent statement so a missing externally-imported table no-ops without affecting the others. One bulk query per source (no N+1), keyed on `_shortlist_coords`.
  - `:8949-8963` (in the second-pass loop, right after `_candidate_feature_snapshot`) — emits `feature_snapshot_json["demand_generator_index"]` only when the flag is on.
- `tests/test_expansion_advisor_demand_generator.py` — new.
- `scripts/diagnostics/l1_index_validation.sql` — new.

## Confirmation: rankings unchanged when OFF
- `expansion_advisor.py` diff is **326 insertions, 0 deletions** — no edits to `component_weights`, `_demand_blend_weights`, the `sum==100` invariant, any gate, or `final_score`.
- Test `test_flag_off_emits_no_index_key` asserts no snapshot key when off; `test_flag_on_emits_index_without_changing_scores` runs the same search off→on and asserts `final_score` + ordering are **identical**, with the index present only as an extra snapshot key.
- `test_demand_blend_weights_unchanged` pins the blend weights; `_normalize_feature_snapshot` (`:1219`) is non-destructive so the new key persists to `expansion_candidate.feature_snapshot_json`.
- Full local run: **271 passed** (advisor service/unit/demand-generator), broader advisor suite **382 passed, 7 skipped** (the skips are the postgres-only radiance tests).

## The composite (transparent, retains raw sub-values)
Each sub-signal is normalized to 0–100 with the same log/sqrt family already used by `_population_score` / `_foot_traffic_score`, then combined:

| Sub-signal | Source | Normalization | Top weight |
|---|---|---|---|
| population | `population_density` (reuses `population_reach`) | `_population_score(..., "dine_in")` (250k ref) | 0.30 |
| fnb_review_weighted | `restaurant_poi` Σ`review_count` (F&B filter, excl. permanently closed) | log-scale (ref ~5000) | 0.25 |
| osm_generators | `planet_osm_point` + `planet_osm_polygon` | Σ(count·weight) → `_demand_anchor_score` sigmoid | 0.25 |
| building_floors | `overture_buildings` floors proxy sum | log-scale (ref ~2000) | 0.20 |

OSM generator buckets + per-type weights (seeded from `_ANCHOR_WEIGHTS`): offices 2.0, malls_retail 4.0, transit 2.0, mosques 1.5, schools 1.75, hospitals 2.0, hotels 2.5. Emitted `weights_version` lets PR-2 recalibrate against real data without re-running enrich.

## Net-of-supply discipline
The index is a demand **numerator only** — it takes no competitor argument and never subtracts competition. Competitor/POI density stays in `_competition_whitespace_score`. The F&B review-weighted term correlates with competitor count by construction; that's expected and is exactly why the denominator stays separate (test `test_index_is_numerator_only_more_fnb_raises_score`).

## Exclusions honored
- **Radiance excluded** from the index (already an advisory tilt via `radiance_growth_pass`).
- `public_transport` excluded from the transit bucket for now (it lives only in the hstore `tags` column) — `# TODO(L1)` left in code to add behind an hstore-safe guard.
- `ms_buildings_raw` skipped (footprint only, no floors); Overture is the floor source.
- `review_count` staleness documented in code (used for relative ranking only).

## Validation plan (run in Codespace after deploy with the flag ON)
`psql "$DATABASE_URL" -f scripts/diagnostics/l1_index_validation.sql` — reads the **most recent dine-in search**'s candidates and checks the three success criteria:
1. **Populated** — index + all sub-components non-null for dine-in candidates.
2. **Varies** — composite spread (min/avg/p50/max/stddev, n_nonzero, distinct values); not constant / not mostly-zero.
3. **Diverges from competition** — Pearson `corr(composite, competitor_count)` and `corr(composite, whitespace)` well below 1 ⇒ independent demand signal, not a competitor proxy.

Plus a `DISTINCT ON (district)` view to eyeball 3+ candidates in different districts.
**Requires a fresh dine-in search after deploy** — old searches won't backfill the snapshot (their `demand_generator_index` is NULL and is filtered out).

## Anchors that had drifted from the prompt
The prompt's `path:line` anchors (from the read-only report) were all slightly shifted on `main` and I adapted:
- `_bulk_foot_traffic` block is ~`:8540` (not `:8435`); `_shortlist_coords` ~`:8330`; `category_keys` does **not** exist in the main function — the in-scope variable is `_cat_expanded` (`:7291`), so the F&B filter uses `_cat_expanded["keys"]`.
- `planet_osm.way` is **SRID 4326** (osm2pgsql `--latlong`), not 3857 as one source in the report claimed; `ST_Transform(...,4326)` is still applied defensively.

## Risk
**Low.** Additive + flag-gated + degrade-silently on missing tables; default-OFF means production behavior is byte-for-byte unchanged until you flip the flag for a validation search.
