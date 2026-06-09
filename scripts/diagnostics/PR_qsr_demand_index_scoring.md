# PR — Wire L1 demand-generator index into QSR scoring (`l1_v3` re-anchor)

**Branch:** `claude/qsr-demand-index-scoring-gmyp8q`
**Scope:** Real scoring change, **flag-gated, default OFF, separate flag from dine-in, QSR only.**
`dine_in` / `cafe` / `delivery_first` scored output is unchanged.

---

## 1. What is wrong now

QSR's demand leg rides `pop_score`, which **saturates** (~76–82 for nearly all candidates) — the
same dead signal that was removed from dine-in. There is no usable cross-candidate spread in the QSR
demand numerator today.

A second, blocking defect sits underneath it (**Phase-A discrepancy E.2**): the live
demand-generator enrich runs at the *flat* `EXPANSION_DEMAND_GENERATOR_RADIUS_M` (3500 m) for **all**
service models, decoupled from `_CATCHMENT_RADII_M[model]['demand']`. So QSR's index is computed at
3500 m even though QSR's catchment is 1500 m.

## 2. Why it happens

- **Demand leg:** the demand blend uses `pop_score` as the population numerator. In dense Riyadh the
  population reach is ~constant across candidates, so the term saturates and carries almost no
  ranking information.
- **Enrich radius:** the enrich reads `settings.EXPANSION_DEMAND_GENERATOR_RADIUS_M` unconditionally
  (one constant for every model) instead of the per-model demand radius.

The Phase-A probe (548 city-wide candidates, signals gathered at QSR's 1500 m radius) established that
the demand-generator index **discriminates well for QSR** and that this is a **clean re-anchor, NOT a
re-mix**:

| sub-signal             | n_zero        | finding                                          | weight |
|------------------------|---------------|--------------------------------------------------|--------|
| `fnb_review_weighted`  | 26/548 (~5%)  | healthy spread                                   | 0.35   |
| `building_floors`      | 0             | clean                                            | 0.20   |
| `osm_weighted_total`   | 45 (~8%)      | discriminating                                   | 0.40   |
| `population_local`     | —             | spread ratio ~1.1 at 1000/1200/1500 m (flat)     | 0.05   |

So the **sub-signal mix (0.40 / 0.35 / 0.20 / 0.05) is unchanged** and the **pop radius stays 1500 m**
— both confirmed by the data, neither touched.

**Why `l1_v2` can't be reused on QSR:** at 1500 m the counts are genuinely smaller (QSR `fnb` p95 ≈ 72k
vs dine-in 225k; `floors` p95 ≈ 6.9k vs 35.6k), so `l1_v2`'s 3500 m anchors would map almost every QSR
candidate near 0. Hence a QSR-keyed `l1_v3` anchor set, read at 1500 m. **If we add `l1_v3` (1500 m)
anchors but leave the enrich at 3500 m, we apply 1500 m anchors to 3500 m counts — worse than doing
nothing.** That is why the enrich-radius fix (Change 1) **must ship in the same PR.**

## 3. The fix (smallest safe change)

Three changes, all in `app/services/expansion_advisor.py` + one flag in `app/core/config.py`.

### Change 1 — service-model-aware enrich radius (the E.2 prerequisite)

New helper `_demand_generator_radius_m(service_model)` reads the model's demand catchment from
`_CATCHMENT_RADII_M[model]['demand']` instead of the flat constant:

| model            | enrich radius | vs. before |
|------------------|---------------|------------|
| `dine_in`        | 3500 m        | **identical** (3500 was already the flat default) |
| `qsr`            | 1500 m        | changed (now correct) |
| `cafe`           | 1000 m        | changed (emit-only) |
| `delivery_first` | 3000 m        | changed (emit-only) |
| unknown          | flat fallback | falls back to `EXPANSION_DEMAND_GENERATOR_RADIUS_M`, **not** qsr's 1500 m |

Applied at the enrich site (`_dg_radius`) and at the emitted `radius_m` so the snapshot's `radius_m`
matches the counts it was computed from. The pop sub-term radius
(`EXPANSION_DEMAND_GENERATOR_POP_RADIUS_M`, 1500 m) is left as-is — the probe confirmed 1500 m for QSR.

- **`dine_in`'s scored output does not change** — its demand radius is already 3500, so reading it from
  `_CATCHMENT_RADII_M['dine_in']['demand']` is identical. Pinned by a test.
- **`cafe` / `delivery_first`:** their emitted `demand_generator_index` composite **will change** (now
  computed at their own demand radius) but it is **emit-only / not scored** for them, so no scored
  output moves. This also correctly fixes the decoupling bug generally, not just for QSR.

### Change 2 — `l1_v3` QSR anchor block + service-model-aware anchor selection

New `_DEMAND_GENERATOR_NORM_ANCHORS_QSR` alongside the existing dine-in `l1_v2`, same tuple shape
`(p5, p95, p99, log?)` and same transforms:

```python
_DEMAND_GENERATOR_NORM_ANCHORS_QSR: dict[str, tuple[float, float, float, bool]] = {
    #  signal                 p5         p95         p99       log
    "fnb_review_weighted": (186.5,   72026.0, 94509.0, True),
    "building_floors":     (1262.9,   6898.0,  9483.7, True),
    "osm_weighted_total":  (3.4,       951.8,   951.8, True),   # see osm caveat
    "population_local":    (7410.4,  51979.7, 53392.6, False),  # ~= l1_v2 at 1500 m
}
```

`_demand_generator_anchors(service_model)` selects **`qsr` → `l1_v3`, everything else → `l1_v2`**.
`service_model` is threaded through `_demand_generator_normalize` / `_demand_generator_osm_subscore` /
`_demand_generator_index` with a **default of `None` → `l1_v2`**, so every existing caller is
byte-identical. The weights-version tag is bumped to **`l1_v3_qsr_2026-06` for QSR only**; the dine-in
`l1_v2_2026-06` tag is untouched.

**osm caveat:** QSR `osm` p95 == p99 == 951.8 — the top is a winsorization plateau, so the highest-OSM
QSR candidates won't separate from each other; the mid-range (p25 = 15 → p75 = 657) still discriminates.
Acceptable for v3; a known limitation to revisit, not a blocker.

**osm p5:** the probe p5 was 0.0. A log anchor needs a positive floor, and `_demand_generator_normalize`
already clamps a 0 p5 safely (`log1p(0)=0`, and `hi > lo` still holds), but we use **p5 = 3.4** (matching
`l1_v2`'s positive osm floor) so the low tail doesn't peg at exactly 0.

### Change 3 — gated QSR blend swap (the actual scoring wire)

New flag **`EXPANSION_DEMAND_GENERATOR_SCORING_QSR_ENABLED: bool = False`** (default OFF), effective
only when `EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED` is also true (else it logs once and falls back to
`pop_score`).

At the demand-blend swap site, a QSR branch mirrors PR-2's dine-in swap: when the QSR flag is on,
`service_model == "qsr"`, and the composite is present, the blend uses `dg_composite` in place of
`pop_score`:

```
demand_score = _clamp(dg_composite · _pop_w + delivery_score · _del_w)
(_pop_w, _del_w) = _demand_blend_weights("qsr")  # = 0.60 / 0.40
```

It reuses the in-memory composite (no snapshot re-read) and emits
`demand_score_source = "dg_index" | "pop_score"` for QSR (only when the QSR flag is on).

**Why a separate flag, not the existing dine-in flag:** the dine-in flag is already **ON in prod**, so
reusing it would take QSR live the instant this deploys with no flag-off → flag-on validation window.
A separate flag means QSR deploys **inert** and is flipped to validate independently.

## 4. Hard constraints honored

- `dine_in` / `cafe` / `delivery_first` scored output identical to pre-PR. `dine_in` stays on `l1_v2` at
  3500 m with its own flag; `cafe` / `delivery_first` stay on `pop_score` (only their emit-only composite
  changes via Change 1). All three pinned by tests.
- Sub-signal mix (0.40 / 0.35 / 0.20 / 0.05), OSM bucket weights, pop radius (1500), the 15.00
  whitespace floor, `component_weights`, `sum == 100`, gates — all unchanged.
- Flag-gated, default OFF, separate flag. Deploys inert.
- No merge / push to other branches / dispatch.

**Emit-only blast-radius note:** with the index flag on, QSR's *emitted* composite changes (now `l1_v3`
@ 1500 m) even with the QSR scoring flag off — but that is emit-only; QSR's **scored** output stays on
`pop_score` until the new flag is flipped. `cafe` / `delivery_first` emitted composites likewise change
(their own demand radius), not scored.

## 5. Files changed

| file | change |
|------|--------|
| `app/core/config.py` | new `EXPANSION_DEMAND_GENERATOR_SCORING_QSR_ENABLED` flag (default OFF) |
| `app/services/expansion_advisor.py` | `_demand_generator_radius_m()` helper; `l1_v3` anchors + `_demand_generator_anchors()`; `service_model` threaded through normalize/osm-subscore/index; QSR version tag; QSR blend swap + one-time warning; emit guard |
| `tests/test_expansion_advisor_demand_generator.py` | QSR + radius + anchor-selection tests |
| `scripts/diagnostics/qsr_index_validation.sql` | QSR analogue of `l1_index_validation.sql` |

## 6. Validation steps

### Tests added / run

- **Flag off (QSR):** QSR `final_score` + ordering identical to a both-flags-off baseline; no
  `demand_score_source` key.
- **Flag on + qsr + composite present:** blend uses `dg_composite` at 0.60 / 0.40,
  `demand_score_source == "dg_index"`, emitted index carries the `l1_v3_qsr_2026-06` tag, score moves.
- **Flag on + composite missing:** falls back to `pop_score`, no exception.
- **Anchor selection:** `qsr` normalizes against `l1_v3`, `dine_in` against `l1_v2`; a representative
  input maps differently (l1_v3's tighter anchors normalize the same count higher).
- **Enrich radius:** `dine_in` still 3500 (unchanged), `qsr` now 1500, asserted from
  `_CATCHMENT_RADII_M`; unknown → flat fallback.
- **`dine_in` scored signal bit-identical:** the index-radius refactor does not perturb dine-in.
- **Blast guard:** flipping the QSR flag leaves `dine_in` / `cafe` / `delivery_first` untouched.

Commands:

```bash
python -m pytest tests/test_expansion_advisor_demand_generator.py -q   # 21 passed
python -m pytest -q                                                    # 2301 passed, 24 skipped
```

> CI gate is `pytest -q` (`.github/workflows/ci.yml`); no flake8/black gate. New code matches the
> existing hand-formatted style of the surrounding flags and the existing `l1_v2` anchor block.

### Live validation (Ahmed, Codespace — after diff review; changes QSR rankings when enabled)

1. Merge → deploy → rollout status.
2. `kubectl set env deployment/oaktree-estimator -n default
   EXPANSION_DEMAND_GENERATOR_SCORING_QSR_ENABLED=true`; wait for rollout;
   `printenv | grep QSR` to confirm (and `INDEX_ENABLED` still true).
3. Fresh city-wide **QSR** search.
4. Run `scripts/diagnostics/qsr_index_validation.sql`. Success criteria:
   - `demand_score_source = "dg_index"` for QSR candidates with a composite (`n_dg_index` ≈ all).
   - `corr(composite, final_score)` jumps materially from its flag-off baseline (the proof QSR now
     scores off the index) — analogous to dine-in's 0.06 → 0.51.
   - QSR composites spread sensibly at 1500 m (NOT bunched near 0 — confirms `l1_v3` anchors fit the
     1500 m counts; the whole point vs reusing `l1_v2`). Check `radius_m = 1500` and
     `weights_version = l1_v3_qsr_2026-06` in the snapshot.
   - QSR shortlist reorders sensibly; top-10 flag-off vs flag-on diff.

## 7. Risk / tradeoff

- **Risk: low while the flag is OFF** — deploys inert; QSR scored output identical to prod.
- **When flipped ON:** QSR rankings move by design. The osm plateau (p95 == p99) is a known limitation
  that compresses the highest-OSM QSR candidates at the top; the mid band still discriminates.
- **Emit-only drift:** `cafe` / `delivery_first` / (index-on) QSR emitted composites change. Not scored;
  surfaced here so the snapshot change is expected, not a surprise.

## 8. Merge recommendation

**Merge with low risk.** The change is additive, flag-gated, default OFF on a separate flag, and ships
the E.2 enrich-radius prerequisite alongside the anchors so 1500 m anchors are applied to 1500 m counts.
Validate live by flipping `EXPANSION_DEMAND_GENERATOR_SCORING_QSR_ENABLED=true` and running
`qsr_index_validation.sql`.
