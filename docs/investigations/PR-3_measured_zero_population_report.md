# PR-3 Report — Distinguish measured-zero population from missing coverage

| | |
|---|---|
| **Branch** | `claude/measured-zero-pop-floors-nw4hsv` (from latest `main`) |
| **Scope** | Finding 5 of the 2026-06 scoring/ranking audit. No other changes. |
| **Status** | Pushed. **Not merged** — awaiting review, per task scope. |
| **Files** | `app/services/expansion_advisor.py`, `tests/test_expansion_advisor_service.py`, `frontend/src/features/expansion-advisor/scoreComponentMeta.test.ts` |
| **Migration** | None (JSON/float column semantics only). `scripts/validate_alembic_chain.py` ✓ (91 revisions valid). |
| **Validation** | Full backend suite: **2478 passed, 24 skipped**, 0 failures. Frontend `tsc` ✓, affected Vitest suites green. |

---

## 1. The merged-cohort defect

`_bulk_enrich_population` returned `COALESCE(SUM(pd.population), 0)` and the
listing-pool SQL defaulted `0 AS population_reach`, so **"no population-grid
coverage in the catchment"** and **"a real measured zero"** both arrived at the
viability pass as `0.0`. They are not the same thing and must not be treated
the same way.

The hard floor had a `pop_val <= 0 → pass` escape hatch and the soft pop leg
required `pop_reach > 0` to be "confident". The combined effect: a site with
`population_reach == 0` **bypassed** the very floor that drops a 19,999-pop
site, and could never be demoted by the soft leg. The defensive `None`
branches in the viability pass were effectively dead for listing candidates,
because listing candidates never carried `None` — they carried a coalesced
`0.0`.

## 2. The `None`-vs-`0` contract

| State | `population_reach` value | Hard floor | Soft pop leg | Scoring |
|---|---|---|---|---|
| **No grid coverage** (unmeasured) | `None` (null) | **pass** (defensive) | does **not** fire | `None → 0.0` |
| **Measured zero** (covered, sum 0) | `0.0` | evaluated → **fails** a `>0` floor | **can** fire | `0.0` |
| **Measured positive** | e.g. `41000.0` | evaluated | fires if below cohort p25 | `41000.0` |

Implementation:

- **Enrichment coverage signal.** `_bulk_enrich_population` now returns
  `dict[str, float | None]`. The LATERAL subquery additionally selects
  `COUNT(*) AS coverage_count`; the Python comprehension maps
  `coverage_count == 0 → None`, `> 0 → float(sum)` (which may legitimately be
  `0.0`).
- **Listing-pool SQL placeholders.** Both active listing paths —
  `_query_candidate_location_pool` and the direct `_query_commercial_unit_candidates`
  fallback — now default `CAST(NULL AS double precision) AS population_reach`
  instead of `0`. So a row that is never enriched reads as *unmeasured*, never
  *measured zero*.
- **Floor path.** The `pop_val <= 0 → pass` branch is deleted. `pop_raw is None`
  keeps today's defensive pass; a measured value (including `0.0`) is evaluated
  against the floor (`0.0 >= 20000` → fail). An unparseable value stays a
  defensive pass.
- **Soft leg.** `pop_confident` is now "value is present" (`pop_reach >= 0`,
  guarding the `-1.0` parse-failure sentinel) instead of `> 0`, so a measured
  zero can demote.
- **rpc leg unchanged.** It keeps `pop_v > 0` — that is a division guard, not a
  viability semantic, and is correct as-is.

## 3. Scoring-unchanged guarantee

Ranking math is **byte-identical for covered candidates**. The first-pass
scoring read keeps `population_reach = _safe_float(row.get("population_reach"))`
(`None → 0.0`), and `_population_score`, the demand blend, the dg-index
population sub-signal, and every `_safe_float(...)` scoring read continue to
coalesce `None → 0.0`. Only the **viability pass** semantics change, and only
for the two states it previously could not tell apart.

The snapshot now carries the *measured* value: a new
`population_reach_measured` (None-preserving) is threaded first-pass →
`prepared_item` → second-pass and written to
`feature_snapshot_json["population_reach"]`, while the scoring variable stays
the `0.0`-coalesced counterpart. The top-level `expansion_candidate.population_reach`
column is unchanged (still the coalesced scoring value).

## 4. Snapshot null-safety (frontend)

`feature_snapshot_json["population_reach"]` may now be `null`. Frontend was
already null-safe and required **no code change**:

- `frontend/src/lib/api/expansionAdvisor.ts` — typed `population_reach: number | null`.
- `scoreComponentMeta.ts` — `asNumber(null) → null`; the Demand card renders
  `null` via `formatInputValue` / `fmtScore` as an em-dash ("unmeasured"),
  never a fabricated `0`.
- `AdvisorySectionCards.tsx` — `if (section.population_reach != null)` guard.

A regression test was added to `scoreComponentMeta.test.ts` locking the
contract: a measured `0` resolves to `0`, a `null` resolves to `null` (em-dash),
never coerced.

Memo wording (e.g. "population reach around 0") is **out of scope** (v12
workstream); the memo builders still receive the coalesced `0.0` and do not
crash on the unmeasured state.

## 5. Tests added (`tests/test_expansion_advisor_service.py`)

- `test_hard_floor_distinguishes_measured_zero_from_unmeasured` — with a
  20,000 floor, of `{20000, 19999, 0.0, None}` only `20000` and the
  unmeasured `None` survive.
- `test_soft_pop_leg_demotes_measured_zero_but_not_unmeasured` — floor
  disabled: a measured `0.0` fires `population_below_quartile`; an unmeasured
  `None` does not fire the pop leg.
- `test_unmeasured_population_snapshot_roundtrips_with_none` — `None`
  round-trips through `_normalize_feature_snapshot` → `_safe_json_dumps` →
  reload as `null`; `_safe_float` still coalesces to `0.0`.

The pre-existing `test_viability_pass_diagnostics_records_hard_floor_drops_per_leg`
continues to pass.

## 6. Post-deploy validation note

After deploy, run a fresh **QSR / Burger** regression search and confirm the
shortlist is sensible (no zero-population sites surviving the active floor).
Then check the persisted distribution:

```sql
SELECT
  COUNT(*)                                                            AS total,
  COUNT(*) FILTER (
    WHERE feature_snapshot_json->>'population_reach' IS NULL
  )                                                                    AS pop_null,
  COUNT(*) FILTER (
    WHERE (gate_status_json->>'population_floor_pass') = 'true'
  )                                                                    AS floor_pass,
  COUNT(*) FILTER (
    WHERE (gate_status_json->>'population_floor_pass') = 'false'
  )                                                                    AS floor_fail
FROM expansion_candidate
WHERE search_id = '<new_search_id>';
```

Expectation: the `pop_null` share reflects genuine no-coverage catchments
(small in dense Riyadh), and `floor_fail` now includes measured-zero /
sub-floor covered sites that previously slipped through.

## 7. Risk

**Low.** The change is confined to the viability-pass semantics for two
previously-indistinguishable states; covered-candidate ranking is unchanged.
The only behavioral shift is intended: measured-zero / sub-floor covered
listing sites now correctly fail the population hard floor (when configured
`> 0`) and can be demoted by the soft leg, while genuinely unmeasured sites
keep their defensive pass. The dead `_build_candidate_sql*` ARCGIS parcel-pool
path is untouched.
