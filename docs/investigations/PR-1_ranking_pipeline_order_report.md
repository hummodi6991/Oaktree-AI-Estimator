# PR-1 Report — Pipeline order: hard floors and score deltas before district selection

| | |
|---|---|
| **PR** | [#1306 — fix(expansion): run hard floors and score deltas before district selection](https://github.com/hummodi6991/Oaktree-Atlas/pull/1306) |
| **Branch** | `claude/fix-ranking-pipeline-order-pnkmt0` (from `main` @ `154296c35`) |
| **Commit** | `249f1e355` |
| **Scope** | Finding 1 + observation 3 of the 2026-06 scoring/ranking audit. No other changes. |
| **Status** | Pushed and PR open. **Not merged** — awaiting review, per task scope. |
| **Files** | `app/services/expansion_advisor.py` (+106/−35), `tests/test_expansion_advisor_service.py` (+240) |
| **Validation** | Full backend suite: **2469 passed, 24 skipped**, 0 failures |

---

## 1. What was wrong

`run_expansion_search` ran its post-scoring stages in this order
(`app/services/expansion_advisor.py:10865-10937` at the pre-fix HEAD):

```
sort(_rank_sort_key)
  → _dedupe_candidates → _dedupe_score_clones
  → district balancing            ← TRUNCATES the pool to ~limit
  → _apply_market_viability_pass  ← hard-floor DROPS + per-leg deltas
  → _apply_score_deltas_and_sort  ← folds deltas, strict re-sort
  → [:limit]
  → _apply_rerank_to_candidates
```

Four concrete consequences:

1. **Under-filled responses.** Hard-floor drops (population floor,
   commercial-activity floor, construction-proximity floor) executed AFTER
   the balancing block had already truncated the pool to roughly `limit`
   rows. Every drop at that point was a permanently lost slot — there was no
   backfill from the rest of the pool, so multi-district searches could
   return fewer rows than `limit` even when hundreds of viable candidates
   existed.

2. **Inconsistent percentile cohorts.** The viability pass computes
   per-search percentile thresholds (population bottom-quartile, demand
   bottom-quartile, rent-per-capita top-quartile) over whatever list it
   receives. Multi-district searches handed it a ≤limit slice; city-wide
   searches handed it the full shortlist. The same env-configured
   thresholds (`EXPANSION_VIABILITY_POP_PERCENTILE`, etc.) therefore had a
   **different statistical meaning** depending on search type.

3. **Voided representation guarantee.** The balancing block ran before
   `_apply_score_deltas_and_sort`, which re-sorts strictly by
   `(final_score DESC, parcel_id ASC)` after folding in value-band,
   viability, freshness, and momentum deltas. The subsequent bare `[:limit]`
   could then evict exactly the candidates balancing had seated — the
   per-district guarantee was silently voided.

4. **No global cap in balancing (audit observation 3).** The balancing
   first pass took `max(2, limit // n_districts)` from *every* district with
   no total cap, so with many districts the intermediate list could exceed
   `limit` before the fill pass's cap applied.

## 2. Why it happened

The balancing block (added for multi-district representation) and the
score-delta refactor (which moved viability from a positional demote to a
score delta + strict re-sort) were introduced independently. Each was
internally correct; their composition was not: balancing assumed it was the
last positional operation, while the delta fold assumed it received the
full pool. Neither assumption held once both were in the pipeline.

## 3. The fix

### New order

```
sort(_rank_sort_key)                                  (unchanged)
  → _dedupe_candidates → _dedupe_score_clones          (unchanged)
  → _apply_market_viability_pass   on the FULL deduped pool
  → _apply_score_deltas_and_sort   on the full survivor list
  → _select_final_candidates       (NEW — replaces balancing + [:limit])
  → _apply_rerank_to_candidates                        (unchanged)
```

### `_select_final_candidates` (new helper, `app/services/expansion_advisor.py:5703`)

- **City-wide / single-district** (`len(target_districts) < 2`):
  returns `candidates[:limit]` — **byte-identical to pre-fix behavior**.
- **Multi-district**: per-district quota of
  `max(1, limit // len(target_districts))` over the score-sorted hard-floor
  survivors, applied in two walks over the sorted input:
  1. **Quota walk** — accept a candidate when its normalized district's
     quota is unfilled; never exceed `limit` total (this adds the global
     cap that was missing, fixing observation 3).
  2. **Fill walk** — top up remaining slots strictly by rank, skipping
     already-selected candidates.
- Output is emitted as a **filtered subsequence of the sorted input**
  (selected indices in ascending order), so it stays in `final_score` order
  by construction — the guarantee can no longer be undone by a later sort.
- Candidates whose district does not normalize to a usable key
  (`_unknown`) get **no quota**; they compete only in the fill walk.

### Intentional semantic changes vs the old balancing block

| Aspect | Before | After |
|---|---|---|
| Per-district floor | `max(2, limit // n)` | `max(1, limit // n)` |
| Global cap in quota phase | none | capped at `limit` |
| Guarantee evaluated on | pre-floor, pre-delta scores | hard-floor survivors with final post-delta scores |
| Guarantee strength | nominally hard, actually voided by later sort | best-effort within `limit`, actually honored |
| `_unknown` district | received its own quota bucket | no quota; fill phase only |
| Floor drops | after truncation, no backfill | before selection, backfilled from full pool |

A district whose every candidate fails a hard floor is now **legitimately
unrepresented** — the code comments state exactly this.

### Deliberately unchanged

- `viability_diagnostics` / `hard_floors` / `demote_legs` API-meta wiring is
  intact. Drop counts now reflect the **full pool** rather than a truncated
  slice — the desired semantics for explaining unsaturated responses.
- `_apply_score_deltas_and_sort` still runs after the viability pass, so its
  contract of consuming and dropping the transient `viability_legs_fired` /
  `viability_delta` fields is preserved.
- The LLM rerank stage keeps its position and remains a production no-op
  (`EXPANSION_LLM_RERANK_ENABLED=False`).
- **No flag gating.** This is a bug-fix correction (same precedent as the
  whitespace PR): it corrects pipeline-order defects, not scoring semantics.

## 4. Tests

Seven new tests appended to `tests/test_expansion_advisor_service.py`
(from line 5206). They compose the exact helpers the main flow uses
(`_apply_market_viability_pass` → `_apply_score_deltas_and_sort` →
`_select_final_candidates`) on synthetic pools:

| Test | Pins |
|---|---|
| `test_pipeline_order_floor_drops_are_backfilled_to_limit` | District A all fails the population floor → response still has `limit` rows, A absent, B/C present; transient viability fields don't leak. |
| `test_pipeline_order_district_with_survivor_is_represented` | A district with one floor survivor (lowest score in pool) is still seated within `limit`. |
| `test_select_final_candidates_quota_math_8_districts_limit_15` | 8 districts, limit 15 → quota = 1; total = 15; every survivor-bearing district seated once before any second slot; output stays score-sorted. |
| `test_select_final_candidates_unknown_district_gets_no_quota` | Top-scoring `_unknown` candidate cannot claim a quota slot; enters via fill only; excluded when quota fills the limit. |
| `test_viability_cohort_identical_for_city_wide_and_multi_district` | Same pool → identical `demote_legs` thresholds, identical `hard_floors` diagnostics, identical per-candidate `market_viability_flag`, with 0 vs 2+ target districts. |
| `test_select_final_candidates_city_wide_is_bare_limit_slice` | City-wide and single-district selection return the exact same objects, same order, as `candidates[:limit]`. |
| `test_pipeline_city_wide_output_matches_pre_fix_order` | Full fixed pipeline output deep-equals the legacy city-wide composition (viability → deltas → `[:limit]`). |

**No existing tests pinned the old balancing-before-floors order — none were
modified.** (The PR-spec carve-out for touched tests is therefore empty.)

## 5. Validation performed

```bash
python -m pytest tests/test_expansion_advisor_service.py -q   # 172 passed
python -m pytest tests -q                                     # 2469 passed, 24 skipped
```

Lint note: `black --check` / `flake8` already fail on `main` for both
touched files (~1180 pre-existing flake8 hits in the service module; CI
runs neither tool). New code matches the file's existing style; no
reformatting was done, keeping the diff minimal.

## 6. Risk assessment

| Risk | Level | Mitigation |
|---|---|---|
| City-wide regression | **Very low** | Selection degenerates to `[:limit]`; byte-identity pinned by two tests. |
| Multi-district ordering drift | **Low–medium (intended)** | This is the fix: results are now ordered by final post-delta score with honored quotas. Shortlists for multi-district searches *will* change. |
| Response size change | **Low** | Responses get fuller (backfill), never smaller. |
| Cohort threshold shift | **Low–medium (intended)** | Multi-district viability thresholds now computed over the full pool — same meaning as city-wide. Demote rates for multi-district searches may shift accordingly. |
| Performance | **Negligible** | Viability/delta passes now process the full deduped pool for multi-district searches (they already did for city-wide); both are O(n) / O(n log n) over an in-memory list. |

## 7. Acceptance / rollout

- Post-deploy acceptance metric: under-fill counts from
  `scripts/diagnostics/balancing_order_probe.sql` (pre-merge baseline run
  by Ahmed) should drop to ~0 for multi-district searches with active
  hard floors.
- Sanity checks after deploy (per repo playbook): results still Riyadh-only,
  scores internally consistent, every survivor-bearing target district
  represented, response row counts saturate `limit`.

> Note: `docs/investigations/scoring_ranking_audit_2026-06.md` and
> `scripts/diagnostics/balancing_order_probe.sql` referenced by the task
> are not present in the repo at `154296c35`; the implementation was
> grounded on the task's code anchors, which matched that HEAD exactly.

## 8. Merge recommendation

**Merge after review — low risk.** City-wide behavior is provably
unchanged; multi-district changes are the intended correction; the full
suite is green; rollback is a single revert of `249f1e355`.
