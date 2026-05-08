# Expansion Advisor — Ranking Quality Investigation

**Date:** 2026-05-08
**Scope:** Read-only diagnosis of whether the deterministic ranking is materially distinguishing top-tier from middle-tier candidates, or whether it is loosely structured noise.
**Status:** Line 1 (code-side analysis) is complete in this file. Lines 2 and 3 (cohort SQL + cross-search counterfactual SQL) are delivered as `cohort_sql_runbook.sql` in this directory. Quantitative claims about saturation and discriminative power must be confirmed by running that runbook against production — the code-side analysis identifies *where* to look, but cannot by itself measure cohort spread.

---

## Line 1 — Discriminative power, code-side analysis

All references below are to `app/services/expansion_advisor.py`.

### 1.1 — The score composition pipeline, end to end

The deterministic score for a single candidate is produced by `_score_breakdown(...)` (line 2819). The function:

- accepts ten raw component inputs, each clamped to `[0, 100]` by its upstream producer,
- multiplies each input by a fixed weight,
- sums the weighted values,
- clamps the sum to `[0, 100]` and rounds to 2 decimals.

```
final_score = clamp(
    round(sum_over_components(raw_input * weight / 100), 2),
    0, 100
)
```

The post-2026-05-07 weight table (lines 2885–2896) is reproduced below. `EXPANSION_CHAIN_STRENGTH_WEIGHT` is env-driven; whatever value it carries is subtracted from the listed `competition_whitespace` ceiling so the total stays at 100. The assertion at line 2901 enforces that invariant at startup.

| Component                | Weight (%) |
|--------------------------|------------|
| occupancy_economics      | 26.2924    |
| listing_quality          | 22.0000    |
| brand_fit                |  9.6404    |
| competition_whitespace   |  8.7640 − chain_strength_weight |
| demand_potential         |  8.7640    |
| access_visibility        |  8.7640    |
| landlord_signal          |  7.0112    |
| delivery_demand          |  4.3820    |
| confidence               |  4.3820    |
| chain_strength           |  EXPANSION_CHAIN_STRENGTH_WEIGHT (env, e.g. 3.0) |

After `_score_breakdown` runs, three later stages can change a candidate's *position* in the ranked list without changing its `final_score`:

1. **`_apply_value_band_pass`** (line 4498): "above_market" rows shift down up to 4 positions, "best_value" rows shift up to 3 positions, both subject to high-confidence guard. Writes `score_breakdown_json["value_pass"].value_uprank_delta` / `value_downrank_delta`.
2. **`_apply_market_viability_pass`** (line 4594): demotes candidates whose population, rent, economics, demand, radiance growth, or rent-per-capita legs fire confidently against them. Demotion is by `demotion_steps` positions (env-configured). Writes `score_breakdown_json["market_viability_flag"].demoted = true`.
3. **`_apply_rerank_to_candidates`** (line 1079): when `EXPANSION_LLM_RERANK_ENABLED` is True, reorders the top `EXPANSION_LLM_RERANK_SHORTLIST_SIZE` candidates within `±EXPANSION_LLM_RERANK_MAX_MOVE` positions of their pre-rerank index. With the flag off (the default per CLAUDE.md), every candidate keeps `final_rank == deterministic_rank`.

**Critical fact for the entire investigation:** all three of these passes are *positional reorder only*. They do not mutate `final_score` and they do not mutate any of the persisted weighted components. Comments at lines 4504, 4663, and 1083 confirm this. The implication is that the persisted `final_score` is an unmoved record of the deterministic scorer's output — it does not reflect the value-band nudge, the viability demote, or the LLM rerank.

`deterministic_rank` is assigned at line 1106 *after* the value-band + viability passes (those run at line 9100 and 9107) and the `[:limit]` truncation (line 9113). So `deterministic_rank` is the row's position post-value-band + post-viability, but pre-LLM-rerank. `rank_position` (line 9128) is the row's final post-rerank position.

### 1.2 — Theoretical max–min spread per component

Because each weighted contribution = raw_input × weight/100 with raw_input ∈ [0,100], the theoretical contribution range of every component equals its weight. With current weights:

| Component               | Theoretical contribution range to final_score |
|-------------------------|-----------------------------------------------|
| occupancy_economics     | [0, 26.29]   → range **26.29** |
| listing_quality         | [0, 22.00]   → range **22.00** |
| brand_fit               | [0,  9.64]   → range **9.64**  |
| competition_whitespace  | [0,  ~5.76]  → range ≈ **5.76** (assuming chain_strength_weight = 3.0) |
| demand_potential        | [0,  8.76]   → range **8.76**  |
| access_visibility       | [0,  8.76]   → range **8.76**  |
| landlord_signal         | [0,  7.01]   → range **7.01**  |
| delivery_demand         | [0,  4.38]   → range **4.38**  |
| confidence              | [0,  4.38]   → range **4.38**  |
| chain_strength          | [0,  ~3.00]  → range ≈ **3.00** |

If every component truly used its full theoretical range, the bottom-three components combined could shift `final_score` by at most ~11.76 points and the top three by ~58.0 points, so the score is *theoretically* dominated by the top three (occupancy_economics, listing_quality, brand_fit) — about 58/100 of the differential headroom even before considering saturation.

### 1.3 — Likely-actual spread per component for listings-branch candidates

Listings-branch is the only kind in production today. The producers that feed `_score_breakdown` for listings have known saturation behavior; this section identifies which components effectively saturate to a narrow band and which span their full theoretical range. **Quantitative confirmation must come from the SQL in cohort_sql_runbook.sql; statements below are the code-derived hypothesis the runbook is built to test.**

#### Likely full-range (high discriminative power)

- **`occupancy_economics`** — driven by `_economics_score` over revenue index, rent burden, fitout, area fit, cannibalization. The `_value_score` geometric mean of revenue index and rent-burden score (line 4411) is unbounded above by saturation and the rent burden percentile is per-listing district-relative. Expected to span most of [0, 100], so its weighted contribution is expected to span most of [0, 26.29]. **This should be the dominant differentiator.**
- **`listing_quality`** — `_listing_quality_score` (line 2415) blends freshness, momentum, suitability, furnished, image, drive-thru, llm_listing_quality. Freshness and momentum are recency-driven and inherently spread. After the 2026-05-07 rebalance lifting the weight from 11 to 22, this is the second largest theoretical lever and is expected to use most of its [0, 22.00] band.
- **`brand_fit`** — `_brand_fit_score` (line 1452) varies with district preference, format fit, demand×whitespace×cannibalization, parking, visibility. Spread should be moderate and meaningful within [0, 9.64].

#### Likely saturated (low discriminative power)

- **`confidence`** — for listings (line 2297) the score sums:
  base 30 + rent_confidence='actual' (+20) + area_confidence='actual' (+15) + unit_street_width_m>0 (+15) + image_url (+10) + landuse_label (+5) + population_reach>0 (+5) = up to 100. Production-grade Aqar listings carry rent_confidence='actual', area_confidence='actual' and an image URL by ingestion contract, and street_width / landuse / population_reach are populated for the materialized rows. Expected steady state: ~85–100, so the weighted contribution sits in roughly [3.7, 4.38] — **a narrow ~0.66 of the 4.38 theoretical range**. Any candidate that lacks one of those fields is rare enough to be noise rather than signal.
- **`chain_strength`** — env-weighted (default ~3.0). `_chain_strength_score` (line 2262) is a low-cardinality feature derived from the maximum brand chain present in the catchment; for a single brand profile + same area window, the cohort-level variation tends to be small. Even with full theoretical [0,100] spread, the *contribution* range is capped at ~3.0 points — barely larger than `_FUZZY_TIE_WINDOW`, so it cannot meaningfully separate ranks.
- **`delivery_demand`** — `provider_intelligence_composite` (line 7626) is a fixed-weight blend of provider_density, provider_whitespace, multi_platform_presence, and inverted competition. `provider_whitespace_score` defaults to 50.0 when district-level data is absent (line 7496) and `delivery_competition_score` is multiplied by a confidence factor `_dd_conf` that often pulls it toward zero. The composite tends to cluster, and even at full spread the weight is only 4.38. Likely contributes <2 points of differentiation across the cohort.
- **`landlord_signal`** — `_landlord_signal_component` (line 2805) returns a neutral 50.0 when the LLM landlord-signal field is None. Until the Patch-12 backfill is fully complete, a non-trivial slice of rows carries None → all of those rows produce identical 3.51 weighted contributions, narrowing the effective spread substantially. Where the LLM signal exists, contribution can span [0, 7.01], but the cohort behavior is mixed.
- **`access_visibility`** — `_access_visibility_score` (line 1782) is dominated by `unit_street_width_m`. Street width inside a single brand area window is often clustered (most commercial listings on similar arterial widths), and the `frontage`+`access` blend additionally has a brand-format coefficient. Expected behavior: spread is real but compressed inside a single search.
- **`competition_whitespace`** — varies by catchment competitor density; expected to behave like a moderate-spread component with weighted range of ~5.76, but bound below by `_dedupe_score_clones` and the dedupe cascade collapsing near-identical scores.
- **`demand_potential`** — pop_score×_pop_w + delivery_score×_del_w (line 7508). For listings inside the same area window, pop_score variation is moderate and delivery_score depends on listing_count; expected to use perhaps half of its [0, 8.76] band.

### 1.4 — Hypothesis: who's doing the work, who's dead weight

Code-side priors (to be confirmed by the runbook):

- **Top three discriminative components** (most weighted spread across the cohort):
  1. `occupancy_economics`
  2. `listing_quality`
  3. `brand_fit`
- **Bottom three "dead weight" components** (smallest weighted spread):
  1. `confidence` — saturated near top of band for listings
  2. `chain_strength` — capped contribution range ~3 points; low cardinality feature
  3. `delivery_demand` — clustered fallback behavior for low-confidence districts; small weight

The runbook's per-component spread query (#5) is the test of this hypothesis. If `occupancy_economics`, `listing_quality`, and `brand_fit` together do not account for the bulk of the weighted-contribution variance across the cohort, the hypothesis is wrong and the table above must be re-read against the empirical numbers.

### 1.5 — Can `rank_position` deviate from sorted-by-`final_score`?

**Yes, by design.** Three mechanisms move rows positionally without touching `final_score`:

| Mechanism | Caps | Persisted marker |
|-----------|------|------------------|
| Value-band pass | downrank ≤ 4 positions; uprank ≤ 3 positions, never past a peer with `final_score` more than `_FUZZY_TIE_WINDOW` higher | `score_breakdown_json["value_pass"].value_downrank_delta` / `.value_uprank_delta`; mirrored to top-level `value_uprank_delta` / `value_downrank_delta` after `_normalize_candidate_payload` |
| Market viability pass | demote by `EXPANSION_VIABILITY_DEMOTION_STEPS` positions on any of: rent_per_capita, population, rent, economics, demand, radiance growth | `score_breakdown_json["market_viability_flag"].demoted = true`, `.reason` = `_and_`-joined leg names |
| LLM rerank | ±`EXPANSION_LLM_RERANK_MAX_MOVE` (default 5) within the top `EXPANSION_LLM_RERANK_SHORTLIST_SIZE`; only when `EXPANSION_LLM_RERANK_ENABLED=True` | `rerank_applied`, `rerank_delta`, `rerank_reason`, `rerank_status` (top-level columns since 20260418_ea_rerank_persistence) |

Order of execution at the call site (lines 9100, 9107, 9124):

```
... dedup → fuzzy tiebreak → district balance →
    _apply_value_band_pass →
    _apply_market_viability_pass →
    truncate to limit →
    _apply_rerank_to_candidates  (deterministic_rank assigned here, then LLM may move within ±max_move) →
    enumerate → rank_position = compare_rank = index
```

Therefore, in the persisted DB:

- `deterministic_rank` is the row's position **after** value-band + viability + truncate, **before** LLM rerank.
- `final_rank` (column) == `rank_position` after the LLM rerank step.
- Sorting persisted candidates by `final_score DESC` reflects only the deterministic scorer; it ignores all three positional passes.
- A divergence between `rank_position` and `dense_rank() OVER (ORDER BY final_score DESC)` therefore decomposes into:
  - LLM rerank delta = `rerank_delta` (or `final_rank − deterministic_rank`).
  - Value-band delta = `value_uprank_delta − value_downrank_delta` (signed).
  - Viability demote delta ≈ `demotion_steps` × `market_viability_flag.demoted` (positive shift).
  - Plus dedup, fuzzy tiebreak, and district-balance reordering, none of which leave per-row markers — these are residual disagreements after the three labelled deltas are accounted for.

The runbook's query #7 (Line 2 cohort) and the cross-search comparison (Line 3) lean on these markers explicitly, so the user can attribute every disagreement to one of the four classes above (the three labelled passes plus the unlabelled residual).

### 1.6 — What this answers

| Question | Code-side answer | Needs SQL to confirm |
|----------|------------------|----------------------|
| Is `final_score` reliably distinguishing top-tier from middle-tier candidates? | The theoretical headroom is dominated by the top three components and the score has 100 points of theoretical range. But several weighted contributions (confidence, chain_strength, delivery_demand) are likely effectively constant in the cohort, so the *practical* range is materially narrower than 100. | Yes — runbook query #1 reports min/max/p10/p25/p50/p75/p90/stddev. |
| Which 3 signals are doing the work, which 7 are noise? | Hypothesis: top = occupancy_economics, listing_quality, brand_fit. Dead weight = confidence, chain_strength, delivery_demand. The middle four (demand_potential, access_visibility, competition_whitespace, landlord_signal) are intermediate. | Yes — runbook queries #5, #6, and the scorecard query. |
| Can `rank_position` diverge from sorted `final_score`? | Yes, via three labelled positional passes (value-band, viability, LLM rerank) and unlabelled dedup/balance reordering. | The runbook's query #7 quantifies the disagreement and attributes it to the labelled markers. |
| Does the same search twice give the same ranking? | The deterministic scorer is fully deterministic — same inputs produce the same `final_score`. But the value-band and viability cohort thresholds are computed *per search* (e.g. percentile cutoffs at line 4866 and 4904), so a different cohort yields different demote/uprank decisions. The LLM rerank is non-deterministic by nature. | Yes — runbook Line 3 query #1: Spearman over the intersection of common parcel_ids. |
| Does changing the brand vertical materially change the ranking? | `brand_fit_score`, `_economics_score`'s revenue-index multiplier, and the chain_strength leg all depend on the brand profile; in principle they should move the ranking. | Yes — runbook Line 3 query #2: Spearman across two brands sharing the area window. |

If the runbook returns answers consistent with the code-side hypothesis, the deterministic ranking is doing real work but is more concentrated than the 10-component façade suggests. If query #1 reports stddev < ~3 or p10–p90 < 8, or if query #5 reports >5 components with weighted-stddev < 0.5, the ranking is loosely structured noise outside the top three components and the value/viability passes are the only sources of meaningful late-stage differentiation.
