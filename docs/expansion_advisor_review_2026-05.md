# Expansion Advisor — End-to-End Review & Improvement Backlog

**Date:** 2026-05-31
**Scope:** Full-stack study of the Expansion Advisor feature (API/service scoring core, ingestion + ML + schema, React frontend, test/doc coverage), followed by a prioritized set of high-value improvements.
**Mission alignment:** decision quality, explainability, internal consistency, Riyadh correctness, safe/minimal diffs.

---

## Table of contents

1. [Executive summary](#1-executive-summary)
2. [Backend: API & service scoring core](#2-backend-api--service-scoring-core)
3. [Data layer: ingestion, ML & schema](#3-data-layer-ingestion-ml--schema)
4. [Frontend: components, flow & explainability](#4-frontend-components-flow--explainability)
5. [Test & documentation coverage](#5-test--documentation-coverage)
6. [Prioritized improvement backlog](#6-prioritized-improvement-backlog)
7. [Validation commands](#7-validation-commands)

---

## 1. Executive summary

The Expansion Advisor is a multi-source, multi-stage real-estate location-intelligence system for Riyadh. It implements a linear workflow **Search → Rank → Compare → Memo**, grounded in **5 normalized ingest pipelines** (roads, parking, delivery, rent, competitors) on weekly schedules, with scoring that blends **site fit**, **competitive pressure**, **economics**, **demand**, and **brand fit** into a weighted final score, optional LLM reranking, gate logic, and LLM-generated decision memos.

Test coverage is **extensive** for core operations (~16k lines of Python tests, ~9k of frontend tests) but has **critical gaps** in ranking diversity, cannibalization edge cases, gate-conjunction logic, and memo verdict thresholds.

The highest-value risks are **silent failures that corrupt rankings invisibly** (district-normalization mismatch, static delivery-category alias map, silent rerank reverts), **internal-consistency traps** (`display_score` vs `final_score` divergence, path-dependent confidence grades), and **explainability gaps** (computed evidence that is never surfaced to operators), plus **frontend/backend contract drift** and **Arabic i18n debt** in the Compare panel.

---

## 2. Backend: API & service scoring core

Primary files: `app/api/expansion_advisor.py`, `app/services/expansion_advisor.py`.

### 2.1 API endpoints

| Endpoint | Method | Request | Response |
|----------|--------|---------|----------|
| `/v1/expansion-advisor/districts` | GET | — | `DistrictOptionsListResponse` (Arabic/English labels) |
| `/v1/expansion-advisor/branch-suggestions` | GET | `q`, `limit` | `BranchSuggestionsResponse` (deduped poi/delivery) |
| `/v1/expansion-advisor/searches` | POST | `ExpansionAdvisorSearchRequest` | `ExpansionSearchResponse` |
| `/v1/expansion-advisor/searches/{id}` | GET | — | `ExpansionSearchDetailResponse` |
| `/v1/expansion-advisor/searches/{id}/candidates` | GET | — | `ExpansionCandidatesListResponse` |
| `/v1/expansion-advisor/searches/{id}/report` | GET | — | `RecommendationReportResponse` (top-3 + summary) |
| `/v1/expansion-advisor/candidates/{id}/memo` | GET | — | `CandidateMemoResponse` |
| `/v1/expansion-advisor/candidates/compare` | POST | `CompareCandidatesRequest` | `CompareCandidatesResponse` |
| `/v1/expansion-advisor/saved-searches` | POST/GET/PATCH/DELETE | `SavedSearchCreateRequest` | `SavedSearchResponse` |
| `/v1/expansion-advisor/decision-memo` | POST | `DecisionMemoRequest` | memo dict (memo_json, memo_text, cached) |

**Key request fields** (`expansion_advisor.py:104–133`): brand_name, category, service_model (qsr/dine_in/delivery_first/cafe), min/max/target_area_m2, target_districts, existing_branches, bbox, limit (1–100), brand_profile (price_tier, average_check_sar, primary_channel, parking/frontage/visibility sensitivity, expansion_goal, cannibalization_tolerance_m, preferred/excluded_districts), lang.

**Key response fields** (`expansion_advisor.py:209–261`): rank_position, confidence_grade (A/B/C/D), gate_status/reasons/feature_snapshot/score_breakdown JSON, top_positives/risks, decision_memo flags, value_score/band, rerank metadata.

### 2.2 Candidate generation

Three source paths (`expansion_advisor.py:6973–7142`):

- **Path A — `candidate_location` (Tier-1 cluster primaries):** used when ≥10 Tier-1 primaries with geom exist (`_query_candidate_location_pool`, line 7074). Returns bare coordinates; needs bulk enrichment.
- **Path B — fallback `commercial_unit` query:** when `candidate_location` has <10 rows (`_query_commercial_unit_candidates`, line 7131). Requires district resolution from arcgis_raw spatial join (lines 7164–7217).
- **Path C — ArcGIS parcel query (legacy/direct):** direct query of `riyadh_parcels_arcgis_proxy` (line 6655). Stratified when city-wide or ≥2 target districts.

**Stratification** (`expansion_advisor.py:6940–6998`): city-wide and multi-district (≥2) searches use fair-share per-district cap `(limit*3)/n_districts`, bounded `[5, 200]`. Single-district uses a global LIMIT.

**Pool limits:** `_CANDIDATE_POOL_LIMIT = 3500` (line 42), `_PER_DISTRICT_HEADROOM_MULTIPLIER = 3` (line 46).

**Enrichment pipeline** (lines 7245–8400+): bulk delivery counts, realized demand (Δrating_count), district delivery stats, district momentum (Black Marble VNP46A3 YoY radiance), rent cache pre-warm, radiance YoY, shortlist perimeter/roads/parking/foot-traffic/competitors.

**Deduplication:** `_dedupe_candidates()` (line 862) removes clones by (parcel_id, ~110m grid); `_dedupe_score_clones()` (line 937) drops score ties within 1.0 pt; report path uses `aggressive=True` (line 10840).

### 2.3 Ranking & scoring

**Gate evaluation** (`expansion_advisor.py:65–110, 1198–1227`):

- **Hard-fail gates** (all must pass for `overall_pass=True`): `zoning_fit_pass` (commercial/mixed code or label, score ≥50), `area_fit_pass`; optional env-gated hard floors `population_floor_pass`, `commercial_floor_pass`, `construction_proximity_pass`.
- **Advisory gates:** `radiance_growth_pass` (used for rescue, not hard block).

**Component weights** (sum = 100.0, asserted at `expansion_advisor.py:3028`):

| Component | Weight % |
|-----------|----------|
| occupancy_economics | 26.29 |
| listing_quality | 22.00 |
| brand_fit | 9.64 |
| demand_potential | 8.76 |
| access_visibility | 8.76 |
| competition_whitespace | `8.764 − CHAIN_STRENGTH_WEIGHT` |
| landlord_signal | 7.01 |
| delivery_demand | 4.38 |
| confidence | 4.38 |
| chain_strength | `EXPANSION_CHAIN_STRENGTH_WEIGHT` (env, ≈2.5) |

**Key sub-scores:**

- **Demand** — population/delivery blend, weights by service model (e.g. delivery_first 40/60, dine_in 75/25); population reach normalized via reference (~50k).
- **Whitespace** (line 2340–2380) — log-scaled competitor decay; 0 competitors→100 only if `confident=True`, else neutral 50 (defensive "F4" rule).
- **Economics** (line 4500–4588) — rent burden (percentile / absolute_fallback / absolute_legacy), fitout burden `(cost−1800)/2600`, cannibalization `100−cannibalization_score`, fit blend. Rent-burden weight ≤0.20 scaled by confidence; revenue absorbs the remainder.
- **Cannibalization** (line 3602–3631) — exponential decay `ceiling * 2^(−d/half_life)`; per-model params (qsr 1200m/82, dine_in 1800m/92, delivery_first 800m/78, cafe 1000m/80); extra penalty for delivery_first <400m.
- **Value score** (line 4603–4617) — geometric mean `sqrt(revenue_index × rent_burden_score)`. Bands: ≥75 best_value (+4 uprank), <25 above_market (−6 downrank), else neutral. Amber low-confidence flag when comp pool is city-wide.
- **Brand fit** (line 1513–1581) — district/goal/channel/overlap/parking/fit/visibility/whitespace components with sensitivity weights and a premium penalty.

**Market viability pass** (lines 4818–4999): five independent soft-demote legs (−10 pts each) — population, rent, economics, realized-demand, radiance-growth. Population/rent legs can be rescued by `radiance_growth_pass` when confident and YoY ≥ threshold.

**Score-delta folding** (lines 4663–4797): value-band delta → viability deltas → freshness bonus (+2/+1) → momentum bonus (+2); all clamped to [0,100].

**Confidence grade** (line 3278–3363): listings graded directly by score; parcels also factor `critical_missing` count and `data_completeness`. **Same score can yield different grades on listing vs parcel paths.**

### 2.4 Cannibalization / provider density / delivery market

- Nearest own-branch distance via `existing_branches` (line 8023); flags `<1.5km` / `>5km` in positives/risks.
- Provider density (`_delivery_score`, line 2280–2320): listed-count `sqrt(count/40)*100`, optionally blended with realized demand `sqrt(demand/REFERENCE)*100` at `EXPANSION_REALIZED_DEMAND_BLEND`.
- Catchment radii by model (line 836–848).
- Multi-platform presence = distinct platforms in catchment, normalized by active platform count (fallback 5).
- District-level delivery fallback when no spatial data (line 7428–7440).

### 2.5 Shortlist diversity

- Dedup passes as above. Shortlist size `min(len(prepared), max(limit, 25))` (line 8047).
- **No explicit geographic-spread constraint** on the final shortlist beyond dedup.
- Reranking (`_apply_rerank_to_candidates`, line 1017–1122) gated by `EXPANSION_LLM_RERANK_ENABLED` (default off); when off `deterministic_rank == final_rank`.

### 2.6 Decision-summary / memo generation

- **Legacy memo** (line 1472–1500): headline, fit_summary, top_reasons, top_risks, next_action, rent_context.
- **Structured memo** (Phase 3): headline_recommendation, ranking_explanation, key_evidence[{id,params,implication,polarity}], risks[], comparison, bottom_line; persisted to `decision_memo_json`.
- **Top positives/risks** (`_top_positives_and_risks`, line 3087–3275): rule-based thresholds on demand/whitespace/brand_fit/economics/gates/area/branch-distance/competitor-count + Phase 4.1 freshness/momentum notes.
- **Report verdict** (line 11000–11030): pass / validation-clear / fail states with district sequencing language.
- **Dimension winners** (line 10914–10948): highest_demand, best_economics, best_brand_fit, strongest_whitespace, most_confident, best_value.

### 2.7 Backend risk register

| Category | Risk | Notes |
|----------|------|-------|
| Scoring | MEDIUM | Cannibalization half-lives & rent ceilings (180/220) not Riyadh-validated |
| Data quality | MEDIUM-HIGH | Sparse rent comps → `absolute_legacy` ceiling can mis-estimate ~2× |
| Delivery matching | MEDIUM | Static `_CATEGORY_ALIAS_MAP`; unknown category → 0 matches (demotes, not "unknown") |
| Gate logic | MEDIUM | Fixed hard-fail gates; few operator-configurable thresholds |
| Confidence | MEDIUM-HIGH | Grade depends on candidate source (listing vs parcel) |
| Memo quality | MEDIUM | Structured memo feature-gated; fallback brief/text-only |
| Reranking | MEDIUM | Disabled by default; on failure reverts silently (no log/alert) |
| District match | MEDIUM-HIGH | Normalization mismatch silently drops candidates |
| Diversity | LOW | Dedup only; no spread constraint |

---

## 3. Data layer: ingestion, ML & schema

Primary dirs: `app/ingest/expansion_advisor_*`, `app/ml/`, `alembic/versions/20260310_*`–`20260314_*`.

### 3.1 Data sources

- **Competitors:** `restaurant_poi` (+ optional Google Places enrichment) → normalized `expansion_competitor_quality`. Chain canonicalization via `brand_alias` (CSV `data/brand_aliases.csv`) with Arabic-aware normalization + generic-term denylist.
- **Delivery:** `delivery_source_record` (hungerstation, jahez, keeta, talabat, mrsool, …) → `expansion_delivery_market`; daily snapshots → `expansion_delivery_rating_history` (realized-demand Δrating_count).
- **Rent:** `rent_comp` table or CSV/Kaggle → `expansion_rent_comp`.
- **Parcels:** `riyadh_parcels_arcgis_raw` enriched with `district_label` via LATERAL joins to `external_feature` (`expansion_advisor_district_labels.py:49–113`).
- **Roads:** OSM lines → `expansion_road_context` (road_class, frontage heuristics, uturn proxy).
- **Parking:** OSM polygon/point + Google Places grid → `expansion_parking_asset` (hardcoded walk/dropoff scores).

### 3.2 Ingest cadence & failure modes

| Job | Schedule (UTC) | Table | Failure mode |
|-----|----------------|-------|--------------|
| Roads | Mon 03:00 | expansion_road_context | exit(1) if no OSM source |
| Parking (OSM) | Tue 04:00 | expansion_parking_asset | skip if planet_osm_* absent |
| Parking (Google) | async w/ delivery | expansion_parking_asset | per-cell errors; resumable checkpoint; silent skip if no key |
| Delivery | Wed 05:00 | expansion_delivery_market | exit(1) on 0 rows unless `ALLOW_EMPTY_DELIVERY_INGEST=true` |
| Rent comps | Thu 06:00 | expansion_rent_comp | falls back to rent_comp table |
| Competitors | Fri 07:00 | expansion_competitor_quality | degrades w/o Google; fails if restaurant_poi missing |

- Most jobs support `--replace` (delete city=riyadh rows first). `expansion_advisor_refresh.py` runs alembic + logs counts. **Materialized views are a placeholder** (lines 50–58).
- No explicit freshness gates; relies on schedule. No data-lineage timestamp of *source* scrape in `feature_snapshot_json`.

### 3.3 Schema (key tables)

- **expansion_search** — brand/category/service_model, area bounds, target_districts, bbox, request_json.
- **expansion_candidate** — identifiers + geo + landuse, competitive counts, all score components, branch context, economics, brand/provider signals, site-fit scores, gate/feature/score_breakdown JSON, theses, comparable_competitors_json, rank fields, commercial-unit integration (source_type, commercial_unit_id, unit_* fields).
- **expansion_branch** — existing brand locations (cannibalization context).
- **expansion_brand_profile** — price tier, sensitivities, expansion_goal, cannibalization_tolerance_m, preferred/excluded districts.
- **expansion_saved_search** — persisted study state.
- **Normalized sources:** expansion_road_context, expansion_parking_asset, expansion_delivery_market, expansion_delivery_rating_history, expansion_rent_comp, expansion_competitor_quality.

### 3.4 ML models

- **`profitability_train.py`** — GradientBoostingRegressor; target = `success_proxy` on Tier-2 candidates; features pop_1km, competitor_count_1km, delivery_density_1km, rent_m2_month, area_sqm, district_encoded, source_tier. Output `models/profitability_v1.pkl` + meta; writes profitability_score/success_proxy/model_* back to `candidate_location`. Returns `insufficient_data` if <50 Tier-2 rows.
- **`restaurant_score_train.py`** — GradientBoostingRegressor; target = demand-per-restaurant proxy (explicitly **not** profitability); 12 features + one-hot category; H3 res-9 with ring-3 (~1.5km). Output `models/restaurant_score_v0.pkl` + meta; optional MLflow.
- **No Expansion-Advisor-specific ML pipeline**; reuses the above.

### 3.5 Completeness & key gaps

- `feature_snapshot_json` tracks `context_sources`, `missing_context`, `data_completeness_score` (no explicit formula; defaults 0).
- `*_score_mode` = observed (normalized table present) vs estimated (fallback).
- Hardcoded fallback scores (Google parking 65/55; OSM unknown 50; rent → aqar median).
- Known gaps: `intersection_distance_m` / `signalized_junction_distance_m` NULL placeholders; `corner_lot` placeholder; `frontage_length_m` approximated from line length; Google parking limited to built-up bbox; brittle late-night text heuristic; competitor reviews → 0 without Google; sparse rent by district.

### 3.6 Notable hardcoded constants (data layer)

| Constant | Location | Risk |
|----------|----------|------|
| chain size ≥5 | competitors.py:216 | excludes emerging brands |
| chain_strength ×12.0 | competitors.py:245 | arbitrary mapping |
| delivery_presence ×15.0 | competitors.py:256 | saturates ~6–7 listings |
| quality weights 0.15/0.35/0.25/0.15/0.10 | competitors.py:280–303 | not externalized |
| parking walk/dropoff heuristics | parking.py:164–175 | not survey-grade |
| delivery_provider_density gate 10.0 | service:2724 | calibrated 2026-05-12 |
| listing freshness bands (days) | service:2601–2614 | hardcoded decay |
| momentum display threshold 70.0 | service:2522 | must match frontend |
| listing-quality sub-weights | service:2662–2668 | rebalanced 2026-05-07 |

---

## 4. Frontend: components, flow & explainability

Primary dir: `frontend/src/features/expansion-advisor/`, API client `frontend/src/lib/api/expansionAdvisor.ts`.

### 4.1 Components & flow

`ExpansionAdvisorPage` orchestrates the **Search → Rank → Compare → Memo** flow across: `ExpansionBriefForm`, `ExpansionResultsPanel` → `ExpansionCandidateCard`, `ExpansionMemoPanel` (Memo + Diagnostics tabs) → `DecisionLogicCard`, `ExpansionComparePanel`, `ExpansionReportPanel`, `SavedSearchesPanel`/`SaveStudyDialog`. Memo/report caches are keyed by `"${id}:${lang}"`.

### 4.2 API handling

- Every search/memo/report request appends `?lang=`; `currentLang()` normalizes ar-SA variants.
- Decimal-as-string coercion at the boundary (`coerceCandidateNumerics`, expansionAdvisor.ts:617).
- Defensive defaults via `normalizeCandidate` (DEFAULT_GATE_REASONS / FEATURE_SNAPSHOT / SCORE_BREAKDOWN).

### 4.3 Score, gate, cannibalization, memo display

- Final score via `getDisplayScore()` prefers `display_score` from `score_breakdown_json`, falls back to `final_score` (`formatHelpers.ts:92–97`).
- Tri-state gates (`true`/`false`/`null`) rendered as pass/fail/unknown in `DecisionLogicCard`.
- Cannibalization shown in Economics tab + Compare (lower-is-better); **does not drive rerank** (only value_band does).
- Value band badges (green/amber/red) with low-confidence ⓘ.
- Rerank "Why #N" chip with delta arrow.
- LLM `DecisionMemoNarrative` (headline, key_evidence with polarity, risks w/ mitigation, bottom_line) with legacy fallback gating.

### 4.4 i18n / Arabic state

- ~470+ `expansionAdvisor.*` keys in `en.json`/`ar.json`; score labels mapped via `SCORE_LABEL_KEYS`.
- **Debt:** `ExpansionComparePanel.tsx:8–98` — `SUMMARY_LABELS` and `DIMENSION_GROUPS` hardcoded English (explicit "i18n debt tracked separately" comment). Compare outcome banner untested for Arabic.

### 4.5 Explainability gaps (frontend)

| Feature | Issue | Severity |
|---------|-------|----------|
| Missing score breakdown | blank Diagnostics tab, no "unavailable" message | Medium |
| Gate failure w/o explanation | gate listed, empty explanation span | Medium |
| Value score nullability | silent; no "percentile unavailable" message | Medium |
| Rent confidence | not shown on card (only detail panel) | High |
| Comparable scope (rent vs competitor) | competitor scope not shown | Medium |
| Aqar null-gate caveat | not explained in UI | Medium |
| Compare panel labels | Arabic missing | High |
| Site-fit context | observed-vs-estimated mode not surfaced | Medium |

### 4.6 Contract-drift hazards

- `LISTING_FRESHNESS_DAYS=7` and `MOMENTUM_DISPLAY_THRESHOLD=70` duplicated in `ExpansionCandidateCard.tsx:70–76` and backend "by convention, not shared config."
- `readJson<T>()` does no try/catch around `JSON.parse` (expansionAdvisor.ts:781).
- List endpoint omits `decision_memo*`; memo panel needs both `memo.candidate.decision_memo_json` and `candidateRaw` — no fallback message if `candidateRaw` is null.

---

## 5. Test & documentation coverage

### 5.1 What exists

- Python: `test_expansion_advisor_service.py` (~4.8k lines), `_regression.py` (~2.3k), `_api.py` (~1.3k), `_data_pipeline.py` (~1.4k), `_rerank.py`, `_radiance.py` (needs live PG), plus confidence/phase3/per-district/parking/brand-alias suites. ~22 files.
- Frontend: ~24 Vitest files (API normalization, components, helpers — scoreComponentMeta, formatHelpers, tiers).
- Docs: `docs/expansion_advisor_data_ingest.md` (124 lines) — covers the 5 normalized tables, preference chain, provenance; **does not** document scoring weights, gate thresholds, cannibalization formula, memo verdict rules, or diversity strategy.

### 5.2 Critical gaps (not tested)

- **Ranking diversity** — no district-spread or score-range assertion on top-N; tie-break determinism untested.
- **Cannibalization** — tolerance gate, multi-branch (3+), cross-district, decay-function shape untested.
- **Gate conjunction** — individual gates tested, combined AND/OR edge cases and exact thresholds not.
- **Memo** — verdict (go/consider/caution) thresholds, key risk/strength selection order untested.
- **Score weights** — exact weight values & sensitivity not asserted.
- **Confidence grade** — boundary values (e.g. 88 vs 89) untested.
- **Delivery** — platform weighting fairness, zero-row behavior untested.

---

## 6. Prioritized improvement backlog

### Tier 1 — Silent failures that corrupt rankings (highest value)

**#1 District-normalization mismatch silently drops candidates.**
Python + SQL mirror normalization (`expansion_advisor.py:920–944, 6520–6556`); divergence between frontend label, ingest label, and SQL normalization yields zero matches with no signal.
*Fix:* one shared normalization path; log a warning and surface `meta.unmatched_districts` when a requested district resolves to zero parcels. Low effort, high signal.

**#2 Static delivery-category alias map → unknown categories score 0.**
`_CATEGORY_ALIAS_MAP` (`expansion_advisor.py:154–225`) hardcoded; unmatched category demotes the candidate instead of marking the signal unavailable.
*Fix:* treat no-alias-match delivery signal as **unknown/neutral** (exclude from gating) + risk note, mirroring the F4 neutral-50 rule.

**#3 Rerank failure reverts silently.**
`_apply_rerank_to_candidates` returns `None` with no log/alert (`expansion_advisor.py:1063–1075`).
*Fix:* always populate + log `rerank_status` (`llm_failed`, etc.) and surface it in the decision-logic card.

### Tier 2 — Internal consistency & explainability

**#4 `display_score` vs `final_score` divergence.**
Deltas folded after base score (`expansion_advisor.py:4663–4797`); no enforcement the frontend reads the right field (`formatHelpers.ts:92–97`); memo can cite +4 freshness while final reflects +1.
*Fix:* persist `base_score`, itemized `deltas[]`, `final_score`; render the same ledger in card + memo. Biggest explainability win; removes a class of "numbers don't add up" bugs.

**#5 Path-dependent confidence grade.**
Same score → different grade for listing vs parcel (`expansion_advisor.py:3278–3363`).
*Fix:* surface the reason via existing `context_sources` / `*_score_mode` (observed vs estimated), which the UI currently never displays.

**#6 Computed evidence not surfaced (violates "never drop explainability").**
`rent_burden_confidence`, `value_score`/`value_band` (cards), competitor data **source** (curated vs raw POI), and `site_fit_context` modes are computed but hidden; missing breakdowns render as a blank tab.
*Fix:* add "data thin / estimated" badges driven by existing fields; "data unavailable" empty-states. Additive, low risk.

### Tier 3 — Contract drift & i18n debt

**#7 FE/BE thresholds matched "by convention."**
`LISTING_FRESHNESS_DAYS`/`MOMENTUM_DISPLAY_THRESHOLD` duplicated (`ExpansionCandidateCard.tsx:70–76` + backend).
*Fix:* echo thresholds in `meta.display_thresholds`; UI reads them. Eliminates drift.

**#8 Compare panel hardcoded English.**
`ExpansionComparePanel.tsx:27–98`. Violates "keep Arabic intact."
*Fix:* move `SUMMARY_LABELS`/`DIMENSION_GROUPS` to `en.json`/`ar.json`. Self-contained.

### Tier 4 — Tests & calibration

**#9 Add regression tests** for gate-conjunction matrix, cannibalization decay/tolerance + multi-branch, memo verdict thresholds, and a "top-N spans ≥K districts" diversity assertion. Should accompany any scoring change.

**#10 Document & confidence-flag Riyadh constants.**
Cannibalization half-lives, rent ceilings (180/220), fitout anchors (1800/2600) lack provenance; `absolute_legacy` rent fallback can mis-estimate ~2× in sparse districts.
*Fix (needs data):* document derivations in `docs/`; raise `value_band_low_confidence` at estimation time when comp pool is city-fallback; add a data-age/lineage signal to the feature snapshot.

### Suggested sequencing

Start with **#1, #3, #4, #6** (highest decision-quality leverage, mostly additive, low regression risk). **#7 + #8** are quick clean wins. **#9** accompanies any scoring change.

---

## 7. Validation commands

```bash
# Backend — narrow first, then broad
pytest tests/test_expansion_advisor_service.py -v
pytest tests/test_expansion_advisor_api.py -v
pytest tests/test_expansion_rerank.py -v
pytest tests/test_expansion_advisor_data_pipeline.py -v
pytest tests/test_expansion_advisor*.py -k cannibalization -v
pytest tests/test_expansion_advisor_radiance.py -v   # requires DATABASE_URL (live PG)
make test                                            # broad/risky changes

# Coverage on the service module
pytest tests/test_expansion_advisor*.py --cov=app.services.expansion_advisor --cov-report=html

# Frontend
cd frontend && npm run build
cd frontend && npm run test -- expansion-advisor

# Geospatial / ranking sanity checks (manual)
# - Results still in Riyadh?  Scores internally consistent?
# - Top candidates overly repetitive? Dedupe too aggressive?
# - Distances / areas / reach plausible? Performance acceptable?
```
