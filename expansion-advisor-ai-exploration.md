# Expansion Advisor — AI Exploration Findings (read-only)

> No files were edited. Deliverable is this report. Line numbers cited throughout; anything whose feasibility depends on live data is flagged `FEASIBILITY UNCONFIRMED — needs probe` with SQL in §5.

## 1. Out of scope — already exists (exclusion list, do not re-propose)

- **LLM suitability / listing-quality / landlord classifier** — `app/services/llm_suitability.py` (text-first + ≤3-photo retry; emits `llm_suitability_score`, `llm_listing_quality_score`, `llm_landlord_signal_score`).
- **LLM decision memo** — `app/services/llm_decision_memo.py` (per-candidate narrative memo).
- **Bounded LLM shortlist rerank** — `app/services/expansion_rerank.py` → `generate_rerank` (reorders, never forks `final_score`).
- **GradientBoosting profitability model** — `app/ml/profitability_train.py` (writes `profitability_score`/`success_proxy`/`model_features`).
- **Hedonic rent model + restaurant-heatmap AI** — `app/services/hedonic_model.py`, `app/services/restaurant_heatmap_ai.py` (parcel/heatmap scoring, not the candidate path).

---

## 2. Phase 1 — System map & observed weaknesses

### 2.1 Candidate lifecycle (stage → file:function)

| # | Stage | Where |
|---|-------|-------|
| 1 | **Listing ingest** (Aqar T1, Bayut showroom-only) → `commercial_unit`; LLM-classify *at ingest* | `scripts/scrape_aqar.py:1420-1526`, `app/ingest/bayut/detail_scraper.py:115`; classifier `app/services/llm_suitability.py` |
| 2 | **Candidate creation** — listings → `candidate_location` (Tier-1 only; Tier-2 delivery / Tier-3 ArcGIS are enrichment) | `app/ingest/candidate_locations.py:_ingest_tier1_aqar` (47-110), `populate_candidate_locations` (546-603) |
| 3 | **Offline profitability scoring** (Tier-2 only) writes `profitability_score` | `app/ml/profitability_train.py:218-396` |
| 4 | **Search request** | `app/api/expansion_advisor.py:897` `POST /v1/.../searches` |
| 5 | **Bulk enrichment** — roads, parking, foot-traffic POIs, radiance, delivery-rating history | `app/services/expansion_advisor.py:_candidate_feature_snapshot` (1915), bulk blocks 7250 / 8297 / 7365 |
| 6 | **Component scoring** — demand, whitespace, brand-fit, economics, access, confidence, listing-quality, landlord, chain-strength, delivery | individual `_*_score` fns (1709-2942) |
| 7 | **Canonical score** — weighted sum `COMPONENT_WEIGHTS` | `_score_breakdown` (2943); weights 3009-3019; called 7981 / 9032 |
| 8 | **Gating** — hard-fail + advisory (incl. radiance) | `_candidate_gate_status` (2684); called 9074 |
| 9 | **Bounded rerank** | `_apply_rerank_to_candidates` (1017); called 9537 |
| 10 | **Memo** | `llm_decision_memo` (background task `app/api/expansion_advisor.py:711`) |
| 11 | **Persist/response** — `expansion_candidate` | service ~9561+ |

**`final_score` = `_score_breakdown`** (2943-3081). Weights: `occupancy_economics 26.29 / listing_quality 22 / brand_fit 9.64 / landlord_signal 7.01 / demand 8.76 / access_visibility 8.76 / competition_whitespace ~5.76 / chain_strength 3 / delivery 4.38 / confidence 4.38` (3009-3019).

### 2.2 Signal catalog (what each *actually* measures)

- **occupancy_economics (26%)** — rent burden vs. percentile comps + fitout + area-fit + cannibalization (`_economics_score` 4484; `_estimate_revenue_index` 4082 uses **street width as a drive-by traffic proxy**, 4112-4114).
- **listing_quality (22%)** — freshness band + LLM/structural suitability + image signal + furnished + **district momentum** (35% of the leg) (`_listing_quality_score` 2533-2681).
- **demand (8.76%)** — blend of `_population_score` (hand-set √ curve, 2254-2277) and `_delivery_score` (2280-2320).
- **access_visibility (8.76%)** — derived almost entirely from a single scalar `street_width_m` via hand-tuned piecewise curves (`_frontage_score_from_street_width` 1709, `_access_score_from_street_width` 1726).
- **whitespace / chain_strength / landlord / confidence / delivery** — hand-tuned curves (2340, 2380, 2929, 2399, 2280).

### 2.3 Ranked weaknesses (with evidence) — the foundation for Phase 2

**W1 — No ground-truth merchant-outcome label anywhere.** The only outcome-like target is `success_proxy`, computed *purely* from delivery metrics (`0.5·log1p(rating_count) + 0.3·(avg_rating) + 0.2·platform_count`) and only for **Tier-2 occupied venues** — not for the Tier-1 vacant listings that are the actual candidates. `app/ml/profitability_train.py:161-186, 223`. Its output is **surfaced but never enters `final_score`** (`expansion_advisor.py:8901`; no read into `_score_breakdown`). *This is the central ceiling.*

**W2 — No candidate-level decision feedback captured.** `UsageEvent` (`tables.py:238-251`) logs only HTTP request metadata (method/path/status/duration) — not *which candidate the user shortlisted, exported, saved, or opened*. No `Feedback`/`Outcome`/`Selection` table exists (grep on `tables.py`). So even the cheap implicit labels are being thrown away.

**W3 — Rich bilingual free-text is ingested but never a scored signal.** `commercial_unit.description` / `title` (AR+EN ad copy) are consumed once at ingest by the LLM classifier (`llm_suitability.py:132-146`) and otherwise unused; `restaurant_poi.name_ar`, `raw` JSON are stored unused. No embeddings/pgvector anywhere in the repo (grep: none). No semantic dedupe, no NL search.

**W4 — Only the *first* photo is captured; the gallery is discarded.** `scrape_aqar.py:1420` sends `photo_urls = [listing["image_url"]]` even though the classifier supports 3 (`llm_suitability.py:48 _MAX_PHOTOS_IN_RETRY = 3`). Fit-out/condition assessment runs on one thumbnail; the rest of the visual modality is lost at ingest.

**W5 — Demand is a stack of proxies, none calibrated to reality.** Population score is a hand-set √-curve with hand-set saturation references (2254-2277, `_population_reference` 849); delivery "realized demand" **systematically undercounts orders** (only 5-30% of orders produce a rating — author's own comment, 2294-2296); foot-traffic POIs are enriched in bulk (8297-8324) but only applied as a **small cafe-only ±12-pt bonus** (`_foot_traffic_score` used solely at 8671-8673, gated on `service_model == "cafe"`).

**W6 — Component weights are directives, not learned.** The weight block is a documented chain of CEO-directed rebalances and proportional rescales (`_score_breakdown` 2981-3019). No data tied any weight to an outcome (because of W1).

**W7 — Black Marble nightlights collapsed to a district-level advisory YoY gate.** A monthly time-series (`district_radiance_monthly`) is reduced to one YoY pass/fail (`radiance_growth_pass`, **advisory only**, 2803-2817); `radiance_median/sum/p90` are ingested and never read. The temporal trajectory (acceleration, seasonality, level) is unused.

**W8 — Access/frontage/revenue all hinge on one scalar (`street_width_m`).** Three legs key off the same hand-tuned piecewise curves (1709-1742, 4112-4114). When street width is missing the road context falls to a sentinel that is explicitly *"not a real measurement"* (`_road_signal_from_context` 3719-3720). Real geometry/accessibility is available (parcel polygons, roads) but unexploited.

**W9 — Legitimacy/late-night/drive-thru signals stored but unscored.** `aqar_advertisement_license` (REGA permit = real legitimacy signal), `candidate_location.has_drive_thru`, `road_class`, `supports_late_night`, `price_level` all stored, none read on the scoring path (confirmed via column cross-reference). *Most of these are deterministic wins, not AI — see honesty note in §3.*

### 2.4 Modality inventory

| Modality | Lands in | Scored today? |
|---|---|---|
| Structured (rent/area/street width/dates) | `commercial_unit`, `candidate_location` | Yes |
| **Free text (AR/EN copy, names, reasoning)** | `commercial_unit.description/title`, `restaurant_poi.name_ar/raw` | **No (ingest-time LLM only)** |
| **Image (listing photos)** | `image_url` (1 per listing) | **No (binary has_image / 1-photo LLM)** |
| Temporal (delivery ratings) | `expansion_delivery_rating_history` | Partial (30-day Δ proxy) |
| **Temporal (nightlights)** | `district_radiance_monthly` | **Advisory gate only** |
| Geospatial (parcels, roads, POIs, buildings) | `external_feature`, ArcGIS, `candidate_location.geom` | Partial (radii joins) |

---

## 3. Phase 2 — Grounded AI recommendations (ranked by ROI)

> Honesty note up front: **W9 is not AI** — REGA-license/drive-thru/late-night should be added to scoring with plain SQL + deterministic logic, not a model. I exclude it from the AI list deliberately. Likewise W8 is best served first by **GIS/spatial joins** (parcel frontage geometry), and only later by ML; I do not dress it up.

### R1 — Multilingual listing-text embeddings (self-supervised) → dedupe + surfaced suitability + NL search
- **Finding:** W3 (bilingual copy ingested, never a scored signal; no vector capability in repo).
- **Technique:** Multilingual sentence embeddings (e.g. a labse/e5-multilingual encoder) over `commercial_unit.description+title`, stored in a pgvector column. Three immediate uses, all label-free: (a) **near-duplicate / re-list detection** across Aqar↔Bayut beyond the REGA-license key (`candidate_locations.py` dedupe), tightening the shortlist diversity the gates already care about; (b) a **surfaced** "copy-implied fit" signal (cosine to curated F&B-intent exemplars) that the UI shows without forking `final_score`; (c) a **natural-language search** front door (the API has none — grep on `api/expansion_advisor.py`).
- **Data feasibility:** Confirmed text is stored. Coverage/length `FEASIBILITY UNCONFIRMED — needs probe` (P1).
- **Labels:** None required (self-supervised). Optional exemplar set is hand-curated, tiny.
- **Effort/Risk:** M / Low — additive column + read-only signal; cannot regress the canonical score.
- **Metric:** duplicate-pair recall on a hand-labeled 100-pair set; or NL-search top-5 hit rate.

### R2 — Decision-feedback instrumentation (the label unlock, MLOps not modeling yet)
- **Finding:** W1 + W2 (no outcomes; `UsageEvent` is request-only at `tables.py:238`).
- **Technique:** Emit candidate-level events (shortlisted / memo-opened / exported / saved / dismissed) — reuse the existing `UsageEvent.meta` JSONB or a small `expansion_candidate_feedback` table. This is the cheapest path to *any* supervised target and to learning W6's weights later.
- **Data feasibility:** Confirmed plumbing exists (`UsageEvent`); needs new event emission.
- **Labels:** This *creates* the labels (implicit relevance now; merchant outcomes later if branch-opening is ever recorded).
- **Effort/Risk:** S / Low.
- **Metric:** % of searches with ≥1 captured candidate-level interaction (instrumentation coverage) in week 1.

### R3 — Forecasting on owned nightlight + delivery-rating time-series → surfaced "trajectory" signal
- **Finding:** W7 + W5 (radiance reduced to one YoY gate; `median/sum/p90` and the monthly curve unused; delivery Δ is a 30-day point estimate).
- **Technique:** Lightweight per-district forecasting / trend decomposition (level + slope + acceleration) on `district_radiance_monthly` and `expansion_delivery_rating_history`. Output a calibrated **growth-trajectory** signal — surfaced, or a *bounded* reorder input, never a fork of `final_score` (2943).
- **Data feasibility:** Tables exist; **history depth is the make-or-break** → `FEASIBILITY UNCONFIRMED — needs probe` (P2, P3).
- **Labels:** None (forecasting on owned series).
- **Effort/Risk:** M / Medium (thin history could make forecasts noisy — kill if probe shows <18 months).
- **Metric:** backtest MAE of next-month radiance vs. persistence baseline; must beat naïve YoY.

### R4 — Multimodal fit-out scoring from the *full* photo gallery (embeddings)
- **Finding:** W4 (only first photo captured; classifier supports 3).
- **Technique:** Capture the gallery at ingest, then a vision-embedding "readiness" score (shell vs. finished vs. operating) via CLIP-style embeddings + nearest-exemplar — self-supervised, complementing (not duplicating) the existing LLM quality call.
- **Data feasibility:** **Requires an ingest change** (store gallery URLs) before any model — flag as a dependency, not free.
- **Labels:** None for the embedding variant; a supervised "readiness" head would need ~few-hundred hand labels.
- **Effort/Risk:** M-L / Medium (depends on R4-capture landing first).
- **Metric:** agreement (κ) with existing `llm_listing_quality_score` on a held-out set, plus lift on disagreements adjudicated by hand.

*(W8/W9 intentionally routed to deterministic SQL/GIS, not listed as AI.)*

---

## 4. Label / data ceiling

**We cannot do supervised "will this branch succeed" today.** There is **no ground-truth merchant outcome** anywhere — no revenue, sales, survival, or footfall per opened branch. The only outcome-shaped field, `success_proxy`, is (a) a delivery-rating popularity proxy that *undercounts orders* (`profitability_train.py:179-186`; comment 2294-2296), (b) computed only for **Tier-2 occupied venues**, not Tier-1 vacant listings, and (c) **not in `final_score`**. Therefore every Phase-2 recommendation above is deliberately **self-supervised / unsupervised / forecasting** — none asks for labels we don't have.

**Cheapest unblock (in priority order):** (1) **R2** — capture the advisor's own decision events to get implicit relevance labels now; (2) if the business ever records *which candidates became real leases/branches*, that single field plus periodic delivery-rating tracking of those branches becomes the first genuine outcome label, which would finally let W6's weights be *learned* instead of decreed.

---

## 5. Probes I need run (paste-ready; `PG*` exported, `sslmode=require`)

```sql
-- P1 (R1): bilingual description coverage & length on active F&B-suitable listings
SELECT count(*) AS total,
       count(*) FILTER (WHERE description IS NOT NULL AND length(trim(description)) >= 40) AS usable_desc,
       round(avg(length(coalesce(description,'')))) AS avg_len,
       count(*) FILTER (WHERE description ~ '[ء-ي]') AS has_arabic,
       count(*) FILTER (WHERE description ~ '[A-Za-z]') AS has_latin
FROM commercial_unit
WHERE status = 'active' AND restaurant_suitable IS TRUE;
```
```sql
-- P2 (R3): nightlight history depth & per-district span
SELECT count(DISTINCT district_key) AS districts,
       min(year_month) AS first_month, max(year_month) AS last_month,
       round(avg(months),1) AS avg_months_per_district
FROM (SELECT district_key, count(DISTINCT year_month) AS months,
             min(year_month) AS year_month
      FROM district_radiance_monthly
      WHERE source = 'nasa_blackmarble_vnp46a3_c2'
      GROUP BY district_key) s;
```
```sql
-- P3 (R3): delivery-rating history depth (snapshots per branch over trailing year)
SELECT count(DISTINCT source_record_id) AS branches,
       min(captured_at) AS first_seen, max(captured_at) AS last_seen,
       round(avg(snaps),2) AS avg_snaps_per_branch
FROM (SELECT source_record_id, count(*) AS snaps
      FROM expansion_delivery_rating_history
      GROUP BY source_record_id) s;
```
```sql
-- P4 (R4): photo availability on the candidate path (today only 1 URL is stored)
SELECT count(*) AS tier1,
       count(*) FILTER (WHERE image_url IS NOT NULL AND image_url <> '') AS with_photo
FROM candidate_location WHERE source_tier = 1;
```
```sql
-- P5 (W9 deterministic win sizing): REGA-license & drive-thru coverage on candidates
SELECT count(*) AS tier1,
       count(rega_advertisement_license) AS with_rega_license,
       count(*) FILTER (WHERE has_drive_thru IS TRUE) AS with_drive_thru
FROM candidate_location WHERE source_tier = 1;
```

---

## 6. Recommended first build

**Build R1 (multilingual listing-text embeddings) — and run R2 (feedback instrumentation) in parallel as the label-unlock track.**

Why R1 wins as the *first AI capability*: it is the only recommendation that (a) exploits data we already collect on every single candidate (`description`/`title`), (b) needs **zero labels**, (c) cannot regress the canonical `final_score` (it adds a vector column + a surfaced/dedup signal), and (d) returns three independent wins (tighter cross-portal dedupe, a copy-implied fit signal, and the natural-language search the API currently lacks). Given the label ceiling in §4, label-free near-term bets are explicitly the strongest, and R1 is the cleanest of them.

**Smallest viable first experiment:** embed `description+title` for the *current active Tier-1 set only*, store vectors, and evaluate **near-duplicate detection** against a hand-labeled ~100-pair Aqar↔Bayut sample — measuring duplicate-pair recall vs. the existing REGA-license dedupe. One read-only batch, one metric, no scoring-path change. If P1 shows description coverage is thin, fall back to embedding `title + neighborhood + listing_type` before abandoning.

*(R2 ships alongside because it costs almost nothing and every future supervised idea — including learning W6's weights — is blocked until decision events start accumulating.)*
