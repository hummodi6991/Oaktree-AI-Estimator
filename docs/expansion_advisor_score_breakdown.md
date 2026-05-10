# Expansion Advisor — How the Site Score Is Computed

Audit of production code on `main` (HEAD `b2f031af7`, 2026-05-10). Every claim
is cited `path:line`. No application code modified.

---

## A. Pillar Inventory

### A1. Canonical pillar declaration

The 10 component weights are declared inside `_score_breakdown` at
`app/services/expansion_advisor.py:2817-2828`. Weights sum to 100 (asserted at
`app/services/expansion_advisor.py:2833-2836`).

The `chain_strength` weight is env-driven via `EXPANSION_CHAIN_STRENGTH_WEIGHT`
(default `3.0`, `app/core/config.py:322-323`). `competition_whitespace` absorbs
the equal-and-opposite move so the total stays at 100
(`app/services/expansion_advisor.py:2815-2816`).

**Backend pillars in weight-DESC order (production defaults):**

| # | Backend key              | Max weight (pts) |
|---|--------------------------|------------------|
| 1 | `occupancy_economics`    | 26.2924          |
| 2 | `listing_quality`        | 22.0000          |
| 3 | `brand_fit`              |  9.6404          |
| 4 | `access_visibility`      |  8.7640          |
| 5 | `demand_potential`       |  8.7640          |
| 6 | `competition_whitespace` |  5.7640 (= 8.7640 − chain_strength_weight) |
| 7 | `landlord_signal`        |  7.0112          |
| 8 | `confidence`             |  4.3820          |
| 9 | `delivery_demand`        |  4.3820          |
|10 | `chain_strength`         |  3.0000 (env-driven; default) |

> ⚠ The Diagnostics pie chart shows **9** of these 10 — `chain_strength`
> is omitted from the frontend `SCORE_COMPONENT_ORDER` (see A2), so its
> weighted contribution lands in `final_score` but never appears on the
> chart. This is a real source of the gap between "sum of 9 chart slices"
> and `final_score`, in addition to `bonus_detail`.

### A2. Frontend label mapping

Mapping table verified at
`frontend/src/features/expansion-advisor/DecisionLogicCard.tsx:24-46`:

```ts
SCORE_COMPONENT_ORDER = [
  "occupancy_economics", "listing_quality", "brand_fit",
  "landlord_signal", "competition_whitespace", "demand_potential",
  "access_visibility", "delivery_demand", "confidence",
];

SCORE_COMPONENT_LABEL = {
  occupancy_economics:    "Economics",
  listing_quality:        "Listing Quality",
  brand_fit:              "Brand Fit",
  landlord_signal:        "Landlord Signal",
  competition_whitespace: "Competitor Openness",
  demand_potential:       "Demand Strength",
  access_visibility:      "Access & Visibility",
  delivery_demand:        "Delivery Market",
  confidence:             "Data Quality",
};
```

Both aliases the prompt called out are confirmed:
- `competition_whitespace` → "Competitor Openness"
  (`DecisionLogicCard.tsx:41`)
- `confidence` → "Data Quality" (`DecisionLogicCard.tsx:45`)

`chain_strength` is intentionally **not** in `SCORE_COMPONENT_ORDER` and
**not** in `SCORE_COMPONENT_LABEL` — it is invisible to the Diagnostics
pie chart by design.

### A3. Compute function for each pillar

Each per-candidate sub-score (0–100) is produced by:

| Pillar                  | Compute fn `path:line`                              |
|-------------------------|-----------------------------------------------------|
| `occupancy_economics`   | `_economics_score`           (`app/services/expansion_advisor.py:4220`) |
| `listing_quality`       | `_listing_quality_score`     (`app/services/expansion_advisor.py:2347`) |
| `brand_fit`             | `_brand_fit_score`           (`app/services/expansion_advisor.py:1384`) |
| `access_visibility`     | `_access_visibility_score`   (`app/services/expansion_advisor.py:1714`) |
| `demand_potential`      | inline blend of `_population_score` + `_delivery_score` (`app/services/expansion_advisor.py:7341-7348`, `_population_score` at `:2074`, `_delivery_score` at `:2100`, blend weights `_demand_blend_weights` at `:2137`) |
| `competition_whitespace`| `_competition_whitespace_score` (`app/services/expansion_advisor.py:2154`) |
| `landlord_signal`       | `_landlord_signal_component` (`app/services/expansion_advisor.py:2737`) |
| `confidence`            | `_confidence_score`          (`app/services/expansion_advisor.py:2213`) |
| `delivery_demand`       | inline `provider_intelligence_composite` (`app/services/expansion_advisor.py:7583-7588`) |
| `chain_strength`        | `_chain_strength_score`      (`app/services/expansion_advisor.py:2194`) |

The contribution to `final_score` is `sub_score / 100 × max_weight`,
assembled in `_score_breakdown` at `app/services/expansion_advisor.py:2851-2866`.

---

## B. Per-Pillar Definitions (weight DESC)

### B.1 Economics

- **i. Backend key:** `occupancy_economics`
- **ii. Frontend label:** Economics
- **iii. Max weight:** 26.2924 / 100
- **iv. Plain-English definition:** Whether the unit pencils financially
  for an F&B operator at this rent, fit-out cost, and revenue potential.
  It blends a revenue-index estimate with rent burden, fit-out burden,
  cannibalization with existing branches, and parcel/format fit.
- **v. Inputs** (read in `_economics_score` at
  `app/services/expansion_advisor.py:4220-4322` unless noted):
  - `estimated_revenue_index` — produced by `_estimate_revenue_index`
    (`:3818-3923`) from listing street width, area vs target,
    listing_type, demand & whitespace scores, category, price tier.
  - `estimated_annual_rent_sar` — for listings: actual `unit_price_sar_annual`
    from `commercial_unit` (`:7488-7493`); for parcels: district rent
    estimate `_estimate_rent_sar_m2_year` (`:7497-7514`) backed by
    Aqar comp tables (`expansion_advisor_rent_comps` ingest job).
  - `estimated_fitout_cost_sar` — `_estimate_fitout_cost_sar`
    (`:7515`), service-model dependent.
  - `area_m2` — listing `unit_area_sqm` or parcel area.
  - `cannibalization_score` — `_cannibalization_score(distance_to_nearest_branch_m, …)`
    (`:7483`); distance from operator's existing branch list (SAR units / m).
  - `fit_score` — `0.55 * area_fit + 0.45 * zoning_fit_score` (`:7355-7357`),
    zoning derived from ArcGIS landuse code/label.
  - **Percentile rent burden** (preferred, listing path): `_percentile_rent_burden`
    (`:4021`), comp pool from `aqar_rent_comps` / `expansion_rent_comp`
    tables filtered by district + area band; fallback chain
    (`district → city_band_type → city`) with confidence damping at
    `_rent_burden_confidence` (`:3976`).
  - Coverage gaps: parcel path uses `absolute_legacy` (180 SAR/m²/mo
    ceiling, `:4259-4264`) — no peer-relative grounding. When
    `_percentile_rent_burden` falls back to `city_band_type` / `city`
    pools, `value_band_low_confidence=True` (`:4363-4373`) and the
    `value_band` bonus/penalty leg is suppressed (`:4541-4543`).
- **vi. Computation:**
  - `monthly_rent_per_m2 = annual_rent / (area * 12)` (`:4234`).
  - `rent_burden_score`: percentile mode for listings (`:4239-4257`);
    legacy ceiling-divide otherwise (`:4252-4264`).
  - `fitout_burden_score = clamp(100 - ((fitout/m² - 1800) / 2600) * 100)`
    (`:4267`).
  - `cannibalization_component = 100 - cannibalization_score` (`:4268`).
  - Confidence-weighted blend (`:4270-4282`):
    `revenue_index * (0.38 + (0.20 - rb_weight))`
    + `rent_burden * (0.20 * rb_confidence)` + `fitout * 0.14`
    + `cannibalization * 0.13` + `fit * 0.15`.
  - Result clamped to `[0,100]`.
- **vii. Known limitations:**
  - Parcel path uses absolute rent ceilings, not peer-relative — drives
    weaker rent_burden differentiation and disqualifies the candidate
    from `value_band` bonuses (`:4292-4308`).
  - When the comp pool is citywide, `rb_confidence < 1` and rent burden's
    weight is partially shifted to revenue (`:4274-4275`).
  - Below `EXPANSION_VIABILITY_ECONOMICS_MIN`, the economics viability
    leg fires (-10) on top of the low pillar score (`:4574-4579`).

### B.2 Listing Quality

- **i. Backend key:** `listing_quality`
- **ii. Frontend label:** Listing Quality
- **iii. Max weight:** 22.0 / 100
- **iv. Plain-English definition:** A signal that this specific Aqar
  listing is a good real-estate opportunity for F&B — recent, well
  presented, in a market that is currently active. Distinct from the
  Data Quality pillar (which measures whether the data we have on the
  listing is trustworthy).
- **v. Inputs** (read in `_listing_quality_score` at
  `app/services/expansion_advisor.py:2347-2495`; called at `:7596-7606`):
  - `effective_age_days` — GREATEST of `commercial_unit.aqar_updated_at`,
    `aqar_created_at`, `first_seen_at` via `_effective_listing_age_days`
    (`:2262-2299`).
  - `is_furnished` — `commercial_unit.is_furnished`.
  - `unit_restaurant_score` — `commercial_unit.restaurant_score` (legacy classifier).
  - `has_image` — non-null `commercial_unit.image_url`.
  - `has_drive_thru` — `commercial_unit.has_drive_thru`.
  - `llm_suitability_score` — `commercial_unit.llm_suitability_score` (Patch 12 LLM classifier).
  - `llm_listing_quality_score` — `commercial_unit.llm_listing_quality_score`.
  - `district_momentum_score` — `_district_momentum_score(db)` (`:375-473`):
    creates+updates over trailing 30 days on `commercial_unit`, joined
    against `external_feature_polygons_mat` (146 Riyadh districts from
    `aqar_district_hulls`); districts below `_MOMENTUM_SAMPLE_FLOOR=20`
    resolve to neutral 50.0 (`:2317-2322`, `:2469-2472`).
- **vi. Computation:**
  - Freshness banded by `effective_age_days`: ≤14→100, ≤30→92, ≤60→80,
    ≤120→65, ≤240→45, ≤365→28, else 15; `None`→50
    (`:2408-2425`).
  - Suitability = `clamp(llm_suitability_score)` if present, else
    `clamp(unit_restaurant_score * 2)` if positive, else 50
    (`:2434-2439`).
  - Image signal = `clamp(llm_listing_quality_score)` if present,
    else 100 if image_url, else 30 (`:2445-2448`).
  - Furnished signal = 100 if furnished, else 50 (`:2451`).
  - Momentum signal = `clamp(district_momentum_score)` else 50 (`:2469-2472`).
  - When `_MOMENTUM_ENABLED=True` (`:2311`), composite =
    `0.30·freshness + 0.20·suitability + 0.10·image + 0.05·furnished + 0.35·momentum`
    (`:2473-2479`).
  - `+5` drive-thru bonus when present (`:2492-2493`).
  - Parcels (no `commercial_unit_id`) shortcut to neutral 50 (`:2402-2403`).
- **vii. Known limitations:**
  - Districts with `<20` listings get neutral 50 momentum (no signal,
    no penalty; `_MOMENTUM_SAMPLE_FLOOR` at `:2322`).
  - Listings missing all three timestamps get neutral 50 freshness
    (`:2408-2409`).
  - Suitability collapses to neutral 50 for rows the LLM hasn't
    classified yet (`:2438-2439`).
  - Parcels (Tier-3 ARCGIS pool) are systematically pinned at 50.0.

### B.3 Brand Fit

- **i. Backend key:** `brand_fit`
- **ii. Frontend label:** Brand Fit
- **iii. Max weight:** 9.6404 / 100
- **iv. Plain-English definition:** How well the candidate matches the
  operator's stated brand profile — preferred districts, expansion
  goal (flagship / neighborhood / delivery_led / balanced), channel mix,
  area target, sensitivities to parking / frontage / visibility, and
  price tier. Heavily configuration-driven.
- **v. Inputs** (read in `_brand_fit_score` at
  `app/services/expansion_advisor.py:1384-1452`; called at `:7567-7582`):
  - `district` (Arabic-preferred string).
  - `area_m2`, `target_area_m2`.
  - `demand_score`, `fit_score`, `cannibalization_score`,
    `provider_density_score`, `provider_whitespace_score`,
    `multi_platform_presence_score`, `delivery_competition_score`,
    `visibility_signal` (= `access_visibility_score`), `parking_signal`.
  - `brand_profile` — operator request body fields:
    `preferred_districts`, `excluded_districts`, `expansion_goal`,
    `cannibalization_tolerance_m`, `parking/frontage/visibility_sensitivity`,
    `primary_channel`, `price_tier`.
  - `service_model` — `qsr` / `cafe` / `dine_in` / `delivery_first`.
- **vi. Computation:**
  - District component: 88 if preferred, 20 if excluded, else 60 (`:1391-1395`).
  - Goal component branches on `expansion_goal` (`:1400-1425`):
    flagship rewards area-vs-target ratio + visibility + demand;
    neighborhood rewards fit + spacing + parking; delivery_led rewards
    provider density + whitespace + low competition; balanced averages
    demand + fit + provider_whitespace.
  - Channel component via `_channel_fit_score` (called at `:1427-1432`).
  - Premium-tier penalty when visibility or district is weak (`:1437-1440`).
  - Final blend (`:1442-1452`): `0.18·district + 0.20·goal + 0.14·channel
    + 0.14·overlap_fit + (0.10..0.16)·parking + (0.12..0.15)·fit
    + (0.08..0.13)·visibility + 0.08·provider_whitespace − premium_penalty`,
    clamped.
- **vii. Known limitations:**
  - Almost every weight multiplier depends on the operator's
    self-reported `*_sensitivity` fields — pillar is highly sensitive to
    free-text request inputs.
  - District hits depend on string normalization
    (`norm_district` / `normalize_district_key`); districts with mixed
    Arabic/English labels can fail to match preferred lists.

### B.4 Access & Visibility

- **i. Backend key:** `access_visibility`
- **ii. Frontend label:** Access & Visibility
- **iii. Max weight:** 8.7640 / 100
- **iv. Plain-English definition:** Whether the storefront is on a road
  that customers can find and reach — measured primarily from the
  street width Aqar reports for the listing, with sensitivity to the
  brand's frontage and visibility preferences.
- **v. Inputs** (read in `_access_visibility_score` at
  `app/services/expansion_advisor.py:1714-1720`; called at `:7562-7566`):
  - `frontage_score` — `_frontage_score` (`:1616-1633`); when
    `unit_street_width_m` exists, returns
    `_frontage_score_from_street_width` (banded curve at `:1580-1594`).
    Parcel path blends OSM road context (perimeter, touches_road,
    nearby_road_count, nearest_major_road_m).
  - `access_score` — `_access_score` (`:1636-1646`); same listing-vs-parcel
    bifurcation, banded curve at `:1597-1613`.
  - `brand_profile` — `frontage_sensitivity`, `visibility_sensitivity`
    (read via `_sensitivity_weight`).
- **vi. Computation:**
  - `blend = 0.5 + frontage_weight * 0.2`,
    `weighted = frontage * blend + access * (1-blend)`,
    `score = clamp(weighted * (0.75 + visibility_weight * 0.25))`.
- **vii. Known limitations:**
  - For listings without `commercial_unit.street_width_m`, parcel-context
    fallbacks return 50 (neutral) when no OSM road context is available
    (`:1624-1628`, `:1641-1642`).
  - Bulk OSM road enrichment describes the surrounding network, **not**
    the listing's own street, so for listings the gate is `None`
    (unknown) when street_width is missing (`:2566-2568`).

### B.5 Demand Strength

- **i. Backend key:** `demand_potential`
- **ii. Frontend label:** Demand Strength
- **iii. Max weight:** 8.7640 / 100
- **iv. Plain-English definition:** Whether enough people live and order
  in the catchment to support a branch. Combines population reach
  (people within the catchment radius) with a delivery-supply proxy
  (and, when available, a realized-demand signal from rating-count
  deltas on delivery platforms).
- **v. Inputs** (assembled at
  `app/services/expansion_advisor.py:7341-7348` and re-blended at
  `:7457-7465`):
  - `population_reach` — bulk-enriched via `_bulk_enrich_population`
    (`:5873-5959`): `SUM(population_density.population)` within the
    service-model catchment radius (`_catchment_radii`, default 1200 m
    legacy). Source table: `population_density`.
  - `delivery_listing_count` — bulk-enriched via
    `_bulk_enrich_competitors` (`:5962-6111`) from `restaurant_poi`
    UNION `delivery_source_record`.
  - `realized_demand_30d` — Σ Δ`rating_count` across same-category
    branches in catchment (only when `EXPANSION_REALIZED_DEMAND_ENABLED`
    and ≥3 contributing branches; assembled at
    `:7330-7335`, generated by `expansion_advisor_delivery.py`
    snapshot pass).
  - `service_model` — sets pop/delivery blend at `:2137-2151`
    (e.g. `qsr` 0.60/0.40, `dine_in` 0.75/0.25, `delivery_first` 0.40/0.60).
- **vi. Computation:**
  - `pop_score = clamp((pop_reach / reference)**0.5 * 100)`, reference
    is service-model dependent (`_population_score`, `:2074-2097`).
  - `delivery_score = clamp((listing_count/40)**0.5 * 100)`; when
    `realized_demand` present, blend `(1-w)·listing + w·realized` with
    `w = EXPANSION_REALIZED_DEMAND_BLEND` (`:2100-2134`).
  - `demand_score = clamp(pop_score * pop_w + delivery_score * del_w)`
    (`:7348` and re-applied at `:7465` after district fallback).
- **vii. Known limitations:**
  - When `provider_listing_count < 5` AND `provider_platform_count < 2`
    AND `delivery_competition_count < 2`, the candidate falls into the
    district-level fallback and `delivery_listing_count` is rebuilt
    from district aggregates (`:7374-7449`); flagged in risks via
    `delivery_observation_mode == "inferred"` (`:2732`).
  - `population_reach == 0` short-circuits to 0
    (`_population_score`, `:2094-2095`).
  - `realized_demand_30d` requires ≥3 branches and the snapshot table
    (`:7334`); often absent today.

### B.6 Landlord Signal

- **i. Backend key:** `landlord_signal`
- **ii. Frontend label:** Landlord Signal
- **iii. Max weight:** 7.0112 / 100
- **iv. Plain-English definition:** An LLM read of the listing copy and
  contact behaviour to estimate whether the landlord is responsive,
  flexible, and serious about leasing. A first-class component since
  Patch 13.
- **v. Inputs** (read in `_landlord_signal_component` at
  `app/services/expansion_advisor.py:2737-2748`; threaded in at
  `:7616`, `:8641`):
  - `commercial_unit.llm_landlord_signal_score` (declared at
    `app/models/tables.py:428`) — single 0–100 value from the Patch-12
    LLM classifier.
- **vi. Computation:**
  - `clamp(float(score))`; `None` → neutral 50.0 (rows that haven't
    been LLM-classified yet).
- **vii. Known limitations:**
  - All structural-fallback rows (pre-LLM-backfill) score 50.0 — no
    differentiation.
  - For all parcel candidates (no `commercial_unit_id`) the value is
    always `None` → 50.0.
  - The signal is a single LLM emission with no calibration audit
    surfaced in the score breakdown.

### B.7 Competitor Openness

- **i. Backend key:** `competition_whitespace`
- **ii. Frontend label:** Competitor Openness
- **iii. Max weight:** 5.7640 (= `8.7640 - chain_strength_weight`,
  computed at `app/services/expansion_advisor.py:2816`)
- **iv. Plain-English definition:** How uncrowded the same-category F&B
  competition is around the candidate, on the brick-and-mortar side.
  Higher when there are fewer same-category restaurants in the
  competition radius.
- **v. Inputs** (read in `_competition_whitespace_score` at
  `app/services/expansion_advisor.py:2154-2191`; called at `:7350-7352`):
  - `competitor_count` — bulk-enriched via `_bulk_enrich_competitors`
    (`:5962-6111`) from `restaurant_poi` ∪ `delivery_source_record`,
    same-category, within the service-model competition radius
    (default 1000 m).
  - `confident` — `competitor_count_confident` (False when both sources
    return zero rows in radius — thin POI coverage, not a true
    greenfield; `:7282-7285`).
- **vi. Computation:**
  - `confident is False AND count == 0` → 50.0 (defensive midpoint).
  - `count == 0` → 100.0.
  - Else `score = clamp(max(15.0, 100 * (1 - log1p(count)/log1p(25))))`
    — log-scaled curve floored at 15.
- **vii. Known limitations:**
  - Inherits all upstream POI coverage gaps (chain_name nulls,
    district pollution — see Part D2).
  - `competitor_count_confident=False` is sticky on the ARCGIS-fallback
    SQL path (`:7280-7285`) and resolves to 50 — not always a true
    "no competitors here" signal.

### B.8 Data Quality

- **i. Backend key:** `confidence`
- **ii. Frontend label:** Data Quality
- **iii. Max weight:** 4.3820 / 100
- **iv. Plain-English definition:** How much we trust the data behind
  this candidate — measured rent vs estimate, measured area vs
  estimate, presence of a street-width measurement, an image, a
  landuse label, and observed population. Listings start higher than
  parcels by construction.
- **v. Inputs** (read in `_confidence_score` at
  `app/services/expansion_advisor.py:2213-2259`; called at
  `:7468-7477`):
  - `is_listing` — boolean (presence of `commercial_unit_id`).
  - `rent_confidence` — `"actual" | "estimate"` from the rent
    estimation pass (`:7470`).
  - `area_confidence` — same shape, from area resolution (`:7471`).
  - `unit_street_width_m` — `commercial_unit.street_width_m` (`:7472`).
  - `image_url` — `commercial_unit.image_url` (`:7473`).
  - `landuse_label` — ARCGIS landuse text (`:7474`).
  - `population_reach` — see B.5 (`:7475`).
  - `delivery_listing_count` — see B.5 (`:7476`).
- **vi. Computation:**
  - Listing path: base 30 +20 (rent actual) +15 (area actual) +15
    (street_width>0) +10 (image) +5 (landuse) +5 (pop>0); clamped 0-100.
  - Parcel path: base 40 +25 (landuse) +20 (pop>0) +15 (delivery>0);
    capped at **70** by `min(70.0, …)` at `:2259`.
- **vii. Known limitations:**
  - Parcels can never exceed 70 on this pillar (≈3.07 weighted points
    cap) regardless of geographic richness.
  - Each component is a binary flag — no graceful gradient between
    "bad data" and "great data".

### B.9 Delivery Market

- **i. Backend key:** `delivery_demand`
- **ii. Frontend label:** Delivery Market
- **iii. Max weight:** 4.3820 / 100
- **iv. Plain-English definition:** A composite read of the delivery
  marketplace around the candidate: how dense providers are, how much
  whitespace remains, how many platforms are present, and how saturated
  same-category delivery competition is.
- **v. Inputs** (assembled at
  `app/services/expansion_advisor.py:7583-7588`):
  - `provider_density_score` — log-scaled from `provider_listing_count`
    (`:7381-7383`, observed path) or district fallback (`:7422-7424`).
  - `provider_whitespace_score` — confidence-scaled from
    `delivery_competition_count` and `provider_density_score`
    (`:7384-7397`); district-fallback variant (`:7431-7433`).
  - `multi_platform_presence_score` — share of active platforms present
    (`:7402-7407` / district fallback `:7435-7440`).
  - `delivery_competition_score` — log-scaled saturation
    (`:7410-7412` / fallback `:7442-7444`).
  - All four originate from `_bulk_enrich_competitors` (`restaurant_poi`
    ∪ `delivery_source_record`) and `expansion_delivery_market` (filled
    by `app/ingest/expansion_advisor_delivery.py:87-187`).
- **vi. Computation:**
  - `composite = clamp(0.28·provider_density + 0.30·provider_whitespace
    + 0.22·multi_platform + 0.20·(100 - delivery_competition))` (`:7583-7588`).
- **vii. Known limitations:**
  - When direct radius observation is thin (`<5` listings, `<2`
    platforms, `<2` same-category), the candidate falls into the
    district-level estimate, with confidence ≤0.65 (`:7416-7449`); when
    even district data is empty, density/competition collapse to 0,
    presence to 0, whitespace to neutral 50 (`:7451-7455`).
  - `expansion_delivery_market` only normalizes 4 broad buckets —
    `international, traditional, coffee_bakery, seafood` (`:247-248`) —
    so finer category requests (e.g. `shawarma`) are mapped via
    `_CATEGORY_TO_DELIVERY_BUCKETS` (`:249+`).

---

## C. Bonuses, Penalties, and Viability Deltas

### C1. Score adjustments outside the nine pillar contributions (PR #1216)

All folded into `final_score` by `_apply_score_deltas_and_sort` at
`app/services/expansion_advisor.py:4396-4530`; recorded under
`score_breakdown_json["bonus_detail"]` (assembled at `:4485-4495`).

| Source                | Magnitude | Condition (citation)                                      |
|-----------------------|-----------|------------------------------------------------------------|
| `value_band_delta`    | **+4**    | `band == "best_value"` AND not low_confidence (`_value_band_score_delta`, `:4533-4548`; band thresholds `_VALUE_BAND_BEST_VALUE_MIN=75.0` at `:4332`; low_conf at `:4541-4543`) |
| `value_band_delta`    | **−6**    | `band == "above_market"` AND not low_confidence (`:4546-4547`; threshold `_VALUE_BAND_ABOVE_MARKET_MAX=25.0` at `:4333`) |
| `freshness_bonus`     | **+2**    | "New": `0 ≤ created_days ≤ _LISTING_FRESHNESS_DAYS` (=7) (`:4441-4445`, `:4452-4454`; constant at `:2344`) |
| `freshness_bonus`     | **+1**    | "Updated": `0 ≤ updated_days ≤ 7` AND not New (mutually exclusive) (`:4446-4451`, `:4455-4457`) |
| `momentum_bonus`      | **+2**    | `district_momentum.momentum_score ≥ _MOMENTUM_DISPLAY_THRESHOLD` (=70.0) AND `sample_floor_applied is False` (`:4462-4472`; constant at `:2333`) |
| `viability_delta`     | **−10 per leg, stacking** | Each fired leg of the market-viability pass adds another −10 (`_apply_market_viability_pass`, `:4551-5257`; per-leg at `:5107-5122`). Maximum 6 legs in stable order: `rpc, population, rent, economics, demand, radiance_growth` (`:5104-5119`). |

**Confirmation vs. user memory:** the magnitudes and conditions match
exactly. The viability legs can stack to **−60** (6 legs × −10), not −60
in practice because most are guarded by confidence requirements and the
RPC leg requires a min cohort. `bonus_detail` keys assembled at
`app/services/expansion_advisor.py:4485-4495`.

### C2. Hard floors that drop the candidate before scoring

Applied inside `_apply_market_viability_pass` *before* the soft-demote
pass at `app/services/expansion_advisor.py:4665-4760`. A floor of `0`
disables the corresponding gate.

| Floor                          | Setting                                                | Drop site `path:line`                              |
|--------------------------------|--------------------------------------------------------|----------------------------------------------------|
| Population reach floor         | `EXPANSION_VIABILITY_POPULATION_HARD_FLOOR`            | `app/services/expansion_advisor.py:4670-4709`, drop at `:4751-4753` |
| Commercial activity floor      | `EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR` (unique brands within 500 m, from `feature_snapshot_json["brand_presence"]["unique_brands"]`) | `:4675-4724`, drop at `:4754-4756` |
| Construction proximity floor   | `EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M` (drop if any `planet_osm_polygon` with `landuse='construction'` OR `building='construction'` within buffer) | `:4680-4741`, drop at `:4757-4759` |

These also become **hard-fail gates** (`overall_pass=False`) when their
setting is non-zero — `_OPTIONAL_HARD_GATES` registration at
`app/services/expansion_advisor.py:95-102`. They never appear on the
"Score contributions" chart because the candidate is excluded from the
shortlist entirely.

### C3. Viability gates that DO NOT subtract from score

`ADVISORY_ONLY_GATES` is declared at
`app/services/expansion_advisor.py:107-109`:

```python
ADVISORY_ONLY_GATES: frozenset[str] = frozenset({
    "radiance_growth_pass",
})
```

Only one entry — `radiance_growth_pass`. The remaining gate semantics:

- **Hard-fail gates** (failure flips `overall_pass=False`, but no
  point penalty either — they live on the gate card, not the score
  chart): the base set is `{"zoning_fit_pass", "area_fit_pass"}`
  (`_HARD_FAIL_GATES_BASE` at `:91-94`), expanded with the three hard
  floors above when configured.
- **Soft / advisory gates** (failure shows ❌ on the card but does not
  subtract from `final_score`):
  - `frontage_access_pass` — threshold 55, `:2570`
  - `parking_pass` — threshold 45, `:2582-2585`
  - `district_pass` — `excluded_districts` membership, `:2587-2593`
  - `cannibalization_pass` — distance-vs-tolerance, `:2595`
  - `delivery_market_pass` — only checked for `delivery` channel, `:2597-2607`
  - `economics_pass` — threshold `EXPANSION_VIABILITY_ECONOMICS_MIN`,
    `:2609`
  - `radiance_growth_pass` — strictly advisory (`ADVISORY_ONLY_GATES`)
- **Score-bearing legs** (these subtract −10 each via the soft-demote
  pass — they can fire independent of the gate's pass/fail state):
  `rent_per_capita_high`, `population_below_quartile`, `rent_high`,
  `economics_below_threshold`, `demand_low`, `radiance_growth_low`
  (`:5104-5119`).

So `economics_pass` failing on the gate card is **not** what subtracts
score; the parallel `economics_below_threshold` viability leg is. The
two are tested independently and can disagree (different thresholds /
data sources).

### C4. End-to-end `final_score` formula

```
final_score = clamp(
    sum(weighted_components.values())   # 10-pillar deterministic, includes chain_strength
    + value_band_delta                  # ∈ {+4, 0, −6}
    + viability_delta                   # ≤ 0; 6 legs × −10
    + freshness_bonus                   # ∈ {0, +1, +2}
    + momentum_bonus,                   # ∈ {0, +2}
    0.0,
    100.0,
)
```

- **Deterministic pillar sum:** `_score_breakdown` builds
  `weighted_components` at `app/services/expansion_advisor.py:2851-2866`
  and computes `final_score = round(sum(weighted_components.values()),2)`
  at `:2867`, then clamps to [0,100] at `:2888` (this is the
  "base_deterministic" later recorded in `bonus_detail.base_deterministic`).
- **Delta fold + clamp:** `_apply_score_deltas_and_sort`
  at `:4474-4497`:
  - `total_delta = value_band_delta + viability_delta + freshness_bonus + momentum_bonus`
  - `raw_final = base + total_delta`
  - `final_clamped = (raw_final < 0) or (raw_final > 100)` → recorded
    in `bonus_detail.final_score_clamped`
  - `_clamp(raw_final, 0.0, 100.0)` → written to both
    `_c["final_score"]` (`:4497`) and
    `score_breakdown_json["final_score"]` (`:4496`).
- **Rounding:** every weighted component rounds to 2 decimals
  (`:2852-2866`); `final_score` rounds to 2 decimals (`:2867`, `:4496-4497`).
- **Sort:** strict `(-final_score, parcel_id ASC)` (`:4524-4529`).
- **Display cap (separate from `final_score`):** `display_score =
  round(max(1.0, min(99.0, final_score)), 1)` — applied at
  `app/services/expansion_advisor.py:9083-9090` and mirrored into
  `score_breakdown_json["display_score"]`.

**Memo tab vs Diagnostics tab — same field?** Both ultimately read from
the same persisted candidate, but they prefer different keys:

- `frontend/src/features/expansion-advisor/DecisionLogicCard.tsx:259`
  — `breakdown?.display_score ?? breakdown?.final_score` (Diagnostics).
- `frontend/src/features/expansion-advisor/ExpansionMemoPanel.tsx:210`
  — `breakdown?.display_score ?? cand.final_score` (Memo).
- `frontend/src/features/expansion-advisor/StudyHeader.tsx:109`
  — `heroCandidate.final_score` (header pill — **no display_score
  fallback**).
- `frontend/src/features/expansion-advisor/studyAdapters.ts:497`,
  `:999` — normalize to `score_breakdown_json.display_score ?? final_score`.

For values strictly inside `[1, 99]`, `display_score == final_score`,
so they should agree. **Possible 86.7 vs 84.7 explanations** (filed in
Part E):
- `score_breakdown_json.final_score` mirrors the *post-clamp* score from
  `_apply_score_deltas_and_sort` (`:4496`); the candidate's column
  `final_score` is overwritten in the same call (`:4497`). They cannot
  diverge for in-flight searches.
- They *can* diverge for stored `expansion_candidate` rows persisted
  before the score-delta refactor landed — the persisted column may
  carry the pre-delta `base_deterministic` while the JSON carries the
  post-delta number. Worth verifying against the actual saved candidate
  (E.1).

**The 79.4 vs 86.7 gap (= 7.3):** matches `chain_strength_weighted +
sum(bonus_detail) + rounding`. Example consistent with the memory:
chain_strength contribution ≈ 1.5–3 pts (50 input × 3% weight ≈ 1.5),
`+4 best_value`, `+2 New`, `+2 Top-tier market` → 9.5; minus rounding
and a partial value-band could plausibly land at 7.3. The "9 components
sum to 79.4" line **excludes `chain_strength`** by construction, so the
gap is *not* purely `bonus_detail` — `chain_strength` is part of it.

---

## D. Data Sources Map

### D1. External data sources consumed by the score pipeline

| Data source                                                    | Lands in (table.column / JSON key)                                  | Ingest entry point                                                        | Read site in scoring                                                |
|----------------------------------------------------------------|---------------------------------------------------------------------|---------------------------------------------------------------------------|---------------------------------------------------------------------|
| Aqar.fm listings (rent, area, street width, images, drive-thru, dates) | `commercial_unit.*` (model: `app/models/tables.py:394-453`)        | `app/ingest/aqar/detail_scraper.py`, `app/ingest/aqar_rent_comps.py`, `app/ingest/aqar_sale_comps.py` | All `_listing_quality_score` / `_economics_score` inputs (`app/services/expansion_advisor.py:7468-7619`) |
| Aqar district hulls (146 Riyadh district polygons)             | `external_feature` (rows w/ aqar source) → matview `external_feature_polygons_mat` | `app/ingest/aqar_district_hulls.py`; matview refresh via `app/services/external_feature_refresh.py:21-44` | `_district_momentum_score` (`app/services/expansion_advisor.py:375-473`); district resolution (`app/services/district_resolver.py:80`) |
| Aqar / `expansion_rent_comp` (rent comparables)                | `expansion_rent_comp` (district-banded medians) and `aqar_rent_comps` raw | `app/ingest/expansion_advisor_rent_comps.py`; `app/ingest/aqar_rent_comps.py` | `_estimate_rent_sar_m2_year` (`:3664`); `_percentile_rent_burden` (`:4021`); `_estimate_rent_from_expansion_table` (`:3582`) |
| Google Places (chain_name, rating, price level)                | `restaurant_poi.*` (model: `app/models/tables.py:302-334`)          | `app/ingest/restaurant_pois.py`; `google-places-grid-search.yml`           | `_bulk_enrich_competitors` (`:5962-6111`); `expansion_competitor_quality` build (`app/ingest/expansion_advisor_competitors.py:135-261`) |
| HungerStation / Talabat / Mrsool delivery scrapes              | `delivery_source_record`; aggregated to `expansion_delivery_market` | `app/ingest/expansion_advisor_delivery.py:87-187` (delivery_source_record → expansion_delivery_market); rating-count snapshot at `:209-263` | `_bulk_enrich_competitors` (`:5962-6111`); `provider_intelligence_composite` (`:7583-7588`); realized-demand at `:7330-7335` |
| `expansion_competitor_quality` (POI quality + chain_strength)  | `expansion_competitor_quality.chain_strength_score`, `review_score`, `review_count` | `app/ingest/expansion_advisor_competitors.py:135-261` (built from `restaurant_poi` + delivery data) | `_chain_strength_score` input via `max_chain_strength` (`:7286-7296`, `:6055-6112`); `brand_presence` enrichment (`:8081-8184`) |
| NASA Black Marble VNP46A3 monthly nighttime radiance           | `district_radiance_monthly`                                         | `app/ingest/black_marble_radiance.py:1-220`                               | YoY signal feeds `radiance_growth` block on `feature_snapshot_json` and the `radiance_growth_pass` gate / viability leg (read at `:5043-5077`, `:8443-8451`); confidence via `app/connectors/blackmarble.py:47-87` |
| ArcGIS Riyadh parcels                                          | `riyadh_parcels_arcgis_raw` → proxy view `riyadh_parcels_arcgis_proxy` (constant `ARCGIS_PARCELS_TABLE` at `app/services/expansion_advisor.py:39`) | `ingest-arcgis-riyadh-parcels.yml`; `app/ingest/expansion_advisor_district_labels.py` | Tier-3 candidate pool, parcel perimeter (`:1810-1819`), landuse/zoning |
| OSM (`planet_osm_polygon`)                                     | `planet_osm_polygon` (existing OSM mirror)                          | (managed externally; queried directly)                                    | Construction-proximity bulk query (`app/services/expansion_advisor.py:8186-8257`) |
| OSM road / parking enrichment                                  | `expansion_road_context`, `expansion_parking_asset`                 | `app/ingest/expansion_advisor_roads.py`, `app/ingest/expansion_advisor_parking.py` | `_frontage_score`/`_access_score`/`_parking_score` parcel paths (`:1616-1679`) |
| Operator branch list + brand profile (request-side)            | Request body / `expansion_search` row                               | n/a (API input)                                                           | `_brand_fit_score` (`:1384`); `cannibalization_score` (`:7483`) |
| Population grid                                                | `population_density.population` (lat/lon or geom)                   | (managed externally)                                                      | `_bulk_enrich_population` (`:5873-5959`) |

### D2. Known-degraded sources (flagged here for the CEO doc)

- **`restaurant_poi.chain_name` mostly null.** The model declares
  `chain_name = Column(String(128))` at
  `app/models/tables.py:320` (no `not null`). This drives the
  `expansion_competitor_quality.canonical_brand_id` join (built at
  `app/ingest/expansion_advisor_competitors.py:135-261`) which directly
  feeds `chain_strength_score` and the brand-presence enrichment. **The
  exact null-row count claimed (30,940 of 31,313) cannot be verified
  from code alone — flagged in E.2.**
- **`restaurant_poi.district` pollution.** Free-text column populated by
  the upstream ingest (`app/ingest/restaurant_pois.py:51`,
  `:210`); only an unconstrained `String(128)` index
  (`app/models/tables.py:331`). Used as a coarse fallback only —
  district resolution prefers the spatial join against `external_feature`
  polygons (`app/services/district_resolver.py:80`).
- **Black Marble confidence carve-outs.** `evaluate_confidence` at
  `app/connectors/blackmarble.py:47-87` returns `(False, "pixel_floor")`
  when either current/prev pixel count `< PIXEL_COUNT_FLOOR` (=10);
  `(False, "small_district")` when `area_km2 < 0.5`
  (`SMALL_DISTRICT_FLOOR_KM2`); `(False, "large_district")` when
  `area_km2 > 500` (`LARGE_DISTRICT_OUTLIER_KM2`). Only confident signals
  feed the radiance leg / radiance gate
  (`app/services/expansion_advisor.py:5043-5077`,
  `:2611-2625`).
- **`profitability_train.py` operationally dead.** The training script
  `app/ml/profitability_train.py` exists, and there is a workflow
  `.github/workflows/train-profitability-model.yml`, but
  `profitability_score` is only **read** (never written) by the scoring
  service — it appears in 4 read sites
  (`app/services/expansion_advisor.py:5618`, `:5700`, `:8519`) and is
  surfaced as an opaque column. No live writer was found.
- **Aqar district median rent gap.** The `expansion_rent_comp` /
  `aqar_rent_comps` pools have districts with insufficient rows; the
  `_percentile_rent_burden` fallback chain is `district → city_band_type
  → city` (`app/services/expansion_advisor.py:4021+`). When fallback
  reaches `city_band_type` or `city`, `value_band_low_confidence=True`
  (`:4363-4373`) and the value-band ±delta is suppressed (`:4541-4543`).

---

## E. Open Questions

The investigation leaves these specific items unanswerable from code
alone — flag, do not guess:

1. **86.7 vs 84.7 hero discrepancy (Memo vs Diagnostics on the same
   candidate).** All four read sites (`StudyHeader.tsx:109`,
   `DecisionLogicCard.tsx:259`, `ExpansionMemoPanel.tsx:210`,
   `studyAdapters.ts:497/999`) resolve to `final_score` for values
   strictly inside `[1,99]`. The most likely explanation is that the
   persisted `expansion_candidate.final_score` column was written before
   the score-delta refactor (PR #1216) landed, while
   `score_breakdown_json["final_score"]` was rewritten on the next
   read. **Needs DB verification**: for the affected candidate, compare
   `expansion_candidate.final_score` (column) vs
   `score_breakdown_json->>'final_score'` (JSON). If they differ, the
   header pill (column) and the diagnostics pill (JSON) will disagree
   exactly as observed.

2. **Restaurant POI null-rate claim (30,940 / 31,313 rows).** The user
   memory cites these magnitudes; the code declares the column nullable
   but cannot confirm the proportions. Run:
   `SELECT COUNT(*) FILTER (WHERE chain_name IS NULL), COUNT(*) FROM restaurant_poi;`
   to verify before quoting in the CEO doc.

3. **`restaurant_poi.district` ~18% clean.** Same — needs a DB query
   to back the precise number. Code evidence supports the *direction*
   (free-text, weakly-typed, fallback-only) but not the percentage.

4. **`chain_strength` invisibility.** The pillar is part of
   `final_score` (3% by default) but is intentionally excluded from
   `SCORE_COMPONENT_ORDER` in `DecisionLogicCard.tsx:24-34`. There is
   no comment in the frontend code explaining the omission. Confirm
   with the frontend owner whether this is a deliberate
   product/legibility choice (the user memory implies "9 components"
   is canonical) or an oversight pending rebalance.

5. **Realized-demand snapshot coverage.** `realized_demand_30d` requires
   ≥3 rating_count snapshots (`app/services/expansion_advisor.py:7334`).
   Coverage in production is unknown from code; CEO doc should not
   claim "we use realized order volume" without confirming the share
   of candidates where the signal actually fires.

6. **Viability legs that can co-fire deterministically.** The pop, rent,
   economics, demand, radiance, and rpc legs each independently subtract
   −10. The conjunction's empirical fire-rate (and whether any pair
   double-counts the same underlying weakness) is not derivable from
   code. Worth a one-off telemetry pull from the
   `expansion_market_viability_pass` log line (`:5199-5214`) before the
   CEO doc quantifies the typical deduction.

---

*End of report — no application code was modified during this audit.*
