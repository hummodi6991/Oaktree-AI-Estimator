# Expansion Advisor — Google-data dependency map (READ-ONLY investigation)

> Scope: `app/services/expansion_advisor.py` and its scoring/ingest dependencies
> (`app/ingest/expansion_advisor_competitors.py`, `expansion_advisor_delivery.py`,
> and the `expansion_competitor_quality` materialization it builds).
> Excludes the Restaurant Location feature (`restaurant_location.py`) by design.
> No edits, no branch, no PR — findings + fix options only.
> Line numbers verified against live repo files (not from any stale doc).

## Executive summary

I traced every `google_*` read across the Advisor service and its scoring/ingest
dependencies.

**The headline: only one frozen field actually moves a candidate's rank —
`business_status`.** Everything else is either display-only (`rating`,
`review_count`), computed-but-never-read (`price_level`→`price_tier`), or not
value-read at all (`google_place_id`, `google_confidence`, `has_google`).
Critically, **`confidence_grade` and `data_completeness_score` do NOT touch any
Google field** — they are built from zoning/delivery/roads/parking coverage only,
so there is no confidence bias as Google data ages.

---

## Dependency table

| # | File:line | Function | Field / expression | Class | Staleness harm |
|---|-----------|----------|--------------------|-------|----------------|
| 1 | `expansion_advisor.py:6436-6437` | `_bulk_enrich_competitors` → SQL | `WHERE (rp.business_status IS NULL OR rp.business_status = 'OPERATIONAL')` | **FILTER** (the big one) | Real & growing. This is the *production* competitor count — it overrides the unfiltered main-query count (`:7115`, `:7159`). Drives `competitor_count` and `max_chain_strength`. |
| 2 | `expansion_advisor_competitors.py:211-212` | `_build_competitor_quality` → `chain_counts` CTE | `AND (business_status IS NULL OR business_status = 'OPERATIONAL')` | **FILTER** | Real & growing. Sets `chain_size` → `chain_strength_score` in ECQ → consumed as `max_chain_strength`. |
| 3 | `expansion_advisor_competitors.py:248-249, 283-286` | `_build_competitor_quality` | `rp.rating` → `review_score` and into `overall_quality_score` (0.35 leg *inside* ECQ) | **MEMO/REPORT** | Negligible to rank. `overall_quality_score` is only ever read into `comparable_competitors_json` (display); never scored. |
| 4 | `expansion_advisor_competitors.py:252` | same | `rp.review_count` → `ecq.review_count` | **MEMO/REPORT** | Display only (Market tab). |
| 5 | `expansion_advisor_competitors.py:273-276` | same | `rp.price_level` → `price_tier` CASE | **INERT** | `ecq.price_tier` is written but **never read** by the Advisor. (`price_tier` reads at `:1483/:1566/:4045/:4097` are the *search brand's own* profile tier, not ECQ.) |
| 6 | `expansion_advisor.py:3472-3473, 3504-3508` | `_comparable_competitors` (ECQ path) | `ecq.review_score/20.0 AS rating`, `ecq.review_count`, `ecq.overall_quality_score` | **MEMO/REPORT** | Display only → `comparable_competitors_json`. |
| 7 | `expansion_advisor.py:3533-3534, 3551-3552, 3575-3576` | `_comparable_competitors` (POI fallback) | `rp.rating`, `rp.review_count` | **MEMO/REPORT** | Display only. |
| 8 | `expansion_advisor.py:8366-8367, 8391, 8424-8428` | bulk competitors block | `ecq.review_score/20.0`, `ecq.review_count`, `ecq.overall_quality_score`, `rp.rating/review_count` | **MEMO/REPORT** | Display only → `comparable_competitors_json`. |
| 9 | `app/api/expansion_advisor.py:609` | branch-suggest autocomplete | `ORDER BY review_count DESC NULLS LAST` (restaurant_poi) | **INERT** (ordering) | Only re-orders the *user's own branch* autocomplete list. No candidate-rank effect. |
| 10 | `expansion_advisor_competitors.py:121-148, 343` | `_has_google_review_columns` / `has_google` | schema-existence check on `google_place_id`/`google_confidence` columns | **INERT** | Boolean = "do the columns exist", logged + put in stats. Never branches the INSERT, never value-reads. |
| — | (absent) | — | `google_place_id` / `google_confidence` *values* | **not used** | Searched all of `app/` — these appear only as **column-name literals** in the schema check (#10). They are **not** read as a trust/confidence signal anywhere in the Advisor. |

### Lines that look like Google deps but aren't (excluded, for the record)

- `expansion_advisor.py:309, 327, 364-366` — `AVG(rating)` / `city_avg_rating`:
  this is **`delivery_source_record.rating`** (delivery-platform rating), not
  Google. Live data.
- `expansion_advisor.py:7372, 8929-8950`, `expansion_advisor_delivery.py:208-265`
  — `realized_demand` / Δ`rating_count`: derived from **delivery** rating-count
  snapshots, not Google. Live data.
- `app/api/expansion_advisor.py:663` — `ORDER BY rating` is on
  `delivery_source_record`, not Google.

---

## How #1 propagates (why business_status is the only rank-mover)

`_bulk_enrich_competitors` is called on every shortlist path (candidate_location
`:7109`, commercial_unit `:7153`) and **overwrites** `competitor_count`,
`max_chain_strength`, `top_chain_strength_name`. That filtered count then feeds:

- **`competition_whitespace`** score — weight **5.764%** (`:3017`, via
  `_competition_whitespace_score` `:2341`).
- **`chain_strength`** score — weight **3.0%**
  (`EXPANSION_CHAIN_STRENGTH_WEIGHT` default in `config.py:336`;
  `_chain_strength_score` `:2383`), fed by `max_chain_strength` (ECQ
  `chain_strength_score`, itself filtered by #2).
- **`occupancy_economics`** (26.29%) — *indirectly & weakly*: `competitor_count`
  → `comp_signal` in `_rent_micro_location_multiplier` (`:3790`) → estimated rent.
- **Memo/report**: high/low-density risk & positive bullets (`:3209-3217`),
  `_build_explanation` (`:3663`), and `feature_snapshot_json["competitor_count"]`
  (`:9325`).

**No gate depends on it.** `_candidate_gate_status` (`:2687`) gates on
area/zoning/frontage/parking/economics/cannibalization and *delivery*
provider-density/platform — none Google-derived.

### Staleness mechanics

The filter keeps rows where `business_status IS NULL OR = 'OPERATIONAL'` and
drops `CLOSED_*`. Frozen means: a venue that is `OPERATIONAL` today and closes
tomorrow **never flips to `CLOSED_*`**, so it keeps counting as live competition
forever. Effect compounds slowly → competitor counts and chain sizes drift
**upward** → `competition_whitespace` and (mildly) rent-multiplier drift
**downward** for candidates near venues that have since closed. Magnitude bound:
combined direct weight ≈ **8.76%** of `final_score`, and only for the fraction of
candidates whose radius contains a since-closed venue. Real but slow.

---

## Codespace census queries (no DB access in this session — run these)

**A. business_status census (the key number):**
```sql
SELECT COALESCE(business_status,'(null)') AS status, COUNT(*) FROM restaurant_poi GROUP BY 1 ORDER BY 2 DESC;
```

**B. How many already-CLOSED rows would be re-included if you dropped the filter (fix option b):**
```sql
SELECT COUNT(*) FROM restaurant_poi WHERE business_status LIKE 'CLOSED%';
```

**C. Substitute-signal coverage — what share of POIs have a delivery match (basis for a non-Google liveness proxy):**
```sql
SELECT COUNT(DISTINCT rp.id) FILTER (WHERE dsr.matched_restaurant_poi_id IS NOT NULL) AS matched, COUNT(DISTINCT rp.id) AS total FROM restaurant_poi rp LEFT JOIN delivery_source_record dsr ON dsr.matched_restaurant_poi_id = rp.id;
```

**D. Freshness of the delivery liveness signal (how recent is `scraped_at`):**
```sql
SELECT date_trunc('month', scraped_at) AS m, COUNT(*) FROM delivery_source_record GROUP BY 1 ORDER BY 1 DESC LIMIT 6;
```

---

## Fix options per real dependency

### #1 + #2 — `business_status` filter (treat as one fix; they share intent)

- **(a) Leave as-is — frozen snapshot.** Accept slow upward drift in competitor
  counts. Zero code, zero risk; harm bounded to ~8.76% weight on the subset of
  candidates near since-closed venues. *Recommended default given the small
  weight.*
- **(b) Drop the operational filter entirely.** All rows count equally.
  **Downside: this re-includes the venues that were marked `CLOSED_*` *before*
  the freeze** (count = query B) — i.e. you'd start counting *known-dead* venues
  as live. Strictly worse than the freeze unless query B returns ~0.
- **(c) Substitute a non-Google liveness signal.** A viable partial substitute
  **exists in data we already have**: `delivery_source_record.scraped_at` (the
  `ix_dsr_platform_scraped` index confirms it) plus the
  `expansion_delivery_rating_history` snapshots — a POI still appearing in recent
  scrapes is plausibly live. Trade-off: coverage is limited to delivery-matched
  POIs (run query C to size it); OSM (`source='osm'`) carries no reliable closure
  tag in this schema. Best as an *additive* "recently-seen-on-delivery ⇒ keep"
  signal layered on the frozen `business_status`, not a full replacement.

### #3/#4/#6/#7/#8 — `rating` / `review_count` in display

- Weight on `final_score` = **0** (display only). **Recommendation: leave
  alone.** Numbers in the Market tab / `comparable_competitors_json` will simply
  be stale — cosmetic. If you ever want to de-stale the display, repoint the
  Market-tab `rating` to `delivery_source_record.rating` (already live), but
  that's a UX nicety, not a correctness fix.

### #5 — `price_level` → `price_tier`

- **INERT** (written to ECQ, never read by Advisor). Leave alone; no drift of any
  consequence.

### #10 + google_place_id/google_confidence — confidence/coverage

- `data_completeness_score` (`:2232-2239`) and `_confidence_score` (`:2402`)
  already use **source-agnostic coverage** (zoning, delivery, roads, parking).
  **No re-pointing needed** — the thing the brief worried about (confidence
  frozen to a Google match-rate) does not exist here. No bias up or down.

---

## The two-list split

### ✅ Leave alone (no meaningful rank drift)

1. `rating` / `review_count` everywhere in the Advisor — **display only**
   (#3,4,6,7,8).
2. `price_level` → `price_tier` (#5) — **computed, never read**.
3. `has_google` schema check (#10) — informational stat only.
4. `google_place_id` / `google_confidence` — **not value-read anywhere** in the
   Advisor; not a trust/confidence signal.
5. API branch-suggest `ORDER BY review_count` (#9) — re-orders the user's
   own-branch autocomplete only.
6. `confidence_grade` / `data_completeness_score` — **already Google-free**.

### ⚠️ Should address (degrades over time) — ranked by rank impact

1. **`business_status` operational filter** (#1 in `_bulk_enrich_competitors`,
   #2 in the competitors ingest `chain_counts`). The *only* frozen Google field
   that changes candidate rank. Within it, the higher-leverage path is
   **`competitor_count` → `competition_whitespace` (5.764%)** plus its indirect
   rent-multiplier touch on `occupancy_economics`; the lower-leverage path is
   **`chain_strength` (3.0%)**. Harm is slow (never-flipped closures silently
   counted as live) and bounded to candidates near since-closed venues.

---

## Bottom line

There is exactly one thing worth a follow-up decision — what to do about the
frozen `business_status` closure filter — and even that is a slow,
~≤8.76%-weighted drift. Run census queries A and B first: if B (`CLOSED_*` count)
is tiny, the freeze is nearly harmless and "do nothing (a)" is defensible; if A
shows `business_status` is mostly NULL anyway, the filter is barely doing
anything today and the drift is correspondingly small.

No code changes made — investigation only.
