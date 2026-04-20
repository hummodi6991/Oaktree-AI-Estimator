## 1. Timestamp fields found

- aqar.listings (raw Aqar table; Postgres schema `aqar`, referenced as `aqar.listings` — no explicit timestamp columns surfaced in any SQL the app issues; ingest uses `today = date.today()` rather than a listing timestamp at `app/ingest/aqar_sale_comps.py:38`, `app/ingest/aqar_rent_comps.py:203`). Columns known to be queried (`city`, `district`, `area_sqm`, `price_sar`, `price_per_sqm`, `property_type`, `title`, `description`, `listing_type`, `price_frequency`, `rent_period`, `ad_type`, `purpose`, `category`) — no timestamp field referenced. nothing found for first_seen/last_seen/posted/listed_at/updated_at on `aqar.listings` in repo code.
- candidate_location.model_scored_at — DateTime(timezone=True) — populated by profitability model scorer (app/models/tables.py:503)
- candidate_location.created_at — DateTime(timezone=True), server_default=now() — populated on insert (app/models/tables.py:506)
- candidate_location.updated_at — DateTime(timezone=True), server_default=now() — populated on insert; not auto-refreshed by trigger (app/models/tables.py:507)
- expansion_candidate.computed_at — DateTime(timezone=True), server_default=now() — populated on insert (alembic/versions/20260310_exp_adv_v0.py:71-76; INSERT at app/services/expansion_advisor.py:6835)
- expansion_search.created_at — DateTime(timezone=True), server_default=now() — populated on insert (alembic/versions/20260310_exp_adv_v0.py:24-29)
- external_feature — no timestamp columns (alembic/versions/0004_external_features.py:16-31; app/models/tables.py:213-222)
- hungerstation_* tables — table not found. "hungerstation" is a platform value, not a table name. Delivery data lands in `expansion_delivery_market` (below) and `restaurant_poi` with `source='hungerstation'`.
- expansion_delivery_market.scraped_at — TIMESTAMPTZ NOT NULL DEFAULT now() — populated on insert (alembic/versions/d4e5f6a1b2c3_create_expansion_advisor_tables.py:86)
- expansion_rent_comp.listed_at — DATE — nullable, populated from source listing date if available (alembic/versions/d4e5f6a1b2c3_create_expansion_advisor_tables.py:115)
- expansion_rent_comp.ingested_at — TIMESTAMPTZ NOT NULL DEFAULT now() — populated on insert (alembic/versions/d4e5f6a1b2c3_create_expansion_advisor_tables.py:116)
- expansion_competitor_quality.refreshed_at — TIMESTAMPTZ NOT NULL DEFAULT now() — populated on insert (alembic/versions/d4e5f6a1b2c3_create_expansion_advisor_tables.py:143)
- commercial_unit.first_seen_at — DateTime, server_default=now() — set on insert; the expansion scoring relies on this (app/models/tables.py:414)
- commercial_unit.last_seen_at — DateTime, server_default=now() — populated on insert; refreshed by upsert path in Aqar scraper ingest (app/models/tables.py:415)
- commercial_unit.llm_classified_at — DateTime — populated when LLM classifier runs (app/models/tables.py:411)
- restaurant_poi.observed_at — DateTime — populated by ingest (app/models/tables.py:323)
- restaurant_poi.google_fetched_at — DateTime(timezone=True) — populated when Google Places enrichment runs (app/models/tables.py:325)
- restaurant_heatmap_cache.computed_at — DateTime(timezone=True) — populated on cache write (app/models/tables.py:358)
- location_score.computed_at — DateTime — populated by scorer (app/models/tables.py:536)
- population_density.observed_at — DateTime — populated by ingest (app/models/tables.py:348)
- price_quote.observed_at — DateTime — populated by ingest (app/models/tables.py:270)

## 2. Is recency used in scoring?

- app/services/explain.py:29 — `recency_days = (date.today() - r.date).days` used as primary ranking key for sale-comp selection (lower days = better).
- app/services/explain.py:30 — `recency_score = recency_days / 365.0` folded into comp-ranking score.
- app/services/explain.py:58 — `days = sorted([(date.today() - c.date).days for c in comps if c.date])`; average comp age surfaced as a "recency" driver on the estimator response.
- app/services/explain.py:114-115 — second copy of the same recency_days / 365 scoring block in `explain.py` (rent-comps path).
- app/services/expansion_advisor.py:2045 — `_listing_quality(first_seen_at, ...)`: expansion advisor's freshness sub-score reads `commercial_unit.first_seen_at`.
- app/services/expansion_advisor.py:2089-2110 — banded freshness: `(datetime.utcnow() - first_seen_at).days` → 100/92/80/65/45/28/15 at 14/30/60/120/240/365 day cutoffs.
- app/services/expansion_advisor.py:2139 — `freshness * 0.30` is 30% of the listing_quality sub-score.
- app/services/expansion_advisor.py:4104-4105 — SQL SELECT exposes `cu.first_seen_at AS unit_first_seen_at` and `cu.last_seen_at AS unit_last_seen_at` for downstream scoring.
- app/services/expansion_advisor.py:5795 — `_listing_quality(first_seen_at=row.get("unit_first_seen_at"), ...)` wires first_seen_at into the preliminary score path.
- app/services/expansion_advisor.py:6469 — same wiring in the full/second scoring pass.
- app/services/expansion_advisor.py:2055 — docstring: "Freshness is measured from first_seen_at — the date the listing".
- app/services/expansion_advisor.py:2070-2071 — docstring: freshness measures "listing age, not scrape recency".
- app/services/expansion_advisor.py:2079-2080 — docstring: Patch 13 rebalance moved 10 points out of freshness into LLM signals.
- No `last_seen_at`, `posted`, `listed_at`, `updated_at`, `days_since`, or generic `age_` term is referenced inside ranking/scoring code beyond the hits above.
- No recency-based scoring on `expansion_candidate.computed_at`, `candidate_location.created_at/updated_at`, `expansion_delivery_market.scraped_at`, `restaurant_poi.observed_at`, or `expansion_rent_comp.listed_at/ingested_at` — those timestamps exist but are not fed into any ranking formula.

## 3. Sort order in Expansion Advisor endpoint

`GET /v1/expansion/searches/{search_id}/candidates` (app/api/expansion_advisor.py:1032) → `get_candidates()` (app/services/expansion_advisor.py:7177) issues `ORDER BY rank_position ASC NULLS LAST, compare_rank ASC NULLS LAST, final_score DESC, computed_at DESC` on `expansion_candidate` (app/services/expansion_advisor.py:7253). `computed_at` is only a final tiebreaker after the persisted deterministic rank, compare_rank, and final_score.
