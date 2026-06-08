# Dine-In Demand Stack — Read-Only Feasibility Findings

**Scope:** verify the 3-layer dine-in demand design is buildable on the *current*
`hummodi6991/Oaktree-Atlas` schema, locate exact insertion points, surface
blockers, and produce go/no-go probes. **Nothing was implemented.** All citations
are against the live tree (`app/services/expansion_advisor.py` is 11,260 lines).

## Per-layer verdict (read first)

- **L1 — modeled demand-generator index: BUILDABLE, with one data-availability
  check.** Every input table is referenced by existing code: `population_density`
  (catchment pop), `planet_osm_point`/`planet_osm_polygon` (OSM generators),
  `overture_buildings` (floors proxy), `district_radiance_monthly` (radiance).
  An almost-complete prototype already exists — `_demand_anchor_score`
  (`app/services/restaurant_scoring_factors.py:858`) — but it is wired into the
  **restaurant-heatmap** path, *not* Expansion Advisor ranking. The Advisor's only
  generator signal today is the cafe-only `_foot_traffic_score`. The one thing I
  cannot confirm without DB access: that `planet_osm_*` and `overture_buildings`
  (both **externally imported**, not migration-created) are actually populated for
  Riyadh in the target environment. `scripts/diagnostics/l1_generator_coverage.sql`
  resolves exactly that.

- **L2 — BestTime busyness: NEEDS-DECISION (gated on a coverage probe, then
  buildable).** The venue universe (`restaurant_poi`), the snapshot-table template
  (`expansion_delivery_rating_history`), the connector pattern, and the bulk-enrich
  + scoring read points all exist and are clean to extend. The blocker is *external*:
  whether BestTime actually covers enough Riyadh F&B venues with usable forecasts to
  justify the paid tier. `scripts/diagnostics/besttime_coverage_probe.py` answers
  that for ≤80 free credits before any spend or schema change.

- **L3 — SAMA POS + GASTAT city multiplier: BUILDABLE (ingest) but NEEDS-DECISION
  (insertion point).** The storage home (`market_indicator`) and the read pattern
  (`latest_re_price_index_scalar`, 2014=1.0) exist and are the right template to
  reuse. **However, there is currently NO uniform city-level multiplier anywhere in
  Expansion Advisor scoring** — `final_score` is a pure per-candidate weighted sum
  with no global scalar hook. So L3 ingest is a copy of an existing pattern, but
  *where* the multiplier attaches to the score is a genuine design decision that must
  be made deliberately (and must not become a per-listing ranker).

---

## Part 1 — Layer 1: modeled demand-generator index

### 1.1 Current proxy — what `foot_traffic` does today

`_foot_traffic_score(nearby_amenity_count)` — `app/services/expansion_advisor.py:1778-1794`.
A log-scaled 30→90 score over the **count of schools/mosques/parks/malls within
500 m**. It reads OSM:

- Bulk SQL block (cafés only): `app/services/expansion_advisor.py:8435-8504`. It
  UNIONs `planet_osm_polygon` (amenity ∈ school/university/college/place_of_worship/
  mosque; leisure ∈ park/garden/playground; shop=mall; building ∈ mosque/school/
  university — `:8449-8452`) and `planet_osm_point` (`:8461-8463`), counted with
  `ST_DWithin(... ::geography, ST_MakePoint(lon,lat) ..., 500)` (`:8474-8475`).
- Both tables are guarded by `_cached_table_available(db, ...)` (`:8443`, `:8455`;
  helper at `:755`) so the block silently no-ops when OSM isn't present.

**Weight / where it lands:** the score is **not** a first-class component in the
factor table (`component_weights`, `:3091-3102`). It is applied only as a small
cafe-only nudge into `occupancy_economics` / preliminary score:
`_ft_bonus = (_foot_traffic_score(_ft_count) - 30.0)/60.0 * 12.0` at
`app/services/expansion_advisor.py:8846-8848`, and only when
`service_model == "cafe"` (`:8846`). **For `dine_in` it contributes nothing today.**

So L1 is replacing a signal that, for dine-in, is effectively absent.

### 1.2 Generator sources on hand (all coordinate-queryable)

The live scoring universe is `candidate_location` (Tier-1 cluster primaries) when
≥10 exist — `app/services/expansion_advisor.py:7107-7116` — joined to
`commercial_unit` (`:6108-6112`). parcel_id joins don't work for these sources, so
everything is enriched by coordinate via the `_shortlist_coords` /
`ST_MakePoint(lon,lat)` pattern (`:8227-8233`). All L1 inputs follow that pattern:

| Generator | Table | Key cols | Geom / SRID | Cited |
|---|---|---|---|---|
| Population | `population_density` | `population`, `lat`, `lon`, `h3_index` | **no geom** (H3; build point from lat/lon) | model `app/models/tables.py:342-353`; enrich `expansion_advisor.py:6383-6416` |
| OSM (points) | `planet_osm_point` | `amenity`,`shop`,`leisure`,`tourism`,`railway`,`office`,`building`,`way` | `way` = **4326** (osm2pgsql `--latlong`, `.github/workflows/osm-import.yml:270`) | queried `expansion_advisor.py:8455-8464`, `restaurant_scoring_factors.py:900-911` |
| OSM (polys) | `planet_osm_polygon` | same + `landuse` | `way` = 4326 | `expansion_advisor.py:8443-8454` |
| Buildings | `overture_buildings` | `class`,`subtype`,`num_floors`,`height`,`geom` | `geom` = **32638** | `overture_buildings_metrics.py:30-32`, floors proxy `:43-46` |
| Radiance | `district_radiance_monthly` | `radiance_mean/median/sum/p90`,`pixel_count_valid`,`district_key`,`year_month` | **no geom** (per-district key) | migration `alembic/versions/20260501_add_district_radiance_monthly.py:20-40`; read `expansion_advisor.py:7744-7789` |

**Proposed OSM tag → generator mapping** (uses dedicated `default.style` columns the
repo already queries; verified against `expansion_advisor.py:8449-8463` and
`restaurant_scoring_factors.py:772-784`):

- **offices:** `office` IS NOT NULL OR `building` ∈ (office, commercial)
- **malls/retail:** `shop` ∈ (mall, supermarket, department_store, wholesale) OR `amenity`=marketplace
- **transit:** `railway` ∈ (station, halt, tram_stop, subway_entrance, stop) OR `amenity`=bus_station — *(public_transport sits only in the hstore `tags` column, so it is intentionally excluded from the probe to avoid an hstore dependency; add later if needed)*
- **mosques:** `amenity` ∈ (place_of_worship, mosque) OR `building`=mosque
- **schools:** `amenity` ∈ (school, college, university, kindergarten) OR `building` ∈ (school, university)
- **hospitals:** `amenity` ∈ (hospital, clinic, doctors)
- **hotels:** `tourism` ∈ (hotel, motel, hostel, guest_house) OR `building`=hotel

**Buildings → daytime-population proxy:** `floors_proxy = num_floors, else round(height/3.2), clamped [1,60]` — exactly `overture_buildings_metrics.py:43-46`. `ms_buildings_raw` has **footprint area only, no floor/height** (`alembic/versions/d2c3b4a5e6f7_create_ms_buildings_raw.py:17-26`), so Overture is the floor-density source; MS buildings can only add footprint coverage.

**Radiance is already wired as advisory** via `radiance_growth_pass`: per-district
YoY% from a rolling-6 window over `district_radiance_monthly`
(`expansion_advisor.py:7744-7789` builds `value_yoy_pct`/`confident`), consumed as
an advisory gate (`:2813-2820`, `ADVISORY_ONLY_GATES`) and as a soft-demote/rescue
leg in `_apply_market_viability_pass` (`:4973-4994`). L1 should treat radiance as an
**advisory tilt**, not double-count it as a generator weight.

### 1.3 Insertion point & proposed wiring (no weights changed)

The demand leg is assembled at `app/services/expansion_advisor.py:7880-7887`:

```
pop_score    = _population_score(population_reach, service_model=...)   # :7880
delivery_score = _delivery_score(...)                                  # :7881-7885
_pop_w, _del_w = _demand_blend_weights(service_model)  # dine_in = (0.75, 0.25)  :7886 / :2335
demand_score = _clamp(pop_score*_pop_w + delivery_score*_del_w)        # :7887
```

`demand_score` feeds the `demand_potential` component (8.764% weight) at
`raw_inputs["demand_potential"]` (`:3139`).

**Recommendation:** L1 should **augment, not replace** `population_reach` inside the
`pop_score` term — i.e. build a composite `demand_generator_index` (population +
weighted OSM generators + Overture floor-density, optionally tilted by advisory
radiance) and feed it through `_population_score` (or a sibling
`_demand_generator_score`) as the `pop_score` input at `:7880`. This keeps the
existing `_demand_blend_weights` (0.75/0.25 dine-in) and `demand_potential` weight
untouched while swapping a richer numerator behind the same leg. Reasons:

1. It reuses the proven `_bulk_enrich_population` bulk pattern (`:6328-6421`) — add
   parallel bulk-enrich blocks for OSM/buildings keyed on `_shortlist_coords`,
   mirroring the existing `_bulk_foot_traffic` block (`:8435-8504`).
2. `foot_traffic` stays as the cafe-only nudge (`:8846-8848`); no regression to the
   cafe path.
3. No factor-weight rebalance is needed, so the `sum==100` invariant
   (`:3126-3129`) and all gate/demote calibration are untouched.

`_demand_anchor_score` (`restaurant_scoring_factors.py:858-935`, weights at
`:772-784`) is the reference implementation to port — it already does the
Overture-class + OSM-amenity weighted proximity query with defensive
`ST_Transform(...,4326)` (`:875`, `:905`). Net-of-supply is preserved by keeping
competitor/POI density in `_competition_whitespace_score` (`:2340`, used at `:7889`)
as the denominator — L1 must feed the *numerator* only and not reward raw proximity
to busy clusters.

### 1.4 SQL probe

`scripts/diagnostics/l1_generator_coverage.sql` — samples ~300 live candidates from
`candidate_location` (Tier-1 primaries), and for the 3.5 km dine-in catchment reports
per-candidate population, per-generator OSM counts, Overture building count + floors
proxy, and district radiance match coverage, plus a roll-up of *how many candidates
have ≥1 of each generator* and the distribution (avg/p50/p90/max). Resilient to
missing external tables (each generator group is an independent statement; run with
`ON_ERROR_STOP` **off**).

---

## Part 2 — Layer 2: BestTime venue busyness

### 2.1 Venue universe — `restaurant_poi`

`app/models/tables.py:302-339`. Columns relevant to L2: `id` (text `source:external_id`),
`name`/`name_ar`, `category`, `subcategory`, `lat`/`lon` (Numeric; DB trigger fills a
`geometry(Point,4326)`), `rating`, `review_count`, `price_level`, `chain_name`,
`district`, `google_place_id`, `business_status` (OPERATIONAL / CLOSED_TEMPORARILY /
CLOSED_PERMANENTLY), `source`. Indexed on category/source/district/google_place_id/
business_status (`:333-339`).

**F&B filter to pick venues to forecast** — reuse the Advisor's own category filter:
`lower(rp.category) = ANY(:category_keys)` (`expansion_advisor.py:6571`), where
`category_keys` come from `_expand_category()` (built from the alias map
`_CATEGORY_TO_DELIVERY_BUCKETS`, `:154-223`) and `restaurant_categories.CATEGORIES`
(`app/services/restaurant_categories.py:10-34`). For L2 forecasting, select
`restaurant_poi WHERE business_status IS DISTINCT FROM 'CLOSED_PERMANENTLY' AND
google_place_id IS NOT NULL` (Google place id gives BestTime a clean match key).

### 2.2 Proposed cache table (design only — DO NOT create the migration)

Template: `expansion_delivery_rating_history`
(`alembic/versions/20260413_ea_delivery_rating_history.py:28-51`) — idempotent daily
snapshot, `captured_at TIMESTAMPTZ` + generated `captured_date DATE`, unique
`(source_record_id, captured_date)`, GiST on `geom`.

Proposed `restaurant_poi_busyness` (mirrors those conventions):

```
id                  BIGSERIAL PK
restaurant_poi_id   TEXT NOT NULL          -- FK-ish to restaurant_poi.id
besttime_venue_id   TEXT                    -- BestTime venue_id (match audit)
google_place_id     TEXT
day_hour_intensity  JSONB                   -- 7x24 relative busyness matrix (0-100)
peak_intensity      SMALLINT                -- derived: max over matrix (0-100)
peak_dow            SMALLINT                -- derived peak day-of-week
size_weight         DOUBLE PRECISION        -- ABSOLUTE-size weight (see note)
forecast_status     TEXT                    -- 'ok' | 'forecast_unavailable'
forecast_fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
forecast_date       DATE GENERATED ALWAYS AS ((forecast_fetched_at AT TIME ZONE 'UTC')::date) STORED
geom                geometry(Point,4326)
UNIQUE (restaurant_poi_id, forecast_date)
```

**Relative-value fix (the core L2 correctness issue):** BestTime values are 0–100%
of *each venue's own peak*, so a tiny busy café and a packed mall food-court both hit
100. They must be weighted by an **absolute-size proxy** before summing into a
catchment. Derive `size_weight` from what we already store on `restaurant_poi`:
`review_count` (best available demand-volume proxy; same signal the realized-demand
leg already trusts, `expansion_advisor.py:2290-2302`), optionally blended with
`price_level` and Overture footprint/floor area at the venue point. Catchment value =
`Σ (peak_or_hourly_intensity × size_weight)`, **not** a raw average of intensities.

Refresh cadence: forecast once, cache, refresh quarterly (matches the paid-tier
economics; the `forecast_date` unique key makes re-fetches idempotent).

### 2.3 Connector + wiring (design only — file plan, no code)

- **`app/connectors/besttime.py`** — mirror `app/connectors/google_places.py`
  (env-var key `GOOGLE_PLACES_API_KEY` → here `BESTTIME_API_KEY`, rate-limit +
  exponential-backoff retries, in-memory cache) and the registry/retry shape of
  `app/connectors/delivery_platforms.py:88-149`. Exposes e.g.
  `fetch_venue_forecast(name, address|lat/lng) -> dict` and/or
  `search_area(lat,lng,radius) -> Iterable[dict]`.
- **`app/ingest/expansion_advisor_busyness.py`** — mirror the harvester upsert shape
  of `app/ingest/harvest_open.py:1-80` and the POI upsert of
  `app/ingest/restaurant_pois.py`: select F&B `restaurant_poi` rows (filter from
  §2.1), call the connector, upsert into `restaurant_poi_busyness`, `db.commit()`,
  return a count. Quarterly schedule via a `.github/workflows/` cron like the
  existing ingest workflows.
- **Scoring read** — add a bulk-enrich block alongside `_bulk_delivery` /
  `_bulk_foot_traffic` (`expansion_advisor.py:7407-7408`, populated `:7410-7574` /
  `:8435-8504`) that sums `peak_intensity × size_weight` from `restaurant_poi_busyness`
  within the catchment per `_shortlist_coords`. Route the weighted busyness as the
  **numerator** into the net-of-supply path: it should enter the demand/whitespace
  computation around `:7918-7951` (where `provider_density_score` /
  `delivery_competition_count` already form the supply denominator) and/or blend into
  the demand leg at `:7880-7887` — **not** create a new "proximity to busy cluster"
  reward. Keep `_competition_whitespace_score` (`:2340`) as the denominator.

### 2.4 Probe script

`scripts/diagnostics/besttime_coverage_probe.py` — standalone (stdlib only), runs in
Codespace with a free `BESTTIME_API_KEY`. Hardcodes 10 Riyadh district centroids
spread N/centre/E/W/S, calls the BestTime venue search (radius) endpoint for
"restaurants", and prints per-district: venues returned, venues *with* a usable
forecast vs none, coverage %, and a running credit tally. Hard caps: per-district
forecast cap (default 8) and a global `CREDIT_CAP` (default 80, < free 100), with
429/auth handling and a `BESTTIME_DRY_RUN=1` mode that prints the plan for 0 credits.
**It was not executed here** (besttime.app is outside the agent allowlist and no key
is held) — verified only via `py_compile` + dry-run.

---

## Part 3 — Layer 3: SAMA POS + GASTAT city multiplier

### 3.1 Current SAMA scope — base rate only, no POS

`app/connectors/sama.py` — `fetch_rates()` reads `settings.SAMA_OPEN_JSON` and yields
the **SAMA overnight base rate** only (`rate_type="SAMA_base"`, tenor "overnight").
**No POS/point-of-sale fetch exists.** `safe_get_json(url)` —
`app/connectors/open_data.py:21-26` — robots-aware `httpx.get` (30 s timeout,
`raise_for_status`, returns parsed JSON.

Settings (`app/core/config.py`): `SAMA_OPEN_JSON` (`:44`, default `None`),
`REGA_CSV_URLS` (`:47-49`). **There is no GASTAT URL var in config** — the
real-estate index is loaded from a bundled CSV into the DB, not fetched live.
(`GASTAT_CCI_CSV_URL` referenced in the task is *not present* in the current tree —
flag as a discrepancy; only `SAMA_OPEN_JSON` / `REGA_CSV_URLS` exist.)

### 3.2 Existing GASTAT real-estate scalar — the pattern to reuse

- **Store:** `market_indicator` table (`app/models/tables.py:111-122`; cols
  `date,city,asset_type,indicator_type,value,unit,source_url,asof_date`). Ingested by
  `app/ingest/real_estate_indices.py` as `indicator_type="real_estate_price_index"`,
  `unit="index_2014_100"` (loaded from `/data/real_estate_indices.csv`).
- **Read:** `latest_re_price_index_scalar()` —
  `app/services/indicators.py:164-172` — returns `latest_index / 100.0` (i.e.
  **2014 = 1.0**), defaulting to `1.0` when absent.
- **Apply (estimator only):** `app/api/estimates.py:1305-1316` injects `re_scalar`
  into `excel_inputs`; `app/services/excel_method.py:615,1099` multiplies rent rates
  by it. **This is Estimator/Feasibility, not Expansion Advisor.**

A new F&B/expenditure scalar is a near-clone: new `indicator_type`
(e.g. `fnb_spend_index` / `pos_restaurants_index`), same table, a new
`latest_fnb_spend_scalar()` read, fed by a new SAMA-POS / GASTAT ingest job mirroring
`real_estate_indices.py`.

### 3.3 Multiplier insertion point — a genuine design gap

`final_score` is a pure per-candidate weighted sum of the 10 components
(`expansion_advisor.py:3144-3148`), invariant-checked to sum to 100
(`:3126-3129`), then sorted with **no post-hoc global scalar**
(sort at `:9632`). **There is no existing uniform city-level multiplier hook** in
Advisor scoring. So L3 must add one deliberately. Options (decision required):

1. **Confidence/market-context band (recommended, lowest-risk):** surface the scalar
   as a uniform market-trend annotation / confidence tag in the decision summary
   and memo, *without* multiplying `final_score`. Because it's identical for every
   candidate, multiplying the score changes no rankings — it only rescales an already
   0–100 axis — so an explanatory/confidence treatment is the honest, non-distorting
   use. This matches the design intent ("NOT a per-listing ranker").
2. **Uniform score multiplier:** apply `final_score *= clamp(fnb_scalar)` right after
   `:3148` (or post-sort). Mechanically simple but ranking-neutral and risks
   breaking the 0–100 invariant unless re-clamped — only meaningful if you later mix
   in any per-candidate spend variation, which the design explicitly forbids.

Either way it must enter as a **global constant**, mirroring how `re_scalar` enters
the estimator as a single settings/DB-driven value — not inside the per-candidate
scoring loop as a ranking factor.

`scripts/diagnostics/l3_market_indicator_check.sql` confirms the `market_indicator`
storage exists, lists current `indicator_type`/`city`/`asset_type` values (so new
rows match conventions, incl. how "Riyadh" is spelled), validates the real-estate
scalar read, and checks that no F&B/POS indicator exists yet (greenfield).

### 3.4 Open real-world questions (to resolve against live sources — not resolved here)

- Exact **SAMA POS** dataset endpoint + JSON schema (KAPSARC vs SAMA portal), and
  whether a city field resolves cleanly to "Riyadh" (SAMA POS is often KSA-national
  or by-region, not city-level).
- **GASTAT F&B / "Restaurants & Cafes"** line-item granularity (national vs Riyadh
  region) and refresh cadence; whether it's an index or absolute spend.
- Confirm there is no live GASTAT URL to wire (current repo uses a bundled CSV) — a
  new live source may need a new settings var + robots check via `safe_get_json`.

---

## Proposed PR sequence (small, single-purpose, ordered)

1. **PR-1 (L1 enrich, additive):** add bulk-enrich blocks for OSM generators +
   Overture floor-density keyed on `_shortlist_coords` (mirror `_bulk_foot_traffic`
   `:8435-8504`); compute a `demand_generator_index`; **do not yet wire into score**
   — emit it into `feature_snapshot_json` for validation. Ship behind a feature flag.
2. **PR-2 (L1 wiring):** feed the index through the `pop_score` term at `:7880`
   (sibling `_demand_generator_score`), keeping `_demand_blend_weights` and the
   `demand_potential` weight unchanged. Update tests + docs. *(Depends on PR-1.)*
3. **PR-3 (L3 ingest):** new `indicator_type` + `latest_fnb_spend_scalar()` read +
   ingest job mirroring `real_estate_indices.py`. Storage only; no scoring change.
   *(Independent of L1.)*
4. **PR-4 (L3 surface):** attach the city scalar as a uniform market-context /
   confidence annotation in the decision summary + memo (per §3.3 option 1).
   *(Depends on PR-3.)*
5. **PR-5 (L2 connector + cache):** `app/connectors/besttime.py` + migration for
   `restaurant_poi_busyness` + `app/ingest/expansion_advisor_busyness.py` + quarterly
   workflow. **Gated on a green coverage probe.** No scoring change yet.
6. **PR-6 (L2 scoring wiring):** bulk-enrich weighted busyness into the catchment and
   route it as the net-of-supply numerator at `:7918-7951` / demand leg `:7880-7887`.
   *(Depends on PR-5.)*
7. **PR-7 (validation):** Riyadh sanity checks — rankings still in Riyadh, scores
   internally consistent, top candidates not overly repetitive, no perf regression in
   the scoring loop.

**Cross-layer dependencies:** L1 and L3 are independent and can land in parallel. L2
PRs are strictly gated on the BestTime coverage probe (PR-5/6 should not start until
the probe verdict is positive). L1's net-of-supply discipline (numerator only,
denominator stays in `_competition_whitespace_score`) is a shared contract L2 must
also honor — do PR-2 first so the numerator/denominator split is established before
L2 adds its numerator.

---

## What I could NOT verify without DB / API access (the Codespace + probe run resolves exactly these)

1. **Are `planet_osm_point/polygon`, `overture_buildings` populated for Riyadh in the
   target env?** They are externally imported (not migration-created) and guarded by
   `_cached_table_available` / `_table_exists`
   (`alembic/versions/0016_spatial_indexes_for_scoring_perf.py:16-17`). → answered by
   `l1_generator_coverage.sql` §0 + §6.
2. **Does the L1 index actually vary across candidates (signal, not mostly-zero)?** →
   the coverage roll-up + per-candidate preview in `l1_generator_coverage.sql` §6.
3. **`overture_buildings` exact SRID.** Metrics SQL treats it as 32638
   (`overture_buildings_metrics.py:12,30-32`) while index migration 0016 builds a
   `ST_Transform(geom,4326)` index — the probe uses `ST_Transform(...,4326)`
   defensively so it's correct either way, but confirm the declared SRID.
4. **`district_radiance_monthly.district_key` ↔ `candidate_location.district_ar`
   match rate.** No canonical normalizer in SQL; §5 of the L1 SQL flags the naive
   match rate so we know how much radiance coverage is real vs lost to naming.
5. **BestTime Riyadh F&B coverage + per-venue credit cost** — only the probe answers
   this; not callable from here (allowlist + no key).
6. **`market_indicator` current contents** — which `indicator_type`/`city` values
   exist and how "Riyadh" is spelled — `l3_market_indicator_check.sql`.
7. **SAMA POS / GASTAT F&B live endpoints + granularity** — confirm against the live
   KAPSARC/SAMA/GASTAT sources (§3.4).
