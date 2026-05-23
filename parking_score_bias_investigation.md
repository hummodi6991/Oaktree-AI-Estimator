# `parking_score` Geographic Bias — Code Investigation

## 1. Locate and quote `_parking_score`

**File / lines:** `app/services/expansion_advisor.py:1780-1791`

```python
def _parking_score(*, area_m2: float, service_model: str, nearby_parking_count: int, access_score: float, parking_context_available: bool = True) -> float:
    area_signal = _clamp((area_m2 / 300.0) * 100.0)
    if not parking_context_available:
        return _clamp(area_signal * 0.50 + access_score * 0.20 + 30.0)
    parking_amenity_signal = _clamp((nearby_parking_count / 6.0) * 100.0)
    model_adjustment = {
        "delivery_first": 80.0,
        "qsr": 70.0,
        "cafe": 62.0,
        "dine_in": 55.0,
    }.get(service_model, 65.0)
    return _clamp(area_signal * 0.35 + parking_amenity_signal * 0.30 + model_adjustment * 0.20 + access_score * 0.15)
```

**Number of distinct branches:** **2** (matches the external analysis).

| Branch | Predicate | Formula | Inputs |
| --- | --- | --- | --- |
| Fallback | `not parking_context_available` | `clamp(area_signal·0.50 + access_score·0.20 + 30.0)` | `area_signal = clamp((area_m2/300)·100)`, `access_score` |
| OSM ("observed") | `parking_context_available` | `clamp(area_signal·0.35 + parking_amenity_signal·0.30 + model_adjustment·0.20 + access_score·0.15)` | `area_signal`, `parking_amenity_signal = clamp((nearby_parking_count/6)·100)`, `model_adjustment ∈ {80,70,62,55,65}` (service_model), `access_score` |

`_clamp` (`app/services/expansion_advisor.py:1000-1003`) hard-clips to `[0,100]` and maps NaN → 0.

## 2. Verify the fallback formula

**Confirmed**: fallback ≈ `area·0.5 + access·0.2 + 30`, with the following exact substitutions.

- **`area`** = `area_signal`, *not* raw `area_m2`. Computed as `_clamp((area_m2/300.0)*100.0)`. Saturates at `area_m2 = 300 m²`. Source column: `area_m2` (parcel `parcel_area_m2` or listing `unit_area_sqm`, normalised earlier in the loop).
- **`access`** = `access_score` (0–100). Returned by `_access_score` (`app/services/expansion_advisor.py:1748-1758`). For listings with `unit_street_width_m > 0` it comes from the calibrated street-width curve at `_access_score_from_street_width`; for parcels without road context it is a flat **50** (line 1754). So `access_score` is essentially a constant 50 in the same case where parking falls back, contributing `50·0.20 = 10`.
- **Constant offset = `30.0`** — present unconditionally on the fallback path.
- **Identical for every fallback case** — no further branching, no `service_model` term, no listing/parcel split.
- **Clamping**: outer `_clamp` clips to `[0,100]`. With `area_m2 ≥ 300` and `access_score = 100` the formula saturates at `100`. With `area_m2 = 0` and `access_score = 50` it floors at `40`. So in practice the fallback band is roughly `40 – 90` before any downstream blending.

> **Analysis claim:** fallback ≈ `area * 0.5 + access * 0.2 + 30`. **Code:** confirmed, with `area` being the area-derived signal (saturated at 300 m²), not raw `area_m2`.

## 3. Trigger conditions for the fallback

The fallback fires **iff** the keyword argument `parking_context_available` is `False`. Quote (`app/services/expansion_advisor.py:1782-1783`):

```python
    if not parking_context_available:
        return _clamp(area_signal * 0.50 + access_score * 0.20 + 30.0)
```

It is **not** driven by `nearby_parking_amenity_count == 0`. `nearby_parking_count` is ignored on this branch entirely.

There are two call sites that supply that flag:

- **Coarse / preliminary pass** (`app/services/expansion_advisor.py:8067-8073`) — hard-coded `parking_context_available=False` for every candidate. Used only for shortlist ranking (`preliminary_final_score`); the result is discarded for the displayed candidate.

```python
        parking_score = _parking_score(
            area_m2=area_m2,
            service_model=service_model,
            nearby_parking_count=0,
            access_score=access_score,
            parking_context_available=False,
        )
```

- **Final scoring pass** (`app/services/expansion_advisor.py:9052, 9114-9120`) — the persisted candidate.

```python
        parking_context_available = bool((feature_snapshot_json.get("context_sources") or {}).get("parking_context_available"))
        ...
        parking_score = _parking_score(
            area_m2=area_m2,
            service_model=service_model,
            nearby_parking_count=_nonnegative_int(feature_snapshot_json.get("nearby_parking_amenity_count")),
            access_score=access_score,
            parking_context_available=parking_context_available,
        )
```

`feature_snapshot_json["context_sources"]["parking_context_available"]` is set inside `_candidate_feature_snapshot` (`app/services/expansion_advisor.py:1898-2189`). Its possible writes:

- Default `False` at init (`app/services/expansion_advisor.py:1922-1927`).
- `True` when the per-candidate bulk path resolves a value (`app/services/expansion_advisor.py:2118-2123`):

```python
    if bulk_parking is not None:
        base["nearby_parking_amenity_count"] = bulk_parking
        base["context_sources"]["parking_context_available"] = True
```

- `True` when the per-candidate `expansion_parking_asset` query returns a row (`app/services/expansion_advisor.py:2146-2149`).
- The legacy OSM-polygon query writes the flag via `_context_checked(parking_row.get("nearby_parking_amenity_count"))` (`app/services/expansion_advisor.py:2185-2187`). `_context_checked` (`app/services/expansion_advisor.py:1142-1148`) returns `value is not None`. The SQL wraps the count in `COALESCE(... , 0)`, so a successful query always yields `True`.
- Stays `False` on `except Exception`, when the candidate has no `parcel_id` (early-return at 1962-1965), or when `parking_table_available` is `False` (the OSM block at line 2153 is skipped entirely).

> **Analysis claim:** "districts with sparse OSM `amenity=parking` coverage fall through to a fallback branch." **Code:** **refuted as stated**. The trigger is `parking_context_available=False`, which in production fires on DB errors, missing-parcel paths, or missing tables — not on a "0 OSM amenities found" result. A count of `0` returned by the OSM query still sets `parking_context_available=True`, and the resulting score comes from the **OSM branch with `parking_amenity_signal=0`**, not the fallback.

## 4. Data sources feeding the OSM (non-fallback) branch

`nearby_parking_count` (the kwarg) is fed from `feature_snapshot_json["nearby_parking_amenity_count"]` (line 9117). That field is populated by `_candidate_feature_snapshot`, with the precedence:

1. **`bulk_parking`** kwarg (when the parcel is in the shortlist) — `app/services/expansion_advisor.py:2118-2123`. Populated by the bulk parking query (`app/services/expansion_advisor.py:8345-8402`):

```python
        if ea_parking_populated or parking_table_available:
            ...
            if ea_parking_populated:
                _parking_query = f"""
                    WITH pids(parcel_id, lon, lat) AS (VALUES {_park_values_sql})
                    SELECT pids.parcel_id,
                        COALESCE((
                            SELECT COUNT(*) FROM {_EA_PARKING_TABLE} epa
                            WHERE epa.geom IS NOT NULL
                              AND ST_DWithin(epa.geom::geography,
                                  ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography, 350)
                        ), 0) AS nearby_parking_amenity_count
                    FROM pids
                """
            else:
                _parking_query = f"""
                    WITH pids(parcel_id, lon, lat) AS (VALUES {_park_values_sql})
                    SELECT pids.parcel_id,
                        COALESCE((
                            SELECT COUNT(*) FROM planet_osm_polygon op
                            WHERE op.way IS NOT NULL
                              AND (lower(COALESCE(op.amenity, '')) = 'parking'
                                   OR lower(COALESCE(op.parking, '')) IN ('surface','multi-storey','underground'))
                              AND ST_DWithin(op.way::geography,
                                  ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography, 350)
                        ), 0) AS nearby_parking_amenity_count
                    FROM pids
                """
```

2. **Per-candidate `expansion_parking_asset` query** (`app/services/expansion_advisor.py:2128-2149`).
3. **Per-candidate `planet_osm_polygon` query** (`app/services/expansion_advisor.py:2156-2189`):

```python
                        SELECT COALESCE((
                            SELECT COUNT(*)
                            FROM planet_osm_polygon op
                            WHERE op.way IS NOT NULL
                              AND (
                                lower(COALESCE(op.amenity, '')) = 'parking'
                                OR lower(COALESCE(op.parking, '')) IN ('surface','multi-storey','underground')
                              )
                              AND ST_DWithin(op.way::geography, ST_Centroid(p.geom)::geography, 350)
                        ), 0) AS nearby_parking_amenity_count
```

**Tables consulted in `_parking_score`'s data path:**
- `expansion_parking_asset` (preferred). `_EA_PARKING_TABLE = settings.EXPANSION_PARKING_TABLE` → defaults to `expansion_parking_asset` (`app/core/config.py:71`).
- `planet_osm_polygon` (legacy fallback).
- `planet_osm_point` is **not** queried by `_parking_score`'s path — only the polygon table.

**Search radius:** `ST_DWithin(..., 350)` (metres). **Confirmed** matching the external analysis.

**Tagging filter (legacy OSM path):**
```
lower(COALESCE(op.amenity, '')) = 'parking'
OR lower(COALESCE(op.parking, '')) IN ('surface','multi-storey','underground')
```
The OSM `parking` tag values `street_side`, `lane`, `parking_entrance`, `parking_space`, and `access=customers` are **not** queried at read time. (They ARE captured by the ingest job — see §6 — so they are reachable via `expansion_parking_asset` but not via the live OSM fallback.)

**Persistence:** `feature_snapshot_json["nearby_parking_amenity_count"]` is written into the candidates table column `feature_snapshot_json` (the column appears in the INSERT/SELECT statements at lines 9783, 10084, 10412, 10708). The count itself is not stored as a top-level candidate column; only the derived `parking_score` is.

## 5. Is `expansion_parking_asset` actually in the `_parking_score` path?

**Yes — confirmed.** `expansion_parking_asset` is queried as the *preferred* source upstream of `_parking_score`. Tracing the call tree:

- `_parking_score` ← `nearby_parking_count` from `feature_snapshot_json["nearby_parking_amenity_count"]`.
- That key is written by `_candidate_feature_snapshot`, and the very first branch (`app/services/expansion_advisor.py:2116-2123`) reads `bulk_parking`, whose value comes from `_bulk_parking` (`app/services/expansion_advisor.py:8395`), populated by the EA-preferred bulk query (`app/services/expansion_advisor.py:8361-8371`):

```python
                if ea_parking_populated:
                    _parking_query = f"""
                        WITH pids(parcel_id, lon, lat) AS (VALUES {_park_values_sql})
                        SELECT pids.parcel_id,
                            COALESCE((
                                SELECT COUNT(*) FROM {_EA_PARKING_TABLE} epa
                                WHERE epa.geom IS NOT NULL
                                  AND ST_DWithin(epa.geom::geography,
                                      ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography, 350)
                            ), 0) AS nearby_parking_amenity_count
                        FROM pids
                    """
```

`ea_parking_populated = _cached_ea_table_has_rows(db, _EA_PARKING_TABLE)` (`app/services/expansion_advisor.py:7359`). When true, the bulk query targets `expansion_parking_asset` only; the OSM polygon fallback is **not consulted in parallel** — it is a single-source decision per candidate.

**Blending vs override:** `expansion_parking_asset` is an *overriding* signal, not blended. Same column name (`nearby_parking_amenity_count`), same downstream consumer. From `_parking_score`'s perspective the source is invisible — it just receives a single integer. **Auxiliary columns on `expansion_parking_asset`** (`walk_access_score`, `dropoff_score`, `capacity`, `covered`, `public_access`, `amenity_type`) **are not read by `_parking_score` or by any helper it calls** (grepped — no read sites outside the ingest job).

## 6. `expansion_parking_asset` provenance

**Ingest file:** `app/ingest/expansion_advisor_parking.py` (single loader, two passes).

**Upstream source(s):** OSM only. There is no Google Places, Aqar, or manual contribution.

- Polygons: `planet_osm_polygon` — `_ingest_from_polygons` (`app/ingest/expansion_advisor_parking.py:105-187`):

```python
if not table_exists(db, "planet_osm_polygon"):
    logger.warning("planet_osm_polygon not found, skipping polygon source")
    return 0
...
INSERT INTO expansion_parking_asset (...)
SELECT 'riyadh', 'osm_polygon', ...
FROM planet_osm_polygon op
WHERE op.way IS NOT NULL AND {where_filter} AND {bbox_filter}
```

  The `WHERE` predicate is built by `_build_where_filter` (`app/ingest/expansion_advisor_parking.py:79-98`): `amenity = 'parking'` OR `parking ∈ ('surface','multi-storey','underground','street_side','lane')`.

- Points: `planet_osm_point` — `_ingest_from_points` (`app/ingest/expansion_advisor_parking.py:194-272`). Predicate widened: `amenity IN ('parking','parking_entrance','parking_space')`.

**Conditional logic:** None across sources — both pass through unconditionally; rows are tagged `source='osm_polygon'` / `source='osm_point'` and bbox-filtered to Riyadh.

**Geographic coverage caveats (code-visible):**
- Hard Riyadh bbox via `riyadh_bbox_filter_sql` (`app/ingest/expansion_advisor_parking.py:122, 211`).
- `walk_access_score` and `dropoff_score` are heuristic constants derived from the parking subtype (`app/ingest/expansion_advisor_parking.py:163-176, 260-261`), not survey data. The docstring is explicit (`app/ingest/expansion_advisor_parking.py:6-9`):

```text
Heuristics:
- walk_access_score: derived from proximity to roads/pedestrian paths (0-100)
- dropoff_score: derived from parking type and road adjacency (0-100)
These are reasonable approximations, not survey measurements.
```

> **Analysis claim (implicit):** "replace OSM with a denser parking layer (`expansion_parking_asset`)." **Code:** **refuted.** `expansion_parking_asset` IS OSM (`planet_osm_polygon` + `planet_osm_point`), normalised; it cannot fix OSM tagging-density bias because it inherits the same input data. Verifying coverage against the actual `expansion_parking_asset` row counts per district would need a Codespace query (`SELECT district, COUNT(*) FROM expansion_parking_asset GROUP BY ...`) — cannot determine from code alone.

## 7. Downstream impact of `parking_score`

**Into `brand_fit_score`** (`app/services/expansion_advisor.py:1496-1564`), `parking_signal` enters twice:

- Always-on term (`app/services/expansion_advisor.py:1559`):

```python
        + parking_signal * (0.1 + parking_weight * 0.06)
```

  where `parking_weight = _sensitivity_weight(brand_profile.get("parking_sensitivity"))` returns `{low:0.3, medium:0.6, high:1.0}` (`app/services/expansion_advisor.py:1482-1483`). Coefficient range: **0.118 – 0.16** (low – high sensitivity).

- "neighborhood"-goal extra term (`app/services/expansion_advisor.py:1531-1533`):

```python
    elif goal == "neighborhood":
        spacing = 100.0 - abs(cannibalization_score - 45.0)
        goal_component = _clamp(fit_score * 0.45 + spacing * 0.25 + parking_signal * 0.3)
```

  `goal_component` then enters `brand_fit_score` at weight 0.20 (`app/services/expansion_advisor.py:1556`), so this contributes `parking_signal · 0.30 · 0.20 = 0.06` of additional coefficient. (For `flagship` / `delivery_led` / balanced goals, `parking_signal` is absent from `goal_component`.)

**Total coefficient on `parking_signal` inside `brand_fit_score`:**
- Most brands: `0.118` (low) to `0.16` (high parking_sensitivity).
- Neighborhood-goal brands: up to `0.16 + 0.06 = 0.22`.

**Final-score weight of `brand_fit_score`** (`app/services/expansion_advisor.py:3103-3114`): `9.6404`. So `parking_signal`'s maximum direct contribution to `final_score` is roughly `0.22 · 9.6404% ≈ 2.1 final-score points per 100 parking-score points`. For a 20-point parking_score gap, expect ~0.4 final-score points (most brands) up to ~0.8 (neighborhood brands with `parking_sensitivity=high`).

> **Analysis claim:** parking-score variation "propagates into `brand_fit_score` and `final_score` for any brand with non-trivial `parking_sensitivity`." **Code:** confirmed in direction. The magnitude implied by "15–25 parking_score points" translates to roughly 1–3 brand_fit points and **under 1 final-score point**, not the dramatic shift the framing suggests. The 15–25-point figure refers to the **parking_score itself**, not to `final_score`.

**`parking_pass` — advisory or hard-fail?** Quoted from `app/services/expansion_advisor.py:2856-2870`:

```python
    parking_pass: bool | None
    if parking_score is None:
        parking_pass = None
    elif parking_score >= thresholds["parking_min"]:
        parking_pass = True
    else:
        parking_pass = False
```

`thresholds["parking_min"] = 45.0` (`app/services/expansion_advisor.py:2808`). The result is exposed via `gate_states["parking_pass"]` only when `parking_context_available` is true (`app/services/expansion_advisor.py:2917`).

**Hard-fail set** (`app/services/expansion_advisor.py:91-102`):

```python
_HARD_FAIL_GATES_BASE: frozenset[str] = frozenset({
    "zoning_fit_pass",
    "area_fit_pass",
})
_OPTIONAL_HARD_GATES: set[str] = set()
if int(getattr(settings, "EXPANSION_VIABILITY_POPULATION_HARD_FLOOR", 0) or 0) > 0:
    _OPTIONAL_HARD_GATES.add("population_floor_pass")
if int(getattr(settings, "EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR", 0) or 0) > 0:
    _OPTIONAL_HARD_GATES.add("commercial_floor_pass")
if float(getattr(settings, "EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M", 0) or 0) > 0:
    _OPTIONAL_HARD_GATES.add("construction_proximity_pass")
HARD_FAIL_GATES: frozenset[str] = _HARD_FAIL_GATES_BASE | frozenset(_OPTIONAL_HARD_GATES)
```

`parking_pass` is **not in `HARD_FAIL_GATES`**. It is **advisory**: a failure raises `advisory_failures` but does not flip `overall_pass` to `False`.

**Other downstream effects of the fallback case:**
- `feature_snapshot.context_sources.parking_evidence_band` degrades. `_candidate_feature_snapshot` explicitly passes `None` into `_parking_evidence_band` when `parking_context_available` is `False` (`app/services/expansion_advisor.py:2196-2198`), so the band becomes `"unknown"` (`_parking_evidence_band`, `app/services/expansion_advisor.py:1799-1800`). For listings the override `_parking_evidence_band_for_listing` also returns `"unknown"` in that case (`app/services/expansion_advisor.py:1868-1869`).
- `parking_score_mode` flips to `"estimated"` in `site_fit_context` (`app/services/expansion_advisor.py:9521`) and in `_derive_site_fit_context` (`app/services/expansion_advisor.py:1167, 1177`).
- `_confidence_grade` (`app/services/expansion_advisor.py:3441-3442`) counts `not parking_context_available` toward `critical_missing` on the parcel path; for listings (line 3416-3431), missing `parking_evidence_band` (`"unknown"`/`"none_found"`/`"none"`) caps the grade at B when ≥2 bands are missing.
- `data_completeness_score` (`app/services/expansion_advisor.py:2221`) loses 1/6 of its weight when `parking_context_available` is false.
- The score `parking_score` itself is exposed both on the candidate (line 9472) and inside `feature_snapshot_json["parking_score"]` (line 9374), so the **value** of the fallback output is observable downstream regardless of context flags.

## 8. Observability — can we tell which branch was taken?

**Yes — indirectly, via multiple fields.**

The branch is not labeled explicitly in `_parking_score` itself (the function does not stash a `branch=` tag). But the predicate `parking_context_available` is itself surfaced in the candidate output:

- `candidate.site_fit_context.parking_score_mode` (`app/services/expansion_advisor.py:9521`): `"observed"` when the OSM branch ran, `"estimated"` when the fallback ran.

```python
                "site_fit_context": {
                    "road_context_available": road_context_available,
                    "parking_context_available": parking_context_available,
                    ...
                    "parking_score_mode": "observed" if parking_context_available else "estimated",
                },
```

- `score_breakdown_json.inputs.parking_context_available` (`app/services/expansion_advisor.py:9193`).
- `score_breakdown_json.inputs.parking_evidence_band` (`app/services/expansion_advisor.py:9195`).
- `feature_snapshot_json.context_sources.parking_context_available` and `feature_snapshot_json.context_sources.parking_evidence_band` (set at `app/services/expansion_advisor.py:2185-2197`; the band is `"unknown"` whenever the fallback fires — see §7 bullet 1).
- `feature_snapshot_json.context_sources.parking_source` (`app/services/expansion_advisor.py:1942, 1958, 2122`) — `"estimated"` / `"expansion_parking_asset"`. **Caveat:** the OSM polygon fallback path (`app/services/expansion_advisor.py:2153-2189`) does **not** overwrite this string, so a candidate whose count came from `planet_osm_polygon` directly may still show `parking_source = "estimated"`. That is a labeling defect — the displayed source can lie about whether OSM-poly was queried.
- `feature_snapshot_json.missing_context` lists `"parking_context_unavailable"` when the fallback fires (`app/services/expansion_advisor.py:2207-2208`).
- `feature_snapshot_json.nearby_parking_amenity_count` is preserved at the candidate level (`app/services/expansion_advisor.py:11098`), so a consumer can also inspect whether the OSM-branch ran with count 0 vs count 6 — that distinction *is* visible.

**What is NOT recorded:** a single explicit "branch taken" tag inside `_parking_score`. To distinguish the two states a consumer must read at least two fields (`parking_context_available` plus `nearby_parking_amenity_count` to tell "OSM branch with 0 amenities" from "OSM branch with 6 amenities"). The function does not log either.

> The `feature_snapshot.context_sources.parking_evidence_band` evidence band **does** degrade to `"unknown"` when the fallback fires (`app/services/expansion_advisor.py:2196-2198` and the `_parking_evidence_band` `None → "unknown"` mapping). That matches the desired behavior the prompt asked us to verify; the analysis hypothesis "the band doesn't degrade" is **refuted** as stated.

> **Real gap in observability:** when the OSM branch runs with `nearby_parking_amenity_count = 0` (the case actually responsible for the 39–62 cluster — see §3), `parking_context_available` stays `True`, `parking_score_mode` stays `"observed"`, and `parking_evidence_band` becomes `"none_found"`. There is no signal that the OSM tagging is locally absent and the score is therefore noisy. That's the missing axis for diagnosing this bug.

## 9. Existing tests

**Direct unit tests for `_parking_score`:** one only — `tests/test_expansion_advisor_service.py:519`:

```python
    assert 0 <= expansion_service._parking_score(area_m2=180, service_model="qsr", nearby_parking_count=3, access_score=65) <= 100
```

This only asserts the output is bounded; it does not pin a numeric expectation and does not exercise the fallback (it relies on the default `parking_context_available=True`).

**Fallback branch coverage:** no direct test calls `_parking_score(..., parking_context_available=False)`. The fallback is exercised **indirectly** through:

- `tests/test_expansion_advisor.py:203-213` (`test_parking_evidence_band_for_listing_unavailable_with_fallback_score`) — asserts the listing band override returns `"unknown"` when `parking_context_available=False` and a fallback-shaped score of `44.88` is passed in. The number 44.88 is hard-coded and matches no formula assertion.
- `tests/test_expansion_advisor.py:383-399` (`test_snapshot_none_parking_count_means_context_unavailable`) — asserts the snapshot pipeline yields `parking_context_available=False` when the OSM query returns `None`.

**Fixture districts used:** the gate / band / confidence-grade tests use `"Al Olaya"` / `"Olaya"` / `"Malqa"` as district labels (e.g. `tests/test_expansion_advisor.py:330-350`, `tests/test_expansion_advisor_service.py:451-471`); none pin parking_score numerically per district. Fixture-refresh cost for a fallback-formula change is therefore low — tests assert only bounds and band transitions, not exact `parking_score` values.

**Integration / golden-fixture tests pinning `parking_score` numerically:** none located that pin to a specific number from `_parking_score`. The frontend tests stub `parking_score: 60` etc. (`frontend/src/features/expansion-advisor/ExpansionMemoPanel.test.tsx:505,828,848,883`, `frontend/src/features/expansion-advisor/ExpansionAdvisorPage.test.tsx:2464,4051,4079,4099`) — these are mocks of the API response, not assertions about the scoring formula.

## 10. Other consumers of the same OSM-only signal

**Consumers of `nearby_parking_amenity_count` outside `_parking_score`:**

- `app/services/expansion_advisor.py:1794-1808` (`_parking_evidence_band`) — derives the `"unknown" / "none_found" / "limited" / "moderate" / "strong"` band from the count. Same OSM source.
- `app/services/expansion_advisor.py:1845-1874` (`_parking_evidence_band_for_listing`) — listing-aware override; same count.
- `app/services/expansion_advisor.py:2196-2198` — writes the band into `context_sources.parking_evidence_band` (callsite of `_parking_evidence_band`).
- `app/services/expansion_advisor.py:9374` — copies `parking_score` (not the count, but downstream of it) into `feature_snapshot_json["parking_score"]`.
- `app/services/expansion_advisor.py:11098` — projects `nearby_parking_amenity_count` into the report panel's `feature_snapshot_json`.

**Other functions doing `amenity=parking` `ST_DWithin` against `planet_osm_*` directly:**

- `app/services/expansion_advisor.py:2156-2187` — per-candidate OSM-polygon parking count (described in §4).
- `app/services/expansion_advisor.py:8345-8402` — bulk shortlist OSM-polygon parking count (`planet_osm_polygon` only; `planet_osm_point` is not used here).
- `app/ingest/expansion_advisor_parking.py:105-272` — ingest of `planet_osm_polygon` and `planet_osm_point` rows into `expansion_parking_asset`.

**Note on `restaurant_scoring_factors.py`:** `app/services/restaurant_scoring_factors.py:451, 540-570, 1672-1692` compute a `street_parking_score` for the Estimator side. It does **not** read `nearby_parking_amenity_count` and does **not** query `amenity=parking` directly — its inputs are road-density counts. It is a separate signal name (`street_parking_score`, surfaced through `meta["street_parking_score"]`) and is not part of the Expansion-Advisor parking pipeline.

## Summary of verified vs. unverified claims

| External-analysis claim | Verdict |
| --- | --- |
| `_parking_score` has two branches (OSM + fallback). | **confirmed** (`app/services/expansion_advisor.py:1780-1791`). |
| Fallback formula ≈ `area·0.5 + access·0.2 + 30`. | **confirmed**, with `area` = clamped `area_m2/300·100` signal, not raw `area_m2`. |
| Fallback fires when OSM `amenity=parking` coverage is sparse for a district. | **refuted** — fallback fires when `parking_context_available=False` (DB error / missing parcel / missing tables). A sparse-OSM result returns `count=0` via the OSM branch, not the fallback. |
| Districts with rich OSM tagging score 15–25 parking-points higher than sparse-OSM districts. | **cannot verify from code alone** — depends on the per-district `expansion_parking_asset` / `planet_osm_polygon` row counts within 350 m of candidates. Code path is consistent with such a bias (linear `count/6·100` term, no normalisation, no clamping below 100). Codespace settles it via `SELECT district, AVG(count) FROM (...)` or per-district sampling of `feature_snapshot_json.nearby_parking_amenity_count`. |
| OSM count for `_parking_score` uses `ST_DWithin(..., 350)` against OSM. | **confirmed** at all three call sites (per-candidate, bulk, ingest). |
| OSM read filter is `amenity=parking` only. | **partially refuted**. The read-time queries also include `parking ∈ ('surface','multi-storey','underground')`. They do NOT include `street_side`, `lane`, `parking_entrance`, `parking_space`, or `access=customers`. (The ingest into `expansion_parking_asset` DOES include `street_side`, `lane`, `parking_entrance`, `parking_space`.) |
| `expansion_parking_asset` is wired into `_parking_score` today. | **confirmed** — preferred source, overriding (not blended). Auxiliary columns (`walk_access_score`, `dropoff_score`, `capacity`) are **not** consumed. |
| `expansion_parking_asset` is a denser, different layer that could fix the bias. | **refuted** — `expansion_parking_asset` is sourced exclusively from `planet_osm_polygon` + `planet_osm_point` (`app/ingest/expansion_advisor_parking.py`). It inherits the same OSM tagging-density distribution. |
| `parking_pass` is a hard-fail gate. | **refuted** — it is advisory; only `zoning_fit_pass` and `area_fit_pass` (plus three env-gated optionals) are in `HARD_FAIL_GATES`. |
| `feature_snapshot.context_sources` does not degrade the parking evidence band when the fallback fires. | **refuted** — the band is forced to `"unknown"` via `app/services/expansion_advisor.py:2196-2198` (and the listing override `_parking_evidence_band_for_listing` returns `"unknown"` in the same case). |
| There is no observability for which branch ran. | **partially refuted** — `parking_context_available` / `parking_score_mode` / `parking_evidence_band` / `missing_context` together indicate which branch ran. A single explicit branch tag is missing, and `parking_source` is mislabeled `"estimated"` on the legacy-OSM read path. The OSM-branch-with-`count=0` case (the actually-suspect cohort) has no degraded indicator at all — that's the real observability gap. |
