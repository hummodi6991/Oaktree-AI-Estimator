# `expansion_parking_asset` Sparsity — Code Investigation

## 1. Locate and quote the ingest entry point

**File:** `app/ingest/expansion_advisor_parking.py`.

**Top-level orchestration** (`app/ingest/expansion_advisor_parking.py:279-309`):

```python
def main() -> None:
    parser = argparse.ArgumentParser(description="Expansion Advisor — Parking Context ingest")
    parser.add_argument("--city", default="riyadh", help="City filter (default: riyadh)")
    parser.add_argument("--replace", type=lambda v: v.lower() in ("true", "1", "yes"), default=True,
                        help="Replace existing rows (default: true)")
    parser.add_argument("--write-stats", type=str, default=None, help="Write JSON stats to path")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    validate_db_env()

    db = get_session()
    try:
        polygon_count = _ingest_from_polygons(db, replace=args.replace)
        point_count = _ingest_from_points(db, replace=args.replace)
        total = polygon_count + point_count
        logger.info("Total parking assets inserted: %d", total)
        counts = log_table_counts(db, ["expansion_parking_asset"])
        stats = {
            "polygon_count": polygon_count,
            "point_count": point_count,
            "total_inserted": total,
            "row_counts": counts,
        }
        if args.write_stats:
            write_stats(args.write_stats, stats)
    finally:
        db.close()
```

`main()` is the *only* decision-maker for inserts; it always runs both passes and never short-circuits early. The actual insert deciders are `_ingest_from_polygons` (`app/ingest/expansion_advisor_parking.py:105-187`) and `_ingest_from_points` (`app/ingest/expansion_advisor_parking.py:194-272`).

**Logging that compares candidate-rows-considered vs rows-inserted: none.** The only insertion-related log is post-execute (`app/ingest/expansion_advisor_parking.py:186, 271`):

```python
    count = result.rowcount
    logger.info("Inserted %d parking assets from planet_osm_polygon", count)
```

The column-introspection log at `app/ingest/expansion_advisor_parking.py:112-114` tells you which OSM tag columns were available, but no `COUNT(*)`-on-source pre-flight is taken. There is **no observability for whether the WHERE filter killed 99 % of candidates or whether the source table itself was almost empty**. That is the most consequential gap for diagnosing the 1,227-row outcome.

## 2. Full filter expressions (polygons)

`_build_where_filter` plus its helpers, `app/ingest/expansion_advisor_parking.py:43-98`:

```python
def _parking_expr(alias: str, cols: set[str]) -> str:
    """Return a SQL expression that resolves the parking tag value.

    Priority:
      1. Direct ``parking`` column (flattened osm2pgsql schema).
      2. hstore ``tags`` column: ``tags->'parking'``.
      3. hstore ``other_tags`` column: ``other_tags->'parking'``.
      4. NULL literal when none of the above exist.
    """
    if "parking" in cols:
        return f"{alias}.parking"
    if "tags" in cols:
        return f"{alias}.tags->'parking'"
    if "other_tags" in cols:
        return f"{alias}.other_tags->'parking'"
    return "NULL"
```

```python
def _build_where_filter(alias: str, cols: set[str]) -> str:
    clauses: list[str] = []
    parking = _parking_expr(alias, cols)
    if "amenity" in cols:
        clauses.append(f"lower(COALESCE({alias}.amenity, '')) = 'parking'")
    if parking != "NULL":
        clauses.append(
            f"lower(COALESCE({parking}, '')) IN "
            "('surface','multi-storey','underground','street_side','lane')"
        )
    if not clauses:
        # No useful column at all – select nothing safely.
        return "FALSE"
    return "(" + " OR ".join(clauses) + ")"
```

Polygon insert WHERE (`app/ingest/expansion_advisor_parking.py:177-181`):

```sql
FROM planet_osm_polygon op
WHERE op.way IS NOT NULL
  AND {where_filter}
  AND {bbox_filter}
```

**Atomic accepted (tag, value) clauses for polygons:**

| Clause | Source | Accepted values |
| --- | --- | --- |
| `lower(amenity) = 'parking'` | direct column (if present) | `amenity=parking` |
| `lower(parking) IN ('surface','multi-storey','underground','street_side','lane')` | direct column OR `tags->'parking'` OR `other_tags->'parking'` (whichever exists) | five literal parking subtype values |

Both are OR'd. There is no clause for `building=parking`, `landuse=garages`, `amenity IN ('parking_space','parking_entrance')` (polygons only — points have those), `service=parking_aisle`, or `access=customers`. `amenity=parking` rows whose `parking` subtype is one of the five accepted literals are double-counted by the OR (harmless, no `DISTINCT`).

**Read-time vs ingest-time comparison** — read-time at `app/services/expansion_advisor.py:2156-2189`:

```sql
            FROM planet_osm_polygon op
            WHERE op.way IS NOT NULL
              AND (
                lower(COALESCE(op.amenity, '')) = 'parking'
                OR lower(COALESCE(op.parking, '')) IN ('surface','multi-storey','underground')
              )
              AND ST_DWithin(op.way::geography, ST_Centroid(p.geom)::geography, 350)
```

Same shape at `app/services/expansion_advisor.py:8378-8385`. Differences:

| Tag value | Ingest | Read-time live query |
| --- | --- | --- |
| `amenity=parking` | yes | yes |
| `parking=surface` | yes | yes |
| `parking=multi-storey` | yes | yes |
| `parking=underground` | yes | yes |
| `parking=street_side` | yes (ingest only) | no |
| `parking=lane` | yes (ingest only) | no |
| `tags->'parking'` hstore | yes (ingest only) | no (`op.parking` direct only) |
| `other_tags->'parking'` hstore | yes (ingest only) | no |

So `expansion_parking_asset` is intentionally a *superset* of the read-time live OSM query — that matches the design's stated purpose. **The ingest filter is wider, not narrower, than the read-time fallback.** Therefore the filter cannot be the reason EPA underperforms a direct read-time OSM count; the gap has to be either (a) source-table sparsity, (b) bbox, or (c) the polygon→centroid reduction.

## 3. Full filter expressions (points)

`_ingest_from_points` (`app/ingest/expansion_advisor_parking.py:194-272`):

```python
    pk = _parking_expr("pt", cols)
    name_expr = _col_expr("pt", "name", cols)
    capacity_expr = _hstore_or_col("pt", "capacity", cols)
    covered_expr = _hstore_or_col("pt", "covered", cols)
    access_expr = _hstore_or_col("pt", "access", cols)
    amenity_expr = _col_expr("pt", "amenity", cols)

    # WHERE: base filter + point-specific amenity values
    where_clauses: list[str] = []
    if "amenity" in cols:
        where_clauses.append(
            f"lower(COALESCE(pt.amenity, '')) IN ('parking','parking_entrance','parking_space')"
        )
    if pk != "NULL":
        where_clauses.append(
            f"lower(COALESCE({pk}, '')) IN "
            "('surface','multi-storey','underground','street_side','lane')"
        )
    where_filter = "(" + " OR ".join(where_clauses) + ")" if where_clauses else "FALSE"
```

```sql
FROM planet_osm_point pt
WHERE pt.way IS NOT NULL
  AND {where_filter}
  AND {bbox_filter}
```

**Accepted `amenity` values:** `parking`, `parking_entrance`, `parking_space`. Plus the same five `parking=*` subtypes via `pk`. No `access=customers`, `barrier=*`, or `building=parking` checks.

**Positional filter:** `pt.way IS NOT NULL` (`app/ingest/expansion_advisor_parking.py:263`). Rows with NULL geometry are silently dropped — expected and harmless for OSM nodes (they should never have NULL `way`).

**The structural problem with the point pass is not the filter** — it is that the OSM source itself may contain almost no parking-related nodes for Riyadh. See §4 and §7 for the upstream cause.

## 4. Bbox / geometry handling

Bbox definitions (`app/ingest/expansion_advisor_common.py:25-30`):

```python
RIYADH_BBOX = {
    "min_lon": 46.3,
    "max_lon": 47.1,
    "min_lat": 24.4,
    "max_lat": 25.0,
}
```

`riyadh_bbox_filter_sql` (`app/ingest/expansion_advisor_common.py:113-129`):

```python
def riyadh_bbox_filter_sql(
    geom_col: str, alias: str = "", source_srid: int = 4326
) -> str:
    prefix = f"{alias}." if alias else ""
    col_expr = f"{prefix}{geom_col}"
    if source_srid != 4326:
        col_expr = f"ST_Transform({col_expr}, 4326)"
    return (
        f"ST_Intersects({col_expr}, "
        f"ST_MakeEnvelope({RIYADH_BBOX['min_lon']}, {RIYADH_BBOX['min_lat']}, "
        f"{RIYADH_BBOX['max_lon']}, {RIYADH_BBOX['max_lat']}, 4326))"
    )
```

**Operator: `ST_Intersects` against `ST_MakeEnvelope`.** Polygons whose boundary touches or crosses the bbox edge are *kept*; polygons fully outside are dropped. No envelope-only `&&` shortcut, no `ST_Centroid IN bbox` — the polygon body is the geometry being checked, so large mall parking polygons that straddle the edge are retained.

**Bbox extent:** ~80 km E–W × ~67 km N–S (lon 46.30→47.10 ≈ 80 km at lat 24.7, lat 24.40→25.00 ≈ 67 km). Riyadh's contiguous urban area is roughly that size; outlying northern growth (Al Sahafa, Al Yasmin, areas above lat 25.0) and SE growth (Diriyah is INSIDE, but Irqah/Al-Olaya-Industrial pockets sit near the edges) brush against the boundary. Whether any active expansion-advisor districts fall fully outside this bbox **cannot be verified from code** — see §10 for the Codespace SQL.

There is an explicitly different bbox used in the roads workflow's osmium pre-clip (`.github/workflows/expansion-advisor-data-roads.yml:96`): `--bbox 46.3,24.4,47.1,25.0`. **Identical** to `RIYADH_BBOX`. And the global Overpass-tile bbox in `.github/workflows/osm-import.yml:23` is wider: `BBOX: "24.20,46.20,25.10,47.30"` (note the file's CSV order is `south,west,north,east`). So:

- `osm-import.yml` (Overpass tiles): wider bbox `(46.20, 24.20) → (47.30, 25.10)` is populated into `planet_osm_*`.
- `expansion-advisor-data-roads.yml` (`--create` mode via Geofabrik PBF): tighter bbox `(46.3, 24.4) → (47.1, 25.0)`.
- `expansion_advisor_parking.py` (this ingest): re-applies the tighter `RIYADH_BBOX` at query time.

If the PBF-clipped `--create` path ran last, the source `planet_osm_*` tables are already narrower than the Overpass path would deliver. **The EPA ingest's own bbox filter is then redundant.** If only the Overpass path ran, source is wider, but the EPA ingest will still re-clip to the tighter Riyadh box. Either way, the bbox is unlikely to be the dominant cause of a 1,227-row total.

## 5. Geometry transformation

`detect_srid` (`app/ingest/expansion_advisor_common.py:88-110`) returns the actual SRID of `planet_osm_polygon.way`, falling back to 4326 if introspection fails. The ingest calls it explicitly (`app/ingest/expansion_advisor_parking.py:120, 209`).

**Polygon reprojection AND centroid reduction** (`app/ingest/expansion_advisor_parking.py:149`):

```python
            ST_Centroid(ST_Transform(op.way, 4326)),
```

**This is a real and consequential simplification.** A multi-hectare mall parking polygon is stored as a single point at its centroid. Read-time consumers (`_parking_score`, `_parking_evidence_band`) treat each EPA row as one "parking amenity", so collapsing one large surface lot into one row gives that lot **the same weight as a single tagged street-side bay**. It is not a row-count loss (one polygon in → one row out) but it is a fidelity loss that compounds the sparsity.

**Point reprojection** (`app/ingest/expansion_advisor_parking.py:249`):

```python
            ST_Transform(pt.way, 4326),
```

No centroid step (points are already 0-D). NULL `pt.way` is filtered upstream (line 263). No `coalesce(lon, lat)` reconstruction — if the OSM node lacks geometry it's dropped silently, but that's the right behavior.

**The polygon-centroid reduction means EPA rows count *parking polygon objects*, not *parking surface area*.** A district with one well-mapped 5 ha mall + a district with five tagged 50 m² lots both contribute 5 rows — though the underlying capacity differs by 100×. This is invisible to `_parking_score` and explains part of why districts cluster: it's not just OSM tagging density, it's OSM tagging *granularity*.

## 6. Upsert / dedup logic

There is **no upsert and no dedup**. The table DDL (`alembic/versions/d4e5f6a1b2c3_create_expansion_advisor_tables.py:46-64`):

```python
    op.execute("""
        CREATE TABLE IF NOT EXISTS expansion_parking_asset (
            id              SERIAL PRIMARY KEY,
            city            VARCHAR(64) NOT NULL DEFAULT 'riyadh',
            source          VARCHAR(64) NOT NULL DEFAULT 'osm',
            name            VARCHAR(256),
            amenity_type    VARCHAR(64),
            geom            geometry(Geometry, 4326),
            capacity        INTEGER,
            covered         BOOLEAN,
            public_access   BOOLEAN,
            walk_access_score   DOUBLE PRECISION,
            dropoff_score       DOUBLE PRECISION,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_epa_geom ON expansion_parking_asset USING gist (geom)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_epa_city ON expansion_parking_asset USING btree (city)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_epa_amenity_type ON expansion_parking_asset USING btree (amenity_type)")
```

**No `osm_id` column.** No unique constraint on `(geom)`, `(city, geom)`, or anything else. No `ON CONFLICT` clause in either INSERT (`app/ingest/expansion_advisor_parking.py:132-181, 233-266`).

The de facto dedup is the `replace=True` DELETE that runs once per source-tag before the INSERT (`app/ingest/expansion_advisor_parking.py:116-118, 205-207`):

```python
    if replace:
        db.execute(text("DELETE FROM expansion_parking_asset WHERE city = 'riyadh' AND source = 'osm_polygon'"))
        db.commit()
```

**Implications for sparsity:**

- A row can never be "dropped because it duplicates an existing one" — there's no constraint that would fire. So dedup is not a cause of sparsity.
- Conversely, if `--replace=false` is ever used (workflow input `replace_mode` defaults to `'true'` at `.github/workflows/expansion-advisor-data-parking.yml:11`), the table compounds with duplicates on every run. The current 1,227 figure could be either (a) a single clean replace-run, or (b) the last failed/partial run. Codespace can settle this by counting rows per `created_at` bucket — see §10.
- The lack of `osm_id` also means: when EPA is queried by `_parking_score`, two different OSM polygons that share a centroid (rare) cannot be told apart. Negligible.

## 7. Last-known successful ingest

**No ingest-state table for parking.** Grep for `ingest_state`, `ingest_run`, `ingest_log` finds zero hits across `app/ingest/expansion_advisor_parking.py` and `app/ingest/expansion_advisor_common.py`. The only "state" tracking in the OSM/ingest area is `osm_import_state` in `.github/workflows/osm-import.yml:204` (and that tracks tile-import progress, not EPA ingest history).

**Stats are written but transient.** `write_stats` (`app/ingest/expansion_advisor_common.py:142-147`) writes JSON to a path the caller provides. The workflow uploads it as an artifact with 30-day retention (`.github/workflows/expansion-advisor-data-parking.yml:69-75`); nothing is persisted to the DB.

**Row-count target / expectation:** the codebase carries none. The test for zero-row behavior (`tests/test_expansion_advisor_parking_ingest.py:253-269`) asserts the ingest doesn't crash when `rowcount=0`. Other tests (e.g. `tests/test_expansion_advisor_data_pipeline.py:1102-1128`) directly insert one or two rows; no test asserts "Riyadh should yield ≥ N rows from OSM".

**CLI entry point:** `python -m app.ingest.expansion_advisor_parking --replace true --write-stats /tmp/parking-stats.json` (`.github/workflows/expansion-advisor-data-parking.yml:48-50`). Scheduled cron: `"0 4 * * 2"` — weekly Tuesday 04:00 UTC (`.github/workflows/expansion-advisor-data-parking.yml:5`).

**Critical workflow finding — no OSM source-population step.** Compare:

- `.github/workflows/expansion-advisor-data-roads.yml:59-112` actively imports OSM data via Geofabrik PBF + osmium clip + `osm2pgsql --create` if `planet_osm_line` is empty.
- `.github/workflows/expansion-advisor-data-parking.yml` does **none** of that. It runs `expansion_advisor_parking` directly against whatever is in `planet_osm_polygon` / `planet_osm_point`.

So the parking workflow inherits whatever OSM data the roads workflow (or `osm-import.yml`) most recently produced. If those upstream workflows hadn't populated `planet_osm_point` with `amenity=parking_entrance` / `amenity=parking_space` nodes — see §10 SQL — the points pass starves at the source, not at the filter.

**Overpass-tile import path is structurally hostile to parking nodes.** The canonical osm-import workflow's Overpass template (`.github/workflows/osm-import.yml:67-91`) queries:

```text
way["building"], way["landuse"], way["amenity"], way["leisure"], way["shop"],
way["tourism"], way["natural"], way["landcover"], way["man_made"]
(plus the relation[…] variants)
```

Notably absent: any `node["amenity"…]` selector. The recursion `(._;>;)` (line 89) pulls geometric member nodes for the selected ways/relations but not standalone amenity-tagged nodes that aren't part of any selected way. So if this path populated `planet_osm_point` most recently, **standalone `node[amenity=parking_entrance]` / `node[amenity=parking_space]` are not in the source table at all** — and the 30-point figure makes sense as "whatever happened to be a member-node of an already-selected way".

## 8. Compare to `restaurant_poi` / other POI ingests

**`expansion_advisor_roads.py`** also reads from `planet_osm_*`. Its source-detection (`app/ingest/expansion_advisor_roads.py:59-65`):

```python
def _detect_source_table(db) -> str | None:
    """Find the best available OSM line table."""
    for candidate in ["planet_osm_line", "planet_osm_roads", "osm_roads"]:
        if table_exists(db, candidate):
            logger.info("Using OSM source table: %s", candidate)
            return candidate
    return None
```

It filters `WHERE l.way IS NOT NULL AND l.highway IS NOT NULL AND {bbox}` (`app/ingest/expansion_advisor_roads.py:134-137`) — a *much* broader filter (`l.highway IS NOT NULL` matches every classified road). Whether `expansion_road_context` has tens of thousands of rows or a few hundred is unknown from code (`app/ingest/expansion_advisor_refresh.py:78` logs counts at refresh time but no assertion or expectation is baked in).

**`restaurant_pois.py`** (`app/ingest/restaurant_pois.py:1-8`) is structurally different — it pulls from Overture Maps DuckDB, the live Overpass API, and the delivery-platform scrapers:

```python
"""
Ingestion pipeline for restaurant POI data from multiple sources.

Sources:
- Overture Maps Places (DuckDB -> S3)
- OSM (Overpass API)
- Delivery platforms (16 platforms via SCRAPER_REGISTRY)
"""
```

It uses Overpass live (`app/ingest/restaurant_pois.py:101-110`), not the local `planet_osm_*` tables. So `restaurant_poi` row counts say nothing about `planet_osm_*` density.

**No hard-coded row-count expectations** are found for any of these tables in the code (greppped `expected.*epa`, `expected.*parking`).

**Anchor for whether OSM-in-Riyadh is sparse parking-specifically vs sparse-everywhere: only Codespace SQL can settle it.** See §10.

## 9. Alternative source candidates

Grep for non-OSM parking data sources turned up nothing usable.

- **Google Places:** `app/connectors/google_places_async.py` and `app/ingest/google_reviews_enrich.py` exist, but `google_places_async.py` exposes only `nearby_search`/`text_search`/`place_details` and is wired into `restaurant_poi` enrichment, not parking. No reference to Google's `parking` field anywhere in `app/`.
- **Aqar listings:** `app/services/expansion_advisor.py:2856-2858`:

```python
    # parking_pass — trust the derived parking_score when it's populated.
    # For Aqar listings, Aqar doesn't expose a structured parking field
    # (their template only has Water/Electricity/Drainage), so the verdict
```

  And reinforced at `app/services/llm_decision_memo.py:1486, 1504` — Aqar has no structured parking field. Confirmed dead end.

- **ArcGIS parcels:** grep for `arcgis.*parking|parking.*arcgis` returns no hits. The parcel proxy view carries land-use class codes but no parking attribute.
- **Manual seeds / `seed.py` / TODO comments:** grep returns no parking-related entries. No CSV import path, no seed table.

**Conclusion for option C:** There is no second parking dataset already half-ingested. Switching off OSM means building a new ingest pipeline from scratch (Google Places `parking` lot search, manual digitisation, or a commercial geodata vendor). Hours of integration work is optimistic; weeks is realistic.

## 10. Codespace SQL to settle source-layer sparsity

```sql
-- A. How many parking-related polygons exist in the *source* table, before the EPA filter?
SELECT
    COUNT(*)                                                                       AS total_polygons,
    COUNT(*) FILTER (WHERE amenity IS NOT NULL)                                    AS with_any_amenity,
    COUNT(*) FILTER (WHERE lower(COALESCE(amenity, '')) = 'parking')               AS amenity_parking,
    COUNT(*) FILTER (
        WHERE lower(COALESCE(amenity, '')) = 'parking'
           OR lower(COALESCE(parking, '')) IN ('surface','multi-storey','underground','street_side','lane')
    )                                                                              AS matches_epa_filter,
    -- If this column is missing, drop the next two; they're for hstore fallback
    COUNT(*) FILTER (WHERE lower(COALESCE(tags->'parking', '')) <> '')             AS hstore_parking_any,
    COUNT(*) FILTER (WHERE lower(COALESCE(building, '')) = 'parking')              AS building_parking,
    COUNT(*) FILTER (WHERE lower(COALESCE(landuse, '')) IN ('garages','parking'))  AS landuse_garages
FROM planet_osm_polygon
WHERE way IS NOT NULL
  AND ST_Intersects(
        way,
        ST_MakeEnvelope(46.3, 24.4, 47.1, 25.0, 4326)
      );
```

```sql
-- B. Same breakdown for points. Tells us whether the 30-row outcome is
--    "filter killed it" or "source had nothing".
SELECT
    COUNT(*)                                                                       AS total_points,
    COUNT(*) FILTER (WHERE amenity IS NOT NULL)                                    AS with_any_amenity,
    COUNT(*) FILTER (WHERE lower(COALESCE(amenity, '')) = 'parking')               AS amenity_parking,
    COUNT(*) FILTER (WHERE lower(COALESCE(amenity, '')) = 'parking_entrance')      AS amenity_parking_entrance,
    COUNT(*) FILTER (WHERE lower(COALESCE(amenity, '')) = 'parking_space')         AS amenity_parking_space
FROM planet_osm_point
WHERE way IS NOT NULL
  AND ST_Intersects(
        way,
        ST_MakeEnvelope(46.3, 24.4, 47.1, 25.0, 4326)
      );
```

```sql
-- C. Source loss vs ingest loss. If matches_epa_filter (from query A) is
--    materially > 1,197, the ingest itself is dropping rows; otherwise the
--    source is sparse and the ingest is faithful.
WITH src AS (
    SELECT COUNT(*) AS n
    FROM planet_osm_polygon
    WHERE way IS NOT NULL
      AND ST_Intersects(way, ST_MakeEnvelope(46.3, 24.4, 47.1, 25.0, 4326))
      AND (
        lower(COALESCE(amenity, '')) = 'parking'
        OR lower(COALESCE(parking, '')) IN ('surface','multi-storey','underground','street_side','lane')
      )
),
epa AS (SELECT COUNT(*) AS n FROM expansion_parking_asset WHERE source = 'osm_polygon')
SELECT src.n AS source_matches, epa.n AS epa_polygon_rows, (src.n - epa.n) AS gap
FROM src, epa;
```

```sql
-- D. Per-district EPA density. Substitute the actual district polygon
--    source if it isn't riyadh_districts_poly (try aqar_district_hulls,
--    expansion_advisor district lookup).
SELECT
    d.name_en                                                                       AS district,
    COUNT(epa.id)                                                                   AS epa_rows,
    AVG(
        (SELECT COUNT(*) FROM expansion_parking_asset epa2
          WHERE ST_DWithin(epa2.geom::geography, d.centroid::geography, 350))
    )                                                                               AS avg_count_around_centroid
FROM riyadh_districts_poly d
LEFT JOIN expansion_parking_asset epa
  ON ST_Intersects(epa.geom, d.geom)
GROUP BY d.name_en
ORDER BY epa_rows ASC NULLS FIRST
LIMIT 50;
```

```sql
-- E. Are EPA rows clumping at one or two big polygons per district? The
--    polygon-centroid reduction (§5) collapses surface area into a single
--    point — if a district has 1 EPA row, that one row might really be a
--    100,000 m² mall lot. Use raw planet_osm_polygon to look at area.
SELECT
    epa.id,
    epa.amenity_type,
    ST_X(epa.geom) AS lon, ST_Y(epa.geom) AS lat,
    ROUND((
        SELECT ST_Area(op.way::geography)
        FROM planet_osm_polygon op
        WHERE ST_DWithin(op.way::geography, epa.geom::geography, 5)
          AND (lower(COALESCE(op.amenity, '')) = 'parking'
               OR lower(COALESCE(op.parking, '')) IN ('surface','multi-storey','underground','street_side','lane'))
        LIMIT 1
    )::numeric, 1) AS approx_source_area_m2
FROM expansion_parking_asset epa
WHERE epa.source = 'osm_polygon'
ORDER BY approx_source_area_m2 DESC NULLS LAST
LIMIT 25;
```

```sql
-- F. Is the table the residue of a partial re-ingest (multiple created_at
--    buckets, suggesting append-without-replace), or a single clean run?
SELECT date_trunc('hour', created_at) AS bucket, source, COUNT(*) AS rows
FROM expansion_parking_asset
WHERE city = 'riyadh'
GROUP BY 1, 2
ORDER BY 1;
```

```sql
-- G. Sanity-check the bbox vs the actual extent of every candidate the
--    Expansion Advisor has surfaced recently. Any candidate outside the
--    bbox is one whose parking lookup will always come back empty.
SELECT
    COUNT(*) FILTER (
        WHERE ST_Intersects(
            ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326),
            ST_MakeEnvelope(46.3, 24.4, 47.1, 25.0, 4326)
        )
    ) AS in_bbox,
    COUNT(*) FILTER (
        WHERE NOT ST_Intersects(
            ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326),
            ST_MakeEnvelope(46.3, 24.4, 47.1, 25.0, 4326)
        )
    ) AS out_of_bbox
FROM expansion_candidate c
WHERE c.created_at > now() - interval '30 days'
  AND c.lon IS NOT NULL AND c.lat IS NOT NULL;
```

## Hypotheses ranked by likelihood

1. **OSM tag density for parking is genuinely sparse in Riyadh.** Most read paths still resolve to `amenity=parking` on ways/relations, and Riyadh's OSM mapper community has historically prioritised buildings/highways over leisure-mapping parking lots. The ingest filter is wider than the read-time filter (§2), so the gap can't be the filter. Settle with Query A — if `matches_epa_filter ≈ 1,197`, this is the cause. *Code cites:* `app/ingest/expansion_advisor_parking.py:79-98, 132-181`.

2. **`planet_osm_point` is starved at the upstream import.** The canonical Overpass tile workflow (`.github/workflows/osm-import.yml:67-91`) does **not** select `node["amenity"…]` — it only selects ways/relations and pulls geometric member nodes via `(._;>;)`. Standalone `amenity=parking_entrance` / `amenity=parking_space` nodes are absent unless they happen to be members of an already-selected way. The 30-row point figure is consistent with "only the few parking nodes that piggybacked on a way". Settle with Query B. *Code cites:* `.github/workflows/osm-import.yml:67-91, 89`.

3. **Polygon→centroid collapse (`ST_Centroid`) is hiding apparent breadth.** Even if the source had, say, 50 well-mapped malls each with a 5 ha parking polygon, the row count would still be 50 — *and each row counts equally with a single street-side bay*. So the headline 1,227 figure is "1,227 parking objects", not "1,227 parking units" or "1,227 km² of parking surface". Sparsity perception is partly an artefact of this 1-row-per-polygon reduction. Settle with Query E. *Code cites:* `app/ingest/expansion_advisor_parking.py:149`.

4. **The bbox cuts off outer Riyadh districts.** Quick math says `(46.3, 24.4) → (47.1, 25.0)` ≈ 80 × 67 km, plausibly clipping growth at the northern fringe (Al Yasmin, Al Sahafa edges) and SE (some Diriyah / Irqah pockets). If candidates routinely live just outside the bbox, EPA would silently return 0 within 350 m. Settle with Query G. *Code cites:* `app/ingest/expansion_advisor_common.py:25-30`; `.github/workflows/expansion-advisor-data-roads.yml:96`.

5. **The hstore column (`tags`/`other_tags`) may not exist on the deployed schema.** If `planet_osm_polygon` was created with `osm2pgsql --hstore` but the column landed under a different name (or the flag wasn't set), `_parking_expr` returns `"NULL"` (`app/ingest/expansion_advisor_parking.py:50-58`), and `_build_where_filter` drops the OR clause entirely (`app/ingest/expansion_advisor_parking.py:90-94`). The `--hstore` flag IS set in both workflows (`.github/workflows/osm-import.yml:265`; `.github/workflows/expansion-advisor-data-roads.yml:105`), so this is unlikely — but Query A's `hstore_parking_any` column will reveal it if `tags` is missing.

6. **The `--replace=false` invocation path leaves stale data and partial re-runs.** Workflow default is `'true'` (`.github/workflows/expansion-advisor-data-parking.yml:11`), but ad-hoc `workflow_dispatch` invocations could override it. Settle with Query F (multiple `created_at` buckets ⇒ this happened).

7. **The polygon ingest silently rolled back.** `db.execute(insert_sql) ; db.commit()` (`app/ingest/expansion_advisor_parking.py:183-184`) has no try/except. A constraint or geometry exception (e.g., a self-intersecting polygon failing `ST_Centroid`) would abort the entire INSERT and leave the table with only the rows that survived a prior `--replace=true` DELETE — i.e., zero `osm_polygon` rows from the failed run. Unlikely given the 1,197-row figure is non-zero, but worth ruling out by re-running with verbose logs.

8. **EPA's lack of a UNIQUE constraint masks accidental double-runs.** Mitigated by the DELETE-then-INSERT pattern, so this is mostly inert today — but it means we cannot rule out "an old partial run left some rows and a newer one ran with `--replace=false`". Same SQL as Query F.

End of investigation. Read-only — no files modified.
