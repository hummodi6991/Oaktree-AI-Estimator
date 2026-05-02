# external_feature_polygons_mat

Materialized view of pre-parsed district polygons from `external_feature`.

## Source

`external_feature` rows where:
- `layer_name = 'aqar_district_hulls'`
- `geometry` is a valid GeoJSON Polygon or MultiPolygon
- `district_raw` or `district` property is non-empty

Geometry is stored as `geometry JSONB` (raw GeoJSON) in `external_feature`.
The matview parses GeoJSON via `ST_SetSRID(ST_GeomFromGeoJSON(...), 4326)`
once at refresh time, so consumers can join on a real `geometry` column
without incurring per-request parse cost.

> **History**: prior to 2026-05-01 the matview also included
> `osm_districts`. That layer was dropped because of non-Riyadh data
> contamination — see `docs/osm_districts_removal.md`. `layer_name` is
> retained on the matview for forward-compatibility if it is ever extended
> to other layers.

## Columns

| column | type | notes |
|---|---|---|
| `feature_id` | text | PK; references `external_feature.id` |
| `layer_name` | text | always `'aqar_district_hulls'` (kept for forward-compat) |
| `district_label` | text | `TRIM(COALESCE(district_raw, district))` |
| `geom` | geometry(Geometry,4326) | parsed at refresh time |

## Indexes

- `ux_external_feature_polygons_mat_feature_id` (UNIQUE) — required for
  CONCURRENT refresh
- `ix_external_feature_polygons_mat_geom` (GIST) — used by `ST_Contains` /
  `ST_Intersects` joins
- `ix_external_feature_polygons_mat_layer_name` (btree) — kept for
  forward-compatibility; currently degenerate (single value)

## Refresh

Polygons effectively never change. Refresh is **on-demand only**:

- **GitHub Actions UI**: run the "Refresh external_feature_polygons_mat"
  workflow (manual `workflow_dispatch`)
- **Codespace**: `python -m app.services.external_feature_refresh`

Refresh is `CONCURRENTLY` so production reads are not blocked.

## Consumers

- `app/services/expansion_advisor.py::_district_momentum_score`

When a new consumer needs district polygons, JOIN against this matview
rather than parsing `external_feature.geometry` inline.
