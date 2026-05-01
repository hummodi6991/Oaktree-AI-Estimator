# external_feature_polygons_mat

Materialized view of pre-parsed district polygons from `external_feature`.

## Source

`external_feature` rows where:
- `layer_name IN ('osm_districts', 'aqar_district_hulls')`
- `geometry` is a valid GeoJSON Polygon or MultiPolygon
- `district_raw` or `district` property is non-empty

Geometry is stored as `geometry JSONB` (raw GeoJSON) in `external_feature` —
the bare `geom` PostGIS column on that table is populated only on a small
out-of-band subset of rows (~12 OSM polygons; zero Aqar hulls). The
matview parses GeoJSON via `ST_SetSRID(ST_GeomFromGeoJSON(...), 4326)` once
at refresh time, so consumers can join on a real `geometry` column without
incurring per-request parse cost.

## Columns

| column | type | notes |
|---|---|---|
| `feature_id` | text | PK; references `external_feature.id` |
| `layer_name` | text | `'osm_districts'` or `'aqar_district_hulls'` |
| `district_label` | text | `TRIM(COALESCE(district_raw, district))` |
| `geom` | geometry(Geometry,4326) | parsed at refresh time |

## Indexes

- `ux_external_feature_polygons_mat_feature_id` (UNIQUE) — required for
  CONCURRENT refresh
- `ix_external_feature_polygons_mat_geom` (GIST) — used by `ST_Contains` /
  `ST_Intersects` joins
- `ix_external_feature_polygons_mat_layer_name` (btree) — for
  OSM-first DISTINCT ON queries

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
