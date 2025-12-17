CREATE TABLE IF NOT EXISTS overture_buildings (
  id text primary key,
  subtype text,
  class text,
  height double precision,
  num_floors int,
  geom geometry(MultiPolygon,32638)
);

DROP TABLE IF EXISTS osm_parcels_proxy;

CREATE TABLE osm_parcels_proxy AS
SELECT
  'ovt' AS source,
  ('ovt:' || id) AS id,
  COALESCE(class, subtype, 'building') AS landuse,
  'overture_building' AS classification,
  ST_Area(geom) AS area_m2,
  geom
FROM overture_buildings
UNION ALL
SELECT
  'osm' AS source,
  CASE WHEN osm_id < 0 THEN 'rel' || abs(osm_id)::text ELSE 'way' || osm_id::text END AS id,
  CASE
    WHEN is_res = 1 AND is_com = 1 THEN 'mixed'
    WHEN is_res = 1 THEN 'residential'
    WHEN is_com = 1 THEN 'commercial'
    ELSE any_tag
  END AS landuse,
  CASE
    WHEN landuse IS NOT NULL THEN 'landuse'
    WHEN building IS NOT NULL THEN 'building'
    WHEN amenity IS NOT NULL THEN 'amenity'
    WHEN leisure IS NOT NULL THEN 'leisure'
    WHEN shop IS NOT NULL THEN 'shop'
    WHEN tourism IS NOT NULL THEN 'tourism'
    WHEN "natural" IS NOT NULL THEN 'natural'
    WHEN tags ? 'landcover' THEN 'landcover'
    WHEN tags ? 'man_made' THEN 'man_made'
  END AS classification,
  ST_Area(geom_32638) AS area_m2,
  geom_32638 AS geom
FROM (
  SELECT
    ST_SetSRID(ST_Transform(way, 32638), 32638) AS geom_32638,
    *,
    CASE
      WHEN landuse IN ('residential') THEN 1
      WHEN building IN ('residential','apartments','house','detached','terrace','semidetached') THEN 1
      ELSE 0
    END AS is_res,
    CASE
      WHEN landuse IN ('commercial','retail') THEN 1
      WHEN building IN ('retail','commercial','office','shop') THEN 1
      ELSE 0
    END AS is_com,
    coalesce(landuse, building, amenity, leisure, shop, tourism, "natural", tags->'landcover', tags->'man_made') AS any_tag
  FROM planet_osm_polygon
  WHERE
    landuse IS NOT NULL
    OR building IS NOT NULL
    OR amenity IS NOT NULL
    OR leisure IS NOT NULL
    OR shop IS NOT NULL
    OR tourism IS NOT NULL
    OR "natural" IS NOT NULL
    OR (tags ? 'landcover')
    OR (tags ? 'man_made')
) candidates;

CREATE INDEX IF NOT EXISTS overture_buildings_geom_gix ON overture_buildings USING GIST (geom);
CREATE INDEX IF NOT EXISTS osm_parcels_proxy_geom_gix ON osm_parcels_proxy USING GIST (geom);
CREATE INDEX IF NOT EXISTS osm_parcels_proxy_id_idx ON osm_parcels_proxy (id);
ANALYZE osm_parcels_proxy;
