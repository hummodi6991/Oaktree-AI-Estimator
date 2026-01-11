"""Endpoints for parcel queries via external GIS."""

import hashlib
import json
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from shapely.geometry import MultiPolygon, Polygon, shape
from sqlalchemy import bindparam, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.connectors.arcgis import query_features
from app.core.config import settings
from app.db.deps import get_db
from app.models.tables import Parcel
from app.services.geo import _landuse_code_from_label
from app.services.overture_buildings_metrics import compute_building_metrics

logger = logging.getLogger(__name__)

Transformer = None  # no local transform; we use ST_Transform on the server

_NO_SIGNAL_LABELS = {"", "building", "yes", "true", "1", "0", "unknown", "none"}
_HAS_OSM_ROADS: bool | None = None


def _label_is_signal(label: str | None, code: str | None) -> bool:
    if not code:
        return False
    tl = (label or "").strip().lower()
    return tl not in _NO_SIGNAL_LABELS


def _safe_identifier(value: str | None, fallback: str) -> str:
    candidate = (value or "").strip()
    if not candidate:
        return fallback
    if all(ch.isalnum() or ch in {"_", "."} for ch in candidate):
        return candidate
    logger.warning("Unsafe identifier %s; falling back to %s", value, fallback)
    return fallback


def _parse_ms_building_parcel_id(value: str) -> tuple[int, int] | None:
    cleaned = value.strip()
    if cleaned.startswith("ms:"):
        cleaned = cleaned[3:]
    parts = cleaned.split(":")
    if len(parts) != 2:
        return None
    try:
        building_id = int(parts[0])
        part_index = int(parts[1])
    except ValueError:
        return None
    if part_index < 1:
        return None
    return building_id, part_index


def _build_ms_building_parcel_payload(parcel_ids: list[str]) -> list[dict[str, int | str]]:
    payload: list[dict[str, int | str]] = []
    for pid in parcel_ids:
        parsed = _parse_ms_building_parcel_id(pid)
        if not parsed:
            continue
        building_id, part_index = parsed
        payload.append({"parcel_id": pid, "building_id": building_id, "part_index": part_index})
    return payload


def _has_osm_roads(db: Session) -> bool:
    global _HAS_OSM_ROADS
    if _HAS_OSM_ROADS is not None:
        return _HAS_OSM_ROADS
    try:
        row = db.execute(text("SELECT to_regclass('public.planet_osm_line')")).scalar()
        _HAS_OSM_ROADS = row is not None
    except SQLAlchemyError as exc:
        logger.warning("OSM roads table lookup failed: %s", exc)
        _HAS_OSM_ROADS = False
    return _HAS_OSM_ROADS


_TARGET_SRID = getattr(settings, "PARCEL_TARGET_SRID", 32638)
_DEFAULT_TOLERANCE = getattr(settings, "PARCEL_IDENTIFY_TOLERANCE_M", 25.0) or 25.0
if _DEFAULT_TOLERANCE <= 0:
    _DEFAULT_TOLERANCE = 25.0

_PARCEL_TABLE = _safe_identifier(getattr(settings, "PARCEL_IDENTIFY_TABLE", "parcels"), "parcels")
_PARCEL_GEOM_COLUMN = _safe_identifier(
    getattr(settings, "PARCEL_IDENTIFY_GEOM_COLUMN", "geom"), "geom"
)
_DERIVED_PARCEL_TABLES = {"public.derived_parcels_v1", "derived_parcels_v1"}
_MS_BUILDINGS_TABLES = {"public.ms_buildings_raw", "ms_buildings_raw"}
_IS_DERIVED_TABLE = _PARCEL_TABLE in _DERIVED_PARCEL_TABLES
_IS_MS_BUILDINGS_TABLE = _PARCEL_TABLE in _MS_BUILDINGS_TABLES
_PARCEL_ID_COLUMN = "parcel_id" if _IS_DERIVED_TABLE else "id"

if _IS_DERIVED_TABLE or _IS_MS_BUILDINGS_TABLE:
    _PARCEL_LANDUSE_EXPR = "NULL::text AS landuse"
    _PARCEL_CLASSIFICATION_EXPR = "NULL::text AS classification"
    _PARCEL_CLASSIFICATION_REF = "NULL::text"
    _PARCEL_AREA_EXPR = "area_m2::bigint AS area_m2" if _IS_MS_BUILDINGS_TABLE else "site_area_m2::bigint AS area_m2"
    _PARCEL_PERIMETER_EXPR = (
        f"ST_Perimeter({_PARCEL_GEOM_COLUMN}::geography)::bigint AS perimeter_m"
    )
    _PARCEL_SITE_AREA_EXPR = "NULL::double precision AS site_area_m2" if _IS_MS_BUILDINGS_TABLE else "site_area_m2"
    _PARCEL_FOOTPRINT_EXPR = "NULL::double precision AS footprint_area_m2" if _IS_MS_BUILDINGS_TABLE else "footprint_area_m2"
    _PARCEL_BUILDING_COUNT_EXPR = "NULL::int AS building_count" if _IS_MS_BUILDINGS_TABLE else "building_count"
else:
    _PARCEL_LANDUSE_EXPR = "landuse"
    _PARCEL_CLASSIFICATION_EXPR = "classification"
    _PARCEL_CLASSIFICATION_REF = "classification"
    _PARCEL_AREA_EXPR = "area_m2::bigint AS area_m2"
    _PARCEL_PERIMETER_EXPR = f"ST_Perimeter({_PARCEL_GEOM_COLUMN})::bigint AS perimeter_m"
    _PARCEL_SITE_AREA_EXPR = "NULL::double precision AS site_area_m2"
    _PARCEL_FOOTPRINT_EXPR = "NULL::double precision AS footprint_area_m2"
    _PARCEL_BUILDING_COUNT_EXPR = "NULL::int AS building_count"

if _TARGET_SRID == 4326:
    _POINT_EXPR = "ST_SetSRID(ST_Point(:lng,:lat), 4326)"
    _DISTANCE_EXPR = (
        f"ST_Distance({_PARCEL_GEOM_COLUMN}::geography, q.pt::geography) AS distance_m"
    )
    _D_WITHIN_EXPR = f"ST_DWithin({_PARCEL_GEOM_COLUMN}::geography, q.pt::geography, :tol_m)"
    _IDENTIFY_GEOM_OUTPUT_EXPR = "ST_AsGeoJSON(geom) AS geom"
    _UNION_GEOM_OUTPUT_EXPR = "ST_AsGeoJSON(u.geom) AS geom"
else:
    _POINT_EXPR = "ST_Transform(ST_SetSRID(ST_Point(:lng,:lat), 4326), :srid)"
    _DISTANCE_EXPR = f"ST_Distance({_PARCEL_GEOM_COLUMN}, q.pt) AS distance_m"
    _D_WITHIN_EXPR = f"ST_DWithin({_PARCEL_GEOM_COLUMN}, q.pt, :tol_m)"
    _IDENTIFY_GEOM_OUTPUT_EXPR = "ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geom"
    _UNION_GEOM_OUTPUT_EXPR = "ST_AsGeoJSON(ST_Transform(u.geom, 4326)) AS geom"

_TRANSFORMER = None  # unused (server-side transform)

_IDENTIFY_SQL = text(
    f"""
  WITH q AS (
    SELECT {_POINT_EXPR} AS pt
  ),
  scored AS (
    SELECT
      {_PARCEL_ID_COLUMN} AS id,
      {_PARCEL_LANDUSE_EXPR},
      {_PARCEL_CLASSIFICATION_EXPR},
      {_PARCEL_GEOM_COLUMN} AS geom,
      {_PARCEL_AREA_EXPR},
      {_PARCEL_PERIMETER_EXPR},
      {_DISTANCE_EXPR},
      {_PARCEL_SITE_AREA_EXPR},
      {_PARCEL_FOOTPRINT_EXPR},
      {_PARCEL_BUILDING_COUNT_EXPR},
      CASE WHEN ST_Contains({_PARCEL_GEOM_COLUMN}, q.pt) THEN 1 ELSE 0 END AS contains,
      CASE WHEN ST_Intersects({_PARCEL_GEOM_COLUMN}, q.pt) THEN 1 ELSE 0 END AS hits,
      CASE WHEN {_D_WITHIN_EXPR} THEN 1 ELSE 0 END AS near,
      CASE WHEN {_PARCEL_CLASSIFICATION_REF} = 'overture_building' THEN 0 ELSE 1 END AS is_non_ovt,
      CASE WHEN {_PARCEL_CLASSIFICATION_REF} = 'overture_building' THEN 1 ELSE 0 END AS is_ovt
    FROM {_PARCEL_TABLE}, q
    WHERE {_D_WITHIN_EXPR}
  )
  SELECT
    id,
    landuse,
    classification,
    area_m2,
    perimeter_m,
    site_area_m2,
    footprint_area_m2,
    building_count,
    {_IDENTIFY_GEOM_OUTPUT_EXPR},
    distance_m,
    contains,
    hits,
    near,
    is_ovt
  FROM scored
  ORDER BY
    contains DESC,
    hits DESC,
    is_non_ovt DESC,
    distance_m ASC,
    area_m2 DESC,
    is_ovt DESC
  LIMIT 1;
  """
)

_DEFAULT_PARCEL_PAD_M = getattr(settings, "PARCEL_ENVELOPE_PAD_M", 5.0)
if _DEFAULT_PARCEL_PAD_M <= 0:
    _DEFAULT_PARCEL_PAD_M = 5.0

_INFER_PARCEL_SQL = text(
    """
    WITH params AS (
      SELECT ST_SetSRID(ST_Point(:lng,:lat), 4326) AS pt
    ),
    win AS (
      SELECT ST_Buffer(pt::geography, :radius_m)::geometry AS geom FROM params
    ),
    win4326 AS (
      SELECT ST_Envelope(geom) AS geom FROM win
    ),
    roads AS (
      SELECT ST_Union(ST_Buffer(way::geography, :road_buf_m)::geometry) AS geom
      FROM public.planet_osm_line, win4326 w
      WHERE way && w.geom
    ),
    free_space AS (
      SELECT ST_Difference(
        (SELECT geom FROM win),
        COALESCE((SELECT geom FROM roads), ST_GeomFromText('POLYGON EMPTY',4326))
      ) AS geom
    ),
    blocks AS (
      SELECT (ST_Dump(ST_Multi(ST_MakeValid(geom)))).geom AS geom
      FROM free_space
      WHERE geom IS NOT NULL
    ),
    block_hit AS (
      SELECT geom
      FROM blocks, params
      WHERE ST_Intersects(geom, params.pt)
      ORDER BY ST_Area(geom::geography) ASC
      LIMIT 1
    ),
    target AS (
      SELECT
        b.id AS building_id,
        ST_GeometryN(b.geom, :part_index) AS geom,
        ST_PointOnSurface(ST_GeometryN(b.geom, :part_index)) AS seed
      FROM public.ms_buildings_raw b
      WHERE b.id = :building_id
    ),
    target_block AS (
      SELECT t.*
      FROM target t, block_hit bh
      WHERE t.geom IS NOT NULL AND ST_Intersects(t.geom, bh.geom)
    ),
    neighbors AS (
      SELECT
        b.id,
        ST_PointOnSurface(ST_GeometryN(b.geom, 1)) AS seed
      FROM public.ms_buildings_raw b, win4326 w, block_hit bh, params
      WHERE b.geom && w.geom
        AND ST_Intersects(b.geom, bh.geom)
      ORDER BY ST_Distance(ST_PointOnSurface(b.geom)::geography, params.pt::geography)
      LIMIT :k
    ),
    seeds AS (
      SELECT id, seed FROM neighbors
      UNION
      SELECT building_id AS id, seed FROM target_block
    ),
    seeds_unique AS (
      SELECT DISTINCT ON (id) id, seed FROM seeds
    ),
    seeds3857 AS (
      SELECT id, ST_Transform(seed, 3857) AS seed FROM seeds_unique
    ),
    env AS (
      SELECT ST_Envelope(ST_Transform((SELECT geom FROM block_hit), 3857)) AS env
    ),
    v AS (
      SELECT (ST_Dump(ST_VoronoiPolygons(ST_Collect(seed), 0.0, (SELECT env FROM env)))).geom AS cell
      FROM seeds3857
    ),
    parcel3857 AS (
      SELECT ST_MakeValid(
        ST_Intersection(v.cell, ST_Transform((SELECT geom FROM block_hit), 3857))
      ) AS geom
      FROM v, target_block t
      WHERE ST_Contains(v.cell, ST_Transform(t.seed, 3857))
      LIMIT 1
    ),
    final AS (
      SELECT geom
      FROM parcel3857
      WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
    )
    SELECT
      ST_AsGeoJSON(ST_Transform(final.geom, 4326)) AS geom,
      ST_Area(final.geom) AS area_m2,
      ST_Perimeter(final.geom) AS perimeter_m,
      (SELECT ST_Area(geom::geography) FROM block_hit) AS block_area_m2,
      (SELECT COUNT(*) FROM seeds_unique) AS neighbor_count
    FROM final;
    """
)

_INFER_PARCEL_FALLBACK_SQL = text(
    """
    WITH target AS (
      SELECT ST_GeometryN(geom, :part_index) AS geom
      FROM public.ms_buildings_raw
      WHERE id = :building_id
    ),
    envelope AS (
      SELECT ST_Buffer(ST_OrientedEnvelope(ST_Transform(geom, 3857)), :pad_m) AS geom3857
      FROM target
      WHERE geom IS NOT NULL
    )
    SELECT
      ST_AsGeoJSON(ST_Transform(geom3857, 4326)) AS geom,
      ST_Area(geom3857) AS area_m2,
      ST_Perimeter(geom3857) AS perimeter_m
    FROM envelope;
    """
)

if _IS_MS_BUILDINGS_TABLE:
    _MS_BUILDING_AREA_EXPR = (
        "ST_Area(ST_GeometryN(b.geom, i.part_index)::geography)"
        if _TARGET_SRID == 4326
        else "ST_Area(ST_GeometryN(b.geom, i.part_index))"
    )
    _MS_BUILDING_UNION_AREA_EXPR = (
        "ST_Area(u.geom::geography)" if _TARGET_SRID == 4326 else "ST_Area(u.geom)"
    )
    _MS_BUILDING_UNION_PERIM_EXPR = (
        "ST_Perimeter(u.geom::geography)"
        if _TARGET_SRID == 4326
        else "ST_Perimeter(u.geom)"
    )

    _COLLATE_META_SQL = text(
        f"""
        WITH input AS (
          SELECT
            (item->>'parcel_id') AS parcel_id,
            (item->>'building_id')::bigint AS building_id,
            (item->>'part_index')::int AS part_index
          FROM jsonb_array_elements(:parcel_json::jsonb) AS item
        )
        SELECT
          i.parcel_id AS id,
          NULL::text AS landuse,
          NULL::text AS classification,
          {_MS_BUILDING_AREA_EXPR}::bigint AS area_m2
        FROM {_PARCEL_TABLE} b
        JOIN input i ON b.id = i.building_id
        WHERE ST_GeometryN(b.geom, i.part_index) IS NOT NULL
        """
    )

    _COLLATE_UNION_SQL = text(
        f"""
        WITH input AS (
          SELECT
            (item->>'parcel_id') AS parcel_id,
            (item->>'building_id')::bigint AS building_id,
            (item->>'part_index')::int AS part_index
          FROM jsonb_array_elements(:parcel_json::jsonb) AS item
        ),
        sel AS (
          SELECT ST_GeometryN(b.geom, i.part_index) AS geom
          FROM {_PARCEL_TABLE} b
          JOIN input i ON b.id = i.building_id
          WHERE ST_GeometryN(b.geom, i.part_index) IS NOT NULL
        ),
        u AS (
          SELECT ST_MakeValid(ST_UnaryUnion(ST_Collect(geom))) AS geom
          FROM sel
        )
        SELECT
          {_UNION_GEOM_OUTPUT_EXPR},
          {_MS_BUILDING_UNION_AREA_EXPR}::bigint AS area_m2,
          {_MS_BUILDING_UNION_PERIM_EXPR}::bigint AS perimeter_m
        FROM u;
        """
    )
else:
    _COLLATE_META_SQL = (
        text(
            f"""
            SELECT
              {_PARCEL_ID_COLUMN}::text AS id,
              {_PARCEL_LANDUSE_EXPR},
              {_PARCEL_CLASSIFICATION_EXPR},
              {_PARCEL_AREA_EXPR}
            FROM {_PARCEL_TABLE}
            WHERE {_PARCEL_ID_COLUMN}::text IN :ids
            """
        )
        .bindparams(bindparam("ids", expanding=True))
    )

    _COLLATE_UNION_SQL = (
        text(
            f"""
            WITH sel AS (
              SELECT {_PARCEL_GEOM_COLUMN} AS geom
              FROM {_PARCEL_TABLE}
              WHERE {_PARCEL_ID_COLUMN}::text IN :ids
            ),
            u AS (
              SELECT ST_MakeValid(ST_UnaryUnion(ST_Collect(geom))) AS geom
              FROM sel
            )
            SELECT
              {_UNION_GEOM_OUTPUT_EXPR},
              {("ST_Area(u.geom::geography)" if _TARGET_SRID == 4326 else "ST_Area(u.geom)")}::bigint AS area_m2,
              {("ST_Perimeter(u.geom::geography)" if _TARGET_SRID == 4326 else "ST_Perimeter(u.geom)")}::bigint AS perimeter_m
            FROM u;
            """
        )
        .bindparams(bindparam("ids", expanding=True))
    )

# Keep router local to "geo"; main.py mounts routers at "/v1".
router = APIRouter(prefix="/geo", tags=["geo"])


class BuildingMetricsRequest(BaseModel):
    geojson: dict = Field(..., description="GeoJSON Polygon or MultiPolygon (WGS84)")
    buffer_m: float | None = Field(
        default=None,
        description="Optional buffer distance in meters applied before computing metrics.",
    )


class BuildingMetricsResponse(BaseModel):
    site_area_m2: float | None = None
    footprint_area_m2: float | None = None
    coverage_ratio: float | None = None
    floors_mean: float | None = None
    floors_median: float | None = None
    existing_bua_m2: float | None = None
    far_proxy_existing: float | None = None
    built_density_m2_per_ha: float | None = None
    building_count: int = 0
    pct_buildings_with_floors_data: float | None = None
    buffer_m: float | None = None


class SuhailSridDebugResponse(BaseModel):
    srid_geom: int | None = None
    srid_geom_32638: int | None = None
    sample_pt_wgs84: str | None = None
    sample_pt_utm: str | None = None


def _normalize_building_geojson(obj: dict) -> dict:
    if not isinstance(obj, dict):
        raise HTTPException(status_code=400, detail="GeoJSON payload must be an object")

    geom = obj
    if obj.get("type") == "Feature":
        geom = obj.get("geometry")
        if geom is None:
            raise HTTPException(status_code=400, detail="Feature must include a geometry")

    if not isinstance(geom, dict):
        raise HTTPException(status_code=400, detail="Geometry must be an object")

    gtype = (geom.get("type") or "").lower()
    if gtype not in {"polygon", "multipolygon"}:
        raise HTTPException(status_code=400, detail="Geometry must be a Polygon or MultiPolygon")

    try:
        parsed = shape(geom)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid GeoJSON geometry: {exc}") from exc

    if not isinstance(parsed, (Polygon, MultiPolygon)):
        raise HTTPException(status_code=400, detail="Geometry must be a Polygon or MultiPolygon")
    if parsed.is_empty:
        raise HTTPException(status_code=400, detail="Geometry is empty")

    return geom


@router.post(
    "/building-metrics",
    response_model=BuildingMetricsResponse,
    summary="Compute Overture building metrics inside a polygon",
    description="Returns coverage, floors proxy stats, and built-up area using Overture buildings in SRID 32638.",
)
def building_metrics(payload: BuildingMetricsRequest, db: Session = Depends(get_db)) -> BuildingMetricsResponse:
    geom = _normalize_building_geojson(payload.geojson)
    metrics = compute_building_metrics(db, geom, buffer_m=payload.buffer_m)
    return BuildingMetricsResponse(**metrics)


@router.get(
    "/debug/suhail_srid",
    response_model=SuhailSridDebugResponse,
    summary="Debug Suhail parcel SRIDs",
    description="Returns SRIDs and sample point-on-surface values for Suhail parcel geometries.",
)
def debug_suhail_srid(db: Session = Depends(get_db)) -> SuhailSridDebugResponse:
    row = db.execute(
        text(
            """
            SELECT
                Find_SRID('public', 'suhail_parcels_mat', 'geom') AS srid_geom,
                Find_SRID('public', 'suhail_parcels_mat', 'geom_32638') AS srid_geom_32638,
                (
                    SELECT ST_AsText(ST_PointOnSurface(geom))
                    FROM public.suhail_parcels_mat
                    WHERE geom IS NOT NULL
                    LIMIT 1
                ) AS sample_pt_wgs84,
                (
                    SELECT ST_AsText(ST_PointOnSurface(geom_32638))
                    FROM public.suhail_parcels_mat
                    WHERE geom_32638 IS NOT NULL
                    LIMIT 1
                ) AS sample_pt_utm
            """
        )
    ).mappings().one()
    return SuhailSridDebugResponse(**row)


_OSM_CLASSIFY_SQL = text(
    """
    WITH g AS (
      SELECT ST_SetSRID(ST_GeomFromGeoJSON(:gj), 4326) AS geom
    ),
    total AS (
      SELECT ST_Area(ST_Transform(geom, 3857)) AS a FROM g
    ),
    res_poly AS (
      SELECT SUM(ST_Area(ST_Transform(ST_Intersection(p.way, g.geom), 3857))) AS a
      FROM planet_osm_polygon p, g
      WHERE ST_Intersects(p.way, g.geom)
        AND (
          p.landuse IN ('residential')
          OR p.building IN ('residential','apartments','house','detached','terrace','semidetached')
        )
    ),
    com_poly AS (
      SELECT SUM(ST_Area(ST_Transform(ST_Intersection(p.way, g.geom), 3857))) AS a
      FROM planet_osm_polygon p, g
      WHERE ST_Intersects(p.way, g.geom)
        AND (
          p.landuse IN ('commercial','retail')
          OR p.building IN ('retail','commercial','office','shop')
        )
    )
    SELECT COALESCE(res_poly.a,0) / NULLIF(total.a,0) AS res_share,
           COALESCE(com_poly.a,0) / NULLIF(total.a,0) AS com_share
    FROM total LEFT JOIN res_poly ON TRUE LEFT JOIN com_poly ON TRUE;
    """
)

_OVT_CLASSIFY_SQL = text(
    """
    WITH g AS (
      SELECT ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(:gj), 4326), 32638) AS geom
    ),
    total AS (
      SELECT ST_Area(geom) AS a FROM g
    ),
    res_poly AS (
      SELECT SUM(ST_Area(ST_Intersection(o.geom, g.geom))) AS a
      FROM overture_buildings o, g
      WHERE o.geom && g.geom
        AND ST_Intersects(o.geom, g.geom)
        AND (
          o.subtype = 'residential'
          OR o.class IN (
            'apartments',
            'apartment',
            'house',
            'detached_house',
            'semidetached_house',
            'terrace',
            'bungalow',
            'dormitory'
          )
        )
    ),
    nonres_poly AS (
      SELECT SUM(ST_Area(ST_Intersection(o.geom, g.geom))) AS a
      FROM overture_buildings o, g
      WHERE o.geom && g.geom
        AND ST_Intersects(o.geom, g.geom)
        AND (
          (o.subtype IS NOT NULL AND o.subtype <> 'residential')
          OR o.class IN (
            'commercial',
            'office',
            'retail',
            'industrial',
            'warehouse',
            'factory',
            'hotel',
            'hospital',
            'clinic',
            'school',
            'university',
            'supermarket',
            'mall'
          )
        )
    )
    SELECT
      COALESCE(res_poly.a,0) / NULLIF(total.a,0) AS res_share,
      COALESCE(nonres_poly.a,0) / NULLIF(total.a,0) AS com_share
    FROM total LEFT JOIN res_poly ON TRUE LEFT JOIN nonres_poly ON TRUE;
    """
)

_LANDUSE_BUFFER_SQL = text(
    """
    SELECT ST_AsGeoJSON(
        ST_Buffer(
            ST_SetSRID(ST_Point(:lng,:lat), 4326)::geography,
            :buffer_m
        )::geometry
    ) AS geom;
    """
)

_SUHAIL_LANDUSE_SQL = text(
    """
    SELECT landuse, classification
    FROM public.suhail_parcels_mat
    WHERE geom && ST_SetSRID(ST_Point(:lng,:lat), 4326)
      AND ST_Intersects(geom, ST_SetSRID(ST_Point(:lng,:lat), 4326))
    LIMIT 1;
    """
)


def _osm_fallback_code(geometry: dict, db) -> tuple[str | None, float, float, float]:
    if not geometry:
        return None, 0.0, 0.0, 0.0
    row = db.execute(_OSM_CLASSIFY_SQL, {"gj": json.dumps(geometry)}).mappings().first()
    if not row:
        return None, 0.0, 0.0, 0.0
    res = float(row["res_share"] or 0.0)
    com = float(row["com_share"] or 0.0)
    code: str | None = None
    conf = 0.0
    if res >= 0.70 and com <= 0.15:
        code = "s"
        conf = 0.85
    elif com >= 0.70:
        code = "m"
        conf = 0.85
    elif res >= 0.35 and com >= 0.25:
        code = "m"
        conf = 0.55
    elif max(res, com) >= 0.40:
        code = "s" if res >= com else "m"
        conf = 0.45
    return code, res, com, conf


def _ovt_overlay_code(geometry: dict, db) -> tuple[str | None, float, float, float]:
    if not geometry:
        return None, 0.0, 0.0, 0.0
    row = db.execute(_OVT_CLASSIFY_SQL, {"gj": json.dumps(geometry)}).mappings().first()
    if not row:
        return None, 0.0, 0.0, 0.0
    res = float(row["res_share"] or 0.0)
    com = float(row["com_share"] or 0.0)
    # NOTE: _OVT_CLASSIFY_SQL returns residential/commercial *shares of parcel area*
    # covered by Overture building footprints. Building footprints often cover only a
    # small fraction of the parcel, so infer land-use from ratios within building area
    # and use footprint coverage as a confidence multiplier.
    coverage = res + com
    if coverage <= 0.0:
        return None, res, com, 0.0
    if coverage < 0.01:
        return None, res, com, 0.0

    res_ratio = res / coverage
    com_ratio = com / coverage
    dominance = max(res_ratio, com_ratio)

    code: str | None = "s" if res_ratio >= com_ratio else "m"
    if min(res_ratio, com_ratio) >= 0.25 and coverage >= 0.03:
        code = "m"

    cov_score = min(coverage / 0.15, 1.0)
    conf = (0.45 + 0.55 * dominance) * (0.65 + 0.35 * cov_score)
    conf = min(conf, 0.95)

    if dominance < 0.60:
        conf *= 0.85
    return code, res, com, conf


def _pick_landuse(
    label_code: str | None,
    label_is_signal: bool,
    ovt_attr_code: str | None,
    ovt_attr_conf: float,
    osm_code: str | None,
    osm_res: float,
    osm_com: float,
    osm_conf: float,
    ovt_code: str | None,
    ovt_conf: float,
) -> tuple[str | None, str | None]:
    osm_strong = bool(
        osm_code
        and ((osm_res >= 0.70 and osm_com <= 0.15) or (osm_com >= 0.70))
    )
    if ovt_attr_code and not osm_strong:
        return ovt_attr_code, "overture_building_attr"
    if osm_strong:
        return osm_code, "osm_overlay"
    if ovt_code and ovt_conf >= 0.55:
        return ovt_code, "overture_overlay"
    if osm_code:
        return osm_code, "osm_overlay"
    return None, None


class ParcelQuery(BaseModel):
    """Request body for parcel lookups."""

    geometry: dict | str = Field(
        ...,
        description="GeoJSON Polygon or MultiPolygon in WGS84 (EPSG:4326).",
        json_schema_extra={
            "example": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [46.675, 24.713],
                        [46.676, 24.713],
                        [46.676, 24.714],
                        [46.675, 24.714],
                        [46.675, 24.713],
                    ]
                ],
            }
        },
    )
    where: str | None = Field(
        default="1=1",
        description='Optional SQL-like predicate understood by ArcGIS (default "1=1").',
    )


class ClassifyResponse(BaseModel):
    code: str | None
    residential_share: float
    commercial_share: float
    method: str = "osm_area_share"


class LanduseResponse(BaseModel):
    landuse_code: str | None = None
    landuse_method: str | None = None
    landuse_raw: str | None = None
    residential_share: float | None = None
    commercial_share: float | None = None
    residential_share_osm: float | None = None
    commercial_share_osm: float | None = None
    residential_share_ovt: float | None = None
    commercial_share_ovt: float | None = None
    osm_conf: float | None = None
    ovt_conf: float | None = None


class InferParcelResponse(BaseModel):
    found: bool
    parcel_id: str | None = None
    method: str | None = None
    area_m2: float | None = None
    perimeter_m: float | None = None
    geom: dict | None = None
    debug: dict | None = None


def _clamp_param(value: float | None, default: float, minimum: float, maximum: float) -> float:
    if value is None:
        return default
    return max(min(value, maximum), minimum)


@router.post("/osm-classify", response_model=ClassifyResponse)
def osm_classify(q: ParcelQuery, db: Session = Depends(get_db)):
    """
    Approximate land-use classification (s|m) using OSM overlays inside the input geometry.
    Requires osm2pgsql-imported tables (planet_osm_polygon) in EPSG:4326.
    """

    # Accept dict or JSON string
    gj = q.geometry
    if isinstance(gj, str):
        try:
            gj = json.loads(gj)
        except Exception as exc:  # pragma: no cover - validated by FastAPI in runtime
            raise HTTPException(status_code=400, detail=f"Invalid GeoJSON: {exc}")

    # Compute residential/commercial area shares from OSM landuse/building tags
    sql = text(
        """
        WITH g AS (
          SELECT ST_SetSRID(ST_GeomFromGeoJSON(:gj), 4326) AS geom
        ),
        total AS (
          SELECT ST_Area(ST_Transform(geom, 3857)) AS a FROM g
        ),
        res_poly AS (
          SELECT SUM(ST_Area(ST_Transform(ST_Intersection(p.way, g.geom), 3857))) AS a
          FROM planet_osm_polygon p, g
          WHERE ST_Intersects(p.way, g.geom)
            AND (
              p.landuse IN ('residential')
              OR p.building IN ('residential','apartments','house','detached','terrace','semidetached')
            )
        ),
        com_poly AS (
          SELECT SUM(ST_Area(ST_Transform(ST_Intersection(p.way, g.geom), 3857))) AS a
          FROM planet_osm_polygon p, g
          WHERE ST_Intersects(p.way, g.geom)
            AND (
              p.landuse IN ('commercial','retail')
              OR p.building IN ('retail','commercial','office','shop')
            )
        )
        SELECT
          COALESCE(res_poly.a,0) / NULLIF(total.a,0) AS res_share,
          COALESCE(com_poly.a,0) / NULLIF(total.a,0) AS com_share
        FROM total LEFT JOIN res_poly ON TRUE LEFT JOIN com_poly ON TRUE;
        """
    )
    row = db.execute(sql, {"gj": json.dumps(gj)}).mappings().first()
    if not row:
        return ClassifyResponse(code=None, residential_share=0.0, commercial_share=0.0)
    res = float(row["res_share"] or 0.0)
    com = float(row["com_share"] or 0.0)

    # Simple rule-of-thumb:
    #   - s if residential dominates and commercial is light
    #   - m if both present meaningfully or commercial dominates
    code: str | None = None
    if res >= 0.50 and com < 0.25:
        code = "s"
    elif (res >= 0.25 and com >= 0.25) or com >= 0.50:
        code = "m"

    return ClassifyResponse(code=code, residential_share=res, commercial_share=com)


@router.get(
    "/landuse",
    response_model=LanduseResponse,
    summary="Infer land-use classification near a point",
    description="Returns land-use inferred from Suhail zoning, OSM overlays, and Overture overlays near the point.",
)
def landuse(
    lng: float = Query(...),
    lat: float = Query(...),
    buffer_m: float | None = Query(None, ge=1.0, le=250.0),
    db: Session = Depends(get_db),
) -> LanduseResponse:
    buffer_val = buffer_m if buffer_m and buffer_m > 0 else 25.0

    try:
        row = db.execute(
            _LANDUSE_BUFFER_SQL,
            {"lng": lng, "lat": lat, "buffer_m": buffer_val},
        ).mappings().first()
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=500, detail=f"Landuse buffer failed: {exc}") from exc

    geom_raw = row.get("geom") if row else None
    geometry: dict | None = None
    if geom_raw:
        try:
            geometry = json.loads(geom_raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            geometry = None

    try:
        suhail = db.execute(_SUHAIL_LANDUSE_SQL, {"lng": lng, "lat": lat}).mappings().first()
    except SQLAlchemyError as exc:
        logger.warning("Suhail landuse lookup failed: %s", exc)
        suhail = None

    landuse_raw = ""
    if suhail:
        landuse_raw = str(suhail.get("landuse") or suhail.get("classification") or "")

    label_code = _landuse_code_from_label(landuse_raw)
    label_is_signal = _label_is_signal(landuse_raw, label_code)

    osm_code, osm_res, osm_com, osm_conf = _osm_fallback_code(geometry, db)
    ovt_code, ovt_res, ovt_com, ovt_conf = _ovt_overlay_code(geometry, db)

    landuse_code = None
    landuse_method = None
    if label_code and label_is_signal:
        landuse_code = label_code
        landuse_method = "suhail_overlay"
    else:
        landuse_code, landuse_method = _pick_landuse(
            label_code,
            label_is_signal,
            None,
            0.0,
            osm_code,
            osm_res,
            osm_com,
            osm_conf,
            ovt_code,
            ovt_conf,
        )

    residential_share = None
    commercial_share = None
    if landuse_method == "osm_overlay":
        residential_share = osm_res
        commercial_share = osm_com
    elif landuse_method == "overture_overlay":
        residential_share = ovt_res
        commercial_share = ovt_com

    return LanduseResponse(
        landuse_code=landuse_code,
        landuse_method=landuse_method,
        landuse_raw=landuse_raw or None,
        residential_share=residential_share,
        commercial_share=commercial_share,
        residential_share_osm=osm_res,
        commercial_share_osm=osm_com,
        residential_share_ovt=ovt_res,
        commercial_share_ovt=ovt_com,
        osm_conf=osm_conf,
        ovt_conf=ovt_conf,
    )


def _infer_parcel_postgis(
    lng: float,
    lat: float,
    building_id: int,
    part_index: int,
    radius_m: float,
    road_buf_m: float,
    k: int,
    db: Session,
) -> tuple[dict | None, dict | None]:
    try:
        row = (
            db.execute(
                _INFER_PARCEL_SQL,
                {
                    "lng": lng,
                    "lat": lat,
                    "building_id": building_id,
                    "part_index": part_index,
                    "radius_m": radius_m,
                    "road_buf_m": road_buf_m,
                    "k": k,
                },
            )
            .mappings()
            .first()
        )
    except SQLAlchemyError as exc:
        logger.warning("PostGIS infer-parcel query failed: %s", exc)
        return None, None

    if not row or not row.get("geom"):
        return None, None

    geom_raw = row.get("geom")
    geometry: dict | None = None
    if isinstance(geom_raw, str):
        try:
            geometry = json.loads(geom_raw)
        except Exception:
            geometry = None
    elif isinstance(geom_raw, dict):
        geometry = geom_raw

    if not geometry:
        return None, None

    debug: dict[str, float | int] = {}
    if row.get("block_area_m2") is not None:
        debug["block_area_m2"] = float(row["block_area_m2"])
    if row.get("neighbor_count") is not None:
        debug["neighbor_count"] = int(row["neighbor_count"])
    return {
        "geom": geometry,
        "area_m2": float(row.get("area_m2") or 0.0),
        "perimeter_m": float(row.get("perimeter_m") or 0.0),
    }, debug or None


def _infer_parcel_fallback(
    building_id: int, part_index: int, db: Session
) -> dict | None:
    try:
        row = (
            db.execute(
                _INFER_PARCEL_FALLBACK_SQL,
                {
                    "building_id": building_id,
                    "part_index": part_index,
                    "pad_m": _DEFAULT_PARCEL_PAD_M,
                },
            )
            .mappings()
            .first()
        )
    except SQLAlchemyError as exc:
        logger.warning("PostGIS infer-parcel fallback query failed: %s", exc)
        return None

    if not row or not row.get("geom"):
        return None

    geom_raw = row.get("geom")
    geometry: dict | None = None
    if isinstance(geom_raw, str):
        try:
            geometry = json.loads(geom_raw)
        except Exception:
            geometry = None
    elif isinstance(geom_raw, dict):
        geometry = geom_raw

    if not geometry:
        return None

    return {
        "geom": geometry,
        "area_m2": float(row.get("area_m2") or 0.0),
        "perimeter_m": float(row.get("perimeter_m") or 0.0),
    }


@router.get(
    "/infer-parcel",
    response_model=InferParcelResponse,
    summary="Infer a parcel around a selected MS building footprint",
)
def infer_parcel(
    lng: float = Query(...),
    lat: float = Query(...),
    building_id: int = Query(..., ge=1),
    part_index: int = Query(..., ge=1),
    radius_m: float | None = Query(None),
    road_buf_m: float | None = Query(None),
    k: int | None = Query(None),
    db: Session = Depends(get_db),
) -> InferParcelResponse:
    radius_val = _clamp_param(radius_m, 250.0, 50.0, 500.0)
    road_buf_val = _clamp_param(road_buf_m, 9.0, 4.0, 16.0)
    k_val = int(_clamp_param(float(k) if k is not None else None, 25.0, 5.0, 80.0))

    parcel_id = f"ms:{building_id}:{part_index}"

    result = None
    debug = None
    method = "road_block_voronoi_v1"
    if _has_osm_roads(db):
        result, debug = _infer_parcel_postgis(
            lng, lat, building_id, part_index, radius_val, road_buf_val, k_val, db
        )
    if not result:
        result = _infer_parcel_fallback(building_id, part_index, db)
        method = "envelope_fallback_v1"

    if not result:
        return InferParcelResponse(found=False, parcel_id=parcel_id, method=method)

    return InferParcelResponse(
        found=True,
        parcel_id=parcel_id,
        method=method,
        area_m2=result.get("area_m2"),
        perimeter_m=result.get("perimeter_m"),
        geom=result.get("geom"),
        debug=debug,
    )


class IdentifyPoint(BaseModel):
    lng: float
    lat: float
    tol_m: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Optional identify tolerance in metres (defaults to 5m).",
    )


class CollateParcelsRequest(BaseModel):
    parcel_ids: list[str] = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Parcel IDs to collate/union into a single site geometry (manual multi-select).",
    )


def _collate_postgis(parcel_ids: list[str], db: Session) -> Optional[Dict[str, Any]]:
    # De-dupe while preserving order
    ids_raw = [str(pid).strip() for pid in (parcel_ids or []) if str(pid).strip()]
    seen: set[str] = set()
    ids: list[str] = []
    for pid in ids_raw:
        if pid in seen:
            continue
        seen.add(pid)
        ids.append(pid)

    if not ids:
        return {
            "found": False,
            "source": "postgis",
            "message": "No parcel_ids provided.",
            "parcel_ids": [],
            "missing_ids": [],
        }

    parcel_json: str | None = None
    if _IS_MS_BUILDINGS_TABLE:
        payload = _build_ms_building_parcel_payload(ids)
        if not payload:
            return {
                "found": False,
                "source": "postgis",
                "message": "No valid parcel_ids provided.",
                "parcel_ids": [],
                "missing_ids": ids,
            }
        parcel_json = json.dumps(payload)

    try:
        params = {"ids": ids} if not _IS_MS_BUILDINGS_TABLE else {"parcel_json": parcel_json}
        rows = db.execute(_COLLATE_META_SQL, params).mappings().all()
    except SQLAlchemyError as exc:
        logger.warning("PostGIS collate meta query failed: %s", exc)
        return None

    found_set = {str(r.get("id")) for r in rows if r.get("id") is not None}
    found_ids: list[str] = [pid for pid in ids if pid in found_set]  # preserve input order
    missing_ids: list[str] = [pid for pid in ids if pid not in found_set]

    if not rows or not found_ids:
        return {
            "found": False,
            "source": "postgis",
            "message": "No matching parcels found for provided parcel_ids.",
            "parcel_ids": [],
            "missing_ids": ids,
        }

    # Pick the "dominant" label/classification as the largest parcel by area
    dominant = max(rows, key=lambda r: int(r.get("area_m2") or 0))
    landuse_raw = dominant.get("landuse") or ""
    classification_raw = dominant.get("classification")

    component_area_m2_sum = sum(int(r.get("area_m2") or 0) for r in rows)

    try:
        union_params = (
            {"ids": found_ids}
            if not _IS_MS_BUILDINGS_TABLE
            else {"parcel_json": parcel_json}
        )
        urow = db.execute(_COLLATE_UNION_SQL, union_params).mappings().first()
    except SQLAlchemyError as exc:
        logger.warning("PostGIS collate union query failed: %s", exc)
        return None

    if not urow or not urow.get("geom"):
        return {
            "found": False,
            "source": "postgis",
            "message": "Union geometry could not be computed.",
            "parcel_ids": found_ids,
            "missing_ids": missing_ids,
        }

    geom_val = urow.get("geom")
    geometry: dict | None = None
    if isinstance(geom_val, str):
        try:
            geometry = json.loads(geom_val)
        except Exception:
            geometry = None
    elif isinstance(geom_val, dict):
        geometry = geom_val

    if not geometry:
        return {
            "found": False,
            "source": "postgis",
            "message": "Union geometry could not be parsed.",
            "parcel_ids": found_ids,
            "missing_ids": missing_ids,
        }

    # Reuse same landuse selection logic as /identify
    label_code = _landuse_code_from_label(str(landuse_raw))
    label_is_signal = _label_is_signal(str(landuse_raw), label_code)

    ovt_attr_code: str | None = None
    ovt_attr_conf = 0.0
    # Only attempt overture building attribute lookup when it's a single Overture "parcel"
    if classification_raw == "overture_building" and len(found_ids) == 1:
        parcel_id = found_ids[0]
        ovt_id = parcel_id[4:] if parcel_id.startswith("ovt:") else parcel_id
        try:
            record = (
                db.execute(
                    text("SELECT subtype, class FROM overture_buildings WHERE id=:id"),
                    {"id": ovt_id},
                )
                .mappings()
                .first()
            )
        except Exception as exc:
            logger.warning("Overture building attribute lookup failed: %s", exc)
            record = None

        if record:
            ovt_attr_code = _landuse_code_from_label(str(record.get("subtype") or ""))
            if not ovt_attr_code:
                ovt_attr_code = _landuse_code_from_label(str(record.get("class") or ""))
            if ovt_attr_code:
                landuse_raw = record.get("subtype") or record.get("class") or landuse_raw
                ovt_attr_conf = 0.90

    osm_code, osm_res, osm_com, osm_conf = _osm_fallback_code(geometry, db)
    ovt_code, ovt_res, ovt_com, ovt_conf = _ovt_overlay_code(geometry, db)

    landuse_code, landuse_method = _pick_landuse(
        label_code,
        label_is_signal,
        ovt_attr_code,
        ovt_attr_conf,
        osm_code,
        osm_res,
        osm_com,
        osm_conf,
        ovt_code,
        ovt_conf,
    )

    residential_share = None
    commercial_share = None
    if landuse_method == "osm_overlay":
        residential_share = osm_res
        commercial_share = osm_com
    elif landuse_method == "overture_overlay":
        residential_share = ovt_res
        commercial_share = ovt_com

    # Stable (deterministic) collated id for UX/caching
    collated_id = hashlib.sha1("|".join(sorted(found_ids)).encode("utf-8")).hexdigest()[:12]

    parcel = {
        "parcel_id": f"collated:{collated_id}",
        "geometry": geometry,
        "area_m2": urow.get("area_m2"),
        "perimeter_m": urow.get("perimeter_m"),
        "landuse_raw": landuse_raw,
        "classification_raw": classification_raw,
        "landuse_code": landuse_code,
        "landuse_method": landuse_method,
        "residential_share": residential_share,
        "commercial_share": commercial_share,
        "residential_share_osm": osm_res,
        "commercial_share_osm": osm_com,
        "residential_share_ovt": ovt_res,
        "commercial_share_ovt": ovt_com,
        "ovt_attr_conf": ovt_attr_conf,
        "osm_conf": osm_conf,
        "ovt_conf": ovt_conf,
        "source_url": f"postgis/{_PARCEL_TABLE}",
        "component_count": len(found_ids),
        "component_area_m2_sum": component_area_m2_sum,
    }

    return {
        "found": True,
        "source": "postgis",
        "parcel_ids": found_ids,
        "missing_ids": missing_ids,
        "parcel": parcel,
    }


def _identify_postgis(lng: float, lat: float, tol_m: float, db: Session) -> Optional[Dict[str, Any]]:
    params = {"lng": lng, "lat": lat, "srid": _TARGET_SRID, "tol_m": tol_m}
    try:
        row = db.execute(_IDENTIFY_SQL, params).mappings().first()
    except SQLAlchemyError as exc:
        logger.warning("PostGIS identify query failed: %s", exc)
        return None

    if not row:
        return {
            "found": False,
            "tolerance_m": tol_m,
            "source": "postgis",
            "message": "No parcels table/row returned.",
        }

    geom_json = row.get("geom")
    geometry: Dict[str, Any] | None = None
    if geom_json:
        try:
            geometry = json.loads(geom_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            geometry = None

    landuse_raw = row.get("landuse") or row.get("classification") or ""
    label_code = _landuse_code_from_label(str(landuse_raw))
    label_is_signal = _label_is_signal(str(landuse_raw), label_code)
    ovt_attr_code: str | None = None
    ovt_attr_conf = 0.0
    if row.get("classification") == "overture_building":
        parcel_id = row.get("id") or ""
        ovt_id = parcel_id[4:] if parcel_id.startswith("ovt:") else parcel_id
        try:
            record = (
                db.execute(
                    text(
                        "SELECT subtype, class FROM overture_buildings WHERE id=:id"
                    ),
                    {"id": ovt_id},
                )
                .mappings()
                .first()
            )
        except SQLAlchemyError as exc:
            logger.warning("Overture building attribute lookup failed: %s", exc)
            record = None

        if record:
            ovt_attr_code = _landuse_code_from_label(str(record.get("subtype") or ""))
            if not ovt_attr_code:
                ovt_attr_code = _landuse_code_from_label(str(record.get("class") or ""))

            if ovt_attr_code:
                landuse_raw = record.get("subtype") or record.get("class") or landuse_raw
                ovt_attr_conf = 0.90

    osm_code = None
    ovt_code = None
    osm_res = osm_com = ovt_res = ovt_com = 0.0
    osm_conf = ovt_conf = 0.0
    try:
        osm_code, osm_res, osm_com, osm_conf = _osm_fallback_code(geometry, db)
    except Exception as exc:
        logger.warning("OSM overlay classification failed: %s", exc)
    try:
        ovt_code, ovt_res, ovt_com, ovt_conf = _ovt_overlay_code(geometry, db)
    except Exception as exc:
        logger.warning("Overture overlay classification failed: %s", exc)

    landuse_code, landuse_method = _pick_landuse(
        label_code,
        label_is_signal,
        ovt_attr_code,
        ovt_attr_conf,
        osm_code,
        osm_res,
        osm_com,
        osm_conf,
        ovt_code,
        ovt_conf,
    )

    residential_share = None
    commercial_share = None
    if landuse_method == "osm_overlay":
        residential_share = osm_res
        commercial_share = osm_com
    elif landuse_method == "overture_overlay":
        residential_share = ovt_res
        commercial_share = ovt_com

    parcel = {
        "parcel_id": row.get("id"),
        "geometry": geometry,
        "area_m2": row.get("area_m2"),
        "perimeter_m": row.get("perimeter_m"),
        "site_area_m2": row.get("site_area_m2"),
        "footprint_area_m2": row.get("footprint_area_m2"),
        "building_count": row.get("building_count"),
        "landuse_raw": landuse_raw,
        "classification_raw": row.get("classification"),
        "landuse_code": landuse_code,
        "landuse_method": landuse_method,
        "residential_share": residential_share,
        "commercial_share": commercial_share,
        "residential_share_osm": osm_res,
        "commercial_share_osm": osm_com,
        "residential_share_ovt": ovt_res,
        "commercial_share_ovt": ovt_com,
        "ovt_attr_conf": ovt_attr_conf,
        "osm_conf": osm_conf,
        "ovt_conf": ovt_conf,
        "source_url": f"postgis/{_PARCEL_TABLE}",
    }

    logger.debug(
        "Identify point %.6f, %.6f (tol=%.1fm, srid=%s)",
        lng,
        lat,
        tol_m,
        _TARGET_SRID,
    )

    return {
        "found": True,
        "tolerance_m": tol_m,
        "source": "postgis",
        "parcel": parcel,
    }


def _identify_arcgis(lng: float, lat: float, tol_m: float, db: Session) -> Dict[str, Any]:
    # Disabled by policy: no fallbacks
    return {
        "found": False,
        "tolerance_m": tol_m,
        "source": "disabled",
        "message": "fallbacks disabled",
    }


@router.post("/identify")
def identify_post(pt: IdentifyPoint, db: Session = Depends(get_db)):
    tol = pt.tol_m if pt.tol_m is not None and pt.tol_m > 0 else _DEFAULT_TOLERANCE

    postgis_result = _identify_postgis(pt.lng, pt.lat, tol, db)
    if postgis_result is None:
        # If PostGIS is misconfigured, return an explicit server error
        raise HTTPException(
            status_code=500,
            detail="PostGIS identify unavailable (check parcels table, geom column, and SRID config).",
        )
    return postgis_result


@router.post(
    "/collate",
    summary="Union multiple parcels into a single site geometry",
    description="Takes manually multi-selected parcel IDs and returns a single unioned GeoJSON geometry + area/perimeter + landuse classification.",
)
def collate_parcels(req: CollateParcelsRequest, db: Session = Depends(get_db)):
    ids = req.parcel_ids or []
    if len(ids) > 200:
        raise HTTPException(status_code=400, detail="Too many parcel_ids (max 200).")

    postgis_result = _collate_postgis(ids, db)
    if postgis_result is None:
        raise HTTPException(
            status_code=500,
            detail="PostGIS collate unavailable (check parcels table, geom column, and SRID config).",
        )
    return postgis_result


@router.get("/identify")
def identify_get(
    lng: float = Query(...),
    lat: float = Query(...),
    tol_m: float | None = Query(None, ge=0.0),
    db: Session = Depends(get_db),
):
    tol = tol_m if tol_m is not None and tol_m > 0 else _DEFAULT_TOLERANCE
    postgis_result = _identify_postgis(lng, lat, tol, db)
    if postgis_result is None:
        raise HTTPException(
            status_code=500,
            detail="PostGIS identify unavailable (check parcels table, geom column, and SRID config).",
        )
    return postgis_result


@router.post("/parcels")
def parcels(q: ParcelQuery, db: Session = Depends(get_db)):
    """Query parcels intersecting the provided geometry."""

    base = getattr(settings, "ARCGIS_BASE_URL", None)
    layer = getattr(settings, "ARCGIS_PARCEL_LAYER", None)
    token = getattr(settings, "ARCGIS_TOKEN", None)
    # ArcGIS is optional in this deployment. If it's not configured, return an
    # empty, non-error payload so the UI doesn't spam an alert.
    if not (base and isinstance(layer, int)):
        return {"items": [], "configured": False, "source": "arcgis"}

    # Accept either a dict or a JSON string for geometry
    geometry = q.geometry
    if isinstance(geometry, str):
        try:
            geometry = json.loads(geometry)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid GeoJSON string: {exc}")

    feats = query_features(base, layer, geometry, where=q.where or "1=1", token=token)
    items = []
    for feature in feats:
        props = feature.get("properties") or {}
        items.append(
            {
                "parcel_id": props.get("PARCEL_ID")
                or props.get("parcel_id")
                or props.get("id"),
                "municipality": props.get("MUNICIPALITY")
                or props.get("municipality"),
                "district": props.get("DISTRICT") or props.get("district"),
                "zoning": props.get("ZONING")
                or props.get("landuse")
                or props.get("zone"),
                "far": props.get("FAR") or props.get("far"),
                "frontage_m": props.get("FRONTAGE") or props.get("frontage"),
                "road_class": props.get("ROAD_CLASS") or props.get("road_class"),
                "setbacks": None,
                "source_url": base,
            }
        )

    for item in items:
        parcel_id = str(item.get("parcel_id") or "")
        if not parcel_id:
            continue

        existing = db.get(Parcel, parcel_id)
        if not existing:
            db.add(
                Parcel(
                    id=parcel_id,
                    gis_polygon=geometry,
                    municipality=item["municipality"],
                    district=item["district"],
                    zoning=item["zoning"],
                    far=item["far"],
                    frontage_m=item["frontage_m"],
                    road_class=item["road_class"],
                    setbacks=None,
                    source_url=base,
                )
            )

    try:
        db.commit()
    except Exception:
        db.rollback()

    return {"items": items}
