"""Endpoints for parcel queries via external GIS."""

import json
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.connectors.arcgis import query_features
from app.core.config import settings
from app.db.deps import get_db
from app.models.tables import Parcel
from app.services.geo import _landuse_code_from_label

logger = logging.getLogger(__name__)

Transformer = None  # no local transform; we use ST_Transform on the server


def _safe_identifier(value: str | None, fallback: str) -> str:
    candidate = (value or "").strip()
    if not candidate:
        return fallback
    if all(ch.isalnum() or ch in {"_", "."} for ch in candidate):
        return candidate
    logger.warning("Unsafe identifier %s; falling back to %s", value, fallback)
    return fallback


_TARGET_SRID = getattr(settings, "PARCEL_TARGET_SRID", 32638)
_DEFAULT_TOLERANCE = getattr(settings, "PARCEL_IDENTIFY_TOLERANCE_M", 25.0) or 25.0
if _DEFAULT_TOLERANCE <= 0:
    _DEFAULT_TOLERANCE = 25.0

_PARCEL_TABLE = _safe_identifier(getattr(settings, "PARCEL_IDENTIFY_TABLE", "parcels"), "parcels")
_PARCEL_GEOM_COLUMN = _safe_identifier(
    getattr(settings, "PARCEL_IDENTIFY_GEOM_COLUMN", "geom"), "geom"
)

_TRANSFORMER = None  # unused (server-side transform)

_IDENTIFY_SQL = text(
    f"""
  WITH q AS (
    SELECT ST_Transform(ST_SetSRID(ST_Point(:lng,:lat), 4326), :srid) AS pt
  ),
  scored AS (
    SELECT
      id,
      landuse,
      classification,
      {_PARCEL_GEOM_COLUMN} AS geom,
      ST_Area({_PARCEL_GEOM_COLUMN})::bigint      AS area_m2,
      ST_Perimeter({_PARCEL_GEOM_COLUMN})::bigint AS perimeter_m,
      ST_Distance({_PARCEL_GEOM_COLUMN}, q.pt)    AS distance_m,
      CASE WHEN ST_Intersects({_PARCEL_GEOM_COLUMN}, q.pt) THEN 1 ELSE 0 END AS hits,
      CASE WHEN ST_DWithin({_PARCEL_GEOM_COLUMN}, q.pt, :tol_m) THEN 1 ELSE 0 END AS near,
      CASE WHEN classification = 'overture_building' THEN 1 ELSE 0 END AS is_ovt
    FROM {_PARCEL_TABLE}, q
  )
  SELECT
    id,
    landuse,
    classification,
    area_m2,
    perimeter_m,
    ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geom,
    distance_m,
    hits,
    near,
    is_ovt
  FROM scored
  ORDER BY
    CASE WHEN hits = 1 THEN 3 WHEN near = 1 THEN 2 ELSE 1 END DESC,
    is_ovt DESC,
    area_m2 ASC,
    distance_m ASC
  LIMIT 1;
  """
)

# Keep router local to "geo"; main.py mounts routers at "/v1".
router = APIRouter(prefix="/geo", tags=["geo"])


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
            'dwelling_house',
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
            'hospital',
            'school',
            'university',
            'hotel',
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


def _osm_fallback_code(geometry: dict, db) -> tuple[str | None, float, float]:
    if not geometry:
        return None, 0.0, 0.0
    row = db.execute(_OSM_CLASSIFY_SQL, {"gj": json.dumps(geometry)}).mappings().first()
    if not row:
        return None, 0.0, 0.0
    res = float(row["res_share"] or 0.0)
    com = float(row["com_share"] or 0.0)
    code: str | None = None
    if res >= 0.50 and com < 0.25:
        code = "s"
    elif (res >= 0.25 and com >= 0.25) or com >= 0.50:
        code = "m"
    return code, res, com


def _overture_fallback_code(geometry: dict, db) -> tuple[str | None, float, float]:
    if not geometry:
        return None, 0.0, 0.0
    row = db.execute(_OVT_CLASSIFY_SQL, {"gj": json.dumps(geometry)}).mappings().first()
    if not row:
        return None, 0.0, 0.0
    res = float(row["res_share"] or 0.0)
    com = float(row["com_share"] or 0.0)
    code: str | None = None
    if res >= 0.50 and com < 0.25:
        code = "s"
    elif (res >= 0.25 and com >= 0.25) or com >= 0.50:
        code = "m"
    return code, res, com


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


class IdentifyPoint(BaseModel):
    lng: float
    lat: float
    tol_m: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Optional identify tolerance in metres (defaults to 5m).",
    )


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
    parcel = {
        "parcel_id": row.get("id"),
        "geometry": geometry,
        "area_m2": row.get("area_m2"),
        "perimeter_m": row.get("perimeter_m"),
        "landuse_raw": landuse_raw,
        "classification_raw": row.get("classification"),
        "landuse_code": _landuse_code_from_label(str(landuse_raw)),
        "landuse_method": "label",
        "residential_share": None,
        "commercial_share": None,
        "source_url": f"postgis/{_PARCEL_TABLE}",
    }
    if parcel["landuse_code"] is None and parcel["classification_raw"] == "overture_building":
        parcel_id = parcel.get("parcel_id") or ""
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
            code = _landuse_code_from_label(str(record.get("subtype") or ""))
            if not code:
                code = _landuse_code_from_label(str(record.get("class") or ""))

            if code:
                parcel["landuse_code"] = code
                parcel["landuse_method"] = "overture_building_attr"
                parcel["landuse_raw"] = (
                    record.get("subtype") or record.get("class") or parcel["landuse_raw"]
                )

    logger.debug(
        "Identify point %.6f, %.6f (tol=%.1fm, srid=%s)",
        lng,
        lat,
        tol_m,
        _TARGET_SRID,
    )

    # If ambiguous (None or boolean-like), fall back to overlays
    raw_l = (str(landuse_raw) or "").strip().lower()
    if (parcel["landuse_code"] is None) or (raw_l in {"", "yes", "true", "1", "y"}):
        try:
            code, rsh, csh = _overture_fallback_code(geometry, db)
            if code:
                parcel["landuse_code"] = code
                parcel["landuse_method"] = "overture_overlay"
                parcel["residential_share"] = rsh
                parcel["commercial_share"] = csh
            else:
                code, rsh, csh = _osm_fallback_code(geometry, db)
                if code:
                    parcel["landuse_code"] = code
                    parcel["landuse_method"] = "osm_overlay"
                    parcel["residential_share"] = rsh
                    parcel["commercial_share"] = csh
        except Exception:
            pass

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
            detail="PostGIS identify unavailable (check parcels table & SRID)",
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
            detail="PostGIS identify unavailable (check parcels table & SRID)",
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
