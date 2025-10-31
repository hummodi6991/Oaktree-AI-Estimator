"""Endpoints for parcel queries via external GIS."""

import json
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from shapely.geometry import shape as shapely_shape

from app.connectors.arcgis import query_features
from app.core.config import settings
from app.db.deps import get_db
from app.models.tables import Parcel
from app.services.geo import area_m2, to_geojson, _landuse_code_from_label

logger = logging.getLogger(__name__)

try:  # pragma: no cover - exercised indirectly in runtime
    from pyproj import Transformer
except ImportError:  # pragma: no cover - keeps runtime resilient if optional dep missing
    Transformer = None  # type: ignore[assignment]
    logger.warning(
        "pyproj is not installed; parcel identify will fall back to ArcGIS/external features."
    )


def _safe_identifier(value: str | None, fallback: str) -> str:
    candidate = (value or "").strip()
    if not candidate:
        return fallback
    if all(ch.isalnum() or ch in {"_", "."} for ch in candidate):
        return candidate
    logger.warning("Unsafe identifier %s; falling back to %s", value, fallback)
    return fallback


_TARGET_SRID = getattr(settings, "PARCEL_TARGET_SRID", 32638)
_DEFAULT_TOLERANCE = getattr(settings, "PARCEL_IDENTIFY_TOLERANCE_M", 5.0) or 5.0
if _DEFAULT_TOLERANCE <= 0:
    _DEFAULT_TOLERANCE = 5.0

_PARCEL_TABLE = _safe_identifier(getattr(settings, "PARCEL_IDENTIFY_TABLE", "parcels"), "parcels")
_PARCEL_GEOM_COLUMN = _safe_identifier(
    getattr(settings, "PARCEL_IDENTIFY_GEOM_COLUMN", "geom"), "geom"
)

_TRANSFORMER = None
if Transformer is not None:
    try:
        _TRANSFORMER = Transformer.from_crs(4326, _TARGET_SRID, always_xy=True)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning("Failed to initialise coordinate transformer: %s", exc)
        _TRANSFORMER = None

_IDENTIFY_SQL = text(
    f"""
    SELECT
        id,
        landuse,
        classification,
        ST_Area({_PARCEL_GEOM_COLUMN})::bigint      AS area_m2,
        ST_Perimeter({_PARCEL_GEOM_COLUMN})::bigint AS perimeter_m,
        ST_AsGeoJSON({_PARCEL_GEOM_COLUMN})         AS geom
    FROM {_PARCEL_TABLE}
    WHERE ST_DWithin(
        {_PARCEL_GEOM_COLUMN},
        ST_SetSRID(ST_Point(:x, :y), :srid),
        :tol
    )
    ORDER BY ST_Distance(
        {_PARCEL_GEOM_COLUMN},
        ST_SetSRID(ST_Point(:x, :y), :srid)
    )
    LIMIT 1;
    """
)

# Keep router local to "geo"; main.py mounts routers at "/v1".
router = APIRouter(prefix="/geo", tags=["geo"])


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


class IdentifyPoint(BaseModel):
    lng: float
    lat: float
    tol_m: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Optional identify tolerance in metres (defaults to 5m).",
    )


def _identify_postgis(lng: float, lat: float, tol_m: float, db: Session) -> Optional[Dict[str, Any]]:
    if _TRANSFORMER is None:
        return None
    try:
        x, y = _TRANSFORMER.transform(lng, lat)
    except Exception as exc:  # pragma: no cover - transformation rarely fails
        logger.warning("Failed to transform identify point: %s", exc)
        return None

    params = {"x": x, "y": y, "tol": tol_m, "srid": _TARGET_SRID}
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
            "message": "No parcel matched within tolerance.",
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
        "source_url": f"postgis/{_PARCEL_TABLE}",
    }

    logger.debug(
        "Identify point %.6f, %.6f transformed to %.2f, %.2f (tol=%.1fm)",
        lng,
        lat,
        x,
        y,
        tol_m,
    )

    return {
        "found": True,
        "tolerance_m": tol_m,
        "source": "postgis",
        "parcel": parcel,
    }


def _identify_arcgis(lng: float, lat: float, tol_m: float, db: Session) -> Dict[str, Any]:
    base = getattr(settings, "ARCGIS_BASE_URL", None)
    layer = getattr(settings, "ARCGIS_PARCEL_LAYER", None)
    token = getattr(settings, "ARCGIS_TOKEN", None)

    feats: list[dict[str, Any]] = []
    if base and isinstance(layer, int):
        point = {"x": lng, "y": lat, "spatialReference": {"wkid": 4326}}
        feats = query_features(
            base,
            layer,
            point,
            where="1=1",
            token=token,
            geometry_type="esriGeometryPoint",
            in_sr=4326,
            distance=tol_m,
            units="esriSRUnit_Meter",
        )
    else:
        from app.models.tables import ExternalFeature

        buf = 0.00003
        poly = {
            "type": "Polygon",
            "coordinates": [[
                [lng - buf, lat - buf],
                [lng + buf, lat - buf],
                [lng + buf, lat + buf],
                [lng - buf, lat + buf],
                [lng - buf, lat - buf],
            ]],
        }
        rows = (
            db.query(ExternalFeature)
            .filter(ExternalFeature.layer_name == "rydpolygons")
            .all()
        )
        p = shapely_shape(poly)
        for r in rows:
            try:
                g = shapely_shape(r.geometry)
                if g.contains(p):
                    feats.append({"geometry": to_geojson(g), "properties": r.properties or {}})
            except Exception:
                continue

    if not feats:
        return {
            "found": False,
            "tolerance_m": tol_m,
            "source": "arcgis" if base else "external_feature/rydpolygons",
            "message": "No parcel found at this location.",
        }

    f = feats[0]
    props = {(k or "").lower(): v for k, v in (f.get("properties") or {}).items()}
    landuse_raw = (
        props.get("landuse")
        or props.get("classification")
        or props.get("land_use")
        or ""
    )
    code = _landuse_code_from_label(str(landuse_raw))

    gj = f.get("geometry")
    try:
        a = area_m2(shapely_shape(gj))
    except Exception:
        a = 0.0

    parcel = {
        "parcel_id": props.get("parcel_id")
        or props.get("id")
        or props.get("parcelid"),
        "geometry": gj,
        "area_m2": a,
        "perimeter_m": None,
        "landuse_raw": landuse_raw,
        "classification_raw": props.get("classification"),
        "landuse_code": code,
        "source_url": base or "external_feature/rydpolygons",
    }

    return {
        "found": True,
        "tolerance_m": tol_m,
        "source": "arcgis" if base else "external_feature/rydpolygons",
        "parcel": parcel,
    }


@router.post("/identify")
def identify(pt: IdentifyPoint, db: Session = Depends(get_db)):
    tol = pt.tol_m if pt.tol_m is not None and pt.tol_m > 0 else _DEFAULT_TOLERANCE

    postgis_result = _identify_postgis(pt.lng, pt.lat, tol, db)
    if postgis_result is not None:
        if postgis_result.get("found"):
            return postgis_result
        fallback = _identify_arcgis(pt.lng, pt.lat, tol, db)
        if fallback.get("found"):
            return fallback
        return postgis_result

    return _identify_arcgis(pt.lng, pt.lat, tol, db)


@router.post("/parcels")
def parcels(q: ParcelQuery, db: Session = Depends(get_db)):
    """Query parcels intersecting the provided geometry."""

    base = getattr(settings, "ARCGIS_BASE_URL", None)
    layer = getattr(settings, "ARCGIS_PARCEL_LAYER", None)
    token = getattr(settings, "ARCGIS_TOKEN", None)
    if not (base and isinstance(layer, int)):
        raise HTTPException(status_code=500, detail="ArcGIS not configured")

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
