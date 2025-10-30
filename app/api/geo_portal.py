"""Endpoints for parcel queries via external GIS."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
import json
from shapely.geometry import shape as shapely_shape

from app.connectors.arcgis import query_features
from app.core.config import settings
from app.db.deps import get_db
from app.models.tables import Parcel
from app.services.geo import area_m2, to_geojson, _landuse_code_from_label

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


@router.post("/identify")
def identify(pt: IdentifyPoint, db: Session = Depends(get_db)):
    base = getattr(settings, "ARCGIS_BASE_URL", None)
    layer = getattr(settings, "ARCGIS_PARCEL_LAYER", None)
    token = getattr(settings, "ARCGIS_TOKEN", None)

    buf = 0.00003
    poly = {
        "type": "Polygon",
        "coordinates": [[
            [pt.lng - buf, pt.lat - buf],
            [pt.lng + buf, pt.lat - buf],
            [pt.lng + buf, pt.lat + buf],
            [pt.lng - buf, pt.lat + buf],
            [pt.lng - buf, pt.lat - buf],
        ]],
    }

    feats = []
    if base and isinstance(layer, int):
        feats = query_features(base, layer, poly, where="1=1", token=token)
    else:
        from app.models.tables import ExternalFeature

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
        raise HTTPException(status_code=404, detail="No parcel at this point")

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

    return {
        "items": [
            {
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
        ]
    }


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
