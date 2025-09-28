"""Endpoints for parcel queries via external GIS."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.connectors.arcgis import query_features
from app.core.config import settings
from app.db.deps import get_db
from app.models.tables import Parcel

router = APIRouter(prefix="/v1/geo", tags=["geo"])


class ParcelQuery(BaseModel):
    """Request body for parcel lookups."""

    geometry: dict
    where: str | None = None


@router.post("/parcels")
def parcels(q: ParcelQuery, db: Session = Depends(get_db)):
    """Query parcels intersecting the provided geometry."""

    base = getattr(settings, "ARCGIS_BASE_URL", None)
    layer = getattr(settings, "ARCGIS_PARCEL_LAYER", None)
    token = getattr(settings, "ARCGIS_TOKEN", None)
    if not (base and isinstance(layer, int)):
        raise HTTPException(status_code=500, detail="ArcGIS not configured")

    feats = query_features(base, layer, q.geometry, where=q.where or "1=1", token=token)
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
                    gis_polygon=q.geometry,
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
