from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.db.deps import get_db
from app.services.district_resolver import resolution_meta
from app.services.land_price_engine import quote_land_price_blended_v1
from app.services.pricing import price_from_kaggle_hedonic, price_from_suhail, store_quote

router = APIRouter(prefix="/pricing", tags=["pricing"])


@router.get("/land")
def land_price(
    city: str = Query(...),
    district: str | None = Query(default=None),
    provider: str = Query(default="blended_v1", description="Provider label (blended_v1, kaggle_hedonic_v0, suhail)."),
    parcel_id: str | None = Query(default=None),
    lng: float | None = Query(default=None, description="Centroid longitude (WGS84)"),
    lat: float | None = Query(default=None, description="Centroid latitude (WGS84)"),
    db: Session = Depends(get_db),
):
    provider_key = (provider or "").lower()
    meta = {}
    method = provider_key
    value = None
    district_resolution = None

    if provider_key == "blended_v1":
        quote = quote_land_price_blended_v1(
            db,
            city=city,
            district=district,
            lon=lng,
            lat=lat,
            geom_geojson=None,
        )
        value = quote.get("value")
        method = quote.get("method") or "blended_v1"
        meta = quote.get("meta") or {}
        district_resolution = quote.get("district_resolution")
        district = quote.get("district_norm") or quote.get("district_raw") or district
    elif provider_key == "suhail":
        value, method, resolution = price_from_suhail(
            db,
            city=city,
            district=district,
            geom_geojson=None,
            lon=lng,
            lat=lat,
        )
        if value is None:
            raise HTTPException(
                status_code=404,
                detail="No land price estimate available for this location.",
            )
        district_norm = resolution.district_norm
        meta = {
            "source": "suhail_land_metrics",
            "district_norm": district_norm,
            "district_resolution": resolution_meta(resolution),
        }
        district_resolution = resolution_meta(resolution)
        district = district_norm or district
    else:
        value, method, meta = price_from_kaggle_hedonic(
            db,
            city=city,
            lon=lng,
            lat=lat,
            district=district,
        )
        district_resolution = meta.get("district_resolution") if isinstance(meta, dict) else None

    if value is None:
        raise HTTPException(
            status_code=404,
            detail="No land price estimate available for this location.",
        )

    district = meta.get("district") or meta.get("district_norm") or district
    district_resolution = district_resolution or meta.get("district_resolution") if isinstance(meta, dict) else district_resolution

    try:
        store_quote(
            db,
            provider or "kaggle_hedonic_v0",
            city,
            district,
            parcel_id,
            value,
            method,
        )
    except Exception:
        pass

    return {
        "provider": provider or "kaggle_hedonic_v0",
        "city": city,
        "district": district,
        "sar_per_m2": value,
        "method": method,
        "meta": meta,
        "district_resolution": district_resolution or {},
        "kaggle_hedonic_v0_meta": meta,
    }
