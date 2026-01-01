from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.db.deps import get_db
from app.services.district_resolver import resolution_meta
from app.services.land_price_engine import quote_land_price_blended_v1
from app.services.pricing import price_from_kaggle_hedonic, price_from_suhail, store_quote
from app.services.pricing_response import normalize_land_price_quote

router = APIRouter(prefix="/pricing", tags=["pricing"])


@router.get("/land")
def land_price(
    city: str | None = Query(default=None, description="City name"),
    district: str | None = Query(default=None),
    provider: str = Query(default="blended_v1", description="Provider label (blended_v1, kaggle_hedonic_v0, suhail)."),
    parcel_id: str | None = Query(default=None),
    lng: float | None = Query(default=None, description="Centroid longitude (WGS84)"),
    lon: float | None = Query(default=None, description="Alias for lng"),
    lat: float | None = Query(default=None, description="Centroid latitude (WGS84)"),
    db: Session = Depends(get_db),
):
    city = city or "Riyadh"
    effective_lng = lng if lng is not None else lon
    provider_key = (provider or "").lower()
    raw_quote: dict = {}
    method = provider_key

    if provider_key == "blended_v1":
        quote = quote_land_price_blended_v1(
            db,
            city=city,
            district=district,
            lon=effective_lng,
            lat=lat,
            geom_geojson=None,
        )
        raw_quote = quote
        method = quote.get("method") or "blended_v1"
    elif provider_key == "suhail":
        value, method, resolution = price_from_suhail(
            db,
            city=city,
            district=district,
            geom_geojson=None,
            lon=effective_lng,
            lat=lat,
        )
        if value is None:
            raise HTTPException(
                status_code=404,
                detail="No land price estimate available for this location.",
            )
        raw_quote = {
            "provider": "suhail",
            "value": value,
            "method": method,
            "district_norm": resolution.district_norm,
            "district_raw": resolution.district_raw or district,
            "district_resolution": resolution_meta(resolution),
            "meta": {
                "source": "suhail_land_metrics",
                "district_norm": resolution.district_norm,
                "district_resolution": resolution_meta(resolution),
            },
        }
    else:
        value, method, meta = price_from_kaggle_hedonic(
            db,
            city=city,
            lon=effective_lng,
            lat=lat,
            district=district,
        )
        raw_quote = {
            "provider": provider_key or "kaggle_hedonic_v0",
            "value": value,
            "method": method,
            "meta": meta,
        }

    normalized = normalize_land_price_quote(city, provider_key or raw_quote.get("provider"), raw_quote, method)

    if normalized["value_sar_m2"] is None:
        raise HTTPException(
            status_code=404,
            detail="No land price estimate available for this location.",
        )

    try:
        store_quote(
            db,
            normalized["provider"] or "kaggle_hedonic_v0",
            city,
            normalized.get("district_norm") or normalized.get("district_raw") or district,
            parcel_id,
            normalized["value_sar_m2"],
            normalized["method"],
        )
    except Exception:
        pass

    return {
        **normalized,
        "sar_per_m2": normalized["value_sar_m2"],
        "value": normalized["value_sar_m2"],
        "district": normalized.get("district_norm") or normalized.get("district_raw"),
    }
