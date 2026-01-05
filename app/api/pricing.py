from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.db.deps import get_db
from app.services.district_resolver import resolution_meta
from app.services.land_price_engine import quote_land_price_blended_v1
from app.services.pricing import price_from_kaggle_hedonic, price_from_suhail, store_quote
from app.services.pricing_response import normalize_land_price_quote

router = APIRouter(prefix="/pricing", tags=["pricing"])


def normalize_land_use_group(landuse: str | None) -> str | None:
    if not landuse:
        return None

    text = str(landuse).strip()
    tl = text.lower()

    direct_map = {
        "s": "سكني",
        "residential": "سكني",
        "commercial": "تجاري",
        "industrial": "صناعي",
        "i": "صناعي",
        "agricultural": "زراعي",
        "a": "زراعي",
    }
    if tl in direct_map:
        return direct_map[tl]

    if "سكن" in text or "residential" in tl or "housing" in tl:
        return "سكني"
    if "تجاري" in text or "commercial" in tl or "retail" in tl or "office" in tl:
        return "تجاري"
    if "صناعي" in text or "industrial" in tl or "factory" in tl or "warehouse" in tl:
        return "صناعي"
    if "زراع" in text or "agricultural" in tl or "farm" in tl:
        return "زراعي"

    return None


def _infer_land_use_group(db: Session, lon: float | None, lat: float | None) -> str | None:
    if lon is None or lat is None:
        return None

    try:
        from app.api.geo_portal import _DEFAULT_TOLERANCE as _IDENTIFY_TOLERANCE_M, _identify_postgis

        identify = _identify_postgis(lon, lat, _IDENTIFY_TOLERANCE_M, db)
    except Exception:
        return None
    if not identify or not identify.get("found"):
        return None

    parcel = identify.get("parcel") or {}
    landuse_raw = parcel.get("landuse_raw") or parcel.get("classification_raw") or parcel.get("landuse_code")
    return normalize_land_use_group(landuse_raw)


@router.get("/land")
def land_price(
    city: str | None = Query(default=None, description="City name"),
    district: str | None = Query(default=None),
    provider: str = Query(default="blended_v1", description="Provider label (blended_v1, kaggle_hedonic_v0, suhail)."),
    parcel_id: str | None = Query(default=None),
    lng: float | None = Query(default=None, description="Centroid longitude (WGS84)"),
    lon: float | None = Query(default=None, description="Alias for lng"),
    lat: float | None = Query(default=None, description="Centroid latitude (WGS84)"),
    land_use_group: str | None = Query(default=None, description="Optional land-use group filter for pricing."),
    db: Session = Depends(get_db),
):
    city = city or "Riyadh"
    effective_lng = lng if lng is not None else lon
    provider_key = (provider or "").lower()
    raw_quote: dict = {}
    method = provider_key

    if provider_key == "blended_v1":
        inferred_land_use_group = land_use_group or _infer_land_use_group(db, effective_lng, lat)
        quote = quote_land_price_blended_v1(
            db,
            city=city,
            district=district,
            lon=effective_lng,
            lat=lat,
            geom_geojson=None,
            land_use_group=inferred_land_use_group,
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
        meta = raw_quote.get("meta") if isinstance(raw_quote, dict) else {}
        detail = {
            "message": "No land price estimate available for this location.",
            "reason": meta.get("reason"),
            "city_used": meta.get("city_used") or city,
            "district_used": meta.get("district_used")
            or normalized.get("district_raw")
            or normalized.get("district_norm")
            or district,
        }
        raise HTTPException(
            status_code=404,
            detail=detail,
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
