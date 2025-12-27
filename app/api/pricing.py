from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.db.deps import get_db
from app.services.pricing import price_from_kaggle_hedonic, store_quote

router = APIRouter(prefix="/pricing", tags=["pricing"])


@router.get("/land")
def land_price(
    city: str = Query(...),
    district: str | None = Query(default=None),
    # Kept only for UI compatibility – backend always uses the Kaggle hedonic model.
    provider: str = Query(
        default="kaggle_hedonic_v0",
        description="Provider label from the UI. Backend always uses Kaggle hedonic model.",
    ),
    parcel_id: str | None = Query(default=None),
    lng: float | None = Query(default=None, description="Centroid longitude (WGS84)"),
    lat: float | None = Query(default=None, description="Centroid latitude (WGS84)"),
    db: Session = Depends(get_db),
):
    value, method, meta = price_from_kaggle_hedonic(
        db,
        city=city,
        lon=lng,
        lat=lat,
        district=district,
    )
    if value is None:
        raise HTTPException(
            status_code=404,
            detail="No land price estimate available for this location.",
        )

    district = meta.get("district") or district

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
        "kaggle_hedonic_v0_meta": meta,
    }
