from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from shapely.geometry import Point

from app.db.deps import get_db
from app.services import geo as geo_svc
from app.services.pricing import price_from_kaggle_hedonic, store_quote

router = APIRouter(prefix="/pricing", tags=["pricing"])


@router.get("/land")
def land_price(
    city: str = Query(...),
    district: str | None = Query(default=None),
    # Kept only for UI compatibility – backend always uses the Kaggle hedonic model.
    provider: str = Query(
        default="kaggle_hedonic",
        description="Provider label from the UI. Backend always uses Kaggle hedonic model.",
    ),
    parcel_id: str | None = Query(default=None),
    lng: float | None = Query(default=None, description="Centroid longitude (WGS84)"),
    lat: float | None = Query(default=None, description="Centroid latitude (WGS84)"),
    db: Session = Depends(get_db),
):
    if (district is None) and (lng is not None) and (lat is not None):
        try:
            inferred = geo_svc.infer_district_from_features(
                db, Point(lng, lat), layer="rydpolygons"
            )
            if inferred:
                district = inferred
        except Exception:
            pass

    result = price_from_kaggle_hedonic(db, city, district)
    if not result:
        # Clean “no data” response instead of a 500
        raise HTTPException(
            status_code=404, detail="No price available from Kaggle hedonic model"
        )

    value, method = result
    try:
        store_quote(db, provider or "kaggle_hedonic", city, district, parcel_id, value, method)
    except Exception:
        pass

    return {
        "provider": provider or "kaggle_hedonic",
        "city": city,
        "district": district,
        "sar_per_m2": value,
        "method": method,
    }
