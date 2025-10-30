from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.services.pricing import price_from_srem, price_from_suhail, store_quote

router = APIRouter(prefix="/pricing", tags=["pricing"])


@router.get("/land")
def land_price(
    city: str = Query(...),
    district: str | None = Query(default=None),
    provider: str = Query(default="srem", pattern="^(srem|suhail)$"),
    parcel_id: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    if provider == "srem":
        result = price_from_srem(db, city, district)
    else:
        result = price_from_suhail(db, city, district)
    if not result:
        raise HTTPException(status_code=404, detail="No price available")

    value, method = result
    try:
        store_quote(db, provider, city, district, parcel_id, value, method)
    except Exception:
        pass

    return {
        "provider": provider,
        "city": city,
        "district": district,
        "sar_per_m2": value,
        "method": method,
    }
