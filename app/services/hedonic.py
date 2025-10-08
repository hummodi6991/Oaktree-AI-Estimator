from datetime import date
from typing import Optional, Tuple, Dict, Any
from sqlalchemy.orm import Session
from app.services.comps import fetch_sale_comps, summarize_ppm2
from app.services.hedonic_model import predict_ppm2


def land_price_per_m2(
    db: Session,
    city: Optional[str],
    since: Optional[date],
    district: Optional[str] = None,
) -> Tuple[Optional[float], Dict[str, Any]]:
    # 1) Try model prediction if available
    model_ppm2, model_meta = predict_ppm2(city, district=district, on=since)
    # 2) Always compute comps median (fallback + transparency)
    comps = fetch_sale_comps(db, city=city, district=district, since=since)
    median_ppm2 = summarize_ppm2(comps)
    ppm2 = model_ppm2 or median_ppm2
    meta = {
        "n_comps": len(comps),
        "city": city,
        "since": since.isoformat() if since else None,
        "model": model_meta,
    }
    return ppm2, meta
