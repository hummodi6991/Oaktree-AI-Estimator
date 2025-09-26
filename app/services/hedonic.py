from datetime import date
from typing import Optional, Tuple, Dict, Any
from sqlalchemy.orm import Session
from app.services.comps import fetch_sale_comps, summarize_ppm2


def land_price_per_m2(
    db: Session, city: Optional[str], since: Optional[date]
) -> Tuple[Optional[float], Dict[str, Any]]:
    comps = fetch_sale_comps(db, city=city, since=since)
    median_ppm2 = summarize_ppm2(comps)
    meta = {"n_comps": len(comps), "city": city, "since": since.isoformat() if since else None}
    return median_ppm2, meta
