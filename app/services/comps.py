from datetime import date
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from app.models.tables import SaleComp


def fetch_sale_comps(
    db: Session,
    city: Optional[str] = None,
    since: Optional[date] = None,
    limit: int = 200,
) -> List[SaleComp]:
    q = db.query(SaleComp)
    if city:
        q = q.filter(SaleComp.city.ilike(city))
    if since:
        q = q.filter(SaleComp.date >= since)
    return q.order_by(SaleComp.date.desc()).limit(limit).all()


def summarize_ppm2(comps: List[SaleComp]) -> Optional[float]:
    ppm2 = [float(c.price_per_m2) for c in comps if c.price_per_m2 is not None]
    if not ppm2:
        return None
    ppm2.sort()
    return ppm2[len(ppm2) // 2]  # median
