from datetime import date
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.models.tables import SaleComp


def top_sale_comps(
    db: Session,
    city: Optional[str],
    district: Optional[str],
    asset_type: str = "land",
    since: Optional[date] = None,
    limit: int = 10,
) -> List[SaleComp]:
    q = db.query(SaleComp).filter(SaleComp.asset_type.ilike(asset_type))
    if city:
        q = q.filter(SaleComp.city.ilike(city))
    if district:
        q = q.filter(SaleComp.district.ilike(district))
    if since:
        q = q.filter(SaleComp.date >= since)
    rows = q.order_by(SaleComp.date.desc()).limit(200).all()

    # Score by recency; reward district match (simple heuristic for MVP)
    def score(r: SaleComp) -> float:
        recency_days = (date.today() - r.date).days if r.date else 9999
        recency_score = recency_days / 365.0
        district_miss = 0 if (district and r.district and r.district.lower() == district.lower()) else 1
        return recency_score + 0.25 * district_miss

    return sorted(rows, key=score)[:limit]


def to_comp_dict(r: SaleComp) -> Dict[str, Any]:
    return {
        "id": r.id,
        "date": r.date.isoformat() if r.date else None,
        "city": r.city,
        "district": r.district,
        "asset_type": r.asset_type,
        "net_area_m2": float(r.net_area_m2) if r.net_area_m2 is not None else None,
        "price_total": float(r.price_total) if r.price_total is not None else None,
        "price_per_m2": float(r.price_per_m2) if r.price_per_m2 is not None else None,
        "source": r.source,
        "source_url": r.source_url,
    }


def heuristic_drivers(ppm2_median: Optional[float], comps: List[SaleComp]) -> List[Dict[str, Any]]:
    drivers: List[Dict[str, Any]] = []
    if not comps:
        return drivers

    # Recency (median months of comps used)
    days = sorted([(date.today() - c.date).days for c in comps if c.date])
    if days:
        drivers.append({
            "name": "recency",
            "direction": "newer comps → higher confidence",
            "magnitude": float(sum(days) / len(days) / 30.0),
            "unit": "months",
        })

    # District cohesion (share of comps from most common district)
    districts = [(c.district or "").lower() for c in comps]
    if districts:
        top = max(set(districts), key=districts.count)
        cohesion = districts.count(top) / len(districts)
        drivers.append({
            "name": "district_cohesion",
            "direction": "higher cohesion → tighter estimate",
            "magnitude": round(cohesion, 2),
            "unit": "ratio",
        })

    # Dispersion of price_per_m2 (std dev)
    prices = [float(c.price_per_m2) for c in comps if c.price_per_m2]
    if len(prices) >= 2:
        mean = sum(prices) / len(prices)
        variance = sum((p - mean) ** 2 for p in prices) / (len(prices) - 1)
        drivers.append({
            "name": "volatility_ppm2",
            "direction": "higher volatility widens bands",
            "magnitude": variance ** 0.5,
            "unit": "SAR/m2",
        })
    return drivers
