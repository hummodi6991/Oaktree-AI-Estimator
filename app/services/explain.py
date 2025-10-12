from datetime import date
from datetime import date
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from app.models.tables import RentComp, SaleComp


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


def top_rent_comps(
    db: Session,
    city: Optional[str],
    district: Optional[str],
    asset_type: str = "residential",
    since: Optional[date] = None,
    limit: int = 10,
) -> List[RentComp]:
    q = db.query(RentComp)
    if asset_type:
        q = q.filter(RentComp.asset_type.ilike(asset_type))
    if city:
        q = q.filter(RentComp.city.ilike(city))
    if district:
        q = q.filter(RentComp.district.ilike(district))
    if since:
        q = q.filter(RentComp.date >= since)

    rows = q.order_by(RentComp.date.desc()).limit(200).all()

    def score(r: RentComp) -> float:
        recency_days = (date.today() - r.date).days if r.date else 9999
        recency_score = recency_days / 365.0
        district_miss = 0 if (district and r.district and r.district.lower() == (district or "").lower()) else 1
        return recency_score + 0.25 * district_miss

    return sorted(rows, key=score)[:limit]


def to_rent_comp_dict(r: RentComp) -> Dict[str, Any]:
    return {
        "id": r.id,
        "date": r.date.isoformat() if r.date else None,
        "city": r.city,
        "district": r.district,
        "asset_type": r.asset_type,
        "sar_per_m2": float(r.rent_per_m2) if r.rent_per_m2 is not None else None,
        "source": r.source,
        "source_url": r.source_url,
    }


def rent_heuristic_drivers(rent_ppm2: Optional[float], comps: List[RentComp]) -> List[Dict[str, Any]]:
    drivers: List[Dict[str, Any]] = []
    if not comps:
        return drivers

    days = sorted([(date.today() - c.date).days for c in comps if c.date])
    if days:
        drivers.append(
            {
                "name": "recency",
                "direction": "newer leases → tighter benchmark",
                "magnitude": float(sum(days) / len(days) / 30.0),
                "unit": "months",
            }
        )

    districts = [(c.district or "").lower() for c in comps]
    if districts:
        top = max(set(districts), key=districts.count)
        cohesion = districts.count(top) / len(districts)
        drivers.append(
            {
                "name": "district_cohesion",
                "direction": "higher cohesion → stronger signal",
                "magnitude": round(cohesion, 2),
                "unit": "ratio",
            }
        )

    rents = [float(c.rent_per_m2) for c in comps if c.rent_per_m2]
    if len(rents) >= 2:
        mean = sum(rents) / len(rents)
        variance = sum((p - mean) ** 2 for p in rents) / (len(rents) - 1)
        drivers.append(
            {
                "name": "volatility_rent_ppm2",
                "direction": "higher volatility widens rent range",
                "magnitude": variance ** 0.5,
                "unit": "SAR/m2",
            }
        )

    if rent_ppm2 is not None:
        drivers.append(
            {
                "name": "headline_rent",
                "direction": "blended rent used for benchmarking",
                "magnitude": float(rent_ppm2),
                "unit": "SAR/m2",
            }
        )

    return drivers
