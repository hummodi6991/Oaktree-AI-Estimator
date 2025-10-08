from datetime import date
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.models.tables import MarketIndicator


def _indicator(
    db: Session, indicator_type: str, city: Optional[str], asset_type: str
) -> Optional[float]:
    q = (
        db.query(MarketIndicator)
        .filter(MarketIndicator.indicator_type == indicator_type)
        .filter(MarketIndicator.asset_type == asset_type)
        .order_by(MarketIndicator.date.desc())
    )
    if city:
        q = q.filter(func.lower(MarketIndicator.city) == city.lower())
    row = q.first()
    return float(row.value) if row else None


def build_to_sell_revenue(
    db: Session,
    net_floor_area_m2: float,
    city: Optional[str],
    asset_type: str = "residential",
    fallback_price_per_m2: float = 5500.0,
) -> Dict[str, Any]:
    indicator_price = _indicator(db, "sale_price_per_m2", city, asset_type)
    if indicator_price is None:
        price = fallback_price_per_m2
        source_type = "Manual"
    else:
        price = indicator_price
        source_type = "Observed"
    gdv = net_floor_area_m2 * price
    return {
        "gdv": gdv,
        "price_per_m2": price,
        "lines": [
            {
                "key": "sale_price_per_m2",
                "value": price,
                "unit": "SAR/m2",
                "source_type": source_type,
            }
        ],
    }


def build_to_lease_revenue(
    db: Session,
    net_floor_area_m2: float,
    city: Optional[str],
    asset_type: str = "residential",
    occ: float = 0.92,
    op_ex_ratio: float = 0.30,
    cap_rate: float = 0.075,
    fallback_rent_per_m2: float = 220.0,
    avg_unit_size_m2: float | None = None,
) -> Dict[str, Any]:
    indicator_rent = _indicator(db, "rent_per_m2", city, asset_type)
    rent_source_type = "Observed"
    if indicator_rent is not None:
        rent = indicator_rent
    else:
        alt = _indicator(db, "rent_avg_unit", city, asset_type)
        if alt is not None and (avg_unit_size_m2 or 0) > 0:
            rent = float(alt) / float(avg_unit_size_m2)
            rent_source_type = "Derived"
        else:
            rent = fallback_rent_per_m2
            rent_source_type = "Manual"
    # SAR/m2/month
    annual_rent = rent * net_floor_area_m2 * 12.0 * occ
    noi = annual_rent * (1.0 - op_ex_ratio)
    exit_value = noi / cap_rate
    return {
        "gdv": exit_value,
        "rent_per_m2": rent,
        "annual_rent": annual_rent,
        "noi": noi,
        "cap_rate": cap_rate,
        "lines": [
            {
                "key": "rent_per_m2",
                "value": rent,
                "unit": "SAR/m2/mo",
                "source_type": rent_source_type,
            },
            *(
                [
                    {
                        "key": "avg_unit_m2",
                        "value": avg_unit_size_m2,
                        "unit": "m2",
                        "source_type": "Manual",
                    }
                ]
                if avg_unit_size_m2
                else []
            ),
            {"key": "occ", "value": occ, "unit": "ratio", "source_type": "Manual"},
            {
                "key": "op_ex_ratio",
                "value": op_ex_ratio,
                "unit": "ratio",
                "source_type": "Manual",
            },
            {"key": "cap_rate", "value": cap_rate, "unit": "ratio", "source_type": "Manual"},
        ],
    }
