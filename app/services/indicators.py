from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.tables import MarketIndicator


def _latest_indicator_value(
    db: Session,
    indicator_type: str,
    city: Optional[str],
    district: Optional[str],
) -> Optional[tuple[float, str]]:
    """Return the latest indicator value and unit for a city/district pair."""

    q = db.query(MarketIndicator).filter(MarketIndicator.indicator_type == indicator_type)
    if city:
        q = q.filter(func.lower(MarketIndicator.city) == city.lower())

    district_column = getattr(MarketIndicator, "district", None)
    if district and district_column is not None:
        district_row = (
            q.filter(func.lower(district_column) == district.lower())
            .order_by(MarketIndicator.date.desc(), MarketIndicator.id.desc())
            .first()
        )
        if district_row:
            return float(district_row.value), (district_row.unit or "")

    row = q.order_by(MarketIndicator.date.desc(), MarketIndicator.id.desc()).first()
    if not row:
        return None
    return float(row.value), (row.unit or "")


def latest_sale_price_per_m2(db: Session, city: Optional[str], district: Optional[str]) -> Optional[float]:
    """Fetch the latest sale price per square meter (SAR/m2)."""

    result = _latest_indicator_value(db, "sale_price_per_m2", city, district)
    if not result:
        return None
    value, _unit = result
    return float(value)


def latest_rent_per_m2(db: Session, city: Optional[str], district: Optional[str]) -> Optional[float]:
    """Fetch the latest rent per square meter per month (SAR/m2/month)."""

    result = _latest_indicator_value(db, "rent_per_m2", city, district)
    if not result:
        return None
    value, unit = result
    unit_lower = unit.lower()
    rent_value = float(value)
    if "year" in unit_lower or "/yr" in unit_lower:
        rent_value = rent_value / 12.0
    return rent_value
