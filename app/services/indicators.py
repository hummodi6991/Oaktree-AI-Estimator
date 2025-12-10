from __future__ import annotations

from datetime import date
from typing import Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.tables import MarketIndicator


def _latest_indicator_value(
    db: Session,
    indicator_type: str,
    asset_type: Optional[str],
    city: Optional[str],
    district: Optional[str],
) -> Optional[tuple[float, str]]:
    """Return the latest indicator value and unit for a city/district pair."""

    q = db.query(MarketIndicator).filter(MarketIndicator.indicator_type == indicator_type)
    if asset_type:
        q = q.filter(func.lower(MarketIndicator.asset_type) == asset_type.lower())
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

    result = _latest_indicator_value(db, "sale_price_per_m2", None, city, district)
    if not result:
        return None
    value, _unit = result
    return float(value)


def latest_rent_per_m2(db: Session, city: Optional[str], district: Optional[str]) -> Optional[float]:
    """Fetch the latest rent per square meter per month (SAR/m2/month)."""

    result = _latest_indicator_value(db, "rent_per_m2", None, city, district)
    if not result:
        return None
    value, unit = result
    unit_lower = unit.lower()
    rent_value = float(value)
    if "year" in unit_lower or "/yr" in unit_lower:
        rent_value = rent_value / 12.0
    return rent_value


def latest_rega_residential_rent_per_m2(
    db: Session,
    city: Optional[str],
    district: Optional[str],
) -> Optional[Tuple[float, str, date, Optional[str]]]:
    """
    Latest REGA residential rent per m² per month for a city/district.

    Returns (rent_value_monthly, unit, as_of_date, source_url).
    """

    q = db.query(MarketIndicator).filter(
        MarketIndicator.indicator_type == "rent_per_m2",
        func.lower(MarketIndicator.asset_type) == "residential",
        MarketIndicator.source_url.ilike("%rega.gov.sa%"),
    )

    if city:
        q = q.filter(func.lower(MarketIndicator.city) == city.lower())

    district_column = getattr(MarketIndicator, "district", None)
    if district and district_column is not None:
        row = (
            q.filter(func.lower(district_column) == district.lower())
            .order_by(MarketIndicator.date.desc(), MarketIndicator.id.desc())
            .first()
        )
    else:
        row = q.order_by(MarketIndicator.date.desc(), MarketIndicator.id.desc()).first()

    if not row:
        return None

    value = float(row.value)
    unit = (row.unit or "").strip()
    unit_lower = unit.lower()

    rent_value = value
    if "year" in unit_lower or "/yr" in unit_lower:
        rent_value = rent_value / 12.0

    return rent_value, unit, row.date, row.source_url


def latest_rent_unit_rate(db: Session, city: Optional[str], district: Optional[str]) -> Optional[float]:
    """Fetch the latest rent per unit indicator when available."""

    for indicator_key in ("rent_unit_rate", "rent_avg_unit"):
        result = _latest_indicator_value(db, indicator_key, None, city, district)
        if result:
            value, _unit = result
            return float(value)
    return None


def latest_rent_vacancy_pct(db: Session, city: Optional[str], district: Optional[str]) -> Optional[float]:
    """Return the latest vacancy percentage tied to rent benchmarks."""

    for indicator_key in ("rent_vacancy_pct", "vacancy_pct"):
        result = _latest_indicator_value(db, indicator_key, None, city, district)
        if result:
            value, _unit = result
            return float(value)
    return None


def latest_rent_growth_pct(db: Session, city: Optional[str], district: Optional[str]) -> Optional[float]:
    """Return the latest rent growth percentage, if tracked."""

    for indicator_key in ("rent_growth_pct", "rent_growth"):
        result = _latest_indicator_value(db, indicator_key, None, city, district)
        if result:
            value, _unit = result
            return float(value)
    return None


def latest_re_price_index(
    db: Session,
    asset_type: str = "Residential",
    city: str = "Saudi Arabia",
) -> float | None:
    """Latest real estate price index (2014=100) for given asset type."""

    result = _latest_indicator_value(
        db,
        indicator_type="real_estate_price_index",
        asset_type=asset_type,
        city=city,
        district=None,
    )
    if not result:
        return None
    value, _unit = result
    return float(value)


def latest_re_price_index_scalar(
    db: Session,
    asset_type: str = "Residential",
    city: str = "Saudi Arabia",
) -> float:
    """Index rescaled so 2014=1.0."""

    idx = latest_re_price_index(db, asset_type=asset_type, city=city)
    return (idx / 100.0) if idx is not None else 1.0
