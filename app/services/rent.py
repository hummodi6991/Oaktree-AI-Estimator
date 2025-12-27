from __future__ import annotations

from datetime import date, timedelta
from math import ceil
from typing import Iterable, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.tables import RentComp


def _percentile_disc(values: Iterable[float], percentile: float) -> Optional[float]:
    """
    Mirror PostgreSQL's percentile_disc: pick the smallest value with cumulative
    distribution >= percentile. Returns None when the iterable is empty.
    """
    vals = sorted(float(v) for v in values)
    if not vals:
        return None
    # percentile_disc uses 1-based indexing with ceiling
    idx = max(1, ceil(percentile * len(vals)))
    return vals[idx - 1]


def _clip(values: Iterable[float], low: Optional[float], high: Optional[float]) -> list[float]:
    clipped: list[float] = []
    for v in values:
        if low is not None and v < low:
            continue
        if high is not None and v > high:
            continue
        clipped.append(v)
    return clipped


def aqar_rent_median(
    db: Session,
    city: str,
    district: Optional[str],
    asset_type: str = "residential",
    unit_type: Optional[str] = None,
    since_days: int = 365,
) -> Tuple[Optional[float], Optional[float], int, int]:
    """
    Median rent_per_m2 (SAR/month) from rent_comp, clipped to the p05–p95 band.

    Returns (district_median, city_median, n_district, n_city), where n_* counts
    the rows that remained after clipping.
    """

    if not city:
        return None, None, 0, 0

    since_date = date.today() - timedelta(days=since_days) if since_days else None

    def _values(scope_district: bool) -> list[float]:
        q = db.query(RentComp.rent_per_m2).filter(RentComp.rent_per_m2.isnot(None))
        if asset_type:
            q = q.filter(func.lower(RentComp.asset_type) == asset_type.lower())
        if unit_type:
            q = q.filter(RentComp.unit_type.ilike(unit_type))
        if city:
            q = q.filter(func.lower(RentComp.city) == city.lower())
        if since_date:
            q = q.filter(RentComp.date >= since_date)
        if scope_district and district:
            q = q.filter(func.lower(RentComp.district) == district.lower())
        return [float(row[0]) for row in q.all() if row[0] is not None]

    city_values = _values(scope_district=False)
    district_values = _values(scope_district=True)

    if not city_values:
        return None, None, 0, 0

    city_low = _percentile_disc(city_values, 0.05)
    city_high = _percentile_disc(city_values, 0.95)
    city_filtered = _clip(city_values, city_low, city_high)
    city_median = _percentile_disc(city_filtered, 0.5) if city_filtered else None

    district_median = None
    district_filtered: list[float] = []
    if district_values:
        dist_low = _percentile_disc(district_values, 0.05)
        dist_high = _percentile_disc(district_values, 0.95)
        # Apply both local (district) and city-wide clipping to keep a conservative band
        district_filtered = _clip(district_values, dist_low, dist_high)
        district_filtered = _clip(district_filtered, city_low, city_high)
        district_median = _percentile_disc(district_filtered, 0.5) if district_filtered else None

    return (
        float(district_median) if district_median is not None else None,
        float(city_median) if city_median is not None else None,
        len(district_filtered),
        len(city_filtered),
    )
