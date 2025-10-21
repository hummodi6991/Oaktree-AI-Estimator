from __future__ import annotations

from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.tables import FarRule


def lookup_far(
    db: Session,
    city: Optional[str],
    district: Optional[str],
    zoning: Optional[str] = None,
    road_class: Optional[str] = None,
    frontage_m: Optional[float] = None,
) -> Optional[float]:
    """
    Return the best FAR (far_max) for the given city/district and optional context.
    Selection heuristic:
      1) filter by city/district (case-insensitive)
      2) optionally prefer matching zoning/road_class if provided
      3) for frontage, prefer rows with frontage_min_m <= frontage_m (largest threshold wins)
      4) latest asof_date wins as tie-breaker
    """
    if not city or not district:
        return None

    q = db.query(FarRule).filter(
        func.lower(FarRule.city) == city.lower(),
        func.lower(FarRule.district) == district.lower(),
    )
    if zoning:
        q = q.filter(func.lower(FarRule.zoning) == zoning.lower())
    if road_class:
        q = q.filter(func.lower(FarRule.road_class) == road_class.lower())

    # Ordering: frontage threshold desc (if we know frontage), then asof desc, then id desc
    if frontage_m is not None:
        q = q.filter((FarRule.frontage_min_m == None) | (FarRule.frontage_min_m <= frontage_m))  # noqa: E711
        q = q.order_by(
            FarRule.frontage_min_m.desc().nulls_last(),
            FarRule.asof_date.desc().nulls_last(),
            FarRule.id.desc(),
        )
    else:
        q = q.order_by(
            FarRule.asof_date.desc().nulls_last(),
            FarRule.id.desc(),
        )

    row = q.first()
    return float(row.far_max) if row and row.far_max is not None else None
