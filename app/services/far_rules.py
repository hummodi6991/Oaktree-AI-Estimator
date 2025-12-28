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


def cap_far_by_landuse(
    far_value: float | None,
    landuse_code: str | None,
    *,
    caps: dict[str, float] | None = None,
) -> tuple[float | None, dict]:
    """
    Clamp inferred FAR to a reasonable maximum based on land-use class.

    landuse_code:
      - 's' = residential
      - 'm' = mixed-use
      - 'c' = commercial (optional; UI sometimes uses 'm' only)

    Returns (clamped_far, meta) where meta includes whether a clamp occurred.
    """
    if far_value is None:
        return None, {"applied": False, "reason": "far_none"}
    try:
        far_f = float(far_value)
    except Exception:
        return far_value, {"applied": False, "reason": "far_not_float"}
    if far_f <= 0:
        return far_value, {"applied": False, "reason": "far_nonpositive"}

    # Defaults tuned to avoid absurd "observed density" outliers from Overture.
    # You can refine these caps later using official zoning data.
    default_caps = {"s": 4.0, "m": 6.0, "c": 10.0}
    cap_map = dict(default_caps)
    if isinstance(caps, dict):
        for k, v in caps.items():
            try:
                cap_map[str(k).strip().lower()] = float(v)
            except Exception:
                continue

    code = (landuse_code or "").strip().lower()
    if code not in cap_map:
        # Unknown / missing: choose a conservative mixed cap
        code = "m"

    cap_val = float(cap_map[code])
    if far_f > cap_val:
        return cap_val, {
            "applied": True,
            "landuse_code": code,
            "cap": cap_val,
            "original": far_f,
        }

    return far_f, {
        "applied": False,
        "landuse_code": code,
        "cap": cap_val,
        "original": far_f,
    }
