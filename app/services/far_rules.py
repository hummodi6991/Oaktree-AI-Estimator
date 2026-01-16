from __future__ import annotations

from typing import Any, Optional

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
    default_caps = {"s": 3.0, "m": 5.0, "c": 10.0}
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


def compute_far_with_priors(
    *,
    landuse_code: str | None,
    overture_typical_far: float | None,
    safety_factor: float = 1.15,
    priors: dict[str, dict[str, float]] | None = None,
) -> tuple[float, dict[str, Any]]:
    """
    FAR logic (Riyadh v2):

      Residential (s): base=2.0 clamp=[1.5,3.0]
      Mixed-use   (m): base=3.5 clamp=[2.5,5.0]

    District FAR is intentionally NOT used (data is corrupted).
    User explicit FAR is intentionally NOT used (avoid confusing UX).

    final_far = min(
        base_far,
        overture_typical_far * safety_factor (if exists),
        landuse_cap
    )
    then clamp to [clamp_min, clamp_max].
    """
    default_priors = {
        "s": {"base": 2.0, "clamp_min": 1.5, "clamp_max": 3.0, "cap": 3.0},
        "m": {"base": 3.5, "clamp_min": 2.5, "clamp_max": 5.0, "cap": 5.0},
        "c": {"base": 3.5, "clamp_min": 2.5, "clamp_max": 5.0, "cap": 10.0},
    }
    priors_map = dict(default_priors)
    if isinstance(priors, dict):
        for key, values in priors.items():
            if not isinstance(values, dict):
                continue
            code = str(key).strip().lower()
            priors_map.setdefault(code, {})
            priors_map[code].update(values)

    code = (landuse_code or "").strip().lower() or "m"
    if code not in priors_map:
        code = "m"

    base_far = float(priors_map[code].get("base", 3.5))
    clamp_min = float(priors_map[code].get("clamp_min", 2.5))
    clamp_max = float(priors_map[code].get("clamp_max", 5.0))
    landuse_cap = float(priors_map[code].get("cap", clamp_max))
    sf = float(safety_factor or 1.0)

    candidate = base_far
    components: dict[str, Any] = {
        "base_far": base_far,
        "clamp_min": clamp_min,
        "clamp_max": clamp_max,
        "landuse_cap": landuse_cap,
        "landuse_code": code,
        "overture_typical_far": float(overture_typical_far) if overture_typical_far is not None else None,
        "safety_factor": sf,
    }

    # Apply overture typical * safety_factor if present
    typical_scaled = None
    if overture_typical_far is not None:
        try:
            typical_scaled = float(overture_typical_far) * sf
            if typical_scaled > 0:
                candidate = min(candidate, typical_scaled)
        except Exception:
            typical_scaled = None
    components["overture_typical_scaled"] = typical_scaled

    # Apply landuse cap
    candidate = min(candidate, landuse_cap)
    components["candidate_pre_clamp"] = candidate

    # Clamp
    final_far = candidate
    final_far = max(final_far, clamp_min)
    final_far = min(final_far, clamp_max)

    if final_far <= 0:
        final_far = base_far

    meta = {
        "method": "priors_min_overture_cap",
        "components": components,
        "final_far": final_far,
    }
    return float(final_far), meta
