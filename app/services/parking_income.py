from __future__ import annotations

from typing import Any, Tuple


def _normalize_landuse_code(raw: Any) -> str | None:
    """
    Normalize land use label to a compact code:
        - residential → "s"
        - mixed → "m"
        - commercial/office/retail → "c"
    """
    if raw is None:
        return None
    s = str(raw or "").strip().lower()
    if not s:
        return None
    if s in {"s", "residential", "res"}:
        return "s"
    if s in {"m", "mixed", "mixed-use", "mixed use"}:
        return "m"
    if s in {"c", "commercial", "office", "retail"}:
        return "c"
    return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _round_to_nearest_25(v: float) -> float:
    return round(float(v) / 25.0) * 25.0


def compute_parking_income(
    extra_spaces: int,
    *,
    monetize: bool,
    landuse_code: str | None,
    land_price_sar_m2: float,
    public_access: bool,
    override_rate: float | None = None,
    occupancy_override: float | None = None,
) -> Tuple[float, dict]:
    """
    Estimate optional parking income from excess stalls.

    Returns:
        parking_income_y1 (SAR/year), metadata dict.
    """
    extra = int(extra_spaces or 0)
    landuse = _normalize_landuse_code(landuse_code)
    land_price = float(land_price_sar_m2 or 0.0)
    base_rate_map = {"s": 400.0, "m": 900.0, "c": 1000.0}
    base_rate = float(base_rate_map.get(landuse, 750.0))
    public = bool(public_access)
    occupancy_base_map = {"s": 0.55, "m": 0.70, "c": 0.75}
    occupancy_base = float(occupancy_base_map.get(landuse, 0.65))

    meta: dict[str, Any] = {
        "monetize_extra_parking": bool(monetize),
        "extra_spaces": extra,
        "landuse_code_used": landuse,
        "land_price_sar_m2": land_price,
        "base_rate": base_rate,
        "public_access": public,
    }

    if not monetize:
        meta["rate_note"] = "Parking income disabled (monetize_extra_parking=false)."
        meta["monthly_rate_used"] = 0.0
        meta["occupancy_used"] = 0.0
        meta["occupancy_base"] = occupancy_base
        meta["premium"] = 0.0
        meta["access_factor"] = 0.0
        meta["rate_clamped_floor"] = False
        meta["rate_clamped_cap"] = False
        meta["thin_market_adjustment"] = False
        return 0.0, meta

    if extra <= 0:
        meta["rate_note"] = "No excess parking spaces available."
        meta["monthly_rate_used"] = 0.0
        meta["occupancy_used"] = 0.0
        meta["occupancy_base"] = occupancy_base
        meta["premium"] = 0.0
        meta["access_factor"] = 0.0
        meta["rate_clamped_floor"] = False
        meta["rate_clamped_cap"] = False
        meta["thin_market_adjustment"] = False
        return 0.0, meta

    premium_raw = land_price / 8000.0
    premium = _clamp(premium_raw, 0.85, 1.35)
    if landuse == "s":
        premium = min(premium, 1.05)

    access_factor = 1.0 if public else 0.65
    occupancy_used = float(occupancy_base)
    thin_market_adjustment = False
    if occupancy_override is not None:
        occupancy_used = _clamp(float(occupancy_override), 0.0, 1.0)
    elif extra < 3:
        occupancy_used = _clamp(occupancy_base - 0.10, 0.0, 1.0)
        thin_market_adjustment = True

    rate_clamped_floor = False
    rate_clamped_cap = False
    if override_rate is not None and override_rate > 0:
        monthly_rate_used = float(override_rate)
        rate_note = "Override rate provided; using explicit monthly rate without adjustments."
    else:
        raw_rate = base_rate * premium * access_factor
        monthly_rate_used = _round_to_nearest_25(raw_rate)
        if monthly_rate_used < 250:
            monthly_rate_used = 250.0
            rate_clamped_floor = True
        if monthly_rate_used > 1500:
            monthly_rate_used = 1500.0
            rate_clamped_cap = True
        rate_note = (
            "Base rate × premium × access factor; rounded to nearest 25 SAR and clamped within 250–1500."
        )

    parking_income_y1 = float(extra) * monthly_rate_used * 12.0 * occupancy_used

    meta.update(
        {
            "premium": premium,
            "access_factor": access_factor,
            "monthly_rate_used": monthly_rate_used,
            "occupancy_used": occupancy_used,
            "occupancy_base": occupancy_base,
            "occupancy_override": occupancy_override,
            "override_rate": override_rate,
            "rate_clamped_floor": rate_clamped_floor,
            "rate_clamped_cap": rate_clamped_cap,
            "thin_market_adjustment": thin_market_adjustment,
            "rate_note": rate_note,
        }
    )

    return parking_income_y1, meta
