from datetime import date
from typing import Dict, Any
from sqlalchemy.orm import Session
from app.models.tables import BoqItem


def compute_hard_costs(db: Session, area_m2: float, month: date) -> Dict[str, Any]:
    items = db.query(BoqItem).all()
    if not items:
        # conservative defaults if BoQ not yet seeded
        return {
            "total": area_m2 * 1800.0,
            "lines": [],
            "note": "fallback: 1800 SAR/m² baseline (no BoQ rows present)"
        }

    total = 0.0
    lines = []
    for it in items:
        qty = area_m2 * float(it.quantity_per_m2)
        unit = float(it.baseline_unit_cost)
        factor = float(it.city_factor)
        cost = qty * unit * factor
        lines.append({
            "code": it.code,
            "uom": it.uom,
            "qty": qty,
            "unit_cost": unit,
            "city_factor": float(it.city_factor),
            "extended_cost": cost,
            "source_type": "Model",
            "url": it.source_url,
        })
        total += cost
    return {"total": total, "lines": lines}
