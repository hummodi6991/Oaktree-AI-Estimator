from datetime import date
from typing import Dict, Any
from sqlalchemy.orm import Session
from app.models.tables import CostIndexMonthly, BoqItem


def _latest_cci(db: Session, for_month: date, sector: str = "construction") -> float:
    row = (
        db.query(CostIndexMonthly)
        .filter(CostIndexMonthly.sector == sector)
        .order_by(CostIndexMonthly.month.desc())
        .first()
    )
    idx = float(row.cci_index) if row and row.cci_index is not None else 100.0
    return idx  # base 2023=100 in GASTAT; scale = idx/100


def compute_hard_costs(db: Session, area_m2: float, month: date) -> Dict[str, Any]:
    items = db.query(BoqItem).all()
    if not items:
        # conservative defaults if BoQ not yet seeded
        return {
            "cci_scalar": _latest_cci(db, month) / 100.0,
            "total": area_m2 * 1800.0 * (_latest_cci(db, month) / 100.0),
            "lines": [],
            "note": "fallback: 1800 SAR/m² baseline (no BoQ rows present)"
        }

    cci_scalar = _latest_cci(db, month) / 100.0
    total = 0.0
    lines = []
    for it in items:
        qty = area_m2 * float(it.quantity_per_m2)
        unit = float(it.baseline_unit_cost)
        factor = float(it.city_factor) * cci_scalar
        cost = qty * unit * factor
        lines.append({
            "code": it.code,
            "uom": it.uom,
            "qty": qty,
            "unit_cost": unit,
            "city_factor": float(it.city_factor),
            "cci_scalar": cci_scalar,
            "extended_cost": cost,
            "source_type": "Model",
            "url": it.source_url,
        })
        total += cost
    return {"cci_scalar": cci_scalar, "total": total, "lines": lines}
