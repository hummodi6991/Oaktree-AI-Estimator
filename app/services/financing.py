from datetime import date
from typing import Dict, Any, List
from sqlalchemy.orm import Session
from app.models.tables import Rate


def _sama_base_rate(db: Session, on_date: date) -> float:
    row = (
        db.query(Rate)
        .filter(Rate.rate_type == "SAMA_base")
        .order_by(Rate.date.desc())
        .first()
    )
    return float(row.value) if row and row.value is not None else 6.0  # % p.a.


def compute_financing(
    db: Session,
    hard_plus_soft: float,
    months: int,
    margin_bps: int = 250,
    ltv: float = 0.6,
    asof: date | None = None,
) -> Dict[str, Any]:
    base = _sama_base_rate(db, asof or date.today()) / 100.0  # decimal p.a.
    apr = base + (margin_bps / 10000.0)
    mr = apr / 12.0

    # simple S-curve (front/mid/back): 15% / 50% / 35%
    seg = max(1, months // 3)
    profile: List[float] = []
    profile += [0.15 / seg] * seg
    profile += [0.50 / seg] * seg
    profile += [0.35 / (months - 2 * seg)] * (months - 2 * seg)
    # normalize (guard integers)
    s = sum(profile)
    profile = [p / s for p in profile]

    principal = hard_plus_soft * ltv
    balance = 0.0
    interest = 0.0
    for p in profile:
        draw = principal * p
        balance += draw
        interest += balance * mr
    return {
        "apr": apr,
        "monthly_rate": mr,
        "ltv": ltv,
        "months": months,
        "interest": interest,
        "principal": principal,
    }
