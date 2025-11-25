from datetime import date
from fastapi import APIRouter, Query, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session
from app.db.deps import get_db
from app.models.tables import CostIndexMonthly, Rate

router = APIRouter(tags=["indices"])

@router.get("/indices/cci")
def get_cci(
    month: str | None = Query(default=None),
    sector: str = Query(default="construction"),
    db: Session = Depends(get_db),
) -> dict[str, list[dict]]:
    q = db.query(CostIndexMonthly).filter(CostIndexMonthly.sector == sector)
    if month:
        q = q.filter(func.to_char(CostIndexMonthly.month, "YYYY-MM") == month[:7])
    rows = q.order_by(CostIndexMonthly.month.desc()).all()
    items = [
        {
            "month": r.month.isoformat(),
            "sector": r.sector,
            "cci_index": float(r.cci_index),
            "source_url": r.source_url,
        }
        for r in rows
    ]
    return {"items": items}


@router.get("/indices/rates")
def get_rates(
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict[str, list[dict]]:
    q = db.query(Rate)
    if date_from:
        q = q.filter(Rate.date >= date.fromisoformat(date_from))
    if date_to:
        q = q.filter(Rate.date <= date.fromisoformat(date_to))
    rows = q.order_by(Rate.date.desc()).all()
    items = [
        {
            "date": r.date.isoformat(),
            "tenor": r.tenor,
            "rate_type": r.rate_type,
            "value": float(r.value),
            "source_url": r.source_url,
        }
        for r in rows
    ]
    return {"items": items}
