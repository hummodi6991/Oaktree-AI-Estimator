from datetime import date
from fastapi import APIRouter, Query, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session
from app.db.deps import get_db
from app.models.tables import CostIndexMonthly, Rate

router = APIRouter(tags=["indices"])

CCI_SAMPLE = [
    {
        "month": "2025-06-01",
        "sector": "construction",
        "cci_index": 108.9,
        "source_url": "https://example-cci",
    }
]

RATES_SAMPLE = [
    {
        "date": "2025-06-01",
        "tenor": "overnight",
        "rate_type": "SAMA_base",
        "value": 6.00,
        "source_url": "https://example-sama",
    },
    {
        "date": "2025-06-01",
        "tenor": "1M",
        "rate_type": "SAIBOR",
        "value": 6.1,
        "source_url": "https://example-sama",
    },
]


@router.get("/indices/cci")
def get_cci(
    month: str | None = Query(default=None),
    sector: str = Query(default="construction"),
    db: Session = Depends(get_db),
) -> dict[str, list[dict]]:
    # Try DB first
    try:
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
        if items:
            return {"items": items}
    except Exception:
        pass
    # Fallback (unchanged behavior)
    items = CCI_SAMPLE
    if month:
        items = [record for record in items if record["month"][0:7] == month[0:7]]
    return {"items": items}


@router.get("/indices/rates")
def get_rates(
    date_str: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict[str, list[dict]]:
    try:
        q = db.query(Rate)
        if date_str:
            q = q.filter(Rate.date == date.fromisoformat(date_str))
        rows = q.order_by(Rate.date.desc(), Rate.tenor.asc()).all()
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
        if items:
            return {"items": items}
    except Exception:
        pass
    items = RATES_SAMPLE
    if date_str:
        items = [record for record in items if record["date"] == date_str]
    return {"items": items}
