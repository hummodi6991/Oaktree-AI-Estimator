from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.db.deps import get_db
from app.models.tables import CostIndexMonthly, Rate, MarketIndicator, SaleComp, RentComp

router = APIRouter(tags=["metadata"])

def _max_date(db: Session, col):
    val = db.query(func.max(col)).scalar()
    return val.isoformat() if val else None

@router.get("/metadata/freshness")
def freshness(db: Session = Depends(get_db)):
    return {
        "cost_index_monthly": _max_date(db, CostIndexMonthly.month),
        "rates": _max_date(db, Rate.date),
        "market_indicator": _max_date(db, MarketIndicator.date),
        "sale_comp": _max_date(db, SaleComp.date),
        "rent_comp": _max_date(db, RentComp.date),
    }
