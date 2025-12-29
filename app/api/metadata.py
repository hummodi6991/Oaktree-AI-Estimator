from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.db.deps import get_db
from app.models.tables import Rate, MarketIndicator, SaleComp, RentComp
from app.services import parking as parking_svc

router = APIRouter(tags=["metadata"])

def _max_date(db: Session, col):
    val = db.query(func.max(col)).scalar()
    return val.isoformat() if val else None

@router.get("/metadata/freshness")
def freshness(db: Session = Depends(get_db)):
    return {
        "rates": _max_date(db, Rate.date),
        "market_indicator": _max_date(db, MarketIndicator.date),
        "sale_comp": _max_date(db, SaleComp.date),
        "rent_comp": _max_date(db, RentComp.date),
    }


@router.get("/metadata/parking-rules")
def parking_rules():
    """Expose the current parking ruleset used by the feasibility engine."""
    return {
        "ruleset_id": parking_svc.RIYADH_PARKING_RULESET_ID,
        "ruleset_name": parking_svc.RIYADH_PARKING_RULESET_NAME,
        "source_url": parking_svc.RIYADH_PARKING_RULESET_SOURCE_URL,
        "rules": parking_svc.RIYADH_RULES,
        "default_component_rule_map": parking_svc.DEFAULT_COMPONENT_RULE_MAP,
    }
