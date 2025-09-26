from datetime import date
from typing import Any, Dict, List, Literal

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.models.tables import AssumptionLedger
from app.services import geo as geo_svc
from app.services.costs import compute_hard_costs
from app.services.financing import compute_financing
from app.services.hedonic import land_price_per_m2
from app.services.proforma import assemble
from pydantic import BaseModel, Field

router = APIRouter(tags=["estimates"])


class UnitMix(BaseModel):
    type: str
    count: int


class Timeline(BaseModel):
    start: str
    months: int


class FinancingParams(BaseModel):
    margin_bps: int = 250
    ltv: float = 0.6


class EstimateRequest(BaseModel):
    geometry: Dict[str, Any]
    asset_program: str
    unit_mix: List[UnitMix] = Field(default_factory=list)
    finish_level: Literal["low", "mid", "high"] = "mid"
    timeline: Timeline
    financing_params: FinancingParams
    strategy: Literal["build_to_sell", "build_to_lease", "hotel"] = "build_to_sell"


@router.post("/estimates")
def create_estimate(req: EstimateRequest, db: Session = Depends(get_db)) -> dict[str, Any]:
    geom = geo_svc.parse_geojson(req.geometry)
    if geom.is_empty:
        raise HTTPException(status_code=400, detail="Empty geometry provided")
    site_area_m2 = geo_svc.area_m2(geom)

    ppm2, meta = land_price_per_m2(db, city=None, since=None)
    if not ppm2:
        ppm2 = 2800.0
        meta = meta or {}
    land_value = site_area_m2 * ppm2

    asof = date.today().replace(day=1)
    hard = compute_hard_costs(db, area_m2=site_area_m2, month=asof)
    hard_costs = hard.get("total", 0.0)
    soft_costs = hard_costs * 0.15

    fin = compute_financing(
        db,
        hard_plus_soft=hard_costs + soft_costs,
        months=req.timeline.months,
        margin_bps=req.financing_params.margin_bps,
        ltv=req.financing_params.ltv,
        asof=asof,
    )

    far = 2.0
    efficiency = 0.82
    sell_price_per_m2 = ppm2 * 2.0
    gdv = site_area_m2 * far * efficiency * sell_price_per_m2

    result = assemble(
        land_value=land_value,
        hard_costs=hard_costs,
        soft_costs=soft_costs,
        financing_interest=fin.get("interest", 0.0),
        revenues=gdv,
    )

    try:
        db.add_all(
            [
                AssumptionLedger(
                    estimate_id="ephemeral",
                    line_id="ppm2",
                    source_type="Model" if (meta or {}).get("n_comps", 0) > 0 else "Manual",
                    value=ppm2,
                    unit="SAR/m2",
                    url=None,
                    owner="api",
                ),
                AssumptionLedger(
                    estimate_id="ephemeral",
                    line_id="soft_cost_pct",
                    source_type="Manual",
                    value=0.15,
                    unit="ratio",
                    owner="api",
                ),
            ]
        )
        db.commit()
    except SQLAlchemyError:
        db.rollback()

    meta = meta or {}
    result["assumptions"] = [
        {
            "key": "ppm2",
            "value": ppm2,
            "source_type": "Model" if meta.get("n_comps", 0) > 0 else "Manual",
        },
        {"key": "site_area_m2", "value": round(site_area_m2, 2), "source_type": "Observed"},
        {"key": "soft_cost_pct", "value": 0.15, "source_type": "Manual"},
        {"key": "ltv", "value": req.financing_params.ltv, "source_type": "Manual"},
        {"key": "margin_bps", "value": req.financing_params.margin_bps, "source_type": "Manual"},
        {"key": "far", "value": far, "source_type": "Manual"},
        {"key": "efficiency", "value": efficiency, "source_type": "Manual"},
    ]
    result["notes"] = {
        "comps_used": meta.get("n_comps", 0),
        "cci_scalar": hard.get("cci_scalar"),
        "financing_apr": fin.get("apr"),
    }
    return result
