from typing import Any, Dict, List, Literal

from fastapi import APIRouter
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
def create_estimate(req: EstimateRequest) -> dict[str, Any]:
    land_value = 10_000_000
    hard_costs = 25_000_000
    soft_costs = 4_000_000
    financing = 2_000_000
    revenues = 45_000_000

    total_cost = land_value + hard_costs + soft_costs + financing
    p50_profit = revenues - total_cost
    irr_guess = 0.18

    return {
        "totals": {
            "land_value": land_value,
            "hard_costs": hard_costs,
            "soft_costs": soft_costs,
            "financing": financing,
            "revenues": revenues,
            "p50_profit": p50_profit,
            "irr_guess": irr_guess,
        },
        "confidence_bands": {
            "p5": p50_profit * 0.6,
            "p50": p50_profit,
            "p95": p50_profit * 1.4,
        },
        "assumptions": [
            {"key": "finish_level", "value": req.finish_level, "source_type": "Manual"},
            {"key": "ltv", "value": req.financing_params.ltv, "source_type": "Manual"},
        ],
    }
