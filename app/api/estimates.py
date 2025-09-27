from typing import Any, Dict, List, Literal, Optional
from datetime import date
import json, uuid, csv, io

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.services import geo as geo_svc
from app.services.hedonic import land_price_per_m2
from app.services.costs import compute_hard_costs
from app.services.financing import compute_financing
from app.services.proforma import assemble
from app.services.revenue import build_to_sell_revenue, build_to_lease_revenue
from app.services.simulate import p_bands
from app.models.tables import EstimateHeader, EstimateLine

router = APIRouter(tags=["estimates"])


class UnitMix(BaseModel):
    type: str
    count: int
    avg_m2: float | None = None  # optional per-type area


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
    city: Optional[str] = None
    far: float = 2.0
    efficiency: float = 0.82


def _nfa_from_mix(site_m2: float, far: float, eff: float, mix: List[UnitMix]) -> float:
    # Simple: FAR × site × efficiency (unit mix fine-tunes later)
    base = site_m2 * far * eff
    # If avg areas provided, constrain to sum(units × avg_m2) if smaller
    mix_total = sum([(u.avg_m2 or 0.0) * u.count for u in mix])
    return min(base, mix_total) if mix_total > 0 else base


@router.post("/estimates")
def create_estimate(req: EstimateRequest, db: Session = Depends(get_db)) -> dict[str, Any]:
    # Geometry → area
    geom = geo_svc.parse_geojson(req.geometry)
    if geom.is_empty:
        raise HTTPException(status_code=400, detail="Empty geometry provided")
    site_area_m2 = geo_svc.area_m2(geom)

    # Land value (hedonic/median comps)
    ppm2, meta = land_price_per_m2(db, city=req.city, since=None)
    if not ppm2:
        ppm2 = 2800.0
    meta = meta or {}
    land_value = site_area_m2 * ppm2

    # Hard + soft
    asof = date.today().replace(day=1)
    hard = compute_hard_costs(db, area_m2=site_area_m2, month=asof)
    hard_costs = hard["total"]
    soft_costs = hard_costs * 0.15  # MVP param

    # Program area
    nfa = _nfa_from_mix(site_area_m2, req.far, req.efficiency, req.unit_mix)

    # Revenue
    if req.strategy == "build_to_sell":
        rev = build_to_sell_revenue(db, net_floor_area_m2=nfa, city=req.city, asset_type="residential")
    elif req.strategy == "build_to_lease":
        rev = build_to_lease_revenue(db, net_floor_area_m2=nfa, city=req.city, asset_type="residential")
    else:
        # Hotel path can plug here (ADR/Occ later per roadmap)
        rev = build_to_lease_revenue(db, net_floor_area_m2=nfa, city=req.city, asset_type="hospitality")

    # Financing
    fin = compute_financing(
        db,
        hard_plus_soft=hard_costs + soft_costs,
        months=req.timeline.months,
        margin_bps=req.financing_params.margin_bps,
        ltv=req.financing_params.ltv,
        asof=asof,
    )

    # Totals + uncertainty
    result = assemble(
        land_value=land_value,
        hard_costs=hard_costs,
        soft_costs=soft_costs,
        financing_interest=fin["interest"],
        revenues=rev["gdv"],
    )
    bands = p_bands(result["totals"]["p50_profit"], drivers={"land_ppm2": (1.0, 0.10), "unit_cost": (1.0, 0.08), "gdv_m2_price": (1.0, 0.10)})
    result["confidence_bands"] = bands
    result["notes"] = {
        "site_area_m2": round(site_area_m2, 2),
        "nfa_m2": round(nfa, 2),
        "cci_scalar": hard.get("cci_scalar"),
        "financing_apr": fin["apr"],
        "revenue_lines": rev.get("lines", []),
    }
    result["assumptions"] = [
        {"key": "ppm2", "value": ppm2, "unit": "SAR/m2", "source_type": "Model" if meta.get("n_comps", 0) > 0 else "Manual"},
        {"key": "far", "value": req.far, "source_type": "Manual"},
        {"key": "efficiency", "value": req.efficiency, "source_type": "Manual"},
        {"key": "soft_cost_pct", "value": 0.15, "source_type": "Manual"},
        {"key": "ltv", "value": req.financing_params.ltv, "source_type": "Manual"},
        {"key": "margin_bps", "value": req.financing_params.margin_bps, "source_type": "Manual"},
    ]

    # Persist
    if all(hasattr(db, attr) for attr in ("add", "add_all", "commit")):
        est_id = str(uuid.uuid4())
        header = EstimateHeader(
            id=est_id,
            strategy=req.strategy,
            input_json=json.dumps(req.model_dump()),
            totals_json=json.dumps(result["totals"]),
            notes_json=json.dumps({"bands": bands, "notes": result["notes"]}),
        )
        db.add(header)
        # store lines (cost/revenue + assumptions)
        lines = []
        t = result["totals"]
        for k in ["land_value", "hard_costs", "soft_costs", "financing", "revenues", "p50_profit"]:
            lines.append(EstimateLine(estimate_id=est_id, category="cost" if k != "revenues" else "revenue", key=k, value=t[k], unit="SAR", source_type="Model", owner="api"))
        for a in result["assumptions"]:
            lines.append(EstimateLine(estimate_id=est_id, category="assumption", key=a["key"], value=a.get("value"), unit=a.get("unit"), source_type=a.get("source_type"), owner="api"))
        db.add_all(lines)
        db.commit()

        result["id"] = est_id
    else:
        result["id"] = None
    return result


@router.get("/estimates/{estimate_id}")
def get_estimate(estimate_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    header = db.get(EstimateHeader, estimate_id)
    if not header:
        raise HTTPException(status_code=404, detail="Estimate not found")
    totals = json.loads(header.totals_json)
    notes = json.loads(header.notes_json) if header.notes_json else {}
    rows = db.query(EstimateLine).filter(EstimateLine.estimate_id == estimate_id).all()
    assumptions = [
        {"key": r.key, "value": float(r.value) if r.value is not None else None, "unit": r.unit, "source_type": r.source_type}
        for r in rows if r.category == "assumption"
    ]
    return {"id": estimate_id, "strategy": header.strategy, "totals": totals, "assumptions": assumptions, "notes": notes}


class ScenarioPatch(BaseModel):
    far: float | None = None
    efficiency: float | None = None
    soft_cost_pct: float | None = None
    margin_bps: int | None = None
    ltv: float | None = None
    price_uplift_pct: float | None = None  # bump sale/rent price


@router.post("/estimates/{estimate_id}/scenario")
def scenario(estimate_id: str, patch: ScenarioPatch, db: Session = Depends(get_db)) -> dict[str, Any]:
    base = get_estimate(estimate_id, db)
    # Apply simple perturbations to totals (fast scenario; full re-solve can also be done)
    t = base["totals"].copy()
    # Price uplift affects revenues
    uplift = 1.0 + (patch.price_uplift_pct or 0.0) / 100.0
    t["revenues"] = t["revenues"] * uplift
    # Financing sensitivity via margin_bps (linear approx)
    if patch.margin_bps is not None:
        delta = (patch.margin_bps - 250) / 250.0  # relative to default 250 bps
        t["financing"] = t["financing"] * (1.0 + 0.4 * delta)
    # Soft-cost pct perturbation
    if patch.soft_cost_pct is not None:
        t["soft_costs"] = (t["hard_costs"] * patch.soft_cost_pct)
    t["p50_profit"] = t["revenues"] - (t["land_value"] + t["hard_costs"] + t["soft_costs"] + t["financing"])
    bands = p_bands(t["p50_profit"], drivers={"land_ppm2": (1.0, 0.10), "unit_cost": (1.0, 0.08), "gdv_m2_price": (1.0, 0.10)})
    return {"baseline": base["totals"], "scenario": t, "delta": {k: t[k] - base["totals"][k] for k in t}, "confidence_bands": bands}


@router.get("/estimates/{estimate_id}/export")
def export_estimate(estimate_id: str, format: Literal["json","csv"] = "json", db: Session = Depends(get_db)):
    base = get_estimate(estimate_id, db)
    if format == "json":
        return base
    # CSV (lines)
    rows = db.query(EstimateLine).filter(EstimateLine.estimate_id == estimate_id).all()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["category", "key", "value", "unit", "source_type", "url", "model_version", "owner", "created_at"])
    for r in rows:
        w.writerow([r.category, r.key, r.value, r.unit, r.source_type, r.url, r.model_version, r.owner, r.created_at])
    return Response(content=buf.getvalue(), media_type="text/csv")
