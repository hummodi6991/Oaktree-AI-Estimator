from typing import Any, Dict, List, Literal, Optional
from datetime import date
import json, uuid, csv, io, os

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.services import geo as geo_svc
from app.services import indicators as indicators_svc
from app.services.hedonic import land_price_per_m2
from app.services.costs import compute_hard_costs
from app.services.financing import compute_financing
from app.services.proforma import assemble
from app.services.simulate import p_bands
from app.services.explain import (
    heuristic_drivers,
    rent_heuristic_drivers,
    top_rent_comps,
    top_sale_comps,
    to_comp_dict,
    to_rent_comp_dict,
)
from app.services.pdf import build_memo_pdf
from app.services.residual import residual_land_value
from app.services.cashflow import build_equity_cashflow
from app.services.far_rules import lookup_far
from app.models.tables import EstimateHeader, EstimateLine, LandUseStat

router = APIRouter(tags=["estimates"])


_INMEM_HEADERS: dict[str, dict[str, Any]] = {}
_INMEM_LINES: dict[str, list[dict[str, Any]]] = {}


def _supports_sqlalchemy(db: Any) -> bool:
    """Return True when the dependency looks like a SQLAlchemy session."""

    required = ("add", "add_all", "commit", "query", "get")
    return all(hasattr(db, attr) for attr in required)


def _persist_inmemory(
    estimate_id: str,
    strategy: str,
    totals: dict[str, Any],
    notes: dict[str, Any],
    assumptions: list[dict[str, Any]],
    lines: list[dict[str, Any]],
) -> None:
    """Persist estimate data in a simple in-memory store (used in tests)."""

    _INMEM_HEADERS[estimate_id] = {
        "strategy": strategy,
        "totals": dict(totals),
        "notes": notes.copy() if isinstance(notes, dict) else dict(notes or {}),
        "assumptions": [dict(a) for a in assumptions],
    }
    _INMEM_LINES[estimate_id] = [dict(line) for line in lines]


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


class BtrParams(BaseModel):
    occupancy: float | None = None
    opex_ratio: float | None = None
    cap_rate: float | None = None


class ExplainabilityRow(BaseModel):
    name: str
    direction: str
    magnitude: float
    unit: Optional[str] = None

    model_config = ConfigDict(extra="ignore")


class RentComparable(BaseModel):
    id: str
    date: str | None = None
    city: Optional[str] = None
    district: Optional[str] = None
    sar_per_m2: Optional[float] = None
    source: Optional[str] = None
    source_url: Optional[str] = None

    model_config = ConfigDict(extra="ignore")


class RentBlock(BaseModel):
    drivers: List[ExplainabilityRow] = Field(default_factory=list)
    top_comps: List[RentComparable] = Field(default_factory=list)
    rent_comparables: List[RentComparable] = Field(default_factory=list)
    top_rent_comparables: List[RentComparable] = Field(default_factory=list)
    rent_price_per_m2: float | None = None
    rent_unit_rate: float | None = None
    rent_vacancy_pct: float | None = None
    rent_growth_pct: float | None = None


class EstimateResponseModel(BaseModel):
    id: str
    strategy: str
    totals: Dict[str, Any]
    assumptions: List[Dict[str, Any]]
    notes: Dict[str, Any]
    rent: RentBlock = Field(default_factory=RentBlock)

    model_config = ConfigDict(extra="allow")


def _default_timeline() -> "Timeline":
    # First day of the current month, 18-month program
    return Timeline(start=date.today().replace(day=1).isoformat(), months=18)


def _default_financing() -> "FinancingParams":
    return FinancingParams()


class EstimateRequest(BaseModel):
    # accept dict or JSON string (Swagger users often paste as text)
    geometry: Dict[str, Any] | str
    asset_program: str = "residential_midrise"
    unit_mix: List[UnitMix] = Field(default_factory=list)
    finish_level: Literal["low", "mid", "high"] = "mid"
    timeline: Timeline = Field(default_factory=_default_timeline)
    financing_params: FinancingParams = Field(default_factory=_default_financing)
    strategy: Literal["build_to_sell", "build_to_rent"] = Field(
        default="build_to_sell",
        description="Development exit strategy. Use 'build_to_sell' for GDV sales, 'build_to_rent' for NOI/cap exits.",
    )
    city: Optional[str] = None
    far: float = 2.0
    efficiency: float = 0.82
    sale_price_per_m2: float | None = Field(
        default=None,
        description="Optional override for sale price per square meter (SAR/m2).",
    )
    soft_cost_pct: float | None = Field(
        default=None,
        description="Soft cost percentage as share of hard costs. Defaults to environment configuration when omitted.",
    )
    btr_params: BtrParams | None = Field(
        default=None,
        description="Optional overrides for build-to-rent assumptions (occupancy, opex_ratio, cap_rate).",
    )
    model_config = ConfigDict(
        json_schema_extra={
            # Use OpenAPI "examples" so Swagger renders a runnable example
            "examples": [{
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [46.675, 24.713],
                            [46.676, 24.713],
                            [46.676, 24.714],
                            [46.675, 24.714],
                            [46.675, 24.713],
                        ]
                    ],
                },
                "asset_program": "residential_midrise",
                "timeline": {"start": "2025-10-01", "months": 18},
                "financing_params": {"margin_bps": 250, "ltv": 0.6},
                "strategy": "build_to_sell",
                "city": "Riyadh",
                "far": 2.0,
                "efficiency": 0.82,
            }]
        }
    )

@router.post("/estimates", response_model=EstimateResponseModel)
def create_estimate(req: EstimateRequest, db: Session = Depends(get_db)) -> EstimateResponseModel:
    # Geometry → area
    try:
        geom = geo_svc.parse_geojson(req.geometry)
    except Exception as exc:
        # Give a precise, user-friendly 400 instead of a cryptic 422
        raise HTTPException(status_code=400, detail=f"Invalid GeoJSON for 'geometry': {exc}")
    district = None
    try:
        district = geo_svc.infer_district_from_features(db, geom, layer="rydpolygons")
    except Exception:
        pass
    if geom.is_empty:
        raise HTTPException(status_code=400, detail="Empty geometry provided")
    site_area_m2 = geo_svc.area_m2(geom)

    far_source = "manual"
    auto_far_value = None
    try:
        auto_far_value = geo_svc.infer_far_from_features(db, geom, layer="rydpolygons")
        if auto_far_value:
            far_source = "external_feature/rydpolygons"
    except Exception:
        auto_far_value = None
    if (auto_far_value is None) and district:
        try:
            rule_far = lookup_far(db, city=req.city or "Riyadh", district=district)
            if rule_far:
                auto_far_value = rule_far
                far_source = "far_rule"
        except Exception:
            pass
    far_used = float(auto_far_value) if auto_far_value else float(req.far)
    efficiency = req.efficiency or 1.0
    nfa_m2 = site_area_m2 * far_used * efficiency

    # Land value (hedonic/median comps)
    ppm2, meta = land_price_per_m2(db, city=req.city, since=None, district=district)
    if not ppm2:
        ppm2 = 2800.0
    meta = meta or {}
    hedonic_land_value = site_area_m2 * ppm2

    # Hard + soft
    asof = date.today().replace(day=1)
    hard = compute_hard_costs(db, area_m2=site_area_m2, month=asof)
    hard_costs = hard["total"]
    default_soft = float(os.getenv("DEFAULT_SOFT_COST_PCT", "0.12"))
    soft_defaulted = (req.soft_cost_pct is None) or (req.soft_cost_pct <= 0)
    soft_pct = req.soft_cost_pct if (req.soft_cost_pct and req.soft_cost_pct > 0) else default_soft
    soft_costs = hard_costs * soft_pct

    rent_block: RentBlock = RentBlock()

    sale_indicator = indicators_svc.latest_sale_price_per_m2(db, city=req.city, district=district)
    if req.sale_price_per_m2 and req.sale_price_per_m2 > 0:
        sale_ppm2 = float(req.sale_price_per_m2)
        sale_source = "Manual"
    elif sale_indicator is not None:
        sale_ppm2 = sale_indicator
        sale_source = "Observed"
    else:
        sale_ppm2 = 5500.0
        sale_source = "Manual"
    sale_gdv = nfa_m2 * sale_ppm2

    rent_indicator = indicators_svc.latest_rent_per_m2(db, city=req.city, district=district)
    rent_unit_indicator = indicators_svc.latest_rent_unit_rate(db, city=req.city, district=district)
    rent_vacancy_indicator = indicators_svc.latest_rent_vacancy_pct(db, city=req.city, district=district)
    rent_growth_indicator = indicators_svc.latest_rent_growth_pct(db, city=req.city, district=district)
    if rent_indicator is not None:
        rent_ppm2 = rent_indicator
        rent_source = "Observed"
    else:
        rent_ppm2 = 220.0
        rent_source = "Manual"
    occ = (req.btr_params.occupancy if req.btr_params and req.btr_params.occupancy is not None else 0.92)
    opex = (req.btr_params.opex_ratio if req.btr_params and req.btr_params.opex_ratio is not None else 0.30)
    cap = (req.btr_params.cap_rate if req.btr_params and req.btr_params.cap_rate is not None else 0.07)
    rent_unit_rate = rent_unit_indicator
    rent_vacancy_pct = rent_vacancy_indicator
    rent_growth_pct = rent_growth_indicator
    nla_m2 = nfa_m2
    egi = rent_ppm2 * nla_m2 * occ * 12.0
    noi = egi * (1.0 - opex)
    if rent_vacancy_pct is None:
        try:
            occ_float = float(occ)
        except (TypeError, ValueError):
            occ_float = None
        if occ_float is not None and 0.0 <= occ_float <= 1.0:
            rent_vacancy_pct = max(0.0, 1.0 - occ_float)
    btr_value = noi / cap if cap else 0.0
    btr_notes = {
        "rent_per_m2_month": rent_ppm2,
        "occupancy": occ,
        "opex_ratio": opex,
        "cap_rate": cap,
        "nla_equals_nfa": True,
    }

    try:
        rent_rows = top_rent_comps(
            db,
            city=req.city,
            district=district,
            asset_type="residential",
            since=None,
            limit=10,
        )
        rent_comp_dicts = [to_rent_comp_dict(r) for r in rent_rows]
        rent_comparables: List[RentComparable] = []
        for comp in rent_comp_dicts:
            comp_id = comp.get("id") or comp.get("identifier")
            if not comp_id:
                comp_id = str(uuid.uuid4())
            rent_comparables.append(
                RentComparable(
                    id=str(comp_id),
                    date=comp.get("date"),
                    city=comp.get("city"),
                    district=comp.get("district"),
                    sar_per_m2=comp.get("sar_per_m2"),
                    source=comp.get("source"),
                    source_url=comp.get("source_url"),
                )
            )

        rent_driver_dicts = rent_heuristic_drivers(rent_ppm2, rent_rows)
        if req.strategy != "build_to_rent":
            rent_driver_dicts.insert(
                0,
                {
                    "name": "strategy_note",
                    "direction": "strategy is build_to_sell; rent shown for reference",
                    "magnitude": 0.0,
                },
            )
        rent_drivers = [ExplainabilityRow(**d) for d in rent_driver_dicts]

        rent_block = RentBlock(
            drivers=rent_drivers,
            top_comps=rent_comparables[:5],
            rent_comparables=rent_comparables,
            top_rent_comparables=rent_comparables[:5],
            rent_price_per_m2=float(rent_ppm2) if rent_ppm2 is not None else None,
            rent_unit_rate=float(rent_unit_rate) if rent_unit_rate is not None else None,
            rent_vacancy_pct=float(rent_vacancy_pct) if rent_vacancy_pct is not None else None,
            rent_growth_pct=float(rent_growth_pct) if rent_growth_pct is not None else None,
        )
    except Exception:
        rent_block = RentBlock()

    if req.strategy == "build_to_rent":
        revenues_value = btr_value
        revenue_lines = [
            {"key": "rent_per_m2_month", "value": rent_ppm2, "unit": "SAR/m2/mo", "source_type": rent_source},
            {"key": "occupancy", "value": occ, "unit": "ratio", "source_type": "Manual"},
            {"key": "opex_ratio", "value": opex, "unit": "ratio", "source_type": "Manual"},
            {"key": "cap_rate", "value": cap, "unit": "ratio", "source_type": "Manual"},
        ]
    else:
        revenues_value = sale_gdv
        revenue_lines = [
            {"key": "sale_price_per_m2", "value": sale_ppm2, "unit": "SAR/m2", "source_type": sale_source},
        ]

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
        land_value=hedonic_land_value,
        hard_costs=hard_costs,
        soft_costs=soft_costs,
        financing_interest=fin["interest"],
        revenues=revenues_value,
    )
    result["totals"]["revenues"] = revenues_value
    result["notes"] = {
        "site_area_m2": round(site_area_m2, 2),
        "nfa_m2": nfa_m2,
        "cci_scalar": hard.get("cci_scalar"),
        "financing_apr": fin["apr"],
        "revenue_lines": revenue_lines,
        "land_model": meta.get("model"),  # shows {"model_used": true/false, mape, n_rows}
        "district": district,
        "far_source": far_source,
        "soft_cost_pct_defaulted": soft_defaulted,
    }
    if req.strategy == "build_to_rent":
        result["notes"]["btr"] = btr_notes
    result["assumptions"] = [
        {"key": "ppm2", "value": ppm2, "unit": "SAR/m2", "source_type": "Model" if meta.get("n_comps", 0) > 0 else "Manual"},
        {
            "key": "far",
            "value": far_used,
            "source_type": "Observed" if far_source != "manual" else "Manual",
        },
        {"key": "efficiency", "value": efficiency, "source_type": "Manual"},
        {
            "key": "soft_cost_pct",
            "value": soft_pct,
            "source_type": "Manual" if not soft_defaulted else "Model",
        },
        {"key": "ltv", "value": req.financing_params.ltv, "source_type": "Manual"},
        {"key": "margin_bps", "value": req.financing_params.margin_bps, "source_type": "Manual"},
    ]
    if req.strategy == "build_to_rent":
        result["assumptions"].extend(
            [
                {"key": "rent_per_m2_month", "value": rent_ppm2, "unit": "SAR/m2/mo", "source_type": rent_source},
                {"key": "occupancy", "value": occ, "unit": "ratio", "source_type": "Manual"},
                {"key": "opex_ratio", "value": opex, "unit": "ratio", "source_type": "Manual"},
                {"key": "cap_rate", "value": cap, "unit": "ratio", "source_type": "Manual"},
            ]
        )
    else:
        result["assumptions"].append(
            {"key": "sale_price_per_m2", "value": sale_ppm2, "unit": "SAR/m2", "source_type": sale_source}
        )
    # Explainability (top comps + drivers)
    comps_rows = top_sale_comps(
        db,
        city=req.city,
        district=district,
        asset_type="land",
        since=None,
        limit=10,
    )
    result["explainability"] = {
        "top_comps": [to_comp_dict(r) for r in comps_rows],
        "drivers": heuristic_drivers(ppm2, comps_rows),
    }

    result["rent"] = rent_block.model_dump()

    # --- Residual land value & combiner (simple weight by comps density) ---
    rlv = residual_land_value(revenues_value, hard_costs, soft_costs, fin["interest"], dev_margin_pct=0.15)
    comps_n = meta.get("n_comps", 0) or 0
    w_hedonic = min(1.0, comps_n / 8.0)
    w_resid = 1.0 - w_hedonic
    combined_land = w_hedonic * hedonic_land_value + w_resid * rlv
    result["land_value_breakdown"] = {
        "hedonic": hedonic_land_value,
        "residual": rlv,
        "combined": combined_land,
        "weights": {"hedonic": w_hedonic, "residual": w_resid},
        "comps_used": comps_n,
    }
    result["totals"]["land_value"] = combined_land
    result["totals"]["p50_profit"] = result["totals"]["revenues"] - (
        combined_land + result["totals"]["hard_costs"] + result["totals"]["soft_costs"] + result["totals"]["financing"]
    )
    res_share = 0.0
    if _supports_sqlalchemy(db):
        try:
            city = req.city or "Riyadh"
            total_area = (
                db.query(func.sum(LandUseStat.value))
                .filter(LandUseStat.city == city)
                .filter(LandUseStat.metric.ilike("%area%"))
                .filter(LandUseStat.value.isnot(None))
                .scalar()
            ) or 0.0
            residential_area = (
                db.query(func.sum(LandUseStat.value))
                .filter(LandUseStat.city == city)
                .filter(LandUseStat.metric.ilike("%area%"))
                .filter(LandUseStat.category.ilike("%residential%"))
                .filter(LandUseStat.value.isnot(None))
                .scalar()
            ) or 0.0
            if float(total_area) > 0:
                res_share = float(residential_area) / float(total_area)
        except Exception:
            res_share = 0.0

    unit_cost_sigma = 0.06 if res_share >= 0.35 else 0.08
    result["confidence_bands"] = p_bands(
        result["totals"]["p50_profit"],
        drivers={"land_ppm2": (1.0, 0.10), "unit_cost": (1.0, unit_cost_sigma), "gdv_m2_price": (1.0, 0.10)},
    )
    bands = result["confidence_bands"]
    result["notes"]["land_use_residential_share"] = res_share

    # --- Monthly cashflow & IRR (equity view) ---
    cf = build_equity_cashflow(
        months=req.timeline.months,
        land_value=combined_land,
        hard_costs=hard_costs,
        soft_costs=soft_costs,
        gdv=revenues_value,
        apr=fin["apr"],
        ltv=req.financing_params.ltv,
        sales_cost_pct=0.02,
    )
    result["metrics"] = {"irr_annual": cf["irr_annual"]}
    result["cashflow"] = {"monthly": cf["schedule"], "peaks": cf["peaks"]}

    # Persist
    est_id = str(uuid.uuid4())
    totals = result["totals"]
    notes_payload = {"bands": bands, "notes": result["notes"]}
    assumptions = result["assumptions"]
    line_dicts: list[dict[str, Any]] = []
    for key in ["land_value", "hard_costs", "soft_costs", "financing", "revenues", "p50_profit"]:
        line_dicts.append(
            {
                "estimate_id": est_id,
                "category": "cost" if key != "revenues" else "revenue",
                "key": key,
                "value": totals[key],
                "unit": "SAR",
                "source_type": "Model",
                "owner": "api",
                "url": None,
                "model_version": None,
                "created_at": None,
            }
        )
    for assumption in assumptions:
        line_dicts.append(
            {
                "estimate_id": est_id,
                "category": "assumption",
                "key": assumption["key"],
                "value": assumption.get("value"),
                "unit": assumption.get("unit"),
                "source_type": assumption.get("source_type"),
                "owner": "api",
                "url": None,
                "model_version": None,
                "created_at": None,
            }
        )

    if _supports_sqlalchemy(db):
        orm_lines = [EstimateLine(**entry) for entry in line_dicts]
        header = EstimateHeader(
            id=est_id,
            strategy=req.strategy,
            input_json=json.dumps(req.model_dump()),
            totals_json=json.dumps(totals),
            notes_json=json.dumps(notes_payload),
        )
        db.add(header)
        db.add_all(orm_lines)
        try:
            db.commit()
        except Exception:
            db.rollback()
            raise
    else:
        _persist_inmemory(est_id, req.strategy, totals, notes_payload, assumptions, line_dicts)

    result["id"] = est_id
    result["strategy"] = req.strategy
    return EstimateResponseModel.model_validate(result)


@router.get("/estimates/{estimate_id}")
def get_estimate(estimate_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    if _supports_sqlalchemy(db):
        header = db.get(EstimateHeader, estimate_id)
        if header:
            totals = json.loads(header.totals_json)
            notes = json.loads(header.notes_json) if header.notes_json else {}
            rows = db.query(EstimateLine).filter(EstimateLine.estimate_id == estimate_id).all()
            assumptions_source: list[dict[str, Any]] = [
                {
                    "key": r.key,
                    "value": float(r.value) if r.value is not None else None,
                    "unit": r.unit,
                    "source_type": r.source_type,
                }
                for r in rows
                if r.category == "assumption"
            ]
            strategy = header.strategy
        else:
            record = _INMEM_HEADERS.get(estimate_id)
            if record is None:
                raise HTTPException(status_code=404, detail="Estimate not found")
            totals = dict(record.get("totals", {}))
            notes = record.get("notes", {})
            assumptions_source = list(record.get("assumptions", []))
            strategy = record.get("strategy")
    else:
        record = _INMEM_HEADERS.get(estimate_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Estimate not found")
        totals = dict(record.get("totals", {}))
        notes = record.get("notes", {})
        assumptions_source = list(record.get("assumptions", []))
        strategy = record.get("strategy")

    normalized_assumptions = []
    for item in assumptions_source:
        value = item.get("value")
        if value is not None:
            try:
                value = float(value)
            except (TypeError, ValueError):
                pass
        normalized_assumptions.append(
            {
                "key": item.get("key"),
                "value": value,
                "unit": item.get("unit"),
                "source_type": item.get("source_type"),
            }
        )

    notes_dict = notes.copy() if isinstance(notes, dict) else dict(notes or {})

    return {
        "id": estimate_id,
        "strategy": strategy,
        "totals": totals,
        "assumptions": normalized_assumptions,
        "notes": notes_dict,
    }



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
    # Fast scenario: scale area-driven items by NFA ratio; then apply margin/softcost/price tweaks.
    base_totals = base["totals"]
    t = base_totals.copy()
    notes = base.get("notes", {}) or {}
    assumptions = base.get("assumptions", []) or []

    def _assumption_value(key: str, default: float) -> float:
        for item in assumptions:
            if item.get("key") == key:
                val = item.get("value")
                try:
                    return float(val)
                except (TypeError, ValueError):
                    return default
        return default

    site_m2 = float(notes.get("site_area_m2") or 0.0)
    base_nfa = float(notes.get("nfa_m2") or (site_m2 * 2.0 * 0.82))
    new_far = patch.far if patch.far is not None else _assumption_value("far", 2.0)
    new_eff = patch.efficiency if patch.efficiency is not None else _assumption_value("efficiency", 0.82)
    new_nfa = site_m2 * new_far * new_eff if site_m2 > 0 else base_nfa
    area_ratio = (new_nfa / base_nfa) if base_nfa > 0 else 1.0

    t["hard_costs"] = t["hard_costs"] * area_ratio
    t["revenues"] = t["revenues"] * area_ratio

    base_soft_ratio = (
        (base_totals.get("soft_costs", 0.0) / base_totals.get("hard_costs", 0.0))
        if base_totals.get("hard_costs")
        else 0.15
    )
    base_cost_sum = base_totals.get("hard_costs", 0.0) + base_totals.get("soft_costs", 0.0)
    base_financing_ratio = (base_totals.get("financing", 0.0) / base_cost_sum) if base_cost_sum else 0.6
    # Price uplift affects revenues
    uplift = 1.0 + (patch.price_uplift_pct or 0.0) / 100.0
    t["revenues"] = t["revenues"] * uplift
    # Financing sensitivity via margin_bps (linear approx)
    if patch.soft_cost_pct is not None:
        t["soft_costs"] = t["hard_costs"] * patch.soft_cost_pct
    else:
        t["soft_costs"] = t["hard_costs"] * base_soft_ratio

    t["financing"] = base_financing_ratio * (t["hard_costs"] + t["soft_costs"])
    if patch.margin_bps is not None:
        delta = (patch.margin_bps - 250) / 250.0  # relative to default 250 bps
        t["financing"] = t["financing"] * (1.0 + 0.4 * delta)
    t["p50_profit"] = t["revenues"] - (t["land_value"] + t["hard_costs"] + t["soft_costs"] + t["financing"])
    bands = p_bands(t["p50_profit"], drivers={"land_ppm2": (1.0, 0.10), "unit_cost": (1.0, 0.08), "gdv_m2_price": (1.0, 0.10)})
    return {"baseline": base_totals, "scenario": t, "delta": {k: t[k] - base_totals[k] for k in t}, "confidence_bands": bands}


@router.get("/estimates/{estimate_id}/export")
def export_estimate(estimate_id: str, format: Literal["json","csv"] = "json", db: Session = Depends(get_db)):
    base = get_estimate(estimate_id, db)
    if format == "json":
        return base
    # CSV (lines)
    rows_data: list[dict[str, Any]] = []
    if _supports_sqlalchemy(db):
        rows = db.query(EstimateLine).filter(EstimateLine.estimate_id == estimate_id).all()
        rows_data = [
            {
                "category": r.category,
                "key": r.key,
                "value": r.value,
                "unit": r.unit,
                "source_type": r.source_type,
                "url": getattr(r, "url", None),
                "model_version": getattr(r, "model_version", None),
                "owner": r.owner,
                "created_at": getattr(r, "created_at", None),
            }
            for r in rows
        ]
    if not rows_data:
        fallback_lines = _INMEM_LINES.get(estimate_id, [])
        rows_data = [dict(line) for line in fallback_lines]
    if not rows_data:
        raise HTTPException(status_code=404, detail="Estimate lines not found")

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["category", "key", "value", "unit", "source_type", "url", "model_version", "owner", "created_at"])
    for row in rows_data:
        w.writerow(
            [
                row.get("category"),
                row.get("key"),
                row.get("value"),
                row.get("unit"),
                row.get("source_type"),
                row.get("url"),
                row.get("model_version"),
                row.get("owner"),
                row.get("created_at"),
            ]
        )
    return Response(content=buf.getvalue(), media_type="text/csv")


@router.get("/estimates/{estimate_id}/ledger")
def get_ledger(estimate_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    """Return a flattened ledger of estimate lines and assumptions."""

    base = get_estimate(estimate_id, db)

    items: list[dict[str, Any]] = []
    if _supports_sqlalchemy(db):
        rows = db.query(EstimateLine).filter(EstimateLine.estimate_id == estimate_id).all()
        items = [
            {
                "category": r.category,
                "key": r.key,
                "value": r.value,
                "unit": r.unit,
                "source_type": r.source_type,
                "url": getattr(r, "url", None),
                "model_version": getattr(r, "model_version", None),
                "owner": r.owner,
                "created_at": getattr(r, "created_at", None),
            }
            for r in rows
        ]
    if not items:
        fallback_lines = _INMEM_LINES.get(estimate_id, [])
        items = [dict(line) for line in fallback_lines]

    if not items:
        # Build a minimal ledger from the totals as a fallback (e.g., legacy estimates).
        totals = base.get("totals", {}) or {}
        notes = base.get("notes", {}) or {}
        if isinstance(notes, dict):
            if "land_model" in notes:
                land_model = notes.get("land_model") or {}
            else:
                land_model = notes.get("notes", {}).get("land_model", {})
        else:
            land_model = {}
        for key in ["land_value", "hard_costs", "soft_costs", "financing", "revenues", "p50_profit"]:
            value = totals.get(key)
            if value is None:
                continue
            if key == "revenues":
                category = "revenue"
            elif key == "p50_profit":
                category = "profit"
            else:
                category = "cost"
            items.append(
                {
                    "category": category,
                    "key": key,
                    "value": value,
                    "unit": "SAR",
                    "source_type": "Model",
                    "url": None,
                    "model_version": "hedonic_v0" if land_model.get("model_used") else None,
                    "owner": None,
                    "created_at": None,
                }
            )

    seen = {(item.get("category"), item.get("key")) for item in items}
    for assumption in base.get("assumptions", []) or []:
        identifier = ("assumption", assumption.get("key"))
        if identifier in seen:
            continue
        items.append(
            {
                "category": "assumption",
                "key": assumption.get("key"),
                "value": assumption.get("value"),
                "unit": assumption.get("unit"),
                "source_type": assumption.get("source_type"),
                "url": assumption.get("url"),
                "model_version": assumption.get("model_version"),
                "owner": None,
                "created_at": None,
            }
        )

    return {"items": items}


@router.get("/estimates/{estimate_id}/memo.pdf")
def export_pdf(estimate_id: str, db: Session = Depends(get_db)):
    base = get_estimate(estimate_id, db)
    comps_rows = top_sale_comps(db, city=None, district=None, asset_type="land", since=None, limit=8)
    comps = [to_comp_dict(r) for r in comps_rows]
    try:
        pdf_bytes = build_memo_pdf(
            title=f"Estimate {estimate_id}",
            totals=base["totals"],
            assumptions=base.get("assumptions", []),
            top_comps=comps,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="estimate_{estimate_id}.pdf"'},
    )
