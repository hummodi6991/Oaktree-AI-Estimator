from typing import Any, Dict, List, Literal, Optional
from datetime import date
import json, uuid, csv, io

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.services import geo as geo_svc
from app.services.excel_method import compute_excel_estimate
from app.services.explain import (
    top_sale_comps,
    to_comp_dict,
)
from app.services.pdf import build_memo_pdf
from app.services.pricing import price_from_kaggle_hedonic, price_from_aqar
from app.services.costs import latest_cci_scalar
from app.services.indicators import (
    latest_rent_per_m2,
    latest_re_price_index_scalar,
    latest_sale_price_per_m2,
)
from app.services.tax import latest_tax_rate
from app.models.tables import EstimateHeader, EstimateLine

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
    # Excel-only mode: require excel_inputs and route exclusively through the Excel method.
    excel_inputs: dict = Field(
        ...,
        description="Required. Parameters for the Excel-style method (see app/services/excel_method.py).",
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
                "excel_inputs": {
                    "area_ratio": {"residential": 1.6, "basement": 0.5},
                    "unit_cost": {"residential": 2200, "basement": 1200},
                    "efficiency": {"residential": 0.82},
                    "cp_sqm_per_space": {"basement": 30},
                    "rent_sar_m2_yr": {"residential": 2400},
                    "fitout_rate": 400,
                    "contingency_pct": 0.10,
                    "consultants_pct": 0.06,
                    "feasibility_fee": 1500000,
                    "transaction_pct": 0.03,
                    "land_price_sar_m2": 2800,
                }
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

    def _finalize_result(
        result: dict[str, Any], bands: dict[str, Any] | None = None
    ) -> EstimateResponseModel:
        payload_bands = bands or {}
        est_id = str(uuid.uuid4())
        totals = result["totals"]
        assumptions = result.get("assumptions", [])
        notes_payload = {"bands": payload_bands, "notes": result["notes"]}

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

    # Excel-only mode: always use the Excel-style computation and short-circuit other paths.
    if req.excel_inputs:
        # Copy inputs and enrich land price if caller didn't provide one (or provided 0/None)
        excel_inputs = dict(req.excel_inputs)
        ppm2_val: float = 0.0
        ppm2_src: str = "Manual"
        try:
            override = excel_inputs.get("land_price_sar_m2")
            override_f = float(override) if override is not None else 0.0
        except Exception:
            override_f = 0.0
        if override_f <= 0.0:
            # Use Kaggle hedonic v0 model (same logic as /v1/pricing/land)
            value, method, meta = price_from_kaggle_hedonic(
                db,
                city=req.city or "Riyadh",
                lon=None,
                lat=None,
                district=district,
            )
            if value is not None:
                ppm2_val = float(value)
                ppm2_src = method or (meta.get("source") if isinstance(meta, dict) else "kaggle_hedonic_v0")
                excel_inputs["land_price_sar_m2"] = ppm2_val
            else:
                # Fallback: previous Kaggle median logic (aqar.mv_city_price_per_sqm / aqar.listings)
                aqar = price_from_aqar(db, req.city or "Riyadh", district)
                if aqar:
                    ppm2_val, ppm2_src = float(aqar[0]), aqar[1]
                    excel_inputs["land_price_sar_m2"] = ppm2_val

        else:
            ppm2_val, ppm2_src = override_f, "Manual"

        asof = date.today()
        try:
            if req.timeline and req.timeline.start:
                asof = date.fromisoformat(req.timeline.start)
        except (TypeError, ValueError):
            pass

        cci_scalar = latest_cci_scalar(db, asof)
        excel_inputs["cci_scalar"] = cci_scalar
        excel_inputs["cci_asof_date"] = asof.isoformat()

        # New: GASTAT real estate price index scalar (2014=1.0)
        re_scalar = latest_re_price_index_scalar(db, asset_type="Residential")
        excel_inputs["re_price_index_scalar"] = re_scalar

        # Always override Excel rent with REGA rent_per_m2 when available.
        rega_rent_monthly = latest_rent_per_m2(db, req.city, district)
        if rega_rent_monthly is not None:
            annual_rent = float(rega_rent_monthly) * 12.0  # SAR/m²/year
            rent_rates = {"residential": annual_rent}
            excel_inputs["rent_sar_m2_yr"] = rent_rates
            excel_inputs["rent_source_metadata"] = {
                "provider": "REGA",
                "indicator_type": "rent_per_m2",
                "unit_input": "SAR/m2/mo",
                "unit_excel": "SAR/m2/yr",
                "city": req.city,
                "district": district,
                "sar_per_m2_month": float(rega_rent_monthly),
                "sar_per_m2_year": annual_rent,
            }
        else:
            # Fallback: keep any manual rent if REGA doesn’t have a benchmark here
            rent_rates = excel_inputs.get("rent_sar_m2_yr") or {}

        rett = latest_tax_rate(db, tax_type="RETT")
        if rett:
            rett_rate = float(rett.get("rate") or 0.0)
            base_type = rett.get("base_type")
            # Always use RETT rate as the default transaction_pct
            excel_inputs["transaction_pct"] = rett_rate
            label = "Real Estate Transaction Tax (RETT)"
            if base_type:
                label += f" on {base_type}"
            excel_inputs["transaction_label"] = label
            excel_inputs["transaction_tax_metadata"] = {
                "tax_type": rett.get("tax_type"),
                "rule_id": rett.get("rule_id"),
                "rate": rett_rate,
                "base_type": base_type,
            }

        excel = compute_excel_estimate(site_area_m2, excel_inputs)
        totals = {
            "land_value": float(excel["land_cost"]),
            "hard_costs": float(excel["sub_total"]),
            "soft_costs": float(
                excel["contingency_cost"]
                + excel["consultants_cost"]
                + excel["feasibility_fee"]
                + excel["transaction_cost"]
            ),
            "financing": 0.0,
            "revenues": float(excel["y1_income"]),
            "p50_profit": float(excel["y1_income"] - excel["grand_total_capex"]),
            # Echo ROI for the UI dialog:
            "excel_roi": float(excel["roi"]),
        }
        direct_cost_total = float(sum(excel.get("direct_cost", {}).values()))
        cost_breakdown = {
            "land_cost": float(excel["land_cost"]),
            "construction_direct_cost": direct_cost_total,
            "fitout_cost": float(excel["fitout_cost"]),
            "contingency_cost": float(excel["contingency_cost"]),
            "consultants_cost": float(excel["consultants_cost"]),
            "feasibility_fee": float(excel["feasibility_fee"]),
            "transaction_cost": float(excel["transaction_cost"]),
            "grand_total_capex": float(excel["grand_total_capex"]),
            "y1_income": float(excel["y1_income"]),
            "roi": float(excel["roi"]),
        }
        if ppm2_src.startswith("kaggle_hedonic"):
            land_source_label = "the Kaggle hedonic v0 land price model"
        elif ppm2_src.startswith("aqar."):
            land_source_label = "Kaggle aqar.fm median land price/m²"
        elif ppm2_src == "Manual":
            land_source_label = "a manually entered land price"
        else:
            land_source_label = ppm2_src or "the configured land price source"

        # Base narrative
        summary_text = (
            f"For a site of {site_area_m2:,.0f} m² in "
            f"{district or (req.city or 'the selected city')}, "
            f"land is valued at {excel['land_cost']:,.0f} SAR based on {land_source_label}. "
            f"Construction and fit-out total "
            f"{excel['sub_total']:,.0f} SAR, with contingency, "
            f"consultants, feasibility fees and transaction costs bringing total capex to "
            f"{excel['grand_total_capex']:,.0f} SAR. "
        )

        # Append rent / income explanation
        rent_meta = excel_inputs.get("rent_source_metadata") or {}
        rent_rates = excel_inputs.get("rent_sar_m2_yr") or {}
        residential_rent = None
        if isinstance(rent_rates, dict):
            residential_rent = rent_rates.get("residential") or next(
                iter(rent_rates.values()), None
            )

        if residential_rent:
            monthly_rent = float(residential_rent) / 12.0
            if rent_meta.get("provider") == "REGA":
                loc_label = (
                    rent_meta.get("district")
                    or rent_meta.get("city")
                    or (req.city or "the selected city")
                )
                summary_text += (
                    f"Year 1 net income assumes REGA residential benchmark rent of "
                    f"{monthly_rent:,.0f} SAR/m²/month "
                    f"({residential_rent:,.0f} SAR/m²/year) in {loc_label}. "
                )
            else:
                summary_text += (
                    f"Year 1 net income assumes average rent of "
                    f"{monthly_rent:,.0f} SAR/m²/month "
                    f"({residential_rent:,.0f} SAR/m²/year). "
                )

        summary_text += (
            f"Year 1 net income of {excel['y1_income']:,.0f} SAR implies an unlevered ROI "
            f"of {excel['roi']*100:,.1f}%."
        )
        result = {
            "totals": totals,
            "assumptions": [
                {"key": "excel_method", "value": 1, "unit": None, "source_type": "Manual"},
                {"key": "site_area_m2", "value": site_area_m2, "unit": "m2", "source_type": "Observed"},
                {"key": "ppm2", "value": float(ppm2_val), "unit": "SAR/m2", "source_type": ppm2_src},
                {
                    "key": "real_estate_price_index_scalar",
                    "value": float(re_scalar),
                    "unit": "2014=1.0",
                    "source_type": "GASTAT",
                },
            ],
            "notes": {
                "excel_inputs_keys": list(excel_inputs.keys()),
                "excel_breakdown": excel,
                "cost_breakdown": cost_breakdown,
                "cci_scalar": cci_scalar,
                "re_price_index_scalar": re_scalar,
                "transaction_tax": excel_inputs.get("transaction_tax_metadata"),
                "summary": summary_text,
                "site_area_m2": site_area_m2,
                "district": district,
                "excel_land_price": {
                    "ppm2": float(ppm2_val),
                    "source_type": ppm2_src,
                },
                "excel_rent": {
                    "rent_sar_m2_yr": excel_inputs.get("rent_sar_m2_yr"),
                    "rent_source_metadata": excel_inputs.get("rent_source_metadata"),
                },
                "excel_roi": excel["roi"],
            },
            "rent": {},
            "explainability": {},
            "confidence_bands": {},
        }
        return _finalize_result(result, {})

    # Defensive guard (should be unreachable because excel_inputs is required)
    raise HTTPException(status_code=400, detail="excel-only mode: 'excel_inputs' is required")


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
    notes = base.get("notes") if isinstance(base, dict) else {}
    try:
        pdf_bytes = build_memo_pdf(
            title=f"Estimate {estimate_id}",
            totals=base["totals"],
            assumptions=base.get("assumptions", []),
            top_comps=comps,
            excel_breakdown=notes.get("excel_breakdown"),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="estimate_{estimate_id}.pdf"'},
    )
