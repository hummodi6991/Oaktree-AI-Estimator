from typing import Any, Dict, List, Literal, Optional
from datetime import date, datetime, timezone
import os
import json, uuid, csv, io
import logging

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import inspect
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.db.deps import get_db
from app.core.config import settings
from app.db import session as db_session
from app.services import geo as geo_svc
from app.services import far_rules
from app.services import parking as parking_svc
from app.services.excel_method import (
    DEFAULT_Y1_INCOME_EFFECTIVE_FACTOR,
    _normalize_y1_income_effective_factor,
    compute_excel_estimate,
    scale_placeholder_area_ratio,
    scale_area_ratio_by_floors,
)
from app.services.explain import (
    top_sale_comps,
    to_comp_dict,
)
from app.services.pdf import build_memo_pdf
from app.services.land_price_engine import quote_land_price_blended_v1
from app.services.pricing import price_from_kaggle_hedonic, price_from_aqar
from app.services.simulate import p_bands
from app.services.indicators import (
    latest_rega_residential_rent_per_m2,
    latest_re_price_index_scalar,
    latest_sale_price_per_m2,
)
from app.services.rent import RentMedianResult, aqar_rent_median
from app.services.district_resolver import resolve_district, resolution_meta
from app.services.overture_buildings_metrics import compute_building_metrics
from app.services.tax import latest_tax_rate
from app.models.tables import EstimateHeader, EstimateLine, UsageEvent
from app.ml.name_normalization import norm_city
from app.security import auth

router = APIRouter(tags=["estimates"])
logger = logging.getLogger(__name__)


def _annual_to_monthly(v_year: float) -> float:
    try:
        return float(v_year or 0.0) / 12.0
    except Exception:
        return 0.0


DEFAULT_UNIT_SIZE_M2 = {
    "residential": 120.0,
    "retail": 80.0,
    "office": 120.0,
}


def _resolve_avg_unit_m2(excel_inputs: dict, key: str, fallback: float) -> float:
    avg_unit_m2 = excel_inputs.get("avg_unit_m2") if isinstance(excel_inputs, dict) else None
    if isinstance(avg_unit_m2, dict):
        try:
            value = float(avg_unit_m2.get(key) or 0.0)
            if value > 0:
                return value
        except Exception:
            pass
    if key == "residential":
        try:
            value = float(excel_inputs.get("parking_assumed_avg_apartment_m2") or 0.0)
            if value > 0:
                return value
        except Exception:
            pass
    return fallback


def _has_avg_unit_override(excel_inputs: dict, key: str) -> bool:
    avg_unit_m2 = excel_inputs.get("avg_unit_m2") if isinstance(excel_inputs, dict) else None
    if isinstance(avg_unit_m2, dict):
        try:
            value = float(avg_unit_m2.get(key) or 0.0)
            if value > 0:
                return True
        except Exception:
            pass
    if key == "residential":
        try:
            value = float(excel_inputs.get("parking_assumed_avg_apartment_m2") or 0.0)
            if value > 0:
                return True
        except Exception:
            pass
    return False


def _format_avg_unit_m2(value: float) -> str:
    if abs(value - round(value)) < 0.05:
        return f"{value:,.0f}"
    return f"{value:,.1f}"


def _estimate_unit_counts(
    excel: dict,
    excel_inputs: dict,
    unit_mix: list[Any] | None,
) -> dict[str, int]:
    area_map = excel.get("nla") or excel.get("built_area") or {}
    counts: dict[str, int] = {}
    residential_units = 0
    for unit in unit_mix or []:
        if not isinstance(unit, dict):
            continue
        try:
            residential_units += int(unit.get("count") or 0)
        except Exception:
            continue

    if residential_units <= 0:
        residential_area = float(area_map.get("residential") or 0.0)
        avg_apartment_m2 = _resolve_avg_unit_m2(
            excel_inputs, "residential", DEFAULT_UNIT_SIZE_M2["residential"]
        )
        if residential_area > 0:
            residential_units = int(
                (residential_area + avg_apartment_m2 - 1) // max(avg_apartment_m2, 1.0)
            )

    counts["residential"] = residential_units

    for key in ("retail", "office"):
        area = float(area_map.get(key) or 0.0)
        avg_unit_m2 = _resolve_avg_unit_m2(excel_inputs, key, DEFAULT_UNIT_SIZE_M2[key])
        if area > 0:
            counts[key] = int((area + avg_unit_m2 - 1) // max(avg_unit_m2, 1.0))
        else:
            counts[key] = 0

    return counts


def _roi_band(roi: float) -> str:
    try:
        value = float(roi)
    except Exception:
        return "uncertain"
    if value < 0:
        return "negative"
    if value < 0.05:
        return "low-single-digit"
    if value < 0.1:
        return "mid-single-digit"
    return "double-digit"


_INMEM_HEADERS: dict[str, dict[str, Any]] = {}
_INMEM_LINES: dict[str, list[dict[str, Any]]] = {}


def _supports_sqlalchemy(db: Any) -> bool:
    """Return True when the dependency looks like a SQLAlchemy session."""

    required = ("add", "add_all", "commit", "query", "get")
    return all(hasattr(db, attr) for attr in required)


def _estimate_header_supports_owner(db: Any) -> bool:
    if not _supports_sqlalchemy(db):
        return False
    try:
        inspector = inspect(db.get_bind())
        return any(col["name"] == "owner" for col in inspector.get_columns("estimate_header"))
    except Exception:
        return False


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _delta_pct(value: float | None, base: float | None) -> float | None:
    if value is None or base in (None, 0):
        return None
    return (value - base) / base


def _extract_excel_breakdown(notes: dict[str, Any]) -> dict[str, Any]:
    excel_breakdown = notes.get("excel_breakdown")
    if not isinstance(excel_breakdown, dict):
        nested = notes.get("notes")
        if isinstance(nested, dict):
            excel_breakdown = nested.get("excel_breakdown")
    return excel_breakdown if isinstance(excel_breakdown, dict) else {}


def _insert_usage_event(db: Any, event: UsageEvent) -> None:
    if _supports_sqlalchemy(db):
        db.add(event)
        db.commit()
        return
    if not os.getenv("DATABASE_URL"):
        return
    with db_session.SessionLocal() as session:
        session.add(event)
        session.commit()


def _persist_inmemory(
    estimate_id: str,
    strategy: str,
    totals: dict[str, Any],
    notes: dict[str, Any],
    assumptions: list[dict[str, Any]],
    lines: list[dict[str, Any]],
    owner: str | None = None,
) -> None:
    """Persist estimate data in a simple in-memory store (used in tests)."""

    _INMEM_HEADERS[estimate_id] = {
        "strategy": strategy,
        "totals": dict(totals),
        "notes": notes.copy() if isinstance(notes, dict) else dict(notes or {}),
        "assumptions": [dict(a) for a in assumptions],
        "owner": owner,
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
    far: float = Field(
        default=2.0,
        description="Deprecated/ignored. FAR is computed automatically by the engine.",
    )
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
    # Excel-only mode: require excel_inputs and route exclusively through the Excel workflow.
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
                    "area_ratio": {"residential": 1.6, "basement": 1},
                    "unit_cost": {"residential": 2200, "basement": 1200},
                    "efficiency": {"residential": 0.82},
                    "cp_sqm_per_space": {"basement": 30},
                    "rent_sar_m2_yr": {"residential": 2400},
                    "fitout_rate": 400,
                    "contingency_pct": 0.05,
                    "consultants_pct": 0.06,
                    "transaction_pct": 0.03,
                    "feasibility_fee_pct": 0.02,
                    "opex_pct": 0.05,
                    "land_price_sar_m2": 2800,
                }
            }]
        }
    )


def _infer_landuse_code_from_excel_inputs(excel_inputs: dict | None) -> str | None:
    """
    Best-effort land-use inference for FAR caps.
    We prefer explicit fields if the frontend provides them, otherwise infer from area_ratio keys.
    """
    if not isinstance(excel_inputs, dict):
        return None

    # 1) Explicit override fields (if frontend passes them)
    for k in ("land_use_code", "landuse_code", "land_use", "landuse"):
        v = excel_inputs.get(k)
        if v is None:
            continue
        s = str(v).strip().lower()
        if s in {"s", "m", "c"}:
            return s
        # common labels
        if s in {"residential", "housing"}:
            return "s"
        if s in {"mixed", "mixed-use", "mixed_use"}:
            return "m"
        if s in {"commercial", "retail", "office"}:
            return "c"

    # 2) Infer from area_ratio keys (template-driven)
    ar = excel_inputs.get("area_ratio")
    if not isinstance(ar, dict) or not ar:
        return None

    keys = {str(k).strip().lower() for k in ar.keys()}

    # Heuristic buckets
    has_res = any("res" in k or k == "residential" for k in keys)
    has_com = any(
        ("com" in k)
        or ("retail" in k)
        or ("office" in k)
        or ("shop" in k)
        or ("commercial" in k)
        for k in keys
    )

    if has_res and has_com:
        return "m"
    if has_com:
        return "c"
    if has_res:
        return "s"
    return None


@router.post("/estimates", response_model=EstimateResponseModel)
def create_estimate(
    req: EstimateRequest,
    request: Request,
    db: Session = Depends(get_db),
) -> EstimateResponseModel:
    # Geometry → area
    try:
        geom = geo_svc.parse_geojson(req.geometry)
    except Exception as exc:
        # Give a precise, user-friendly 400 instead of a cryptic 422
        raise HTTPException(status_code=400, detail=f"Invalid GeoJSON for 'geometry': {exc}")
    city_for_district = req.city or "Riyadh"
    city_norm = norm_city(city_for_district) or city_for_district
    if geom.is_empty:
        raise HTTPException(status_code=400, detail="Empty geometry provided")
    geom_geojson = geo_svc.to_geojson(geom)
    site_area_m2 = geo_svc.area_m2(geom)

    # Compute centroid early so both rent + land pricing can use it.
    # IMPORTANT: Passing lon/lat to price_from_kaggle_hedonic enables district inference fallback
    # and avoids "citywide constant ppm2" behavior when district is missing.
    lon = lat = None
    try:
        centroid = geom.centroid
        lon = float(centroid.x)
        lat = float(centroid.y)
    except Exception:
        lon = lat = None
    district_resolution = resolve_district(
        db,
        city=city_norm,
        geom_geojson=geom_geojson,
        lon=lon,
        lat=lat,
        district=None,
    )
    city_norm = district_resolution.city_norm or city_norm
    district_raw = district_resolution.district_raw
    district_norm = district_resolution.district_norm
    district = district_norm or district_raw
    district_inference = resolution_meta(district_resolution)
    district_inference["district_normalized"] = district_norm

    overture_site_metrics: dict[str, Any] = {}
    overture_context_metrics: dict[str, Any] = {}
    try:
        overture_site_metrics = compute_building_metrics(db, geom_geojson)
        if overture_site_metrics.get("site_area_m2"):
            site_area_m2 = float(overture_site_metrics["site_area_m2"])
    except Exception:
        overture_site_metrics = {}
    try:
        overture_context_metrics = compute_building_metrics(db, geom_geojson, buffer_m=500.0)
    except Exception:
        overture_context_metrics = {}

    # FAR is intentionally fully automatic:
    # - Do NOT use district FAR (data is corrupted)
    # - Do NOT use user-supplied FAR (confusing UX)
    far_max = None
    far_max_source = None
    typical_source = None

    typical_far_proxy = overture_context_metrics.get("far_proxy_existing")
    typical_source = "overture_proxy" if typical_far_proxy is not None else None
    if overture_context_metrics.get("building_count") == 0:
        typical_far_proxy = None
        typical_source = None
    landuse_for_cap = _infer_landuse_code_from_excel_inputs(
        dict(req.excel_inputs) if isinstance(req.excel_inputs, dict) else None
    )
    FAR_SAFETY_FACTOR = 1.15
    final_far, far_calc_meta = far_rules.compute_far_with_priors(
        landuse_code=landuse_for_cap,
        overture_typical_far=float(typical_far_proxy) if typical_far_proxy is not None else None,
        safety_factor=FAR_SAFETY_FACTOR,
    )
    suggested_far = float(final_far)

    method = str((far_calc_meta or {}).get("method") or "priors_min_overture_cap")
    source_label = "priors_min_overture_cap"
    far_used = float(suggested_far)
    far_inference_notes = {
        "suggested_far": float(suggested_far or 0.0) if suggested_far is not None else None,
        "far_max": None,
        "source": source_label,
        "far_max_source": far_max_source,
        "typical_source": typical_source,
        "method": method,
        "buffer_m": overture_context_metrics.get("buffer_m"),
        "typical_far_proxy": float(typical_far_proxy or 0.0) if typical_far_proxy is not None else None,
        "district": district_norm or district,
        "explicit_far": False,
        "far_calc": far_calc_meta,
    }

    def _finalize_result(
        result: dict[str, Any], bands: dict[str, Any] | None = None
    ) -> EstimateResponseModel:
        payload_bands = bands or {}
        est_id = str(uuid.uuid4())
        totals = result["totals"]
        assumptions = result.get("assumptions", [])
        notes_payload = {"bands": payload_bands, "notes": result["notes"]}
        persisted = False
        auth_payload = getattr(request.state, "auth", None) or {}
        if auth.MODE == "disabled":
            user_id = "anonymous"
        else:
            user_id = auth_payload.get("sub") or "api"

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
                    "owner": user_id,
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
                    "owner": user_id,
                    "url": None,
                    "model_version": None,
                    "created_at": None,
                }
            )

        if _supports_sqlalchemy(db):
            # Persist to DB if possible. In non-prod, failures fall back to in-memory
            # so the UI can still show results.
            try:
                orm_lines = [EstimateLine(**entry) for entry in line_dicts]
                header_payload = {
                    "id": est_id,
                    "strategy": req.strategy,
                    "input_json": json.dumps(req.model_dump(), default=str),
                    "totals_json": json.dumps(totals, default=str),
                    "notes_json": json.dumps(notes_payload, default=str),
                }
                if _estimate_header_supports_owner(db):
                    header_payload["owner"] = user_id
                header = EstimateHeader(**header_payload)
                db.add(header)
                db.add_all(orm_lines)
                db.commit()
                persisted = True
            except Exception:
                db.rollback()
                logger.exception(
                    "Failed to persist estimate to DB",
                    extra={"estimate_id": est_id, "app_env": settings.APP_ENV},
                )
                if (settings.APP_ENV or "").lower() == "prod":
                    raise HTTPException(status_code=500, detail="Failed to persist estimate")
                _persist_inmemory(
                    est_id,
                    req.strategy,
                    totals,
                    notes_payload,
                    assumptions,
                    line_dicts,
                    owner=user_id,
                )
        else:
            if (settings.APP_ENV or "").lower() == "prod":
                logger.error(
                    "Estimate persistence unavailable in production",
                    extra={"estimate_id": est_id, "app_env": settings.APP_ENV},
                )
                raise HTTPException(status_code=500, detail="Failed to persist estimate")
            _persist_inmemory(
                est_id,
                req.strategy,
                totals,
                notes_payload,
                assumptions,
                line_dicts,
                owner=user_id,
            )

        notes_payload["persisted"] = persisted
        result_notes = result.get("notes")
        if not isinstance(result_notes, dict):
            result_notes = {}
        result_notes["persisted"] = persisted
        result["notes"] = result_notes

        result["id"] = est_id
        result["strategy"] = req.strategy
        _log_estimate_result_event(result)
        return EstimateResponseModel.model_validate(result)

    def _log_estimate_result_event(result: dict[str, Any]) -> None:
        try:
            auth_payload = getattr(request.state, "auth", None) or {}
            if auth.MODE == "disabled":
                user_id = "anonymous"
                is_admin = False
            else:
                user_id = auth_payload.get("sub")
                is_admin = bool(auth_payload.get("is_admin", False))

            notes = result.get("notes") if isinstance(result.get("notes"), dict) else {}
            excel_breakdown = _extract_excel_breakdown(notes)
            excel_inputs = (
                dict(req.excel_inputs) if isinstance(req.excel_inputs, dict) else {}
            )

            land_price_auto = _safe_float(
                excel_breakdown.get("land_price_sar_m2")
            )
            if land_price_auto is None:
                land_price_auto = _safe_float(
                    (notes.get("excel_land_price") or {}).get("ppm2")
                )
            land_price_input = _safe_float(excel_inputs.get("land_price_sar_m2"))

            site_area = _safe_float(notes.get("site_area_m2"))
            land_cost = _safe_float(excel_breakdown.get("land_cost"))
            land_price_final = (
                land_cost / site_area
                if land_cost is not None and site_area not in (None, 0)
                else land_price_auto
            )

            land_price_overridden = None
            if land_price_input is not None and land_price_auto not in (None, 0):
                land_price_overridden = abs(land_price_input - land_price_auto) / land_price_auto > 0.01

            far_effective = _safe_float(excel_breakdown.get("far_above_ground"))
            far_input = _safe_float(req.far) or _safe_float(excel_inputs.get("far"))
            far_auto = _safe_float(notes.get("suggested_far"))
            if far_auto is None:
                far_inference = notes.get("far_inference") if isinstance(notes.get("far_inference"), dict) else {}
                far_auto = _safe_float(far_inference.get("suggested_far"))
            far_overridden = None
            if far_input is not None and far_auto not in (None, 0):
                far_overridden = abs(far_input - far_auto) / far_auto > 0.01

            meta = {
                "land_price_auto": land_price_auto,
                "land_price_input": land_price_input,
                "land_price_final": land_price_final,
                "land_price_overridden": land_price_overridden,
                "land_price_delta_pct": _delta_pct(land_price_final, land_price_auto),
                "far_effective": far_effective,
                "far_input": far_input,
                "far_overridden": far_overridden,
                "far_delta_pct": _delta_pct(far_input, far_auto),
                "landuse_code": notes.get("landuse_code") or excel_inputs.get("land_use_code"),
                "landuse_method": notes.get("landuse_method"),
                "provider": excel_inputs.get("provider"),
            }

            event = UsageEvent(
                ts=datetime.now(timezone.utc),
                user_id=user_id,
                is_admin=is_admin,
                event_name="estimate_result",
                method="POST",
                path="/v1/estimates",
                status_code=200,
                duration_ms=0,
                estimate_id=result.get("id"),
                meta=meta,
            )
            _insert_usage_event(db, event)
        except SQLAlchemyError as exc:
            logger.warning("Estimate usage event insert failed: %s", exc)
        except Exception as exc:
            logger.warning("Estimate usage event logging failed: %s", exc)

    # Excel-only mode: always use the Excel-style computation and short-circuit other paths.
    if req.excel_inputs:
        # Copy inputs and enrich land price if caller didn't provide one (or provided 0/None)
        excel_inputs = dict(req.excel_inputs)

        # In Excel-mode, BUA is computed from `area_ratio`. To avoid it getting stuck on
        # template placeholders (e.g. residential 1.60), scale placeholder area ratios to
        # match the automatic FAR.
        target_far: float | None = None
        target_far_source: str = ""
        try:
            tf = float(far_used)
        except Exception:
            tf = 0.0
        if tf > 0:
            target_far = tf
            target_far_source = "auto_far_priors"

        excel_inputs = scale_placeholder_area_ratio(
            excel_inputs, target_far=target_far, target_far_source=target_far_source
        )

        # Option B — Use floors to scale area_ratio.
        # Force mixed-use ("m") to 3.5 floors above ground and reflect that on FAR/BUA.
        floors_adjustment: dict[str, Any] | None = None
        if landuse_for_cap == "m":
            desired_floors_above_ground = 3.5

            baseline_floors_above_ground: float | None = None
            baseline_source: str | None = None
            for source_name, metrics in (
                ("overture_context_metrics", overture_context_metrics),
                ("overture_site_metrics", overture_site_metrics),
            ):
                if isinstance(metrics, dict):
                    v = metrics.get("floors_median") or metrics.get("floors_mean")
                    if isinstance(v, (int, float)) and v > 0:
                        baseline_floors_above_ground = float(v)
                        baseline_source = source_name
                        break

            if baseline_floors_above_ground is None:
                baseline_floors_above_ground = 2.0
                baseline_source = "default_assumption"

            excel_inputs = scale_area_ratio_by_floors(
                excel_inputs,
                desired_floors_above_ground=desired_floors_above_ground,
                baseline_floors_above_ground=baseline_floors_above_ground,
                desired_floors_source="fixed_mixed_use_rule",
                baseline_floors_source=baseline_source,
                min_above_ground_far=2.5,
                max_above_ground_far=5.0,
                enforce_far_source="mixed_use_far_floor",
            )

            floors_adjustment = {
                "landuse": "m",
                "desired_floors_above_ground": desired_floors_above_ground,
                "baseline_floors_above_ground": baseline_floors_above_ground,
                "baseline_source": baseline_source,
                "scale_factor": desired_floors_above_ground / baseline_floors_above_ground
                if baseline_floors_above_ground > 0
                else None,
            }

            # Convenience: computed above-ground FAR after floors scaling.
            try:
                ar = excel_inputs.get("area_ratio") or {}
                far_above_ground = 0.0
                for k, v in ar.items():
                    if isinstance(k, str) and ("basement" in k.lower() or "underground" in k.lower()):
                        continue
                    far_above_ground += float(v or 0)
                floors_adjustment["far_above_ground_after"] = far_above_ground
            except Exception:
                pass

        # --- Parking minimums (Riyadh) ---
        parking_meta: dict[str, Any] = {"applied": False}
        try:
            excel_inputs, parking_meta = parking_svc.ensure_parking_minimums(
                excel_inputs=excel_inputs,
                site_area_m2=site_area_m2,
                unit_mix=req.unit_mix,
                city=req.city or "Riyadh",
            )
        except Exception as e:
            parking_meta = {"applied": False, "error": str(e)}
        ppm2_val: float = 0.0
        ppm2_src: str = "Manual"
        try:
            override = excel_inputs.get("land_price_sar_m2")
            override_f = float(override) if override is not None else 0.0
        except Exception:
            override_f = 0.0
        land_pricing_meta: dict[str, Any] | None = None
        if override_f <= 0.0:
            quote = quote_land_price_blended_v1(
                db,
                city=req.city or "Riyadh",
                district=(district_norm or district) if (district_norm or district) else None,
                lon=lon,
                lat=lat,
                geom_geojson=geom_geojson,
            )
            land_pricing_meta = quote.get("meta") if isinstance(quote, dict) else None
            quoted_value = quote.get("value") if isinstance(quote, dict) else None
            if quoted_value is not None:
                ppm2_val = float(quoted_value)
                ppm2_src = quote.get("method") or quote.get("provider") or "blended_v1"
                excel_inputs["land_price_sar_m2"] = ppm2_val
            else:
                # Fallback: Kaggle hedonic v0 model (previous default)
                value, method, meta = price_from_kaggle_hedonic(
                    db,
                    city=req.city or "Riyadh",
                    lon=lon,
                    lat=lat,
                    district=(district_norm or district) if (district_norm or district) else None,
                    geom_geojson=geom_geojson,
                )
                land_pricing_meta = meta if isinstance(meta, dict) else land_pricing_meta
                if isinstance(land_pricing_meta, dict):
                    land_pricing_meta = {**land_pricing_meta, "fallback_from": "blended_v1"}
                if value is not None:
                    ppm2_val = float(value)
                    ppm2_src = method or (meta.get("source") if isinstance(meta, dict) else "kaggle_hedonic_v0")
                    excel_inputs["land_price_sar_m2"] = ppm2_val
                else:
                    # Fallback: previous Kaggle median logic (aqar.mv_city_price_per_sqm / aqar.listings)
                    aqar = price_from_aqar(db, req.city or "Riyadh", district_norm or district)
                    if aqar:
                        ppm2_val, ppm2_src = float(aqar[0]), aqar[1]
                        excel_inputs["land_price_sar_m2"] = ppm2_val
                        land_pricing_meta = {
                            "source": "aqar_median",
                            "method": ppm2_src,
                            "city": req.city or "Riyadh",
                            "district": district_norm or district,
                            "fallback_from": "blended_v1",
                        }

        else:
            ppm2_val, ppm2_src = override_f, "Manual"
            land_pricing_meta = {
                "source": "manual_override",
                "district": district_norm or district,
                "city": req.city or "Riyadh",
            }

        asof = date.today()
        try:
            if req.timeline and req.timeline.start:
                asof = date.fromisoformat(req.timeline.start)
        except (TypeError, ValueError):
            pass

        # New: GASTAT real estate price index scalar (2014=1.0)
        # Optional: if indicators aren't available yet, default to 1.0 (no indexation).
        try:
            re_scalar = latest_re_price_index_scalar(db, asset_type="Residential")
        except Exception:
            logger.warning(
                "Failed to fetch real estate price index scalar; defaulting to 1.0",
                exc_info=True,
            )
            re_scalar = None
        re_scalar = float(re_scalar) if re_scalar else 1.0
        excel_inputs["re_price_index_scalar"] = re_scalar

        # NEW: Prefer REGA rent benchmarks blended with Kaggle Aqar district medians when available.
        city_for_rent = city_for_district
        district_norm = district_norm or ""
        try:
            rega_result = latest_rega_residential_rent_per_m2(
                db, req.city, district_norm or district
            )
        except Exception:
            logger.warning(
                "REGA rent-per-m2 lookup failed; continuing without it",
                exc_info=True,
            )
            rega_result = None
        if rega_result is not None:
            rega_rent_monthly, rega_unit, rent_date, rent_source_url = rega_result
        else:
            rega_rent_monthly = rent_date = rent_source_url = None
            rega_unit = "SAR/m²/month"

        rent_rates_raw = excel_inputs.get("rent_sar_m2_yr")
        rent_rates = dict(rent_rates_raw) if isinstance(rent_rates_raw, dict) else {}

        rent_strategy: str = "manual_or_template"
        rent_meta_common: dict[str, Any] = {
            "city": req.city,
            "district": district,
            "district_raw": district_raw or district,
            "district_normalized": district_norm or None,
            "district_raw_inferred": (district_raw or (district_inference or {}).get("district_raw")) or district,
            "district_normalized_used": district_norm or None,
            "district_inference": district_inference,
            "district_inference_method": (district_inference or {}).get("method"),
            "district_inference_distance_m": (district_inference or {}).get("distance_m"),
        }

        rent_source_metadata: dict[str, Any] = {
            **rent_meta_common,
            "provider": "template",
            "method": "manual_or_template",
            "components": {},
            "rent_strategy": rent_strategy,
        }
        providers_used: set[str] = set()

        COMPONENT_TO_AQAR = {
            "residential": ("residential", None),
            "retail": ("commercial", "retail"),
            "office": ("commercial", "office"),
        }
        COMPONENT_AQAR_CONFIG = {
            "residential": {"min_samples": 10, "since_days": 365},
            "retail": {"min_samples": 5, "since_days": 730},
            "office": {"min_samples": 5, "since_days": 730},
        }
        component_candidates: set[str] = set()
        for mapping in (rent_rates, excel_inputs.get("efficiency") or {}, excel_inputs.get("area_ratio") or {}):
            if isinstance(mapping, dict):
                component_candidates.update(mapping.keys())

        component_meta: dict[str, Any] = {}
        for component in sorted(component_candidates):
            if component not in COMPONENT_TO_AQAR:
                continue
            asset_type_for_comp, unit_type_for_comp = COMPONENT_TO_AQAR[component]
            config = COMPONENT_AQAR_CONFIG.get(component, COMPONENT_AQAR_CONFIG["residential"])
            min_samples = int(config.get("min_samples") or 0)
            try:
                result = aqar_rent_median(
                    db,
                    city_norm,
                    district_norm or None,
                    asset_type=asset_type_for_comp,
                    unit_type=unit_type_for_comp,
                    since_days=int(config.get("since_days") or 365),
                )
            except Exception:
                # Keep Excel-mode robust if the backing table/view isn't present
                result = RentMedianResult(None, None, 0, 0, None, 0, None, None, 0)

            applied_scope = None
            applied_sample = 0
            applied_monthly = None
            method_label = None
            base_city_median = None
            base_city_sample = 0
            if result.city_median is not None:
                base_city_median = float(result.city_median)
                base_city_sample = result.n_city
            elif result.city_asset_median is not None:
                base_city_median = float(result.city_asset_median)
                base_city_sample = result.n_city_asset

            if result.district_median is not None and result.n_district >= min_samples:
                applied_scope = "district"
                applied_sample = result.n_district
                applied_monthly = float(result.district_median)
                method_label = "aqar_district_median"
            elif (
                result.district_median is not None
                and result.n_district > 0
                and min_samples > 0
                and base_city_median is not None
            ):
                weight = min(1.0, result.n_district / float(min_samples))
                applied_scope = "district_shrinkage"
                applied_sample = result.n_district
                applied_monthly = float(result.district_median) * weight + base_city_median * (1.0 - weight)
                method_label = "aqar_district_shrinkage"
            elif result.city_median is not None and result.n_city >= min_samples:
                applied_scope = "city_unit_type"
                applied_sample = result.n_city
                applied_monthly = float(result.city_median)
                method_label = "aqar_city_median"
            elif result.city_asset_median is not None and result.n_city_asset >= min_samples:
                applied_scope = "city_asset_type"
                applied_sample = result.n_city_asset
                applied_monthly = float(result.city_asset_median)
                method_label = "aqar_city_median"
            elif component == "residential" and rega_rent_monthly is not None:
                applied_scope = "city_rega"
                applied_sample = 0
                applied_monthly = float(rega_rent_monthly)
                method_label = "rega_city_rent"

            provider_label = "manual"
            if method_label and method_label.startswith("aqar"):
                provider_label = "Kaggle Aqar rent comps"
            elif method_label == "rega_city_rent":
                provider_label = "REGA (Real Estate General Authority)"

            component_meta[component] = {
                **rent_meta_common,
                "asset_type": asset_type_for_comp,
                "unit_type": unit_type_for_comp,
                "scope": applied_scope or result.scope,
                "selected_scope": applied_scope or result.scope,
                "median_rent_per_m2_month": applied_monthly or result.median_rent_per_m2_month,
                "sample_count": applied_sample or result.sample_count,
                "aqar_sample_sizes": {
                    "district": int(result.n_district),
                    "city": int(result.n_city),
                    "city_asset": int(result.n_city_asset),
                },
                "aqar_base_city_sample_count": base_city_sample,
                "aqar_medians": {
                    "district": result.district_median,
                    "city": result.city_median,
                    "city_asset": result.city_asset_median,
                },
                "method": method_label or result.scope or "manual_or_template",
                "provider": provider_label,
                "unit": "SAR/m²/month",
            }
            component_meta[component]["benchmark_per_m2_month"] = applied_monthly
            component_meta[component]["benchmark_per_m2_year"] = applied_monthly * 12.0 if applied_monthly is not None else None
            providers_used.add(provider_label)
            if applied_monthly is not None:
                rent_rates[component] = applied_monthly * 12.0
                rent_strategy = "component_aqar_median"
                if rent_source_metadata.get("provider") not in (None, provider_label):
                    rent_source_metadata["provider"] = "mixed"
                else:
                    rent_source_metadata["provider"] = provider_label
                rent_source_metadata["method"] = "component_aqar_medians"
                rent_source_metadata.setdefault("components", {})
                rent_source_metadata["components"][component] = dict(component_meta[component])

        excel_inputs["rent_sar_m2_yr"] = rent_rates
        rent_source_metadata["rent_strategy"] = rent_strategy
        rent_source_metadata.setdefault("components", {})
        excel_inputs["rent_source_metadata"] = rent_source_metadata

        rent_debug_metadata = {
            **rent_meta_common,
            "rent_strategy": rent_strategy,
            "aqar_components": component_meta,
        }
        res_meta = component_meta.get("residential") if isinstance(component_meta.get("residential"), dict) else {}
        if res_meta:
            samples = res_meta.get("aqar_sample_sizes") or {}
            try:
                rent_debug_metadata["aqar_district_samples"] = int(samples.get("district", 0))
                rent_debug_metadata["aqar_city_samples"] = int(samples.get("city", 0))
            except Exception:
                pass

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
        parking_income_meta = excel.get("parking_income_meta") or {}

        # Attach final parking status (after any auto-adjustment)
        try:
            if isinstance(parking_meta, dict):
                parking_meta = dict(parking_meta)
                parking_meta["required_spaces_final"] = int(excel.get("parking_required_spaces") or 0)
                parking_meta["provided_spaces_final"] = int(excel.get("parking_provided_spaces") or 0)
                parking_meta["deficit_spaces_final"] = int(excel.get("parking_deficit_spaces") or 0)
                parking_meta["compliant"] = bool(excel.get("parking_compliant"))
                parking_meta["parking_area_m2_final"] = float(excel.get("parking_area_m2") or 0.0)
        except Exception:
            pass
        rent_applied_sar_m2_yr = excel.get("rent_applied_sar_m2_yr") or excel_inputs.get("rent_sar_m2_yr") or {}
        rent_input_sar_m2_yr = excel_inputs.get("rent_sar_m2_yr") or {}
        try:
            rent_debug_metadata.update(
                {
                    "rent_applied_sar_m2_yr": {
                        k: float(v) for k, v in (rent_applied_sar_m2_yr.items() if isinstance(rent_applied_sar_m2_yr, dict) else [])
                    },
                    "rent_input_sar_m2_yr": {
                        k: float(v) for k, v in (rent_input_sar_m2_yr.items() if isinstance(rent_input_sar_m2_yr, dict) else [])
                    },
                    "rent_re_price_index_scalar": float(re_scalar),
                }
            )
        except Exception:
            pass
        # Ensure rent_source_metadata.components includes entries for all income-bearing components
        # (retail/office often fall back to template defaults when no Aqar commercial comps exist).
        try:
            rmeta = excel_inputs.get("rent_source_metadata")
            if isinstance(rmeta, dict):
                comps = rmeta.get("components")
                if not isinstance(comps, dict):
                    comps = {}
                    rmeta["components"] = comps

                # Ensure per-component meta exists at least for keys present in rent_applied_sar_m2_yr
                if isinstance(rent_applied_sar_m2_yr, dict):
                    for k, v_year in rent_applied_sar_m2_yr.items():
                        base_meta = component_meta.get(k) if isinstance(component_meta.get(k), dict) else {}
                        existing_meta = comps.get(k) if isinstance(comps.get(k), dict) else {}
                        merged_meta = {**base_meta, **existing_meta}

                        scope_val = merged_meta.get("selected_scope") or merged_meta.get("scope") or merged_meta.get("method")
                        merged_meta["selected_scope"] = scope_val
                        merged_meta["scope"] = scope_val
                        merged_meta.setdefault("method", "manual_or_template")
                        merged_meta.setdefault("provider", "template")
                        merged_meta.setdefault("unit", "SAR/m²/month")
                        merged_meta.setdefault(
                            "aqar_sample_sizes",
                            (base_meta.get("aqar_sample_sizes") if isinstance(base_meta.get("aqar_sample_sizes"), dict) else {"district": 0, "city": 0, "city_asset": 0}),
                        )
                        merged_meta.setdefault("sample_count", base_meta.get("sample_count") or 0)
                        merged_meta["benchmark_per_m2_year"] = float(v_year or merged_meta.get("benchmark_per_m2_year") or 0.0)
                        merged_meta["benchmark_per_m2_month"] = _annual_to_monthly(merged_meta["benchmark_per_m2_year"])
                        merged_meta.setdefault("median_rent_per_m2_month", merged_meta.get("benchmark_per_m2_month"))
                        comps[k] = merged_meta
                        providers_used.add(merged_meta.get("provider", "template"))

                provider_summary = "mixed" if len(providers_used) > 1 else (next(iter(providers_used)) if providers_used else "template")
                rmeta["provider"] = provider_summary
        except Exception:
            pass

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
            "parking_required_spaces": int(excel.get("parking_required_spaces") or 0),
            "parking_provided_spaces": int(excel.get("parking_provided_spaces") or 0),
            "parking_deficit_spaces": int(excel.get("parking_deficit_spaces") or 0),
            "parking_compliant": bool(excel.get("parking_compliant")),
        }
        direct_cost_total = float(sum(excel.get("direct_cost", {}).values()))
        parking_income_y1 = float(excel.get("parking_income_y1") or 0.0)
        y1_income_effective_factor = _normalize_y1_income_effective_factor(
            excel.get("y1_income_effective_factor", DEFAULT_Y1_INCOME_EFFECTIVE_FACTOR)
        )

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
            "y1_income_effective": float(excel.get("y1_income_effective") or 0.0),
            "y1_income_effective_factor": y1_income_effective_factor,
            "opex_pct": float(excel.get("opex_pct") or 0.0),
            "opex_cost": float(excel.get("opex_cost") or 0.0),
            "y1_noi": float(excel.get("y1_noi") or 0.0),
            "roi": float(excel["roi"]),
        }
        unit_counts = _estimate_unit_counts(excel, excel_inputs, req.unit_mix)
        avg_res = _resolve_avg_unit_m2(excel_inputs, "residential", DEFAULT_UNIT_SIZE_M2["residential"])
        avg_retail = _resolve_avg_unit_m2(excel_inputs, "retail", DEFAULT_UNIT_SIZE_M2["retail"])
        avg_office = _resolve_avg_unit_m2(excel_inputs, "office", DEFAULT_UNIT_SIZE_M2["office"])
        res_default = not _has_avg_unit_override(excel_inputs, "residential")
        retail_default = not _has_avg_unit_override(excel_inputs, "retail")
        office_default = not _has_avg_unit_override(excel_inputs, "office")
        roi_band = _roi_band(excel.get("roi", 0.0))
        roi_band_ar = {
            "negative": "سلبي",
            "low-single-digit": "منخفض (أحادي الرقم)",
            "mid-single-digit": "متوسط (أحادي الرقم)",
            "double-digit": "رقم مزدوج",
            "uncertain": "غير مؤكد",
        }.get(roi_band, roi_band)
        summary_en = (
            "Estimated unit yield: "
            f"{unit_counts.get('residential', 0):,} apartments, "
            f"{unit_counts.get('retail', 0):,} retail units, "
            f"{unit_counts.get('office', 0):,} office units."
        )
        summary_en += (
            " Based on current assumptions, the unlevered Year-1 yield on cost is "
            f"{roi_band}."
        )
        summary_en += (
            "\n\nAssumed average unit sizes: "
            f"Residential ≈ {_format_avg_unit_m2(avg_res)} m², "
            f"Retail ≈ {_format_avg_unit_m2(avg_retail)} m², "
            f"Office ≈ {_format_avg_unit_m2(avg_office)} m²."
        )
        summary_ar = (
            "تقدير عدد الوحدات: "
            f"{unit_counts.get('residential', 0):,} شقق سكنية، "
            f"{unit_counts.get('retail', 0):,} وحدات تجزئة، "
            f"{unit_counts.get('office', 0):,} وحدات مكتبية."
        )
        summary_ar += (
            " استنادًا إلى الافتراضات الحالية، فإن عائد السنة الأولى غير الممول على التكلفة هو "
            f"{roi_band_ar}."
        )
        summary_ar += (
            "\n\nمتوسط مساحات الوحدات المفترضة: "
            f"سكني ≈ {_format_avg_unit_m2(avg_res)} م²، "
            f"تجزئة ≈ {_format_avg_unit_m2(avg_retail)} م²، "
            f"مكاتب ≈ {_format_avg_unit_m2(avg_office)} م²."
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
                {
                    "key": "far",
                    "value": float(far_used),
                    "unit": None,
                    "source_type": "Model",
                },
                {
                    "key": "parking_required_spaces",
                    "value": float(excel.get("parking_required_spaces") or 0),
                    "unit": "spaces",
                    "source_type": "Riyadh Municipality",
                },
                {
                    "key": "parking_provided_spaces",
                    "value": float(excel.get("parking_provided_spaces") or 0),
                    "unit": "spaces",
                    "source_type": "Derived",
                },
                {
                    "key": "parking_supply_gross_m2_per_space",
                    "value": float(excel_inputs.get("parking_supply_gross_m2_per_space") or 30.0),
                    "unit": "m²/space",
                    "source_type": "Assumption",
                },
                {
                    "key": "avg_unit_size_residential_m2",
                    "value": avg_res,
                    "unit": "m²",
                    "source_type": "Market assumption",
                },
                {
                    "key": "avg_unit_size_retail_m2",
                    "value": avg_retail,
                    "unit": "m²",
                    "source_type": "Market assumption",
                },
                {
                    "key": "avg_unit_size_office_m2",
                    "value": avg_office,
                    "unit": "m²",
                    "source_type": "Market assumption",
                },
            ],
            "notes": {
                "excel_inputs_keys": list(excel_inputs.keys()),
                "excel_breakdown": excel,
                "cost_breakdown": cost_breakdown,
                "re_price_index_scalar": re_scalar,
                "transaction_tax": excel_inputs.get("transaction_tax_metadata"),
                "rent_source_metadata": excel_inputs.get("rent_source_metadata"),
                "district_inference": district_inference,
                "summary": summary_en,
                "summary_en": summary_en,
                "summary_ar": summary_ar,
                "unit_count_methodology": (
                    "Unit counts are derived from unit_mix when provided; otherwise estimated by "
                    "dividing net lettable area by average unit sizes."
                ),
                "unit_count_methodology_en": (
                    "Unit counts are derived from unit_mix when provided; otherwise estimated by "
                    "dividing net lettable area by average unit sizes."
                ),
                "unit_count_methodology_ar": (
                    "يتم اشتقاق عدد الوحدات من مزيج الوحدات عند توفيره؛ وإلا فيتم تقديره عبر "
                    "قسمة المساحة القابلة للتأجير على متوسط مساحات الوحدات."
                ),
                "avg_unit_size_defaults": {
                    "residential": res_default,
                    "retail": retail_default,
                    "office": office_default,
                },
                "site_area_m2": site_area_m2,
                "district": district,
                "excel_land_price": {
                    "ppm2": float(ppm2_val),
                    "source_type": ppm2_src,
                    "meta": land_pricing_meta,
                    "district_resolution": district_inference,
                },
                "excel_rent": {
                    "rent_sar_m2_yr": rent_applied_sar_m2_yr,
                    "rent_inputs_sar_m2_yr": rent_input_sar_m2_yr,
                    "rent_source_metadata": excel_inputs.get("rent_source_metadata"),
                    "re_price_index_scalar": float(re_scalar),
                },
                "rent_debug_metadata": rent_debug_metadata,
                "excel_roi": excel["roi"],
                "far_inference": {**far_inference_notes, "far_used": float(far_used)},
                "landuse_for_far_cap": landuse_for_cap,
                "floors_adjustment": floors_adjustment,
                "parking": parking_meta,
                "parking_income": parking_income_meta,
                "overture_buildings": {
                    "site_metrics": overture_site_metrics,
                    "context_metrics": overture_context_metrics,
                },
                "existing_footprint_area_m2": overture_site_metrics.get("footprint_area_m2"),
                "existing_bua_m2": overture_site_metrics.get("existing_bua_m2"),
                "potential_bua_m2": float(far_used * site_area_m2) if far_used is not None else None,
                "suggested_far": float(suggested_far or 0.0) if suggested_far is not None else None,
                "far_max": None,
            },
            "rent": {},
            "explainability": {},
            "confidence_bands": {},
        }

        # Surface parking-income assumptions in the ledger without requiring new frontend inputs.
        if parking_income_y1 > 0 and parking_income_meta.get("monetize_extra_parking"):
            try:
                extra_spaces_assumption = int(
                    parking_income_meta.get("extra_spaces")
                    or excel.get("parking_extra_spaces")
                    or 0
                )
            except Exception:
                extra_spaces_assumption = int(excel.get("parking_extra_spaces") or 0)

            monthly_rate_used_assumption = float(parking_income_meta.get("monthly_rate_used") or 0.0)
            occupancy_used_assumption = float(parking_income_meta.get("occupancy_used") or 0.0)
            public_access_assumption = 1.0 if bool(parking_income_meta.get("public_access")) else 0.0

            assumptions_list = result.get("assumptions")
            if isinstance(assumptions_list, list):
                assumptions_list.extend(
                    [
                        {
                            "key": "parking_extra_spaces_monetized",
                            "value": float(extra_spaces_assumption),
                            "unit": "spaces",
                            "source_type": "Derived",
                        },
                        {
                            "key": "parking_monthly_rate_sar_per_space",
                            "value": float(monthly_rate_used_assumption),
                            "unit": "SAR/space/month",
                            "source_type": "Model",
                        },
                        {
                            "key": "parking_occupancy",
                            "value": float(occupancy_used_assumption),
                            "unit": "fraction",
                            "source_type": "Assumption",
                        },
                        {
                            "key": "parking_public_access",
                            "value": float(public_access_assumption),
                            "unit": "bool(1=public)",
                            "source_type": "Assumption",
                        },
                        {
                            "key": "parking_income_y1",
                            "value": float(parking_income_y1),
                            "unit": "SAR/year",
                            "source_type": "Model",
                        },
                    ]
                )

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
    if isinstance(notes_dict, dict) and "excel_breakdown" not in notes_dict:
        nested_notes = notes_dict.get("notes")
        if isinstance(nested_notes, dict) and "excel_breakdown" in nested_notes:
            notes_dict["excel_breakdown"] = nested_notes.get("excel_breakdown")

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
    land_price_sar_m2: float | None = None
    provider: str | None = None


@router.post("/estimates/{estimate_id}/scenario")
def scenario(estimate_id: str, patch: ScenarioPatch, db: Session = Depends(get_db)) -> dict[str, Any]:
    errors: dict[str, str] = {}
    normalized_far = None
    normalized_efficiency = None
    normalized_land_price = None
    normalized_uplift = None

    if patch.far is not None:
        normalized_far = _safe_float(patch.far)
        if normalized_far is None or normalized_far <= 0:
            errors["far"] = "far must be greater than 0"

    if patch.efficiency is not None:
        normalized_efficiency = _safe_float(patch.efficiency)
        if normalized_efficiency is None:
            errors["efficiency"] = "efficiency must be a number"
        else:
            if normalized_efficiency > 1 and normalized_efficiency <= 100:
                normalized_efficiency = normalized_efficiency / 100
            if normalized_efficiency <= 0 or normalized_efficiency > 1:
                errors["efficiency"] = "efficiency must be between 0 and 1"

    if patch.land_price_sar_m2 is not None:
        normalized_land_price = _safe_float(patch.land_price_sar_m2)
        if normalized_land_price is None or normalized_land_price <= 0:
            errors["land_price_sar_m2"] = "land_price_sar_m2 must be greater than 0"

    if patch.price_uplift_pct is not None:
        normalized_uplift = _safe_float(patch.price_uplift_pct)
        if normalized_uplift is None:
            errors["price_uplift_pct"] = "price_uplift_pct must be a number"
        else:
            normalized_uplift = max(-90.0, min(200.0, normalized_uplift))

    if errors:
        logger.warning("Scenario patch validation failed: %s", errors)
        raise HTTPException(status_code=400, detail={"errors": errors})

    try:
        base = get_estimate(estimate_id, db)
        # Fast scenario: scale area-driven items by NFA ratio; then apply margin/softcost/price tweaks.
        base_totals = base["totals"]
        notes = base.get("notes", {}) or {}
        assumptions = base.get("assumptions", []) or []
        base_notes = notes if isinstance(notes, dict) else {}
        nested_notes = base_notes.get("notes") if isinstance(base_notes.get("notes"), dict) else {}
        cost_breakdown = base_notes.get("cost_breakdown")
        if cost_breakdown is None:
            cost_breakdown = nested_notes.get("cost_breakdown")
        if not isinstance(cost_breakdown, dict):
            cost_breakdown = {}
        excel_breakdown = base_notes.get("excel_breakdown")
        if excel_breakdown is None:
            excel_breakdown = nested_notes.get("excel_breakdown")
        if not isinstance(excel_breakdown, dict):
            excel_breakdown = {}
        is_excel = isinstance(cost_breakdown, dict) and bool(cost_breakdown)

        def _assumption_value(key: str, default: float) -> float:
            for item in assumptions:
                if item.get("key") == key:
                    val = item.get("value")
                    try:
                        return float(val)
                    except (TypeError, ValueError):
                        return default
            return default

        def _float_or_zero(value: Any) -> float:
            parsed = _safe_float(value)
            return parsed if parsed is not None else 0.0

        def _get_note_float(*candidates: Any) -> float | None:
            for candidate in candidates:
                parsed = _safe_float(candidate)
                if parsed is not None:
                    return parsed
            return None

        def _resolve_site_m2(
            base_notes_dict: dict[str, Any],
            nested_notes_dict: dict[str, Any],
            cost_breakdown_dict: dict[str, Any],
            excel_breakdown_dict: dict[str, Any],
            base_totals_dict: dict[str, Any],
        ) -> float:
            site_m2 = _get_note_float(
                base_notes_dict.get("site_area_m2"),
                nested_notes_dict.get("site_area_m2"),
                cost_breakdown_dict.get("site_area_m2"),
                excel_breakdown_dict.get("site_area_m2"),
            )
            if site_m2 is not None and site_m2 > 0:
                return site_m2

            land_cost = _get_note_float(
                cost_breakdown_dict.get("land_cost"),
                excel_breakdown_dict.get("land_cost"),
                base_totals_dict.get("land_value"),
            )
            ppm2 = _get_note_float(
                cost_breakdown_dict.get("land_price_final"),
                cost_breakdown_dict.get("land_price_sar_m2"),
                excel_breakdown_dict.get("land_price_sar_m2"),
            )
            if land_cost is not None and land_cost > 0 and ppm2 is not None and ppm2 > 0:
                return land_cost / ppm2
            return 0.0

        def _resolve_base_nfa(
            site_m2_value: float,
            base_notes_dict: dict[str, Any],
            nested_notes_dict: dict[str, Any],
            excel_breakdown_dict: dict[str, Any],
            cost_breakdown_dict: dict[str, Any],
        ) -> float:
            base_nfa_value = _get_note_float(
                base_notes_dict.get("nfa_m2"),
                nested_notes_dict.get("nfa_m2"),
                excel_breakdown_dict.get("nfa_m2"),
            )
            if base_nfa_value is not None and base_nfa_value > 0:
                return base_nfa_value

            if site_m2_value > 0:
                far = _get_note_float(
                    excel_breakdown_dict.get("far_above_ground"),
                    base_notes_dict.get("far_used"),
                )
                far = far if far is not None and far > 0 else 2.0
                efficiency = _get_note_float(excel_breakdown_dict.get("efficiency_overall"))
                efficiency = efficiency if efficiency is not None and efficiency > 0 else 0.82
                return site_m2_value * far * efficiency

            return 0.0

        def _build_excel_totals(costs: dict[str, Any], totals: dict[str, Any]) -> dict[str, float]:
            land_value = _float_or_zero(costs.get("land_cost") or totals.get("land_value"))
            hard_costs = _float_or_zero(costs.get("construction_direct_cost")) + _float_or_zero(
                costs.get("fitout_cost")
            )
            soft_costs = (
                _float_or_zero(costs.get("contingency_cost"))
                + _float_or_zero(costs.get("consultants_cost"))
                + _float_or_zero(costs.get("feasibility_fee"))
                + _float_or_zero(costs.get("transaction_cost"))
            )
            financing = _float_or_zero(totals.get("financing"))
            revenues = _float_or_zero(costs.get("y1_income") or totals.get("revenues"))
            p50_profit = _safe_float(totals.get("p50_profit"))
            if p50_profit is None:
                p50_profit = revenues - (land_value + hard_costs + soft_costs + financing)
            return {
                "land_value": land_value,
                "hard_costs": hard_costs,
                "soft_costs": soft_costs,
                "financing": financing,
                "revenues": revenues,
                "p50_profit": float(p50_profit),
            }

        site_m2 = _resolve_site_m2(base_notes, nested_notes, cost_breakdown, excel_breakdown, base_totals)
        base_nfa = _resolve_base_nfa(
            site_m2,
            base_notes,
            nested_notes,
            excel_breakdown,
            cost_breakdown,
        )
        if site_m2 <= 0 or base_nfa <= 0:
            raise HTTPException(
                status_code=400,
                detail="Scenario cannot run: missing site area / NFA in persisted estimate. Re-run estimate.",
            )
        new_far = normalized_far if normalized_far is not None else _assumption_value("far", 2.0)
        new_eff = (
            normalized_efficiency
            if normalized_efficiency is not None
            else _assumption_value("efficiency", 0.82)
        )
        new_nfa = site_m2 * new_far * new_eff if site_m2 > 0 else base_nfa
        area_ratio = (new_nfa / base_nfa) if base_nfa > 0 else 1.0

        if is_excel:
            excel_totals = _build_excel_totals(cost_breakdown, base_totals)
            base_totals_for_scenario = {**base_totals, **excel_totals}
            t = excel_totals.copy()
        else:
            base_totals_for_scenario = base_totals
            t = base_totals.copy()

        t["hard_costs"] = _float_or_zero(t.get("hard_costs")) * area_ratio
        t["revenues"] = _float_or_zero(t.get("revenues")) * area_ratio
        t["land_value"] = _float_or_zero(t.get("land_value"))

        base_soft_ratio = (
            (base_totals_for_scenario.get("soft_costs", 0.0) / base_totals_for_scenario.get("hard_costs", 0.0))
            if base_totals_for_scenario.get("hard_costs")
            else 0.15
        )
        base_cost_sum = base_totals_for_scenario.get("hard_costs", 0.0) + base_totals_for_scenario.get(
            "soft_costs", 0.0
        )
        base_financing_ratio = (
            (base_totals_for_scenario.get("financing", 0.0) / base_cost_sum) if base_cost_sum else 0.6
        )
        # Price uplift affects revenues
        uplift_pct = normalized_uplift if normalized_uplift is not None else 0.0
        uplift = 1.0 + uplift_pct / 100.0
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
        if normalized_land_price is not None and site_m2 > 0:
            t["land_value"] = float(normalized_land_price) * site_m2
        t["p50_profit"] = t["revenues"] - (t["land_value"] + t["hard_costs"] + t["soft_costs"] + t["financing"])
        bands = p_bands(
            t["p50_profit"],
            drivers={"land_ppm2": (1.0, 0.10), "unit_cost": (1.0, 0.08), "gdv_m2_price": (1.0, 0.10)},
        )
        base_baseline = base_totals_for_scenario
        response_notes = dict(base_notes) if isinstance(base_notes, dict) else {}
        response_totals = dict(base_baseline)
        for key in ("land_value", "hard_costs", "soft_costs", "financing", "revenues", "p50_profit"):
            if key in t:
                response_totals[key] = t[key]

        if is_excel:
            updated_breakdown = dict(cost_breakdown)
            old_hard = _float_or_zero(updated_breakdown.get("construction_direct_cost")) + _float_or_zero(
                updated_breakdown.get("fitout_cost")
            )
            old_soft = (
                _float_or_zero(updated_breakdown.get("contingency_cost"))
                + _float_or_zero(updated_breakdown.get("consultants_cost"))
                + _float_or_zero(updated_breakdown.get("feasibility_fee"))
                + _float_or_zero(updated_breakdown.get("transaction_cost"))
            )
            old_revenues = _float_or_zero(updated_breakdown.get("y1_income"))
            hard_scale = (t["hard_costs"] / old_hard) if old_hard > 0 else 1.0
            soft_scale = (t["soft_costs"] / old_soft) if old_soft > 0 else 1.0
            rev_scale = (t["revenues"] / old_revenues) if old_revenues > 0 else 1.0
            for key in ("construction_direct_cost", "fitout_cost"):
                updated_breakdown[key] = _float_or_zero(updated_breakdown.get(key)) * hard_scale
            for key in ("contingency_cost", "consultants_cost", "feasibility_fee", "transaction_cost"):
                updated_breakdown[key] = _float_or_zero(updated_breakdown.get(key)) * soft_scale
            updated_breakdown["y1_income"] = _float_or_zero(updated_breakdown.get("y1_income")) * rev_scale
            if normalized_land_price is not None and site_m2 > 0:
                updated_breakdown["land_cost"] = float(normalized_land_price) * site_m2
                updated_breakdown["land_price_final"] = float(normalized_land_price)

            y1_income_effective_factor = _float_or_zero(
                updated_breakdown.get("y1_income_effective_factor")
                or (excel_breakdown or {}).get("y1_income_effective_factor")
                or 0.9
            )
            updated_breakdown["y1_income_effective_factor"] = y1_income_effective_factor
            updated_breakdown["y1_income_effective"] = updated_breakdown["y1_income"] * y1_income_effective_factor
            opex_pct = _float_or_zero(
                updated_breakdown.get("opex_pct") or (excel_breakdown or {}).get("opex_pct") or 0.05
            )
            updated_breakdown["opex_pct"] = opex_pct
            updated_breakdown["opex_cost"] = updated_breakdown["y1_income_effective"] * opex_pct
            updated_breakdown["y1_noi"] = updated_breakdown["y1_income_effective"] - updated_breakdown["opex_cost"]
            total_capex = (
                _float_or_zero(updated_breakdown.get("land_cost"))
                + _float_or_zero(updated_breakdown.get("construction_direct_cost"))
                + _float_or_zero(updated_breakdown.get("fitout_cost"))
                + _float_or_zero(updated_breakdown.get("contingency_cost"))
                + _float_or_zero(updated_breakdown.get("consultants_cost"))
                + _float_or_zero(updated_breakdown.get("feasibility_fee"))
                + _float_or_zero(updated_breakdown.get("transaction_cost"))
            )
            updated_breakdown["grand_total_capex"] = total_capex
            updated_breakdown["roi"] = (updated_breakdown["y1_noi"] / total_capex) if total_capex > 0 else 0.0
            t["excel_roi"] = updated_breakdown["roi"]
            response_totals["excel_roi"] = updated_breakdown["roi"]

            response_notes["cost_breakdown"] = updated_breakdown
            if isinstance(excel_breakdown, dict):
                response_notes.setdefault("excel_breakdown", excel_breakdown)
            if isinstance(nested_notes, dict):
                nested_copy = dict(nested_notes)
                nested_copy["cost_breakdown"] = updated_breakdown
                if isinstance(excel_breakdown, dict):
                    nested_copy.setdefault("excel_breakdown", excel_breakdown)
                response_notes["notes"] = nested_copy

        scenario_overrides = {
            "far": normalized_far,
            "efficiency": normalized_efficiency,
            "area_ratio": float(area_ratio),
            "land_price_sar_m2": normalized_land_price,
            "provider": patch.provider,
            "price_uplift_pct": normalized_uplift,
        }
        response_notes["scenario_overrides"] = scenario_overrides
        if isinstance(response_notes.get("notes"), dict):
            nested_copy = dict(response_notes["notes"])
            nested_copy["scenario_overrides"] = scenario_overrides
            response_notes["notes"] = nested_copy

        response: dict[str, Any] = {
            "id": estimate_id,
            "totals": response_totals,
            "notes": response_notes,
            "baseline": base_baseline,
            "scenario": t,
            "delta": {k: t[k] - base_baseline.get(k, 0.0) for k in t},
            "confidence_bands": bands,
        }
        return response
    except HTTPException:
        raise
    except Exception:
        logger.exception("Scenario calculation failed for estimate %s", estimate_id)
        raise HTTPException(status_code=500, detail="Scenario calculation failed.")


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
    excel_breakdown = None
    cost_breakdown = None
    if isinstance(notes, dict):
        excel_breakdown = notes.get("excel_breakdown")
        cost_breakdown = notes.get("cost_breakdown")
        nested_notes = notes.get("notes")
        if isinstance(nested_notes, dict):
            if excel_breakdown is None:
                excel_breakdown = nested_notes.get("excel_breakdown")
            if cost_breakdown is None:
                cost_breakdown = nested_notes.get("cost_breakdown")
    try:
        pdf_bytes = build_memo_pdf(
            title=f"Estimate {estimate_id}",
            totals=base["totals"],
            assumptions=base.get("assumptions", []),
            top_comps=comps,
            excel_breakdown=excel_breakdown,
            cost_breakdown=cost_breakdown,
            notes=notes,
        )
    except Exception as exc:
        logger.exception("PDF generation failed for estimate %s", estimate_id)
        raise HTTPException(status_code=500, detail="PDF generation failed") from exc
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="estimate_{estimate_id}.pdf"'},
    )
