from typing import Any, Dict, List, Literal, Optional
from datetime import date
import json, uuid, csv, io
import logging

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.services import geo as geo_svc
from app.services import far_rules
from app.services import parking as parking_svc
from app.services.excel_method import compute_excel_estimate, scale_placeholder_area_ratio, scale_area_ratio_by_floors
from app.services.explain import (
    top_sale_comps,
    to_comp_dict,
)
from app.services.pdf import build_memo_pdf
from app.services.land_price_engine import quote_land_price_blended_v1
from app.services.pricing import price_from_kaggle_hedonic, price_from_aqar
from app.services.indicators import (
    latest_rega_residential_rent_per_m2,
    latest_re_price_index_scalar,
    latest_sale_price_per_m2,
)
from app.services.rent import RentMedianResult, aqar_rent_median
from app.services.district_resolver import resolve_district, resolution_meta
from app.services.overture_buildings_metrics import compute_building_metrics
from app.services.tax import latest_tax_rate
from app.models.tables import EstimateHeader, EstimateLine
from app.ml.name_normalization import norm_city

router = APIRouter(tags=["estimates"])
logger = logging.getLogger(__name__)


def _annual_to_monthly(v_year: float) -> float:
    try:
        return float(v_year or 0.0) / 12.0
    except Exception:
        return 0.0


def _fmt0(x: float) -> str:
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return "0"


def _component_rent_source_label(comp_meta: dict | None) -> str:
    """
    Return a human label for rent source per component.
    Expected comp_meta keys (when present):
      - method (e.g., aqar_district_median, manual_or_template)
      - scope (district/city/None)
      - provider
    """
    if not isinstance(comp_meta, dict):
        return "template default"
    method = (comp_meta.get("method") or "").strip()
    scope = (comp_meta.get("scope") or "").strip()
    provider = (comp_meta.get("provider") or "").strip()

    if method.startswith("aqar") and scope:
        # e.g. "Aqar district median"
        s = f"Aqar {scope} median"
        return f"{s}{f' ({provider})' if provider else ''}"

    if method and method not in ("manual", "manual_or_template"):
        return f"{method}{f' ({provider})' if provider else ''}"

    return "template default"


def _pick_base_annual_rent_sar_m2(rent_map: dict) -> float:
    """
    Pick a representative base annual rent (SAR/m²/year) for summary text.

    Why: rent_applied_sar_m2_yr may include non-income lines (e.g. basement=0),
    and dict iteration order can cause us to accidentally pick 0 as the "base rent".
    """
    if not isinstance(rent_map, dict) or not rent_map:
        return 0.0

    # 1) Prefer explicit residential (Excel-mode currently uses residential as the primary rent band).
    try:
        v = rent_map.get("residential")
        if v is not None:
            f = float(v)
            if f > 0:
                return f
    except Exception:
        pass

    # 2) Otherwise, pick the first positive rent.
    try:
        for _k, _v in rent_map.items():
            f = float(_v or 0.0)
            if f > 0:
                return f
    except Exception:
        pass

    # 3) Last resort: preserve previous behavior (may be 0, but avoids crashing).
    try:
        return float(next(iter(rent_map.values())))
    except Exception:
        return 0.0


def _base_rent_items(rent_map: dict) -> list[tuple[str, float]]:
    """Return non-zero base rents excluding basement-like components."""
    items: list[tuple[str, float]] = []
    if not isinstance(rent_map, dict):
        return items
    for key, value in rent_map.items():
        key_lower = str(key).lower()
        if key_lower.startswith("basement") or key_lower.startswith("parking"):
            continue
        try:
            v = float(value or 0.0)
        except Exception:
            continue
        if v <= 0:
            continue
        items.append((str(key), v))
    return items


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
                    "contingency_pct": 0.10,
                    "consultants_pct": 0.06,
                    "feasibility_fee": 1500000,
                    "transaction_pct": 0.03,
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
def create_estimate(req: EstimateRequest, db: Session = Depends(get_db)) -> EstimateResponseModel:
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

    # The frontend may send FAR even when the user didn't override it (it just echoes the default).
    # Treat FAR as explicit only when provided AND different from the model default.
    far_in_fields_set = hasattr(req, "model_fields_set") and "far" in req.model_fields_set
    far_default = 0.0
    try:
        if hasattr(EstimateRequest, "model_fields"):
            far_default = float(EstimateRequest.model_fields.get("far").default or 0.0)
    except Exception:
        far_default = 0.0
    far_explicit = bool(far_in_fields_set and abs(float(req.far) - far_default) > 1e-9)
    far_max = None
    far_max_source = None
    typical_source = None
    try:
        far_max = far_rules.lookup_far(db, req.city, district_norm or district)
        if far_max is not None:
            far_max_source = "far_rules"
    except Exception:
        far_max = None

    typical_far_proxy = overture_context_metrics.get("far_proxy_existing")
    typical_source = "overture_proxy" if typical_far_proxy is not None else None
    if overture_context_metrics.get("building_count") == 0:
        typical_far_proxy = None
        typical_source = None
    typical_far_clamped = max(0.3, float(typical_far_proxy)) if typical_far_proxy is not None else None
    suggested_far = None
    if far_max is not None and typical_far_clamped is not None:
        suggested_far = min(float(far_max), typical_far_clamped)
    elif far_max is not None:
        suggested_far = float(far_max)
    elif typical_far_clamped is not None:
        suggested_far = typical_far_clamped
    else:
        suggested_far = req.far

    # --- NEW: cap inferred FAR by land-use class to avoid absurd outliers (e.g., FAR=30) ---
    landuse_for_cap = _infer_landuse_code_from_excel_inputs(
        dict(req.excel_inputs) if isinstance(req.excel_inputs, dict) else None
    )
    far_cap_meta: dict[str, Any] | None = None
    if not far_explicit:
        # Only cap non-explicit FAR (i.e., inferred from Overture/rules)
        capped, meta = far_rules.cap_far_by_landuse(
            float(suggested_far) if suggested_far is not None else None,
            landuse_for_cap,
        )
        far_cap_meta = meta
        if capped is not None:
            # Keep source_label intact; this is a post-processing safety clamp
            suggested_far = capped
    else:
        far_cap_meta = {"applied": False, "reason": "explicit_far"}

    method = "default_far"
    source_label = None
    if far_explicit:
        method = "explicit_far"
        source_label = "explicit"
    elif far_max is not None and typical_far_clamped is not None:
        method = "min_far_max_and_typical"
        source_label = far_max_source if float(far_max) <= float(typical_far_clamped) else typical_source
    elif far_max is not None:
        method = "far_max_only"
        source_label = far_max_source
    elif typical_far_clamped is not None:
        method = "overture_typical"
        source_label = typical_source
    else:
        source_label = None

    far_used = req.far if far_explicit else float(suggested_far or req.far)
    far_inference_notes = {
        "suggested_far": float(suggested_far or 0.0) if suggested_far is not None else None,
        "far_max": float(far_max) if far_max is not None else None,
        "source": source_label,
        "far_max_source": far_max_source,
        "typical_source": typical_source,
        "method": method,
        "buffer_m": overture_context_metrics.get("buffer_m"),
        "typical_far_proxy": float(typical_far_proxy or 0.0) if typical_far_proxy is not None else None,
        "district": district_norm or district,
        "explicit_far": far_explicit,
        "far_cap": far_cap_meta,
    }

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
            # Persist to DB if possible. Any failure here should not break the user flow
            # (we'll fall back to an in-memory store so the UI can still show results).
            try:
                orm_lines = [EstimateLine(**entry) for entry in line_dicts]
                header = EstimateHeader(
                    id=est_id,
                    strategy=req.strategy,
                    input_json=json.dumps(req.model_dump(), default=str),
                    totals_json=json.dumps(totals, default=str),
                    notes_json=json.dumps(notes_payload, default=str),
                )
                db.add(header)
                db.add_all(orm_lines)
                db.commit()
            except Exception:
                db.rollback()
                logger.exception(
                    "Failed to persist estimate to DB; falling back to in-memory store"
                )
                _persist_inmemory(
                    est_id, req.strategy, totals, notes_payload, assumptions, line_dicts
                )
        else:
            _persist_inmemory(est_id, req.strategy, totals, notes_payload, assumptions, line_dicts)

        result["id"] = est_id
        result["strategy"] = req.strategy
        return EstimateResponseModel.model_validate(result)

    # Excel-only mode: always use the Excel-style computation and short-circuit other paths.
    if req.excel_inputs:
        # Copy inputs and enrich land price if caller didn't provide one (or provided 0/None)
        excel_inputs = dict(req.excel_inputs)

        # In Excel-mode, BUA is computed from `area_ratio`. To avoid it getting stuck on
        # template placeholders (e.g. residential 1.60), scale placeholder area ratios to
        # match the best available FAR (explicit > inferred > default).
        target_far: float | None = None
        target_far_source: str = ""
        if far_explicit:
            try:
                target_far = float(req.far)
                if target_far > 0:
                    target_far_source = "explicit_far"
            except Exception:
                target_far = None

        if (target_far is None or target_far <= 0) and suggested_far is not None:
            # Only auto-scale placeholder ratios when FAR is actually inferred
            # (Overture / rules). If we fell back to the request default FAR
            # (method == "default_far"), keep template ratios unchanged.
            far_method_norm = str(method or "").strip().lower()
            if far_method_norm and far_method_norm != "default_far":
                try:
                    sf = float(suggested_far)
                except Exception:
                    sf = 0.0
                if sf > 0:
                    target_far = sf
                    target_far_source = str(method or "far")

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

        rent_source_metadata: dict[str, Any] | None = None
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

        COMPONENT_TO_AQAR = {
            "residential": ("residential", None),
            "retail": ("commercial", "retail"),
            "office": ("commercial", "office"),
        }
        AQAR_MIN_SAMPLES = 10
        component_candidates: set[str] = set()
        for mapping in (rent_rates, excel_inputs.get("efficiency") or {}, excel_inputs.get("area_ratio") or {}):
            if isinstance(mapping, dict):
                component_candidates.update(mapping.keys())

        component_meta: dict[str, Any] = {}
        for component in sorted(component_candidates):
            if component not in COMPONENT_TO_AQAR:
                continue
            asset_type_for_comp, unit_type_for_comp = COMPONENT_TO_AQAR[component]
            try:
                result = aqar_rent_median(
                    db,
                    city_norm,
                    district_norm or None,
                    asset_type=asset_type_for_comp,
                    unit_type=unit_type_for_comp,
                    since_days=365,
                )
            except Exception:
                # Keep Excel-mode robust if the backing table/view isn't present
                result = RentMedianResult(None, None, 0, 0, None, 0, None, None, 0)

            applied_scope = None
            applied_sample = 0
            applied_monthly = None
            method_label = None
            if result.district_median is not None and result.n_district >= AQAR_MIN_SAMPLES:
                applied_scope = "district"
                applied_sample = result.n_district
                applied_monthly = float(result.district_median)
                method_label = "aqar_district_median"
            elif result.city_median is not None and result.n_city >= AQAR_MIN_SAMPLES:
                applied_scope = "city_unit_type"
                applied_sample = result.n_city
                applied_monthly = float(result.city_median)
                method_label = "aqar_city_median"
            elif result.city_asset_median is not None and result.n_city_asset >= AQAR_MIN_SAMPLES:
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
                "median_rent_per_m2_month": applied_monthly or result.median_rent_per_m2_month,
                "sample_count": applied_sample or result.sample_count,
                "aqar_sample_sizes": {
                    "district": int(result.n_district),
                    "city": int(result.n_city),
                    "city_asset": int(result.n_city_asset),
                },
                "aqar_medians": {
                    "district": result.district_median,
                    "city": result.city_median,
                    "city_asset": result.city_asset_median,
                },
                "method": method_label or result.scope or "manual_or_template",
                "provider": provider_label,
                "unit": "SAR/m²/month",
            }
            if applied_monthly is not None:
                rent_rates[component] = applied_monthly * 12.0
                rent_strategy = "component_aqar_median"
                if rent_source_metadata is None:
                    rent_source_metadata = {
                        **rent_meta_common,
                        "provider": provider_label,
                        "method": "component_aqar_medians",
                        "components": {},
                    }
                elif rent_source_metadata.get("provider") != provider_label:
                    rent_source_metadata["provider"] = "mixed"
                rent_source_metadata["components"][component] = {
                    **component_meta[component],
                    "benchmark_per_m2_month": applied_monthly,
                    "benchmark_per_m2_year": applied_monthly * 12.0,
                }

        excel_inputs["rent_sar_m2_yr"] = rent_rates
        if rent_source_metadata is not None and rent_source_metadata.get("components"):
            rent_source_metadata["rent_strategy"] = rent_strategy
            excel_inputs["rent_source_metadata"] = rent_source_metadata
        else:
            excel_inputs.pop("rent_source_metadata", None)

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
                        if k in comps:
                            continue
                        # If we didn't record aqar medians for this component, mark as template default
                        comps[k] = {
                            "method": "manual_or_template",
                            "provider": "template",
                            "scope": None,
                            "unit": "SAR/m²/month",
                            "median_rent_per_m2_month": None,
                            "sample_count": 0,
                            "benchmark_per_m2_month": _annual_to_monthly(float(v_year or 0.0)),
                            "benchmark_per_m2_year": float(v_year or 0.0),
                        }
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
            land_source_label = "a hedonic land price model"
        else:
            land_source_label = ppm2_src or "the configured land price source"

        district_display = district_raw or district or (req.city or "the selected city")
        summary_text = (
            f"For a site of {site_area_m2:,.0f} m² in "
            f"{district_display}, "
            f"land is valued at {excel['land_cost']:,.0f} SAR based on {land_source_label}. "
            f"Construction and fit-out total "
            f"{excel['sub_total']:,.0f} SAR, with contingency, "
            f"consultants, feasibility fees and transaction costs bringing total capex to "
            f"{excel['grand_total_capex']:,.0f} SAR. Year 1 net income of "
            f"{excel['y1_income']:,.0f} SAR implies an unlevered ROI of "
            f"{excel['roi']*100:,.1f}%."
        )

        # Summary footer: show per-component rents AND correctly label their sources.
        try:
            rmeta = excel_inputs.get("rent_source_metadata") or {}
            comps_meta = rmeta.get("components") if isinstance(rmeta, dict) else {}
            if not isinstance(comps_meta, dict):
                comps_meta = {}

            # only show meaningful components
            show_keys = [k for k in ("residential", "retail", "office") if k in rent_applied_sar_m2_yr]
            parts: list[str] = []
            for k in show_keys:
                y = float(rent_applied_sar_m2_yr.get(k) or 0.0)
                m = _annual_to_monthly(y)
                label = _component_rent_source_label(comps_meta.get(k) if isinstance(comps_meta, dict) else None)
                parts.append(f"{k} {_fmt0(m)} SAR/m²/month ({label})")

            if parts:
                # Replace any misleading "Aqar district medians" phrasing with a truthful line
                summary_text += " Base rents: " + ", ".join(parts) + "."
        except Exception:
            pass
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
                    "source_type": "Manual" if far_explicit else (far_max_source or "Overture"),
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
            ],
            "notes": {
                "excel_inputs_keys": list(excel_inputs.keys()),
                "excel_breakdown": excel,
                "cost_breakdown": cost_breakdown,
                "re_price_index_scalar": re_scalar,
                "transaction_tax": excel_inputs.get("transaction_tax_metadata"),
                "rent_source_metadata": excel_inputs.get("rent_source_metadata"),
                "district_inference": district_inference,
                "summary": summary_text,
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
                "overture_buildings": {
                    "site_metrics": overture_site_metrics,
                    "context_metrics": overture_context_metrics,
                },
                "existing_footprint_area_m2": overture_site_metrics.get("footprint_area_m2"),
                "existing_bua_m2": overture_site_metrics.get("existing_bua_m2"),
                "potential_bua_m2": float(far_max * site_area_m2) if far_max is not None else None,
                "suggested_far": float(suggested_far or 0.0) if suggested_far is not None else None,
                "far_max": float(far_max) if far_max is not None else None,
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
