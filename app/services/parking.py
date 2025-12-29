from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple


# ---------------------------------------------------------------------------
# Riyadh parking minimums (Amanat Al Riyadh / TRC guide)
# ---------------------------------------------------------------------------
#
# Source: "دليل حساب مواقف السيارات حسب نوع المشروع"
# https://trc.alriyadh.gov.sa/pdf/ParkingCalculationGuideByProjectType.pdf
#
# This module focuses on the most common land uses used by the Feasibility Engine:
#   - Residential (apartments)
#   - Retail (shops)
#   - Office
#
# The guide contains many additional categories (healthcare, education, etc.).
# Those can be added later by extending RIYADH_RULES below.
#
# IMPORTANT:
# - Minimums are generally enforced as whole parking spaces (ceil for required).
# - Provided spaces are also treated as whole stalls (floor for provided) to avoid
#   overstating capacity.
# ---------------------------------------------------------------------------


RIYADH_PARKING_RULESET_ID = "riyadh_municipality_parking_guide"
RIYADH_PARKING_RULESET_NAME = "Amanat Al Riyadh – Parking Calculation Guide by Project Type"
RIYADH_PARKING_RULESET_SOURCE_URL = "https://trc.alriyadh.gov.sa/pdf/ParkingCalculationGuideByProjectType.pdf"


def _norm(s: Any) -> str:
    return str(s or "").strip().lower()


def is_riyadh_city(city: str | None) -> bool:
    c = _norm(city)
    if not c:
        # Repo is Riyadh-first; default to applying Riyadh parking rules unless explicitly disabled.
        return True
    return ("riyadh" in c) or ("الرياض" in c)


def _is_basement_key(key: str) -> bool:
    k = _norm(key)
    return ("basement" in k) or ("underground" in k) or ("below" in k)


def _is_parking_area_key(key: str) -> bool:
    k = _norm(key)
    # Count basement as parking area by default; also allow explicit keys containing "parking".
    return _is_basement_key(k) or ("parking" in k) or ("carpark" in k) or ("car_park" in k)


def _float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return default


def _ceil_int(x: float) -> int:
    # Robust ceil for floats close to an integer.
    return int(math.ceil(x - 1e-9))


def _floor_int(x: float) -> int:
    return int(math.floor(x + 1e-9))


# --- Rules ---
#
# Supported rule types:
#   - per_m2_gfa: m2_per_space
#   - per_unit: spaces_per_unit
#   - apartments_size_threshold: < threshold => spaces_small, else spaces_large
#

RIYADH_RULES: Dict[str, Dict[str, Any]] = {
    # الشقق السكنية: 1 space per apartment if <180 m2, 2 if >180 m2
    "residential_apartments": {
        "type": "apartments_size_threshold",
        "threshold_m2": 180.0,
        "spaces_small": 1,
        "spaces_large": 2,
    },
    # الفلل والدوبلكس: 1 space per unit
    "residential_villa_duplex": {"type": "per_unit", "spaces_per_unit": 1},
    # المحلات التجارية: 1 space per 45 m2 GFA
    "retail_shops": {"type": "per_m2_gfa", "m2_per_space": 45.0},
    # المكاتب: 1 space per 40 m2 GFA
    "office": {"type": "per_m2_gfa", "m2_per_space": 40.0},
    # الأسواق التجارية: 1 space per 15 m2 GFA
    "markets": {"type": "per_m2_gfa", "m2_per_space": 15.0},
}


DEFAULT_COMPONENT_RULE_MAP: Dict[str, str] = {
    # Excel template component keys → Riyadh rule category
    "residential": "residential_apartments",
    "apartments": "residential_apartments",
    "retail": "retail_shops",
    "shops": "retail_shops",
    "commercial": "retail_shops",
    "office": "office",
    "markets": "markets",
}


def compute_built_area_from_area_ratio(site_area_m2: float, area_ratio: Dict[str, Any]) -> Dict[str, float]:
    """Compute built areas (m2) from an area_ratio dict (FAR-style ratios)."""
    built: Dict[str, float] = {}
    for k, v in (area_ratio or {}).items():
        try:
            r = float(v or 0.0)
        except Exception:
            continue
        if r <= 0:
            continue
        built[str(k)] = float(site_area_m2) * r
    return built


def parking_area_from_built_area(built_area: Dict[str, float]) -> Tuple[float, Dict[str, float]]:
    total = 0.0
    by_key: Dict[str, float] = {}
    for k, a in (built_area or {}).items():
        if not isinstance(k, str):
            continue
        if _is_parking_area_key(k):
            aa = float(a or 0.0)
            if aa > 0:
                by_key[k] = aa
                total += aa
    return total, by_key


def provided_spaces_from_area(
    parking_area_m2: float,
    *,
    gross_m2_per_space: float = 30.0,
    layout_efficiency: float = 1.0,
) -> Tuple[int, Dict[str, Any]]:
    g = _float(gross_m2_per_space, 30.0)
    eff = _float(layout_efficiency, 1.0)
    if g <= 0:
        return 0, {"gross_m2_per_space": g, "layout_efficiency": eff, "provided_raw": 0.0}
    provided_raw = float(parking_area_m2) * max(eff, 0.0) / g
    return _floor_int(provided_raw), {
        "gross_m2_per_space": g,
        "layout_efficiency": eff,
        "provided_raw": provided_raw,
    }


def _unit_mix_to_dicts(unit_mix: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in (unit_mix or []) if isinstance(unit_mix, list) else []:
        if isinstance(item, dict):
            out.append(item)
            continue
        # Pydantic v2 models have model_dump; v1 have dict()
        try:
            out.append(item.model_dump())  # type: ignore[attr-defined]
            continue
        except Exception:
            pass
        try:
            out.append(item.dict())  # type: ignore[attr-defined]
            continue
        except Exception:
            pass
        out.append(
            {
                "type": getattr(item, "type", None),
                "count": getattr(item, "count", None),
                "avg_m2": getattr(item, "avg_m2", None),
            }
        )
    return out


def _classify_residential_unit_type(unit_type: str | None) -> str:
    t = _norm(unit_type)
    if any(x in t for x in ["villa", "فيلا", "فلل", "duplex", "دوبلكس", "townhouse", "تاون"]):
        return "residential_villa_duplex"
    return "residential_apartments"


def required_spaces_riyadh(
    *,
    built_area: Dict[str, float],
    unit_mix: Any = None,
    component_rule_map: Dict[str, str] | None = None,
    assumed_avg_apartment_m2: float = 120.0,
) -> Tuple[int, Dict[str, int], Dict[str, Any]]:
    """Compute minimum required parking spaces using the Riyadh municipal guide.

    Returns:
      - required_spaces (int)
      - required_by_component (dict[str, int]) keyed by component key (e.g., residential/retail/office)
      - meta (dict) with rule provenance and warnings
    """
    warnings: List[str] = []
    by_component: Dict[str, int] = {}

    rule_map = {**DEFAULT_COMPONENT_RULE_MAP, **(component_rule_map or {})}

    # --- Residential: prefer unit_mix if available ---
    unit_dicts = _unit_mix_to_dicts(unit_mix)
    total_units = 0
    for u in unit_dicts:
        try:
            total_units += int(u.get("count") or 0)
        except Exception:
            pass

    # Residential GFA (used only for approximations when avg_m2 is missing).
    residential_gfa = 0.0
    for k, a in (built_area or {}).items():
        if not isinstance(k, str):
            continue
        kk = _norm(k)
        if kk in {"residential", "apartments", "apartment"}:
            try:
                residential_gfa += float(a or 0.0)
            except Exception:
                pass

    if residential_gfa > 0:
        if total_units > 0:
            # If avg_m2 missing, approximate from residential GFA ÷ units (very rough; flags warning).
            approx_avg = residential_gfa / max(total_units, 1)
            for u in unit_dicts:
                unit_type = u.get("type")
                count = int(u.get("count") or 0)
                if count <= 0:
                    continue
                unit_rule = _classify_residential_unit_type(str(unit_type or ""))
                rule = RIYADH_RULES.get(unit_rule, {})
                avg_m2 = _float(u.get("avg_m2"), 0.0)
                if avg_m2 <= 0:
                    avg_m2 = approx_avg
                    warnings.append("unit_mix.avg_m2 missing; approximated from residential GFA ÷ total units")
                if rule.get("type") == "apartments_size_threshold":
                    thr = _float(rule.get("threshold_m2"), 180.0)
                    spaces_per_unit = int(rule.get("spaces_large") if avg_m2 >= thr else rule.get("spaces_small"))
                else:
                    spaces_per_unit = int(rule.get("spaces_per_unit") or 1)
                by_component["residential"] = by_component.get("residential", 0) + count * spaces_per_unit
        else:
            # No unit mix → approximate apartments using assumed average size
            avg_unit = _float(assumed_avg_apartment_m2, 120.0)
            est_units_raw = residential_gfa / max(avg_unit, 1.0)
            est_units = _ceil_int(est_units_raw)
            by_component["residential"] = est_units  # assumes 1 space per unit (<180 m2 typical)
            warnings.append(
                "unit_mix missing/empty; residential parking approximated as 1 space per estimated unit "
                f"(units≈ceil(residential_gfa/avg_unit_m2) with avg_unit_m2={avg_unit:g})"
            )

    # --- Non-residential components (per m2 GFA) ---
    for key, gfa in (built_area or {}).items():
        if not isinstance(key, str):
            continue
        k = _norm(key)
        if k == "residential":
            continue
        # Ignore parking area itself when computing *required* spaces.
        if _is_parking_area_key(k):
            continue

        rule_id = rule_map.get(k)
        if not rule_id:
            continue
        rule = RIYADH_RULES.get(rule_id)
        if not isinstance(rule, dict):
            continue
        if rule.get("type") == "per_m2_gfa":
            m2_per_space = _float(rule.get("m2_per_space"), 0.0)
            if m2_per_space > 0:
                raw = float(gfa or 0.0) / m2_per_space
                by_component[key] = _ceil_int(raw)

    total_required = int(sum(by_component.values()))

    meta: Dict[str, Any] = {
        "ruleset_id": RIYADH_PARKING_RULESET_ID,
        "ruleset_name": RIYADH_PARKING_RULESET_NAME,
        "source_url": RIYADH_PARKING_RULESET_SOURCE_URL,
        "warnings": warnings,
    }
    return total_required, by_component, meta


def ensure_parking_minimums(
    *,
    excel_inputs: Dict[str, Any],
    site_area_m2: float,
    unit_mix: Any = None,
    city: str | None = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Apply Riyadh parking minimums to an excel_inputs dict.

    Behavior is controlled by these optional excel_inputs keys:
      - parking_apply: bool (default True for Riyadh)
      - parking_minimum_policy: 'auto_add_basement' (default) | 'flag_only' | 'disabled'
      - parking_supply_gross_m2_per_space: float (default 30)
      - parking_supply_layout_efficiency: float (default 1.0)
      - parking_assumed_avg_apartment_m2: float (default 120)

    The function sets:
      - parking_required_spaces_override
      - parking_required_by_component_override

    and (optionally) increases area_ratio['basement'] to eliminate a deficit.
    """

    meta: Dict[str, Any] = {"applied": False}

    if not isinstance(excel_inputs, dict):
        return excel_inputs, meta

    apply_flag = excel_inputs.get("parking_apply")
    if apply_flag is None:
        apply_flag = True

    policy = _norm(excel_inputs.get("parking_minimum_policy") or "auto_add_basement")
    if policy in {"off", "false", "0", "disabled"}:
        apply_flag = False
    if not apply_flag:
        meta["applied"] = False
        meta["reason"] = "parking_apply disabled"
        return excel_inputs, meta

    if not is_riyadh_city(city):
        meta["applied"] = False
        meta["reason"] = "city not Riyadh"
        return excel_inputs, meta

    area_ratio = excel_inputs.get("area_ratio") if isinstance(excel_inputs.get("area_ratio"), dict) else {}
    built_area = compute_built_area_from_area_ratio(site_area_m2, area_ratio or {})

    required, required_by_component, req_meta = required_spaces_riyadh(
        built_area=built_area,
        unit_mix=unit_mix,
        assumed_avg_apartment_m2=_float(excel_inputs.get("parking_assumed_avg_apartment_m2"), 120.0),
    )

    # Parking supply from built parking area (basement by default)
    parking_area_m2, parking_area_by_key = parking_area_from_built_area(built_area)
    provided, prov_meta = provided_spaces_from_area(
        parking_area_m2,
        gross_m2_per_space=_float(excel_inputs.get("parking_supply_gross_m2_per_space"), 30.0),
        layout_efficiency=_float(excel_inputs.get("parking_supply_layout_efficiency"), 1.0),
    )

    deficit = max(0, int(required) - int(provided))

    # Persist overrides for the downstream cost engine
    excel_inputs = dict(excel_inputs)
    excel_inputs["parking_required_spaces_override"] = int(required)
    excel_inputs["parking_required_by_component_override"] = dict(required_by_component)

    # Auto-fix by increasing basement ratio (does not affect FAR caps; basement excluded by scaler).
    basement_key_used = None
    basement_ratio_before = None
    basement_ratio_after = None
    basement_area_added_m2 = 0.0

    if policy == "auto_add_basement" and deficit > 0:
        # Choose a basement key to increase
        for k in (area_ratio or {}).keys():
            if isinstance(k, str) and _is_basement_key(k):
                basement_key_used = k
                break
        if basement_key_used is None:
            basement_key_used = "basement"

        # Ensure area_ratio is mutable dict
        ar_new = dict(area_ratio or {})
        basement_ratio_before = _float(ar_new.get(basement_key_used), 0.0)

        gross_m2_per_space = _float(excel_inputs.get("parking_supply_gross_m2_per_space"), 30.0)
        layout_eff = _float(excel_inputs.get("parking_supply_layout_efficiency"), 1.0)
        layout_eff = layout_eff if layout_eff > 0 else 1.0

        basement_area_added_m2 = float(deficit) * gross_m2_per_space / layout_eff
        add_ratio = basement_area_added_m2 / max(float(site_area_m2), 1e-9)

        ar_new[basement_key_used] = basement_ratio_before + add_ratio
        basement_ratio_after = float(ar_new[basement_key_used])

        excel_inputs["area_ratio"] = ar_new

        # Append an informative note for traceability
        prev_note = str(excel_inputs.get("area_ratio_note") or "").strip()
        adj_note = (
            f"Auto-added basement area to meet parking minimums: +{basement_area_added_m2:,.0f} m² "
            f"(+{add_ratio:.3f} FAR-equivalent below-grade) for {deficit} spaces."
        )
        excel_inputs["area_ratio_note"] = (prev_note + " | " + adj_note).strip(" |")

    meta = {
        "applied": True,
        "city": city or "Riyadh",
        "policy": policy,
        "required_spaces": int(required),
        "required_by_component": dict(required_by_component),
        "provided_spaces_before": int(provided),
        "deficit_spaces_before": int(deficit),
        "parking_area_m2_before": float(parking_area_m2),
        "parking_area_by_key_before": dict(parking_area_by_key),
        "requirement_meta": req_meta,
        "provision_meta": prov_meta,
        "basement_key_used": basement_key_used,
        "basement_ratio_before": basement_ratio_before,
        "basement_ratio_after": basement_ratio_after,
        "basement_area_added_m2": basement_area_added_m2,
    }
    return excel_inputs, meta
