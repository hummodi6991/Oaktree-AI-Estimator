import math
from typing import Any, Dict


def _is_basement_key(key: str) -> bool:
    k = (key or "").strip().lower()
    if k in {"basement", "underground"}:
        return True
    if "basement" in k:
        return True
    return False


def _area_ratio_positive_sum(ar: Any, exclude_basement: bool = False) -> float:
    if not isinstance(ar, dict):
        return 0.0
    total = 0.0
    for k, v in ar.items():
        if exclude_basement and _is_basement_key(str(k)):
            continue
        try:
            fv = float(v)
            if fv > 0:
                total += fv
        except Exception:
            continue
    return total


def _is_placeholder_area_ratio(ar: Any) -> bool:
    if not isinstance(ar, dict) or not ar:
        return True
    positive: dict[str, float] = {}
    for k, v in ar.items():
        try:
            fv = float(v)
            if fv > 0:
                positive[str(k).strip().lower()] = fv
        except Exception:
            continue
    if not positive:
        return True
    # Legacy placeholder: 2.7 everywhere
    if all(abs(v - 2.7) < 1e-6 for v in positive.values()):
        return True
    # Current residential template placeholder: residential 1.6 + basement 1.0
    r = positive.get("residential")
    b = positive.get("basement")
    if (
        r is not None
        and b is not None
        and abs(r - 1.6) < 1e-6
        and abs(b - 1.0) < 1e-6
        and len(positive) <= 2
    ):
        return True
    return False


def scale_placeholder_area_ratio(
    excel_inputs: Dict[str, Any], target_far: float | None, target_far_source: str | None = None
) -> Dict[str, Any]:
    """Scale Excel area ratios to match a target FAR if the ratios are template placeholders."""

    result = dict(excel_inputs or {})
    area_ratio = result.get("area_ratio") if isinstance(result, dict) else {}
    placeholder_area_ratio = _is_placeholder_area_ratio(area_ratio)

    # Guardrail:
    # If FAR is coming only from the API default fallback (method == "default_far"),
    # do NOT scale the template ratios. Otherwise every parcel becomes FAR=2.0
    # (e.g., 1.6 -> 2.0 scale 1.25).
    src_norm = (target_far_source or "").strip().lower()
    if src_norm in {"default_far", "default"}:
        return result

    should_scale_area_ratio = placeholder_area_ratio and target_far is not None and float(target_far) > 0
    if not should_scale_area_ratio:
        return result

    # Match above-ground FAR only; basement stays as-is
    base_sum = _area_ratio_positive_sum(area_ratio, exclude_basement=True)
    scaled = None
    scale_str = "n/a"
    if base_sum > 0 and isinstance(area_ratio, dict):
        scale = float(target_far) / base_sum
        scale_str = f"{scale:.3f}"
        scaled = {}
        for key, val in area_ratio.items():
            try:
                fv = float(val or 0.0)
            except Exception:
                fv = 0.0
            if fv <= 0:
                scaled[key] = fv
            elif _is_basement_key(str(key)):
                scaled[key] = fv
            else:
                scaled[key] = fv * scale
    elif isinstance(area_ratio, dict):
        scaled = {}
        for key, val in area_ratio.items():
            try:
                scaled[key] = float(val or 0.0)
            except Exception:
                scaled[key] = 0.0
        scaled["residential"] = float(target_far)
    else:
        scaled = {"residential": float(target_far)}

    result["area_ratio"] = scaled
    result["area_ratio_note"] = (
        f"Auto-scaled above-ground area ratios: baseline FAR {base_sum:.2f} → "
        f"target FAR {float(target_far):.2f} (scale {scale_str}; source {target_far_source or 'far'}); "
        "basement ratio unchanged"
    )
    return result


def _is_basement_area_ratio_key(key: str) -> bool:
    """True if this `area_ratio` key represents below-grade area (not scaled by above-ground floors)."""
    k = (key or "").strip().lower()
    return ("basement" in k) or ("underground" in k) or ("below" in k)


def scale_area_ratio_by_floors(
    inputs: Dict[str, Any],
    *,
    desired_floors_above_ground: float | int | str | None,
    baseline_floors_above_ground: float | int | str | None,
    desired_floors_source: str | None = None,
    baseline_floors_source: str | None = None,
) -> Dict[str, Any]:
    """
    Option B — Use floors (stories) to scale `area_ratio`.

    We treat the *current* above-ground `area_ratio` values as representing a baseline FAR at
    `baseline_floors_above_ground`. To reflect a different allowed floors count, we scale all
    above-ground area_ratio entries by:

        factor = desired_floors_above_ground / baseline_floors_above_ground

    Basement / underground ratios are left unchanged.
    """
    if desired_floors_above_ground is None or baseline_floors_above_ground is None:
        return inputs

    try:
        desired = float(desired_floors_above_ground)
        baseline = float(baseline_floors_above_ground)
    except Exception:
        return inputs

    if desired <= 0 or baseline <= 0:
        return inputs

    factor = desired / baseline

    area_ratio = inputs.get("area_ratio")
    if not isinstance(area_ratio, dict):
        return inputs

    # No-op factor: still record metadata for transparency.
    if abs(factor - 1.0) < 1e-9:
        inputs.setdefault("floors_above_ground", desired)
        inputs.setdefault("baseline_floors_above_ground", baseline)
        if desired_floors_source is not None:
            inputs.setdefault("floors_above_ground_source", desired_floors_source)
        if baseline_floors_source is not None:
            inputs.setdefault("baseline_floors_above_ground_source", baseline_floors_source)
        return inputs

    new_area_ratio: Dict[str, Any] = {}
    scaled_keys: list[str] = []
    for k, v in area_ratio.items():
        # Preserve non-numeric values verbatim.
        try:
            r = float(v or 0)
        except Exception:
            new_area_ratio[k] = v
            continue

        if isinstance(k, str) and _is_basement_area_ratio_key(k):
            new_area_ratio[k] = r
            continue

        new_area_ratio[k] = r * factor
        if isinstance(k, str):
            scaled_keys.append(k)

    inputs["area_ratio"] = new_area_ratio

    note_parts: list[str] = [
        f"Floors scaling applied: above-ground area ratios × {factor:.3f} "
        f"(baseline floors {baseline:g} → desired floors {desired:g}; basement unchanged)."
    ]
    if baseline_floors_source or desired_floors_source:
        sources: list[str] = []
        if baseline_floors_source:
            sources.append(f"baseline={baseline_floors_source}")
        if desired_floors_source:
            sources.append(f"desired={desired_floors_source}")
        note_parts.append(f"Sources: {', '.join(sources)}.")
    if scaled_keys:
        note_parts.append(f"Scaled keys: {', '.join(scaled_keys)}.")

    existing_note = str(inputs.get("area_ratio_note") or "").strip()
    appended_note = " ".join(note_parts).strip()
    inputs["area_ratio_note"] = (existing_note + " " + appended_note).strip() if existing_note else appended_note

    # Helpful metadata for UI/debugging (ignored by estimator math).
    inputs["floors_above_ground"] = desired
    inputs["baseline_floors_above_ground"] = baseline
    if desired_floors_source is not None:
        inputs["floors_above_ground_source"] = desired_floors_source
    if baseline_floors_source is not None:
        inputs["baseline_floors_above_ground_source"] = baseline_floors_source

    return inputs


def _fmt_amount(value: float | int | str | None, decimals: int = 3) -> str:
    try:
        return f"{float(value):,.{decimals}f}"
    except Exception:
        return str(value)


def build_excel_explanations(
    site_area_m2: float, inputs: Dict[str, Any], breakdown: Dict[str, Any]
) -> Dict[str, str]:
    """Human-readable explanations for the Excel-style cost breakdown.

    These are used by the web UI and PDF export, so keep wording synchronized here.
    """

    unit_cost = inputs.get("unit_cost", {}) or {}
    rent_rates = inputs.get("rent_sar_m2_yr", {}) or {}
    rent_applied = breakdown.get("rent_applied_sar_m2_yr", {}) or {}
    efficiency = inputs.get("efficiency", {}) or {}
    area_ratio = inputs.get("area_ratio", {}) or {}

    built_area = breakdown.get("built_area", {}) or {}
    nla = breakdown.get("nla", {}) or {}
    y1_income_components = breakdown.get("y1_income_components", {}) or {}
    direct_cost = breakdown.get("direct_cost", {}) or {}

    re_scalar = float(inputs.get("re_price_index_scalar") or 1.0)

    explanations: Dict[str, str] = {}
    area_ratio_note = str(inputs.get("area_ratio_note") or "").strip()
    if area_ratio_note:
        explanations["area_ratio_override"] = area_ratio_note

    land_price = float(inputs.get("land_price_sar_m2", 0.0) or 0.0)
    land_cost_total = float(breakdown.get("land_cost", site_area_m2 * land_price) or 0.0)
    explanations["land_cost"] = (
        f"{_fmt_amount(site_area_m2)} m² × {land_price:,.0f} SAR/m² = {_fmt_amount(land_cost_total)} SAR. "
        "Land price is estimated using a hedonic model calibrated on comparable transactions."
    )

    note_appended = False
    for key, area in built_area.items():
        ratio = float(area_ratio.get(key, 0.0) or 0.0)
        if ratio:
            key_lower = str(key).lower()
            if key == "residential":
                explanations[f"{key}_bua"] = (
                    f"{_fmt_amount(site_area_m2)} m² × FAR {ratio:.3f} = {_fmt_amount(area)} m². "
                    "FAR reflects observed surrounding development density inferred from Overture building data."
                )
            elif key_lower.startswith("basement"):
                explanations[f"{key}_bua"] = (
                    f"{_fmt_amount(site_area_m2)} m² × basement ratio {ratio:.3f} = {_fmt_amount(area)} m². "
                    "Basement area is estimated separately and excluded from FAR calculations."
                )
            else:
                explanations[f"{key}_bua"] = (
                    f"{_fmt_amount(site_area_m2)} m² × area ratio {ratio:.3f} = {_fmt_amount(area)} m²."
                )
            if area_ratio_note and not note_appended and not key_lower.startswith("basement"):
                explanations[f"{key}_bua"] = f"{explanations[f'{key}_bua']} {area_ratio_note}"
                note_appended = True
        else:
            explanations[f"{key}_bua"] = f"Built-up area {_fmt_amount(area)} m²."

    sub_total = float(breakdown.get("sub_total", 0.0) or 0.0)
    direct_total = sum(direct_cost.values())
    construction_parts = []
    for key, area in built_area.items():
        base_unit = unit_cost.get("basement") if key.lower().startswith("basement") else unit_cost.get(key, 0.0)
        construction_parts.append(
            f"{key}: {_fmt_amount(area)} m² × {float(base_unit):,.0f} SAR/m² = "
            f"{_fmt_amount(float(direct_cost.get(key, 0.0)))} SAR"
        )
    if construction_parts:
        explanations["construction_direct"] = (
            "; ".join(construction_parts) + f". Total construction cost: {_fmt_amount(direct_total)} SAR."
        )

    fitout_area = sum(
        value for key, value in built_area.items() if not key.lower().startswith("basement")
    )
    fitout_rate = float(inputs.get("fitout_rate") or 0.0)
    fitout_cost = float(breakdown.get("fitout_cost", 0.0) or 0.0)
    explanations["fitout"] = (
        f"{_fmt_amount(fitout_area)} m² × {fitout_rate:,.0f} SAR/m² = {_fmt_amount(fitout_cost)} SAR. "
        "Applied to above-ground built-up area only."
    )

    contingency_pct = float(inputs.get("contingency_pct") or 0.0)
    contingency_cost = float(breakdown.get("contingency_cost", 0.0) or 0.0)
    explanations["contingency"] = (
        f"{contingency_pct * 100:.1f}% × (construction direct cost {_fmt_amount(direct_total)} SAR + "
        f"fit-out {_fmt_amount(fitout_cost)} SAR) = {_fmt_amount(contingency_cost)} SAR. "
        "This applies contingency to total hard construction scope, including above-ground fit-out, as an allowance "
        "for design development and execution risk."
    )

    consultants_pct = float(inputs.get("consultants_pct") or 0.0)
    consultants_base = sub_total + contingency_cost
    explanations["consultants"] = (
        f"{consultants_pct * 100:.1f}% × (construction + contingency) {_fmt_amount(consultants_base)} SAR "
        f"= {_fmt_amount(float(breakdown.get('consultants_cost', 0.0) or 0.0))} SAR."
    )

    transaction_pct = float(inputs.get("transaction_pct") or 0.0)
    tx_label = inputs.get("transaction_label") or "transaction"
    explanations["transaction_cost"] = (
        f"{transaction_pct * 100:.1f}% × {_fmt_amount(land_cost_total)} SAR = "
        f"{_fmt_amount(float(breakdown.get('transaction_cost', 0.0) or 0.0))} SAR. "
        "Calculated in accordance with Saudi RETT regulations."
    )

    income_parts = []
    rent_meta = inputs.get("rent_source_metadata") or {}
    rent_components_meta = rent_meta.get("components") if isinstance(rent_meta, dict) else {}
    for key, component in y1_income_components.items():
        nla_val = float(nla.get(key, 0.0) or 0.0)
        base_rent = float(rent_rates.get(key, 0.0) or 0.0)
        applied_rent = float(rent_applied.get(key, 0.0) or 0.0)
        effective_rent = applied_rent if applied_rent > 0 else base_rent * re_scalar
        rent_used = (component / nla_val) if nla_val else effective_rent
        comp_meta = rent_components_meta.get(key) if isinstance(rent_components_meta, dict) else {}
        # More precise label when a component fell back to template defaults
        rent_label = "template default"
        if isinstance(comp_meta, dict):
            rent_label = comp_meta.get("method") or comp_meta.get("provider") or rent_label
        if rent_label == "template default" and isinstance(rent_meta, dict):
            rent_label = (
                rent_meta.get("method")
                or rent_meta.get("provider")
                or rent_label
            )
        note = ""
        if applied_rent and abs(applied_rent - base_rent) > 1e-9 and re_scalar not in (0.0, 1.0):
            note = f" (includes real estate price index scalar {re_scalar:,.3f})"
        income_parts.append(
            f"{key} net lettable area {_fmt_amount(nla_val, decimals=2)} m² × {rent_used:,.0f} SAR/m²/year "
            f"= {_fmt_amount(component)} SAR/year. Rent benchmark sourced from {rent_label}.{note}"
        )
    if income_parts:
        explanations["y1_income"] = "; ".join(income_parts)

    y1_income_total = float(breakdown.get("y1_income", 0.0) or 0.0)
    grand_total_capex = float(breakdown.get("grand_total_capex", 0.0) or 0.0)
    roi = float(breakdown.get("roi", 0.0) or 0.0)
    explanations["roi"] = (
        f"Year-1 net income {_fmt_amount(y1_income_total)} SAR ÷ total development cost "
        f"{_fmt_amount(grand_total_capex)} SAR = {roi * 100:,.2f}%."
    )

    return explanations


def _build_cost_breakdown_rows(
    built_area: Dict[str, float],
    area_ratio: Dict[str, Any],
    inputs: Dict[str, Any],
    explanations: Dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def _norm_land_use() -> str:
        lu = (inputs.get("land_use_code") or inputs.get("land_use") or "").strip().lower()
        if lu:
            return lu
        keys = {str(k).strip().lower() for k in area_ratio.keys()}
        if {"retail", "office"} & keys:
            return "m"
        return ""

    land_use = _norm_land_use()

    def _append_bua_row(key: str, label: str, *, require_mixed_use: bool = False) -> None:
        if require_mixed_use and land_use != "m":
            return
        if key in {"retail", "office"}:
            if key not in area_ratio or built_area.get(key, 0.0) <= 0:
                return
            # Retail and Office BUA are surfaced for mixed-use clarity; values already existed but were previously implicit.
        value = built_area.get(key)
        if value is None:
            return
        rows.append(
            {
                "category": "cost",
                "key": f"{key}_bua",
                "label": label,
                "unit": "m²",
                "value": value,
                "note": explanations.get(f"{key}_bua"),
            }
        )

    _append_bua_row("residential", "Residential BUA")
    _append_bua_row("retail", "Retail BUA", require_mixed_use=True)
    _append_bua_row("office", "Office BUA", require_mixed_use=True)
    _append_bua_row("basement", "Basement BUA")

    return rows


def compute_excel_estimate(site_area_m2: float, inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Compute an Excel-style estimate using caller-provided parameters."""

    area_ratio = inputs.get("area_ratio", {}) or {}
    unit_cost = inputs.get("unit_cost", {}) or {}
    cp_density = inputs.get("cp_sqm_per_space", {}) or {}
    efficiency = inputs.get("efficiency", {}) or {}
    rent_rates = inputs.get("rent_sar_m2_yr", {}) or {}
    re_scalar = float(inputs.get("re_price_index_scalar") or 1.0)

    built_area = {key: float(area_ratio.get(key, 0.0)) * float(site_area_m2) for key in area_ratio.keys()}
    shell_unit = (unit_cost.get("residential") or 0.0)
    basement_unit = (unit_cost.get("basement") or 0.0)
    direct_cost = {}
    for key in area_ratio.keys():
        unit_rate = float(unit_cost.get(key, 0.0))
        if key == "residential":
            unit_rate = float(shell_unit)
        elif key.lower().startswith("basement"):
            unit_rate = float(basement_unit)
        direct_cost[key] = built_area.get(key, 0.0) * unit_rate

    # --- Parking (required + provided) ---
    # parking_required_* can be overridden upstream (e.g., using Riyadh municipal minimums).
    parking_required_by_component: Dict[str, int] = {}
    parking_required_by_component_override = inputs.get("parking_required_by_component_override")
    if isinstance(parking_required_by_component_override, dict):
        for k, v in parking_required_by_component_override.items():
            try:
                parking_required_by_component[str(k)] = int(float(v or 0.0))
            except Exception:
                parking_required_by_component[str(k)] = 0
    else:
        # Fallback: interpret cp_sqm_per_space as "m² GFA per required space" for above-ground components.
        for key in area_ratio.keys():
            if isinstance(key, str):
                kk = key.strip().lower()
                if _is_basement_area_ratio_key(kk) or ("parking" in kk) or ("carpark" in kk) or ("car_park" in kk):
                    continue
            cp = float(cp_density.get(key, 0.0) or 0.0)
            if cp > 0:
                raw = float(built_area.get(key, 0.0) or 0.0) / cp
                parking_required_by_component[str(key)] = int(math.ceil(raw - 1e-9))
            else:
                parking_required_by_component[str(key)] = 0

    parking_required_spaces_override = inputs.get("parking_required_spaces_override")
    if parking_required_spaces_override is not None:
        try:
            parking_required_spaces_raw = float(parking_required_spaces_override)
        except Exception:
            parking_required_spaces_raw = float(sum(parking_required_by_component.values()))
    else:
        parking_required_spaces_raw = float(sum(parking_required_by_component.values()))
    parking_required_spaces = int(math.ceil(parking_required_spaces_raw - 1e-9))

    # Parking supply: derive provided stalls from below-grade + explicit parking areas.
    parking_supply_gross_m2_per_space = float(inputs.get("parking_supply_gross_m2_per_space") or 30.0)
    parking_supply_layout_efficiency = float(inputs.get("parking_supply_layout_efficiency") or 1.0)
    parking_area_m2 = 0.0
    parking_area_by_key: Dict[str, float] = {}
    for key, area in built_area.items():
        if not isinstance(key, str):
            continue
        kk = key.strip().lower()
        if _is_basement_area_ratio_key(kk) or ("parking" in kk) or ("carpark" in kk) or ("car_park" in kk):
            a = float(area or 0.0)
            if a > 0:
                parking_area_by_key[key] = a
                parking_area_m2 += a
    if parking_supply_gross_m2_per_space > 0:
        parking_provided_raw = (
            parking_area_m2 * max(parking_supply_layout_efficiency, 0.0) / parking_supply_gross_m2_per_space
        )
    else:
        parking_provided_raw = 0.0
    parking_provided_spaces = int(math.floor(parking_provided_raw + 1e-9))
    parking_deficit_spaces = max(0, parking_required_spaces - parking_provided_spaces)
    parking_compliant = parking_deficit_spaces == 0

    fitout_area = sum(
        value for key, value in built_area.items() if not key.lower().startswith("basement")
    )
    fitout_rate = float(inputs.get("fitout_rate") or 0.0)
    fitout_cost = fitout_area * fitout_rate

    sub_total = sum(direct_cost.values()) + fitout_cost
    contingency_cost = sub_total * float(inputs.get("contingency_pct", 0.0))
    consultants_cost = (sub_total + contingency_cost) * float(inputs.get("consultants_pct", 0.0))
    feasibility_fee = float(inputs.get("feasibility_fee", 0.0))
    land_cost = float(site_area_m2) * float(inputs.get("land_price_sar_m2", 0.0))
    transaction_cost = land_cost * float(inputs.get("transaction_pct", 0.0))

    grand_total_capex = (
        sub_total
        + contingency_cost
        + consultants_cost
        + feasibility_fee
        + land_cost
        + transaction_cost
    )

    nla = {key: built_area.get(key, 0.0) * float(efficiency.get(key, 0.0)) for key in area_ratio.keys()}
    rent_applied = {
        key: float(rent_rates.get(key, 0.0)) * re_scalar
        for key in set(rent_rates.keys()) | set(area_ratio.keys())
    }
    y1_income_components = {key: nla.get(key, 0.0) * rent_applied.get(key, 0.0) for key in area_ratio.keys()}
    y1_income = sum(y1_income_components.values())

    roi = (y1_income / grand_total_capex) if grand_total_capex > 0 else 0.0

    result = {
        "built_area": built_area,
        "direct_cost": direct_cost,
        "fitout_cost": fitout_cost,
        "parking_required_spaces": parking_required_spaces,
        "parking_required_spaces_raw": parking_required_spaces_raw,
        "parking_required_by_component": parking_required_by_component,
        "parking_provided_spaces": parking_provided_spaces,
        "parking_provided_spaces_raw": parking_provided_raw,
        "parking_deficit_spaces": parking_deficit_spaces,
        "parking_compliant": parking_compliant,
        "parking_area_m2": parking_area_m2,
        "parking_area_by_key": parking_area_by_key,
        "parking_supply_gross_m2_per_space": parking_supply_gross_m2_per_space,
        "parking_supply_layout_efficiency": parking_supply_layout_efficiency,
        "sub_total": sub_total,
        "contingency_cost": contingency_cost,
        "consultants_cost": consultants_cost,
        "feasibility_fee": feasibility_fee,
        "land_cost": land_cost,
        "transaction_cost": transaction_cost,
        "grand_total_capex": grand_total_capex,
        "nla": nla,
        "y1_income_components": y1_income_components,
        "y1_income": y1_income,
        "rent_applied_sar_m2_yr": rent_applied,
        "roi": roi,
    }

    explanations = build_excel_explanations(site_area_m2, inputs, result)
    result["explanations"] = explanations
    result["cost_breakdown_rows"] = _build_cost_breakdown_rows(built_area, area_ratio, inputs, explanations)

    return result
