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
    # Current residential template placeholder: residential 1.6 + basement 0.5
    r = positive.get("residential")
    b = positive.get("basement")
    if (
        r is not None
        and b is not None
        and abs(r - 1.6) < 1e-6
        and abs(b - 0.5) < 1e-6
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
        f"{contingency_pct * 100:.1f}% × {_fmt_amount(sub_total)} SAR = {_fmt_amount(contingency_cost)} SAR. "
        "Allowance for design development and execution risk."
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
    rent_label = rent_meta.get("method") or rent_meta.get("provider") or "the supplied rent benchmark"
    for key, component in y1_income_components.items():
        nla_val = float(nla.get(key, 0.0) or 0.0)
        base_area = float(built_area.get(key, 0.0) or 0.0)
        eff = float(efficiency.get(key, 0.0) or 0.0)
        base_rent = float(rent_rates.get(key, 0.0) or 0.0)
        effective_rent = base_rent * re_scalar
        rent_used = (component / nla_val) if nla_val else effective_rent
        income_parts.append(
            f"{key} net lettable area {_fmt_amount(nla_val, decimals=2)} m² × {rent_used:,.0f} SAR/m²/year "
            f"= {_fmt_amount(component)} SAR/year. Rent benchmark sourced from {rent_label}."
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
    parking_required = {
        key: (
            (built_area.get(key, 0.0) / float(cp_density.get(key, 1.0)))
            if float(cp_density.get(key, 0.0)) > 0
            else 0.0
        )
        for key in area_ratio.keys()
    }

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
    y1_income_components = {
        key: nla.get(key, 0.0) * float(rent_rates.get(key, 0.0)) * re_scalar
        for key in area_ratio.keys()
    }
    y1_income = sum(y1_income_components.values())

    roi = (y1_income / grand_total_capex) if grand_total_capex > 0 else 0.0

    result = {
        "built_area": built_area,
        "direct_cost": direct_cost,
        "fitout_cost": fitout_cost,
        "parking_required_spaces": sum(parking_required.values()),
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
        "roi": roi,
    }

    result["explanations"] = build_excel_explanations(site_area_m2, inputs, result)

    return result
