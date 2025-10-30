from typing import Any, Dict


def compute_excel_estimate(site_area_m2: float, inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Compute an Excel-style estimate using caller-provided parameters."""

    area_ratio = inputs.get("area_ratio", {}) or {}
    unit_cost = inputs.get("unit_cost", {}) or {}
    cp_density = inputs.get("cp_sqm_per_space", {}) or {}
    efficiency = inputs.get("efficiency", {}) or {}
    rent_rates = inputs.get("rent_sar_m2_yr", {}) or {}

    built_area = {key: float(area_ratio.get(key, 0.0)) * float(site_area_m2) for key in area_ratio.keys()}
    direct_cost = {
        key: built_area.get(key, 0.0) * float(unit_cost.get(key, 0.0)) for key in area_ratio.keys()
    }
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
    fitout_cost = fitout_area * float(inputs.get("fitout_rate", 0.0))

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
        key: nla.get(key, 0.0) * float(rent_rates.get(key, 0.0)) for key in area_ratio.keys()
    }
    y1_income = sum(y1_income_components.values())

    roi = (y1_income / grand_total_capex) if grand_total_capex > 0 else 0.0

    return {
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
