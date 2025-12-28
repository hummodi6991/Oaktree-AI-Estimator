from __future__ import annotations


def sample_excel_inputs() -> dict:
    return {
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
