from datetime import date
from typing import Dict, Any


def assemble(
    land_value: float,
    hard_costs: float,
    soft_costs: float,
    financing_interest: float,
    revenues: float,
) -> Dict[str, Any]:
    total_cost = land_value + hard_costs + soft_costs + financing_interest
    p50_profit = revenues - total_cost
    # light bands: widen when profit close to zero
    band = max(abs(p50_profit) * 0.4, total_cost * 0.05)
    return {
        "totals": {
            "land_value": land_value,
            "hard_costs": hard_costs,
            "soft_costs": soft_costs,
            "financing": financing_interest,
            "revenues": revenues,
            "p50_profit": p50_profit,
        },
        "confidence_bands": {
            "p5": p50_profit - band,
            "p50": p50_profit,
            "p95": p50_profit + band,
        },
    }
