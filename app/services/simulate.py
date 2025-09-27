import math
import random
from typing import Dict, Any, Tuple, List


def percentile(xs: List[float], p: float) -> float:
    xs = sorted(xs)
    k = (len(xs) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return xs[int(k)]
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def p_bands(p50_profit: float, drivers: Dict[str, Tuple[float, float]], runs: int = 1000) -> Dict[str, float]:
    """
    drivers: { "land_ppm2": (mu, sigma), "unit_cost": (mu, sigma), "gdv_m2_price": (mu, sigma) }
    Simple lognormal-ish perturbation: x' = mu * exp(N(0, sigma))
    """
    outcomes = []
    mu_land, s_land = drivers.get("land_ppm2", (1.0, 0.10))
    mu_unit, s_unit = drivers.get("unit_cost", (1.0, 0.08))
    mu_price, s_price = drivers.get("gdv_m2_price", (1.0, 0.10))
    for _ in range(runs):
        land_f = mu_land * math.exp(random.gauss(0.0, s_land))
        unit_f = mu_unit * math.exp(random.gauss(0.0, s_unit))
        price_f = mu_price * math.exp(random.gauss(0.0, s_price))
        # approximate profit scaling: price ↑ helps, unit cost ↑ hurts, land ↑ hurts
        profit = p50_profit * (price_f / (unit_f * land_f))
        outcomes.append(profit)
    return {
        "p5": percentile(outcomes, 0.05),
        "p50": percentile(outcomes, 0.50),
        "p95": percentile(outcomes, 0.95),
    }
