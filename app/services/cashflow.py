from typing import List, Dict, Any
import math


def _s_curve_profile(months: int, a: float = 0.15, b: float = 0.50, c: float = 0.35) -> List[float]:
    months = max(1, months)
    if months < 3:
        # tiny projects: spread roughly evenly
        return [1.0 / months] * months
    seg = months // 3
    tail = months - 2 * seg
    prof = [a / seg] * seg + [b / seg] * seg + [c / tail] * tail
    s = sum(prof)
    return [p / s for p in prof]


def _irr_monthly(cash: List[float], lo: float = -0.99, hi: float = 1.0, tol: float = 1e-6, iters: int = 200) -> float:
    """Robust bisection IRR on monthly cash flows (no numpy_financial dependency)."""

    def npv(r: float) -> float:
        acc = 0.0
        for t, cf in enumerate(cash):
            acc += cf / ((1 + r) ** t)
        return acc

    f_lo, f_hi = npv(lo), npv(hi)
    if f_lo * f_hi > 0:
        return 0.0  # no sign change -> fall back
    for _ in range(iters):
        mid = (lo + hi) / 2.0
        f_mid = npv(mid)
        if abs(f_mid) < tol:
            return mid
        if f_lo * f_mid < 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return (lo + hi) / 2.0


def build_equity_cashflow(
    months: int,
    land_value: float,
    hard_costs: float,
    soft_costs: float,
    gdv: float,
    apr: float,  # decimal p.a. (e.g., 0.085)
    ltv: float,  # debt share of hard+soft draws (land paid by equity in MVP)
    sales_cost_pct: float = 0.02,  # closing/fees at exit
) -> Dict[str, Any]:
    """
    Simple equity cash flow:
      - Land is paid at t0 with equity.
      - Hard+Soft funded by LTV debt + (1-LTV) equity, drawn on an S-curve.
      - Interest is capitalized monthly and repaid with principal at exit.
      - At exit (last month), receive GDV minus sales costs, then repay debt+interest.
    Returns schedule and IRR (annualized).
    """
    prof = _s_curve_profile(months)
    cost_total = hard_costs + soft_costs
    mr = apr / 12.0

    # Monthly draws
    debt_draws = [cost_total * ltv * p for p in prof]
    eq_draws = [cost_total * (1.0 - ltv) * p for p in prof]

    # Debt balance & interest (capitalized)
    bal = 0.0
    interest_m = []
    bal_m = []
    for d in debt_draws:
        bal += d
        i = bal * mr
        interest_m.append(i)
        bal += i
        bal_m.append(bal)

    # Equity cash flows: t0 land purchase, then monthly equity draws, then exit proceeds
    cash = [-land_value]  # t0
    cash += [-x for x in eq_draws]  # months 1..M

    # Exit in month M: proceeds net of sales costs, minus debt repayment (principal+interest)
    net_proceeds = gdv * (1.0 - sales_cost_pct) - bal
    cash[-1] = cash[-1] + net_proceeds  # add to last month equity line

    irr_m = _irr_monthly(cash)
    irr_a = (1.0 + irr_m) ** 12 - 1.0

    schedule = []
    cum_eq = 0.0
    for m in range(len(cash)):
        if m == 0:
            eq = cash[m]
            cum_eq += eq
            schedule.append(
                {
                    "month": m,
                    "equity_flow": eq,
                    "debt_draw": 0.0,
                    "debt_balance": 0.0,
                    "interest": 0.0,
                    "cum_equity": cum_eq,
                }
            )
        else:
            eq = cash[m]
            cum_eq += eq
            schedule.append(
                {
                    "month": m,
                    "equity_flow": eq,
                    "debt_draw": debt_draws[m - 1],
                    "debt_balance": bal_m[m - 1],
                    "interest": interest_m[m - 1],
                    "cum_equity": cum_eq,
                }
            )
    return {
        "irr_annual": irr_a,
        "schedule": schedule,
        "peaks": {
            "peak_equity": min(0.0, min(s["cum_equity"] for s in schedule)),
            "peak_debt": max(bal_m) if bal_m else 0.0,
            "capitalized_interest": sum(interest_m),
        },
    }
