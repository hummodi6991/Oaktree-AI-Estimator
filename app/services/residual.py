def residual_land_value(
    gdv: float,
    hard_costs: float,
    soft_costs: float,
    financing_interest: float,
    dev_margin_pct: float = 0.15,
) -> float:
    """
    Residual land = GDV - (Hard + Soft + Financing + Developer margin).
    Returns 0 if negative (guard for early MVP).
    """
    required_profit = dev_margin_pct * gdv
    rlv = gdv - (hard_costs + soft_costs + financing_interest + required_profit)
    return max(0.0, rlv)
