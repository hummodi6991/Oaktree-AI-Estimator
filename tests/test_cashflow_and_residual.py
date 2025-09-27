from app.services.cashflow import build_equity_cashflow
from app.services.residual import residual_land_value


def test_residual_non_negative():
    assert residual_land_value(10_000, 8_000, 1_000, 500, 0.15) >= 0.0


def test_cashflow_shape():
    cf = build_equity_cashflow(
        months=12, land_value=2_000_000, hard_costs=10_000_000, soft_costs=1_500_000,
        gdv=16_000_000, apr=0.08, ltv=0.6, sales_cost_pct=0.02
    )
    assert "irr_annual" in cf and "schedule" in cf and len(cf["schedule"]) == 13  # t0..t12
