import copy

import pytest

from app.services.excel_method import compute_excel_estimate
from tests.excel_inputs import sample_excel_inputs


def test_default_off_does_not_change_y1_income():
    site_area = 1000.0
    excel = compute_excel_estimate(site_area, sample_excel_inputs())
    assert excel["parking_income_y1"] == 0.0
    assert "parking_income" not in excel["y1_income_components"]
    assert excel["y1_income"] == pytest.approx(sum(excel["y1_income_components"].values()))


def test_monetized_adds_income_and_meta_present():
    site_area = 1000.0
    base_inputs = sample_excel_inputs()
    excel_off = compute_excel_estimate(site_area, base_inputs)

    monetized_inputs = sample_excel_inputs()
    monetized_inputs["monetize_extra_parking"] = True
    monetized_inputs["parking_public_access"] = True
    excel_on = compute_excel_estimate(site_area, monetized_inputs)

    assert excel_on["parking_income_y1"] > 0
    assert "parking_income" in excel_on["y1_income_components"]
    assert excel_on["y1_income"] > excel_off["y1_income"]
    assert excel_on["parking_income_meta"]["monetize_extra_parking"] is True


def test_landuse_changes_rate_order_s_m_c():
    site_area = 1000.0

    def _rate_for_land_use(code: str) -> float:
        inputs = sample_excel_inputs()
        inputs["monetize_extra_parking"] = True
        inputs["parking_public_access"] = True
        inputs["land_use_code"] = code
        excel = compute_excel_estimate(site_area, inputs)
        return excel["parking_monthly_rate_used"]

    rate_s = _rate_for_land_use("s")
    rate_m = _rate_for_land_use("m")
    rate_c = _rate_for_land_use("c")

    assert rate_s < rate_m < rate_c


def test_premium_increases_with_land_price():
    site_area = 1000.0
    base = sample_excel_inputs()
    base["monetize_extra_parking"] = True
    base["parking_public_access"] = True
    base["land_use_code"] = "c"
    low_price = copy.deepcopy(base)
    low_price["land_price_sar_m2"] = 8000
    high_price = copy.deepcopy(base)
    high_price["land_price_sar_m2"] = 12000

    low_excel = compute_excel_estimate(site_area, low_price)
    high_excel = compute_excel_estimate(site_area, high_price)

    assert high_excel["parking_monthly_rate_used"] > low_excel["parking_monthly_rate_used"]


def test_override_rate_is_used_exactly():
    site_area = 1000.0
    inputs = sample_excel_inputs()
    inputs["monetize_extra_parking"] = True
    inputs["parking_public_access"] = False
    inputs["parking_monthly_rate_sar_per_space"] = 777.0
    inputs["land_use_code"] = "c"

    excel = compute_excel_estimate(site_area, inputs)

    assert excel["parking_monthly_rate_used"] == pytest.approx(777.0)
    assert excel["parking_income_meta"]["rate_clamped_floor"] is False
    assert excel["parking_income_meta"]["rate_clamped_cap"] is False


def test_deterministic_same_inputs():
    site_area = 1000.0
    inputs = sample_excel_inputs()
    inputs["monetize_extra_parking"] = True
    inputs["parking_public_access"] = True

    excel1 = compute_excel_estimate(site_area, inputs)
    excel2 = compute_excel_estimate(site_area, inputs)

    assert excel1["parking_income_y1"] == excel2["parking_income_y1"]
    assert excel1["parking_income_meta"] == excel2["parking_income_meta"]
