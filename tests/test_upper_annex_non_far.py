import pytest

from app.services.excel_method import compute_excel_estimate


def test_upper_annex_non_far_defaults_and_breakdown_rows():
    site_area = 1000.0
    inputs = {
        "land_use_code": "m",
        "area_ratio": {
            "residential": 1.0,
            "retail": 0.5,
            "upper_annex_non_far": 0.5,
            "basement": 1.0,
        },
        "unit_cost": {
            "residential": 2000.0,
            "retail": 2500.0,
            "basement": 1200.0,
        },
    }

    excel = compute_excel_estimate(site_area, inputs)

    upper_annex_area = site_area * inputs["area_ratio"]["upper_annex_non_far"]
    expected_cost = upper_annex_area * 2200.0
    assert excel["direct_cost"]["upper_annex_non_far"] == pytest.approx(expected_cost)
    assert excel["unit_cost_resolved"]["upper_annex_non_far"] == pytest.approx(2200.0)

    cost_rows = {row["key"]: row for row in excel["cost_breakdown_rows"]}
    assert cost_rows["upper_annex_non_far_bua"]["label"] == "Upper annex (non-FAR, +0.5 floor)"
    assert "Added +0.5 floor" in cost_rows["upper_annex_non_far_bua"]["note"]
    assert cost_rows["upper_annex_non_far_cost"]["value"] == pytest.approx(expected_cost)
    assert "SAR/m²" in cost_rows["upper_annex_non_far_cost"]["note"]
