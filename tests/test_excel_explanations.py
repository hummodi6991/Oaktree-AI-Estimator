from app.services.excel_method import build_excel_explanations


def test_build_excel_explanations_uses_area_ratio_label_and_effective_far_sum():
    site_area_m2 = 1000
    inputs = {
        "area_ratio": {
            "residential": 1.5,
            "retail": 0.5,
            "basement": 0.4,
        },
        "unit_cost": {"residential": 1000, "retail": 800, "basement": 500},
    }
    built_area = {
        "residential": inputs["area_ratio"]["residential"] * site_area_m2,
        "retail": inputs["area_ratio"]["retail"] * site_area_m2,
        "basement": inputs["area_ratio"]["basement"] * site_area_m2,
    }
    breakdown = {
        "built_area": built_area,
        "area_ratio": inputs["area_ratio"],
        "far_above_ground": 2.0,
        "far_total_including_basement": 2.4,
        "direct_cost": {
            "residential": built_area["residential"] * inputs["unit_cost"]["residential"],
            "retail": built_area["retail"] * inputs["unit_cost"]["retail"],
            "basement": built_area["basement"] * inputs["unit_cost"]["basement"],
        },
    }

    explanations = build_excel_explanations(site_area_m2, inputs, breakdown)

    residential_note = explanations["residential_bua"]
    assert "area ratio" in residential_note.lower()
    assert "far" not in residential_note

    effective_far_note = explanations["effective_far_above_ground"]
    assert "2.000" in effective_far_note
    assert "excluding basement" in effective_far_note
