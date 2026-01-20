import pytest

from app.api.estimates import _persist_inmemory, get_estimate
from app.services.pdf import build_memo_pdf

pytest.importorskip("fpdf")


def test_build_memo_pdf_excel_mode_minimal():
    estimate_id = "excel-minimal"
    totals = {"land_value": None, "hard_costs": 125000}
    notes = {
        "cost_breakdown": {
            "land_cost": 0.0,
            "construction_direct_cost": 0.0,
            "fitout_cost": 0.0,
            "contingency_cost": 0.0,
            "consultants_cost": 0.0,
            "feasibility_fee": 0.0,
            "transaction_cost": 0.0,
            "grand_total_capex": 0.0,
            "y1_income": 0.0,
            "y1_income_effective": 0.0,
            "y1_income_effective_factor": 0.85,
            "opex_pct": 0.05,
            "opex_cost": 0.0,
            "y1_noi": 0.0,
            "roi": 0.0,
        },
        "excel_breakdown": {
            "built_area": {"residential": 1500},
            "direct_cost": None,
            "y1_income_effective_factor": None,
        }
    }
    assumptions = [
        {"key": "cap_rate", "value": None, "unit": None, "source_type": None},
        {"value": 10},
    ]

    _persist_inmemory(estimate_id, "build_to_sell", totals, notes, assumptions, [])
    base = get_estimate(estimate_id, db=object())

    pdf_bytes = build_memo_pdf(
        title=f"Estimate {estimate_id}",
        totals=base["totals"],
        assumptions=base.get("assumptions", []),
        top_comps=[],
        excel_breakdown=base.get("notes", {}).get("excel_breakdown"),
        cost_breakdown=base.get("notes", {}).get("cost_breakdown"),
        notes=base.get("notes", {}),
    )

    assert pdf_bytes.startswith(b"%PDF")
    assert len(pdf_bytes) > 0


def test_build_memo_pdf_handles_arabic_text():
    estimate_id = "excel-arabic"
    totals = {"land_value": 500000, "hard_costs": 125000}
    notes = {
        "cost_breakdown": {
            "land_cost": 500000.0,
            "construction_direct_cost": 0.0,
            "fitout_cost": 0.0,
            "contingency_cost": 0.0,
            "consultants_cost": 0.0,
            "feasibility_fee": 0.0,
            "transaction_cost": 0.0,
            "grand_total_capex": 500000.0,
            "y1_income": 0.0,
            "y1_income_effective": 0.0,
            "y1_income_effective_factor": 0.85,
            "opex_pct": 0.05,
            "opex_cost": 0.0,
            "y1_noi": 0.0,
            "roi": 0.0,
        },
        "excel_breakdown": {
            "built_area": {"residential": 1500},
            "direct_cost": None,
            "y1_income_effective_factor": None,
            "explanations": {"land_cost": "حي العليا"},
        }
    }
    assumptions = [
        {"key": "zoning", "value": "مختلط", "unit": None, "source_type": "manual"},
    ]

    _persist_inmemory(estimate_id, "build_to_sell", totals, notes, assumptions, [])
    base = get_estimate(estimate_id, db=object())

    pdf_bytes = build_memo_pdf(
        title=f"Estimate {estimate_id}",
        totals=base["totals"],
        assumptions=base.get("assumptions", []),
        top_comps=[],
        excel_breakdown=base.get("notes", {}).get("excel_breakdown"),
        cost_breakdown=base.get("notes", {}).get("cost_breakdown"),
        notes=base.get("notes", {}),
    )

    assert pdf_bytes.startswith(b"%PDF")
    assert len(pdf_bytes) > 0
