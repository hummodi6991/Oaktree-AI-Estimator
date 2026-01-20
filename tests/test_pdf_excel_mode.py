import pytest

from app.api.estimates import _persist_inmemory, get_estimate
from app.services.pdf import build_memo_pdf

pytest.importorskip("fpdf")


def test_build_memo_pdf_excel_mode_minimal():
    estimate_id = "excel-minimal"
    totals = {"land_value": None, "hard_costs": 125000}
    notes = {
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
    )

    assert pdf_bytes.startswith(b"%PDF")
    assert len(pdf_bytes) > 0
