import pytest

from app.services.pdf import build_memo_pdf

pytest.importorskip("fpdf")


def test_pdf_labels_match_ui_naming():
    cost_breakdown = {
        "land_cost": 2_000_000,
        "grand_total_capex": 10_000_000,
        "y1_income": 1_200_000,
        "y1_income_effective": 1_000_000,
        "opex_pct": 0.2,
        "opex_cost": 200_000,
        "y1_noi": 800_000,
        "roi": 0.08,
    }
    pdf_bytes = build_memo_pdf(
        title="Label Check",
        totals={},
        assumptions=[],
        top_comps=[],
        excel_breakdown={},
        cost_breakdown=cost_breakdown,
    )

    pdf_text = pdf_bytes.decode("latin-1")
    assert "Annual net revenue" in pdf_text
    assert "Annual net income" in pdf_text
    assert "Annual NOI" in pdf_text
    assert "Year 1 income" not in pdf_text
    assert "Year 1 NOI" not in pdf_text
