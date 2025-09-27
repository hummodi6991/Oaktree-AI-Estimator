from typing import List, Dict, Any
from fpdf import FPDF


def _fmt_money(x: float | None) -> str:
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return str(x)


def build_memo_pdf(
    title: str,
    totals: Dict[str, Any],
    assumptions: List[Dict[str, Any]],
    top_comps: List[Dict[str, Any]],
) -> bytes:
    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.add_page()
    pdf.set_title(title)

    # Header
    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, title, ln=True)

    # Totals
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "Totals (SAR)", ln=True)
    pdf.set_font("Arial", "", 11)
    for k in ["land_value", "hard_costs", "soft_costs", "financing", "revenues", "p50_profit"]:
        pdf.cell(60, 7, k.replace("_", " ").title()+":", border=0)
        pdf.cell(0, 7, _fmt_money(totals.get(k)), ln=True)

    # Assumptions
    pdf.ln(2)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "Key Assumptions", ln=True)
    pdf.set_font("Arial", "", 10)
    for a in (assumptions or [])[:12]:
        unit = f" {a.get('unit')}" if a.get("unit") else ""
        pdf.cell(0, 6, f"- {a.get('key')}: {a.get('value')}{unit} [{a.get('source_type')}]", ln=True)

    # Top comps
    pdf.ln(2)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "Top Comps (abbrev.)", ln=True)
    pdf.set_font("Arial", "", 10)
    for c in (top_comps or [])[:8]:
        line = (
            f"{c.get('id')} | {c.get('date')} | "
            f"{c.get('city')}/{c.get('district') or ''} | "
            f"{_fmt_money(c.get('price_per_m2'))} SAR/m²"
        )
        pdf.cell(0, 6, line, ln=True)

    return bytes(pdf.output(dest="S"))
