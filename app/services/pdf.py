from typing import List, Dict, Any

from app.services.excel_method import DEFAULT_Y1_INCOME_EFFECTIVE_FACTOR, _normalize_y1_income_effective_factor

try:  # pragma: no cover - dependency availability handled at runtime
    from fpdf import FPDF
except ModuleNotFoundError:  # pragma: no cover
    FPDF = None  # type: ignore[assignment]


def _fmt_money(x: float | None) -> str:
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return str(x)


def _fmt_amount(x: float | None) -> str:
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return str(x)


def build_memo_pdf(
    title: str,
    totals: Dict[str, Any],
    assumptions: List[Dict[str, Any]],
    top_comps: List[Dict[str, Any]],
    excel_breakdown: Dict[str, Any] | None = None,
) -> bytes:
    if FPDF is None:
        raise RuntimeError("fpdf library is not installed")
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

    if excel_breakdown:
        pdf.ln(2)
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 8, "Cost breakdown", ln=True)
        pdf.set_font("Arial", "", 10)

        explanations = excel_breakdown.get("explanations") or {}
        y1_eff_factor = _normalize_y1_income_effective_factor(
            excel_breakdown.get("y1_income_effective_factor", DEFAULT_Y1_INCOME_EFFECTIVE_FACTOR)
        )
        y1_eff_label = f"Year 1 net income ({y1_eff_factor*100:.0f}% effective)"
        y1_income_effective = excel_breakdown.get("y1_income_effective") or 0
        opex_pct = excel_breakdown.get("opex_pct", 0.05) or 0.0
        opex_amount = excel_breakdown.get("opex_cost", y1_income_effective * float(opex_pct or 0.0)) or 0.0
        opex_note = f"{float(opex_pct):.0%} of effective income"
        y1_noi = excel_breakdown.get("y1_noi")
        direct_cost_total = sum((excel_breakdown.get("direct_cost") or {}).values())
        built_area = excel_breakdown.get("built_area") or {}

        def _format_amount(value: Any, unit: str | None = "SAR") -> str:
            if value is None:
                return ""
            formatter = _fmt_amount if unit and unit != "SAR" else _fmt_money
            unit_suffix = f" {unit}" if unit else ""
            return f"{formatter(value)}{unit_suffix}"

        rows = [
            (
                "Residential BUA",
                built_area.get("residential"),
                explanations.get("residential_bua"),
                "m²",
            ),
        ]

        # Mixed-use (template "m") includes retail/office components.
        # Only show these lines when the component exists to avoid empty rows for resi templates.
        if "retail" in built_area:
            rows.append(
                (
                    "Retail BUA",
                    built_area.get("retail"),
                    explanations.get("retail_bua"),
                    "m²",
                )
            )
        if "office" in built_area:
            rows.append(
                (
                    "Office BUA",
                    built_area.get("office"),
                    explanations.get("office_bua"),
                    "m²",
                )
            )

        rows.extend(
            [
                (
                    "Basement BUA",
                    built_area.get("basement"),
                    explanations.get("basement_bua"),
                    "m²",
                ),
                ("Land cost", excel_breakdown.get("land_cost"), explanations.get("land_cost")),
                (
                    "Construction",
                    direct_cost_total,
                    explanations.get("construction_direct"),
                ),
                ("Fit-out", excel_breakdown.get("fitout_cost"), explanations.get("fitout")),
                ("Contingency", excel_breakdown.get("contingency_cost"), explanations.get("contingency")),
                ("Consultants", excel_breakdown.get("consultants_cost"), explanations.get("consultants")),
                (
                    "Transaction costs",
                    excel_breakdown.get("transaction_cost"),
                    explanations.get("transaction_cost"),
                ),
                ("Year 1 net income", excel_breakdown.get("y1_income"), explanations.get("y1_income")),
                (
                    y1_eff_label,
                    y1_income_effective,
                    explanations.get("y1_income_effective"),
                ),
                (
                    "OPEX",
                    opex_amount,
                    opex_note,
                ),
                (
                    "Year 1 NOI",
                    y1_noi,
                    "Effective income − OPEX",
                ),
            ]
        )

        for label, amount, note, *unit in rows:
            pdf.cell(60, 6, f"{label}:", ln=False)
            pdf.cell(0, 6, _format_amount(amount, unit[0] if unit else "SAR"), ln=True)
            if note:
                pdf.set_font("Arial", "", 8)
                pdf.multi_cell(0, 5, f"    {note}")
                pdf.set_font("Arial", "", 10)

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
