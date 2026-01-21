from typing import List, Dict, Any, Iterable

from app.services.excel_method import DEFAULT_Y1_INCOME_EFFECTIVE_FACTOR, _normalize_y1_income_effective_factor

try:  # pragma: no cover - dependency availability handled at runtime
    from fpdf import FPDF
except ModuleNotFoundError:  # pragma: no cover
    FPDF = None  # type: ignore[assignment]


FONT_FAMILY = "Helvetica"
MARGIN_MM = 12
SECTION_SPACING = 4
ROW_HEIGHT = 6
HEADER_ROW_HEIGHT = 7


def _fmt_money(x: float | None) -> str:
    if x is None:
        return "N/A"
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return "N/A"


def _fmt_number(x: float | None) -> str:
    if x is None:
        return "N/A"
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return "N/A"


def _fmt_percent(x: float | None, digits: int = 1) -> str:
    if x is None:
        return "N/A"
    try:
        return f"{float(x) * 100:.{digits}f}%"
    except Exception:
        return "N/A"


def _fmt_decimal(x: float | None, digits: int = 3) -> str:
    if x is None:
        return "N/A"
    try:
        return f"{float(x):,.{digits}f}"
    except Exception:
        return "N/A"


def _is_ascii(value: str) -> bool:
    try:
        value.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


def _strip_non_ascii(text: str) -> str:
    return "".join(ch for ch in text if ord(ch) < 128)


def _pdf_safe_text(value: Any) -> str:
    if value is None:
        return ""
    text = _strip_non_ascii(str(value))
    return text.encode("latin-1", errors="ignore").decode("latin-1")


def _ellipsize(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 3)].rstrip()}..."


def _short_note(note: Any, limit: int = 72) -> str:
    if not note:
        return ""
    text = _strip_non_ascii(str(note))
    if not text:
        return ""
    text = text.split("|")[0].strip()
    text = " ".join(text.split())
    if "=" in text and len(text) > 40:
        text = text.split("=")[0].strip()
    return _ellipsize(text, limit)


def _ensure_space(pdf: "FPDF", height: float) -> None:
    if pdf.get_y() + height > pdf.page_break_trigger:
        pdf.add_page()


def _draw_section_title(pdf: "FPDF", title: str) -> None:
    _ensure_space(pdf, HEADER_ROW_HEIGHT + SECTION_SPACING)
    pdf.set_font(FONT_FAMILY, "B", 12)
    pdf.set_text_color(23, 74, 63)
    pdf.cell(0, HEADER_ROW_HEIGHT, _pdf_safe_text(title), ln=True)
    pdf.set_text_color(0, 0, 0)
    pdf.ln(1)


def _draw_table(
    pdf: "FPDF",
    headers: Iterable[str],
    rows: Iterable[Dict[str, Any]],
    col_widths: List[float],
    aligns: List[str],
    row_height: float = ROW_HEIGHT,
    header_height: float = HEADER_ROW_HEIGHT,
    max_chars: List[int] | None = None,
) -> None:
    headers_list = list(headers)
    pdf.set_font(FONT_FAMILY, "B", 9)
    pdf.set_fill_color(229, 240, 236)
    _ensure_space(pdf, header_height)
    for idx, header in enumerate(headers_list):
        text = _pdf_safe_text(header)
        pdf.cell(col_widths[idx], header_height, text, border=1, align=aligns[idx], fill=True)
    pdf.ln(header_height)

    for row in rows:
        _ensure_space(pdf, row_height)
        font_style = "B" if row.get("bold") else ""
        pdf.set_font(FONT_FAMILY, font_style, 9)
        cells = row.get("cells", [])
        for idx, cell in enumerate(cells):
            cell_text = _pdf_safe_text(cell)
            if max_chars:
                cell_text = _ellipsize(cell_text, max_chars[idx])
            pdf.cell(col_widths[idx], row_height, cell_text, border=1, align=aligns[idx])
        pdf.ln(row_height)


def _resolve_explanations(excel_breakdown: Dict[str, Any]) -> Dict[str, Any]:
    explanations = excel_breakdown.get("explanations_en") or excel_breakdown.get("explanations") or {}
    return explanations if isinstance(explanations, dict) else {}


def _resolve_ascii(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if not _is_ascii(text):
        return ""
    return text


def _format_amount(value: Any, unit: str = "SAR") -> str:
    if unit == "SAR":
        return _fmt_money(value)
    return f"{_fmt_number(value)} {unit}"


def _build_cost_breakdown_rows(
    excel_breakdown: Dict[str, Any],
    cost_breakdown: Dict[str, Any],
    explanations: Dict[str, Any],
) -> List[Dict[str, Any]]:
    far_above_ground = excel_breakdown.get("far_above_ground")
    built_area = excel_breakdown.get("built_area")
    built_area = built_area if isinstance(built_area, dict) else {}

    direct_cost = excel_breakdown.get("direct_cost")
    direct_cost = direct_cost if isinstance(direct_cost, dict) else {}

    construction_direct = cost_breakdown.get("construction_direct_cost")
    if construction_direct is None:
        construction_direct = sum(direct_cost.values()) if direct_cost else None

    rows = []
    if far_above_ground is not None:
        rows.append(
            {
                "cells": [
                    "Effective FAR (above-ground)",
                    _fmt_decimal(far_above_ground, 3),
                    _short_note(explanations.get("far_above_ground")),
                ]
            }
        )
    built_rows = [
        ("Residential BUA", "residential", "residential_bua"),
        ("Retail BUA", "retail", "retail_bua"),
        ("Office BUA", "office", "office_bua"),
        ("Basement BUA", "basement", "basement_bua"),
    ]
    for label, key, explanation_key in built_rows:
        if key not in built_area:
            continue
        amount = built_area.get(key)
        rows.append(
            {
                "cells": [
                    label,
                    _format_amount(amount, "m2"),
                    _short_note(explanations.get(explanation_key)),
                ]
            }
        )

    rows.extend(
        [
            {
                "cells": [
                    "Land cost",
                    _format_amount(cost_breakdown.get("land_cost"), "SAR"),
                    _short_note(explanations.get("land_cost")),
                ]
            },
            {
                "cells": [
                    "Construction (direct)",
                    _format_amount(construction_direct, "SAR"),
                    _short_note(explanations.get("construction_direct")),
                ]
            },
            {
                "cells": [
                    "Fit-out",
                    _format_amount(cost_breakdown.get("fitout_cost"), "SAR"),
                    _short_note(explanations.get("fitout")),
                ]
            },
            {
                "cells": [
                    "Contingency",
                    _format_amount(cost_breakdown.get("contingency_cost"), "SAR"),
                    _short_note(explanations.get("contingency")),
                ]
            },
            {
                "cells": [
                    "Consultants",
                    _format_amount(cost_breakdown.get("consultants_cost"), "SAR"),
                    _short_note(explanations.get("consultants")),
                ]
            },
            {
                "cells": [
                    "Feasibility fee",
                    _format_amount(cost_breakdown.get("feasibility_fee"), "SAR"),
                    _short_note(explanations.get("feasibility_fee")),
                ]
            },
            {
                "cells": [
                    "Transaction costs",
                    _format_amount(cost_breakdown.get("transaction_cost"), "SAR"),
                    _short_note(explanations.get("transaction_cost")),
                ]
            },
            {
                "cells": [
                    "Total capex",
                    _format_amount(cost_breakdown.get("grand_total_capex"), "SAR"),
                    _short_note(explanations.get("grand_total_capex")),
                ],
                "bold": True,
            },
        ]
    )
    return rows


def _build_revenue_breakdown_rows(
    excel_breakdown: Dict[str, Any],
    cost_breakdown: Dict[str, Any],
) -> List[Dict[str, Any]]:
    income_components = excel_breakdown.get("y1_income_components")
    income_components = income_components if isinstance(income_components, dict) else {}

    rows: List[Dict[str, Any]] = []
    for key, amount in income_components.items():
        label = str(key).replace("_", " ")
        rows.append(
            {
                "cells": [
                    label,
                    _format_amount(amount, "SAR"),
                    "NLA * rent rate",
                ]
            }
        )

    y1_income = cost_breakdown.get("y1_income") or excel_breakdown.get("y1_income")
    y1_income_effective = cost_breakdown.get("y1_income_effective") or excel_breakdown.get("y1_income_effective")
    y1_income_effective_factor = cost_breakdown.get("y1_income_effective_factor") or excel_breakdown.get(
        "y1_income_effective_factor"
    )
    if y1_income_effective is None and y1_income is not None:
        factor = _normalize_y1_income_effective_factor(
            y1_income_effective_factor
            if y1_income_effective_factor is not None
            else DEFAULT_Y1_INCOME_EFFECTIVE_FACTOR
        )
        y1_income_effective = float(y1_income) * factor
        y1_income_effective_factor = factor

    opex_pct = cost_breakdown.get("opex_pct") or excel_breakdown.get("opex_pct")
    opex_cost = cost_breakdown.get("opex_cost") or excel_breakdown.get("opex_cost")
    if opex_cost is None and y1_income_effective is not None and opex_pct is not None:
        opex_cost = float(y1_income_effective) * float(opex_pct)

    y1_noi = cost_breakdown.get("y1_noi") or excel_breakdown.get("y1_noi")
    roi = cost_breakdown.get("roi") or excel_breakdown.get("roi")

    rows.extend(
        [
            {
                "cells": [
                    "Year 1 income",
                    _format_amount(y1_income, "SAR"),
                    "Sum of income components",
                ]
            },
            {
                "cells": [
                    "Year 1 income (effective)",
                    _format_amount(y1_income_effective, "SAR"),
                    f"{_fmt_percent(y1_income_effective_factor, 0)} effective",
                ]
            },
            {
                "cells": [
                    "OPEX",
                    _format_amount(opex_cost, "SAR"),
                    f"{_fmt_percent(opex_pct, 0)} of effective income",
                ]
            },
            {
                "cells": [
                    "Year 1 NOI",
                    _format_amount(y1_noi, "SAR"),
                    "Effective income - OPEX",
                ]
            },
            {
                "cells": [
                    "Unlevered ROI",
                    _fmt_percent(roi, 1),
                    "NOI / total capex",
                ],
                "bold": True,
            },
        ]
    )
    return rows


def _build_assumption_rows(assumptions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in assumptions:
        if not isinstance(item, dict):
            continue
        key = _resolve_ascii(item.get("key") or "")
        if not key:
            continue
        if key.lower() == "far":
            key = "FAR (model prior)"
        value = item.get("value")
        unit = item.get("unit") or ""
        source_type = _resolve_ascii(item.get("source_type") or "")
        if isinstance(value, (int, float)):
            value_text = _fmt_number(value)
        else:
            value_text = _resolve_ascii(value) or "N/A"
        if unit:
            unit_text = _resolve_ascii(unit)
            if unit_text:
                value_text = f"{value_text} {unit_text}"
        rows.append(
            {
                "cells": [
                    key,
                    value_text,
                    source_type or "N/A",
                ]
            }
        )
    return rows


def _build_appendix_rows(
    explanations: Dict[str, Any],
    excel_breakdown: Dict[str, Any],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    label_map = {
        "residential_bua": "Residential BUA",
        "retail_bua": "Retail BUA",
        "office_bua": "Office BUA",
        "basement_bua": "Basement BUA",
        "land_cost": "Land cost",
        "construction_direct": "Construction (direct)",
        "fitout": "Fit-out",
        "contingency": "Contingency",
        "consultants": "Consultants",
        "feasibility_fee": "Feasibility fee",
        "transaction_cost": "Transaction costs",
        "grand_total_capex": "Total capex",
        "y1_income": "Year 1 income",
        "y1_income_effective": "Year 1 income (effective)",
        "opex": "OPEX",
        "y1_noi": "Year 1 NOI",
    }
    for key, label in label_map.items():
        note = explanations.get(key)
        note_text = _resolve_ascii(note)
        if not note_text:
            continue
        rows.append(
            {
                "cells": [label, _ellipsize(note_text, 160)],
            }
        )

    income_components = excel_breakdown.get("y1_income_components")
    if isinstance(income_components, dict):
        for key in income_components.keys():
            label = f"Income: {str(key).replace('_', ' ')}"
            rows.append(
                {
                    "cells": [label, "NLA * rent rate"],
                }
            )
    return rows


def _build_comps_rows(top_comps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for comp in top_comps:
        if not isinstance(comp, dict):
            continue
        comp_id = _resolve_ascii(comp.get("id") or "")
        comp_date = _resolve_ascii(comp.get("date") or "")
        city = _resolve_ascii(comp.get("city") or "")
        district = _resolve_ascii(comp.get("district") or "")
        location = ""
        if city and district:
            location = f"{city}/{district}"
        elif city:
            location = city
        elif district:
            location = district
        price = _fmt_money(comp.get("price_per_m2"))
        if not any([comp_id, comp_date, location, price]):
            continue
        rows.append(
            {
                "cells": [
                    comp_id or "N/A",
                    comp_date or "N/A",
                    location or "",
                    f"{price} SAR/m2",
                ]
            }
        )
    return rows


def _extract_notes(notes: Dict[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(notes, dict):
        return {}
    if isinstance(notes.get("notes"), dict):
        return notes.get("notes")
    return notes


def build_memo_pdf(
    title: str,
    totals: Dict[str, Any],
    assumptions: List[Dict[str, Any]],
    top_comps: List[Dict[str, Any]],
    excel_breakdown: Dict[str, Any] | None = None,
    cost_breakdown: Dict[str, Any] | None = None,
    notes: Dict[str, Any] | None = None,
) -> bytes:
    if FPDF is None:
        raise RuntimeError("fpdf library is not installed")
    totals = totals if isinstance(totals, dict) else {}
    assumptions = assumptions if isinstance(assumptions, list) else []
    top_comps = top_comps if isinstance(top_comps, list) else []
    excel_breakdown = excel_breakdown if isinstance(excel_breakdown, dict) else {}
    cost_breakdown = cost_breakdown if isinstance(cost_breakdown, dict) else {}
    notes = _extract_notes(notes)

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_margins(MARGIN_MM, MARGIN_MM, MARGIN_MM)
    pdf.set_auto_page_break(auto=True, margin=MARGIN_MM)
    pdf.set_compression(False)
    pdf.add_page()
    pdf.set_title(title)

    pdf.set_font(FONT_FAMILY, "B", 16)
    pdf.cell(0, 10, _pdf_safe_text(title), ln=True)

    _draw_section_title(pdf, "Totals (SAR)")
    metrics = [
        ("Land value", _fmt_money(cost_breakdown.get("land_cost") or totals.get("land_value"))),
        ("Total capex", _fmt_money(cost_breakdown.get("grand_total_capex"))),
        ("Year 1 income", _fmt_money(cost_breakdown.get("y1_income") or totals.get("revenues"))),
        ("Year 1 NOI", _fmt_money(cost_breakdown.get("y1_noi"))),
        ("Unlevered ROI", _fmt_percent(cost_breakdown.get("roi"), 1)),
        ("P50 profit", _fmt_money(totals.get("p50_profit"))),
    ]
    metric_width = (pdf.w - pdf.l_margin - pdf.r_margin) / len(metrics)
    pdf.set_font(FONT_FAMILY, "", 8)
    pdf.set_fill_color(229, 240, 236)
    for label, _value in metrics:
        pdf.cell(metric_width, 5, _pdf_safe_text(label), border=1, align="C", fill=True)
    pdf.ln(5)
    pdf.set_font(FONT_FAMILY, "B", 10)
    for _label, value in metrics:
        pdf.cell(metric_width, 7, _pdf_safe_text(value), border=1, align="C")
    pdf.ln(10)

    explanations = _resolve_explanations(excel_breakdown)

    _draw_section_title(pdf, "Cost breakdown")
    cost_rows = _build_cost_breakdown_rows(excel_breakdown, cost_breakdown, explanations)
    cost_headers = ["Item", "Amount", "How calculated"]
    table_width = pdf.w - pdf.l_margin - pdf.r_margin
    cost_col_widths = [55, 35, table_width - 90]
    _draw_table(
        pdf,
        cost_headers,
        cost_rows,
        cost_col_widths,
        ["L", "R", "L"],
        max_chars=[28, 16, 68],
    )
    pdf.ln(SECTION_SPACING)

    _draw_section_title(pdf, "Revenue breakdown")
    revenue_rows = _build_revenue_breakdown_rows(excel_breakdown, cost_breakdown)
    revenue_col_widths = [55, 35, table_width - 90]
    _draw_table(
        pdf,
        cost_headers,
        revenue_rows,
        revenue_col_widths,
        ["L", "R", "L"],
        max_chars=[28, 16, 68],
    )

    parking_summary = []
    parking_notes = notes.get("parking") if isinstance(notes, dict) else {}
    if isinstance(parking_notes, dict):
        required = totals.get("parking_required_spaces")
        if required is None:
            required = parking_notes.get("required_spaces_final") or parking_notes.get("required_spaces")
        provided = totals.get("parking_provided_spaces")
        if provided is None:
            provided = parking_notes.get("provided_spaces_final") or parking_notes.get("provided_spaces_before")
        deficit = totals.get("parking_deficit_spaces")
        if deficit is None:
            deficit = parking_notes.get("deficit_spaces_final") or parking_notes.get("deficit_spaces_before")
        compliant = totals.get("parking_compliant")
        if compliant is None:
            compliant = parking_notes.get("compliant")
        parking_summary = [
            ("Required spaces", _fmt_number(required)),
            ("Provided spaces", _fmt_number(provided)),
            ("Deficit", _fmt_number(deficit)),
            ("Compliant", "Yes" if compliant is True else "No" if compliant is False else "N/A"),
        ]

    if any(value for _label, value in parking_summary if value != "N/A"):
        pdf.ln(SECTION_SPACING)
        _draw_section_title(pdf, "Parking summary")
        box_width = pdf.w - pdf.l_margin - pdf.r_margin
        pdf.set_fill_color(245, 248, 247)
        pdf.set_draw_color(200, 220, 214)
        pdf.rect(pdf.l_margin, pdf.get_y(), box_width, ROW_HEIGHT * len(parking_summary) + 4)
        pdf.set_xy(pdf.l_margin + 2, pdf.get_y() + 2)
        pdf.set_font(FONT_FAMILY, "", 9)
        for label, value in parking_summary:
            pdf.cell(box_width * 0.6, ROW_HEIGHT, _pdf_safe_text(label), align="L")
            pdf.cell(box_width * 0.35, ROW_HEIGHT, _pdf_safe_text(value), align="R", ln=True)

    summary_text = _resolve_ascii(notes.get("summary_en") or notes.get("summary") or "")
    if summary_text:
        pdf.ln(SECTION_SPACING)
        _draw_section_title(pdf, "Executive summary")
        pdf.set_font(FONT_FAMILY, "", 9)
        pdf.multi_cell(0, 5, _pdf_safe_text(summary_text))

    pdf.add_page()
    _draw_section_title(pdf, "Key assumptions")
    assumption_rows = _build_assumption_rows(assumptions)
    if assumption_rows:
        assumption_headers = ["Assumption", "Value", "Source"]
        assumption_col_widths = [60, 60, table_width - 120]
        _draw_table(
            pdf,
            assumption_headers,
            assumption_rows,
            assumption_col_widths,
            ["L", "R", "L"],
            max_chars=[30, 26, 26],
        )
    else:
        pdf.set_font(FONT_FAMILY, "", 9)
        pdf.cell(0, ROW_HEIGHT, "No assumptions available.", ln=True)

    appendix_rows = _build_appendix_rows(explanations, excel_breakdown)
    if appendix_rows:
        pdf.ln(SECTION_SPACING)
        _draw_section_title(pdf, "Appendix: calculation trace")
        appendix_headers = ["Item", "Detail"]
        appendix_col_widths = [60, table_width - 60]
        _draw_table(
            pdf,
            appendix_headers,
            appendix_rows,
            appendix_col_widths,
            ["L", "L"],
            max_chars=[30, 120],
        )

    comps_rows = _build_comps_rows(top_comps)
    if comps_rows:
        pdf.ln(SECTION_SPACING)
        _draw_section_title(pdf, "Appendix: top comps")
        comps_headers = ["ID", "Date", "Location", "Price (SAR/m2)"]
        comps_col_widths = [30, 30, 60, table_width - 120]
        _draw_table(
            pdf,
            comps_headers,
            comps_rows,
            comps_col_widths,
            ["L", "L", "L", "R"],
            max_chars=[18, 16, 32, 20],
        )

    return bytes(pdf.output(dest="S"))
