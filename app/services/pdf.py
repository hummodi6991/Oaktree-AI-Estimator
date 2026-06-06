import os
from typing import List, Dict, Any, Iterable

from app.services.excel_method import DEFAULT_Y1_INCOME_EFFECTIVE_FACTOR, _normalize_y1_income_effective_factor
from app.services.estimator_i18n import (
    t as _label,
    source_type_label,
    income_component_label,
    assumption_key_label,
)

try:  # pragma: no cover - dependency availability handled at runtime
    from fpdf import FPDF
except ModuleNotFoundError:  # pragma: no cover
    FPDF = None  # type: ignore[assignment]

try:  # pragma: no cover - only needed on the Arabic render path
    import arabic_reshaper

    try:
        from bidi.algorithm import get_display as _bidi_get_display
    except ImportError:  # python-bidi >= 0.5 exposes get_display at top level
        from bidi import get_display as _bidi_get_display
except ModuleNotFoundError:  # pragma: no cover
    arabic_reshaper = None  # type: ignore[assignment]
    _bidi_get_display = None  # type: ignore[assignment]


FONT_FAMILY = "Helvetica"
# Arabic faces are vendored (committed) under assets/fonts so the server can
# embed them at render time without any build-time download.
AR_FONT_FAMILY = "NotoNaskh"
_FONT_DIR = os.path.join(os.path.dirname(__file__), "assets", "fonts")
_AR_FONT_REGULAR = os.path.join(_FONT_DIR, "NotoNaskhArabic-Regular.ttf")
_AR_FONT_BOLD = os.path.join(_FONT_DIR, "NotoNaskhArabic-Bold.ttf")
MARGIN_MM = 12
SECTION_SPACING = 4
ROW_HEIGHT = 6
HEADER_ROW_HEIGHT = 7


def _shape_ar(text: str) -> str:
    """Reshape (contextual joining) + apply the BiDi algorithm so Arabic
    renders correctly in fpdf (which has no native shaping/BiDi).

    Must be applied to AR text *immediately before drawing* — after any
    composition/ellipsizing — because BiDi reordering depends on the full,
    final string. EN text is never shaped.
    """
    if not text:
        return text
    if arabic_reshaper is None or _bidi_get_display is None:  # pragma: no cover
        return text
    return _bidi_get_display(arabic_reshaper.reshape(text))


def _register_ar_fonts(pdf: "FPDF") -> None:
    """Register the vendored Naskh faces once for the AR render path."""
    pdf.add_font(AR_FONT_FAMILY, "", _AR_FONT_REGULAR, uni=True)
    pdf.add_font(AR_FONT_FAMILY, "B", _AR_FONT_BOLD, uni=True)


def _doc_lang(pdf: "FPDF") -> str:
    """Document language stamped on the pdf in ``build_memo_pdf`` (en/ar)."""
    return getattr(pdf, "_oak_lang", "en")


def _doc_family(pdf: "FPDF") -> str:
    """Lang-appropriate font family: Helvetica for EN, NotoNaskh for AR."""
    return getattr(pdf, "_oak_font_family", FONT_FAMILY)


def _flip_align(align: str) -> str:
    """Mirror a horizontal alignment for the RTL (AR) layout."""
    return {"L": "R", "R": "L"}.get(align, align)


# Eastern Arabic numerals (٠١٢٣٤٥٦٧٨٩). The AR PDF renders every digit with
# these; grouping (",") and decimal (".") separators are kept for legibility.
_AR_DIGITS = str.maketrans("0123456789", "٠١٢٣٤٥٦٧٨٩")

# AR unit tokens. Crucially the squared-metre superscript renders as a baseline
# ``٢`` (never U+00B2 — the embedded Naskh face lacks the superscript glyph).
_AR_UNIT_MAP = {
    "SAR": "ر.س",
    "m2": "م٢",
    "m²": "م٢",
    "SAR/m2": "ر.س/م٢",
    "SAR/m²": "ر.س/م٢",
    "SAR/m2/yr": "ر.س/م٢/سنة",
    "SAR/m2/mo": "ر.س/م٢/شهر",
    "%": "٪",
}


def _ar_digits(text: str) -> str:
    """Map ASCII digits to Eastern Arabic numerals (separators untouched)."""
    return text.translate(_AR_DIGITS)


def _localize_unit(unit: Any, lang: str = "en") -> str:
    """Localize an inline unit token for the AR path; EN passes through.

    Never emits U+00B2: a known token resolves via the map (baseline ``٢``), and
    the general fallback rewrites ``SAR``/``m²``/``m2`` and replaces any stray
    superscript-two with a baseline ``٢`` before converting digits.
    """
    if unit is None:
        return ""
    text = str(unit)
    if lang != "ar":
        return text
    if text in _AR_UNIT_MAP:
        return _AR_UNIT_MAP[text]
    text = text.replace("SAR", "ر.س").replace("m²", "م٢").replace("m2", "م٢")
    text = text.replace("²", "٢").replace("/yr", "/سنة").replace("/mo", "/شهر")
    return _ar_digits(text)


def _fmt_money(x: float | None, lang: str = "en") -> str:
    if x is None:
        return _label("na", lang)
    try:
        out = f"{float(x):,.0f}"
        return _ar_digits(out) if lang == "ar" else out
    except Exception:
        return _label("na", lang)


def _fmt_number(x: float | None, lang: str = "en") -> str:
    if x is None:
        return _label("na", lang)
    try:
        out = f"{float(x):,.0f}"
        return _ar_digits(out) if lang == "ar" else out
    except Exception:
        return _label("na", lang)


def _fmt_percent(x: float | None, digits: int = 1, lang: str = "en") -> str:
    if x is None:
        return _label("na", lang)
    try:
        out = f"{float(x) * 100:.{digits}f}%"
        if lang == "ar":
            return _ar_digits(out).replace("%", "٪")
        return out
    except Exception:
        return _label("na", lang)


def _fmt_decimal(x: float | None, digits: int = 3, lang: str = "en") -> str:
    if x is None:
        return _label("na", lang)
    try:
        out = f"{float(x):,.{digits}f}"
        return _ar_digits(out) if lang == "ar" else out
    except Exception:
        return _label("na", lang)


def _is_ascii(value: str) -> bool:
    try:
        value.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


def _strip_non_ascii(text: str) -> str:
    return "".join(ch for ch in text if ord(ch) < 128)


def _pdf_safe_text(value: Any, lang: str = "en") -> str:
    if value is None:
        return ""
    if lang == "ar":
        # AR path: keep Arabic + Latin/digits, shape immediately before drawing.
        return _shape_ar(str(value))
    text = _strip_non_ascii(str(value))
    return text.encode("latin-1", errors="ignore").decode("latin-1")


def _ellipsize(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 3)].rstrip()}..."


def _short_note(note: Any, limit: int = 72, lang: str = "en") -> str:
    if not note:
        return ""
    # AR keeps Arabic through (shaping is applied later, at draw time); EN keeps
    # the exact ASCII-strip behavior.
    text = str(note) if lang == "ar" else _strip_non_ascii(str(note))
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
    lang = _doc_lang(pdf)
    _ensure_space(pdf, HEADER_ROW_HEIGHT + SECTION_SPACING)
    pdf.set_font(_doc_family(pdf), "B", 12)
    pdf.set_text_color(23, 74, 63)
    if lang == "ar":
        pdf.cell(0, HEADER_ROW_HEIGHT, _pdf_safe_text(title, lang), ln=True, align="R")
    else:
        pdf.cell(0, HEADER_ROW_HEIGHT, _pdf_safe_text(title), ln=True)
    pdf.set_text_color(0, 0, 0)
    pdf.ln(1)


def _draw_rtl_paragraph(pdf: "FPDF", text: str, line_height: float) -> None:
    """Render an Arabic narrative as right-aligned, per-line-shaped lines.

    fpdf's ``multi_cell`` wraps in logical order and cannot BiDi-reorder, so we
    greedily wrap the logical (unshaped) text by rendered width, shape each line
    independently, then draw it right-aligned. Minimum-viable RTL (no
    justification / right-origin rework).
    """
    width = pdf.w - pdf.l_margin - pdf.r_margin
    words = str(text).split()

    def _flush(line_words: List[str]) -> None:
        if not line_words:
            return
        pdf.cell(width, line_height, _shape_ar(" ".join(line_words)), align="R", ln=True)

    line: List[str] = []
    for word in words:
        candidate = line + [word]
        if line and pdf.get_string_width(_shape_ar(" ".join(candidate))) > width:
            _flush(line)
            line = [word]
        else:
            line = candidate
    _flush(line)


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
    lang = _doc_lang(pdf)
    family = _doc_family(pdf)
    headers_list = list(headers)
    col_widths = list(col_widths)
    aligns = list(aligns)
    max_chars_list = list(max_chars) if max_chars else None
    table_font_size = 9
    if lang == "ar":
        # Arabic labels run longer than the EN labels these column widths were
        # tuned for, so the standard label set was truncating mid-word. Drop the
        # AR table font one step, widen the label column (logical col 0) by
        # borrowing from the widest trailing column, and raise its char budget so
        # the standard labels fit cleanly. Done on logical-order lists, before the
        # RTL mirroring below.
        table_font_size = 8
        if len(col_widths) >= 2:
            label_min_w = 80.0
            if col_widths[0] < label_min_w:
                donor = max(range(1, len(col_widths)), key=lambda i: col_widths[i])
                extra = label_min_w - col_widths[0]
                if col_widths[donor] - extra >= 20.0:
                    col_widths[0] = label_min_w
                    col_widths[donor] -= extra
            if max_chars_list:
                max_chars_list[0] = max(max_chars_list[0], 50)
        # Minimum-viable RTL: mirror column order + widths and swap L/R cell
        # alignment so the label column sits on the right. Per-cell BiDi
        # handles intra-cell ordering.
        headers_list.reverse()
        col_widths.reverse()
        aligns = [_flip_align(a) for a in reversed(aligns)]
        if max_chars_list:
            max_chars_list.reverse()

    pdf.set_font(family, "B", table_font_size)
    pdf.set_fill_color(229, 240, 236)
    _ensure_space(pdf, header_height)
    for idx, header in enumerate(headers_list):
        text = _pdf_safe_text(header, lang)
        pdf.cell(col_widths[idx], header_height, text, border=1, align=aligns[idx], fill=True)
    pdf.ln(header_height)

    for row in rows:
        _ensure_space(pdf, row_height)
        font_style = "B" if row.get("bold") else ""
        pdf.set_font(family, font_style, table_font_size)
        cells = list(row.get("cells", []))
        if lang == "ar":
            cells = list(reversed(cells))
        for idx, cell in enumerate(cells):
            if lang == "ar":
                # Ellipsize the raw (logical) string, then shape — shaping after
                # truncation keeps the BiDi/joining correct.
                raw = "" if cell is None else str(cell)
                if max_chars_list:
                    raw = _ellipsize(raw, max_chars_list[idx])
                cell_text = _pdf_safe_text(raw, lang)
            else:
                cell_text = _pdf_safe_text(cell)
                if max_chars_list:
                    cell_text = _ellipsize(cell_text, max_chars_list[idx])
            pdf.cell(col_widths[idx], row_height, cell_text, border=1, align=aligns[idx])
        pdf.ln(row_height)


def _resolve_explanations(excel_breakdown: Dict[str, Any], lang: str = "en") -> Dict[str, Any]:
    if lang == "ar":
        explanations = (
            excel_breakdown.get("explanations_ar")
            or excel_breakdown.get("explanations_en")
            or excel_breakdown.get("explanations")
            or {}
        )
    else:
        explanations = excel_breakdown.get("explanations_en") or excel_breakdown.get("explanations") or {}
    return explanations if isinstance(explanations, dict) else {}


def _resolve_ascii(value: Any, lang: str = "en") -> str:
    if value is None:
        return ""
    text = str(value)
    if lang == "ar":
        # AR path passes non-ASCII (Arabic) through; shaping happens at draw time.
        return text
    if not _is_ascii(text):
        return ""
    return text


def _format_amount(value: Any, unit: str = "SAR", lang: str = "en") -> str:
    if unit == "SAR":
        return _fmt_money(value, lang)
    return f"{_fmt_number(value, lang)} {_localize_unit(unit, lang)}"


def _build_cost_breakdown_rows(
    excel_breakdown: Dict[str, Any],
    cost_breakdown: Dict[str, Any],
    explanations: Dict[str, Any],
    lang: str = "en",
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
                    _label("effective_far_above_ground", lang),
                    _fmt_decimal(far_above_ground, 3, lang),
                    _short_note(explanations.get("far_above_ground"), lang=lang),
                ]
            }
        )
    built_rows = [
        ("residential_bua", "residential", "residential_bua"),
        ("retail_bua", "retail", "retail_bua"),
        ("office_bua", "office", "office_bua"),
        ("basement_bua", "basement", "basement_bua"),
        ("upper_annex_non_far_bua", "upper_annex_non_far", "upper_annex_non_far_bua"),
    ]
    for label_token, key, explanation_key in built_rows:
        if key not in built_area:
            continue
        amount = built_area.get(key)
        if key == "upper_annex_non_far" and (amount is None or float(amount or 0.0) <= 0):
            continue
        rows.append(
            {
                "cells": [
                    _label(label_token, lang),
                    _format_amount(amount, "m2", lang),
                    _short_note(explanations.get(explanation_key), lang=lang),
                ]
            }
        )

    rows.extend(
        [
            {
                "cells": [
                    _label("land_cost", lang),
                    _format_amount(cost_breakdown.get("land_cost"), "SAR", lang),
                    _short_note(explanations.get("land_cost"), lang=lang),
                ]
            },
            {
                "cells": [
                    _label("construction_direct", lang),
                    _format_amount(construction_direct, "SAR", lang),
                    _short_note(explanations.get("construction_direct"), lang=lang),
                ]
            },
        ]
    )
    upper_annex_area = built_area.get("upper_annex_non_far")
    if upper_annex_area is not None and float(upper_annex_area or 0.0) > 0:
        rows.append(
            {
                "cells": [
                    _label("upper_annex_non_far_cost", lang),
                    _format_amount(direct_cost.get("upper_annex_non_far"), "SAR", lang),
                    _short_note(explanations.get("upper_annex_non_far_cost"), lang=lang),
                ]
            }
        )
    rows.extend(
        [
            {
                "cells": [
                    _label("fitout", lang),
                    _format_amount(cost_breakdown.get("fitout_cost"), "SAR", lang),
                    _short_note(explanations.get("fitout"), lang=lang),
                ]
            },
            {
                "cells": [
                    _label("contingency", lang),
                    _format_amount(cost_breakdown.get("contingency_cost"), "SAR", lang),
                    _short_note(explanations.get("contingency"), lang=lang),
                ]
            },
            {
                "cells": [
                    _label("consultants", lang),
                    _format_amount(cost_breakdown.get("consultants_cost"), "SAR", lang),
                    _short_note(explanations.get("consultants"), lang=lang),
                ]
            },
            {
                "cells": [
                    _label("feasibility_fee", lang),
                    _format_amount(cost_breakdown.get("feasibility_fee"), "SAR", lang),
                    _short_note(explanations.get("feasibility_fee"), lang=lang),
                ]
            },
            {
                "cells": [
                    _label("transaction_costs", lang),
                    _format_amount(cost_breakdown.get("transaction_cost"), "SAR", lang),
                    _short_note(explanations.get("transaction_cost"), lang=lang),
                ]
            },
            {
                "cells": [
                    _label("total_capex", lang),
                    _format_amount(cost_breakdown.get("grand_total_capex"), "SAR", lang),
                    _short_note(explanations.get("grand_total_capex"), lang=lang),
                ],
                "bold": True,
            },
        ]
    )
    return rows


def _build_revenue_breakdown_rows(
    excel_breakdown: Dict[str, Any],
    cost_breakdown: Dict[str, Any],
    lang: str = "en",
) -> List[Dict[str, Any]]:
    income_components = excel_breakdown.get("y1_income_components")
    income_components = income_components if isinstance(income_components, dict) else {}

    rows: List[Dict[str, Any]] = []
    for key, amount in income_components.items():
        label = income_component_label(key, lang)
        rows.append(
            {
                "cells": [
                    label,
                    _format_amount(amount, "SAR", lang),
                    _label("rev_nla_rent_rate", lang),
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
                    _label("annual_net_revenue", lang),
                    _format_amount(y1_income, "SAR", lang),
                    _label("rev_sum_income_components", lang),
                ]
            },
            {
                "cells": [
                    _label("annual_net_income", lang),
                    _format_amount(y1_income_effective, "SAR", lang),
                    f"{_fmt_percent(y1_income_effective_factor, 0, lang)} {_label('rev_effective_suffix', lang)}",
                ]
            },
            {
                "cells": [
                    _label("opex", lang),
                    _format_amount(opex_cost, "SAR", lang),
                    f"{_fmt_percent(opex_pct, 0, lang)} {_label('rev_of_annual_net_income', lang)}",
                ]
            },
            {
                "cells": [
                    _label("annual_noi", lang),
                    _format_amount(y1_noi, "SAR", lang),
                    _label("rev_annual_net_income_minus_opex", lang),
                ]
            },
            {
                "cells": [
                    _label("unlevered_roi", lang),
                    _fmt_percent(roi, 1, lang),
                    _label("rev_noi_over_capex", lang),
                ],
                "bold": True,
            },
        ]
    )
    return rows


def _build_assumption_rows(
    assumptions: List[Dict[str, Any]], lang: str = "en"
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in assumptions:
        if not isinstance(item, dict):
            continue
        key = _resolve_ascii(item.get("key") or "", lang)
        if not key:
            continue
        if key.lower() == "far":
            key = _label("far_model_prior", lang)
        else:
            key = assumption_key_label(key, lang)
        value = item.get("value")
        unit = item.get("unit") or ""
        source_type = _resolve_ascii(item.get("source_type") or "", lang)
        if isinstance(value, (int, float)):
            value_text = _fmt_number(value, lang)
        else:
            value_text = _resolve_ascii(value, lang) or _label("na", lang)
        if unit:
            # AR localizes the unit token (and kills any U+00B2); EN keeps the
            # exact ASCII-strip behavior so "m²" still drops as it did before.
            unit_text = _localize_unit(unit, lang) if lang == "ar" else _resolve_ascii(unit, lang)
            if unit_text:
                value_text = f"{value_text} {unit_text}"
        rows.append(
            {
                "cells": [
                    key,
                    value_text,
                    source_type_label(source_type, lang) or _label("na", lang),
                ]
            }
        )
    return rows


def _build_appendix_rows(
    explanations: Dict[str, Any],
    excel_breakdown: Dict[str, Any],
    lang: str = "en",
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    # explanation key -> estimator_i18n label token
    label_map = {
        "residential_bua": "residential_bua",
        "retail_bua": "retail_bua",
        "office_bua": "office_bua",
        "basement_bua": "basement_bua",
        "land_cost": "land_cost",
        "construction_direct": "construction_direct",
        "fitout": "fitout",
        "contingency": "contingency",
        "consultants": "consultants",
        "feasibility_fee": "feasibility_fee",
        "transaction_cost": "transaction_costs",
        "grand_total_capex": "total_capex",
        "y1_income": "annual_net_revenue",
        "y1_income_effective": "annual_net_income",
        "opex": "opex",
        "y1_noi": "annual_noi",
    }
    for key, token in label_map.items():
        note = explanations.get(key)
        note_text = _resolve_ascii(note, lang)
        if not note_text:
            continue
        rows.append(
            {
                "cells": [_label(token, lang), _ellipsize(note_text, 160)],
            }
        )

    income_components = excel_breakdown.get("y1_income_components")
    if isinstance(income_components, dict):
        for key in income_components.keys():
            label = f"{_label('income_prefix', lang)} {income_component_label(key, lang)}"
            rows.append(
                {
                    "cells": [label, _label("rev_nla_rent_rate", lang)],
                }
            )
    return rows


def _build_comps_rows(top_comps: List[Dict[str, Any]], lang: str = "en") -> List[Dict[str, Any]]:
    rows = []
    for comp in top_comps:
        if not isinstance(comp, dict):
            continue
        comp_id = _resolve_ascii(comp.get("id") or "", lang)
        comp_date = _resolve_ascii(comp.get("date") or "", lang)
        city = _resolve_ascii(comp.get("city") or "", lang)
        district = _resolve_ascii(comp.get("district") or "", lang)
        location = ""
        if city and district:
            location = f"{city}/{district}"
        elif city:
            location = city
        elif district:
            location = district
        price = _fmt_money(comp.get("price_per_m2"), lang)
        if not any([comp_id, comp_date, location, price]):
            continue
        rows.append(
            {
                "cells": [
                    comp_id or _label("na", lang),
                    comp_date or _label("na", lang),
                    location or "",
                    f"{price} {_localize_unit('SAR/m2', lang)}",
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
    lang: str = "en",
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
    # Stamp the document language + font family so the draw helpers resolve the
    # lang-appropriate face. EN keeps Helvetica (unchanged); AR embeds NotoNaskh.
    pdf._oak_lang = lang
    pdf._oak_font_family = AR_FONT_FAMILY if lang == "ar" else FONT_FAMILY
    if lang == "ar":
        _register_ar_fonts(pdf)
    pdf.add_page()
    pdf.set_title(title)

    pdf.set_font(_doc_family(pdf), "B", 16)
    if lang == "ar":
        pdf.cell(0, 10, _pdf_safe_text(title, lang), ln=True, align="R")
    else:
        pdf.cell(0, 10, _pdf_safe_text(title), ln=True)

    _draw_section_title(pdf, _label("totals_section", lang))
    metrics = [
        (_label("land_value", lang), _fmt_money(cost_breakdown.get("land_cost") or totals.get("land_value"), lang)),
        (_label("total_capex", lang), _fmt_money(cost_breakdown.get("grand_total_capex"), lang)),
        (_label("annual_net_revenue", lang), _fmt_money(cost_breakdown.get("y1_income") or totals.get("revenues"), lang)),
        (_label("annual_noi", lang), _fmt_money(cost_breakdown.get("y1_noi"), lang)),
        (_label("unlevered_roi", lang), _fmt_percent(cost_breakdown.get("roi"), 1, lang)),
    ]
    metric_width = (pdf.w - pdf.l_margin - pdf.r_margin) / len(metrics)
    # RTL: draw the metric strip right-to-left (cells are center-aligned, so only
    # the order is mirrored). EN order is untouched.
    metrics_draw = list(reversed(metrics)) if lang == "ar" else metrics
    pdf.set_font(_doc_family(pdf), "", 8)
    pdf.set_fill_color(229, 240, 236)
    for label, _value in metrics_draw:
        pdf.cell(metric_width, 5, _pdf_safe_text(label, lang), border=1, align="C", fill=True)
    pdf.ln(5)
    pdf.set_font(_doc_family(pdf), "B", 10)
    for _name, value in metrics_draw:
        pdf.cell(metric_width, 7, _pdf_safe_text(value, lang), border=1, align="C")
    pdf.ln(10)

    explanations = _resolve_explanations(excel_breakdown, lang)

    _draw_section_title(pdf, _label("cost_breakdown_section", lang))
    cost_rows = _build_cost_breakdown_rows(excel_breakdown, cost_breakdown, explanations, lang)
    cost_headers = [
        _label("header_item", lang),
        _label("header_amount", lang),
        _label("header_how_calculated", lang),
    ]
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

    _draw_section_title(pdf, _label("revenue_breakdown_section", lang))
    revenue_rows = _build_revenue_breakdown_rows(excel_breakdown, cost_breakdown, lang)
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
            (_label("parking_required_spaces", lang), _fmt_number(required, lang)),
            (_label("parking_provided_spaces", lang), _fmt_number(provided, lang)),
            (_label("parking_deficit", lang), _fmt_number(deficit, lang)),
            (
                _label("parking_compliant", lang),
                _label("yes", lang)
                if compliant is True
                else _label("no", lang)
                if compliant is False
                else _label("na", lang),
            ),
        ]

    if any(value for _name, value in parking_summary if value != _label("na", lang)):
        pdf.ln(SECTION_SPACING)
        _draw_section_title(pdf, _label("parking_summary_section", lang))
        box_width = pdf.w - pdf.l_margin - pdf.r_margin
        pdf.set_fill_color(245, 248, 247)
        pdf.set_draw_color(200, 220, 214)
        pdf.rect(pdf.l_margin, pdf.get_y(), box_width, ROW_HEIGHT * len(parking_summary) + 4)
        pdf.set_xy(pdf.l_margin + 2, pdf.get_y() + 2)
        pdf.set_font(_doc_family(pdf), "", 9)
        for label, value in parking_summary:
            if lang == "ar":
                # Mirror: value on the left, label on the right.
                pdf.cell(box_width * 0.35, ROW_HEIGHT, _pdf_safe_text(value, lang), align="L")
                pdf.cell(box_width * 0.6, ROW_HEIGHT, _pdf_safe_text(label, lang), align="R", ln=True)
            else:
                pdf.cell(box_width * 0.6, ROW_HEIGHT, _pdf_safe_text(label), align="L")
                pdf.cell(box_width * 0.35, ROW_HEIGHT, _pdf_safe_text(value), align="R", ln=True)

    if lang == "ar":
        summary_text = _resolve_ascii(
            notes.get("summary_ar") or notes.get("summary_en") or notes.get("summary") or "",
            lang,
        )
    else:
        summary_text = _resolve_ascii(notes.get("summary_en") or notes.get("summary") or "")
    if summary_text:
        pdf.ln(SECTION_SPACING)
        _draw_section_title(pdf, _label("executive_summary_section", lang))
        pdf.set_font(_doc_family(pdf), "", 9)
        if lang == "ar":
            _draw_rtl_paragraph(pdf, summary_text, 5)
        else:
            pdf.multi_cell(0, 5, _pdf_safe_text(summary_text))

    pdf.add_page()
    _draw_section_title(pdf, _label("key_assumptions_section", lang))
    assumption_rows = _build_assumption_rows(assumptions, lang)
    if assumption_rows:
        assumption_headers = [
            _label("header_assumption", lang),
            _label("header_value", lang),
            _label("header_source", lang),
        ]
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
        pdf.set_font(_doc_family(pdf), "", 9)
        if lang == "ar":
            pdf.cell(
                0,
                ROW_HEIGHT,
                _pdf_safe_text(_label("no_assumptions_available", lang), lang),
                ln=True,
                align="R",
            )
        else:
            pdf.cell(0, ROW_HEIGHT, _label("no_assumptions_available", lang), ln=True)

    appendix_rows = _build_appendix_rows(explanations, excel_breakdown, lang)
    if appendix_rows:
        pdf.ln(SECTION_SPACING)
        _draw_section_title(pdf, _label("appendix_calc_trace_section", lang))
        appendix_headers = [_label("header_item", lang), _label("header_detail", lang)]
        appendix_col_widths = [60, table_width - 60]
        _draw_table(
            pdf,
            appendix_headers,
            appendix_rows,
            appendix_col_widths,
            ["L", "L"],
            max_chars=[30, 120],
        )

    comps_rows = _build_comps_rows(top_comps, lang)
    if comps_rows:
        pdf.ln(SECTION_SPACING)
        _draw_section_title(pdf, _label("appendix_top_comps_section", lang))
        comps_headers = [
            _label("header_id", lang),
            _label("header_date", lang),
            _label("header_location", lang),
            _label("header_price_sar_m2", lang),
        ]
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
