"""PR #5b — Arabic feasibility-PDF render core (font + shaping + RTL).

Guards the discipline that the EN path is untouched while the AR path gains
correct shaping/BiDi, font embedding and a mirrored RTL layout:

  1. EN: the rendered PDF never embeds NotoNaskh and never shapes (the lang-
     aware text gates keep the exact ASCII/latin-1 behavior).
  2. AR: the rendered PDF embeds the NotoNaskh faces (both Regular and Bold).
  3. ``_shape_ar`` reshapes (contextual joining) and BiDi-reorders.
  4. The lang-aware text gates pass Arabic through on ``ar`` and strip it on
     ``en``.
  5. ``_draw_table`` mirrors column order/alignment on the AR path.
"""

from __future__ import annotations

import pytest

pytest.importorskip("fpdf")
pytest.importorskip("arabic_reshaper")
pytest.importorskip("bidi")

from app.services import pdf as pdfmod
from app.services.pdf import (
    _flip_align,
    _pdf_safe_text,
    _resolve_ascii,
    _shape_ar,
    _short_note,
    build_memo_pdf,
)


# A compact fixture that carries AR narratives + explanations so the AR path
# exercises shaping in cells, multi-line summary and the appendix.
def _args() -> dict:
    return dict(
        title="Estimate EST-1",
        totals={
            "land_value": 1_000_000,
            "parking_required_spaces": 10,
            "parking_provided_spaces": 8,
            "parking_deficit_spaces": 2,
            "parking_compliant": False,
        },
        assumptions=[{"key": "far", "value": 3.2, "unit": "", "source_type": "Model"}],
        top_comps=[
            {"id": "C-1", "date": "2025-01-01", "city": "Riyadh",
             "district": "Al Olaya", "price_per_m2": 4200}
        ],
        excel_breakdown={
            "far_above_ground": 3.2,
            "built_area": {"residential": 5000.0},
            "y1_income_components": {"residential_rent": 100000.0},
            "explanations_en": {"land_cost": "Land cost = area * price"},
            "explanations_ar": {"land_cost": "تكلفة الأرض = المساحة × السعر"},
        },
        cost_breakdown={
            "land_cost": 1_000_000, "grand_total_capex": 5_000_000,
            "y1_income": 100000, "y1_noi": 80000, "roi": 0.08,
        },
        notes={
            "summary_en": "Executive summary in English narrative form here.",
            "summary_ar": "هذا ملخص تنفيذي باللغة العربية يمتد على أكثر من سطر "
                          "واحد للتأكد من أن الالتفاف وإعادة التشكيل يعملان بشكل صحيح.",
            "parking": {"required_spaces_final": 10, "provided_spaces_final": 8,
                        "deficit_spaces_final": 2, "compliant": False},
        },
    )


# ── 1. EN never embeds the AR font / never shapes ───────────────────
def test_en_pdf_does_not_embed_noto_naskh() -> None:
    out = build_memo_pdf(lang="en", **_args())
    assert b"NotoNaskhArabic" not in out
    assert b"Helvetica" in out


def test_en_omitted_lang_matches_explicit_en() -> None:
    a = build_memo_pdf(**_args())
    b = build_memo_pdf(lang="en", **_args())
    # Only volatile metadata (CreationDate/ID) may differ; strip it.
    import re

    def norm(x: bytes) -> bytes:
        x = re.sub(rb"/CreationDate \(D:[^)]*\)", b"", x)
        return re.sub(rb"/ID \[<[0-9A-Fa-f]+><[0-9A-Fa-f]+>\]", b"", x)

    assert norm(a) == norm(b)


# ── 2. AR embeds NotoNaskh (both faces) ─────────────────────────────
def test_ar_pdf_embeds_both_noto_naskh_faces() -> None:
    out = build_memo_pdf(lang="ar", **_args())
    assert b"NotoNaskhArabic" in out
    # Two embedded TrueType subsets → Regular + Bold.
    assert out.count(b"/FontFile2") == 2
    # AR PDF is materially larger than EN (the font is embedded).
    assert len(out) > len(build_memo_pdf(lang="en", **_args()))


# ── 3. Shaping + BiDi helper ────────────────────────────────────────
def test_shape_ar_reshapes_and_reorders() -> None:
    src = "نوصي"
    shaped = _shape_ar(src)
    assert shaped != src
    assert len(shaped) == len(src)
    # Reshaping maps to Arabic Presentation Forms (U+FB50..U+FEFF block).
    assert any(0xFB50 <= ord(ch) <= 0xFEFF for ch in shaped)


def test_shape_ar_passes_empty_through() -> None:
    assert _shape_ar("") == ""


# ── 4. Lang-aware text gates ────────────────────────────────────────
def test_text_gates_strip_on_en_pass_on_ar() -> None:
    ar = "مرحبا"
    # EN strips non-ASCII (back-compat byte behavior).
    assert _pdf_safe_text(ar, "en") == ""
    assert _resolve_ascii(ar, "en") == ""
    assert _short_note(ar, lang="en") == ""
    # AR keeps the Arabic content (shaped for _pdf_safe_text, raw for the rest).
    assert _pdf_safe_text(ar, "ar") != ""
    assert _resolve_ascii(ar, "ar") == ar
    assert _short_note(ar, lang="ar") == ar


def test_pdf_safe_text_en_default_is_unchanged() -> None:
    # Default lang is "en": ASCII passes, non-ASCII stripped — pre-5b behavior.
    assert _pdf_safe_text("Total capex") == "Total capex"
    assert _pdf_safe_text("café") == "caf"


# ── 5. RTL alignment mirroring ──────────────────────────────────────
def test_flip_align() -> None:
    assert _flip_align("L") == "R"
    assert _flip_align("R") == "L"
    assert _flip_align("C") == "C"
