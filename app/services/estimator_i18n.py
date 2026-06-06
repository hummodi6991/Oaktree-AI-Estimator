"""Server-side EN/AR labels for the feasibility (Estimator) PDF memo.

The feasibility PDF is rendered on the backend (``app/services/pdf.py``) and
therefore cannot reach the frontend i18next catalog at render time. This
module mirrors ``app/services/expansion_advisor_i18n.py``: a single label
table keyed by a stable token, each entry carrying ``{"en", "ar"}``, plus a
``t(token, lang)`` accessor with graceful ``en`` fallback that never raises.

Conventions (mirroring the EA module and the on-screen estimator catalog):

  - **EN values are byte-identical to the literals currently hardcoded in
    ``pdf.py``** (e.g. "Cost breakdown", "Total capex", "How calculated",
    and "m2" stays "m2" — unit-glyph normalization is a later decision).
    Routing the existing labels through ``t(token, "en")`` therefore keeps
    the English PDF byte-for-byte identical.
  - **AR values reuse the authored terminology** from
    ``frontend/src/i18n/ar.json`` (the on-screen estimator surface) wherever a
    concept already has an Arabic string — we do not fork terminology. The
    estimator on-screen catalog uses "ر.س" and "م²" inline (not "SAR"/"m²"),
    and Latin digits; the AR strings here follow the same conventions.
  - Arabic byte hygiene: Arabic yeh is U+064A (ي) and heh is U+0647 (ه);
    no Farsi yeh (U+06CC) or heh-doachashmee (U+06BE).

This PR (5a) wires the labels and selects the persisted ``*_ar`` narratives,
but leaves the ASCII gates in ``pdf.py`` untouched — so the AR path is latent
(blank/garbled) and unexposed until PR-5b adds the font + shaping + BiDi.
"""

from __future__ import annotations

from typing import Dict


# token -> {"en": ..., "ar": ...}
# EN values MUST match the current hardcoded literals in app/services/pdf.py.
LABELS: Dict[str, Dict[str, str]] = {
    # ── Document title (built in app/api/estimates.py) ──────────────
    "doc_title_prefix": {"en": "Estimate", "ar": "تقدير"},

    # ── Section titles ──────────────────────────────────────────────
    "totals_section": {"en": "Totals (SAR)", "ar": "الإجماليات (ر.س)"},
    "cost_breakdown_section": {"en": "Cost breakdown", "ar": "تفصيل التكاليف"},
    "revenue_breakdown_section": {"en": "Revenue breakdown", "ar": "تفصيل الإيرادات"},
    "parking_summary_section": {"en": "Parking summary", "ar": "ملخص المواقف"},
    "executive_summary_section": {"en": "Executive summary", "ar": "الملخص التنفيذي"},
    "key_assumptions_section": {"en": "Key assumptions", "ar": "الافتراضات الرئيسية"},
    "appendix_calc_trace_section": {
        "en": "Appendix: calculation trace",
        "ar": "ملحق: أثر الحساب",
    },
    "appendix_top_comps_section": {
        "en": "Appendix: top comps",
        "ar": "ملحق: أبرز المقارنات",
    },

    # ── Totals metric labels ────────────────────────────────────────
    "land_value": {"en": "Land value", "ar": "قيمة الأرض"},
    "total_capex": {"en": "Total capex", "ar": "إجمالي النفقات الرأسمالية"},
    "annual_net_revenue": {"en": "Annual net revenue", "ar": "صافي الإيراد السنوي"},
    "annual_noi": {"en": "Annual NOI", "ar": "صافي الدخل التشغيلي السنوي"},
    "unlevered_roi": {"en": "Unlevered ROI", "ar": "ROI غير المموّل"},

    # ── Table headers ───────────────────────────────────────────────
    "header_item": {"en": "Item", "ar": "البند"},
    "header_amount": {"en": "Amount", "ar": "القيمة"},
    "header_how_calculated": {"en": "How calculated", "ar": "طريقة الحساب"},
    "header_assumption": {"en": "Assumption", "ar": "الافتراض"},
    "header_value": {"en": "Value", "ar": "القيمة"},
    "header_source": {"en": "Source", "ar": "المصدر"},
    "header_detail": {"en": "Detail", "ar": "التفاصيل"},
    "header_id": {"en": "ID", "ar": "المعرّف"},
    "header_date": {"en": "Date", "ar": "التاريخ"},
    "header_location": {"en": "Location", "ar": "الموقع"},
    "header_price_sar_m2": {"en": "Price (SAR/m2)", "ar": "السعر (ر.س/م²)"},

    # ── Cost-breakdown row labels ───────────────────────────────────
    "effective_far_above_ground": {
        "en": "Effective FAR (above-ground)",
        "ar": "معامل الكثافة الفعّال (فوق الأرض)",
    },
    "residential_bua": {"en": "Residential BUA", "ar": "مساحة البناء السكنية"},
    "retail_bua": {"en": "Retail BUA", "ar": "مساحة البناء التجارية"},
    "office_bua": {"en": "Office BUA", "ar": "مساحة البناء المكتبية"},
    "basement_bua": {"en": "Basement BUA", "ar": "مساحة البناء للقبو"},
    "upper_annex_non_far_bua": {
        "en": "Upper annex (non-FAR, +0.5 floor)",
        "ar": "الملحق العلوي (غير محسوب في معامل الكثافة، +0.5 طابق)",
    },
    "land_cost": {"en": "Land cost", "ar": "تكلفة الأرض"},
    "construction_direct": {"en": "Construction (direct)", "ar": "الإنشاء (مباشر)"},
    "upper_annex_non_far_cost": {
        "en": "Upper annex construction cost (non-FAR)",
        "ar": "تكلفة إنشاء الملحق العلوي (غير محسوب في معامل الكثافة)",
    },
    "fitout": {"en": "Fit-out", "ar": "التجهيزات"},
    "contingency": {"en": "Contingency", "ar": "الاحتياطي"},
    "consultants": {"en": "Consultants", "ar": "الاستشاريون"},
    "feasibility_fee": {"en": "Feasibility fee", "ar": "رسوم الجدوى"},
    "transaction_costs": {"en": "Transaction costs", "ar": "تكاليف المعاملة"},

    # ── Revenue-breakdown labels ────────────────────────────────────
    "annual_net_income": {"en": "Annual net income", "ar": "صافي الدخل السنوي"},
    "opex": {"en": "OPEX", "ar": "المصاريف التشغيلية (OPEX)"},
    # Revenue "how calculated" strings (hardcoded in pdf.py today).
    "rev_nla_rent_rate": {"en": "NLA * rent rate", "ar": "NLA × معدل الإيجار"},
    "rev_sum_income_components": {
        "en": "Sum of income components",
        "ar": "مجموع مكوّنات الدخل",
    },
    "rev_effective_suffix": {"en": "effective", "ar": "فعّال"},
    "rev_of_annual_net_income": {
        "en": "of annual net income",
        "ar": "من صافي الدخل السنوي",
    },
    "rev_annual_net_income_minus_opex": {
        "en": "Annual net income - OPEX",
        "ar": "صافي الدخل السنوي − المصاريف التشغيلية",
    },
    "rev_noi_over_capex": {
        "en": "NOI / total capex",
        "ar": "NOI ÷ إجمالي النفقات الرأسمالية",
    },

    # ── Parking summary labels ──────────────────────────────────────
    "parking_required_spaces": {"en": "Required spaces", "ar": "المواقف المطلوبة"},
    "parking_provided_spaces": {"en": "Provided spaces", "ar": "المواقف المتوفرة"},
    "parking_deficit": {"en": "Deficit", "ar": "العجز"},
    "parking_compliant": {"en": "Compliant", "ar": "مطابق"},

    # ── Boolean / empty-state values ────────────────────────────────
    "yes": {"en": "Yes", "ar": "نعم"},
    "no": {"en": "No", "ar": "لا"},
    "na": {"en": "N/A", "ar": "غير متاح"},
    "no_assumptions_available": {
        "en": "No assumptions available.",
        "ar": "لا توجد افتراضات متاحة.",
    },

    # ── Key-assumptions special labels ──────────────────────────────
    "far_model_prior": {"en": "FAR (model prior)", "ar": "معامل الكثافة (القيمة المسبقة للنموذج)"},

    # ── Appendix: calculation trace ─────────────────────────────────
    "income_prefix": {"en": "Income:", "ar": "الدخل:"},

    # ── Assumption source_type enums ────────────────────────────────
    "source_type.Model": {"en": "Model", "ar": "نموذج"},
    "source_type.Observed": {"en": "Observed", "ar": "ملاحَظ"},
    "source_type.GASTAT": {"en": "GASTAT", "ar": "الهيئة العامة للإحصاء (GASTAT)"},
    "source_type.Riyadh Municipality": {
        "en": "Riyadh Municipality",
        "ar": "أمانة الرياض",
    },
    "source_type.Derived": {"en": "Derived", "ar": "مشتق"},
    "source_type.Assumption": {"en": "Assumption", "ar": "افتراض"},
    "source_type.Manual": {"en": "Manual", "ar": "يدوي"},
}


def t(token: str, lang: str = "en") -> str:
    """Resolve a label token to ``lang``, falling back to ``en``.

    Never raises: an unknown token is passed through unchanged, and a missing
    AR (or unknown-lang) value degrades to the English string.
    """
    entry = LABELS.get(token)
    if entry is None:
        return token
    value = entry.get(lang)
    if value:
        return value
    return entry.get("en", token)


def source_type_label(value: str | None, lang: str = "en") -> str:
    """Translate an assumption ``source_type`` value (e.g. "GASTAT").

    Unknown values pass through unchanged so unexpected source types are never
    dropped. For ``lang="en"`` every known value maps to itself, keeping the
    English PDF byte-identical.
    """
    if not value:
        return value or ""
    token = f"source_type.{value}"
    if token in LABELS:
        return t(token, lang)
    return value
