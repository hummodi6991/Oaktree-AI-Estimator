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
    estimator surface uses "ر.س" inline (not "SAR"). For the unit token that
    carries a squared-metre superscript the PDF renders a **baseline ``٢``**
    (i.e. ``م٢`` / ``ر.س/م٢``), never U+00B2 — the embedded Naskh face lacks
    the superscript glyph, so ``م²`` would silently drop its ``2``.
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
    "header_price_sar_m2": {"en": "Price (SAR/m2)", "ar": "السعر (ر.س/م٢)"},

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

    # ── Income-component keys (revenue table + calc-trace appendix) ──────
    # EN values are the humanized key (str(key).replace("_", " ")) so the EN
    # PDF is unchanged; AR reuses the asset-class terminology (سكني/تجاري/مكتبي).
    "income_component.residential": {"en": "residential", "ar": "سكني"},
    "income_component.retail": {"en": "retail", "ar": "تجاري"},
    "income_component.office": {"en": "office", "ar": "مكتبي"},
    "income_component.commercial": {"en": "commercial", "ar": "تجاري"},
    "income_component.parking_income": {"en": "parking income", "ar": "دخل المواقف"},

    # ── Key-assumption keys (Key Assumptions table) ─────────────────────
    # EN values equal the raw key (current behavior); AR is the translation.
    "assumption.land_price": {"en": "land_price", "ar": "سعر الأرض"},
    "assumption.rent_rate": {"en": "rent_rate", "ar": "معدل الإيجار"},
    "assumption.excel_method": {"en": "excel_method", "ar": "طريقة إكسل"},
    "assumption.site_area_m2": {"en": "site_area_m2", "ar": "مساحة الموقع (م٢)"},
    "assumption.ppm2": {"en": "ppm2", "ar": "سعر المتر المربع"},
    "assumption.real_estate_price_index_scalar": {
        "en": "real_estate_price_index_scalar",
        "ar": "معامل مؤشر أسعار العقار",
    },
    "assumption.parking_required_spaces": {
        "en": "parking_required_spaces",
        "ar": "المواقف المطلوبة",
    },
    "assumption.parking_provided_spaces": {
        "en": "parking_provided_spaces",
        "ar": "المواقف المتوفرة",
    },
    "assumption.parking_supply_gross_m2_per_space": {
        "en": "parking_supply_gross_m2_per_space",
        "ar": "إجمالي مساحة الموقف (م٢/موقف)",
    },
    "assumption.parking_supply_layout_efficiency": {
        "en": "parking_supply_layout_efficiency",
        "ar": "كفاءة تخطيط المواقف",
    },
    "assumption.avg_unit_size_residential_m2": {
        "en": "avg_unit_size_residential_m2",
        "ar": "متوسط مساحة الوحدة السكنية (م٢)",
    },
    "assumption.avg_unit_size_retail_m2": {
        "en": "avg_unit_size_retail_m2",
        "ar": "متوسط مساحة الوحدة التجارية (م٢)",
    },
    "assumption.avg_unit_size_office_m2": {
        "en": "avg_unit_size_office_m2",
        "ar": "متوسط مساحة الوحدة المكتبية (م٢)",
    },
    "assumption.sale_price_per_m2": {
        "en": "sale_price_per_m2",
        "ar": "سعر البيع لكل م٢",
    },
    "assumption.rent_per_m2": {"en": "rent_per_m2", "ar": "الإيجار لكل م٢"},
    "assumption.avg_unit_m2": {"en": "avg_unit_m2", "ar": "متوسط مساحة الوحدة (م٢)"},
    "assumption.occ": {"en": "occ", "ar": "نسبة الإشغال"},
    "assumption.op_ex_ratio": {"en": "op_ex_ratio", "ar": "نسبة المصاريف التشغيلية"},
    "assumption.cap_rate": {"en": "cap_rate", "ar": "معدل الرسملة"},
    "assumption.parking_extra_spaces_monetized": {
        "en": "parking_extra_spaces_monetized",
        "ar": "المواقف الإضافية المستثمَرة",
    },
    "assumption.parking_monthly_rate_sar_per_space": {
        "en": "parking_monthly_rate_sar_per_space",
        "ar": "الإيجار الشهري للموقف (ر.س/موقف)",
    },
    "assumption.parking_occupancy": {"en": "parking_occupancy", "ar": "إشغال المواقف"},
    "assumption.parking_public_access": {
        "en": "parking_public_access",
        "ar": "وصول عام للمواقف",
    },
    "assumption.parking_income_y1": {
        "en": "parking_income_y1",
        "ar": "دخل المواقف (السنة الأولى)",
    },
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


def _humanize_key(key: str) -> str:
    """The legacy display form for a raw key (snake_case → spaced words)."""
    return str(key).replace("_", " ")


def income_component_label(key: str, lang: str = "en") -> str:
    """Translate a Y1 income-component key (``residential``, ``retail`` …).

    EN is byte-identical to the prior behavior (``str(key).replace("_", " ")``)
    for every key. AR maps the known canonical components to the asset-class
    terminology and passes unknown keys through humanized.
    """
    if lang != "ar":
        return _humanize_key(key)
    canon = str(key).strip().lower()
    if canon.endswith("_rent"):
        canon = canon[: -len("_rent")]
    entry = LABELS.get(f"income_component.{canon}")
    if entry and entry.get("ar"):
        return entry["ar"]
    return _humanize_key(key)


def assumption_key_label(key: str, lang: str = "en") -> str:
    """Translate a Key-Assumptions row key (``land_price``, ``rent_rate`` …).

    EN returns the raw key unchanged (current behavior). AR maps the known
    assumption keys and passes unknown keys through unchanged.
    """
    if not key:
        return key or ""
    if lang != "ar":
        return key
    entry = LABELS.get(f"assumption.{key}")
    if entry and entry.get("ar"):
        return entry["ar"]
    return key
