"""Locale-invariant rendering for Expansion Advisor structured records.

PR #2a persists structured records of the form {"id": ..., "params": ...}
in JSONB columns on expansion_candidate (five columns at PR #2a, plus
two structured strengths/risks columns at PR #3). PR #2b and PR #3
render them into the requested language at read time. The en-side templates are
byte-identical to the producer's English f-strings/append literals
at HEAD; the ar-side templates are Arabic anchor translations
following these formatting conventions:

  - Latin digits throughout (78.4, not ٧٨٫٤)
  - English units inline: "SAR", "m²", "km"
  - Parenthetical English for jargon without a stable Arabic
    equivalent: "providers", "whitespace"
  - Em-dash (U+2014) preserved in both langs
  - "gate" is translated as "معيار" (criterion) — the domain term
  - "gate failed" vs "gate could not be verified" use two distinct
    Arabic frames, not one

When a requested-lang template is missing, render() falls back to
"en" (degraded, never raises). When a structured record itself is
missing/malformed, the caller falls back to the English persisted
column (rule #4 spirit).
"""

from __future__ import annotations

from typing import Any


TEMPLATES: dict[str, dict[str, str]] = {
    "pos.demand_strong": {
        "en": "Demand potential is strong for this district.",
        "ar": "إمكانية الطلب قوية في هذا الحي.",
    },
    "pos.bnm_whitespace_favorable": {
        "en": "Brick-and-mortar competitor whitespace remains favorable.",
        "ar": "فجوة المنافسين في المتاجر الفعلية لا تزال مواتية.",
    },
    "pos.inferred_whitespace": {
        "en": "Inferred competitor whitespace opportunity — low observed delivery activity nearby.",
        "ar": "فرصة فجوة سوقية مُستنتَجة — نشاط توصيل ملاحَظ منخفض في المحيط القريب.",
    },
    "pos.brand_fit_aligned": {
        "en": "Brand-fit profile aligns with site characteristics.",
        "ar": "ملاءمة العلامة التجارية تتوافق مع خصائص الموقع.",
    },
    "pos.economics_meets_band": {
        "en": "Economics profile meets target screening band.",
        "ar": "الجدوى الاقتصادية ضمن النطاق المستهدف.",
    },
    "pos.all_gates_pass": {
        "en": "All required gates pass under available context.",
        "ar": "جميع المعايير المطلوبة مستوفاة وفق البيانات المتاحة.",
    },
    "pos.area_well_aligned": {
        "en": "Site area is well-aligned with target range.",
        "ar": "مساحة الموقع متوافقة جيداً مع النطاق المستهدف.",
    },
    "pos.strong_economics": {
        "en": "Strong economics with favorable rent-to-revenue ratio.",
        "ar": "جدوى اقتصادية قوية مع نسبة إيجار إلى إيرادات مواتية.",
    },
    "pos.well_separated_branch": {
        "en": "Well-separated from nearest branch ({nearest_km:.1f} km) — low overlap.",
        "ar": "بعيد بدرجة كافية عن أقرب فرع ({nearest_km:.1f} km) — تداخل منخفض.",
    },
    "pos.low_competitor_density": {
        "en": "Low same-category competitor density — potential first-mover advantage.",
        "ar": "كثافة منخفضة لمنافسي نفس الفئة — ميزة محتملة للحركة المبكرة.",
    },
    "pos.new_in_top_market": {
        "en": "Newly listed in a top-tier market.",
        "ar": "إعلان جديد في سوق من الفئة الأعلى.",
    },
    "pos.refreshed_in_top_market": {
        "en": "Recently refreshed listing in a top-tier market.",
        "ar": "إعلان مُحدَّث مؤخراً في سوق من الفئة الأعلى.",
    },
    "pos.newly_listed": {
        "en": "Newly listed within the last week.",
        "ar": "إعلان جديد خلال الأسبوع الماضي.",
    },
    "pos.refreshed_listing": {
        "en": "Listing refreshed by the owner within the last week.",
        "ar": "إعلان حدَّثه المالك خلال الأسبوع الماضي.",
    },
    "pos.top_tier_market": {
        "en": "District ranks in the top tier for recent listing activity.",
        "ar": "الحي ضمن الفئة الأعلى من حيث نشاط الإعلانات الأخيرة.",
    },
    "risk.cannibalization_elevated": {
        "en": "Cannibalization risk is elevated versus branch network.",
        "ar": "مخاطر التهام المبيعات (cannibalization) مرتفعة مقابل شبكة الفروع.",
    },
    "risk.economics_below_threshold": {
        "en": "Economics score is below preferred threshold.",
        "ar": "درجة الجدوى الاقتصادية دون الحد المفضَّل.",
    },
    "risk.delivery_competition_high": {
        "en": "Delivery competition intensity is high.",
        "ar": "حدة منافسة التوصيل مرتفعة.",
    },
    "risk.delivery_whitespace_limited": {
        "en": "Delivery platform competition is dense — limited delivery-channel whitespace.",
        "ar": "منافسة منصات التوصيل كثيفة — فجوة سوقية محدودة في قناة التوصيل.",
    },
    "risk.gate_failed": {
        "en": "{_gate_label_capitalized} gate failed.",
        "ar": "معيار {_gate_label_ar} لم يتحقق.",
    },
    "risk.gate_unknown": {
        "en": "{_gate_label_capitalized} could not be verified from current data.",
        "ar": "تعذّر التحقق من معيار {_gate_label_ar} من البيانات الحالية.",
    },
    "risk.delivery_district_estimates": {  # K7 defensive — unreachable at HEAD
        "en": "Delivery data is based on district-level estimates — no listings observed within 1.2 km.",
        "ar": "بيانات التوصيل مبنية على تقديرات على مستوى الحي — لا توجد إعلانات ملاحَظة ضمن 1.2 km.",
    },
    "risk.delivery_inferred": {
        "en": "Delivery market data is inferred — no observed listings near site.",
        "ar": "بيانات سوق التوصيل مُستنتَجة — لا توجد إعلانات ملاحَظة قرب الموقع.",
    },
    "risk.area_near_min": {
        "en": "Area ({area_m2:.0f} m²) is near the minimum of the requested range.",
        "ar": "المساحة ({area_m2:.0f} m²) قريبة من الحد الأدنى للنطاق المطلوب.",
    },
    "risk.area_near_max": {
        "en": "Area ({area_m2:.0f} m²) is near the maximum — may increase fit-out cost.",
        "ar": "المساحة ({area_m2:.0f} m²) قريبة من الحد الأعلى — قد ترفع تكلفة التجهيز.",
    },
    "risk.economics_marginal": {
        "en": "Economics are marginal — rent burden may be high relative to revenue potential.",
        "ar": "الجدوى الاقتصادية حدّية — عبء الإيجار قد يكون مرتفعاً مقارنةً بالإيرادات المحتملة.",
    },
    "risk.nearest_branch_close": {
        "en": "Nearest own branch is only {nearest_km:.1f} km away — high overlap risk.",
        "ar": "أقرب فرع للعلامة على بُعد {nearest_km:.1f} km فقط — مخاطر تداخل مرتفعة.",
    },
    "risk.high_competitor_density": {
        "en": "High competitor density ({count} nearby) — market may be saturated.",
        "ar": "كثافة المنافسين مرتفعة ({count} في المحيط القريب) — السوق قد يكون مشبعاً.",
    },
    # ── PR #3: _build_strengths_and_risks structured records ──
    # Six zero-param templates. The en side is byte-identical to the
    # producer's append literals in _build_strengths_and_risks (no
    # trailing period — matches the producer). The ar side follows the
    # PR #2b conventions: Latin digits, inline English units, and
    # parenthetical English for jargon that is ambiguous in F&B context.
    # S1 en: "High demand index supports branch throughput"
    #   — a strong demand index supports the branch's customer throughput.
    "S1": {
        "en": "High demand index supports branch throughput",
        "ar": "مؤشر طلب مرتفع يدعم معدّل تدفق الفرع (throughput)",
    },
    # S2 en: "Competitive whitespace remains attractive"
    #   — competitor whitespace (market gap) is still attractive.
    "S2": {
        "en": "Competitive whitespace remains attractive",
        "ar": "الفجوة السوقية التنافسية (whitespace) لا تزال جذابة",
    },
    # S3 en: "Parcel characteristics align with target format"
    #   — the parcel's characteristics fit the target branch format.
    "S3": {
        "en": "Parcel characteristics align with target format",
        "ar": "خصائص قطعة الأرض تتوافق مع الصيغة المستهدفة",
    },
    # R1 en: "Rent benchmark fell back to conservative city default (lower confidence)"
    #   — the rent benchmark used the conservative city-wide default.
    "R1": {
        "en": "Rent benchmark fell back to conservative city default (lower confidence)",
        "ar": "تراجع مرجع الإيجار إلى القيمة الافتراضية المحافظة على مستوى المدينة (ثقة أقل)",
    },
    # R2 en: "High overlap risk with existing branches"
    #   — high cannibalization/overlap risk against the branch network.
    "R2": {
        "en": "High overlap risk with existing branches",
        "ar": "مخاطر تداخل مرتفعة مع الفروع الحالية",
    },
    # R3 en: "Competitive density may pressure launch economics"
    #   — competitor density could pressure the launch economics.
    "R3": {
        "en": "Competitive density may pressure launch economics",
        "ar": "كثافة المنافسة قد تضغط على اقتصاديات الإطلاق",
    },
    "demand_thesis": {
        "en": ("Demand is {_demand_label} (score {demand_score:.1f}) with "
               "population reach around {population_reach:.0f}; provider "
               "activity is {_provider_label}, whitespace is "
               "{_whitespace_label}, and delivery competition is "
               "{_competition_label}."),
        "ar": ("الطلب {_demand_label} (الدرجة {demand_score:.1f}) مع "
               "وصول سكاني يقارب {population_reach:.0f}؛ نشاط المنصات "
               "(providers) {_provider_label}، الفجوة السوقية (whitespace) "
               "{_whitespace_label}، ومنافسة التوصيل {_competition_label}."),
    },
    "cost_thesis": {
        "en": ("Estimated rent is {estimated_rent_sar_m2_year:.0f} "
               "SAR/m²/year (~{estimated_annual_rent_sar:,.0f} SAR "
               "annually), fit-out is "
               "~{estimated_fitout_cost_sar:,.0f} SAR."),
        "ar": ("الإيجار التقديري {estimated_rent_sar_m2_year:.0f} "
               "SAR/m²/سنة (~{estimated_annual_rent_sar:,.0f} SAR "
               "سنوياً)، وتكلفة التجهيز "
               "~{estimated_fitout_cost_sar:,.0f} SAR."),
    },
    # decision_summary is composed. The dict uses non-standard keys
    # (en_main, en_suffix_*, ar_main, ar_suffix_*); render() handles
    # this in a dedicated branch.
    "decision_summary": {
        "en_main": ("This {_area_label} candidate in {_district_label} "
                    "scores {final_score:.1f}/100 overall with an "
                    "economics score of {economics_score:.1f}/100. "
                    "It is a practical first-pass option for {_use_case}."),
        "en_suffix_from_key_risks": " Biggest commercial risk: {_risk_text_en}.",
        "en_suffix_tight_economics": (
            " Biggest commercial risk: Rent economics are tight and "
            "should be validated with actual lease terms."),
        "en_suffix_execution": (
            " Biggest commercial risk: Execution risk should be "
            "managed during leasing and design."),
        "ar_main": ("هذا الموقع المرشّح {_area_label} في {_district_label} "
                    "يحصل على {final_score:.1f}/100 إجمالاً وعلى "
                    "{economics_score:.1f}/100 في الجدوى الاقتصادية. "
                    "خيار عملي كمرحلة أولى لـ {_use_case}."),
        "ar_suffix_from_key_risks": " أبرز مخاطر تجارية: {_risk_text_en}.",
        "ar_suffix_tight_economics": (
            " أبرز مخاطر تجارية: الإيجار حدّي وينبغي التحقق منه عبر "
            "شروط العقد الفعلية."),
        "ar_suffix_execution": (
            " أبرز مخاطر تجارية: مخاطر التنفيذ تستلزم الإدارة خلال "
            "مرحلتَي التأجير والتصميم."),
    },
}


DEMAND_LABELS: dict[str, dict[str, str]] = {
    "en": {"strong": "strong", "moderate": "moderate", "limited": "limited"},
    "ar": {"strong": "قوي", "moderate": "متوسط", "limited": "محدود"},
}

PROVIDER_LABELS: dict[str, dict[str, str]] = {
    "en": {
        "dense": "dense",
        "steady": "steady",
        "thin": "thin",
        "district_estimate": "district-level estimate",
        "limited_district": "limited district data",
        "not_observed": "not observed (inferred)",
    },
    "ar": {
        "dense": "كثيف",
        "steady": "مستقر",
        "thin": "ضعيف",
        "district_estimate": "تقدير على مستوى الحي",
        "limited_district": "بيانات حي محدودة",
        "not_observed": "غير ملاحَظ (مُستنتَج)",
    },
}

WHITESPACE_LABELS: dict[str, dict[str, str]] = {
    "en": {
        "attractive": "attractive",
        "balanced": "balanced",
        "tight": "tight",
        "district_inferred": "district-inferred",
        "tight_district": "potentially tight (district-level)",
        "inferred_opportunity": "inferred whitespace opportunity",
    },
    "ar": {
        "attractive": "جذابة",
        "balanced": "متوازنة",
        "tight": "ضيقة",
        "district_inferred": "مُستنتَجة على مستوى الحي",
        "tight_district": "ضيقة محتملاً (على مستوى الحي)",
        "inferred_opportunity": "فرصة فجوة سوقية مُستنتَجة",
    },
}

COMPETITION_LABELS: dict[str, dict[str, str]] = {
    "en": {
        "intense": "intense",
        "manageable": "manageable",
        "district_estimate": "district-level estimate",
        "not_directly_observed": "not directly observed",
    },
    "ar": {
        "intense": "حادة",
        "manageable": "في حدود يمكن إدارتها",
        "district_estimate": "تقدير على مستوى الحي",
        "not_directly_observed": "غير ملاحَظة مباشرةً",
    },
}

AREA_LABELS: dict[str, dict[str, str]] = {
    "en": {"compact": "compact", "standard": "standard"},
    "ar": {"compact": "المتوسط الحجم", "standard": "القياسي"},
}

USE_CASE_LABELS: dict[str, dict[str, str]] = {
    "en": {
        "flagship_dine_in": "flagship dine-in",
        "neighborhood_dine_in": "neighborhood dine-in",
        "delivery_led_branch": "delivery-led branch",
        "compact_cafe": "compact cafe",
        "destination_cafe": "destination cafe",
        "neighborhood_qsr": "neighborhood qsr",
    },
    "ar": {
        "flagship_dine_in": "فرع رئيسي للأكل في الموقع",
        "neighborhood_dine_in": "فرع حي للأكل في الموقع",
        "delivery_led_branch": "فرع موجَّه للتوصيل",
        "compact_cafe": "مقهى مدمج",
        "destination_cafe": "مقهى وجهة",
        "neighborhood_qsr": "مطعم وجبات سريعة في الحي",
    },
}

# Default district label when params.district_label is None.
_DISTRICT_DEFAULT: dict[str, str] = {
    "en": "the target district",
    "ar": "الحي المستهدف",
}


GATE_LABELS: dict[str, dict[str, str]] = {
    "en": {
        "zoning_fit_pass": "zoning fit",
        "area_fit_pass": "area fit",
        "frontage_access_pass": "frontage/access",
        "parking_pass": "parking",
        "district_pass": "district",
        "cannibalization_pass": "cannibalization",
        "delivery_market_pass": "delivery market",
        "economics_pass": "economics",
        "radiance_growth_pass": "Market growth signal",
        "population_floor_pass": "Population reach floor",
        "commercial_floor_pass": "Commercial activity floor",
        "construction_proximity_pass": "Construction proximity floor",
    },
    "ar": {
        "zoning_fit_pass": "ملاءمة التنطيق",
        "area_fit_pass": "ملاءمة المساحة",
        "frontage_access_pass": "الواجهة/الوصول",
        "parking_pass": "مواقف السيارات",
        "district_pass": "الحي",
        "cannibalization_pass": "التهام المبيعات",
        "delivery_market_pass": "سوق التوصيل",
        "economics_pass": "الجدوى الاقتصادية",
        "radiance_growth_pass": "إشارة نمو السوق",
        "population_floor_pass": "الحد الأدنى للوصول السكاني",
        "commercial_floor_pass": "الحد الأدنى للنشاط التجاري",
        "construction_proximity_pass": "الحد الأدنى للقرب الإنشائي",
    },
}


def humanize_gate(gate_key: str, lang: str) -> str:
    """Translate a raw gate key (e.g. 'parking_pass') to the lang's
    label. Falls back to the en table, then to the legacy derivation
    ``key.replace("_pass","").replace("_"," ")`` for unknown keys.

    ``humanize_gate(key, "en")`` reproduces ``_gate_key_to_label(key)``
    at HEAD byte-for-byte.
    """
    table = GATE_LABELS.get(lang) or GATE_LABELS["en"]
    if gate_key in table:
        return table[gate_key]
    if gate_key in GATE_LABELS["en"]:
        return GATE_LABELS["en"][gate_key]
    return gate_key.replace("_pass", "").replace("_", " ")


def _token(table: dict[str, dict[str, str]], lang: str, key: str) -> str:
    """Resolve a token through a label table with en fallback."""
    sub = table.get(lang) or table["en"]
    if key in sub:
        return sub[key]
    if key in table["en"]:
        return table["en"][key]
    return key  # unknown token — pass through


def _render_decision_summary(
    resolved: dict[str, Any],
    tmpl_set: dict[str, str],
    lang: str,
) -> str:
    """Compose main + suffix for decision_summary."""
    main_key = "ar_main" if lang == "ar" else "en_main"
    main_tmpl = tmpl_set.get(main_key) or tmpl_set.get("en_main")
    if not main_tmpl:
        return ""
    summary = main_tmpl.format(**resolved)

    risk_kind = resolved.get("risk_kind")
    if risk_kind == "from_key_risks":
        suffix_key = f"{lang}_suffix_from_key_risks" if lang == "ar" else "en_suffix_from_key_risks"
        # PR #3 dual-read (Q3): prefer the structured risk_id — rendered
        # from the localized R-template — over the persisted English
        # risk_text_en. risk_text_en is retained as the fallback for
        # post-PR-2a-pre-PR-3 rows, which carry no risk_id. The
        # en/omitted path never enters this branch, so it stays
        # byte-identical to HEAD (discipline rule #2).
        if lang == "ar":
            risk_id = resolved.get("risk_id")
            if isinstance(risk_id, str) and risk_id in TEMPLATES:
                ar_clause = render({"id": risk_id, "params": {}}, "ar")
                if ar_clause:
                    resolved = {**resolved, "_risk_text_en": ar_clause}
    elif risk_kind == "tight_economics":
        suffix_key = f"{lang}_suffix_tight_economics" if lang == "ar" else "en_suffix_tight_economics"
    elif risk_kind == "execution":
        suffix_key = f"{lang}_suffix_execution" if lang == "ar" else "en_suffix_execution"
    else:
        return summary

    suffix_tmpl = tmpl_set.get(suffix_key)
    if lang == "ar" and not suffix_tmpl:
        # ar fallback to en
        suffix_tmpl = tmpl_set.get(suffix_key.replace("ar_", "en_"))
    if not suffix_tmpl:
        return summary
    try:
        return summary + suffix_tmpl.format(**resolved)
    except (KeyError, IndexError, ValueError, TypeError):
        return summary


def render(record: dict[str, Any], lang: str) -> str:
    """Render a structured record to a string in the requested lang.

    Returns "" on any error; the caller falls back to the English
    persisted column.
    """
    if not isinstance(record, dict):
        return ""
    try:
        tid = record["id"]
        params = dict(record.get("params") or {})
    except (TypeError, KeyError):
        return ""

    resolved = dict(params)

    # Token resolutions — populate the leading-_ variants the
    # templates reference.
    if "demand_label" in params:
        resolved["_demand_label"] = _token(DEMAND_LABELS, lang, params["demand_label"])
    if "provider_label" in params:
        resolved["_provider_label"] = _token(PROVIDER_LABELS, lang, params["provider_label"])
    if "whitespace_label" in params:
        resolved["_whitespace_label"] = _token(WHITESPACE_LABELS, lang, params["whitespace_label"])
    if "competition_label" in params:
        resolved["_competition_label"] = _token(COMPETITION_LABELS, lang, params["competition_label"])
    if "area_label" in params:
        resolved["_area_label"] = _token(AREA_LABELS, lang, params["area_label"])
    if "use_case" in params:
        resolved["_use_case"] = _token(USE_CASE_LABELS, lang, params["use_case"])

    # district_label: raw string passes through; None → default.
    if "district_label" in params:
        dl = params["district_label"]
        if dl is None:
            resolved["_district_label"] = _DISTRICT_DEFAULT.get(lang) or _DISTRICT_DEFAULT["en"]
        else:
            resolved["_district_label"] = dl

    # gate_key: humanize + (en only) .capitalize() to mirror the
    # producer at expansion_advisor.py (risk.gate_failed/gate_unknown).
    if "gate_key" in params:
        en_label = humanize_gate(params["gate_key"], "en")
        ar_label = humanize_gate(params["gate_key"], lang)
        resolved["_gate_label_capitalized"] = en_label.capitalize()
        resolved["_gate_label_ar"] = ar_label

    # risk_text_en: mirror the producer's normalization for the
    # decision_summary risk clause — ``strip().rstrip(".").strip()`` then
    # first-letter-upper (the template re-appends the period). For ar,
    # the English text is reused unchanged — degraded fallback
    # documented as PR #3 work.
    if "risk_text_en" in params:
        rt = params["risk_text_en"]
        if rt and isinstance(rt, str):
            rs = rt.strip().rstrip(".").strip()
            if rs and not rs[0].isupper():
                rs = rs[0].upper() + rs[1:]
            resolved["_risk_text_en"] = rs
        else:
            resolved["_risk_text_en"] = rt or ""

    try:
        tmpl_set = TEMPLATES.get(tid) or {}
        if tid == "decision_summary":
            return _render_decision_summary(resolved, tmpl_set, lang)
        tmpl = tmpl_set.get(lang) or tmpl_set.get("en")
        if not tmpl:
            return ""
        return tmpl.format(**resolved)
    except (KeyError, IndexError, ValueError, TypeError):
        return ""
