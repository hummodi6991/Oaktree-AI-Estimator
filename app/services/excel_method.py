import copy
import math
from typing import Any, Dict

from app.services.parking import (
    DEFAULT_PARKING_SUPPLY_GROSS_M2_PER_SPACE,
    DEFAULT_PARKING_SUPPLY_LAYOUT_EFFICIENCY,
)
from app.services.parking_income import compute_parking_income, _normalize_landuse_code

# Accounting/underwriting haircut default: only 90% of Year-1 net income is treated as "effective"
# for headline unlevered ROI (stabilization, downtime, leakage, collection loss, etc.).
DEFAULT_Y1_INCOME_EFFECTIVE_FACTOR = 0.90
DEFAULT_NON_FAR_UNIT_COST = 2200.0


def _parse_bool(value: Any) -> bool | None:
    """
    Best-effort bool parser.

    Returns:
        True/False if value is a recognizable boolean-like input.
        None if value is missing or unparseable (caller can apply defaults).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in {"1", "true", "t", "yes", "y"}:
        return True
    if s in {"0", "false", "f", "no", "n"}:
        return False
    return None


def _normalize_y1_income_effective_factor(value: Any) -> float:
    """Normalize user-provided annual net income percentage to a 0–1 factor."""

    try:
        factor = float(value)
    except Exception:
        return DEFAULT_Y1_INCOME_EFFECTIVE_FACTOR

    if factor < 0:
        return 0.0

    if factor > 1.0:
        factor = factor / 100.0

    return max(0.0, min(factor, 1.0))


def _is_basement_key(key: str) -> bool:
    k = (key or "").strip().lower()
    if k in {"basement", "underground"}:
        return True
    if "basement" in k:
        return True
    return False


def _is_non_far_area_key(key: str) -> bool:
    k = (key or "").strip().lower()
    if not k:
        return False
    return ("non_far" in k) or ("annex" in k) or ("upper_annex" in k)


def _canon_key(key: Any) -> str:
    return str(key or "").strip().lower()


def _build_area_by_canon_key(area_ratio: dict[str, Any], site_area_m2: float) -> dict[str, float]:
    """
    Build built-area dict keyed by canonical (lowercased) land-use keys.
    Aggregates areas if caller provided inconsistent casing/labels.
    """
    out: dict[str, float] = {}
    for k in (area_ratio or {}).keys():
        ck = _canon_key(k)
        if not ck:
            continue
        try:
            ratio = float((area_ratio or {}).get(k, 0.0) or 0.0)
        except Exception:
            ratio = 0.0
        out[ck] = float(out.get(ck, 0.0) or 0.0) + (ratio * float(site_area_m2))
    return out


def _lookup_by_canon(d: dict[str, Any], key: Any, default: float = 0.0) -> float:
    """
    Best-effort value lookup using canonicalized key.
    Tries exact key first, then canonical lowercased key.
    """
    if not isinstance(d, dict):
        return float(default)
    if key in d:
        try:
            return float(d.get(key) or 0.0)
        except Exception:
            return float(default)
    ck = _canon_key(key)
    try:
        return float(d.get(ck) or 0.0)
    except Exception:
        return float(default)


def _choose_upper_annex_revenue_sink(area_ratio: dict[str, Any]) -> str | None:
    """
    Choose where the upper annex revenue should flow:
      1) residential (preferred)
      2) office (fallback)
      3) never retail
    Returns the target key from area_ratio, or None if no suitable target exists.
    """
    if not isinstance(area_ratio, dict) or not area_ratio:
        return None

    # Treat "present" as having a *positive* area_ratio entry.
    # (Some callers/clients may keep keys with 0.0 values.)
    def _first_positive_key(pred) -> str | None:
        for k, v in area_ratio.items():
            ck = _canon_key(k)
            if not ck:
                continue
            if not pred(ck):
                continue
            try:
                fv = float(v or 0.0)
            except Exception:
                continue
            if fv > 1e-9:
                return ck
        return None

    # 1) Residential first
    res_ck = _first_positive_key(
        lambda ck: ck == "residential"
        or ck.startswith("residential")
        or ck in {"res", "housing"}
    )
    if res_ck:
        return res_ck

    # 2) Office next
    office_ck = _first_positive_key(lambda ck: ck == "office" or ck.startswith("office"))
    if office_ck:
        return office_ck

    return None


def _area_ratio_positive_sum(
    ar: Any,
    *,
    exclude_basement: bool = False,
    exclude_non_far: bool = False,
) -> float:
    if not isinstance(ar, dict):
        return 0.0
    total = 0.0
    for k, v in ar.items():
        if exclude_basement and _is_basement_key(str(k)):
            continue
        if exclude_non_far and _is_non_far_area_key(str(k)):
            continue
        try:
            fv = float(v)
            if fv > 0:
                total += fv
        except Exception:
            continue
    return total


def _is_placeholder_area_ratio(ar: Any) -> bool:
    if not isinstance(ar, dict) or not ar:
        return True
    positive: dict[str, float] = {}
    for k, v in ar.items():
        try:
            fv = float(v)
            if fv > 0:
                positive[str(k).strip().lower()] = fv
        except Exception:
            continue
    if not positive:
        return True
    # Legacy placeholder: 2.7 everywhere
    if all(abs(v - 2.7) < 1e-6 for v in positive.values()):
        return True
    # Current residential template placeholder: residential 1.6 + basement 1.0
    r = positive.get("residential")
    b = positive.get("basement")
    if (
        r is not None
        and b is not None
        and abs(r - 1.6) < 1e-6
        and abs(b - 1.0) < 1e-6
        and len(positive) <= 2
    ):
        return True
    return False


def scale_placeholder_area_ratio(
    excel_inputs: Dict[str, Any], target_far: float | None, target_far_source: str | None = None
) -> Dict[str, Any]:
    """Scale Excel area ratios to match a target FAR if the ratios are template placeholders."""

    result = dict(excel_inputs or {})
    area_ratio = result.get("area_ratio") if isinstance(result, dict) else {}
    placeholder_area_ratio = _is_placeholder_area_ratio(area_ratio)

    # Guardrail:
    # If FAR is coming only from the API default fallback (method == "default_far"),
    # do NOT scale the template ratios. Otherwise every parcel becomes FAR=2.0
    # (e.g., 1.6 -> 2.0 scale 1.25).
    src_norm = (target_far_source or "").strip().lower()
    if src_norm in {"default_far", "default"}:
        return result

    should_scale_area_ratio = placeholder_area_ratio and target_far is not None and float(target_far) > 0
    if not should_scale_area_ratio:
        return result

    # Match above-ground FAR only; basement stays as-is
    base_sum = _area_ratio_positive_sum(area_ratio, exclude_basement=True)
    scaled = None
    scale_str = "n/a"
    if base_sum > 0 and isinstance(area_ratio, dict):
        scale = float(target_far) / base_sum
        scale_str = f"{scale:.3f}"
        scaled = {}
        for key, val in area_ratio.items():
            try:
                fv = float(val or 0.0)
            except Exception:
                fv = 0.0
            if fv <= 0:
                scaled[key] = fv
            elif _is_basement_key(str(key)):
                scaled[key] = fv
            else:
                scaled[key] = fv * scale
    elif isinstance(area_ratio, dict):
        scaled = {}
        for key, val in area_ratio.items():
            try:
                scaled[key] = float(val or 0.0)
            except Exception:
                scaled[key] = 0.0
        scaled["residential"] = float(target_far)
    else:
        scaled = {"residential": float(target_far)}

    result["area_ratio"] = scaled
    result["area_ratio_note"] = (
        f"Auto-scaled above-ground area ratios: baseline FAR {base_sum:.2f} → "
        f"target FAR {float(target_far):.2f} (scale {scale_str}; source {target_far_source or 'far'}); "
        "basement ratio unchanged"
    )
    result["area_ratio_note_ar"] = (
        f"تم ضبط نسب المساحة فوق الأرض تلقائيًا: معامل البناء الأساسي {base_sum:.2f} → "
        f"المعامل المستهدف {float(target_far):.2f} (معامل التحجيم {scale_str}؛ المصدر {target_far_source or 'far'}); "
        "نسبة القبو دون تغيير"
    )
    return result


def _is_basement_area_ratio_key(key: str) -> bool:
    """True if this `area_ratio` key represents below-grade area (not scaled by above-ground floors)."""
    k = (key or "").strip().lower()
    return ("basement" in k) or ("underground" in k) or ("below" in k)


def scale_area_ratio_by_floors(
    inputs: Dict[str, Any],
    *,
    desired_floors_above_ground: float | int | str | None,
    baseline_floors_above_ground: float | int | str | None,
    desired_floors_source: str | None = None,
    baseline_floors_source: str | None = None,
) -> Dict[str, Any]:
    """
    Option B — Use floors (stories) to scale `area_ratio`.

    We treat the *current* above-ground `area_ratio` values as representing a baseline FAR at
    `baseline_floors_above_ground`. To reflect a different allowed floors count, we scale all
    above-ground area_ratio entries by:

        factor = desired_floors_above_ground / baseline_floors_above_ground

    Basement / underground ratios are left unchanged.
    """
    if desired_floors_above_ground is None or baseline_floors_above_ground is None:
        return inputs

    try:
        desired = float(desired_floors_above_ground)
        baseline = float(baseline_floors_above_ground)
    except Exception:
        return inputs

    if desired <= 0 or baseline <= 0:
        return inputs

    factor = desired / baseline

    area_ratio = inputs.get("area_ratio")
    if not isinstance(area_ratio, dict):
        return inputs

    # No-op factor: still record metadata for transparency.
    if abs(factor - 1.0) < 1e-9:
        inputs.setdefault("floors_above_ground", desired)
        inputs.setdefault("baseline_floors_above_ground", baseline)
        if desired_floors_source is not None:
            inputs.setdefault("floors_above_ground_source", desired_floors_source)
        if baseline_floors_source is not None:
            inputs.setdefault("baseline_floors_above_ground_source", baseline_floors_source)
        return inputs

    new_area_ratio: Dict[str, Any] = {}
    scaled_keys: list[str] = []
    for k, v in area_ratio.items():
        # Preserve non-numeric values verbatim.
        try:
            r = float(v or 0)
        except Exception:
            new_area_ratio[k] = v
            continue

        if isinstance(k, str) and _is_basement_area_ratio_key(k):
            new_area_ratio[k] = r
            continue

        new_area_ratio[k] = r * factor
        if isinstance(k, str):
            scaled_keys.append(k)

    inputs["area_ratio"] = new_area_ratio

    note_parts: list[str] = [
        f"Floors scaling applied: above-ground area ratios × {factor:.3f} "
        f"(baseline floors {baseline:g} → desired floors {desired:g}; basement unchanged)."
    ]
    if baseline_floors_source or desired_floors_source:
        sources: list[str] = []
        if baseline_floors_source:
            sources.append(f"baseline={baseline_floors_source}")
        if desired_floors_source:
            sources.append(f"desired={desired_floors_source}")
        note_parts.append(f"Sources: {', '.join(sources)}.")
    if scaled_keys:
        note_parts.append(f"Scaled keys: {', '.join(scaled_keys)}.")

    existing_note = str(inputs.get("area_ratio_note") or "").strip()
    appended_note = " ".join(note_parts).strip()
    inputs["area_ratio_note"] = (existing_note + " " + appended_note).strip() if existing_note else appended_note

    # Helpful metadata for UI/debugging (ignored by estimator math).
    inputs["floors_above_ground"] = desired
    inputs["baseline_floors_above_ground"] = baseline
    if desired_floors_source is not None:
        inputs["floors_above_ground_source"] = desired_floors_source
    if baseline_floors_source is not None:
        inputs["baseline_floors_above_ground_source"] = baseline_floors_source

    return inputs


def _fmt_amount(value: float | int | str | None, decimals: int = 3) -> str:
    try:
        return f"{float(value):,.{decimals}f}"
    except Exception:
        return str(value)


def build_excel_explanations(
    site_area_m2: float, inputs: Dict[str, Any], breakdown: Dict[str, Any]
) -> tuple[Dict[str, str], Dict[str, str]]:
    """Human-readable explanations for the Excel-style cost breakdown.

    These are used by the web UI and PDF export, so keep wording synchronized here.
    """

    unit_cost = breakdown.get("unit_cost_resolved") or inputs.get("unit_cost", {}) or {}
    rent_rates = inputs.get("rent_sar_m2_yr", {}) or {}
    rent_applied = breakdown.get("rent_applied_sar_m2_yr", {}) or {}
    efficiency = inputs.get("efficiency", {}) or {}
    area_ratio = inputs.get("area_ratio", {}) or {}
    far_above_ground = breakdown.get("far_above_ground")
    far_total_including_basement = breakdown.get("far_total_including_basement")
    try:
        far_above_ground = float(far_above_ground)
    except Exception:
        far_above_ground = _area_ratio_positive_sum(
            area_ratio,
            exclude_basement=True,
            exclude_non_far=True,
        )
    try:
        far_total_including_basement = float(far_total_including_basement)
    except Exception:
        far_total_including_basement = _area_ratio_positive_sum(area_ratio, exclude_basement=False)

    built_area = breakdown.get("built_area", {}) or {}
    nla = breakdown.get("nla", {}) or {}
    y1_income_components = breakdown.get("y1_income_components", {}) or {}
    direct_cost = breakdown.get("direct_cost", {}) or {}

    re_scalar = float(inputs.get("re_price_index_scalar") or 1.0)

    explanations_en: Dict[str, str] = {}
    explanations_ar: Dict[str, str] = {}
    area_ratio_note = str(inputs.get("area_ratio_note") or "").strip()
    area_ratio_note_ar = str(inputs.get("area_ratio_note_ar") or area_ratio_note).strip()
    if area_ratio_note:
        explanations_en["area_ratio_override"] = area_ratio_note
    if area_ratio_note_ar:
        explanations_ar["area_ratio_override"] = area_ratio_note_ar

    land_price = float(inputs.get("land_price_sar_m2", 0.0) or 0.0)
    land_cost_total = float(breakdown.get("land_cost", site_area_m2 * land_price) or 0.0)
    explanations_en["land_cost"] = (
        f"{_fmt_amount(site_area_m2)} m² × {land_price:,.0f} SAR/m² = {_fmt_amount(land_cost_total)} SAR."
    )
    explanations_ar["land_cost"] = (
        f"{_fmt_amount(site_area_m2)} م² × {land_price:,.0f} SAR/م² = {_fmt_amount(land_cost_total)} SAR."
    )

    note_appended = False
    for key, area in built_area.items():
        ratio = float(area_ratio.get(key, 0.0) or 0.0)
        if ratio:
            key_lower = str(key).lower()
            if key_lower == "residential":
                explanations_en[f"{key}_bua"] = (
                    f"{_fmt_amount(site_area_m2)} m² × area ratio {ratio:.3f} = {_fmt_amount(area)} m²."
                )
                explanations_ar[f"{key}_bua"] = (
                    f"{_fmt_amount(site_area_m2)} م² × نسبة المساحة {ratio:.3f} = {_fmt_amount(area)} م²."
                )
            elif key_lower == "upper_annex_non_far":
                coverage = 0.0
                try:
                    coverage = float(inputs.get("coverage_ratio") or 0.0)
                except Exception:
                    coverage = 0.0
                if coverage > 0 and area > 0:
                    explanations_en[f"{key}_bua"] = (
                        "Upper annex (non-FAR) = site area × (0.5 × coverage). "
                        f"= {_fmt_amount(site_area_m2)} × (0.5 × {coverage:.3f}) "
                        f"= {_fmt_amount(area)} m². Excluded from FAR."
                    )
                    explanations_ar[f"{key}_bua"] = (
                        "الملحق العلوي (غير محسوب في معامل البناء) = مساحة الأرض × (0.5 × نسبة التغطية). "
                        f"= {_fmt_amount(site_area_m2)} × (0.5 × {coverage:.3f}) "
                        f"= {_fmt_amount(area)} م². مستبعد من معامل البناء."
                    )
                else:
                    explanations_en[f"{key}_bua"] = (
                        "Upper annex (non-FAR) added for mixed-use. Excluded from FAR."
                    )
                    explanations_ar[f"{key}_bua"] = (
                        "تمت إضافة الملحق العلوي (غير محسوب في معامل البناء) للاستخدام المختلط. "
                        "مستبعد من معامل البناء."
                    )
            elif key_lower.startswith("basement"):
                explanations_en[f"{key}_bua"] = (
                    f"{_fmt_amount(site_area_m2)} m² × basement ratio {ratio:.3f} = {_fmt_amount(area)} m². "
                    "Basement excluded from FAR."
                )
                explanations_ar[f"{key}_bua"] = (
                    f"{_fmt_amount(site_area_m2)} م² × نسبة القبو {ratio:.3f} = {_fmt_amount(area)} م². "
                    "القبو مستبعد من معامل البناء."
                )
            else:
                explanations_en[f"{key}_bua"] = (
                    f"{_fmt_amount(site_area_m2)} m² × area ratio {ratio:.3f} = {_fmt_amount(area)} m²."
                )
                explanations_ar[f"{key}_bua"] = (
                    f"{_fmt_amount(site_area_m2)} م² × نسبة المساحة {ratio:.3f} = {_fmt_amount(area)} م²."
                )
            if (
                area_ratio_note
                and not note_appended
                and not key_lower.startswith("basement")
                and key_lower not in {"residential", "retail", "office", "upper_annex_non_far"}
            ):
                explanations_en[f"{key}_bua"] = f"{explanations_en[f'{key}_bua']} {area_ratio_note}"
                if area_ratio_note_ar:
                    explanations_ar[f"{key}_bua"] = f"{explanations_ar[f'{key}_bua']} {area_ratio_note_ar}"
                note_appended = True
        else:
            explanations_en[f"{key}_bua"] = f"Built-up area {_fmt_amount(area)} m²."
            explanations_ar[f"{key}_bua"] = f"المساحة المبنية {_fmt_amount(area)} م²."

    far_explanation = (
        "Above-ground FAR = Σ(area ratios excluding basement) "
        f"= {far_above_ground:.3f}."
    )
    far_explanation_ar = (
        "معامل البناء فوق الأرض = Σ(نسب المساحة باستثناء القبو) "
        f"= {far_above_ground:.3f}."
    )
    explanations_en["effective_far_above_ground"] = far_explanation
    explanations_ar["effective_far_above_ground"] = far_explanation_ar

    sub_total = float(breakdown.get("sub_total", 0.0) or 0.0)
    direct_total = sum(direct_cost.values())
    construction_parts = []
    construction_parts_ar = []
    for key, area in built_area.items():
        base_unit = unit_cost.get("basement") if key.lower().startswith("basement") else unit_cost.get(key, 0.0)
        construction_parts.append(
            f"{key}: {_fmt_amount(area)} m² × {float(base_unit):,.0f} SAR/m² = "
            f"{_fmt_amount(float(direct_cost.get(key, 0.0)))} SAR"
        )
        construction_parts_ar.append(
            f"{key}: {_fmt_amount(area)} م² × {float(base_unit):,.0f} SAR/م² = "
            f"{_fmt_amount(float(direct_cost.get(key, 0.0)))} SAR"
        )
    if construction_parts:
        explanations_en["construction_direct"] = (
            "; ".join(construction_parts) + "."
        )
        explanations_ar["construction_direct"] = (
            "; ".join(construction_parts_ar) + f". إجمالي الإنشاء المباشر: {_fmt_amount(direct_total)} SAR."
        )

    upper_annex_area = built_area.get("upper_annex_non_far")
    if upper_annex_area is not None:
        unit_rate = float(unit_cost.get("upper_annex_non_far") or 0.0)
        explanations_en["upper_annex_non_far_cost"] = (
            f"{_fmt_amount(upper_annex_area)} m² × {unit_rate:,.0f} SAR/m²."
        )
        explanations_ar["upper_annex_non_far_cost"] = (
            f"{_fmt_amount(upper_annex_area)} م² × {unit_rate:,.0f} ر.س/م²."
        )

    fitout_area = sum(
        value for key, value in built_area.items() if not key.lower().startswith("basement")
    )
    fitout_rate = float(inputs.get("fitout_rate") or 0.0)
    fitout_cost = float(breakdown.get("fitout_cost", 0.0) or 0.0)
    explanations_en["fitout"] = (
        f"{_fmt_amount(fitout_area)} m² × {fitout_rate:,.0f} SAR/m² = {_fmt_amount(fitout_cost)} SAR "
        "(above-ground only)."
    )
    explanations_ar["fitout"] = (
        f"{_fmt_amount(fitout_area)} م² × {fitout_rate:,.0f} SAR/م² = {_fmt_amount(fitout_cost)} SAR "
        "(فوق الأرض فقط)."
    )

    contingency_pct = float(inputs.get("contingency_pct") or 0.0)
    contingency_cost = float(breakdown.get("contingency_cost", 0.0) or 0.0)
    explanations_en["contingency"] = (
        f"{contingency_pct * 100:.1f}% × (direct {_fmt_amount(direct_total)} SAR + "
        f"fit-out {_fmt_amount(fitout_cost)} SAR) = {_fmt_amount(contingency_cost)} SAR. "
        "Allowance for execution risk."
    )
    explanations_ar["contingency"] = (
        f"{contingency_pct * 100:.1f}% × (الإنشاء المباشر {_fmt_amount(direct_total)} SAR + "
        f"التشطيبات {_fmt_amount(fitout_cost)} SAR) = {_fmt_amount(contingency_cost)} SAR. "
        "بدل لمخاطر التنفيذ."
    )

    consultants_pct = float(inputs.get("consultants_pct") or 0.0)
    consultants_base = sub_total + contingency_cost
    explanations_en["consultants"] = (
        f"{consultants_pct * 100:.1f}% × (construction + contingency) {_fmt_amount(consultants_base)} SAR "
        f"= {_fmt_amount(float(breakdown.get('consultants_cost', 0.0) or 0.0))} SAR."
    )
    explanations_ar["consultants"] = (
        f"{consultants_pct * 100:.1f}% × (الإنشاء + الاحتياطي) {_fmt_amount(consultants_base)} SAR "
        f"= {_fmt_amount(float(breakdown.get('consultants_cost', 0.0) or 0.0))} SAR."
    )

    transaction_pct = float(inputs.get("transaction_pct") or 0.0)
    explanations_en["transaction_cost"] = (
        f"{transaction_pct * 100:.1f}% × {_fmt_amount(land_cost_total)} SAR = "
        f"{_fmt_amount(float(breakdown.get('transaction_cost', 0.0) or 0.0))} SAR. "
        "Per Saudi RETT."
    )
    explanations_ar["transaction_cost"] = (
        f"{transaction_pct * 100:.1f}% × {_fmt_amount(land_cost_total)} SAR = "
        f"{_fmt_amount(float(breakdown.get('transaction_cost', 0.0) or 0.0))} SAR. "
        "وفق أنظمة ضريبة التصرفات العقارية."
    )

    income_parts = []
    income_parts_ar = []
    parking_income_meta = breakdown.get("parking_income_meta") or {}
    parking_income_component = None
    rent_meta = inputs.get("rent_source_metadata") or {}
    rent_components_meta = rent_meta.get("components") if isinstance(rent_meta, dict) else {}
    for key, component in y1_income_components.items():
        if key == "parking_income":
            parking_income_component = component
            continue
        nla_val = float(nla.get(key, 0.0) or 0.0)
        base_rent = float(rent_rates.get(key, 0.0) or 0.0)
        applied_rent = float(rent_applied.get(key, 0.0) or 0.0)
        effective_rent = applied_rent if applied_rent > 0 else base_rent * re_scalar
        rent_used = (component / nla_val) if nla_val else effective_rent
        comp_meta = rent_components_meta.get(key) if isinstance(rent_components_meta, dict) else {}
        # More precise label when a component fell back to template defaults
        rent_label = "template default"
        if isinstance(comp_meta, dict):
            rent_label = comp_meta.get("method") or comp_meta.get("provider") or rent_label
        if rent_label == "template default" and isinstance(rent_meta, dict):
            rent_label = (
                rent_meta.get("method")
                or rent_meta.get("provider")
                or rent_label
            )
        rent_label_ar = "القالب الافتراضي" if rent_label == "template default" else rent_label
        note = ""
        note_ar = ""
        if applied_rent and abs(applied_rent - base_rent) > 1e-9 and re_scalar not in (0.0, 1.0):
            note = f" (includes real estate price index scalar {re_scalar:,.3f})"
            note_ar = f" (يشمل معامل مؤشر أسعار العقارات {re_scalar:,.3f})"
        income_parts.append(
            f"{key} NLA {_fmt_amount(nla_val, decimals=2)} m² × {rent_used:,.0f} SAR/m²/yr "
            f"= {_fmt_amount(component)} SAR/yr. Benchmark: {rent_label}.{note}"
        )
        income_parts_ar.append(
            f"{key} المساحة القابلة للتأجير {_fmt_amount(nla_val, decimals=2)} م² × {rent_used:,.0f} SAR/م²/سنة "
            f"= {_fmt_amount(component)} SAR/سنة. المعيار: {rent_label_ar}.{note_ar}"
        )
    if parking_income_component:
        extra_spaces = int(parking_income_meta.get("extra_spaces") or 0)
        monthly_rate_used = float(parking_income_meta.get("monthly_rate_used") or 0.0)
        occupancy_used = float(parking_income_meta.get("occupancy_used") or 0.0)
        rate_note = str(parking_income_meta.get("rate_note") or "").strip()
        parking_line = (
            f"Parking: {extra_spaces} excess spaces × {monthly_rate_used:,.0f} SAR/space/mo "
            f"× {occupancy_used:.2f} occupancy = {_fmt_amount(parking_income_component)} SAR/yr."
        )
        parking_line_ar = (
            f"مواقف: {extra_spaces} مواقف إضافية × {monthly_rate_used:,.0f} SAR/موقف/شهر "
            f"× إشغال {occupancy_used:.2f} = {_fmt_amount(parking_income_component)} SAR/سنة."
        )
        if rate_note:
            parking_line = f"{parking_line} {rate_note}"
            parking_line_ar = f"{parking_line_ar} {rate_note}"
        # Row-level explanation hook for the UI "How we calculated it" column.
        # The combined y1_income explanation remains as an overall summary, but the UI can
        # now also display parking math directly on the "parking income" row.
        explanations_en["parking_income"] = parking_line
        explanations_ar["parking_income"] = parking_line_ar
        income_parts.append(parking_line)
        income_parts_ar.append(parking_line_ar)
    if income_parts:
        explanations_en["y1_income"] = "; ".join(income_parts)
    if income_parts_ar:
        explanations_ar["y1_income"] = "؛ ".join(income_parts_ar)

    y1_income_total = float(breakdown.get("y1_income", 0.0) or 0.0)
    y1_income_effective_factor = _normalize_y1_income_effective_factor(
        breakdown.get("y1_income_effective_factor")
    )
    y1_income_effective = float(
        breakdown.get("y1_income_effective", 0.0) or (y1_income_total * y1_income_effective_factor)
    )
    opex_pct_raw = breakdown.get("opex_pct", 0.05)
    try:
        opex_pct = float(opex_pct_raw or 0.0)
    except Exception:
        opex_pct = 0.0
    opex_pct = max(0.0, min(opex_pct, 1.0))
    opex_cost = float(breakdown.get("opex_cost", y1_income_effective * opex_pct) or 0.0)
    y1_noi = float(breakdown.get("y1_noi", y1_income_effective - opex_cost) or 0.0)
    explanations_en["y1_income_effective"] = (
        f"annual net income = {y1_income_total:,.0f} SAR × {y1_income_effective_factor*100:.0f}% "
        f"= {y1_income_effective:,.0f} SAR."
    )
    explanations_ar["y1_income_effective"] = (
        f"الدخل الفعّال للسنة الأولى = {y1_income_total:,.0f} SAR × {y1_income_effective_factor*100:.0f}% "
        f"= {y1_income_effective:,.0f} SAR."
    )
    explanations_en["opex_cost"] = f"OPEX = {opex_pct*100:.0f}% × annual net income = {opex_cost:,.0f} SAR."
    explanations_ar["opex_cost"] = f"المصاريف التشغيلية = {opex_pct*100:.0f}% × الدخل الفعّال = {opex_cost:,.0f} SAR."
    explanations_en["y1_noi"] = "Year-1 NOI = annual net income − OPEX."
    explanations_ar["y1_noi"] = "صافي الدخل التشغيلي للسنة الأولى = الدخل الفعّال − المصاريف التشغيلية."
    grand_total_capex = float(breakdown.get("grand_total_capex", 0.0) or 0.0)
    roi = float(breakdown.get("roi", 0.0) or 0.0)
    explanations_en["roi"] = (
        f"Year-1 NOI {_fmt_amount(y1_noi)} SAR ÷ total development cost "
        f"{_fmt_amount(grand_total_capex)} SAR = {roi * 100:,.2f}%."
    )
    explanations_ar["roi"] = (
        f"صافي الدخل التشغيلي للسنة الأولى {_fmt_amount(y1_noi)} SAR ÷ إجمالي تكلفة التطوير "
        f"{_fmt_amount(grand_total_capex)} SAR = {roi * 100:,.2f}%."
    )

    return explanations_en, explanations_ar


def _build_cost_breakdown_rows(
    built_area: Dict[str, float],
    area_ratio: Dict[str, Any],
    direct_cost: Dict[str, float],
    inputs: Dict[str, Any],
    explanations: Dict[str, str],
    *,
    far_above_ground: float | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def _norm_land_use() -> str:
        lu = (inputs.get("land_use_code") or inputs.get("land_use") or "").strip().lower()
        if lu:
            return lu
        keys = {str(k).strip().lower() for k in area_ratio.keys()}
        if {"retail", "office"} & keys:
            return "m"
        return ""

    land_use = _norm_land_use()

    try:
        if far_above_ground is not None:
            far_value = float(far_above_ground)
            rows.append(
                {
                    "category": "info",
                    "key": "effective_far_above_ground",
                    "label": "Effective FAR (above-ground)",
                    "unit": None,
                    "value": far_value,
                    "note": explanations.get("effective_far_above_ground"),
                }
            )
    except Exception:
        pass

    def _append_bua_row(key: str, label: str, *, require_mixed_use: bool = False) -> None:
        if require_mixed_use and land_use != "m":
            return
        if key in {"retail", "office"}:
            if key not in area_ratio or built_area.get(key, 0.0) <= 0:
                return
            # Retail and Office BUA are surfaced for mixed-use clarity; values already existed but were previously implicit.
        value = built_area.get(key)
        if value is None:
            return
        rows.append(
            {
                "category": "cost",
                "key": f"{key}_bua",
                "label": label,
                "unit": "m²",
                "value": value,
                "note": explanations.get(f"{key}_bua"),
            }
        )

    _append_bua_row("residential", "Residential BUA")
    _append_bua_row("retail", "Retail BUA", require_mixed_use=True)
    _append_bua_row("office", "Office BUA", require_mixed_use=True)
    _append_bua_row("basement", "Basement BUA")
    try:
        if land_use == "m":
            upper_annex_area = float(built_area.get("upper_annex_non_far") or 0.0)
            if upper_annex_area > 0:
                rows.append(
                    {
                        "category": "cost",
                        "key": "upper_annex_non_far_bua",
                        "label": "Upper annex (non-FAR, +0.5 floor)",
                        "unit": "m²",
                        "value": upper_annex_area,
                        "note": explanations.get("upper_annex_non_far_bua"),
                    }
                )
                rows.append(
                    {
                        "category": "cost",
                        "key": "upper_annex_non_far_cost",
                        "label": "Upper annex construction cost (non-FAR)",
                        "unit": "SAR",
                        "value": float(direct_cost.get("upper_annex_non_far", 0.0) or 0.0),
                        "note": explanations.get("upper_annex_non_far_cost"),
                    }
                )
    except Exception:
        pass

    return rows


def compute_excel_estimate(site_area_m2: float, inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Compute an Excel-style estimate using caller-provided parameters."""

    resolved_inputs = dict(inputs or {})
    area_ratio = resolved_inputs.get("area_ratio", {}) or {}
    unit_cost_raw = resolved_inputs.get("unit_cost", {}) or {}
    unit_cost = dict(unit_cost_raw) if isinstance(unit_cost_raw, dict) else {}
    resolved_inputs["unit_cost"] = unit_cost
    cp_density = resolved_inputs.get("cp_sqm_per_space", {}) or {}
    efficiency = resolved_inputs.get("efficiency", {}) or {}
    rent_rates = resolved_inputs.get("rent_sar_m2_yr", {}) or {}
    re_scalar = float(resolved_inputs.get("re_price_index_scalar") or 1.0)
    revenue_meta: dict[str, Any] = {}

    built_area = {key: float(area_ratio.get(key, 0.0)) * float(site_area_m2) for key in area_ratio.keys()}
    built_area_canon = _build_area_by_canon_key(area_ratio, site_area_m2)
    shell_unit = (unit_cost.get("residential") or 0.0)
    basement_unit = (unit_cost.get("basement") or 0.0)
    direct_cost = {}
    resolved_unit_cost: Dict[str, float] = {}
    for key in area_ratio.keys():
        if _is_non_far_area_key(str(key)):
            raw = unit_cost.get(key)
            try:
                parsed = float(raw) if raw is not None else 0.0
            except Exception:
                parsed = 0.0
            if parsed <= 0:
                parsed = DEFAULT_NON_FAR_UNIT_COST
                unit_cost[key] = parsed
            unit_rate = float(parsed)
        else:
            unit_rate = float(unit_cost.get(key, 0.0))
        if key == "residential":
            unit_rate = float(shell_unit)
        elif key.lower().startswith("basement"):
            unit_rate = float(basement_unit)
        elif _is_non_far_area_key(str(key)):
            pass
        resolved_unit_cost[str(key)] = unit_rate
        direct_cost[key] = built_area.get(key, 0.0) * unit_rate

    # --- Parking (required + provided) ---
    # parking_required_* can be overridden upstream (e.g., using Riyadh municipal minimums).
    parking_required_by_component: Dict[str, int] = {}
    parking_required_by_component_override = resolved_inputs.get("parking_required_by_component_override")
    if isinstance(parking_required_by_component_override, dict):
        for k, v in parking_required_by_component_override.items():
            try:
                parking_required_by_component[str(k)] = int(float(v or 0.0))
            except Exception:
                parking_required_by_component[str(k)] = 0
    else:
        # Fallback: interpret cp_sqm_per_space as "m² GFA per required space" for above-ground components.
        for key in area_ratio.keys():
            if isinstance(key, str):
                kk = key.strip().lower()
                if _is_basement_area_ratio_key(kk) or ("parking" in kk) or ("carpark" in kk) or ("car_park" in kk):
                    continue
                if _is_non_far_area_key(kk):
                    continue
            cp = float(cp_density.get(key, 0.0) or 0.0)
            if cp > 0:
                raw = float(built_area.get(key, 0.0) or 0.0) / cp
                parking_required_by_component[str(key)] = int(math.ceil(raw - 1e-9))
            else:
                parking_required_by_component[str(key)] = 0

    parking_required_spaces_override = resolved_inputs.get("parking_required_spaces_override")
    if parking_required_spaces_override is not None:
        try:
            parking_required_spaces_raw = float(parking_required_spaces_override)
        except Exception:
            parking_required_spaces_raw = float(sum(parking_required_by_component.values()))
    else:
        parking_required_spaces_raw = float(sum(parking_required_by_component.values()))
    parking_required_spaces = int(math.ceil(parking_required_spaces_raw - 1e-9))

    # Parking supply: derive provided stalls from below-grade + explicit parking areas.
    parking_supply_gross_m2_per_space = float(
        resolved_inputs.get("parking_supply_gross_m2_per_space") or DEFAULT_PARKING_SUPPLY_GROSS_M2_PER_SPACE
    )
    parking_supply_layout_efficiency = float(
        resolved_inputs.get("parking_supply_layout_efficiency") or DEFAULT_PARKING_SUPPLY_LAYOUT_EFFICIENCY
    )
    if parking_supply_layout_efficiency <= 0:
        parking_supply_layout_efficiency = DEFAULT_PARKING_SUPPLY_LAYOUT_EFFICIENCY
    parking_area_m2 = 0.0
    parking_area_by_key: Dict[str, float] = {}
    for key, area in built_area.items():
        if not isinstance(key, str):
            continue
        kk = key.strip().lower()
        if _is_basement_area_ratio_key(kk) or ("parking" in kk) or ("carpark" in kk) or ("car_park" in kk):
            a = float(area or 0.0)
            if a > 0:
                parking_area_by_key[key] = a
                parking_area_m2 += a
    if parking_supply_layout_efficiency > 1.0:
        effective_gross_m2_per_space = parking_supply_layout_efficiency
    else:
        parking_supply_layout_efficiency = max(0.0, min(parking_supply_layout_efficiency, 1.0))
        effective_gross_m2_per_space = (
            parking_supply_gross_m2_per_space / parking_supply_layout_efficiency
            if parking_supply_layout_efficiency > 0
            else 0.0
        )
    if effective_gross_m2_per_space > 0:
        parking_provided_raw = parking_area_m2 / effective_gross_m2_per_space
    else:
        parking_provided_raw = 0.0
    parking_provided_spaces = int(math.floor(parking_provided_raw + 1e-9))
    parking_deficit_spaces = max(0, parking_required_spaces - parking_provided_spaces)
    parking_compliant = parking_deficit_spaces == 0
    parking_extra_spaces = max(0, parking_provided_spaces - parking_required_spaces)

    fitout_area = sum(
        value for key, value in built_area.items() if not key.lower().startswith("basement")
    )
    fitout_rate = float(resolved_inputs.get("fitout_rate") or 0.0)
    fitout_cost = fitout_area * fitout_rate

    sub_total = sum(direct_cost.values()) + fitout_cost
    contingency_cost = sub_total * float(resolved_inputs.get("contingency_pct", 0.0))
    consultants_cost = (sub_total + contingency_cost) * float(resolved_inputs.get("consultants_pct", 0.0))
    land_cost = float(site_area_m2) * float(resolved_inputs.get("land_price_sar_m2", 0.0))
    feasibility_fee_pct = float(resolved_inputs.get("feasibility_fee_pct", 0.02) or 0.0)
    feasibility_fee = land_cost * feasibility_fee_pct
    transaction_cost = land_cost * float(resolved_inputs.get("transaction_pct", 0.0))

    grand_total_capex = (
        sub_total
        + contingency_cost
        + consultants_cost
        + feasibility_fee
        + land_cost
        + transaction_cost
    )

    # --- Revenue-only reallocation for upper annex (non-FAR) ---
    #
    # Keep construction/cost accounting as-is (built_area / direct_cost).
    # But for revenue, treat the upper annex as part of:
    #   residential (preferred), else office, and never retail.
    built_area_for_revenue_canon = built_area_canon
    try:
        # Identify upper annex in a way that cannot be "lost" by canonical aggregation.
        #
        # IMPORTANT:
        # - `built_area` is keyed by the original area_ratio keys and always includes the annex key if present.
        # - `built_area_canon` may omit non-FAR keys depending on canonicalization logic elsewhere.
        #
        upper_annex_raw_key: str | None = None
        for k in (area_ratio or {}).keys():
            kk = str(k).strip().lower()
            if not kk:
                continue
            # be generous: any "upper"+"annex" pattern counts (non_far often present, but don't require it)
            if ("upper" in kk and "annex" in kk) or (kk == "upper_annex_non_far"):
                upper_annex_raw_key = str(k)
                break

        # Area from raw built_area (robust)
        upper_annex_area = float(built_area.get(upper_annex_raw_key, 0.0) or 0.0) if upper_annex_raw_key else 0.0

        # Canonical key name we will use in the revenue-only canon map
        upper_annex_ck = "upper_annex_non_far" if upper_annex_area > 1e-9 else None

        if upper_annex_area > 1e-9:
            sink = _choose_upper_annex_revenue_sink(area_ratio)
            # Never flow to retail (even if only retail exists).
            # Also avoid self-flow (defensive).
            if sink and sink != "retail" and sink != upper_annex_ck:
                built_area_for_revenue_canon = copy.deepcopy(built_area_canon)
                built_area_for_revenue_canon[sink] = (
                    float(built_area_for_revenue_canon.get(sink, 0.0) or 0.0) + upper_annex_area
                )
                # Ensure annex isn't double-counted in revenue even if canon map didn't originally have it.
                built_area_for_revenue_canon[upper_annex_ck] = 0.0
                revenue_meta["upper_annex_flow"] = {
                    "from_key": upper_annex_raw_key,
                    "to_key": sink,
                    "area_m2": upper_annex_area,
                    "rule": "upper annex revenue -> residential else office; never retail",
                }
            else:
                revenue_meta["upper_annex_flow"] = {
                    "from_key": upper_annex_raw_key,
                    "to_key": None,
                    "area_m2": upper_annex_area,
                    "rule": "no residential/office sink found; kept as-is (no retail fallback)",
                }
    except Exception as exc:
        revenue_meta["upper_annex_flow_error"] = str(exc)

    # IMPORTANT:
    # Compute revenue using CANONICAL keys, then expose a stable `nla` keyed by canonical component.
    # This prevents key-shape mismatches (e.g., UI keys vs template keys) from nullifying the annex flow.
    nla_canon: dict[str, float] = {}
    for ck, area_m2 in (built_area_for_revenue_canon or {}).items():
        eff = _lookup_by_canon(efficiency, ck, 0.0)
        nla_canon[ck] = float(area_m2 or 0.0) * float(eff or 0.0)

    # Back-compat: also provide `nla` under canonical names used by the UI ("residential", "retail", "office", etc.)
    nla: dict[str, float] = dict(nla_canon)

    rent_applied: dict[str, float] = {}
    # Apply rent using canonical keys too
    rent_keys: set[str] = set()
    if isinstance(rent_rates, dict):
        rent_keys |= {_canon_key(k) for k in rent_rates.keys()}
    if isinstance(built_area_for_revenue_canon, dict):
        rent_keys |= set(built_area_for_revenue_canon.keys())
    rent_keys.discard(None)
    for ck in rent_keys:
        rent_applied[ck] = _lookup_by_canon(rent_rates, ck, 0.0) * re_scalar

    # Revenue by canonical component
    y1_income_components = {
        ck: float(nla.get(ck, 0.0) or 0.0) * float(rent_applied.get(ck, 0.0) or 0.0)
        for ck in (built_area_for_revenue_canon or {}).keys()
    }
    base_y1_income = sum(y1_income_components.values())

    # Optional: surface revenue_meta if your function already returns a meta dict.
    # If you already have a meta/notes structure, merge it there instead of creating a new field.
    try:
        existing_meta = resolved_inputs.get("meta")
        if isinstance(existing_meta, dict):
            existing_meta = {**existing_meta, "revenue": {**existing_meta.get("revenue", {}), **revenue_meta}}
            resolved_inputs["meta"] = existing_meta
        else:
            resolved_inputs["meta"] = {"revenue": revenue_meta}
    except Exception:
        pass

    # Debug visibility (safe): expose what happened so the UI / memo can confirm the flow.
    # This does NOT affect calculations.
    try:
        resolved_inputs.setdefault("meta", {})
        if isinstance(resolved_inputs["meta"], dict):
            resolved_inputs["meta"].setdefault("revenue", {})
            if isinstance(resolved_inputs["meta"]["revenue"], dict):
                resolved_inputs["meta"]["revenue"].update(revenue_meta)
                resolved_inputs["meta"]["revenue"]["built_area_for_revenue_canon"] = dict(built_area_for_revenue_canon or {})
                resolved_inputs["meta"]["revenue"]["nla_canon"] = dict(nla_canon or {})
                resolved_inputs["meta"]["revenue"]["rent_applied_canon"] = dict(rent_applied or {})
    except Exception:
        pass

    def _infer_parking_landuse_code() -> str | None:
        explicit_keys = ("land_use_code", "landuse_code", "land_use", "landuse")
        for k in explicit_keys:
            code = _normalize_landuse_code(resolved_inputs.get(k))
            if code:
                return code
        keys = {str(k).strip().lower() for k in area_ratio.keys()}
        has_res = any(k.startswith("res") or k == "residential" for k in keys)
        has_commercial = any(k in {"retail", "office", "commercial"} for k in keys)
        if has_res and not has_commercial:
            return "s"
        if has_res and has_commercial:
            return "m"
        if has_commercial and not has_res:
            return "c"
        return None

    parking_landuse_code = _infer_parking_landuse_code()

    monetize_raw = _parse_bool(resolved_inputs.get("monetize_extra_parking"))
    # Frontend currently doesn't expose this; default to ON so extra spaces contribute revenue.
    monetize_extra_parking = True if monetize_raw is None else bool(monetize_raw)
    monetize_defaulted = monetize_raw is None

    public_raw = _parse_bool(resolved_inputs.get("parking_public_access"))
    if public_raw is None:
        # Default: commercial/mixed projects can plausibly monetize public parking;
        # residential is typically private/assigned.
        parking_public_access = parking_landuse_code in {"m", "c"}
        public_defaulted = True
    else:
        parking_public_access = bool(public_raw)
        public_defaulted = False

    override_rate_raw = resolved_inputs.get("parking_monthly_rate_sar_per_space")
    try:
        override_rate = float(override_rate_raw) if override_rate_raw is not None else None
    except Exception:
        override_rate = None
    occupancy_override_raw = resolved_inputs.get("parking_occupancy")
    try:
        occupancy_override = float(occupancy_override_raw) if occupancy_override_raw is not None else None
    except Exception:
        occupancy_override = None

    parking_income_y1, parking_income_meta = compute_parking_income(
        parking_extra_spaces,
        monetize=monetize_extra_parking,
        landuse_code=parking_landuse_code,
        land_price_sar_m2=float(resolved_inputs.get("land_price_sar_m2", 0.0) or 0.0),
        public_access=parking_public_access,
        override_rate=override_rate,
        occupancy_override=occupancy_override,
    )

    # Enrich meta for transparency (frontend doesn't yet send these toggles).
    if isinstance(parking_income_meta, dict):
        parking_income_meta = dict(parking_income_meta)
        parking_income_meta.setdefault("monetize_extra_parking_defaulted", monetize_defaulted)
        parking_income_meta.setdefault("parking_public_access_defaulted", public_defaulted)
        parking_income_meta.setdefault("landuse_code_inferred", parking_landuse_code)

    if parking_income_y1 > 0:
        y1_income_components["parking_income"] = parking_income_y1
    y1_income = base_y1_income + parking_income_y1

    # Apply accounting efficiency haircut for ROI headline (user-adjustable)
    y1_income_effective_factor = _normalize_y1_income_effective_factor(
        resolved_inputs.get("y1_income_effective_pct")
        if resolved_inputs.get("y1_income_effective_pct") is not None
        else resolved_inputs.get("y1_income_effective_factor")
    )
    y1_income_effective = float(y1_income) * y1_income_effective_factor
    opex_pct_raw = resolved_inputs.get("opex_pct", 0.05)
    try:
        opex_pct = float(opex_pct_raw or 0.0)
    except Exception:
        opex_pct = 0.0
    opex_pct = max(0.0, min(opex_pct, 1.0))
    opex_cost = y1_income_effective * opex_pct
    y1_noi = y1_income_effective - opex_cost
    parking_monthly_rate_used = float(parking_income_meta.get("monthly_rate_used") or 0.0)
    parking_occupancy_used = float(parking_income_meta.get("occupancy_used") or 0.0)

    roi = (y1_noi / grand_total_capex) if grand_total_capex > 0 else 0.0

    far_above_ground = _area_ratio_positive_sum(
        area_ratio,
        exclude_basement=True,
        exclude_non_far=True,
    )
    far_total_including_basement = _area_ratio_positive_sum(area_ratio, exclude_basement=False)

    result = {
        "built_area": built_area,
        "direct_cost": direct_cost,
        "unit_cost_resolved": resolved_unit_cost,
        "fitout_cost": fitout_cost,
        "parking_required_spaces": parking_required_spaces,
        "parking_required_spaces_raw": parking_required_spaces_raw,
        "parking_required_by_component": parking_required_by_component,
        "parking_provided_spaces": parking_provided_spaces,
        "parking_provided_spaces_raw": parking_provided_raw,
        "parking_deficit_spaces": parking_deficit_spaces,
        "parking_extra_spaces": parking_extra_spaces,
        "parking_compliant": parking_compliant,
        "parking_area_m2": parking_area_m2,
        "parking_area_by_key": parking_area_by_key,
        "parking_supply_gross_m2_per_space": parking_supply_gross_m2_per_space,
        "parking_supply_layout_efficiency": parking_supply_layout_efficiency,
        "sub_total": sub_total,
        "contingency_cost": contingency_cost,
        "consultants_cost": consultants_cost,
        "feasibility_fee": feasibility_fee,
        "feasibility_fee_pct": feasibility_fee_pct,
        "land_cost": land_cost,
        "transaction_cost": transaction_cost,
        "grand_total_capex": grand_total_capex,
        "nla": nla,
        "y1_income_components": y1_income_components,
        "y1_income": y1_income,
        "y1_income_effective": y1_income_effective,
        "y1_income_effective_factor": y1_income_effective_factor,
        "opex_pct": opex_pct,
        "opex_cost": opex_cost,
        "y1_noi": y1_noi,
        "rent_applied_sar_m2_yr": rent_applied,
        "parking_income_y1": parking_income_y1,
        "parking_income_meta": parking_income_meta,
        "parking_monthly_rate_used": parking_monthly_rate_used,
        "parking_occupancy_used": parking_occupancy_used,
        "roi": roi,
        "far_above_ground": far_above_ground,
        "far_total_including_basement": far_total_including_basement,
    }

    try:
        result["non_far_area_ratio"] = {
            k: float(v)
            for k, v in area_ratio.items()
            if isinstance(k, str) and _is_non_far_area_key(k)
        }
    except Exception:
        pass

    explanations_en, explanations_ar = build_excel_explanations(site_area_m2, resolved_inputs, result)
    result["explanations"] = explanations_en
    result["explanations_en"] = explanations_en
    result["explanations_ar"] = explanations_ar
    result["cost_breakdown_rows"] = _build_cost_breakdown_rows(
        built_area,
        area_ratio,
        direct_cost,
        resolved_inputs,
        explanations_en,
        far_above_ground=far_above_ground,
    )

    return result
