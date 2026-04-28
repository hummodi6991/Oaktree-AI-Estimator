"""LLM-powered decision memo generator for Expansion Advisor candidates.

Generates a 200-300 word narrative on click of "Decision Memo", explaining
whether a candidate site is a fit for the operator's specific brand brief.

Uses the same OpenAI client pattern as llm_suitability.py. Model and cost
ceiling are configurable via environment variables.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from app.core.config import settings

logger = logging.getLogger(__name__)

# ── Model & cost configuration ──────────────────────────────────────

MODEL_ID = os.environ.get("DECISION_MEMO_MODEL", "gpt-4o-mini-2024-07-18")
MAX_TOKENS = 800
TEMPERATURE = 0.3

# Bumped whenever STRUCTURED_MEMO_SYSTEM_PROMPT changes meaningfully.
# Cached memos with a different version are treated as cache-miss and
# regenerated lazily on next view.
MEMO_PROMPT_VERSION = "v4.2-advisor-2026-04"

# Soft daily ceiling in USD.  Raises RuntimeError before calling OpenAI
# if the running total for today exceeds this value.
DAILY_CEILING_USD = float(
    os.environ.get("DECISION_MEMO_DAILY_CEILING_USD", "5.00")
)

# Per-token costs for gpt-4o-mini (as of 2024-07).  Used for cost
# tracking only — not billing.
_INPUT_COST_PER_TOKEN = 0.15 / 1_000_000   # $0.15 per 1M input tokens
_OUTPUT_COST_PER_TOKEN = 0.60 / 1_000_000  # $0.60 per 1M output tokens

# ── Daily cost tracker (in-process, resets on restart) ──────────────

_daily_cost_tracker: dict[str, float] = {}


def _today_key() -> str:
    return date.today().isoformat()


def _check_daily_ceiling() -> None:
    today = _today_key()
    spent = _daily_cost_tracker.get(today, 0.0)
    if spent >= DAILY_CEILING_USD:
        raise RuntimeError(
            f"Decision memo daily cost ceiling reached "
            f"(${spent:.4f} / ${DAILY_CEILING_USD:.2f}). "
            f"Try again tomorrow or raise DECISION_MEMO_DAILY_CEILING_USD."
        )


def _record_cost(input_tokens: int, output_tokens: int) -> float:
    cost = (input_tokens * _INPUT_COST_PER_TOKEN
            + output_tokens * _OUTPUT_COST_PER_TOKEN)
    today = _today_key()
    _daily_cost_tracker[today] = _daily_cost_tracker.get(today, 0.0) + cost
    return cost


# ── OpenAI client (lazy) ────────────────────────────────────────────

_client = None


def _get_client():
    global _client
    if _client is None:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError(
                "OPENAI_API_KEY is not set. Decision memo generation "
                "requires an OpenAI API key."
            )
        from openai import OpenAI
        _client = OpenAI(api_key=api_key)
    return _client


# ── Rent-vs-median helper ───────────────────────────────────────────

def _format_rent_vs_median(
    candidate_rent: float | None,
    district_median: float | None,
    lang: str = "en",
) -> str:
    if candidate_rent is None or district_median is None or district_median <= 0:
        return "غير معروف" if lang == "ar" else "unknown"

    pct = ((candidate_rent - district_median) / district_median) * 100

    if abs(pct) < 5:
        return "متوافق مع المتوسط" if lang == "ar" else "in line with median"
    elif pct > 0:
        rounded = round(pct)
        if lang == "ar":
            return f"أعلى من المتوسط بنسبة {rounded}%"
        return f"{rounded}% above median"
    else:
        rounded = round(abs(pct))
        if lang == "ar":
            return f"أقل من المتوسط بنسبة {rounded}%"
        return f"{rounded}% below median"


# ── Prompt templates ────────────────────────────────────────────────
#
# Legacy fallback prompt — intentionally narrower than
# STRUCTURED_MEMO_SYSTEM_PROMPT and only used when the structured-output
# parse fails or EXPANSION_MEMO_STRUCTURED_ENABLED is off. Do not extend
# it with new advisory-report directives; the structured prompt is the
# canonical path for advisor-grade memos.

_PROMPT_TEMPLATE_EN = """You are a real estate advisor evaluating a commercial site in Riyadh, Saudi Arabia for a specific restaurant/retail operator.

OPERATOR'S BRAND BRIEF:
- Brand: {brand_name}
- Category: {category}
- Service model: {service_model}
- Target area: {target_area} m²  (acceptable range: {min_area}–{max_area} m²)
- Existing branches: {existing_branches}
- Primary channel: {primary_channel}

CANDIDATE SITE:
- District: {district}
- Area: {area} m²
- Annual rent: SAR {annual_rent}
- Rent per m²/year: SAR {rent_per_sqm}
- Street width: {street_width} m
- Final score: {final_score}/100
- Economics score: {economics_score}/100
- Brand fit score: {brand_fit_score}/100
- Demand score: {demand_score}/100
- Whitespace score: {whitespace_score}/100
- Listing quality score: {listing_quality_score}/100
- District median rent: SAR {district_median_rent}
- Rent vs median: {rent_vs_median}
- LLM site assessment: {llm_reasoning}

Return a JSON object with exactly these fields:
{{
  "headline": "<≤15 words: GO/CONSIDER/CAUTION verdict + key reason>",
  "fit_summary": "<2-3 sentences explaining fit for {brand_name} as a {category} operator. Reference the brand and category by name.>",
  "top_reasons_to_pursue": ["<reason 1>", "<reason 2>", "<reason 3>"],
  "top_risks": ["<risk 1>", "<risk 2>", "<risk 3>"],
  "recommended_next_action": "<1 concrete, actionable next step>",
  "rent_context": "<1 sentence comparing this site's rent to the district median>"
}}

RULES:
- Do not invent facts. If you don't know something, omit it or say "data not available".
- Reference the operator's brand name ({brand_name}) and category ({category}) explicitly.
- Keep the total response under 300 words.
- Return ONLY the JSON object."""

_PROMPT_TEMPLATE_AR = """أنت مستشار عقاري تُقيّم موقعاً تجارياً في الرياض، المملكة العربية السعودية لصالح مُشغّل مطاعم/تجزئة محدد.

موجز العلامة التجارية للمشغّل:
- العلامة التجارية: {brand_name}
- الفئة: {category}
- نموذج الخدمة: {service_model}
- المساحة المستهدفة: {target_area} م² (النطاق المقبول: {min_area}–{max_area} م²)
- الفروع الحالية: {existing_branches}
- القناة الرئيسية: {primary_channel}

الموقع المرشح:
- الحي: {district}
- المساحة: {area} م²
- الإيجار السنوي: {annual_rent} ريال
- الإيجار لكل م²/سنة: {rent_per_sqm} ريال
- عرض الشارع: {street_width} م
- الدرجة النهائية: {final_score}/100
- درجة الجدوى: {economics_score}/100
- درجة ملاءمة العلامة: {brand_fit_score}/100
- درجة الطلب: {demand_score}/100
- درجة الفراغ السوقي: {whitespace_score}/100
- درجة جودة الإعلان: {listing_quality_score}/100
- متوسط إيجار الحي: {district_median_rent} ريال
- الإيجار مقارنة بالمتوسط: {rent_vs_median}
- تقييم الموقع: {llm_reasoning}

أعد كائن JSON يحتوي على هذه الحقول بالضبط:
{{
  "headline": "<≤15 كلمة: حكم انطلق/تأمّل/احذر + السبب الرئيسي>",
  "fit_summary": "<2-3 جمل توضح مدى ملاءمة الموقع لـ {brand_name} كمُشغّل {category}. اذكر العلامة التجارية والفئة بالاسم.>",
  "top_reasons_to_pursue": ["<سبب 1>", "<سبب 2>", "<سبب 3>"],
  "top_risks": ["<خطر 1>", "<خطر 2>", "<خطر 3>"],
  "recommended_next_action": "<خطوة عملية واحدة قابلة للتنفيذ>",
  "rent_context": "<جملة واحدة تقارن إيجار هذا الموقع بمتوسط الحي>"
}}

القواعد:
- لا تختلق حقائق. إذا لم تعرف شيئاً، احذفه أو قل "البيانات غير متوفرة".
- اذكر اسم العلامة التجارية ({brand_name}) والفئة ({category}) صراحةً.
- اجعل الرد أقل من 300 كلمة.
- أعد كائن JSON فقط."""


# ── Main generation function ────────────────────────────────────────

_REQUIRED_STRING_FIELDS = (
    "headline", "fit_summary", "recommended_next_action", "rent_context"
)
_REQUIRED_LIST_FIELDS = ("top_reasons_to_pursue", "top_risks")


def generate_decision_memo(
    *,
    candidate: dict[str, Any],
    brief: dict[str, Any],
    lang: str = "en",
) -> dict[str, Any]:
    """Generate an LLM decision memo for a candidate site.

    Args:
        candidate: The candidate dict (from frontend state / API response).
        brief: The brand brief dict (from frontend state / API response).
        lang: "en" or "ar" — controls prompt language.

    Returns:
        Dict with headline, fit_summary, top_reasons_to_pursue,
        top_risks, recommended_next_action, rent_context.

    Raises:
        RuntimeError: If daily cost ceiling is exceeded or API call fails.
    """
    _check_daily_ceiling()

    template = _PROMPT_TEMPLATE_AR if lang == "ar" else _PROMPT_TEMPLATE_EN

    # Extract fields with safe fallbacks
    existing_branches = brief.get("existing_branches") or []
    if isinstance(existing_branches, list):
        branch_desc = (
            f"{len(existing_branches)} branches"
            if lang == "en"
            else f"{len(existing_branches)} فروع"
        )
    else:
        branch_desc = str(existing_branches)

    district_median_rent = candidate.get("district_median_rent")
    annual_rent = candidate.get(
        "display_annual_rent_sar",
        candidate.get("estimated_annual_rent_sar"),
    )
    rent_vs_median = _format_rent_vs_median(
        annual_rent, district_median_rent, lang
    )

    area = candidate.get("area_m2") or candidate.get("unit_area_sqm")
    rent_per_sqm = candidate.get("estimated_rent_sar_m2_year")
    street_width = (
        candidate.get("unit_street_width_m")
        or candidate.get("street_width_m")
    )

    prompt = template.format(
        brand_name=brief.get("brand_name", "—"),
        category=brief.get("category", "—"),
        service_model=brief.get("service_model", "—"),
        target_area=brief.get("target_area_m2") or "—",
        min_area=brief.get("min_area_m2", "—"),
        max_area=brief.get("max_area_m2", "—"),
        existing_branches=branch_desc,
        primary_channel=(
            brief.get("brand_profile", {}) or {}
        ).get("primary_channel", "—"),
        district=(
            candidate.get("district_display")
            or candidate.get("district")
            or "—"
        ),
        area=area or "—",
        annual_rent=annual_rent or "—",
        rent_per_sqm=rent_per_sqm or "—",
        street_width=street_width or "—",
        final_score=candidate.get("final_score", "—"),
        economics_score=candidate.get("economics_score", "—"),
        brand_fit_score=candidate.get("brand_fit_score", "—"),
        demand_score=candidate.get("demand_score", "—"),
        whitespace_score=candidate.get(
            "provider_whitespace_score",
            candidate.get("whitespace_score", "—"),
        ),
        listing_quality_score=candidate.get("listing_quality_score", "—"),
        llm_reasoning=candidate.get("llm_reasoning") or "not available",
        district_median_rent=district_median_rent or "—",
        rent_vs_median=rent_vs_median,
    )

    client = _get_client()
    aqar_id = candidate.get("parcel_id") or candidate.get("id") or "unknown"

    try:
        response = client.chat.completions.create(
            model=MODEL_ID,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
        )
    except Exception as exc:
        logger.error(
            "Decision memo OpenAI call failed for %s: %s", aqar_id, exc
        )
        raise RuntimeError(f"Decision memo generation failed: {exc}") from exc

    # Parse response
    content = (response.choices[0].message.content or "").strip()
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        logger.error(
            "Decision memo JSON parse failed for %s: %s | raw=%s",
            aqar_id, exc, content[:500],
        )
        raise RuntimeError(
            "Decision memo returned invalid JSON from LLM"
        ) from exc

    if not isinstance(parsed, dict):
        raise RuntimeError("Decision memo LLM returned non-object JSON")

    # Fill missing fields gracefully
    for field in _REQUIRED_STRING_FIELDS:
        if not isinstance(parsed.get(field), str) or not parsed[field].strip():
            parsed[field] = "—"

    for field in _REQUIRED_LIST_FIELDS:
        if not isinstance(parsed.get(field), list):
            parsed[field] = []

    # Record cost
    usage = response.usage
    input_tokens = usage.prompt_tokens if usage else 0
    output_tokens = usage.completion_tokens if usage else 0
    cost = _record_cost(input_tokens, output_tokens)

    logger.info(
        "Decision memo generated | aqar_id=%s lang=%s "
        "input_tokens=%d output_tokens=%d cost=$%.5f",
        aqar_id, lang, input_tokens, output_tokens, cost,
    )

    return parsed


# ── Structured decision memo (Phase 1) ──────────────────────────────
#
# The structured memo path is additive: it sits on top of the 9-component
# deterministic scorer, does NOT modify scoring/gating/ranking, and can be
# toggled off via ``settings.EXPANSION_MEMO_STRUCTURED_ENABLED`` to revert
# to the legacy ``generate_decision_memo`` output byte-for-byte.

# Deterministic scorer weights — kept in sync with the 9-component weights
# in ``app.services.expansion_advisor``. Editing these here does NOT change
# scoring; these are used only to compute memo-display contributions.
COMPONENT_WEIGHTS: dict[str, float] = {
    "occupancy_economics": 0.30,
    "listing_quality": 0.11,
    "brand_fit": 0.11,
    "competition_whitespace": 0.10,
    "demand_potential": 0.10,
    "access_visibility": 0.10,
    "landlord_signal": 0.08,
    "delivery_demand": 0.05,
    "confidence": 0.05,
}

# Feature-snapshot fields that actually drive a decision. Used to truncate
# oversized snapshots to a compact LLM-friendly payload.
#
# Phase 4 split: the memo LLM and the rerank LLM historically shared a
# single whitelist. Phase 4 widens the memo's narrative surface to
# include listing_age and district_momentum (so the memo can cite Phase
# 3a/3b signals in one sentence) while holding the rerank LLM's signal
# surface constant. The two whitelists are therefore distinct; rerank
# keeps the scalar-only set, memo gets the scalar set plus the two
# Phase-3 dict keys.
_RERANK_WHITELIST: tuple[str, ...] = (
    "district",
    "district_display",
    "area_m2",
    "unit_area_sqm",
    "estimated_annual_rent_sar",
    "display_annual_rent_sar",
    "estimated_rent_sar_m2_year",
    "unit_street_width_m",
    "street_width_m",
    "population_reach",
    "competitor_count",
    "delivery_listing_count",
    "access_visibility_score",
    "landlord_signal",
    "realized_demand_30d",
    "realized_demand_branches",
    "realized_demand_district_median",
    "zoning_fit",
    "parking_score",
    "frontage_score",
)

_MEMO_WHITELIST: tuple[str, ...] = _RERANK_WHITELIST + (
    "listing_age",
    "district_momentum",
    # PR #1 — advisory-grade snapshot plumbing. Comparable-rent
    # context (median, sample size, scope label) is plumbed into
    # feature_snapshot_json so the memo LLM can ground rent
    # arguments in observed peer listings rather than hand-wave.
    "comparable_median_annual_rent_sar",
    "comparable_n",
    "comparable_source_label",
)

# Back-compat alias for existing memo call sites (:730, :901, :932).
# DO NOT import this name from new code — it resolves to the memo-
# inflated whitelist and quietly grants any new consumer access to
# listing_age and district_momentum. New consumers must import either
# _MEMO_WHITELIST or _RERANK_WHITELIST explicitly so the memo-vs-rerank
# signal boundary stays visible at the import site.
_FEATURE_SNAPSHOT_WHITELIST = _MEMO_WHITELIST


@dataclass
class MemoContext:
    """Inputs to the structured memo LLM call.

    All optional fields tolerate None / empty; callers should never have
    to pre-normalize before constructing this.
    """

    candidate_id: str
    parcel_id: str | None
    rank_position: int
    next_candidate_summary: dict | None
    brand_profile: dict
    feature_snapshot: dict
    score_breakdown: dict
    gate_verdicts: list[dict]
    gate_buckets: dict = field(default_factory=dict)
    comparable_competitors: list[dict] = field(default_factory=list)
    realized_demand: dict | None = None
    listing_image_url: str | None = None
    locale: str = "en"
    overall_pass: bool | None = None
    final_rank: int | None = None
    final_score: float | None = None
    deterministic_verdict: str | None = None


# ── Context assembly helpers ────────────────────────────────────────


def _as_dict(v: Any) -> dict:
    if isinstance(v, dict):
        return dict(v)
    return {}


def _as_list(v: Any) -> list:
    if isinstance(v, list):
        return list(v)
    return []


# Bucketed gate_reasons_json has these top-level keys. When a dict has them
# we treat it as the bucketed shape (authoritative source of tri-state);
# otherwise we treat it as the flat gate_status_json shape.
_GATE_REASONS_BUCKET_KEYS: tuple[str, ...] = ("passed", "failed", "unknown")


def _lookup_gate_explanation(
    gate_name: str,
    explanations: dict[str, Any],
) -> str:
    """Look up an explanation for ``gate_name`` in the ``explanations`` map,
    tolerating either the humanized ("parking") or raw ("parking_pass") form."""
    if not isinstance(explanations, dict) or not gate_name:
        return ""
    # Direct hit (humanized name used as key, or raw key form).
    direct = explanations.get(gate_name)
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    # Try raw-key form ("parking" -> "parking_pass").
    raw_key = gate_name.replace(" ", "_").replace("/", "_") + "_pass"
    hit = explanations.get(raw_key)
    if isinstance(hit, str) and hit.strip():
        return hit.strip()
    # Last attempt: scan explanation keys, humanize to compare.
    for k, v in explanations.items():
        if not isinstance(v, str) or not v.strip():
            continue
        humanized = str(k).replace("_pass", "").replace("_", " ").replace("/", " ")
        if humanized.strip().lower() == gate_name.strip().lower():
            return v.strip()
    return ""


def _coerce_gate_verdicts(raw: Any) -> list[dict]:
    """Normalize various gate-status shapes into a list of
    ``{gate, verdict, reason}`` dicts.

    Tri-state preserving:
      - ``None`` / missing booleans map to ``verdict="unknown"`` (NOT "fail").
      - Bucketed ``gate_reasons_json`` (``{passed:[], failed:[], unknown:[],
        explanations:{}}``) is the authoritative source when present; gates
        are drawn from the bucket arrays, not by iterating top-level keys.
    """
    if isinstance(raw, list):
        out: list[dict] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            verdict_raw = (
                item.get("verdict")
                or item.get("status")
                or ""
            )
            verdict = str(verdict_raw).strip().lower() if verdict_raw else ""
            if verdict in ("pass", "passed"):
                verdict = "pass"
            elif verdict in ("fail", "failed"):
                verdict = "fail"
            elif verdict in ("unknown", "indeterminate", "n/a", "na", ""):
                verdict = "unknown" if verdict else ""
            out.append({
                "gate": item.get("gate") or item.get("name") or "",
                "verdict": verdict,
                "reason": item.get("reason") or item.get("message") or "",
            })
        return out

    if isinstance(raw, dict):
        # Bucketed shape: gate_reasons_json with passed/failed/unknown arrays.
        if any(k in raw for k in _GATE_REASONS_BUCKET_KEYS):
            explanations = raw.get("explanations") if isinstance(raw.get("explanations"), dict) else {}
            out = []
            for bucket_key, verdict_label in (
                ("passed", "pass"),
                ("failed", "fail"),
                ("unknown", "unknown"),
            ):
                names = raw.get(bucket_key) or []
                if not isinstance(names, list):
                    continue
                for name in names:
                    gate_name = str(name or "").strip()
                    if not gate_name:
                        continue
                    out.append({
                        "gate": gate_name,
                        "verdict": verdict_label,
                        "reason": _lookup_gate_explanation(gate_name, explanations or {}),
                    })
            return out

        # Flat shape: gate_status_json with booleans / None values.
        out = []
        for name, val in raw.items():
            if name == "overall_pass":
                continue
            if isinstance(val, dict):
                out.append({
                    "gate": name,
                    "verdict": val.get("verdict") or val.get("status") or "",
                    "reason": val.get("reason") or val.get("message") or "",
                })
            elif val is True:
                out.append({"gate": name, "verdict": "pass", "reason": ""})
            elif val is False:
                out.append({"gate": name, "verdict": "fail", "reason": ""})
            else:
                # None / anything else → unknown, NOT fail. This fixes the
                # tri-state collapse bug.
                out.append({"gate": name, "verdict": "unknown", "reason": ""})
        return out

    return []


def _build_gate_buckets(
    gate_reasons: dict[str, Any] | None,
    gate_verdicts: list[dict],
) -> dict[str, list[dict]]:
    """Return ``{"passed": [...], "failed": [...], "unknown": [...]}`` of
    ``{name, explanation}`` entries.

    Prefers ``gate_reasons`` (``passed/failed/unknown`` arrays + ``explanations``)
    as the authoritative source, because the bucket arrays carry the right
    tri-state semantics. Falls back to ``gate_verdicts`` when
    ``gate_reasons`` is unavailable or empty.
    """
    buckets: dict[str, list[dict]] = {"passed": [], "failed": [], "unknown": []}

    def _append(bucket: str, name: str, explanation: str) -> None:
        gate_name = str(name or "").strip()
        if not gate_name:
            return
        buckets[bucket].append({
            "name": gate_name,
            "explanation": explanation or "",
        })

    if isinstance(gate_reasons, dict) and any(
        isinstance(gate_reasons.get(k), list) and gate_reasons.get(k)
        for k in _GATE_REASONS_BUCKET_KEYS
    ):
        explanations = gate_reasons.get("explanations") if isinstance(
            gate_reasons.get("explanations"), dict
        ) else {}
        for bucket_key in _GATE_REASONS_BUCKET_KEYS:
            for name in gate_reasons.get(bucket_key) or []:
                _append(
                    bucket_key,
                    str(name),
                    _lookup_gate_explanation(str(name), explanations or {}),
                )
        return buckets

    # Fallback from the verdict list (tri-state preserving).
    for v in gate_verdicts or []:
        verdict = str(v.get("verdict") or "").strip().lower()
        name = v.get("gate") or ""
        reason = v.get("reason") or ""
        if verdict == "pass":
            _append("passed", name, reason)
        elif verdict == "fail":
            _append("failed", name, reason)
        elif verdict == "unknown":
            _append("unknown", name, reason)
    return buckets


def _derive_deterministic_verdict(candidate: dict[str, Any]) -> str | None:
    """Re-compute the deterministic verdict label ("go" | "consider" |
    "caution") from candidate scores using the same formula as
    ``get_recommendation_report``.

    Returns None when any required score is missing — the caller will then
    omit ``deterministic_verdict`` from the payload rather than bake in a
    default. This keeps the LLM anchor truthful: absence of anchor is
    signaled as absence, not as "caution".
    """
    final_score = candidate.get("final_score")
    economics = candidate.get("economics_score")
    cannib = candidate.get("cannibalization_score")
    if (
        not isinstance(final_score, (int, float))
        or not isinstance(economics, (int, float))
        or not isinstance(cannib, (int, float))
    ):
        return None
    if final_score >= 78 and economics >= 70 and cannib <= 55:
        return "go"
    if final_score >= 58 and economics >= 45 and cannib <= 75:
        return "consider"
    return "caution"


def _component_score_value(
    score_breakdown: dict,
    candidate: dict | None,
    comp: str,
) -> float:
    """Best-effort lookup of a per-component score (0..100) by trying the
    canonical key, then ``<comp>_score``, then the flat candidate dict."""
    bd = score_breakdown or {}
    if isinstance(bd.get(comp), (int, float)):
        return float(bd[comp])
    score_key = f"{comp}_score"
    if isinstance(bd.get(score_key), (int, float)):
        return float(bd[score_key])
    components = bd.get("components")
    if isinstance(components, dict):
        v = components.get(comp)
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, dict) and isinstance(v.get("score"), (int, float)):
            return float(v["score"])
    if candidate is not None:
        for k in (comp, score_key):
            if isinstance(candidate.get(k), (int, float)):
                return float(candidate[k])
    return 0.0


def _build_contributions(
    score_breakdown: dict,
    candidate: dict | None = None,
) -> dict[str, float]:
    """Return ``{component: weight × component_score}`` for all 9 components."""
    return {
        comp: round(weight * _component_score_value(score_breakdown, candidate, comp), 3)
        for comp, weight in COMPONENT_WEIGHTS.items()
    }


def _extract_realized_demand(
    candidate: dict,
    feature_snapshot: dict,
) -> dict | None:
    """Pull realized-demand fields from candidate/feature_snapshot into the
    canonical ``{value_30d, branch_count, district_median}`` shape.
    Returns None when neither value_30d nor branch_count is present — matching
    the realized-demand signal being OFF or absent for this catchment.
    """
    def _pick(key: str) -> Any:
        return candidate.get(key) if candidate.get(key) is not None else feature_snapshot.get(key)

    v30 = _pick("realized_demand_30d")
    branches = _pick("realized_demand_branches")
    district_median = _pick("realized_demand_district_median")
    if v30 is None and branches is None:
        return None
    out: dict[str, Any] = {}
    if v30 is not None:
        out["value_30d"] = v30
    if branches is not None:
        out["branch_count"] = branches
    if district_median is not None:
        out["district_median"] = district_median
    return out or None


def build_memo_context(
    *,
    candidate: dict[str, Any],
    brief: dict[str, Any],
    lang: str = "en",
    next_candidate_summary: dict[str, Any] | None = None,
) -> MemoContext:
    """Build a MemoContext from the raw candidate/brief dicts already
    available at the existing call site.

    Pure: never raises on missing fields. Missing optional fields become
    None or an empty list. Always enriches ``score_breakdown`` with
    ``weights`` and ``contributions`` (``weight × component_score``) so the
    LLM can cite per-component impact directly.
    """
    candidate_id = str(
        candidate.get("id")
        or candidate.get("candidate_id")
        or candidate.get("parcel_id")
        or ""
    )
    parcel_id = candidate.get("parcel_id")

    feature_snapshot = _as_dict(candidate.get("feature_snapshot_json"))
    if not feature_snapshot:
        # Fall back to a flat whitelist of candidate fields so the LLM still
        # gets the decision-driving signals even when feature_snapshot_json
        # isn't populated.
        feature_snapshot = {
            k: candidate.get(k)
            for k in _FEATURE_SNAPSHOT_WHITELIST
            if candidate.get(k) is not None
        }

    raw_breakdown = _as_dict(candidate.get("score_breakdown_json"))
    contributions = _build_contributions(raw_breakdown, candidate)
    # Do not mutate caller's dict.
    score_breakdown = dict(raw_breakdown)
    score_breakdown["weights"] = dict(COMPONENT_WEIGHTS)
    score_breakdown["contributions"] = contributions

    # Tri-state-preserving gate verdicts. When the candidate carries both
    # gate_status_json (flat bool/None) and gate_reasons_json (bucketed),
    # prefer gate_reasons_json as the authoritative source since its
    # passed/failed/unknown arrays already encode the correct semantics and
    # carry human-readable explanations.
    raw_gate_reasons = candidate.get("gate_reasons_json")
    raw_gate_status = candidate.get("gate_status_json")
    if isinstance(raw_gate_reasons, dict) and any(
        isinstance(raw_gate_reasons.get(k), list) and raw_gate_reasons.get(k)
        for k in _GATE_REASONS_BUCKET_KEYS
    ):
        gate_verdicts = _coerce_gate_verdicts(raw_gate_reasons)
    else:
        gate_verdicts = _coerce_gate_verdicts(raw_gate_status or raw_gate_reasons)

    gate_buckets = _build_gate_buckets(
        raw_gate_reasons if isinstance(raw_gate_reasons, dict) else None,
        gate_verdicts,
    )

    comparable_competitors = _as_list(
        candidate.get("comparable_competitors_json")
    )
    realized_demand = _extract_realized_demand(candidate, feature_snapshot)

    rank_raw = candidate.get("rank_position")
    try:
        rank_position = int(rank_raw) if rank_raw is not None else 0
    except (TypeError, ValueError):
        rank_position = 0

    # Anchors the LLM narrative can be held to. The deterministic verdict
    # is not part of the memo's output schema (it lives on
    # ``recommendation.verdict`` produced by ``get_recommendation_report``),
    # but the LLM needs to see it as an input so headline/bottom_line stay
    # directionally consistent with it.
    overall_pass = None
    if isinstance(raw_gate_status, dict) and "overall_pass" in raw_gate_status:
        overall_pass = raw_gate_status.get("overall_pass")
    final_rank_raw = candidate.get("final_rank")
    try:
        final_rank = int(final_rank_raw) if final_rank_raw is not None else None
    except (TypeError, ValueError):
        final_rank = None
    final_score_raw = candidate.get("final_score")
    final_score = (
        float(final_score_raw)
        if isinstance(final_score_raw, (int, float))
        else None
    )
    deterministic_verdict = _derive_deterministic_verdict(candidate)

    brand_profile = _as_dict(brief.get("brand_profile"))
    for k in (
        "brand_name",
        "category",
        "service_model",
        "expansion_goal",
        "target_area_m2",
        "min_area_m2",
        "max_area_m2",
    ):
        if brand_profile.get(k) is None and brief.get(k) is not None:
            brand_profile[k] = brief.get(k)

    listing_image_url = (
        candidate.get("listing_image_url")
        or candidate.get("primary_image_url")
    )
    locale = "ar" if lang == "ar" else "en"

    return MemoContext(
        candidate_id=candidate_id,
        parcel_id=parcel_id,
        rank_position=rank_position,
        next_candidate_summary=next_candidate_summary,
        brand_profile=brand_profile,
        feature_snapshot=feature_snapshot,
        score_breakdown=score_breakdown,
        gate_verdicts=gate_verdicts,
        gate_buckets=gate_buckets,
        comparable_competitors=comparable_competitors,
        realized_demand=realized_demand,
        listing_image_url=listing_image_url,
        locale=locale,
        overall_pass=overall_pass,
        final_rank=final_rank,
        final_score=final_score,
        deterministic_verdict=deterministic_verdict,
    )


# ── Prompt ──────────────────────────────────────────────────────────


STRUCTURED_MEMO_SYSTEM_PROMPT = """You are a senior real-estate advisor in Riyadh writing an investment memo for a restaurant operator's principal. The reader is the person making the capital call on this specific listing — they want a clear, persuasive, numerically-grounded answer to one question: is this site worth the capital?

Write like an advisor, not a junior analyst. Lead with the strongest investment argument grounded in a specific number. Synthesize the score breakdown into a thesis — never restate the breakdown back at the reader as a list of percentages. Be direct. Be specific to this candidate, this listing, this catchment. Density beats length.

You will receive a JSON object describing the brand profile, the candidate's feature snapshot, the score_breakdown (9 components with weights and contributions), the gate buckets (gates.passed / gates.failed / gates.unknown — tri-state), deterministic anchors (overall_pass, final_rank, final_score, deterministic_verdict), comparable competitors, the rank-2 alternative (next_candidate_summary), and optionally a realized_demand block.

Return ONLY a single JSON object — no markdown fences, no commentary before or after. The object must contain EXACTLY these six top-level keys:

{
  "headline_recommendation": "string — one short sentence. MUST start with 'Recommend', 'Recommend with reservations', or 'Decline'. Never start with 'Consider' — that is a non-decision. Never start with 'consider due to'.",
  "ranking_explanation": "string — 3 to 5 sentences. Lead with the strongest investment argument grounded in a specific number (rent vs comparable median, foot traffic, brand saturation, demand signal). MUST cite at least one number with units. Mention one real trade-off. Do NOT recite the score breakdown back at the reader; synthesize it into an investment thesis.",
  "key_evidence": [
    {"signal": "string", "value": "string — MUST include a unit (SAR/yr, SAR/m²/yr, orders/30d, /100, m, count, %, etc.); never a bare number", "implication": "string — one clause naming the investment consequence (not a description of what the number is)", "polarity": "positive | negative | neutral"}
  ],
  "risks": [
    {"risk": "string", "mitigation": "string or null — specific tactics only; see rule below"}
  ],
  "comparison": "string — 2 to 3 sentences. MUST reference (a) at least one named competitor from comparable_competitors AND (b) the rank-2 alternative from next_candidate_summary, by rank ('rank 2 in this search...'). Be specific about what beats what.",
  "bottom_line": "string — one sentence. The 'tell over coffee' closer. MUST NOT repeat the headline."
}

LENGTH BUDGET:
- Aim for 350–500 words across all six sections combined. Do not pad to hit the upper bound. Density beats length; a tight 380-word memo is better than a padded 480-word memo.

USE THESE SPECIFIC FIELDS when making the case. Do not hand-wave; cite numbers.

Financial framing:
- estimated_annual_rent_sar / display_annual_rent_sar: absolute annual rent in SAR.
- comparable_median_annual_rent_sar + comparable_n + comparable_source_label:
  the peer-listing median to compare rent against. When comparable_source_label
  starts with "district_", phrase the comparison as "vs N district comparables."
  When it starts with "city_", phrase it as "vs N citywide comparables in the
  same band/type." Do NOT claim district scope when the label is city-scoped —
  honesty about scope is non-negotiable.
- score_breakdown.economics_detail.rent_burden.percentile: a 0–1 fraction.
  Multiply by 100 to phrase ("at the 69th percentile vs comparables").

Property overview:
- area_m2 / unit_area_sqm: site area in m².
- unit_street_width_m: frontage width in meters.
- access_visibility_score, parking_score, frontage_score: 0–100 site-quality
  scalars. Cite the number with the unit "/100".

Market context:
- population_reach: reachable population within walking distance.
- district_momentum: trajectory signal for the district.
- delivery_listing_count: depth of delivery demand in the area.
- realized_demand_30d / realized_demand_branches: actual delivery orders, not
  proxies. Lead with these when present.

Competitive landscape:
- brand_presence.top_chains: named chains within 500m with branch_count and
  nearest_distance_m. Use display_name_en in English memos and display_name_ar
  in Arabic memos.
- comparable_competitors: rated peer restaurants for the operator's brand.
- next_candidate_summary: the rank-2 site in this search, for explicit
  alternative comparison. Reference it by rank in the comparison field.

Risk signals:
- gates.failed and gates.unknown: enumerate these as candidate risks.
- listing_age.created_days / updated_days: flag stale listings (>90 days).
- landlord_signal: counterparty / landlord behaviour score.

HARD RULES:
- The headline_recommendation, ranking_explanation, and bottom_line must agree directionally with `deterministic_verdict`, `overall_pass`, and `final_rank`. If `final_rank == 1` AND `final_score >= 70`, the headline must be 'Recommend' — not 'Recommend with reservations', not 'Consider'. If `overall_pass == false`, the headline must be 'Decline'.
- key_evidence must be 4–6 items. Every value MUST include a unit. A bare number ('81.48', '15') is a hard error — write '81/100', '15 count', '15 m frontage', 'SAR 480,000/yr'.
- Polarity discipline: 'neutral' is rare. If the implication mentions a concern, drawback, or risk, polarity is 'negative'. If it strengthens the investment case, 'positive'. The implication and polarity must agree.
- Neutral-case honesty: for the rent-vs-comparables percentile specifically, treat 40th–60th percentile as essentially at-market — neither a discount nor a premium. Do NOT spin a 45th–55th percentile result as "competitive rent" or "favorable pricing." Phrase it plainly: "asking rent of SAR X/yr is at the 52nd percentile vs N comparables — essentially at market." Polarity for an at-market rent signal MUST be 'neutral'. The investment case must rest on other signals (demand, frontage, demographics, competition density), not on rent being something it isn't. This ban applies to the implication string of any rent or rent-percentile evidence item, not just the headline or ranking_explanation. Banned vocabulary anywhere in the rent or rent-percentile signal — including its implication string — for 40th–60th percentile rows: "competitive", "favorable", "well-positioned", "attractive pricing", "market-friendly", "advantageous". Replace with neutral phrasing: "essentially at market", "market-clearing", "neither premium nor discount", "at the median for comparable listings". The implication for an at-market rent must name an investment consequence ("offers no entry advantage; margin must come from operations", "deal pricing is market-clearing"), not editorialize the price.
- Implication phrasing: name the investment consequence in one clause, not a description. Write "the spread justifies the entry rent" — not "rent is below median". Write "signage works in both traffic directions" — not "site is on a corner".
- risks must be 2–4 distinct items. One-risk memos are a defect. Draw from gates.failed, gates.unknown, listing staleness, parking unknowns, frontage signals, cannibalization, brand saturation. Each item needs a `risk` field; the `mitigation` field is optional.
- Mitigations must be specific tactics the operator can act on (e.g. 'Lease curbside pickup zone from neighbour', 'Partner with HungerStation for delivery-first hours in the first 90 days', 'Add LED frontage signage on the corner approach'). If you can only think of generic advice like 'consider marketing strategies', 'focus on differentiation', 'enhance visibility' — OMIT the mitigation field (set it to null). Better silent than empty.
- Comparison MUST reference at least one named competitor from comparable_competitors. If next_candidate_summary is present in the payload, the comparison MUST also reference the rank-2 alternative by rank ("rank 2 in this search..."). If next_candidate_summary is null, absent, or empty, OMIT any reference to a rank-2 alternative — do NOT spin its absence as a positive signal. Banned phrasings include but are not limited to: "absence of competition", "best option available", "no alternatives", "stands out as the top choice", "the top choice", "stands alone", "no peer", "uncontested", "the only viable option", "the obvious pick", "the clear winner". Do NOT invent a phantom alternative. When next_candidate_summary is absent, the comparison field must focus entirely on named competitors from comparable_competitors — describe what this site beats or loses to vs. those named competitors, and stop. Do NOT mention the absence of a rank-2 candidate at all; silence on that point is correct, not awkward. A comparison that names neither a real competitor nor a real rank-2 candidate is a defect.
- Banned openers: 'Overall,', 'Generally speaking,', 'It appears that', 'consider due to', 'This candidate could potentially'.
- Banned hedging modals when stating evidence or rationale: 'may', 'could', 'might', 'potentially'. Save them only for genuine future uncertainty in `risks`.
- Do not start consecutive memos with the same skeleton. Lead with the strongest concrete signal for THIS site.

GATE LANGUAGE RULES (factual, not stylistic — violations are errors):
- For any gate in `gates.unknown`, you MUST say 'could not be verified from current data' or 'not evaluable from current data'. You MUST NOT use 'failed', 'failing', 'decline', 'not viable', 'undermines viability', or any synonym treating the gate as a negative finding. Unknown means absence of evidence, NOT a negative signal.
- For any gate in `gates.failed`, 'fails on...', 'does not meet...' is appropriate; describe the failure plainly with the threshold.
- Parking is frequently unknown for Aqar listings by architectural design, not by listing defect. If parking is in `gates.unknown`, treat it as a routine data-availability note — do not downgrade the site.

VOICE EXAMPLES (target tone — match this directness and depth):

Example C — strong recommend, score 84, rank 1, district-tier comparable:
{
  "headline_recommendation": "Recommend — asking rent sits at the 28th percentile vs district comparables and the corner frontage gives the brand visibility from two arteries.",
  "ranking_explanation": "The investment case here is rent: SAR 432,000/yr lands at the 28th percentile vs 14 district comparables, a roughly SAR 110,000/yr discount to the median that compounds materially over a five-year lease. Site quality reinforces the economics — a 24 m corner on a primary artery with an access/visibility score of 82/100 — and a population reach of 41,000 inside the walking catchment supports the dine-in model. The trade-off is depth of competition: three named chains operate within 500 m, so the brand will need a defensible category position rather than a generic offer.",
  "key_evidence": [
    {"signal": "annual rent", "value": "SAR 432,000/yr", "implication": "the spread to the district median justifies the entry — roughly SAR 110k/yr saved vs peer listings", "polarity": "positive"},
    {"signal": "rent percentile vs comparables", "value": "28th percentile (vs 14 district comparables)", "implication": "deal pricing is genuinely below market, not just below list", "polarity": "positive"},
    {"signal": "frontage", "value": "24 m corner", "implication": "signage works in both traffic directions on a primary artery", "polarity": "positive"},
    {"signal": "access/visibility score", "value": "82/100", "implication": "site quality reinforces the rent advantage rather than offsetting it", "polarity": "positive"},
    {"signal": "population reach", "value": "41,000 within walking catchment", "implication": "dine-in mix is supportable without leaning on delivery to fill seats", "polarity": "positive"},
    {"signal": "named chains within 500 m", "value": "3 count", "implication": "the catchment validates the category but raises the bar on differentiation", "polarity": "negative"}
  ],
  "risks": [
    {"risk": "Three established chains operate within 500 m, including two with strong delivery presence — undifferentiated entry will compete on price.", "mitigation": "Lead with a single-SKU hero menu and a sharper delivery price point in the first 90 days; revisit the dine-in mix once order velocity stabilises."},
    {"risk": "Parking provision could not be verified from current data — typical for Aqar listings, not a site defect.", "mitigation": "Walk the block at peak hours during diligence; lease two adjacent street stalls from the neighbour if curbside turnover is constrained."},
    {"risk": "Listing has been live for 102 days, longer than is typical for prime corner units in this district.", "mitigation": "Open negotiation 8–12% below asking and ask the landlord to absorb fit-out contribution."}
  ],
  "comparison": "This site beats Peer Chain A on rent by roughly SAR 90k/yr and matches Peer Chain B on visibility, while pulling ahead of rank 2 in this search on rent percentile (28th vs 47th) and access/visibility (82/100 vs 71/100). Rank 2 has a marginally larger footprint but no comparable corner exposure.",
  "bottom_line": "This is the deal in the shortlist — sign it before the listing turns."
}

Example D — decline, score 41, rank 9, gates failed:
{
  "headline_recommendation": "Decline — economics gate fails at the 88th rent percentile and the catchment cannot underwrite the asking price.",
  "ranking_explanation": "The asking rent of SAR 920,000/yr lands at the 88th percentile vs 11 citywide comparables in the same band/type — roughly 34% above the SAR 685,000 median, which by itself is enough to fail the economics gate. The site does not earn that premium: population reach inside the walking catchment is 18,000 and the access/visibility score sits at 54/100, both below the levels needed to support a premium-rent thesis for this category. The next-best alternative in the shortlist offers materially better economics for a comparable footprint, so capital is better deployed there.",
  "key_evidence": [
    {"signal": "annual rent", "value": "SAR 920,000/yr", "implication": "asking sits 34% above the comparable median — the deal is mispriced for the catchment", "polarity": "negative"},
    {"signal": "rent percentile vs comparables", "value": "88th percentile (vs 11 citywide comparables in the same band/type)", "implication": "no peer-listing evidence that this rent is achievable for this format", "polarity": "negative"},
    {"signal": "economics gate", "value": "failed", "implication": "deterministic threshold breached; the deal cannot be defended on rent burden", "polarity": "negative"},
    {"signal": "population reach", "value": "18,000 within walking catchment", "implication": "thin demand base does not justify a premium rent position", "polarity": "negative"},
    {"signal": "access/visibility score", "value": "54/100", "implication": "site quality is mid-tier and does not earn the premium pricing", "polarity": "negative"},
    {"signal": "listing age (created)", "value": "147 days", "implication": "stale listing suggests the market has already declined this rent", "polarity": "negative"}
  ],
  "risks": [
    {"risk": "Economics gate failure — current rent leaves no margin for under-performance against forecast.", "mitigation": "Walk unless the landlord accepts a 25%+ rent reduction with a documented break clause."},
    {"risk": "Frontage gate could not be verified from current data; signage potential is unclear.", "mitigation": null},
    {"risk": "Two saturated chain operators within 500 m increase cannibalisation risk for any value-format entry.", "mitigation": "Reposition the brief towards a differentiated dayparting strategy if the operator chooses to override the recommendation."},
    {"risk": "Listing has been on market for 147 days — pricing has not cleared, suggesting the asking is structurally above the catchment's willingness-to-pay.", "mitigation": null}
  ],
  "comparison": "Peer Chain A in this district closed at roughly SAR 640,000/yr — a 30% discount to this asking — and rank 2 in this search clears the economics gate at the 49th rent percentile with a slightly larger footprint and a stronger access/visibility profile. There is no scenario in which this site is the rational shortlist pick over rank 2.",
  "bottom_line": "Walk this one and redeploy the capital into rank 2 — the math on this listing does not work."
}

Example E — recommend with reservations, score 68, rank 2, at-market rent (illustrating neutral polarity):
{
  "headline_recommendation": "Recommend with reservations — rent is at market and the case rests on access and demand, not pricing.",
  "ranking_explanation": "This site does not win on rent. Asking SAR 540,000/yr lands at the 51st percentile vs 22 district comparables — essentially at market, neither a discount nor a premium. The investment case rests on the access/visibility score of 88/100 on a primary artery, a 4-named-chain catchment within 500 m suggesting validated demand, and a population reach of 33,000 within walking distance. The trade-off is that without a rent advantage, margin pressure is higher than in a discounted-entry deal.",
  "key_evidence": [
    {"signal": "annual rent", "value": "SAR 540,000/yr", "implication": "at-market pricing offers no entry advantage; margin must come from operations", "polarity": "neutral"},
    {"signal": "rent percentile vs comparables", "value": "51st percentile (vs 22 district comparables)", "implication": "deal pricing is market-clearing, neither premium nor discount", "polarity": "neutral"},
    {"signal": "access/visibility score", "value": "88/100", "implication": "site quality is the primary thesis here; signage and approach support brand visibility", "polarity": "positive"},
    {"signal": "named chains within 500 m", "value": "4 count", "implication": "validates the catchment for the category at the cost of competitive intensity", "polarity": "neutral"},
    {"signal": "population reach", "value": "33,000 within walking catchment", "implication": "demand base supports the dine-in mix at typical capture rates", "polarity": "positive"}
  ],
  "risks": [
    {"risk": "Rent offers no margin cushion — operational missteps will translate directly to P&L.", "mitigation": "Pre-commit a delivery partnership in month one; do not assume dine-in ramp covers fixed cost in the first 90 days."},
    {"risk": "Four established chains within 500 m means category competition is real and ongoing.", "mitigation": "Position on a single dayparting strength rather than competing on the full menu."}
  ],
  "comparison": "Peer Chain A in the same district closed at SAR 510,000/yr — roughly comparable, confirming this is the market-clearing range. Rank 1 in this search has a 22% rent discount on a similar footprint, which makes it the stronger pick on pure economics, but this site has materially better access/visibility (88/100 vs 71/100), so the call depends on whether the operator weights cost basis or street presence more heavily.",
  "bottom_line": "A working deal, but only if access matters more than rent — otherwise rank 1 is the cleaner trade."
}

Inline note on missing rank-2 alternatives: when next_candidate_summary is absent (small result set), a correct comparison reads like "Peer Chain A in this district closed at SAR 510,000/yr — roughly comparable, confirming this is the market-clearing range. Burger King operates 1.4 km away with a stronger brand-recall position but a less central frontage, so the trade is between brand pull and street presence." Notice: this references two named competitors, makes a real comparison, and does not mention rank-2 at all. The reader does not need to be told an alternative is absent — saying so spins missing data as a feature.

Now write the memo for the candidate JSON the user provides. Match the voice in the examples. Be specific to this site, not generic."""


_MAX_USER_PAYLOAD_CHARS = 12000
_FEATURE_SNAPSHOT_SOFT_LIMIT = 4000


def _serialize_context_for_user_message(ctx: MemoContext) -> str:
    """Serialize the MemoContext to a compact JSON string for the user turn,
    truncating ``feature_snapshot`` to the whitelist if the full payload would
    blow the size budget."""
    snap = ctx.feature_snapshot
    serialized_full = json.dumps(snap, default=str, ensure_ascii=False)
    if len(serialized_full) > _FEATURE_SNAPSHOT_SOFT_LIMIT:
        snap = {
            k: snap.get(k)
            for k in _FEATURE_SNAPSHOT_WHITELIST
            if snap.get(k) is not None
        }

    body = {
        "candidate_id": ctx.candidate_id,
        "parcel_id": ctx.parcel_id,
        "rank_position": ctx.rank_position,
        "brand_profile": ctx.brand_profile,
        "feature_snapshot": snap,
        "score_breakdown": ctx.score_breakdown,
        # Kept for backward-compat with any downstream consumer. The
        # authoritative tri-state view for the LLM is ``gates`` below.
        "gate_verdicts": ctx.gate_verdicts,
        "gates": ctx.gate_buckets or {"passed": [], "failed": [], "unknown": []},
        "overall_pass": ctx.overall_pass,
        "final_rank": ctx.final_rank,
        "final_score": ctx.final_score,
        "deterministic_verdict": ctx.deterministic_verdict,
        "comparable_competitors": ctx.comparable_competitors[:5],
        "next_candidate_summary": ctx.next_candidate_summary,
        "realized_demand": ctx.realized_demand,
        "listing_image_url": ctx.listing_image_url,
        "locale": ctx.locale,
    }
    text = json.dumps(body, default=str, ensure_ascii=False)
    if len(text) > _MAX_USER_PAYLOAD_CHARS:
        # Last-resort: drop most competitors and tighten feature_snapshot.
        body["comparable_competitors"] = body["comparable_competitors"][:2]
        body["feature_snapshot"] = {
            k: snap.get(k)
            for k in _FEATURE_SNAPSHOT_WHITELIST
            if snap.get(k) is not None
        }
        text = json.dumps(body, default=str, ensure_ascii=False)
    return text


def render_structured_memo_prompt(ctx: MemoContext) -> list[dict]:
    """Render an OpenAI-style ``[system, user]`` messages list for the
    structured memo call. Appends situational instructions to the system
    prompt when locale is Arabic, realized_demand is present, or any gate
    failed.
    """
    addenda: list[str] = []
    if ctx.locale == "ar":
        addenda.append(
            "LOCALE: Produce every string value in Modern Standard Arabic "
            "(فصحى) — natural, professional Arabic the way a Saudi "
            "real-estate analyst would speak to a restaurant operator. "
            "JSON keys stay in English. Match the directness of the English "
            "voice examples; do not become more formal or hedged just "
            "because you are writing in Arabic. The headline must start "
            "with 'نوصي', 'نوصي مع تحفظات', or 'نرفض'."
        )
    if ctx.realized_demand is not None:
        addenda.append(
            "REALIZED DEMAND: This candidate has actual delivery-order data "
            "(not a supply proxy). Lead the key_evidence with the realized "
            "demand figure (orders/30d), cite the district median for "
            "context, and let it anchor the ranking_explanation if it is "
            "the strongest signal."
        )
    buckets = ctx.gate_buckets or {}
    failed_entries = [e for e in (buckets.get("failed") or []) if e.get("name")]
    unknown_entries = [e for e in (buckets.get("unknown") or []) if e.get("name")]

    if failed_entries:
        failure_list = "; ".join(
            f"{e['name']}: {e.get('explanation','')}".rstrip(": ")
            for e in failed_entries
        )
        addenda.append(
            "GATE FAILURE: This candidate fails on "
            + failure_list
            + ". The headline_recommendation, bottom_line, and overall "
            "direction MUST be consistent with overall_pass=False and "
            "deterministic_verdict. You may use 'fails on...' or 'does not "
            "meet...' for the failed gates — never for unknowns."
        )

    if unknown_entries:
        unknown_list = "; ".join(
            f"{e['name']}: {e.get('explanation','')}".rstrip(": ")
            for e in unknown_entries
        )
        addenda.append(
            "UNKNOWN GATES: The following gates could not be verified from "
            "current data: " + unknown_list + ". Use 'could not be verified "
            "from current data' or 'not evaluable from current data' for "
            "these — never 'fails' or 'decline'. An unknown gate must NOT "
            "flip a positive overall_pass or deterministic_verdict into a "
            "negative recommendation."
        )

    system_content = STRUCTURED_MEMO_SYSTEM_PROMPT
    if addenda:
        system_content = (
            system_content
            + "\n\nSITUATIONAL INSTRUCTIONS:\n- "
            + "\n- ".join(addenda)
        )

    return [
        {"role": "system", "content": system_content},
        {"role": "user", "content": _serialize_context_for_user_message(ctx)},
    ]


# ── Generation with graceful fallback ───────────────────────────────


_STRUCTURED_REQUIRED_KEYS: tuple[str, ...] = (
    "headline_recommendation",
    "ranking_explanation",
    "key_evidence",
    "risks",
    "comparison",
    "bottom_line",
)


def generate_structured_memo(ctx: MemoContext) -> dict | None:
    """Call the LLM for a structured memo, or return None on any failure.

    Never raises — logs a warning and returns None so the caller can fall
    back to the legacy ``generate_decision_memo`` path.

    Returns the parsed JSON dict on success. Token usage is recorded against
    the same ``_daily_cost_tracker`` as the legacy path so the $/day ceiling
    covers both surfaces.
    """
    if not settings.EXPANSION_MEMO_STRUCTURED_ENABLED:
        return None

    # Same hard cost cap as the legacy path. Skip (not raise) when breached so
    # the caller can fall through to legacy; legacy will raise uniformly and
    # the endpoint will translate to a 503 as it does today.
    #
    # NOTE: on a ceiling breach this function returns None and relies on the
    # caller's legacy fallback to raise. The endpoint's 503 therefore comes
    # from the legacy path, not from here — the path is indirect but the
    # end-user behavior (HTTP 503) is unchanged.
    try:
        _check_daily_ceiling()
    except RuntimeError as exc:
        logger.warning("Structured memo skipped (cost ceiling): %s", exc)
        return None

    try:
        client = _get_client()
    except RuntimeError as exc:
        logger.warning("Structured memo client unavailable: %s", exc)
        return None

    messages = render_structured_memo_prompt(ctx)

    try:
        response = client.chat.completions.create(
            model=settings.EXPANSION_MEMO_MODEL,
            messages=messages,
            temperature=settings.EXPANSION_MEMO_TEMPERATURE,
            max_tokens=settings.EXPANSION_MEMO_MAX_TOKENS,
            response_format={"type": "json_object"},
        )
    except Exception as exc:
        logger.warning(
            "Structured memo OpenAI call failed for %s: %s",
            ctx.candidate_id, exc,
        )
        return None

    try:
        content = (response.choices[0].message.content or "").strip()
    except Exception as exc:
        logger.warning(
            "Structured memo response malformed for %s: %s",
            ctx.candidate_id, exc,
        )
        return None

    try:
        parsed = json.loads(content)
    except (json.JSONDecodeError, TypeError) as exc:
        # TypeError guards against a client returning a non-string for
        # ``content`` (real OpenAI always returns str; defensive only).
        logger.warning(
            "Structured memo JSON parse failed for %s: %s | raw=%s",
            ctx.candidate_id, exc, str(content)[:500],
        )
        return None

    if not isinstance(parsed, dict):
        logger.warning(
            "Structured memo returned non-object JSON for %s",
            ctx.candidate_id,
        )
        return None

    missing = [k for k in _STRUCTURED_REQUIRED_KEYS if k not in parsed]
    if missing:
        logger.warning(
            "Structured memo missing keys for %s: %s",
            ctx.candidate_id, missing,
        )
        return None

    if not isinstance(parsed.get("key_evidence"), list) or not parsed["key_evidence"]:
        logger.warning(
            "Structured memo key_evidence invalid/empty for %s",
            ctx.candidate_id,
        )
        return None

    if not isinstance(parsed.get("risks"), list):
        logger.warning(
            "Structured memo risks not a list for %s",
            ctx.candidate_id,
        )
        return None

    usage = getattr(response, "usage", None)
    input_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
    output_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
    cost = _record_cost(input_tokens, output_tokens)

    logger.info(
        "Structured memo generated | candidate_id=%s locale=%s "
        "input_tokens=%d output_tokens=%d cost=$%.5f",
        ctx.candidate_id, ctx.locale, input_tokens, output_tokens, cost,
    )

    return parsed


# ── Text rendering of structured memo (for legacy text column) ──────


_TEXT_SECTION_HEADERS_EN: tuple[tuple[str, str], ...] = (
    ("headline_recommendation", "Headline Recommendation"),
    ("ranking_explanation", "Ranking Explanation"),
    ("key_evidence", "Key Evidence"),
    ("risks", "Risks"),
    ("comparison", "Comparison"),
    ("bottom_line", "Bottom Line"),
)
_TEXT_SECTION_HEADERS_AR: tuple[tuple[str, str], ...] = (
    ("headline_recommendation", "التوصية الرئيسية"),
    ("ranking_explanation", "تفسير الترتيب"),
    ("key_evidence", "الأدلة الرئيسية"),
    ("risks", "المخاطر"),
    ("comparison", "المقارنة"),
    ("bottom_line", "الخلاصة"),
)


def render_structured_memo_as_text(memo_json: dict, locale: str) -> str:
    """Render the six-section structured memo as a plain-text memo with
    markdown-style headers, suitable for the legacy ``decision_memo`` text
    column so existing consumers keep working unchanged.
    """
    headers = _TEXT_SECTION_HEADERS_AR if locale == "ar" else _TEXT_SECTION_HEADERS_EN
    lines: list[str] = []
    for key, label in headers:
        value = memo_json.get(key)
        lines.append(f"## {label}")
        if key == "key_evidence" and isinstance(value, list):
            if not value:
                lines.append("- —")
            for item in value:
                if not isinstance(item, dict):
                    continue
                signal = str(item.get("signal", "—"))
                v = str(item.get("value", "—"))
                impl = str(item.get("implication", "") or "")
                line = f"- {signal}: {v}"
                if impl:
                    line = f"{line} — {impl}"
                lines.append(line)
        elif key == "risks" and isinstance(value, list):
            if not value:
                lines.append("- —")
            for item in value:
                if not isinstance(item, dict):
                    continue
                risk = str(item.get("risk", "—"))
                mit = item.get("mitigation")
                if mit:
                    lines.append(f"- {risk} (mitigation: {mit})")
                else:
                    lines.append(f"- {risk}")
        else:
            lines.append(str(value) if value else "—")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
