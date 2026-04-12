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
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

# ── Model & cost configuration ──────────────────────────────────────

MODEL_ID = os.environ.get("DECISION_MEMO_MODEL", "gpt-4o-mini-2024-07-18")
MAX_TOKENS = 800
TEMPERATURE = 0.3

# Soft daily ceiling in USD.  Raises RuntimeError before calling OpenAI
# if the running total for today exceeds this value.
DAILY_CEILING_USD = float(
    os.environ.get("DECISION_MEMO_DAILY_CEILING_USD", "1.00")
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
