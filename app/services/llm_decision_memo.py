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
_FEATURE_SNAPSHOT_WHITELIST: tuple[str, ...] = (
    "district",
    "district_display",
    "area_m2",
    "unit_area_sqm",
    "estimated_annual_rent_sar",
    "display_annual_rent_sar",
    "estimated_rent_sar_m2_year",
    "district_median_rent",
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
    comparable_competitors: list[dict]
    realized_demand: dict | None
    listing_image_url: str | None
    locale: str


# ── Context assembly helpers ────────────────────────────────────────


def _as_dict(v: Any) -> dict:
    if isinstance(v, dict):
        return dict(v)
    return {}


def _as_list(v: Any) -> list:
    if isinstance(v, list):
        return list(v)
    return []


def _coerce_gate_verdicts(raw: Any) -> list[dict]:
    """Normalize various gate-status shapes into a list of
    ``{gate, verdict, reason}`` dicts."""
    if isinstance(raw, list):
        out: list[dict] = []
        for item in raw:
            if isinstance(item, dict):
                out.append({
                    "gate": item.get("gate") or item.get("name") or "",
                    "verdict": item.get("verdict") or item.get("status") or "",
                    "reason": item.get("reason") or item.get("message") or "",
                })
        return out
    if isinstance(raw, dict):
        out = []
        for name, val in raw.items():
            if isinstance(val, dict):
                out.append({
                    "gate": name,
                    "verdict": val.get("verdict") or val.get("status") or "",
                    "reason": val.get("reason") or val.get("message") or "",
                })
            else:
                out.append({
                    "gate": name,
                    "verdict": "pass" if bool(val) else "fail",
                    "reason": "",
                })
        return out
    return []


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

    gate_verdicts = _coerce_gate_verdicts(
        candidate.get("gate_status_json")
        or candidate.get("gate_reasons_json")
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
        comparable_competitors=comparable_competitors,
        realized_demand=realized_demand,
        listing_image_url=listing_image_url,
        locale=locale,
    )


# ── Prompt ──────────────────────────────────────────────────────────


STRUCTURED_MEMO_SYSTEM_PROMPT = """You are a senior commercial real-estate analyst covering Riyadh, Saudi Arabia, evaluating a candidate site for a specific restaurant/retail operator's expansion.

You will receive a JSON object describing:
- the brand profile (category, service model, expansion goal, preferences),
- the candidate's feature snapshot (area, rent, street width, competitor counts, delivery signals, etc.),
- the candidate's score_breakdown, including the 9 deterministic component scores, their weights, and each component's contribution (weight × component_score) to the final score,
- the gate verdicts (pass/fail) applied during screening,
- comparable competitors and (optionally) the next-ranked candidate,
- optionally, a realized_demand block (actual customer-order growth, not supply proxy).

Return ONLY a single JSON object (no markdown fences, no commentary before or after). The object must contain EXACTLY these six top-level keys with the types and semantics described:

{
  "headline_recommendation": "string — one sentence. Must be one of 'recommend', 'recommend with reservations', or 'decline', plus the single strongest reason. Use 'decline' (not 'pass') so the verdict is unambiguous in English.",
  "ranking_explanation": "string — 2-3 sentences. Must cite which of the 9 components drove the rank and by how much, using the numbers from score_breakdown.contributions (e.g., 'occupancy_economics contributed 27.2 out of 30').",
  "key_evidence": [
    {"signal": "string", "value": "string with units", "implication": "string — what this fact means for the decision", "polarity": "positive | negative | neutral"}
  ],
  "risks": [
    {"risk": "string", "mitigation": "string or null"}
  ],
  "comparison": "string — 1-2 sentences on how this compares to the comparable competitors or the next-ranked candidate.",
  "bottom_line": "string — one sentence an analyst would say to the CEO in a hallway."
}

Hard rules:
- Cite specific numbers with units. Do not hedge.
- If a signal is strong, say so. If it's weak, say so.
- Do not use the phrases "overall,", "appears to be,", "could potentially,", or "generally speaking."
- Write like an analyst who has actually visited the site, not a summarizer.
- key_evidence must be a non-empty list. risks may be an empty list, but the key must be present.
- Each key_evidence item MUST include a polarity field. Use 'positive' for facts that favor pursuing the site (strong demand, below-median rent, motivated landlord, etc.), 'negative' for facts that discourage pursuing it (high competition, bad frontage, above-median rent, parking failure, etc.), and 'neutral' for context that is important but neither favorable nor unfavorable. A key_evidence item with `implication` text that describes a concern or drawback MUST be tagged 'negative', not 'neutral'.
- Return ONLY the JSON object, with no markdown fences, no leading or trailing commentary."""


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
        "gate_verdicts": ctx.gate_verdicts,
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
            "(فصحى). JSON keys stay in English; all string values in Arabic."
        )
    if ctx.realized_demand is not None:
        addenda.append(
            "EVIDENCE PRIORITY: realized_demand is present. Prioritize it in "
            "key_evidence because it reflects actual customer orders, not just "
            "supply presence. Cite value_30d, branch_count, and district_median "
            "when available."
        )
    failed = [
        g for g in ctx.gate_verdicts
        if str(g.get("verdict") or "").lower() == "fail"
    ]
    if failed:
        failure_list = "; ".join(
            f"{g.get('gate','?')}: {g.get('reason','')}".rstrip(": ")
            for g in failed
        )
        addenda.append(
            "GATE FAILURE: One or more gates failed — "
            + failure_list
            + ". Lead the headline_recommendation with this failure and frame "
            "the overall recommendation accordingly (likely 'decline' unless the "
            "failure is borderline and clearly mitigable)."
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
