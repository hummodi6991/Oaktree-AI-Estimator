"""LLM-based F&B suitability and listing-quality classifier.

Wraps OpenAI GPT-4o-mini to produce structured per-listing judgments
that augment the deterministic scoring math in expansion_advisor.py.

The classifier returns three numeric scores plus reasoning text:

- llm_suitability_score (0-100): F&B retail suitability. Replaces the
  structural restaurant_score for the listing_quality suitability
  sub-component.
- llm_listing_quality_score (0-100): visual/copy quality and fit-out
  readiness. Replaces the binary has_image / is_furnished signals in
  the listing_quality calculation.
- llm_landlord_signal_score (0-100): landlord seriousness and F&B-
  friendliness as inferred from copy. Stored but not yet wired into
  ranking in Patch 12 — added in a follow-up patch once we have a
  feel for the value distribution.

Plus:
- llm_suitability_verdict (suitable / unsuitable / uncertain): the
  high-level boolean derived from llm_suitability_score with explicit
  uncertainty handling.
- llm_reasoning: 2-3 sentences explaining the verdict. Audit trail
  AND seed for the eventual operator-facing decision memo feature.

Two-pass design: text-first classification, photos retried only when
the first pass returns 'uncertain'. Most listings resolve from text
alone, so steady-state cost is dominated by the cheaper text path.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# Locked to dated snapshot rather than the bare alias so OpenAI cannot
# silently change classifier behavior on us. Bump deliberately.
MODEL_ID = "gpt-4o-mini-2024-07-18"
CLASSIFIER_VERSION = "v1.0"

# Maximum number of photo URLs to send in the photo-retry pass.
# More photos = more cost; 3 is enough to get a sense of the space.
_MAX_PHOTOS_IN_RETRY = 3

_SYSTEM_PROMPT = """You are evaluating commercial real estate listings in Riyadh, Saudi Arabia for an F&B (food & beverage) operator looking for retail space to open a new branch.

For each listing, return a JSON object with these fields:

{
  "suitability_score": <0-100 integer>,
  "suitability_verdict": "suitable" | "unsuitable" | "uncertain",
  "listing_quality_score": <0-100 integer>,
  "landlord_signal_score": <0-100 integer>,
  "reasoning": "<2-3 sentences explaining the scores>"
}

SCORING GUIDELINES:

suitability_score (0-100): How appropriate is this physical space for an F&B retail tenant?
- 80-100: Clearly designed/suited for retail or F&B. Storefront, ground-floor, on or near a commercial corridor.
- 50-79: Plausibly suitable but some concerns or missing information.
- 20-49: Probably not F&B-suitable. Could be office space, warehouse, residential conversion, or industrial.
- 0-19: Clearly unsuitable. Clinic, medical office, car-related business (showroom for cars, auto parts, rental), warehouse, raw shell with no retail context.

suitability_verdict:
- "suitable" if score >= 60 AND you are confident
- "unsuitable" if score < 40 AND you are confident
- "uncertain" otherwise (including when description is missing/empty/scraped page chrome and no photos provided)

listing_quality_score (0-100): How ready is this space to operate as F&B?
- 80-100: Finished retail unit, visible HVAC/plumbing/storefront glass, photographed clearly, would need minimal fit-out.
- 50-79: Standard bare-shell retail unit, normal fit-out cost.
- 20-49: Raw concrete shell or significant structural work needed.
- 0-19: Photos show severe issues, or photos are missing/placeholder and no description detail.

landlord_signal_score (0-100): How serious and F&B-friendly does the landlord seem from their copy?
- 80-100: Landlord copy is detailed, mentions location context, mentions adjacent businesses, explicitly welcomes or excludes specific tenant types in a way that signals F&B appetite (e.g., "no laundromats or shisha shops" implies they want better tenants).
- 50-79: Standard listing copy, basic facts only.
- 20-49: Sparse or generic copy.
- 0-19: Empty description, scraped page chrome, or copy that suggests indifference / non-F&B intent.

reasoning: 2-3 sentences. Cite specific evidence from the description or photos. Be concrete.

EXAMPLES:

Example 1 — strong positive (Marwah shawarma-adjacent):
Description: "for rent: shop in al-shafa district on the active dirab road — in front of bait al-shawarma restaurants and hyper panda. total area approximately 60 sqm. we do not rent to laundromats or shisha shops. annual rent is 35,000 in two installments."
Output: {"suitability_score": 88, "suitability_verdict": "suitable", "listing_quality_score": 70, "landlord_signal_score": 90, "reasoning": "Landlord explicitly positions the unit next to an existing shawarma restaurant and Hyper Panda, signaling strong F&B context. The exclusion of laundromats and shisha shops indicates the landlord is selecting for higher-value retail tenants. 60 sqm is a standard small QSR/cafe footprint."}

Example 2 — clear negative (Tuwaiq car rental shell):
Description: "Shop for Rent in Riyadh Tuwaiq § 80000 / annually 2,435m² 27m Tuwaiq, Riyadh"
Photos: [photos show raw concrete shell with car rental signs visible in background]
Output: {"suitability_score": 15, "suitability_verdict": "unsuitable", "listing_quality_score": 10, "landlord_signal_score": 5, "reasoning": "Description text is scraped page chrome with no real ad copy. Photos show a raw concrete shell embedded in a row of car rental units, with no F&B context. No fit-out, no storefront glass, no plumbing visible."}

Example 3 — uncertain (sparse copy, no photos):
Description: "store for rent in al olaya, 120 sqm"
Output: {"suitability_score": 50, "suitability_verdict": "uncertain", "listing_quality_score": 50, "landlord_signal_score": 30, "reasoning": "Description is too sparse to make a confident judgment. Al Olaya is a strong F&B district and 120 sqm is a viable QSR/cafe size, but without context on the specific street, adjacent uses, or photos, the suitability cannot be confirmed."}

Return ONLY the JSON object. No preamble, no explanation outside the JSON."""


_client = None


def _get_client():
    """Lazily construct the OpenAI client.

    We import openai inside the function so that the module can be
    imported (and unit-tested via mocks) even when the openai package
    is not installed in the current environment.  Production deploys
    always have openai installed via requirements.txt.
    """
    global _client
    if _client is None:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError(
                "OPENAI_API_KEY is not set. The LLM suitability classifier "
                "requires an OpenAI API key. Add it to GitHub secrets and "
                "the K8s deployment env."
            )
        from openai import OpenAI  # local import — see docstring
        _client = OpenAI(api_key=api_key)
    return _client


def _build_text_user_message(row: dict[str, Any]) -> str:
    """Render the listing facts into a structured text prompt."""
    parts = [
        f"Listing ID: {row.get('aqar_id', 'unknown')}",
        f"Listing type: {row.get('listing_type', 'unknown')}",
        f"Property type: {row.get('property_type') or 'unspecified'}",
        f"Neighborhood: {row.get('neighborhood', 'unknown')}",
        f"Area: {row.get('area_sqm', 'unknown')} m²",
        f"Annual rent: SAR {row.get('price_sar_annual', 'unknown')}",
        f"Street width: {row.get('street_width_m') or 'unknown'} m",
        f"Furnished: {bool(row.get('is_furnished'))}",
        f"Has drive-thru: {bool(row.get('has_drive_thru'))}",
        "",
        "Description:",
        (row.get("description") or "(empty)").strip(),
    ]
    return "\n".join(parts)


def _parse_response(content: str) -> dict[str, Any] | None:
    """Defensively parse the LLM JSON response.

    The LLM is told to return JSON only, but defensive parsing handles
    cases where it wraps the JSON in code fences or adds preamble.
    Returns None if the response cannot be parsed at all.
    """
    if not content:
        return None
    text = content.strip()
    # Strip optional markdown code fences
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(
            line for line in lines if not line.strip().startswith("```")
        )
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        # Try finding the first JSON object in the response
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        try:
            parsed = json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def _coerce_int_score(value: Any, default: int = 50) -> int:
    """Coerce an LLM-returned score to a 0-100 integer."""
    try:
        n = int(round(float(value)))
    except (TypeError, ValueError):
        return default
    return max(0, min(100, n))


def _coerce_verdict(value: Any) -> str:
    """Coerce verdict string to one of the three allowed values."""
    if not isinstance(value, str):
        return "uncertain"
    v = value.strip().lower()
    if v in ("suitable", "unsuitable", "uncertain"):
        return v
    return "uncertain"


def _classify_text_only(row: dict[str, Any]) -> dict[str, Any] | None:
    """First pass: classify from description text and structured fields only."""
    user_message = _build_text_user_message(row)
    try:
        response = _get_client().chat.completions.create(
            model=MODEL_ID,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
            max_tokens=500,
        )
    except Exception as exc:
        logger.warning(
            "LLM text classification failed for aqar_id=%s: %s",
            row.get("aqar_id"),
            exc,
        )
        return None

    return _parse_response(response.choices[0].message.content)


def _classify_with_photos(
    row: dict[str, Any],
    photo_urls: list[str],
) -> dict[str, Any] | None:
    """Second pass: classify using both text and photos.

    Only called when the first text-only pass returns 'uncertain'.
    """
    if not photo_urls:
        return None
    user_content: list[dict[str, Any]] = [
        {"type": "text", "text": _build_text_user_message(row)},
    ]
    for url in photo_urls[:_MAX_PHOTOS_IN_RETRY]:
        user_content.append({"type": "image_url", "image_url": {"url": url}})
    try:
        response = _get_client().chat.completions.create(
            model=MODEL_ID,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
            max_tokens=500,
        )
    except Exception as exc:
        logger.warning(
            "LLM photo classification failed for aqar_id=%s: %s",
            row.get("aqar_id"),
            exc,
        )
        return None

    return _parse_response(response.choices[0].message.content)


def classify_listing(
    row: dict[str, Any],
    *,
    photo_urls: list[str] | None = None,
    use_photos_on_uncertain: bool = True,
) -> dict[str, Any]:
    """Classify a single listing and return structured fields ready to persist.

    Returns a dict with keys matching the new commercial_unit columns:
        llm_suitability_verdict, llm_suitability_score,
        llm_listing_quality_score, llm_landlord_signal_score,
        llm_reasoning, llm_classified_at, llm_classifier_version

    Always returns a dict — never raises. On failure, returns a verdict
    of 'uncertain' with neutral scores so the deterministic fallback
    (the structural restaurant_score path) takes over for that row.
    """
    try:
        parsed = _classify_text_only(row)
    except Exception as exc:  # defensive — _classify_text_only already catches
        logger.warning(
            "LLM text classification raised for aqar_id=%s: %s",
            row.get("aqar_id"),
            exc,
        )
        parsed = None

    # Photo retry on uncertain
    if (
        parsed is not None
        and use_photos_on_uncertain
        and _coerce_verdict(parsed.get("suitability_verdict")) == "uncertain"
        and photo_urls
    ):
        try:
            photo_parsed = _classify_with_photos(row, photo_urls)
        except Exception as exc:
            logger.warning(
                "LLM photo classification raised for aqar_id=%s: %s",
                row.get("aqar_id"),
                exc,
            )
            photo_parsed = None
        if photo_parsed is not None:
            parsed = photo_parsed

    if parsed is None:
        # Total failure — return neutral classification so downstream
        # scoring falls back to the structural restaurant_score.
        return {
            "llm_suitability_verdict": "uncertain",
            "llm_suitability_score": None,
            "llm_listing_quality_score": None,
            "llm_landlord_signal_score": None,
            "llm_reasoning": "LLM classification failed; falling back to structural score.",
            "llm_classified_at": datetime.utcnow(),
            "llm_classifier_version": CLASSIFIER_VERSION,
        }

    return {
        "llm_suitability_verdict": _coerce_verdict(parsed.get("suitability_verdict")),
        "llm_suitability_score": _coerce_int_score(parsed.get("suitability_score")),
        "llm_listing_quality_score": _coerce_int_score(
            parsed.get("listing_quality_score")
        ),
        "llm_landlord_signal_score": _coerce_int_score(
            parsed.get("landlord_signal_score")
        ),
        "llm_reasoning": str(parsed.get("reasoning") or "")[:1000],
        "llm_classified_at": datetime.utcnow(),
        "llm_classifier_version": CLASSIFIER_VERSION,
    }
