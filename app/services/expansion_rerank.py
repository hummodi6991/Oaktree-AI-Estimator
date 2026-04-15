"""Bounded LLM shortlist reranking for the Expansion Advisor (Phase 2).

The deterministic scorer in app.services.expansion_advisor produces a ranked
candidate list. When EXPANSION_LLM_RERANK_ENABLED is True, this module
reranks the top min(len(candidates), shortlist_cap) candidates within
+/-max_move ranks from their deterministic position, producing structured
rerank decisions with auditable evidence. Candidates outside the cap pass
through unchanged.

The LLM never replaces the scorer - it expresses bounded judgment on a
shortlist. Every override carries structured evidence (positives_cited,
negatives_cited, comparison) that the API surfaces and the UI can render.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from app.core.config import settings
from app.services.llm_decision_memo import (
    _FEATURE_SNAPSHOT_WHITELIST,
    _check_daily_ceiling,
    _get_client,
    _record_cost,
)

logger = logging.getLogger(__name__)


RERANK_SYSTEM_PROMPT = """You are a senior commercial real-estate analyst covering Riyadh, Saudi Arabia, evaluating a shortlist of candidate sites that have already been ranked by a deterministic scorer for a specific F&B brand's expansion.

Your job is to identify cases where the deterministic scoring missed an interaction effect - for example, a strong score that hides a disqualifying context, or a mediocre score that masks an exceptional fit. You may rerank candidates within a tight window (defined below). When in doubt, leave the deterministic ranking alone.

You will receive a JSON object containing:
- The brand profile (category, service model, expansion goal, preferences).
- The shortlist: an array of candidates, each with deterministic_rank, final_score, score_breakdown (9 components with weights and contributions), gate_verdicts, feature_snapshot (rent, area, frontage, competitor counts, etc.), and - when present - realized_demand (actual customer order velocity over the last 30 days, by far the highest-quality demand signal available).

Hard rules (violations cause your output to be discarded):

1. You may move any candidate up or down by AT MOST {max_move} positions from its deterministic_rank. A candidate at deterministic_rank #7 may end up anywhere from #2 to #12, no further.

2. You may NOT move a candidate across the shortlist boundary. The shortlist contains exactly N candidates (N is provided in the input). Every new_rank must satisfy 1 <= new_rank <= N. You cannot move shortlist candidates outside it, and you cannot bring in candidates from outside.

3. Every new_rank must be unique across the shortlist (no two candidates may share a rank).

4. For every candidate where new_rank != deterministic_rank, you MUST provide a structured rerank_reason object with all four fields:
   - summary: one human-readable sentence
   - positives_cited: list of specific facts (with numbers and units) that favor this candidate
   - negatives_cited: list of specific facts (with numbers and units) that count against this candidate
   - comparison_to_displaced_candidate: one sentence explaining why this candidate now outranks the one it displaced

   For candidates where new_rank == deterministic_rank, set rerank_reason to null.

5. Realized demand is the strongest signal you have, when present. If a candidate has realized_demand_30d significantly above its peers' median, treat that as a high-priority positive - the deterministic scorer caps its weight at 5% but real customer demand is what determines actual site performance. Cite it in positives_cited when relevant.

6. ANTI-HALLUCINATION GUARANTEES (strictly enforced):
   a. Do NOT assert causal relationships between facts unless the causation is directly supported by the data. Forbidden phrases when joining unrelated facts: "due to", "because of", "as a result of", "leading to", "causing". If two facts are both true but not causally linked, report them separately.
   b. Every numerical claim with units in positives_cited or negatives_cited (e.g., "rent 18% below median", "realized_demand_30d=1400") must be directly derivable from the candidate's feature_snapshot or score_breakdown. Do not invent percentages, dollar amounts, or counts. If you don't have a number for a claim, phrase it qualitatively without inventing one.
   c. Do not use the phrases "overall", "appears to be", "could potentially", or "generally speaking".

Output: a single JSON object with one top-level key "reranked", whose value is an array of exactly N objects (one per shortlist candidate, in any order). Each object has:
{{
  "parcel_id": "string (must match an input candidate)",
  "original_rank": int (must match the candidate's deterministic_rank),
  "new_rank": int (1 to N, must be unique across the array),
  "rerank_reason": object or null (null when new_rank == original_rank, required when they differ)
}}

Return ONLY the JSON object, no markdown fences, no commentary."""


# Hard cap on the user-message JSON payload sent to the LLM. If the fully
# serialized shortlist exceeds this, we progressively trim (comparable
# competitors first, then non-whitelist feature fields, then truncate
# competitor descriptions) until we fit, logging a warning.
_MAX_USER_MESSAGE_CHARS = 16000


def _trim_feature_snapshot(
    snapshot: dict[str, Any] | None, *, whitelist_only: bool
) -> dict[str, Any]:
    """Return a copy of ``snapshot`` trimmed for prompt use.

    When ``whitelist_only`` is True, only keys in ``_FEATURE_SNAPSHOT_WHITELIST``
    are retained. Otherwise the whitelist is preferred but other scalar fields
    are kept if present (non-dict, non-list), with values that serialize
    cleanly to JSON.
    """
    if not snapshot or not isinstance(snapshot, dict):
        return {}
    trimmed: dict[str, Any] = {}
    for k in _FEATURE_SNAPSHOT_WHITELIST:
        if k in snapshot and snapshot[k] is not None:
            trimmed[k] = snapshot[k]
    if whitelist_only:
        return trimmed
    for k, v in snapshot.items():
        if k in trimmed:
            continue
        if v is None:
            continue
        if isinstance(v, (str, int, float, bool)):
            trimmed[k] = v
    return trimmed


def _truncate_competitors(
    competitors: list[dict[str, Any]] | None,
    *,
    keep: int,
    truncate_descriptions: bool,
) -> list[dict[str, Any]]:
    """Return up to ``keep`` comparable competitors, optionally with
    descriptions truncated to ~80 chars."""
    if not competitors or not isinstance(competitors, list):
        return []
    out: list[dict[str, Any]] = []
    for c in competitors[:keep]:
        if not isinstance(c, dict):
            continue
        entry = {
            k: v
            for k, v in c.items()
            if isinstance(v, (str, int, float, bool)) and v is not None
        }
        if truncate_descriptions:
            desc = entry.get("description")
            if isinstance(desc, str) and len(desc) > 80:
                entry["description"] = desc[:77] + "..."
        out.append(entry)
    return out


def _candidate_payload(
    candidate: dict[str, Any],
    *,
    whitelist_only: bool,
    competitor_keep: int,
    truncate_descriptions: bool,
) -> dict[str, Any]:
    """Build the per-candidate dict that goes into the user message."""
    payload: dict[str, Any] = {
        "parcel_id": candidate.get("parcel_id") or candidate.get("id"),
        "deterministic_rank": candidate.get(
            "deterministic_rank", candidate.get("rank")
        ),
        "final_score": candidate.get("final_score", candidate.get("score")),
    }
    breakdown = candidate.get("score_breakdown")
    if breakdown:
        payload["score_breakdown"] = breakdown
    gates = candidate.get("gate_verdicts")
    if gates:
        payload["gate_verdicts"] = gates
    snapshot = _trim_feature_snapshot(
        candidate.get("feature_snapshot"), whitelist_only=whitelist_only
    )
    if snapshot:
        payload["feature_snapshot"] = snapshot
    realized = candidate.get("realized_demand")
    if realized is not None:
        payload["realized_demand"] = realized
    competitors = _truncate_competitors(
        candidate.get("comparable_competitors"),
        keep=competitor_keep,
        truncate_descriptions=truncate_descriptions,
    )
    if competitors:
        payload["comparable_competitors"] = competitors
    return payload


def _serialize_shortlist_for_prompt(
    candidates: list[dict[str, Any]],
    brand_profile: dict[str, Any] | None,
    shortlist_size: int,
) -> str:
    """Serialize the shortlist and brand profile into a compact JSON string
    for the LLM user message.

    Applies progressive trimming to stay under ``_MAX_USER_MESSAGE_CHARS``:
    1. Keep up to 2 comparable competitors per candidate (full descriptions).
    2. Drop to 1 competitor per candidate.
    3. Drop competitors entirely.
    4. Collapse feature_snapshot to the whitelist only.
    5. Drop competitor descriptions (already gone by step 3) and truncate
       remaining description fields to ~80 chars.

    Returns the final JSON string. Logs a warning if the cap was hit.
    """
    shortlist = candidates[:shortlist_size]
    brand_payload = brand_profile or {}

    # Progressive trim tiers (most generous -> most compact).
    tiers = [
        {"whitelist_only": False, "competitor_keep": 2, "truncate_descriptions": False},
        {"whitelist_only": False, "competitor_keep": 1, "truncate_descriptions": False},
        {"whitelist_only": False, "competitor_keep": 0, "truncate_descriptions": False},
        {"whitelist_only": True, "competitor_keep": 0, "truncate_descriptions": False},
        {"whitelist_only": True, "competitor_keep": 0, "truncate_descriptions": True},
    ]

    serialized = ""
    trimmed_at_tier: int | None = None
    for i, tier in enumerate(tiers):
        payload = {
            "brand_profile": brand_payload,
            "shortlist_size": len(shortlist),
            "candidates": [
                _candidate_payload(c, **tier) for c in shortlist
            ],
        }
        serialized = json.dumps(payload, ensure_ascii=False, default=str)
        if len(serialized) <= _MAX_USER_MESSAGE_CHARS:
            if i > 0:
                trimmed_at_tier = i
            break
        trimmed_at_tier = i

    if trimmed_at_tier is not None and len(serialized) > _MAX_USER_MESSAGE_CHARS:
        logger.warning(
            "rerank shortlist payload still exceeds cap after max trimming "
            "(%d chars > %d); sending anyway",
            len(serialized),
            _MAX_USER_MESSAGE_CHARS,
        )
    elif trimmed_at_tier is not None and trimmed_at_tier > 0:
        logger.warning(
            "rerank shortlist payload hit size cap; applied trim tier %d "
            "(final size %d chars)",
            trimmed_at_tier,
            len(serialized),
        )

    return serialized


# ---------------------------------------------------------------------------
# Stubs filled in subsequent steps (Step 3: validation; Step 4: generation).
# ---------------------------------------------------------------------------


def _validate_rerank_response(
    parsed: Any,
    shortlist: list[dict[str, Any]],
    max_move: int,
) -> tuple[bool, list[str]]:
    """Validate an LLM rerank response. Filled in Step 3."""
    raise NotImplementedError("Step 3 will implement _validate_rerank_response")


def generate_rerank(
    candidates: list[dict[str, Any]],
    brand_profile: dict[str, Any] | None,
) -> list[dict[str, Any]] | None:
    """Public entry point for bounded LLM shortlist reranking. Filled in Step 4."""
    raise NotImplementedError("Step 4 will implement generate_rerank")
