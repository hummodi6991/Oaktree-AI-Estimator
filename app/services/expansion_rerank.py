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
import re
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
# Validation (Step 3)
# ---------------------------------------------------------------------------

_REQUIRED_DECISION_KEYS: tuple[str, ...] = (
    "parcel_id",
    "original_rank",
    "new_rank",
    "rerank_reason",
)

_REQUIRED_REASON_FIELDS: tuple[str, ...] = (
    "summary",
    "positives_cited",
    "negatives_cited",
    "comparison_to_displaced_candidate",
)

# Case-insensitive word-boundary matches. Spaces between words are matched
# literally; leading/trailing \b prevents "introduced to" from matching
# "due to" (no word boundary inside "introduced") and "undue toll" from
# matching because "d" is preceded by a word char.
_FORBIDDEN_PHRASES: tuple[str, ...] = (
    "due to",
    "because of",
    "as a result of",
    "leading to",
    "causing",
)
_FORBIDDEN_PHRASE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (p, re.compile(r"\b" + re.escape(p) + r"\b", re.IGNORECASE))
    for p in _FORBIDDEN_PHRASES
)

# Patterns used by the soft anti-hallucination spot-check. We look for
# numerical claims with units that the LLM might fabricate.
_NUMERIC_CLAIM_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(\d+(?:\.\d+)?)\s*%"),
    re.compile(r"(\d+(?:\.\d+)?)\s*SAR", re.IGNORECASE),
    re.compile(r"=\s*(\d+(?:\.\d+)?)"),
)

_MIN_SUMMARY_CHARS = 20
_MIN_COMPARISON_CHARS = 20


def _flatten_numbers(value: Any) -> set[str]:
    """Recursively extract numeric tokens (as normalized strings) from a
    nested value, used for the soft anti-hallucination spot-check."""
    out: set[str] = set()
    if value is None:
        return out
    if isinstance(value, bool):
        return out
    if isinstance(value, (int, float)):
        # Normalize: drop trailing zeros for floats, keep ints as-is.
        if isinstance(value, float) and value.is_integer():
            out.add(str(int(value)))
        out.add(str(value))
        return out
    if isinstance(value, str):
        for m in re.finditer(r"\d+(?:\.\d+)?", value):
            out.add(m.group(0))
        return out
    if isinstance(value, dict):
        for v in value.values():
            out |= _flatten_numbers(v)
        return out
    if isinstance(value, (list, tuple)):
        for v in value:
            out |= _flatten_numbers(v)
        return out
    return out


def _candidate_known_numbers(candidate: dict[str, Any]) -> set[str]:
    """Collect numeric tokens from the candidate's feature_snapshot and
    score_breakdown for the soft spot-check."""
    known: set[str] = set()
    known |= _flatten_numbers(candidate.get("feature_snapshot"))
    known |= _flatten_numbers(candidate.get("score_breakdown"))
    known |= _flatten_numbers(candidate.get("realized_demand"))
    return known


def _spot_check_numeric_claims(
    decision: dict[str, Any], candidate: dict[str, Any]
) -> list[str]:
    """Return a list of unverifiable numeric claims found in the reason's
    positives_cited / negatives_cited. Soft check - caller logs, does not
    fail."""
    reason = decision.get("rerank_reason")
    if not isinstance(reason, dict):
        return []
    known = _candidate_known_numbers(candidate)
    unverifiable: list[str] = []
    for field in ("positives_cited", "negatives_cited"):
        entries = reason.get(field) or []
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, str):
                continue
            for pat in _NUMERIC_CLAIM_PATTERNS:
                for m in pat.finditer(entry):
                    num = m.group(1)
                    norm = num
                    try:
                        f = float(num)
                        if f.is_integer():
                            norm = str(int(f))
                    except ValueError:
                        pass
                    if num not in known and norm not in known:
                        unverifiable.append(entry)
                        break
                else:
                    continue
                break
    return unverifiable


def _find_forbidden_phrase(text: str) -> str | None:
    """Return the first forbidden phrase found in ``text`` (word-boundary,
    case-insensitive), or None."""
    if not isinstance(text, str):
        return None
    for phrase, pattern in _FORBIDDEN_PHRASE_PATTERNS:
        if pattern.search(text):
            return phrase
    return None


def _validate_rerank_response(
    parsed: Any,
    shortlist: list[dict[str, Any]],
    max_move: int,
) -> tuple[bool, list[str]]:
    """Validate an LLM rerank response against the bounded-rerank contract.

    Returns ``(True, [])`` on success, or ``(False, [reasons])`` on the
    first hard-fail. The soft anti-hallucination spot-check (check 11)
    logs a warning but does NOT affect the return value.
    """
    # Check 1: parsed is a dict with key "reranked".
    if not isinstance(parsed, dict) or "reranked" not in parsed:
        return False, ["missing_reranked_key"]

    reranked = parsed["reranked"]

    # Check 2: reranked is a list with length == len(shortlist).
    if not isinstance(reranked, list):
        return False, ["reranked_not_list"]
    if len(reranked) != len(shortlist):
        return False, [
            f"reranked_length_mismatch: got {len(reranked)}, expected {len(shortlist)}"
        ]

    # Check 3: every item has the required keys.
    for i, item in enumerate(reranked):
        if not isinstance(item, dict):
            return False, [f"decision_not_dict_at_index_{i}"]
        missing = [k for k in _REQUIRED_DECISION_KEYS if k not in item]
        if missing:
            return False, [
                f"decision_missing_keys_at_index_{i}: {missing}"
            ]

    # Build lookup maps from the shortlist.
    shortlist_by_pid: dict[Any, dict[str, Any]] = {}
    shortlist_ranks_by_pid: dict[Any, int] = {}
    for c in shortlist:
        pid = c.get("parcel_id") or c.get("id")
        shortlist_by_pid[pid] = c
        shortlist_ranks_by_pid[pid] = c.get(
            "deterministic_rank", c.get("rank")
        )

    # Check 4: set of parcel_ids matches exactly.
    response_pids = [item["parcel_id"] for item in reranked]
    if len(set(response_pids)) != len(response_pids):
        return False, ["duplicate_parcel_id_in_response"]
    if set(response_pids) != set(shortlist_by_pid.keys()):
        extra = set(response_pids) - set(shortlist_by_pid.keys())
        missing = set(shortlist_by_pid.keys()) - set(response_pids)
        return False, [
            f"parcel_id_set_mismatch: extra={sorted(map(str, extra))} "
            f"missing={sorted(map(str, missing))}"
        ]

    n = len(shortlist)

    # Check 5 + 6: original_rank matches, new_rank is int in [1, N].
    for item in reranked:
        pid = item["parcel_id"]
        expected_rank = shortlist_ranks_by_pid[pid]
        if item["original_rank"] != expected_rank:
            return False, [
                f"original_rank_mismatch for {pid}: "
                f"got {item['original_rank']}, expected {expected_rank}"
            ]
        new_rank = item["new_rank"]
        if not isinstance(new_rank, int) or isinstance(new_rank, bool):
            return False, [f"new_rank_not_int for {pid}: {new_rank!r}"]
        if new_rank < 1 or new_rank > n:
            return False, [
                f"new_rank_out_of_range for {pid}: {new_rank} not in [1,{n}]"
            ]

    # Check 7: set of new_ranks is exactly {1..N} (no duplicates, no gaps).
    new_ranks = [item["new_rank"] for item in reranked]
    if set(new_ranks) != set(range(1, n + 1)):
        return False, [
            f"new_rank_set_mismatch: got {sorted(new_ranks)}, "
            f"expected {list(range(1, n + 1))}"
        ]

    # Check 8: |new_rank - original_rank| <= max_move.
    for item in reranked:
        delta = abs(item["new_rank"] - item["original_rank"])
        if delta > max_move:
            return False, [
                f"move_exceeds_max for {item['parcel_id']}: "
                f"delta={delta} > max_move={max_move}"
            ]

    # Check 9 + 10: rerank_reason shape depends on whether the candidate moved.
    for item in reranked:
        pid = item["parcel_id"]
        moved = item["new_rank"] != item["original_rank"]
        reason = item["rerank_reason"]
        if not moved:
            if reason is not None:
                return False, [
                    f"rerank_reason_must_be_null_when_unchanged for {pid}"
                ]
            continue
        # moved -> reason must be a fully populated dict.
        if not isinstance(reason, dict):
            return False, [
                f"rerank_reason_not_dict for moved candidate {pid}"
            ]
        missing = [k for k in _REQUIRED_REASON_FIELDS if k not in reason]
        if missing:
            return False, [
                f"rerank_reason_missing_fields for {pid}: {missing}"
            ]
        summary = reason.get("summary")
        if not isinstance(summary, str) or len(summary.strip()) < _MIN_SUMMARY_CHARS:
            return False, [
                f"rerank_reason_summary_too_short for {pid} "
                f"(min {_MIN_SUMMARY_CHARS} chars)"
            ]
        if not isinstance(reason.get("positives_cited"), list):
            return False, [f"positives_cited_not_list for {pid}"]
        if not isinstance(reason.get("negatives_cited"), list):
            return False, [f"negatives_cited_not_list for {pid}"]
        comp = reason.get("comparison_to_displaced_candidate")
        if not isinstance(comp, str) or len(comp.strip()) < _MIN_COMPARISON_CHARS:
            return False, [
                f"comparison_to_displaced_candidate_too_short for {pid} "
                f"(min {_MIN_COMPARISON_CHARS} chars)"
            ]

    # Check 12 (HARD): forbidden causal phrases (word-boundary, case-insensitive).
    for item in reranked:
        reason = item.get("rerank_reason")
        if not isinstance(reason, dict):
            continue
        pid = item["parcel_id"]
        # summary + comparison (strings)
        for field in ("summary", "comparison_to_displaced_candidate"):
            hit = _find_forbidden_phrase(reason.get(field, ""))
            if hit:
                return False, [
                    f"forbidden_phrase: '{hit}' in {field} of {pid}"
                ]
        # positives_cited + negatives_cited (lists of strings)
        for field in ("positives_cited", "negatives_cited"):
            entries = reason.get(field) or []
            if not isinstance(entries, list):
                continue
            for entry in entries:
                hit = _find_forbidden_phrase(entry if isinstance(entry, str) else "")
                if hit:
                    return False, [
                        f"forbidden_phrase: '{hit}' in {field} of {pid}"
                    ]

    # Check 11 (SOFT): anti-hallucination spot-check. Log warnings only.
    for item in reranked:
        if item["new_rank"] == item["original_rank"]:
            continue
        pid = item["parcel_id"]
        candidate = shortlist_by_pid.get(pid)
        if candidate is None:
            continue
        unverifiable = _spot_check_numeric_claims(item, candidate)
        for claim in unverifiable:
            logger.warning(
                "rerank anti-hallucination: unverifiable numeric claim for "
                "%s: %r",
                pid,
                claim,
            )

    return True, []


def generate_rerank(
    candidates: list[dict[str, Any]],
    brand_profile: dict[str, Any] | None,
) -> list[dict[str, Any]] | None:
    """Public entry point for bounded LLM shortlist reranking.

    Returns the list of rerank decisions (one per shortlist candidate) on
    success, or ``None`` on any failure path (flag off, shortlist below
    minimum, ceiling exceeded, client unavailable, LLM error, JSON parse
    failure, validation failure). The caller preserves deterministic order
    whenever this returns ``None``.
    """
    # 1. Flag check. Never call the client when disabled.
    if not settings.EXPANSION_LLM_RERANK_ENABLED:
        return None

    # 2. Compute shortlist size.
    cap = settings.EXPANSION_LLM_RERANK_SHORTLIST_SIZE
    min_size = settings.EXPANSION_LLM_RERANK_MIN_SHORTLIST
    shortlist_size = min(len(candidates), cap)
    if shortlist_size < min_size:
        return None

    shortlist = candidates[:shortlist_size]
    max_move = settings.EXPANSION_LLM_RERANK_MAX_MOVE

    # 3. Daily cost ceiling (reuses the decision-memo tracker).
    try:
        _check_daily_ceiling()
    except Exception as exc:
        logger.warning(
            "rerank skipped: daily cost ceiling exceeded (candidates=%d, "
            "error=%s: %s)",
            shortlist_size,
            type(exc).__name__,
            exc,
        )
        return None

    # 4. Lazy client.
    try:
        client = _get_client()
    except Exception as exc:
        logger.warning(
            "rerank skipped: LLM client unavailable (candidates=%d, "
            "error=%s: %s)",
            shortlist_size,
            type(exc).__name__,
            exc,
        )
        return None

    # 5. Build messages and call the LLM.
    system_prompt = RERANK_SYSTEM_PROMPT.format(max_move=max_move)
    user_message = _serialize_shortlist_for_prompt(
        shortlist, brand_profile, shortlist_size
    )

    try:
        response = client.chat.completions.create(
            model=settings.EXPANSION_LLM_RERANK_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            response_format={"type": "json_object"},
            temperature=settings.EXPANSION_LLM_RERANK_TEMPERATURE,
            max_tokens=settings.EXPANSION_LLM_RERANK_MAX_TOKENS,
        )
    except Exception as exc:
        logger.warning(
            "rerank LLM call failed (candidates=%d, error=%s: %s)",
            shortlist_size,
            type(exc).__name__,
            exc,
        )
        return None

    # 6. Parse JSON.
    try:
        content = (response.choices[0].message.content or "").strip()
    except Exception as exc:
        logger.warning(
            "rerank response had no content (candidates=%d, error=%s: %s)",
            shortlist_size,
            type(exc).__name__,
            exc,
        )
        return None

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        logger.warning(
            "rerank JSON parse failed (candidates=%d, error=%s, raw=%r)",
            shortlist_size,
            exc,
            content[:1000],
        )
        return None

    # 7. Validate (hard-fail on any violation, discard the entire rerank).
    ok, reasons = _validate_rerank_response(parsed, shortlist, max_move)
    if not ok:
        logger.warning(
            "rerank validation failed (candidates=%d, reasons=%s, raw=%r)",
            shortlist_size,
            reasons,
            content[:1000],
        )
        return None

    # 8. Record cost against the shared daily tracker.
    usage = getattr(response, "usage", None)
    input_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
    output_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
    cost = _record_cost(input_tokens, output_tokens)

    logger.info(
        "rerank generated | candidates=%d prompt_tokens=%d "
        "completion_tokens=%d cost=$%.5f",
        shortlist_size,
        input_tokens,
        output_tokens,
        cost,
    )

    # 9. Return the validated list of decisions.
    return parsed["reranked"]
