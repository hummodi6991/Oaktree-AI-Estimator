"""Unit tests for app.services.expansion_rerank (Phase 2).

All LLM calls are mocked. These tests exercise the rerank module in
isolation; the integration with the search service is covered in
tests/test_expansion_advisor_service.py.
"""
from __future__ import annotations

import json
import logging

import pytest
from unittest.mock import MagicMock, patch

from app.core.config import settings
from app.services import expansion_rerank
from app.services.expansion_rerank import (
    _find_forbidden_phrase,
    generate_rerank,
)
from app.services.llm_decision_memo import _daily_cost_tracker


def _make_mock_response(
    content_dict: dict | str,
    input_tokens: int = 500,
    output_tokens: int = 300,
):
    """Build a mock OpenAI ChatCompletion response (mirrors the helper in
    tests/test_llm_decision_memo.py so mock shapes stay consistent across
    the expansion-advisor LLM layers)."""
    mock = MagicMock()
    if isinstance(content_dict, dict):
        mock.choices = [
            MagicMock(message=MagicMock(content=json.dumps(content_dict)))
        ]
    else:
        mock.choices = [MagicMock(message=MagicMock(content=content_dict))]
    mock.usage = MagicMock(
        prompt_tokens=input_tokens, completion_tokens=output_tokens
    )
    return mock


@pytest.fixture(autouse=True)
def _reset_cost_tracker():
    _daily_cost_tracker.clear()
    yield
    _daily_cost_tracker.clear()


@pytest.fixture
def _rerank_enabled(monkeypatch):
    """Turn on the rerank feature flag for the duration of a test."""
    monkeypatch.setattr(settings, "EXPANSION_LLM_RERANK_ENABLED", True)


def _ok_reason(summary: str = "moved after reweighing signals overall",
               comparison: str = "the displaced candidate has a weaker overall fit",
               positives: list[str] | None = None,
               negatives: list[str] | None = None) -> dict:
    return {
        "summary": summary,
        "positives_cited": positives or [],
        "negatives_cited": negatives or [],
        "comparison_to_displaced_candidate": comparison,
    }


def _shortlist(n: int) -> list[dict]:
    """Build a minimal n-candidate shortlist with deterministic_rank set."""
    return [
        {
            "parcel_id": f"p{i}",
            "deterministic_rank": i,
            "final_score": 1.0 - i * 0.01,
            "feature_snapshot": {"area_m2": 300 + i * 10},
        }
        for i in range(1, n + 1)
    ]


def _unchanged_decisions(n: int) -> list[dict]:
    return [
        {
            "parcel_id": f"p{i}",
            "original_rank": i,
            "new_rank": i,
            "rerank_reason": None,
        }
        for i in range(1, n + 1)
    ]


# ---------------------------------------------------------------------------
# 1. Flag-off returns None without calling client
# ---------------------------------------------------------------------------
def test_flag_off_returns_none_without_client_call(monkeypatch):
    monkeypatch.setattr(settings, "EXPANSION_LLM_RERANK_ENABLED", False)
    with patch.object(expansion_rerank, "_get_client") as mock_get_client:
        result = generate_rerank(_shortlist(10), {"category": "QSR"})
    assert result is None
    assert not mock_get_client.called


# ---------------------------------------------------------------------------
# 2. Shortlist below minimum returns None without LLM call
# ---------------------------------------------------------------------------
def test_below_minimum_shortlist_skips_llm(_rerank_enabled):
    with patch.object(expansion_rerank, "_get_client") as mock_get_client:
        result = generate_rerank(_shortlist(2), {"category": "QSR"})
    assert result is None
    assert not mock_get_client.called


# ---------------------------------------------------------------------------
# 3. Happy path: valid response with no moves
# ---------------------------------------------------------------------------
def test_happy_path_no_moves(_rerank_enabled):
    shortlist = _shortlist(10)
    response_payload = {"reranked": _unchanged_decisions(10)}
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _make_mock_response(
        response_payload
    )
    with patch.object(expansion_rerank, "_get_client", return_value=mock_client):
        result = generate_rerank(shortlist, {"category": "QSR"})
    assert result is not None
    assert len(result) == 10
    assert all(d["rerank_reason"] is None for d in result)
    assert all(d["new_rank"] == d["original_rank"] for d in result)


# ---------------------------------------------------------------------------
# 4. Happy path: one swap (ranks 3 <-> 5) with structured reasons
# ---------------------------------------------------------------------------
def test_happy_path_one_swap(_rerank_enabled):
    shortlist = _shortlist(10)
    decisions = _unchanged_decisions(10)
    decisions[2] = {
        "parcel_id": "p3", "original_rank": 3, "new_rank": 5,
        "rerank_reason": _ok_reason(
            "moved down after reweighing frontage and landlord signal",
            "p5 has a stronger realized-demand profile than this candidate"),
    }
    decisions[4] = {
        "parcel_id": "p5", "original_rank": 5, "new_rank": 3,
        "rerank_reason": _ok_reason(
            "moved up on realized demand and motivated landlord signal",
            "p3 has an unmitigated frontage gap that this candidate lacks"),
    }
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _make_mock_response(
        {"reranked": decisions}
    )
    with patch.object(expansion_rerank, "_get_client", return_value=mock_client):
        result = generate_rerank(shortlist, {"category": "QSR"})
    assert result is not None
    by_pid = {d["parcel_id"]: d for d in result}
    assert by_pid["p3"]["new_rank"] == 5
    assert by_pid["p5"]["new_rank"] == 3
    assert isinstance(by_pid["p3"]["rerank_reason"], dict)
    assert isinstance(by_pid["p5"]["rerank_reason"], dict)


# ---------------------------------------------------------------------------
# 5. Validation failure: move exceeds max (returns None, WARN logged)
# ---------------------------------------------------------------------------
def test_move_exceeds_max_discarded(_rerank_enabled, caplog):
    shortlist = _shortlist(10)
    decisions = _unchanged_decisions(10)
    # Move p1 from 1 to 10 (delta 9 > max 5), and p10 from 10 to 1 so
    # uniqueness is preserved (isolate the max-move failure).
    decisions[0] = {
        "parcel_id": "p1", "original_rank": 1, "new_rank": 10,
        "rerank_reason": _ok_reason(),
    }
    decisions[9] = {
        "parcel_id": "p10", "original_rank": 10, "new_rank": 1,
        "rerank_reason": _ok_reason(),
    }
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _make_mock_response(
        {"reranked": decisions}
    )
    with patch.object(expansion_rerank, "_get_client", return_value=mock_client), \
         caplog.at_level(logging.WARNING, logger="app.services.expansion_rerank"):
        result = generate_rerank(shortlist, {"category": "QSR"})
    assert result is None
    assert any("move_exceeds_max" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# 6. Validation failure: missing parcel_id
# ---------------------------------------------------------------------------
def test_missing_parcel_id_discarded(_rerank_enabled):
    shortlist = _shortlist(5)
    decisions = _unchanged_decisions(5)
    decisions[-1]["parcel_id"] = "p_unknown"  # replace p5
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _make_mock_response(
        {"reranked": decisions}
    )
    with patch.object(expansion_rerank, "_get_client", return_value=mock_client):
        result = generate_rerank(shortlist, {})
    assert result is None


# ---------------------------------------------------------------------------
# 7. Validation failure: duplicate new_rank
# ---------------------------------------------------------------------------
def test_duplicate_new_rank_discarded(_rerank_enabled):
    shortlist = _shortlist(5)
    decisions = _unchanged_decisions(5)
    # Two candidates both assigned new_rank=3.
    decisions[1] = {
        "parcel_id": "p2", "original_rank": 2, "new_rank": 3,
        "rerank_reason": _ok_reason(),
    }
    decisions[2] = {
        "parcel_id": "p3", "original_rank": 3, "new_rank": 3,
        "rerank_reason": None,
    }
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _make_mock_response(
        {"reranked": decisions}
    )
    with patch.object(expansion_rerank, "_get_client", return_value=mock_client):
        result = generate_rerank(shortlist, {})
    assert result is None


# ---------------------------------------------------------------------------
# 8. Validation failure: moved candidate with null rerank_reason
# ---------------------------------------------------------------------------
def test_moved_candidate_with_null_reason_discarded(_rerank_enabled):
    shortlist = _shortlist(5)
    decisions = _unchanged_decisions(5)
    decisions[0] = {
        "parcel_id": "p1", "original_rank": 1, "new_rank": 2,
        "rerank_reason": None,  # moved but no reason
    }
    decisions[1] = {
        "parcel_id": "p2", "original_rank": 2, "new_rank": 1,
        "rerank_reason": _ok_reason(),
    }
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _make_mock_response(
        {"reranked": decisions}
    )
    with patch.object(expansion_rerank, "_get_client", return_value=mock_client):
        result = generate_rerank(shortlist, {})
    assert result is None


# ---------------------------------------------------------------------------
# 9. Forbidden phrase: per-candidate revert (cycle-aware), not whole-response
# ---------------------------------------------------------------------------
def test_forbidden_phrase_reverts_cycle_only(_rerank_enabled, caplog):
    """A forbidden phrase in one candidate reverts that candidate's entire
    permutation cycle but preserves independent clean swaps."""
    shortlist = _shortlist(6)
    decisions = _unchanged_decisions(6)
    # Tainted swap: p1 <-> p2, p1 has "as a result of" in summary.
    decisions[0] = {
        "parcel_id": "p1", "original_rank": 1, "new_rank": 2,
        "rerank_reason": _ok_reason(
            summary="moved down as a result of weaker realized demand signal",
            comparison="the displaced candidate has a weaker overall fit"),
    }
    decisions[1] = {
        "parcel_id": "p2", "original_rank": 2, "new_rank": 1,
        "rerank_reason": _ok_reason(
            summary="promoted after reweighing frontage and landlord signal",
            comparison="the displaced candidate has a weaker overall fit"),
    }
    # Clean swap: p3 <-> p4 (no forbidden phrases).
    decisions[2] = {
        "parcel_id": "p3", "original_rank": 3, "new_rank": 4,
        "rerank_reason": _ok_reason(
            summary="moved down after reweighing frontage and landlord signal",
            comparison="the displaced candidate has a weaker overall fit"),
    }
    decisions[3] = {
        "parcel_id": "p4", "original_rank": 4, "new_rank": 3,
        "rerank_reason": _ok_reason(
            summary="promoted after reweighing frontage and landlord signal",
            comparison="the displaced candidate has a weaker overall fit"),
    }
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _make_mock_response(
        {"reranked": decisions}
    )
    with patch.object(expansion_rerank, "_get_client", return_value=mock_client), \
         caplog.at_level(logging.WARNING, logger="app.services.expansion_rerank"):
        result = generate_rerank(shortlist, {})
    # The tainted cycle (p1 <-> p2) is reverted; clean cycle preserved.
    assert result is not None
    by_pid = {d["parcel_id"]: d for d in result}
    # p1 and p2 reverted to original ranks.
    assert by_pid["p1"]["new_rank"] == 1
    assert by_pid["p1"]["rerank_reason"] is None
    assert by_pid["p2"]["new_rank"] == 2
    assert by_pid["p2"]["rerank_reason"] is None
    # p3 <-> p4 swap preserved.
    assert by_pid["p3"]["new_rank"] == 4
    assert isinstance(by_pid["p3"]["rerank_reason"], dict)
    assert by_pid["p4"]["new_rank"] == 3
    assert isinstance(by_pid["p4"]["rerank_reason"], dict)
    # Warning logged for the tainted candidate.
    assert any(
        "forbidden-phrase" in r.getMessage() and "as a result of" in r.getMessage()
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# 10. Soft validation: unverifiable number logs warning, does NOT fail
# ---------------------------------------------------------------------------
def test_unverifiable_numeric_claim_is_soft(_rerank_enabled, caplog):
    shortlist = _shortlist(5)
    # p1's feature_snapshot has area_m2=310 and no percentage near 99.
    decisions = _unchanged_decisions(5)
    decisions[0] = {
        "parcel_id": "p1", "original_rank": 1, "new_rank": 2,
        "rerank_reason": _ok_reason(
            summary="moved down after reweighing frontage and landlord signal",
            comparison="the displaced candidate has a weaker overall fit",
            positives=["rent 99% below median"]),
    }
    decisions[1] = {
        "parcel_id": "p2", "original_rank": 2, "new_rank": 1,
        "rerank_reason": _ok_reason(),
    }
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _make_mock_response(
        {"reranked": decisions}
    )
    with patch.object(expansion_rerank, "_get_client", return_value=mock_client), \
         caplog.at_level(logging.WARNING, logger="app.services.expansion_rerank"):
        result = generate_rerank(shortlist, {})
    # Soft check: rerank succeeds.
    assert result is not None
    assert len(result) == 5
    # Soft check: a warning was logged about the unverifiable claim.
    assert any(
        "anti-hallucination" in r.getMessage() and "99%" in r.getMessage()
        for r in caplog.records
    ), [r.getMessage() for r in caplog.records]


# ---------------------------------------------------------------------------
# 11. LLM raises exception -> returns None, WARN logged
# ---------------------------------------------------------------------------
def test_llm_exception_returns_none(_rerank_enabled, caplog):
    shortlist = _shortlist(5)
    mock_client = MagicMock()
    mock_client.chat.completions.create.side_effect = TimeoutError("upstream timeout")
    with patch.object(expansion_rerank, "_get_client", return_value=mock_client), \
         caplog.at_level(logging.WARNING, logger="app.services.expansion_rerank"):
        result = generate_rerank(shortlist, {})
    assert result is None
    assert any(
        "TimeoutError" in r.getMessage() or "LLM call failed" in r.getMessage()
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# 12. Cost recorded on success with exact token counts
# ---------------------------------------------------------------------------
def test_cost_recorded_on_success(_rerank_enabled):
    shortlist = _shortlist(5)
    response = {"reranked": _unchanged_decisions(5)}
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _make_mock_response(
        response, input_tokens=500, output_tokens=300
    )
    # _record_cost must be patched at the import site inside
    # expansion_rerank - patching app.services.llm_decision_memo._record_cost
    # would not affect the already-imported local reference.
    with patch.object(expansion_rerank, "_get_client", return_value=mock_client), \
         patch.object(expansion_rerank, "_record_cost", return_value=0.00012) as mock_cost:
        result = generate_rerank(shortlist, {})
    assert result is not None
    mock_cost.assert_called_once_with(500, 300)


# ---------------------------------------------------------------------------
# 13. Forbidden-phrase regex: variants hit, false-positive neighbors don't
# ---------------------------------------------------------------------------
def test_forbidden_phrase_case_and_boundary_variants():
    # All three case variants must hit.
    assert _find_forbidden_phrase("moved up as a result of realized demand") == "as a result of"
    assert _find_forbidden_phrase("moved up As A Result Of realized demand") == "as a result of"
    assert _find_forbidden_phrase("moved up AS A RESULT OF realized demand") == "as a result of"

    # Non-matches (word-boundary + substring-neighbor guards). "due to"
    # and "because of" were intentionally removed from the blocklist
    # because the LLM uses them reflexively as plain English connectives.
    assert _find_forbidden_phrase("moved up due to realized demand") is None
    assert _find_forbidden_phrase("strong frontage because of the corner") is None
    assert _find_forbidden_phrase("introduced to the market") is None
    assert _find_forbidden_phrase("introducing to market") is None

    # Other forbidden phrases covered too.
    assert _find_forbidden_phrase("high signal leading to approval") == "leading to"
    assert _find_forbidden_phrase("changes AS A RESULT OF the audit") == "as a result of"
    assert _find_forbidden_phrase("latency issues causing churn") == "causing"


# ---------------------------------------------------------------------------
# Phase 4 — rerank prompt must not gain listing_age / district_momentum
# visibility. The whitelist split in llm_decision_memo.py is invisible
# unless we assert the trim output is byte-identical with and without
# the new Phase 3a/3b keys in the input snapshot.
# ---------------------------------------------------------------------------


def test_rerank_trim_ignores_listing_age_and_district_momentum():
    """_trim_feature_snapshot output must be byte-identical whether or
    not the input snapshot carries listing_age / district_momentum.
    The scalar fallback (isinstance str/int/float/bool) rejects dicts
    on its own, but the whitelist path is authoritative, so this
    test double-locks the guarantee."""
    from app.services.expansion_rerank import _trim_feature_snapshot

    baseline = {
        "area_m2": 200.0,
        "estimated_annual_rent_sar": 180000.0,
        "competitor_count": 4,
        "population_reach": 15000,
        "realized_demand_30d": 42.0,
    }

    enriched = dict(baseline)
    enriched["listing_age"] = {"effective_age_days": 3, "source": "aqar_created"}
    enriched["district_momentum"] = {
        "momentum_score": 82.0,
        "sample_floor_applied": False,
        "activity_30d": 120,
    }

    for whitelist_only in (True, False):
        out_baseline = _trim_feature_snapshot(
            baseline, whitelist_only=whitelist_only
        )
        out_enriched = _trim_feature_snapshot(
            enriched, whitelist_only=whitelist_only
        )
        assert out_baseline == out_enriched, (
            f"rerank trim surfaced Phase 4 dict keys "
            f"(whitelist_only={whitelist_only}): {out_enriched}"
        )
        # Explicit: no dict values in the rerank payload from Phase 4 keys.
        assert "listing_age" not in out_enriched
        assert "district_momentum" not in out_enriched
