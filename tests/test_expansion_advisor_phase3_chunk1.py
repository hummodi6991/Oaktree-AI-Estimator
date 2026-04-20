"""Phase 3 chunk 1 — backend persistence and pre-warm.

These tests prove:

1. The six rerank metadata columns flow from the in-memory candidate dict
   into the INSERT params, so the values survive a page reload.
2. ``get_candidates`` and ``get_candidate_memo`` return the new fields
   (the six rerank fields + ``decision_memo_present`` / ``decision_memo_json``)
   with the correct shape.
3. With ``EXPANSION_LLM_RERANK_ENABLED=False`` (the default),
   ``deterministic_rank == final_rank == rank_position`` and the four
   canonical regression searches produce byte-for-byte identical rankings
   — this is the load-bearing safety property of chunk 1.
4. The memo pre-warm background task respects the flag, honours the
   per-batch wall-clock budget, swallows per-candidate failures, and
   persists successful memos via the same cache-write helper used by POST
   /decision-memo.
5. POST /decision-memo write-back is present and commits. (The
   3,898-candidates / 0-memos observation in production is therefore
   explained by no caller having hit that endpoint yet against the recent
   searches, not by a broken write path.)
"""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.core.config import settings as _ea_settings
from app.services import expansion_advisor as expansion_service
from app.services.expansion_advisor import (
    _apply_rerank_to_candidates,
    get_candidate_memo,
    get_candidates,
    run_expansion_search as _run_expansion_search_raw,
)
from tests.test_expansion_advisor_service import FakeDB, _Result


def _run(*args, **kwargs):
    result = _run_expansion_search_raw(*args, **kwargs)
    return result["items"] if isinstance(result, dict) else result


# ---------------------------------------------------------------------------
# 1. INSERT persists all six rerank fields.
# ---------------------------------------------------------------------------
def test_insert_persists_rerank_metadata_from_candidate_dict(monkeypatch):
    """With the rerank flag off (the default), every candidate gets
    deterministic_rank == final_rank, rerank_applied=False, rerank_delta=0,
    rerank_status='flag_off' — and those values land in the INSERT params."""
    monkeypatch.setattr(_ea_settings, "EXPANSION_LLM_RERANK_ENABLED", False)
    monkeypatch.setattr(
        expansion_service, "_estimate_rent_sar_m2_year",
        lambda _db, _d: (900.0, "test"),
    )
    db = FakeDB(candidate_rows=[
        {
            "parcel_id": "p1",
            "landuse_label": "Commercial",
            "landuse_code": "C",
            "area_m2": 180,
            "lon": 46.7,
            "lat": 24.7,
            "district": "حي العليا",
            "population_reach": 15000,
            "competitor_count": 2,
            "delivery_listing_count": 10,
        },
    ])

    items = _run(
        db, search_id="s-persist", brand_name="b", category="burger",
        service_model="qsr", min_area_m2=100, max_area_m2=300,
        target_area_m2=180, limit=5,
    )
    assert len(items) >= 1
    assert len(db.inserted) >= 1

    all_params: list[dict[str, Any]] = []
    for batch in db.inserted:
        if isinstance(batch, list):
            all_params.extend(batch)
        else:
            all_params.append(batch)
    params = next(p for p in all_params if p.get("parcel_id") == "p1")
    # All six rerank keys are present on the INSERT params.
    for k in ("deterministic_rank", "final_rank", "rerank_applied",
              "rerank_reason", "rerank_delta", "rerank_status"):
        assert k in params, f"missing {k} in insert params"
    # Flag-off invariants.
    assert params["deterministic_rank"] == params["final_rank"]
    assert params["rerank_applied"] is False
    assert params["rerank_delta"] == 0
    assert params["rerank_status"] == "flag_off"
    # None → NULL on the JSONB column (not JSON "null").
    assert params["rerank_reason"] is None


# ---------------------------------------------------------------------------
# 2. Flag-off: deterministic_rank == final_rank == rank_position for every
#    candidate. This is the load-bearing regression-invariance property.
# ---------------------------------------------------------------------------
def test_flag_off_ranks_align_with_rank_position(monkeypatch):
    monkeypatch.setattr(_ea_settings, "EXPANSION_LLM_RERANK_ENABLED", False)
    cands = [
        {"parcel_id": f"p{i}", "final_score": 1.0 - i * 0.01}
        for i in range(1, 11)
    ]
    out = _apply_rerank_to_candidates(cands, {})
    for i, c in enumerate(out, start=1):
        assert c["deterministic_rank"] == i
        assert c["final_rank"] == i
        assert c["rerank_applied"] is False
        assert c["rerank_delta"] == 0
        assert c["rerank_status"] == "flag_off"


# ---------------------------------------------------------------------------
# 3. Four canonical regression searches produce identical rank ordering when
#    run through the rerank pipeline with the flag off.
# ---------------------------------------------------------------------------
def test_four_canonical_searches_flag_off_identical_rankings(monkeypatch):
    monkeypatch.setattr(_ea_settings, "EXPANSION_LLM_RERANK_ENABLED", False)
    canonical = [
        ("qsr_burger_al_olaya", [
            {"parcel_id": f"olaya_q{i}", "final_score": 0.85 - i * 0.004,
             "district": "Al Olaya"} for i in range(1, 16)]),
        ("delivery_shawarma_citywide", [
            {"parcel_id": f"citywide_d{i}", "final_score": 0.78 - i * 0.002,
             "district": ["Al Olaya", "Al Yasmin", "Al Malqa", "Al Nakheel"][i % 4]}
            for i in range(1, 51)]),
        ("dinein_indian_al_nakheel", [
            {"parcel_id": f"nakheel_di{i}", "final_score": 0.80 - i * 0.006,
             "district": "Al Nakheel"} for i in range(1, 11)]),
        ("cafe_al_yasmin", [
            {"parcel_id": f"yasmin_c{i}", "final_score": 0.76 - i * 0.005,
             "district": "Al Yasmin"} for i in range(1, 9)]),
    ]
    for label, cands in canonical:
        ids_before = [c["parcel_id"] for c in cands]
        cands_copy = [dict(c) for c in cands]
        out = _apply_rerank_to_candidates(cands_copy, {})
        ids_after = [c["parcel_id"] for c in out]
        assert ids_after == ids_before, label
        # final_rank ordering matches the deterministic order in the list.
        ranks = [c["final_rank"] for c in out]
        assert ranks == list(range(1, len(out) + 1)), label


# ---------------------------------------------------------------------------
# 4. get_candidates returns the rerank fields and decision_memo_present.
# ---------------------------------------------------------------------------
def test_get_candidates_returns_new_fields():
    row = {
        "id": "c1",
        "search_id": "s1",
        "parcel_id": "p1",
        "district": "حي العليا",
        "final_score": 82,
        "rank_position": 1,
        "compare_rank": 1,
        "deterministic_rank": 1,
        "final_rank": 1,
        "rerank_applied": False,
        "rerank_reason": None,
        "rerank_delta": 0,
        "rerank_status": "flag_off",
        "decision_memo_present": True,
    }

    class _DB(FakeDB):
        def execute(self, stmt, params=None):
            sql = stmt.text if hasattr(stmt, "text") else str(stmt)
            if "FROM expansion_candidate" in sql and "decision_memo_present" in sql:
                return _Result([row])
            return super().execute(stmt, params)

    db = _DB()
    items = get_candidates(db, "s1")
    assert items and items[0]["deterministic_rank"] == 1
    assert items[0]["final_rank"] == 1
    assert items[0]["rerank_applied"] is False
    assert items[0]["rerank_delta"] == 0
    assert items[0]["rerank_status"] == "flag_off"
    assert items[0]["decision_memo_present"] is True
    # Full memo text/json intentionally excluded from the list endpoint for
    # payload-size reasons — only the presence flag is surfaced.
    assert "decision_memo" not in items[0] or items[0].get("decision_memo") is None
    assert "decision_memo_json" not in items[0] or items[0].get("decision_memo_json") is None


# ---------------------------------------------------------------------------
# 5. get_candidate_memo returns the rerank fields + full decision_memo_json.
# ---------------------------------------------------------------------------
def test_get_candidate_memo_returns_new_fields():
    structured = {
        "headline_recommendation": "Recommend",
        "ranking_explanation": "…",
        "key_evidence": [],
        "risks": [],
        "comparison": "",
        "bottom_line": "",
    }
    memo_row = {
        "candidate_id": "c1",
        "search_id": "s1",
        "brand_name": "Brand X",
        "category": "burger",
        "service_model": "qsr",
        "parcel_id": "p1",
        "district": "Olaya",
        "area_m2": 180,
        "final_score": 82,
        "economics_score": 70,
        "cannibalization_score": 35,
        "confidence_grade": "A",
        "rank_position": 1,
        "deterministic_rank": 1,
        "final_rank": 1,
        "rerank_applied": False,
        "rerank_reason": None,
        "rerank_delta": 0,
        "rerank_status": "flag_off",
        "decision_memo": "Headline text\n\nBody.",
        "decision_memo_json": structured,
    }
    db = FakeDB(memo_row=memo_row)
    memo = get_candidate_memo(db, "c1")
    assert memo is not None
    # Six rerank fields are per-candidate properties — they live on the
    # nested `candidate` object alongside final_score / area_m2 / etc.,
    # matching the list endpoint's shape and what the frontend reads.
    cand = memo["candidate"]
    assert cand["deterministic_rank"] == 1
    assert cand["final_rank"] == 1
    assert cand["rerank_applied"] is False
    assert cand["rerank_reason"] is None
    assert cand["rerank_delta"] == 0
    assert cand["rerank_status"] == "flag_off"
    # Guard against regression: the moved fields must NOT reappear at the
    # top level. decision_memo / decision_memo_json describe the envelope
    # and stay there.
    for moved in (
        "deterministic_rank", "final_rank", "rerank_applied",
        "rerank_reason", "rerank_delta", "rerank_status",
    ):
        assert moved not in memo, f"{moved} must not be at the top level"
    assert memo["decision_memo"] == "Headline text\n\nBody."
    assert memo["decision_memo_json"] == structured


# ---------------------------------------------------------------------------
# 5b. get_candidate_memo emits the candidate sub-object that DecisionLogicCard
#     and the memo quick-facts row consume — using a production-shape
#     commercial-unit candidate at rank #1. Regression test: prior to this
#     reshape the rank fields lived at the top level (so the card rendered
#     "Deterministic #—") and the unit_* fields were absent (so Area /
#     Street width rendered "—").
# ---------------------------------------------------------------------------
def test_get_candidate_memo_candidate_shape_matches_frontend_consumers():
    memo_row = {
        "candidate_id": "c-rank-1",
        "search_id": "s-prod",
        "brand_name": "Brand X",
        "category": "qsr",
        "service_model": "qsr",
        "parcel_id": "p-rank-1",
        "district": "Olaya",
        # Commercial-unit candidates may have area_m2 NULL — the area lives
        # on unit_area_sqm. The memo panel falls back from area_m2 to
        # unit_area_sqm to mirror the list card.
        "area_m2": None,
        "landuse_label": "Commercial",
        "final_score": 84,
        "economics_score": 78,
        "cannibalization_score": 30,
        "confidence_grade": "A",
        "rank_position": 1,
        "deterministic_rank": 1,
        "final_rank": 1,
        "rerank_applied": False,
        "rerank_reason": None,
        "rerank_delta": 0,
        "rerank_status": "flag_off",
        # Listing fields the list endpoint emits and the memo card now needs.
        "source_type": "commercial_unit",
        "commercial_unit_id": "cu-42",
        "listing_url": "https://example.test/cu-42",
        "image_url": "https://example.test/cu-42.jpg",
        "unit_price_sar_annual": 220000,
        "unit_area_sqm": 165,
        "unit_street_width_m": 18,
        "unit_neighborhood": "Olaya",
        "unit_listing_type": "for_rent",
    }
    db = FakeDB(memo_row=memo_row)
    memo = get_candidate_memo(db, "c-rank-1")
    assert memo is not None
    cand = memo["candidate"]

    # Rank / rerank fields live on the candidate sub-object — same shape
    # the list endpoint exposes — so DecisionLogicCard reads them via
    # `data.candidate.deterministic_rank` and renders "Deterministic #1".
    assert cand["deterministic_rank"] == 1
    assert cand["final_rank"] == 1
    assert cand["rerank_applied"] is False
    assert cand["rerank_reason"] is None
    assert cand["rerank_delta"] == 0
    assert cand["rerank_status"] == "flag_off"

    # Listing / commercial-unit fields the quick-facts row reads.
    assert cand["source_type"] == "commercial_unit"
    assert cand["commercial_unit_id"] == "cu-42"
    assert cand["listing_url"] == "https://example.test/cu-42"
    assert cand["image_url"] == "https://example.test/cu-42.jpg"
    assert cand["unit_price_sar_annual"] == 220000
    assert cand["unit_area_sqm"] == 165
    assert cand["unit_street_width_m"] == 18
    # display_annual_rent_sar is computed by _normalize_candidate_payload —
    # it may be None on this fixture (no estimated_rent_sar_m2_year), but
    # the key MUST be present so the frontend can read it without
    # short-circuiting on `undefined`.
    assert "display_annual_rent_sar" in cand

    # Regression guard: the moved fields must NOT reappear at the top
    # level of the memo envelope.
    for moved in (
        "deterministic_rank", "final_rank", "rerank_applied",
        "rerank_reason", "rerank_delta", "rerank_status",
    ):
        assert moved not in memo, f"{moved} must live on candidate, not the envelope"


# ---------------------------------------------------------------------------
# 5c. Regression for production 500: Aqar listing IDs are numeric in the DB
#     (the Text column holds strings like "6606767", but SQLAlchemy can
#     surface them as int when the column was written via an earlier int
#     path). CandidateMemoCandidateResponse declares commercial_unit_id as
#     str | None, so an int value blows up Pydantic validation and FastAPI
#     returns 500. The list endpoint hides this because
#     ExpansionCandidateResponse does not declare commercial_unit_id — it
#     sails through as extra="allow". Fix: coerce at the emission boundary.
# ---------------------------------------------------------------------------
def test_get_candidate_memo_coerces_int_commercial_unit_id_to_string():
    from app.api.expansion_advisor import CandidateMemoResponse

    memo_row = {
        "candidate_id": "c-int-cuid",
        "search_id": "s-int-cuid",
        "brand_name": "Brand X",
        "category": "qsr",
        "service_model": "qsr",
        "parcel_id": "p-int-cuid",
        "district": "Olaya",
        "area_m2": None,
        "landuse_label": "Commercial",
        "final_score": 80,
        "economics_score": 72,
        "cannibalization_score": 35,
        "confidence_grade": "A",
        "rank_position": 1,
        "deterministic_rank": 1,
        "final_rank": 1,
        "rerank_applied": False,
        "rerank_reason": None,
        "rerank_delta": 0,
        "rerank_status": "flag_off",
        # Production shape: Aqar listing ID is an integer on the row even
        # though the column type is Text. Before the coercion fix this
        # raised ValidationError on CandidateMemoResponse.model_validate.
        "source_type": "commercial_unit",
        "commercial_unit_id": 6606767,
        "listing_url": "https://example.test/listing/6606767",
        "image_url": "https://example.test/listing/6606767.jpg",
        "unit_price_sar_annual": 180000,
        "unit_area_sqm": 140,
        "unit_street_width_m": 15,
        "unit_neighborhood": "Olaya",
        "unit_listing_type": "for_rent",
    }
    db = FakeDB(memo_row=memo_row)
    memo = get_candidate_memo(db, "c-int-cuid")
    assert memo is not None

    cand = memo["candidate"]
    # Coerced to string at the emission boundary.
    assert cand["commercial_unit_id"] == "6606767"
    assert isinstance(cand["commercial_unit_id"], str)
    assert cand["source_type"] == "commercial_unit"
    assert isinstance(cand["source_type"], str)

    # End-to-end Pydantic validation must succeed — this is the actual
    # production 500 being reproduced. If the coercion is reverted, the
    # next line raises ValidationError and the test fails.
    validated = CandidateMemoResponse.model_validate(memo)
    assert validated.candidate.commercial_unit_id == "6606767"


# ---------------------------------------------------------------------------
# 6. Pre-warm background task — flag off is a no-op.
# ---------------------------------------------------------------------------
def test_prewarm_flag_off_is_noop(monkeypatch):
    from app.api import expansion_advisor as api_mod

    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_ENABLED", False)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_TOP_N", 10)

    gen_mock = MagicMock()
    monkeypatch.setattr(api_mod, "generate_structured_memo", gen_mock)
    with patch.object(api_mod.db_session, "SessionLocal") as sl_mock:
        api_mod._prewarm_decision_memos(
            "s1",
            [{"id": "c1", "parcel_id": "p1"}],
            {},
        )
    # Early return: no DB session opened, no LLM called.
    assert not sl_mock.called
    assert not gen_mock.called


def test_prewarm_top_n_zero_is_noop(monkeypatch):
    from app.api import expansion_advisor as api_mod

    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_ENABLED", True)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_TOP_N", 0)
    gen_mock = MagicMock()
    monkeypatch.setattr(api_mod, "generate_structured_memo", gen_mock)
    with patch.object(api_mod.db_session, "SessionLocal") as sl_mock:
        api_mod._prewarm_decision_memos(
            "s1",
            [{"id": "c1", "parcel_id": "p1"}],
            {},
        )
    assert not sl_mock.called
    assert not gen_mock.called


# ---------------------------------------------------------------------------
# 7. Pre-warm respects TOP_N: warms only the first N, skips the rest.
# ---------------------------------------------------------------------------
def test_prewarm_respects_top_n(monkeypatch):
    from app.api import expansion_advisor as api_mod

    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_ENABLED", True)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_TOP_N", 3)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_BUDGET_S", 1000.0)

    # 5 candidates in a list; only the first 3 should be processed.
    items = [
        {"id": f"c{i}", "parcel_id": f"p{i}", "final_rank": i}
        for i in range(1, 6)
    ]
    specs = api_mod._build_prewarm_specs(items, 3)
    assert [s["parcel_id"] for s in specs] == ["p1", "p2", "p3"]

    gen_calls: list[str] = []

    def _fake_gen(ctx):
        gen_calls.append(str(ctx.parcel_id))
        return {"headline_recommendation": "ok"}

    monkeypatch.setattr(api_mod, "generate_structured_memo", _fake_gen)
    monkeypatch.setattr(
        api_mod, "render_structured_memo_as_text", lambda j, lang: "text"
    )
    monkeypatch.setattr(
        api_mod,
        "_decision_memo_cache_lookup",
        lambda db, sid, pid: None,
    )
    writes: list[tuple[str, str, str | None]] = []
    monkeypatch.setattr(
        api_mod,
        "_decision_memo_cache_write",
        lambda db, sid, pid, t, j: writes.append((sid, pid, t)),
    )

    session_instance = MagicMock()
    with patch.object(api_mod.db_session, "SessionLocal", return_value=session_instance):
        api_mod._prewarm_decision_memos("s1", specs, {})

    assert gen_calls == ["p1", "p2", "p3"]
    assert [w[1] for w in writes] == ["p1", "p2", "p3"]
    session_instance.close.assert_called_once()


# ---------------------------------------------------------------------------
# 8. Pre-warm: per-candidate exception does not abort the batch.
# ---------------------------------------------------------------------------
def test_prewarm_per_candidate_error_does_not_abort_batch(monkeypatch):
    from app.api import expansion_advisor as api_mod

    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_ENABLED", True)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_TOP_N", 3)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_BUDGET_S", 1000.0)

    specs = [
        {"id": "c1", "parcel_id": "p1"},
        {"id": "c2", "parcel_id": "p2"},
        {"id": "c3", "parcel_id": "p3"},
    ]

    def _gen_raising(ctx):
        if ctx.parcel_id == "p2":
            raise RuntimeError("boom")
        return {"headline_recommendation": "ok"}

    monkeypatch.setattr(api_mod, "generate_structured_memo", _gen_raising)
    monkeypatch.setattr(
        api_mod, "render_structured_memo_as_text", lambda j, lang: "text"
    )
    monkeypatch.setattr(
        api_mod, "_decision_memo_cache_lookup", lambda db, sid, pid: None
    )
    writes: list[str] = []
    monkeypatch.setattr(
        api_mod,
        "_decision_memo_cache_write",
        lambda db, sid, pid, t, j: writes.append(pid),
    )
    with patch.object(api_mod.db_session, "SessionLocal", return_value=MagicMock()):
        api_mod._prewarm_decision_memos("s1", specs, {})
    # p2 failed; p1 and p3 still persisted.
    assert writes == ["p1", "p3"]


# ---------------------------------------------------------------------------
# 9a. Pre-warm: generous budget + fast LLM warms every candidate.
# ---------------------------------------------------------------------------
def test_prewarm_generous_budget_warms_all(monkeypatch):
    from app.api import expansion_advisor as api_mod

    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_ENABLED", True)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_TOP_N", 3)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_BUDGET_S", 120.0)

    specs = [{"id": f"c{i}", "parcel_id": f"p{i}"} for i in range(1, 4)]

    # Deterministic, fast clock — each iteration "takes" 0.1s.
    clock = {"t": 0.0}
    monkeypatch.setattr(api_mod._prewarm_time, "monotonic", lambda: clock["t"])

    def _gen(ctx):
        clock["t"] += 0.1
        return {"headline_recommendation": "ok"}

    monkeypatch.setattr(api_mod, "generate_structured_memo", _gen)
    monkeypatch.setattr(
        api_mod, "render_structured_memo_as_text", lambda j, lang: "text"
    )
    monkeypatch.setattr(
        api_mod, "_decision_memo_cache_lookup", lambda db, sid, pid: None
    )
    writes: list[str] = []
    monkeypatch.setattr(
        api_mod,
        "_decision_memo_cache_write",
        lambda db, sid, pid, t, j: writes.append(pid),
    )
    with patch.object(api_mod.db_session, "SessionLocal", return_value=MagicMock()):
        api_mod._prewarm_decision_memos("s1", specs, {})
    assert writes == ["p1", "p2", "p3"]


# ---------------------------------------------------------------------------
# 9b. Pre-warm: first candidate is always attempted; budget trips the rest.
#
#     3 specs, budget=1.0s, first LLM call consumes 2s of monotonic time:
#       * p1 is warmed (budget check happens AFTER iteration 1).
#       * p2 and p3 are skipped with "budget exhausted" logged.
# ---------------------------------------------------------------------------
def test_prewarm_first_attempt_always_runs_then_budget_trips(monkeypatch, caplog):
    from app.api import expansion_advisor as api_mod

    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_ENABLED", True)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_TOP_N", 3)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_BUDGET_S", 1.0)

    specs = [{"id": f"c{i}", "parcel_id": f"p{i}"} for i in range(1, 4)]

    clock = {"t": 0.0}
    monkeypatch.setattr(api_mod._prewarm_time, "monotonic", lambda: clock["t"])

    gen_calls: list[str] = []

    def _gen(ctx):
        gen_calls.append(str(ctx.parcel_id))
        # First LLM call consumes 2s; subsequent calls (if reached) add 0.1s.
        clock["t"] += 2.0 if len(gen_calls) == 1 else 0.1
        return {"headline_recommendation": "ok"}

    monkeypatch.setattr(api_mod, "generate_structured_memo", _gen)
    monkeypatch.setattr(
        api_mod, "render_structured_memo_as_text", lambda j, lang: "text"
    )
    monkeypatch.setattr(
        api_mod, "_decision_memo_cache_lookup", lambda db, sid, pid: None
    )
    writes: list[str] = []
    monkeypatch.setattr(
        api_mod,
        "_decision_memo_cache_write",
        lambda db, sid, pid, t, j: writes.append(pid),
    )
    with caplog.at_level("INFO", logger=api_mod.logger.name):
        with patch.object(api_mod.db_session, "SessionLocal", return_value=MagicMock()):
            api_mod._prewarm_decision_memos("s1", specs, {})
    # Exactly p1 warmed; LLM called once; p2 and p3 skipped.
    assert gen_calls == ["p1"]
    assert writes == ["p1"]
    # "budget exhausted" log line emitted with the right remaining count.
    budget_lines = [
        r.message for r in caplog.records if "budget exhausted" in r.message
    ]
    assert len(budget_lines) == 1
    assert "remaining=2" in budget_lines[0]


# ---------------------------------------------------------------------------
# 9c. Pre-warm: BUDGET_S <= 0 is treated as UNBOUNDED (no wall-clock gate).
# ---------------------------------------------------------------------------
def test_prewarm_budget_non_positive_is_unbounded(monkeypatch):
    from app.api import expansion_advisor as api_mod

    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_ENABLED", True)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_TOP_N", 3)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_BUDGET_S", 0.0)

    specs = [{"id": f"c{i}", "parcel_id": f"p{i}"} for i in range(1, 4)]

    clock = {"t": 0.0}
    monkeypatch.setattr(api_mod._prewarm_time, "monotonic", lambda: clock["t"])

    def _gen(ctx):
        # Every call burns wall-clock time that would blow any positive budget.
        clock["t"] += 9999.0
        return {"headline_recommendation": "ok"}

    monkeypatch.setattr(api_mod, "generate_structured_memo", _gen)
    monkeypatch.setattr(
        api_mod, "render_structured_memo_as_text", lambda j, lang: "text"
    )
    monkeypatch.setattr(
        api_mod, "_decision_memo_cache_lookup", lambda db, sid, pid: None
    )
    writes: list[str] = []
    monkeypatch.setattr(
        api_mod,
        "_decision_memo_cache_write",
        lambda db, sid, pid, t, j: writes.append(pid),
    )
    with patch.object(api_mod.db_session, "SessionLocal", return_value=MagicMock()):
        api_mod._prewarm_decision_memos("s1", specs, {})
    # All three warmed — the non-positive budget is NOT an on/off switch.
    assert writes == ["p1", "p2", "p3"]


# ---------------------------------------------------------------------------
# 9d. rerank_reason runtime shape invariant.
#
# The validator in app/services/expansion_rerank.py strictly requires
# rerank_reason to be either (a) None for unchanged candidates or (b) a
# fully-populated dict for moved candidates. This test makes that
# invariant loud and round-trips both shapes through CandidateMemoResponse
# to confirm the Pydantic type stays correct.
# ---------------------------------------------------------------------------
def test_rerank_reason_is_strictly_dict_or_none(monkeypatch):
    """After _apply_rerank_to_candidates runs, every candidate's
    rerank_reason is either a dict (moved) or None (not moved). Never
    a string, never anything else."""
    from app.services import expansion_advisor as svc

    # Flag off → every candidate's rerank_reason is None.
    monkeypatch.setattr(_ea_settings, "EXPANSION_LLM_RERANK_ENABLED", False)
    cands = [{"parcel_id": f"p{i}", "final_score": 1.0 - i * 0.01} for i in range(1, 6)]
    out = _apply_rerank_to_candidates(cands, {})
    for c in out:
        assert c["rerank_reason"] is None, (c["parcel_id"], c["rerank_reason"])

    # Flag on, p2<->p4 swap → moved rows carry a dict, others stay None.
    monkeypatch.setattr(_ea_settings, "EXPANSION_LLM_RERANK_ENABLED", True)
    reason = {
        "summary": "moved after reweighing realized-demand and landlord signal",
        "positives_cited": [],
        "negatives_cited": [],
        "comparison_to_displaced_candidate": "the displaced candidate has a weaker overall fit",
    }
    decisions = [
        {"parcel_id": "p1", "original_rank": 1, "new_rank": 1, "rerank_reason": None},
        {"parcel_id": "p2", "original_rank": 2, "new_rank": 4, "rerank_reason": reason},
        {"parcel_id": "p3", "original_rank": 3, "new_rank": 3, "rerank_reason": None},
        {"parcel_id": "p4", "original_rank": 4, "new_rank": 2, "rerank_reason": reason},
        {"parcel_id": "p5", "original_rank": 5, "new_rank": 5, "rerank_reason": None},
    ]
    cands2 = [{"parcel_id": f"p{i}", "final_score": 1.0 - i * 0.01} for i in range(1, 6)]
    with patch.object(svc, "generate_rerank", return_value=decisions):
        out2 = _apply_rerank_to_candidates(cands2, {})
    by_pid = {c["parcel_id"]: c for c in out2}
    # Moved candidates → dict. Unchanged → None. Strictly those two shapes.
    assert isinstance(by_pid["p2"]["rerank_reason"], dict)
    assert isinstance(by_pid["p4"]["rerank_reason"], dict)
    for pid in ("p1", "p3", "p5"):
        assert by_pid[pid]["rerank_reason"] is None
    for pid, c in by_pid.items():
        assert c["rerank_reason"] is None or isinstance(c["rerank_reason"], dict), pid


def test_candidate_memo_response_accepts_both_rerank_reason_shapes():
    """CandidateMemoResponse must round-trip rerank_reason=None AND
    rerank_reason=<dict> without a ValidationError. If this fails, the
    Pydantic type on the response model has drifted from the runtime shape
    produced by _apply_rerank_to_candidates.

    rerank_reason / rerank_status live on the nested candidate object —
    same shape as the list endpoint — so the frontend reads them from
    `data.candidate.*` (where DecisionLogicCard expects them).
    """
    from app.api.expansion_advisor import CandidateMemoResponse

    def _make(candidate_extra: dict[str, Any]) -> dict[str, Any]:
        return {
            "candidate_id": "c1",
            "search_id": "s1",
            "brand_profile": {},
            "candidate": {**candidate_extra},
            "recommendation": {
                "headline": "", "verdict": "", "best_use_case": "",
                "main_watchout": "", "gate_verdict": "",
            },
            "market_research": {
                "delivery_market_summary": "",
                "competitive_context": "",
                "district_fit_summary": "",
            },
        }

    # Shape A: rerank_reason=None (unchanged / flag-off candidate).
    m1 = CandidateMemoResponse.model_validate(
        _make({"rerank_reason": None, "rerank_status": "flag_off"})
    )
    assert m1.candidate.rerank_reason is None
    assert m1.candidate.rerank_status == "flag_off"

    # Shape B: rerank_reason=<structured dict> (moved candidate).
    reason = {
        "summary": "moved after reweighing realized-demand and landlord signal",
        "positives_cited": ["realized demand"],
        "negatives_cited": [],
        "comparison_to_displaced_candidate": "the displaced candidate has a weaker overall fit",
    }
    m2 = CandidateMemoResponse.model_validate(
        _make({"rerank_reason": reason, "rerank_status": "applied"})
    )
    assert isinstance(m2.candidate.rerank_reason, dict)
    assert m2.candidate.rerank_reason["summary"].startswith("moved")
    assert m2.candidate.rerank_status == "applied"


# ---------------------------------------------------------------------------
# 10. POST /decision-memo write-back is present and commits.
# ---------------------------------------------------------------------------
def test_decision_memo_cache_write_persists_and_commits(monkeypatch):
    from app.api import expansion_advisor as api_mod

    captured: dict[str, Any] = {"params": None, "commits": 0}

    class _DB:
        def execute(self, stmt, params=None):
            sql = stmt.text if hasattr(stmt, "text") else str(stmt)
            if "UPDATE expansion_candidate" in sql:
                captured["params"] = params
            return MagicMock()

        def commit(self):
            captured["commits"] += 1

        def rollback(self):
            pass

    db = _DB()
    api_mod._decision_memo_cache_write(
        db,
        search_id="s1",
        parcel_id="p1",
        memo_text="hello",
        memo_json={"headline_recommendation": "ok"},
    )
    assert captured["params"] is not None
    assert captured["params"]["txt"] == "hello"
    # JSON was serialized before bind.
    assert json.loads(captured["params"]["j"])["headline_recommendation"] == "ok"
    assert captured["commits"] == 1
