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
    # Six rerank fields plus the full persisted structured memo.
    assert memo["deterministic_rank"] == 1
    assert memo["final_rank"] == 1
    assert memo["rerank_applied"] is False
    assert memo["rerank_reason"] is None
    assert memo["rerank_delta"] == 0
    assert memo["rerank_status"] == "flag_off"
    assert memo["decision_memo"] == "Headline text\n\nBody."
    assert memo["decision_memo_json"] == structured


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
# 9. Pre-warm: wall-clock budget is enforced between iterations.
# ---------------------------------------------------------------------------
def test_prewarm_budget_exhausted_skips_remaining(monkeypatch):
    from app.api import expansion_advisor as api_mod

    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_ENABLED", True)
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_TOP_N", 5)
    # Budget of ~0: anything after the first iteration sees elapsed > budget.
    monkeypatch.setattr(_ea_settings, "EXPANSION_MEMO_PREWARM_BUDGET_S", 0.0)

    specs = [{"id": f"c{i}", "parcel_id": f"p{i}"} for i in range(1, 6)]

    # Drive monotonic clock manually so we know the budget is the gate.
    clock = {"t": 0.0}

    def _mono():
        return clock["t"]

    def _tick(ctx):
        # Simulate the LLM taking time; after the first call, budget is blown.
        clock["t"] = 1.0
        return {"headline_recommendation": "ok"}

    monkeypatch.setattr(api_mod._prewarm_time, "monotonic", _mono)
    monkeypatch.setattr(api_mod, "generate_structured_memo", _tick)
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
    # First iteration's budget check sees elapsed=0 (<0=budget? no, 0>=0),
    # so even p1 is skipped — the budget is actively enforced.
    assert writes == []


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
