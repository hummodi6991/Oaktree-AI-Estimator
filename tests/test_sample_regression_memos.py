"""Plumbing coverage for ``scripts/sample_regression_memos.py``.

The script is manually invoked against the live stack to eyeball structured
memo quality (Step 9 of the Phase-1 decision-memo upgrade). This test does
NOT hit the real DB or real LLM — it only checks that the script's ``main``
produces the documented output shape when its two external dependencies
(``run_expansion_search`` and ``generate_structured_memo``) are mocked.

Purpose: stop future edits to the script from silently breaking the shape
contract that Ahmed's live-run output relies on.
"""
from __future__ import annotations

import json
from typing import Any

import pytest


class _DummyDB:
    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def _fake_candidate(search_id: str, parcel_id: str = "parcel-rank-1") -> dict[str, Any]:
    return {
        "id": "cand-1",
        "parcel_id": parcel_id,
        "search_id": search_id,
        "rank_position": 1,
        "feature_snapshot_json": {
            "district": "Al Olaya",
            "area_m2": 160,
            "estimated_annual_rent_sar": 420000,
        },
        "score_breakdown_json": {
            "occupancy_economics": 82,
            "listing_quality": 70,
            "brand_fit": 75,
            "competition_whitespace": 60,
            "demand_potential": 65,
            "access_visibility": 70,
            "landlord_signal": 55,
            "delivery_demand": 50,
            "confidence": 80,
        },
        "gate_status_json": [
            {"gate": "zoning_fit_pass", "verdict": "pass", "reason": "ok"},
        ],
        "comparable_competitors_json": [],
    }


_CANNED_MEMO = {
    "headline_recommendation": "recommend — strong occupancy economics",
    "ranking_explanation": "occupancy_economics contributed 24.6 of 30, driving rank 1.",
    "key_evidence": [
        {"signal": "rent", "value": "SAR 420k/yr", "implication": "below district median"},
    ],
    "risks": [
        {"risk": "limited frontage", "mitigation": None},
    ],
    "comparison": "Beats the next candidate on occupancy economics by 3 points.",
    "bottom_line": "Take it.",
}


def _install_mocks(monkeypatch, *, memo_override=None):
    """Mock the two external collaborators the script calls inside
    _run_one_search. Must be patched where the script imports them
    (local imports inside _run_one_search)."""
    import app.services.expansion_advisor as svc
    import app.services.llm_decision_memo as memo_mod

    def fake_run_expansion_search(db, *, search_id, **kwargs):
        return [_fake_candidate(search_id)]

    def fake_generate_structured_memo(ctx):
        return memo_override if memo_override is not None else dict(_CANNED_MEMO)

    monkeypatch.setattr(svc, "run_expansion_search", fake_run_expansion_search)
    monkeypatch.setattr(memo_mod, "generate_structured_memo", fake_generate_structured_memo)


# ── Tests ────────────────────────────────────────────────────────────


def test_main_returns_dict_keyed_by_search_name_with_structured_memos(monkeypatch, capsys):
    _install_mocks(monkeypatch)

    from scripts.sample_regression_memos import main, REGRESSION_BRIEFS

    results = main(argv=[], db=_DummyDB())

    # Keyed by every canonical search name
    assert set(results.keys()) == set(REGRESSION_BRIEFS.keys())
    for name, memo in results.items():
        assert isinstance(memo, dict), f"{name} value not a dict"
        # Structured memo shape — all six keys from the prompt contract
        for key in (
            "headline_recommendation",
            "ranking_explanation",
            "key_evidence",
            "risks",
            "comparison",
            "bottom_line",
        ):
            assert key in memo, f"{name} missing {key}"
        assert "skipped" not in memo

    # stdout is a single pretty-printed JSON blob
    stdout = capsys.readouterr().out
    parsed = json.loads(stdout)
    assert set(parsed.keys()) == set(REGRESSION_BRIEFS.keys())


def test_main_filter_to_single_search(monkeypatch, capsys):
    _install_mocks(monkeypatch)

    from scripts.sample_regression_memos import main

    results = main(argv=["--search", "cafe_al_yasmin"], db=_DummyDB())

    assert list(results.keys()) == ["cafe_al_yasmin"]
    assert results["cafe_al_yasmin"]["bottom_line"] == "Take it."


def test_main_writes_to_out_path(monkeypatch, tmp_path):
    _install_mocks(monkeypatch)

    from scripts.sample_regression_memos import main

    out_path = tmp_path / "memos.json"
    results = main(
        argv=["--search", "qsr_burger_al_olaya", "--out", str(out_path)],
        db=_DummyDB(),
    )

    assert out_path.exists()
    on_disk = json.loads(out_path.read_text(encoding="utf-8"))
    assert on_disk == results
    assert "qsr_burger_al_olaya" in on_disk


def test_skipped_when_memo_generation_returns_none(monkeypatch):
    _install_mocks(monkeypatch, memo_override=False)  # sentinel replaced below
    # Override to simulate generate_structured_memo returning None
    import app.services.llm_decision_memo as memo_mod
    monkeypatch.setattr(memo_mod, "generate_structured_memo", lambda ctx: None)

    from scripts.sample_regression_memos import main

    results = main(argv=["--search", "cafe_al_yasmin"], db=_DummyDB())

    entry = results["cafe_al_yasmin"]
    assert entry.get("skipped") is True
    assert "error" in entry
    assert "generate_structured_memo" in entry["error"]


def test_skipped_when_rank_one_missing_required_fields(monkeypatch):
    import app.services.expansion_advisor as svc
    import app.services.llm_decision_memo as memo_mod

    def bad_run(db, *, search_id, **kwargs):
        # Candidate missing feature_snapshot_json & score_breakdown_json
        return [{
            "id": "cand-x",
            "parcel_id": "p-x",
            "search_id": search_id,
            "rank_position": 1,
        }]

    monkeypatch.setattr(svc, "run_expansion_search", bad_run)
    # Should never be called on this path
    monkeypatch.setattr(
        memo_mod, "generate_structured_memo",
        lambda ctx: pytest.fail("generate_structured_memo should not be called"),
    )

    from scripts.sample_regression_memos import main

    results = main(argv=["--search", "qsr_burger_al_olaya"], db=_DummyDB())
    entry = results["qsr_burger_al_olaya"]
    assert entry.get("skipped") is True
    assert "missing fields" in entry["error"]


def test_skipped_when_search_returns_no_candidates(monkeypatch):
    import app.services.expansion_advisor as svc

    monkeypatch.setattr(
        svc, "run_expansion_search",
        lambda db, *, search_id, **kwargs: [],
    )

    from scripts.sample_regression_memos import main

    results = main(argv=["--search", "delivery_shawarma_citywide"], db=_DummyDB())
    entry = results["delivery_shawarma_citywide"]
    assert entry.get("skipped") is True
    assert "0 candidates" in entry["error"]
