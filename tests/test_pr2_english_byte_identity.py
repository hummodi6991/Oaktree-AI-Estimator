"""PR #2a — English byte-identity + structured-record golden tests.

PR #2a adds a parallel locale-invariant structured record to each of the
five Expansion Advisor heuristic producers. The hard constraint is that
the producers' *English* output stays character-for-character identical
to HEAD. These tests enforce that, plus the structured-record contract.

Golden fixtures live in ``tests/fixtures/pr2_golden/`` — one ``<id>.json``
per firing condition (see ``scripts/gen_pr2_golden.py``). Each carries:

  - ``kwargs``               the producer input
  - ``expected_english``     the English output captured from HEAD
                             (``baseline_english.json`` is the same data
                             keyed by id; it is the authoritative
                             pre-PR reference)
  - ``expected_structured``  the structured record from the post-PR
                             producer

``test_english_byte_identical`` proves the English path is unchanged
(rule #1). ``test_structured_record_matches`` pins the structured
record. Together they are the drift guard: a future PR that edits a
producer's English string without updating the structured record (or
vice versa) trips exactly one of the two. The third leg of the lockstep
guard — re-rendering the structured record through the i18n module's
``en`` template and asserting equality with the producer output — lands
in PR #2b, since the i18n module is PR #2b's deliverable.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.services.expansion_advisor import (
    _build_cost_thesis,
    _build_demand_thesis,
    _decision_summary,
    _gate_key_to_label,
    _sanitize_for_json,
    _top_positives_and_risks,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "pr2_golden"


def _load_fixtures() -> list[dict]:
    fixtures = []
    for path in sorted(FIXTURE_DIR.glob("*.json")):
        if path.name == "baseline_english.json":
            continue
        fixtures.append(json.loads(path.read_text(encoding="utf-8")))
    return fixtures


FIXTURES = _load_fixtures()
FIXTURE_IDS = [fx["id"] for fx in FIXTURES]


def _invoke(producer: str, kwargs: dict):
    """Return ``(english, structured)`` for the named producer."""
    if producer == "_top_positives_and_risks":
        positives, risks, pos_struct, risk_struct = _top_positives_and_risks(**kwargs)
        return (
            {"positives": positives, "risks": risks},
            {"positives": pos_struct, "risks": risk_struct},
        )
    if producer == "_build_demand_thesis":
        english, structured = _build_demand_thesis(**kwargs)
        return english, structured
    if producer == "_build_cost_thesis":
        english, structured = _build_cost_thesis(**kwargs)
        return english, structured
    if producer == "_decision_summary":
        english, structured = _decision_summary(**kwargs)
        return english, structured
    if producer == "_gate_key_to_label":
        return _gate_key_to_label(kwargs["gate_key"]), None
    raise AssertionError(f"unknown producer {producer}")


def test_fixture_set_is_non_empty():
    # Guard against an empty/missing fixture directory silently passing.
    assert len(FIXTURES) >= 60, f"expected the full golden set, found {len(FIXTURES)}"


@pytest.mark.parametrize("fx", FIXTURES, ids=FIXTURE_IDS)
def test_english_byte_identical(fx):
    """The producer's English output must equal the HEAD baseline exactly."""
    english, _structured = _invoke(fx["producer"], fx["kwargs"])
    assert english == fx["expected_english"], (
        f"English output drifted for fixture {fx['id']!r} — this violates "
        f"PR #2a rule #1 (English persisted strings byte-identical)."
    )


@pytest.mark.parametrize("fx", FIXTURES, ids=FIXTURE_IDS)
def test_structured_record_matches(fx):
    """The producer's structured record must match the pinned fixture."""
    _english, structured = _invoke(fx["producer"], fx["kwargs"])
    assert structured == fx["expected_structured"], (
        f"Structured record drifted for fixture {fx['id']!r} — the "
        f"structured output and the English output are now out of lockstep."
    )


@pytest.mark.parametrize("fx", FIXTURES, ids=FIXTURE_IDS)
def test_baseline_english_matches_per_fixture(fx):
    """The per-fixture expected_english must equal baseline_english.json."""
    baseline = json.loads(
        (FIXTURE_DIR / "baseline_english.json").read_text(encoding="utf-8")
    )
    assert baseline[fx["id"]] == fx["expected_english"]


@pytest.mark.parametrize("fx", FIXTURES, ids=FIXTURE_IDS)
def test_structured_records_are_jsonb_serializable(fx):
    """Every structured record survives the exact serialization the INSERT
    applies (``json.dumps(_sanitize_for_json(...), ensure_ascii=False)``)
    losslessly — this is the persistence round-trip for the five new
    JSONB columns."""
    _english, structured = _invoke(fx["producer"], fx["kwargs"])
    if structured is None:  # gate-label fixtures emit no structured record
        return
    round_tripped = json.loads(
        json.dumps(_sanitize_for_json(structured), ensure_ascii=False)
    )
    assert round_tripped == structured


def test_insert_wires_all_five_structured_columns():
    """run_expansion_search's INSERT must reference all five new columns
    and bind all five params — covers the write path for the new
    columns without standing up a full DB mock."""
    import inspect

    from app.services import expansion_advisor

    src = inspect.getsource(expansion_advisor.run_expansion_search)
    cols = (
        "top_positives_structured_json",
        "top_risks_structured_json",
        "decision_summary_structured_json",
        "demand_thesis_structured_json",
        "cost_thesis_structured_json",
    )
    for col in cols:
        assert src.count(col) >= 3, (
            f"{col} must appear in the INSERT column list, the VALUES "
            f"binding, and _candidate_insert_params"
        )
        assert f"CAST(:{col} AS jsonb)" in src, f"{col} not CAST in VALUES"
        assert f'"{col}"' in src, f"{col} not in the candidate dict / params"
