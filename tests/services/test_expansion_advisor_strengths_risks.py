"""PR #3 — _build_strengths_and_risks structured-inputs tests.

Covers the producer's new 4-tuple return shape and the six zero-param
S1-R3 templates that PR #3 adds to ``expansion_advisor_i18n``:

  - each of the six firing conditions emits its English literal AND the
    matching structured record (deliberately NOT DRY'd);
  - the English literal is byte-identical to the i18n ``en`` template
    (discipline rule #1 — lockstep);
  - the ``ar`` template renders a non-empty, distinct Arabic string;
  - the producer is locale-invariant — it only emits English strings
    and structured ids, never Arabic.
"""

from __future__ import annotations

import pytest

from app.services.expansion_advisor import _build_strengths_and_risks, _decision_summary
from app.services.expansion_advisor_i18n import TEMPLATES, render

# Baseline scalars that fire NONE of the six conditions:
#   demand 50 < 70, whitespace 55 (not >=65, not <=45), fit 50 < 70,
#   cannibalization 50 < 70, rent_source != "conservative_default".
_NEUTRAL = dict(
    demand_score=50.0,
    whitespace_score=55.0,
    fit_score=50.0,
    cannibalization_score=50.0,
    rent_source="market",
)

# (id, override kwargs that fire exactly one condition, English literal,
#  whether it lands in strengths)
_CONDITIONS = [
    (
        "S1",
        {"demand_score": 80.0},
        "High demand index supports branch throughput",
        True,
    ),
    (
        "S2",
        {"whitespace_score": 70.0},
        "Competitive whitespace remains attractive",
        True,
    ),
    (
        "S3",
        {"fit_score": 80.0},
        "Parcel characteristics align with target format",
        True,
    ),
    (
        "R1",
        {"rent_source": "conservative_default"},
        "Rent benchmark fell back to conservative city default (lower confidence)",
        False,
    ),
    (
        "R2",
        {"cannibalization_score": 80.0},
        "High overlap risk with existing branches",
        False,
    ),
    (
        "R3",
        {"whitespace_score": 40.0},
        "Competitive density may pressure launch economics",
        False,
    ),
]
_CONDITION_IDS = [c[0] for c in _CONDITIONS]


# ── Producer: return shape ──────────────────────────────────────────


def test_producer_returns_four_tuple() -> None:
    out = _build_strengths_and_risks(**_NEUTRAL)
    assert isinstance(out, tuple) and len(out) == 4
    strengths, risks, strengths_structured, risks_structured = out
    assert strengths == []
    assert risks == []
    assert strengths_structured == []
    assert risks_structured == []


def test_producer_all_six_fire_together() -> None:
    """All six conditions firing — English + structured stay parallel."""
    strengths, risks, s_struct, r_struct = _build_strengths_and_risks(
        demand_score=80.0,
        whitespace_score=70.0,  # >=65 fires S2; not <=45 so R3 stays off
        fit_score=80.0,
        cannibalization_score=80.0,
        rent_source="conservative_default",
    )
    assert len(strengths) == 3 and len(s_struct) == 3
    assert len(risks) == 2 and len(r_struct) == 2  # R1 + R2 (R3 needs ws<=45)
    assert [r["id"] for r in s_struct] == ["S1", "S2", "S3"]
    assert [r["id"] for r in r_struct] == ["R1", "R2"]
    assert all(r["params"] == {} for r in s_struct + r_struct)


# ── Producer: each firing condition in isolation (6 conditions) ─────


@pytest.mark.parametrize(
    "tid,override,literal,is_strength", _CONDITIONS, ids=_CONDITION_IDS
)
def test_producer_condition_emits_literal_and_structured(
    tid: str, override: dict, literal: str, is_strength: bool
) -> None:
    kwargs = {**_NEUTRAL, **override}
    strengths, risks, s_struct, r_struct = _build_strengths_and_risks(**kwargs)
    if is_strength:
        assert strengths == [literal]
        assert s_struct == [{"id": tid, "params": {}}]
        assert risks == [] and r_struct == []
    else:
        assert risks == [literal]
        assert r_struct == [{"id": tid, "params": {}}]
        assert strengths == [] and s_struct == []


# ── Lockstep: en template byte-identical to producer literal ────────
# (6 conditions × en = the first 6 of the 12 producer fixtures)


@pytest.mark.parametrize(
    "tid,override,literal,is_strength", _CONDITIONS, ids=_CONDITION_IDS
)
def test_en_template_byte_identical_to_producer_literal(
    tid: str, override: dict, literal: str, is_strength: bool
) -> None:
    """render(record, "en") reproduces the producer's English literal
    byte-for-byte (discipline rule #1)."""
    assert render({"id": tid, "params": {}}, "en") == literal


# ── Arabic render: non-empty and distinct (6 conditions × ar) ───────


@pytest.mark.parametrize(
    "tid,override,literal,is_strength", _CONDITIONS, ids=_CONDITION_IDS
)
def test_ar_template_renders_non_empty_distinct(
    tid: str, override: dict, literal: str, is_strength: bool
) -> None:
    ar = render({"id": tid, "params": {}}, "ar")
    assert ar
    assert ar != literal


# ── i18n: structural completeness of the six new templates ─────────


def test_six_new_templates_present() -> None:
    for tid in ("S1", "S2", "S3", "R1", "R2", "R3"):
        assert tid in TEMPLATES
        assert set(TEMPLATES[tid]) == {"en", "ar"}


def test_ar_templates_use_latin_digits_only() -> None:
    """No Arabic-Indic digits in any of the six ar strings."""
    arabic_indic = "٠١٢٣٤٥٦٧٨٩"
    for tid in ("S1", "S2", "S3", "R1", "R2", "R3"):
        ar = TEMPLATES[tid]["ar"]
        assert not any(ch in arabic_indic for ch in ar), tid


# ── _decision_summary: risk_id sibling field (PR #3 §4) ─────────────


def test_decision_summary_emits_risk_id_from_structured() -> None:
    """When the risk clause is spliced from key_risks, the structured
    record carries the index-aligned risk_id alongside risk_text_en."""
    _, structured = _decision_summary(
        district="Al Olaya",
        final_score=70.0,
        economics_score=60.0,
        key_risks=["High overlap risk with existing branches"],
        service_model="qsr",
        area_m2=200.0,
        key_risks_structured=[{"id": "R2", "params": {}}],
    )
    assert structured["params"]["risk_kind"] == "from_key_risks"
    assert structured["params"]["risk_id"] == "R2"
    # risk_text_en is retained for dual-read of pre-PR-3 rows (Q3).
    assert (
        structured["params"]["risk_text_en"]
        == "High overlap risk with existing branches"
    )


def test_decision_summary_no_risk_id_when_no_key_risks() -> None:
    """No key_risks → risk_kind is tight_economics/execution and no
    risk_id is emitted."""
    _, structured = _decision_summary(
        district="Al Olaya",
        final_score=70.0,
        economics_score=40.0,
        key_risks=[],
        service_model="qsr",
        area_m2=200.0,
        key_risks_structured=[],
    )
    assert structured["params"]["risk_kind"] == "tight_economics"
    assert "risk_id" not in structured["params"]


def test_decision_summary_english_clause_unchanged() -> None:
    """The English summary string is byte-untouched by the risk_id
    addition (rule #1)."""
    summary, _ = _decision_summary(
        district="Al Olaya",
        final_score=70.0,
        economics_score=60.0,
        key_risks=["High overlap risk with existing branches"],
        service_model="qsr",
        area_m2=200.0,
        key_risks_structured=[{"id": "R2", "params": {}}],
    )
    assert (
        "Biggest commercial risk: High overlap risk with existing branches." in summary
    )
