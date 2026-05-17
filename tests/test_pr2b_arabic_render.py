"""PR #2b — Arabic read-path render tests.

Covers the new ``expansion_advisor_i18n`` module and the ``lang="ar"``
branch of ``_normalize_candidate_payload``:

  - structured records render to Arabic;
  - NULL structured columns fall back to the English persisted column
    (discipline rule #4);
  - ``humanize_gate(key, "en")`` reproduces ``_gate_key_to_label`` for
    every gate key (the 12 ``gate_*`` golden fixtures pin this);
  - the third lockstep leg — re-rendering a structured record through
    the i18n module's ``en`` template equals the producer's English
    string — for every PR #2a golden fixture;
  - ``render`` degrades gracefully (returns "") on unknown ids /
    missing params.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.services.expansion_advisor import (
    _gate_key_to_label,
    _normalize_candidate_payload,
)
from app.services.expansion_advisor_i18n import (
    GATE_LABELS,
    TEMPLATES,
    humanize_gate,
    render,
)

PR2_GOLDEN = Path(__file__).parent / "fixtures" / "pr2_golden"


# Minimal params per template id so render() produces a non-empty string.
_MINIMAL_PARAMS: dict[str, dict] = {
    "pos.well_separated_branch": {"nearest_km": 5.5},
    "risk.gate_failed": {"gate_key": "parking_pass"},
    "risk.gate_unknown": {"gate_key": "parking_pass"},
    "risk.area_near_min": {"area_m2": 90.0},
    "risk.area_near_max": {"area_m2": 480.0},
    "risk.nearest_branch_close": {"nearest_km": 1.2},
    "risk.high_competitor_density": {"count": 9},
    "demand_thesis": {
        "demand_score": 80.0,
        "population_reach": 12000.0,
        "demand_label": "strong",
        "provider_label": "dense",
        "whitespace_label": "attractive",
        "competition_label": "intense",
    },
    "cost_thesis": {
        "estimated_rent_sar_m2_year": 1200.0,
        "estimated_annual_rent_sar": 240000.0,
        "estimated_fitout_cost_sar": 300000.0,
    },
    "decision_summary": {
        "area_label": "compact",
        "district_label": "Al Olaya",
        "final_score": 70.0,
        "economics_score": 60.0,
        "use_case": "neighborhood_qsr",
        "risk_kind": "execution",
        "risk_text_en": "",
    },
}


# ── i18n module: structural completeness ────────────────────────────

@pytest.mark.parametrize("tid", sorted(TEMPLATES.keys()))
def test_every_template_renders_non_empty_en(tid: str) -> None:
    """Every TEMPLATES id renders a non-empty English string with its
    producer-shaped params — proves there are no template typos."""
    params = _MINIMAL_PARAMS.get(tid, {})
    assert render({"id": tid, "params": params}, "en"), f"empty en render for {tid}"


@pytest.mark.parametrize("tid", sorted(TEMPLATES.keys()))
def test_every_template_renders_non_empty_ar(tid: str) -> None:
    """Every TEMPLATES id renders a non-empty Arabic string."""
    params = _MINIMAL_PARAMS.get(tid, {})
    assert render({"id": tid, "params": params}, "ar"), f"empty ar render for {tid}"


def test_templates_count() -> None:
    """15 pos.* + 13 risk.* (incl. the defensive K7 entry) + 3 theses."""
    pos = [k for k in TEMPLATES if k.startswith("pos.")]
    risk = [k for k in TEMPLATES if k.startswith("risk.")]
    assert len(pos) == 15
    assert len(risk) == 13
    assert {"demand_thesis", "cost_thesis", "decision_summary"} <= set(TEMPLATES)
    assert "risk.delivery_district_estimates" in risk  # K7, defensive


# ── humanize_gate: en byte-identity with _gate_key_to_label ─────────

def test_humanize_gate_en_matches_gate_key_to_label() -> None:
    for key in GATE_LABELS["en"]:
        assert humanize_gate(key, "en") == _gate_key_to_label(key)


def test_humanize_gate_en_matches_gate_golden_fixtures() -> None:
    """The 12 gate_* PR #2a golden fixtures pin every English label."""
    checked = 0
    for path in sorted(PR2_GOLDEN.glob("gate_*.json")):
        fx = json.loads(path.read_text(encoding="utf-8"))
        key = fx["kwargs"]["gate_key"]
        assert humanize_gate(key, "en") == fx["expected_english"]
        checked += 1
    assert checked == 12


def test_humanize_gate_ar_translates_all_12_keys() -> None:
    for key in GATE_LABELS["en"]:
        ar = humanize_gate(key, "ar")
        assert ar and ar != humanize_gate(key, "en")


def test_humanize_gate_unknown_key_fallback() -> None:
    assert humanize_gate("some_new_gate_pass", "en") == "some new gate"
    assert humanize_gate("some_new_gate_pass", "ar") == "some new gate"


# ── Third lockstep leg: en render == producer English ───────────────

def test_en_render_matches_pr2a_golden_producer_output() -> None:
    """Re-rendering each PR #2a structured record through the i18n en
    template reproduces the producer's English string byte-for-byte."""
    failures: list[str] = []
    checked = 0
    for path in sorted(PR2_GOLDEN.glob("*.json")):
        fx = json.loads(path.read_text(encoding="utf-8"))
        structured = fx.get("expected_structured")
        english = fx.get("expected_english")
        if structured is None:
            continue
        if isinstance(structured, dict) and "id" in structured and isinstance(english, str):
            checked += 1
            if render(structured, "en") != english:
                failures.append(fx["id"])
        elif isinstance(structured, dict) and isinstance(english, dict):
            for bucket in ("positives", "risks"):
                for rec, eng in zip(structured.get(bucket, []), english.get(bucket, [])):
                    checked += 1
                    if render(rec, "en") != eng:
                        failures.append(f"{fx['id']}:{bucket}")
    assert not failures, f"en lockstep mismatch: {failures}"
    assert checked > 0


# ── Graceful degradation ────────────────────────────────────────────

def test_render_unknown_id_returns_empty() -> None:
    assert render({"id": "does.not.exist", "params": {}}, "ar") == ""


def test_render_malformed_record_returns_empty() -> None:
    assert render(None, "ar") == ""  # type: ignore[arg-type]
    assert render({}, "ar") == ""
    assert render({"params": {}}, "ar") == ""


def test_render_missing_params_returns_empty() -> None:
    """A thesis template missing its params can't format → "" (caller
    falls back to the English persisted column)."""
    assert render({"id": "demand_thesis", "params": {}}, "en") == ""
    assert render({"id": "cost_thesis", "params": {}}, "ar") == ""


# ── Arabic content / formatting conventions ─────────────────────────

def test_demand_thesis_ar_uses_parenthetical_english_jargon() -> None:
    rendered = render({"id": "demand_thesis", "params": _MINIMAL_PARAMS["demand_thesis"]}, "ar")
    assert "الطلب" in rendered
    assert "providers" in rendered      # parenthetical English jargon
    assert "whitespace" in rendered
    assert "12000" in rendered          # Latin digits


def test_decision_summary_ar_suffix_for_each_risk_kind() -> None:
    for risk_kind in ("from_key_risks", "tight_economics", "execution"):
        params = dict(_MINIMAL_PARAMS["decision_summary"])
        params["risk_kind"] = risk_kind
        params["risk_text_en"] = "cannibalization risk is elevated"
        rendered = render({"id": "decision_summary", "params": params}, "ar")
        assert "أبرز مخاطر تجارية" in rendered, risk_kind


def test_pos_template_ar_distinct_from_en() -> None:
    en = render({"id": "pos.demand_strong", "params": {}}, "en")
    ar = render({"id": "pos.demand_strong", "params": {}}, "ar")
    assert en == "Demand potential is strong for this district."
    assert ar == "إمكانية الطلب قوية في هذا الحي."


# ── _normalize_candidate_payload ar branch ──────────────────────────

def test_normalize_candidate_ar_renders_structured() -> None:
    candidate = {
        "candidate_id": "c-ar-1",
        "parcel_id": "P-AR-1",
        "district": "Al Olaya",
        "top_positives_json": ["English fallback positive."],
        "top_risks_json": ["English fallback risk."],
        "decision_summary": "English decision summary.",
        "demand_thesis": "English demand thesis.",
        "cost_thesis": "English cost thesis.",
        "top_positives_structured_json": [{"id": "pos.demand_strong", "params": {}}],
        "top_risks_structured_json": [{"id": "risk.economics_marginal", "params": {}}],
        "decision_summary_structured_json": {
            "id": "decision_summary",
            "params": {
                "area_label": "compact",
                "district_label": "Al Olaya",
                "final_score": 70.0,
                "economics_score": 60.0,
                "use_case": "neighborhood_qsr",
                "risk_kind": "execution",
                "risk_text_en": "",
            },
        },
        "demand_thesis_structured_json": {
            "id": "demand_thesis",
            "params": _MINIMAL_PARAMS["demand_thesis"],
        },
        "cost_thesis_structured_json": {
            "id": "cost_thesis",
            "params": _MINIMAL_PARAMS["cost_thesis"],
        },
    }
    result = _normalize_candidate_payload(dict(candidate), lang="ar")
    assert result["top_positives_json"] == ["إمكانية الطلب قوية في هذا الحي."]
    assert result["top_risks_json"][0].startswith("الجدوى الاقتصادية حدّية")
    assert "الجدوى الاقتصادية" in result["decision_summary"]
    assert "الطلب" in result["demand_thesis"]
    assert "الإيجار التقديري" in result["cost_thesis"]
    # Internal structured columns never leak into the payload.
    for col in (
        "top_positives_structured_json",
        "top_risks_structured_json",
        "decision_summary_structured_json",
        "demand_thesis_structured_json",
        "cost_thesis_structured_json",
    ):
        assert col not in result


def test_normalize_candidate_ar_falls_back_to_english_when_structured_null() -> None:
    """Pre-PR-2a rows have NULL structured columns — the ar read path
    must serve the English persisted column unchanged (rule #4)."""
    candidate = {
        "candidate_id": "c-ar-2",
        "parcel_id": "P-AR-2",
        "district": "Al Olaya",
        "top_positives_json": ["English fallback positive."],
        "top_risks_json": ["English fallback risk."],
        "decision_summary": "English decision summary.",
        "demand_thesis": "English demand thesis.",
        "cost_thesis": "English cost thesis.",
        "top_positives_structured_json": None,
        "top_risks_structured_json": None,
        "decision_summary_structured_json": None,
        "demand_thesis_structured_json": None,
        "cost_thesis_structured_json": None,
    }
    result = _normalize_candidate_payload(dict(candidate), lang="ar")
    assert result["top_positives_json"] == ["English fallback positive."]
    assert result["top_risks_json"] == ["English fallback risk."]
    assert result["decision_summary"] == "English decision summary."
    assert result["demand_thesis"] == "English demand thesis."
    assert result["cost_thesis"] == "English cost thesis."


def test_normalize_candidate_ar_partial_failure_falls_back_whole_list() -> None:
    """If any structured positive fails to render, the whole English
    list is served (no mixed-language list)."""
    candidate = {
        "candidate_id": "c-ar-3",
        "parcel_id": "P-AR-3",
        "top_positives_json": ["English A.", "English B."],
        "top_positives_structured_json": [
            {"id": "pos.demand_strong", "params": {}},
            {"id": "does.not.exist", "params": {}},
        ],
    }
    result = _normalize_candidate_payload(dict(candidate), lang="ar")
    assert result["top_positives_json"] == ["English A.", "English B."]
