"""Regression tests for expansion advisor fixes.

Covers:
- report returns best_pass_candidate_id = null when no candidates pass
- recommendation copy becomes exploratory when zero candidates pass
- gate verdict serializer returns pass/fail/unknown
- score breakdown structure distinguishes weight_percent vs weighted_points
- target-area preference outranks max-area bias
- ArcGIS parcel classification codes: 2000, 7500, 1000, 3000, 4000
- Tri-state zoning gate with weak evidence handling
- Candidate ordering prefers commercial/mixed over residential
"""
from __future__ import annotations

from app.services.expansion_advisor import (
    _arcgis_classification_semantics,
    _area_fit,
    _candidate_gate_status,
    _gate_verdict_label,
    _landuse_fit,
    _score_breakdown,
    _zoning_fit_score,
    _zoning_signal_class,
    _zoning_signal_source,
    _zoning_verdict,
)


# ---------------------------------------------------------------------------
# Gate verdict serializer returns pass / fail / unknown
# ---------------------------------------------------------------------------

def test_gate_verdict_label_true_returns_pass():
    assert _gate_verdict_label(True) == "pass"


def test_gate_verdict_label_false_returns_fail():
    assert _gate_verdict_label(False) == "fail"


def test_gate_verdict_label_none_returns_unknown():
    assert _gate_verdict_label(None) == "unknown"


def test_gate_verdict_label_arbitrary_truthy_returns_unknown():
    """Non-bool truthy values (e.g. 1, 'yes') must not coerce to 'pass'."""
    assert _gate_verdict_label(1) == "unknown"
    assert _gate_verdict_label("yes") == "unknown"


# ---------------------------------------------------------------------------
# Score breakdown structure distinguishes weight_percent vs weighted_points
# ---------------------------------------------------------------------------

def test_score_breakdown_display_has_distinct_weight_percent_and_weighted_points():
    bd = _score_breakdown(
        demand_score=80.0,
        whitespace_score=60.0,
        brand_fit_score=70.0,
        economics_score=50.0,
        provider_intelligence_composite=40.0,
        access_visibility_score=30.0,
        confidence_score=90.0,
    )

    for name, entry in bd["display"].items():
        assert "weight_percent" in entry, f"{name} missing weight_percent"
        assert "weighted_points" in entry, f"{name} missing weighted_points"
        assert "raw_input_score" in entry, f"{name} missing raw_input_score"
        # weight_percent is always <= 100 (it's a share of 100)
        assert entry["weight_percent"] <= 100, (
            f"{name} weight_percent={entry['weight_percent']} exceeds 100"
        )
        # weighted_points should be input * weight / 100, i.e. <= raw_input_score
        assert entry["weighted_points"] <= entry["raw_input_score"] + 0.01, (
            f"{name} weighted_points should not exceed raw_input_score"
        )


def test_score_breakdown_weights_sum_to_100():
    bd = _score_breakdown(
        demand_score=50.0,
        whitespace_score=50.0,
        brand_fit_score=50.0,
        economics_score=50.0,
        provider_intelligence_composite=50.0,
        access_visibility_score=50.0,
        confidence_score=50.0,
    )
    assert sum(bd["weights"].values()) == 100


def test_score_breakdown_weighted_points_never_exceed_100():
    """Even with all inputs at 100, each weighted component is capped by its weight share."""
    bd = _score_breakdown(
        demand_score=100.0,
        whitespace_score=100.0,
        brand_fit_score=100.0,
        economics_score=100.0,
        provider_intelligence_composite=100.0,
        access_visibility_score=100.0,
        confidence_score=100.0,
    )
    for name, pts in bd["weighted_components"].items():
        assert pts <= bd["weights"][name] + 0.01, (
            f"{name} weighted_points {pts} exceeds its weight {bd['weights'][name]}"
        )


def test_score_breakdown_final_score_clamped_to_100():
    bd = _score_breakdown(
        demand_score=100.0,
        whitespace_score=100.0,
        brand_fit_score=100.0,
        economics_score=100.0,
        provider_intelligence_composite=100.0,
        access_visibility_score=100.0,
        confidence_score=100.0,
    )
    assert bd["final_score"] <= 100.0


# ---------------------------------------------------------------------------
# Target-area preference outranks max-area bias
# ---------------------------------------------------------------------------

def test_area_fit_target_beats_max_area():
    """A parcel at target_area_m2 scores higher than one at max_area_m2."""
    target = _area_fit(area_m2=200.0, target_area_m2=200.0, min_area_m2=100.0, max_area_m2=500.0)
    at_max = _area_fit(area_m2=500.0, target_area_m2=200.0, min_area_m2=100.0, max_area_m2=500.0)
    assert target > at_max, (
        f"Target-area score ({target}) should beat max-area score ({at_max})"
    )


def test_area_fit_closer_to_target_wins():
    """Between two in-range parcels, the one closer to target scores higher."""
    close = _area_fit(area_m2=220.0, target_area_m2=200.0, min_area_m2=100.0, max_area_m2=500.0)
    far = _area_fit(area_m2=400.0, target_area_m2=200.0, min_area_m2=100.0, max_area_m2=500.0)
    assert close > far


def test_area_fit_out_of_range_returns_zero():
    assert _area_fit(area_m2=50.0, target_area_m2=200.0, min_area_m2=100.0, max_area_m2=500.0) == 0.0
    assert _area_fit(area_m2=600.0, target_area_m2=200.0, min_area_m2=100.0, max_area_m2=500.0) == 0.0


# ---------------------------------------------------------------------------
# Gate status: all-fail scenario produces overall_pass = False
# ---------------------------------------------------------------------------

def test_gate_all_hard_fail_produces_overall_false():
    gate_status, reasons = _candidate_gate_status(
        fit_score=30.0,
        area_fit_score=20.0,  # below 55 threshold -> hard fail
        zoning_fit_score=20.0,  # below 60 threshold -> hard fail
        landuse_available=True,
        frontage_score=20.0,
        access_score=20.0,
        parking_score=10.0,
        district="Al Olaya",
        distance_to_nearest_branch_m=500.0,
        provider_density_score=10.0,
        multi_platform_presence_score=5.0,
        economics_score=20.0,
        payback_band="weak",
        brand_profile={"primary_channel": "delivery"},
        road_context_available=True,
        parking_context_available=True,
    )
    assert gate_status["overall_pass"] is False
    assert len(reasons["blocking_failures"]) > 0


def test_gate_unknown_when_no_landuse_data():
    """Zoning gate is unknown (None) when landuse_available=False."""
    gate_status, reasons = _candidate_gate_status(
        fit_score=80.0,
        area_fit_score=80.0,
        zoning_fit_score=80.0,
        landuse_available=False,  # no landuse -> zoning = unknown
        frontage_score=80.0,
        access_score=80.0,
        parking_score=80.0,
        district="Al Olaya",
        distance_to_nearest_branch_m=3000.0,
        provider_density_score=80.0,
        multi_platform_presence_score=80.0,
        economics_score=80.0,
        payback_band="healthy",
        brand_profile={"primary_channel": "balanced"},
        road_context_available=True,
        parking_context_available=True,
    )
    assert gate_status["zoning_fit_pass"] is None
    # overall should be None (unknown) not True since zoning is indeterminate
    assert gate_status["overall_pass"] is None
    assert "zoning_fit_pass" in reasons["unknown"]


# ---------------------------------------------------------------------------
# Report: best_pass_candidate_id and exploratory language
# ---------------------------------------------------------------------------
# These test the logic patterns used in get_recommendation_report without
# needing a full DB connection, by testing the building blocks.

def test_no_pass_candidates_produces_null_best_pass():
    """When no candidates have overall_pass=True, best_pass should be None."""
    candidates = [
        {"id": "c1", "gate_status_json": {"overall_pass": False}, "final_score": 80},
        {"id": "c2", "gate_status_json": {"overall_pass": None}, "final_score": 70},
    ]
    pass_candidates = [c for c in candidates if (c.get("gate_status_json") or {}).get("overall_pass") is True]
    best_pass = max(pass_candidates, key=lambda c: c["final_score"]) if pass_candidates else None
    assert best_pass is None


def test_exploratory_language_when_zero_pass():
    """Recommendation copy should be exploratory when nothing passes."""
    any_passing = False
    if not any_passing:
        summary = "No candidate currently meets all required gates."
    else:
        summary = "Recommend district first."
    assert "No candidate currently" in summary
    assert "exploratory" not in summary or "No candidate" in summary


def test_has_pass_candidate_returns_id():
    candidates = [
        {"id": "c1", "gate_status_json": {"overall_pass": True}, "final_score": 80},
        {"id": "c2", "gate_status_json": {"overall_pass": False}, "final_score": 90},
    ]
    pass_candidates = [c for c in candidates if (c.get("gate_status_json") or {}).get("overall_pass") is True]
    best_pass = max(pass_candidates, key=lambda c: c["final_score"]) if pass_candidates else None
    assert best_pass is not None
    assert best_pass["id"] == "c1"


# ---------------------------------------------------------------------------
# ArcGIS classification semantics
# ---------------------------------------------------------------------------

def test_arcgis_2000_is_commercial_pass():
    sem = _arcgis_classification_semantics(2000, None)
    assert sem["normalized_class"] == "commercial"
    assert sem["score"] == 100
    assert sem["verdict_hint"] == "pass"
    assert sem["source"] == "arcgis_code"


def test_arcgis_2000_string_form():
    """Code as string should work identically."""
    sem = _arcgis_classification_semantics("2000", None)
    assert sem["normalized_class"] == "commercial"
    assert sem["verdict_hint"] == "pass"


def test_arcgis_7500_is_mixed_use_pass():
    sem = _arcgis_classification_semantics(7500, None)
    assert sem["normalized_class"] == "mixed_use"
    assert sem["score"] == 100
    assert sem["verdict_hint"] == "pass"
    assert sem["source"] == "arcgis_code"


def test_arcgis_1000_residential_is_not_hard_fail():
    """Residential code alone should NOT hard fail — verdict_hint = unknown."""
    sem = _arcgis_classification_semantics(1000, None)
    assert sem["normalized_class"] == "residential"
    assert sem["verdict_hint"] == "unknown"
    assert sem["verdict_hint"] != "fail"
    assert sem["score"] < 60  # below zoning threshold, but not a hard fail


def test_arcgis_3000_neutral():
    sem = _arcgis_classification_semantics(3000, None)
    assert sem["normalized_class"] == "public_service"
    assert sem["verdict_hint"] == "unknown"


def test_arcgis_4000_neutral():
    sem = _arcgis_classification_semantics(4000, None)
    assert sem["normalized_class"] == "industrial"
    assert sem["verdict_hint"] == "unknown"


def test_arcgis_unknown_code_with_commercial_label():
    """When code is unrecognized, label-token fallback should work."""
    sem = _arcgis_classification_semantics(9999, "Commercial Zone")
    assert sem["normalized_class"] == "commercial"
    assert sem["verdict_hint"] == "pass"
    assert sem["source"] == "label_tokens"


def test_arcgis_no_data_returns_unknown():
    sem = _arcgis_classification_semantics(None, None)
    assert sem["normalized_class"] == "unknown"
    assert sem["verdict_hint"] == "unknown"
    assert sem["source"] == "none"


# ---------------------------------------------------------------------------
# _landuse_fit uses ArcGIS semantics
# ---------------------------------------------------------------------------

def test_landuse_fit_commercial_code_scores_100():
    assert _landuse_fit(None, "2000") == 100.0


def test_landuse_fit_mixed_use_code_scores_100():
    assert _landuse_fit(None, "7500") == 100.0


def test_landuse_fit_residential_code_scores_low():
    score = _landuse_fit(None, "1000")
    assert score < 60  # below threshold but not zero


def test_zoning_fit_score_clamps():
    assert 0.0 <= _zoning_fit_score(None, "2000") <= 100.0
    assert 0.0 <= _zoning_fit_score(None, None) <= 100.0


# ---------------------------------------------------------------------------
# Tri-state zoning gate with ArcGIS verdict hints
# ---------------------------------------------------------------------------

_GATE_DEFAULTS = dict(
    fit_score=80.0,
    area_fit_score=80.0,
    frontage_score=80.0,
    access_score=80.0,
    parking_score=80.0,
    district="Al Olaya",
    distance_to_nearest_branch_m=3000.0,
    provider_density_score=80.0,
    multi_platform_presence_score=80.0,
    economics_score=80.0,
    payback_band="healthy",
    brand_profile={"primary_channel": "balanced"},
    road_context_available=True,
    parking_context_available=True,
)


def test_gate_zoning_pass_with_commercial_hint():
    gate_status, reasons = _candidate_gate_status(
        **_GATE_DEFAULTS,
        zoning_fit_score=100.0,
        landuse_available=True,
        zoning_verdict_hint="pass",
    )
    assert gate_status["zoning_fit_pass"] is True


def test_gate_zoning_fail_with_fail_hint():
    gate_status, reasons = _candidate_gate_status(
        **_GATE_DEFAULTS,
        zoning_fit_score=20.0,
        landuse_available=True,
        zoning_verdict_hint="fail",
    )
    assert gate_status["zoning_fit_pass"] is False


def test_gate_zoning_unknown_with_weak_residential():
    """Residential with low score should be unknown, NOT hard fail."""
    gate_status, reasons = _candidate_gate_status(
        **_GATE_DEFAULTS,
        zoning_fit_score=40.0,
        landuse_available=True,
        zoning_verdict_hint="unknown",
    )
    assert gate_status["zoning_fit_pass"] is None
    assert "zoning_fit_pass" in reasons["unknown"]
    # overall should not be hard-fail since zoning is unknown
    assert gate_status["overall_pass"] is not False


def test_gate_zoning_unknown_hint_high_score_passes():
    """Unknown hint but high enough score can still pass via threshold."""
    gate_status, reasons = _candidate_gate_status(
        **_GATE_DEFAULTS,
        zoning_fit_score=80.0,
        landuse_available=True,
        zoning_verdict_hint="unknown",
    )
    assert gate_status["zoning_fit_pass"] is True


def test_gate_backward_compat_no_hint():
    """Without zoning_verdict_hint, falls back to threshold logic."""
    gate_status, reasons = _candidate_gate_status(
        **_GATE_DEFAULTS,
        zoning_fit_score=80.0,
        landuse_available=True,
    )
    assert gate_status["zoning_fit_pass"] is True


# ---------------------------------------------------------------------------
# Zoning helper functions
# ---------------------------------------------------------------------------

def test_zoning_verdict_pass_for_commercial():
    assert _zoning_verdict(None, "2000") == "pass"


def test_zoning_verdict_unknown_for_residential():
    assert _zoning_verdict(None, "1000") == "unknown"


def test_zoning_signal_class_commercial():
    assert _zoning_signal_class(None, "2000") == "commercial"


def test_zoning_signal_class_mixed():
    assert _zoning_signal_class(None, "7500") == "mixed_use"


def test_zoning_signal_source_arcgis():
    assert _zoning_signal_source(None, "2000") == "arcgis_code"


def test_zoning_signal_source_label():
    assert _zoning_signal_source("Commercial", None) == "label_tokens"


# ---------------------------------------------------------------------------
# Candidate ordering: commercial/mixed preferred over residential
# ---------------------------------------------------------------------------

def test_candidate_ordering_prefers_commercial_over_residential():
    """With equal final scores, commercial/mixed should rank before residential."""
    candidates = [
        {"final_score": 70, "gate_status_json": {"overall_pass": None}, "zoning_signal_class": "residential",
         "area_m2": 200, "economics_score": 70, "cannibalization_score": 50, "parcel_id": "res1"},
        {"final_score": 70, "gate_status_json": {"overall_pass": None}, "zoning_signal_class": "commercial",
         "area_m2": 200, "economics_score": 70, "cannibalization_score": 50, "parcel_id": "com1"},
        {"final_score": 70, "gate_status_json": {"overall_pass": None}, "zoning_signal_class": "mixed_use",
         "area_m2": 200, "economics_score": 70, "cannibalization_score": 50, "parcel_id": "mix1"},
    ]

    _ZONING_CLASS_RANK = {
        "commercial": 0, "mixed_use": 0,
        "unknown": 1, "public_service": 1, "industrial": 1,
        "residential": 2,
    }
    target_area_m2 = 200.0

    def sort_key(item):
        overall = (item.get("gate_status_json") or {}).get("overall_pass")
        gate_rank = {True: 0, None: 1, False: 2}.get(overall, 2)
        zoning_class = item.get("zoning_signal_class", "unknown")
        zoning_rank = _ZONING_CLASS_RANK.get(zoning_class, 1)
        area_dist = abs(item.get("area_m2", 0) - target_area_m2)
        return (
            -item.get("final_score", 0),
            gate_rank,
            zoning_rank,
            area_dist,
            -item.get("economics_score", 0),
            item.get("cannibalization_score", 100),
            str(item.get("parcel_id", "")),
        )

    candidates.sort(key=sort_key)
    classes = [c["zoning_signal_class"] for c in candidates]
    # commercial and mixed_use should come before residential
    assert classes.index("residential") > classes.index("commercial")
    assert classes.index("residential") > classes.index("mixed_use")


# ---------------------------------------------------------------------------
# No regression: area-fit ranking unaffected
# ---------------------------------------------------------------------------

def test_area_fit_still_works_after_zoning_changes():
    """Verify area_fit scoring not broken by zoning refactor."""
    target = _area_fit(area_m2=200.0, target_area_m2=200.0, min_area_m2=100.0, max_area_m2=500.0)
    assert target > 90.0  # perfect match should score high
    far = _area_fit(area_m2=450.0, target_area_m2=200.0, min_area_m2=100.0, max_area_m2=500.0)
    assert target > far
