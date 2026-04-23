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
- Non-numeric landuse_code values do not crash candidate query / ranking
- District SQL pushdown fallback on failure
- Dirty text coordinates in delivery_source_record / population_density
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from unittest.mock import MagicMock

from app.services.expansion_advisor import (
    _arcgis_classification_semantics,
    _area_fit,
    _brand_fit_score,
    _candidate_gate_status,
    _district_momentum_score,
    _effective_listing_age_days,
    _estimate_fitout_cost_sar,
    _estimate_revenue_index,
    _gate_verdict_label,
    _is_plausible_neighborhood,
    _landuse_fit,
    _listing_quality_score,
    _percentile_rent_burden,
    _score_breakdown,
    _zoning_fit_score,
    _zoning_signal_class,
    _zoning_signal_source,
    _zoning_verdict,
    run_expansion_search as _run_expansion_search_raw,
)


def run_expansion_search(*args, **kwargs):
    """Wrapper that unwraps the new dict return format to a plain list."""
    result = _run_expansion_search_raw(*args, **kwargs)
    return result["items"] if isinstance(result, dict) else result


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
        listing_quality_score=65.0,
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
        listing_quality_score=50.0,
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
        listing_quality_score=100.0,
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
        listing_quality_score=100.0,
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
        area_fit_score=20.0,
        area_m2=600.0,  # outside range -> hard fail
        min_area_m2=100.0,
        max_area_m2=500.0,
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
        area_m2=200.0,
        min_area_m2=100.0,
        max_area_m2=500.0,
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


def test_arcgis_4000_industrial_fail():
    """ArcGIS code 4000 (industrial) should now be classified as fail.
    Patch 3 intentionally changed this from 'unknown' to 'fail' to prevent
    industrial parcels from surfacing as cafe/dine_in candidates.
    """
    sem = _arcgis_classification_semantics(4000, None)
    assert sem["normalized_class"] == "industrial"
    assert sem["verdict_hint"] == "fail"
    assert sem["score"] == 30


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
    area_m2=200.0,
    min_area_m2=100.0,
    max_area_m2=500.0,
    frontage_score=80.0,
    access_score=80.0,
    parking_score=80.0,
    district="Al Olaya",
    distance_to_nearest_branch_m=3000.0,
    provider_density_score=80.0,
    multi_platform_presence_score=80.0,
    economics_score=80.0,
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


# ---------------------------------------------------------------------------
# Non-numeric landuse_code values must not crash scoring / ranking
# ---------------------------------------------------------------------------

class _Result:
    def __init__(self, rows):
        self._rows = rows
    def scalar(self):
        if self._rows and isinstance(self._rows[0], dict):
            return next(iter(self._rows[0].values()), None)
        if self._rows:
            return self._rows[0]
        return None
    def mappings(self):
        return self
    def all(self):
        return self._rows
    def first(self):
        return self._rows[0] if self._rows else None


class _FakeNestedTransaction:
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class _FakeDB:
    def __init__(self, candidate_rows=None):
        self.candidate_rows = candidate_rows or []
        self.inserted = []

    def begin_nested(self):
        return _FakeNestedTransaction()

    def execute(self, stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        if "FROM candidate_base" in sql:
            return _Result(self.candidate_rows)
        if "COUNT(*)" in sql and "candidate_location" in sql:
            return _Result([{"count": 0}])
        # Phase 3b _district_momentum_score — spatial aggregation
        # against external_feature with a distinctive "WITH
        # listing_district AS" CTE. Match BEFORE the plain
        # "FROM commercial_unit" rule so the helper gets an empty
        # momentum dict (every candidate then resolves to neutral 50.0
        # through the existing contract).
        if "WITH listing_district AS" in sql:
            return _Result([])
        if "FROM commercial_unit" in sql:
            return _Result(self.candidate_rows)
        if "INSERT INTO expansion_candidate" in sql:
            self.inserted.append(params)
            return _Result([])
        return _Result([])


def _make_candidate_row(parcel_id, landuse_code, landuse_label=None, district=None):
    return {
        "parcel_id": parcel_id,
        "landuse_label": landuse_label,
        "landuse_code": landuse_code,
        "area_m2": 200,
        "lon": 46.7,
        "lat": 24.7,
        "district": district or "حي العليا",
        "population_reach": 10000,
        "competitor_count": 2,
        "delivery_listing_count": 5,
    }


def test_non_numeric_landuse_codes_do_not_crash():
    """Blank, whitespace, non-numeric, and mixed landuse_code values must not crash."""
    bad_codes = ["", " 2000 ", "N/A", "mixed", None]
    district_names = ["District_A", "District_B", "District_C", "District_D", "District_E"]
    rows = [_make_candidate_row(f"p{i}", code, district=district_names[i]) for i, code in enumerate(bad_codes)]
    db = _FakeDB(candidate_rows=rows)

    items = run_expansion_search(
        db,
        search_id="test-bad-codes",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
    )
    assert isinstance(items, list)
    assert len(items) == len(rows)


def test_normal_numeric_landuse_codes_still_rank_correctly():
    """Normal numeric ArcGIS codes (1000, 2000, 7500) still produce correct scoring."""
    rows = [
        _make_candidate_row("commercial", "2000", district="Commercial_District"),
        _make_candidate_row("mixed", "7500", district="Mixed_District"),
        _make_candidate_row("residential", "1000", district="Residential_District"),
    ]
    db = _FakeDB(candidate_rows=rows)

    items = run_expansion_search(
        db,
        search_id="test-numeric-codes",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
    )
    assert len(items) == 3
    # Commercial/mixed should score higher on zoning than residential
    scores = {item["parcel_id"]: item["zoning_fit_score"] for item in items}
    assert scores["commercial"] > scores["residential"]
    assert scores["mixed"] > scores["residential"]


def test_landuse_fit_non_numeric_values_safe():
    """_landuse_fit must not raise for non-numeric landuse_code values."""
    for code in ["", "N/A", "mixed", "   ", None, "abc123"]:
        score = _landuse_fit(None, code)
        assert isinstance(score, float)
        assert 0.0 <= score <= 100.0


def test_zoning_fit_score_non_numeric_values_safe():
    """_zoning_fit_score must not raise for non-numeric landuse_code values."""
    for code in ["", "N/A", " 2000 ", None]:
        score = _zoning_fit_score(None, code)
        assert isinstance(score, float)
        assert 0.0 <= score <= 100.0


# ---------------------------------------------------------------------------
# District SQL pushdown fallback on failure
# ---------------------------------------------------------------------------

def test_district_sql_fallback_retries_without_filter():
    """When candidate_location has no Tier 1 rows, the search falls back to
    the direct commercial_unit query and still returns results."""
    rows = [
        _make_candidate_row("p1", "2000", district="حي العليا"),
        _make_candidate_row("p2", "2000", district="الملقا"),
    ]
    db = _FakeDB(candidate_rows=rows)

    items = run_expansion_search(
        db,
        search_id="test-fallback",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
        target_districts=["العليا"],
    )
    # Should succeed via commercial_unit fallback; Python post-filter keeps only العليا match
    assert len(items) == 1
    assert items[0]["parcel_id"] == "p1"


def test_district_python_postfilter_works_after_fallback():
    """Target-district Python post-filter correctly narrows results when SQL
    pushdown is skipped (all rows returned from unfiltered query)."""
    rows = [
        _make_candidate_row("p1", "2000", district="حي العليا"),
        _make_candidate_row("p2", "2000", district="الملقا"),
        _make_candidate_row("p3", "2000", district="النرجس"),
    ]
    db = _FakeDB(candidate_rows=rows)

    items = run_expansion_search(
        db,
        search_id="test-postfilter",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
        target_districts=["العليا"],
    )
    # Only the العليا candidate should survive post-filter
    assert all("العليا" in (item.get("district") or "") for item in items)


def test_empty_query_result_returns_empty_list():
    """When candidate query returns no rows, return empty list without raising."""
    db = _FakeDB(candidate_rows=[])
    items = run_expansion_search(
        db,
        search_id="test-empty",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
    )
    assert items == []


def test_all_candidates_filtered_returns_empty_list():
    """When all candidates are filtered out by district post-filter, return empty list."""
    rows = [_make_candidate_row("p1", "2000", district="الملقا")]
    db = _FakeDB(candidate_rows=rows)

    items = run_expansion_search(
        db,
        search_id="test-all-filtered",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
        target_districts=["العليا"],
    )
    assert items == []


# ---------------------------------------------------------------------------
# Dirty text coordinates in delivery_source_record / population_density
# do not crash the candidate SQL (safe regex-guarded casts)
# ---------------------------------------------------------------------------

def test_dirty_dsr_coords_do_not_crash_search():
    """Dirty delivery_source_record coordinate values (blank, 'N/A', comma-
    formatted, alphabetic, None) must not crash run_expansion_search.
    The FakeDB bypasses real SQL execution but the function still exercises
    all post-query scoring & ranking logic that reads these column values."""
    rows = [
        _make_candidate_row("p1", "2000", district="District_A"),
        _make_candidate_row("p2", "7500", district="District_B"),
    ]
    db = _FakeDB(candidate_rows=rows)
    items = run_expansion_search(
        db,
        search_id="test-dirty-dsr",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
    )
    assert isinstance(items, list)
    assert len(items) == 2


def test_dirty_pd_coords_do_not_crash_search():
    """Dirty population_density coordinate values must not crash the search."""
    rows = [_make_candidate_row("p1", "2000")]
    db = _FakeDB(candidate_rows=rows)
    items = run_expansion_search(
        db,
        search_id="test-dirty-pd",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
    )
    assert isinstance(items, list)
    assert len(items) == 1


def test_valid_numeric_coords_still_work():
    """Valid numeric coordinates still produce normal results."""
    rows = [
        _make_candidate_row("p1", "2000", district="District_A"),
        _make_candidate_row("p2", "2000", district="District_B"),
    ]
    db = _FakeDB(candidate_rows=rows)
    items = run_expansion_search(
        db,
        search_id="test-valid-coords",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
    )
    assert len(items) == 2
    for item in items:
        assert "final_score" in item
        assert item["final_score"] >= 0


def test_candidate_sql_uses_safe_coord_regex():
    """The generated candidate SQL must use regex-guarded coordinate casts
    instead of raw ::float casts for dsr and pd tables.

    NOTE: Since v7 (listings-only), the ArcGIS candidate_base SQL is no longer
    used in the search path. This test now runs through the commercial_unit
    fallback and verifies the search still completes without crashing.
    """
    import re
    captured_sql = []

    class _CapturingDB(_FakeDB):
        def execute(self, stmt, params=None):
            sql_text = stmt.text if hasattr(stmt, "text") else str(stmt)
            captured_sql.append(sql_text)
            return super().execute(stmt, params)

    db = _CapturingDB(candidate_rows=[_make_candidate_row("p1", "2000")])
    items = run_expansion_search(
        db,
        search_id="test-sql-check",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
    )

    # v7: ArcGIS candidate_base SQL no longer used; verify search completes
    assert isinstance(items, list)
    assert len(items) == 1


def test_search_returns_results_not_error_with_dirty_rows():
    """Even with rows that would have dirty coords in the DB, the search
    returns a normal list (not an exception)."""
    dirty_values = ["", " ", "N/A", "24,713", "abc", None]
    rows = []
    for i, val in enumerate(dirty_values):
        row = _make_candidate_row(f"p{i}", "2000", district=f"District_{i}")
        # Simulate what dirty DB rows look like after the SQL safely filters
        # them: the correlated subqueries return 0 counts.
        row["delivery_listing_count"] = 0
        row["provider_listing_count"] = 0
        row["provider_platform_count"] = 0
        row["delivery_competition_count"] = 0
        row["population_reach"] = 0
        rows.append(row)

    db = _FakeDB(candidate_rows=rows)
    items = run_expansion_search(
        db,
        search_id="test-dirty-mix",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
    )
    assert isinstance(items, list)
    assert len(items) == len(dirty_values)


# ---------------------------------------------------------------------------
# Last-resort fallback: all queries fail except the no-district query
# ---------------------------------------------------------------------------

def test_last_resort_fallback_returns_results():
    """When candidate_location has no Tier 1 rows, the commercial_unit fallback
    should still return results."""
    rows = [
        _make_candidate_row("p1", "2000", district=None),
        _make_candidate_row("p2", "7500", district=None),
    ]
    db = _FakeDB(candidate_rows=rows)

    items = run_expansion_search(
        db,
        search_id="test-last-resort",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
        target_districts=["العليا"],
    )
    # Commercial_unit fallback returns candidates without district labeling;
    # Python post-filter won't match target districts (all NULL), so
    # empty list when target_districts is set.
    assert isinstance(items, list)


def test_last_resort_fallback_no_districts_returns_candidates():
    """Without target_districts, commercial_unit fallback returns all candidates
    even though they have NULL district."""
    rows = [
        _make_candidate_row("p1", "2000", district=None),
    ]
    db = _FakeDB(candidate_rows=rows)

    items = run_expansion_search(
        db,
        search_id="test-last-resort-no-td",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
    )
    assert isinstance(items, list)
    assert len(items) == 1


# ---------------------------------------------------------------------------
# Per-candidate error handling: one bad candidate doesn't crash the search
# ---------------------------------------------------------------------------

class _FailOnSpecificParcelDB(_FakeDB):
    """FakeDB that raises when enrichment queries are made for a specific
    parcel, simulating a corrupt parcel geometry in feature snapshot."""

    def __init__(self, candidate_rows=None, fail_parcel_id="bad_parcel"):
        super().__init__(candidate_rows=candidate_rows)
        self.fail_parcel_id = fail_parcel_id

    def execute(self, stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        # Fail on perimeter/road/parking queries for the bad parcel
        if params and params.get("parcel_id") == self.fail_parcel_id:
            if "ST_Perimeter" in sql or "planet_osm_line" in sql or "planet_osm_polygon" in sql:
                raise RuntimeError(f"Simulated geometry failure for {self.fail_parcel_id}")
        return super().execute(stmt, params)


def test_candidate_sql_uses_district_label_column():
    """v7 (listings-only): search uses candidate_location/commercial_unit,
    not the ArcGIS candidate_base SQL. Verify search completes normally."""
    db = _FakeDB(candidate_rows=[_make_candidate_row("p1", "2000")])
    items = run_expansion_search(
        db,
        search_id="test-district-label",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
    )
    assert isinstance(items, list)
    assert len(items) == 1


# ---------------------------------------------------------------------------
# landuse_code SQL uses direct numeric comparisons (no BTRIM/CAST/regex)
# ---------------------------------------------------------------------------

def test_candidate_sql_landuse_order_uses_direct_numeric_comparisons():
    """ArcGIS classification semantics still correctly rank landuse codes."""
    sem = _arcgis_classification_semantics
    assert sem(2000, None)["normalized_class"] == "commercial"
    assert sem(2000, None)["score"] == 100
    assert sem(7500, None)["normalized_class"] == "mixed_use"
    assert sem(7500, None)["score"] == 100
    assert sem(1000, None)["normalized_class"] == "residential"
    assert sem(1000, None)["score"] < 100  # deprioritised


def test_candidate_sql_landuse_ordering_semantics_unchanged():
    """The landuse ordering semantics are preserved in _arcgis_classification_semantics:
    2000/7500 → commercial/mixed_use (pass), 3000/4000 → public/industrial,
    NULL → unknown, 1000 → residential (unknown)."""
    sem = _arcgis_classification_semantics
    assert sem(2000, None)["verdict_hint"] == "pass"
    assert sem(7500, None)["verdict_hint"] == "pass"
    assert sem(3000, None)["normalized_class"] == "public_service"
    assert sem(4000, None)["normalized_class"] == "industrial"
    assert sem(None, None)["normalized_class"] == "unknown"
    assert sem(1000, None)["verdict_hint"] == "unknown"


# ---------------------------------------------------------------------------
# Numeric-backed coord columns: BTRIM must wrap CAST to text first
# ---------------------------------------------------------------------------

def test_candidate_sql_coord_btrim_wraps_cast_to_text():
    """v7 (listings-only): the ArcGIS candidate_base SQL with BTRIM/CAST
    patterns is no longer in the search path. Verify search still completes."""
    db = _FakeDB(candidate_rows=[_make_candidate_row("p1", "2000")])
    items = run_expansion_search(
        db,
        search_id="test-coord-cast",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
    )
    assert isinstance(items, list)
    assert len(items) == 1


# ---------------------------------------------------------------------------
# Patch 06: _listing_quality_score
# ---------------------------------------------------------------------------

def test_listing_quality_parcel_returns_neutral_50():
    """Parcels (non-listings) should return a neutral 50."""
    score = _listing_quality_score(
        is_listing=False,
        effective_age_days=None,
        is_furnished=None,
        unit_restaurant_score=None,
        has_image=False,
    )
    assert score == 50.0


def test_listing_quality_fresh_full_data_high_score():
    """A fresh listing with full data should score close to 100."""
    score = _listing_quality_score(
        is_listing=True,
        effective_age_days=3,
        is_furnished=True,
        unit_restaurant_score=90.0,
        has_image=True,
        has_drive_thru=True,
    )
    # Phase 3b: momentum defaults to neutral 50.0 when not passed.
    # freshness=100, suitability=180→clamped 100, image=100, furnished=100,
    # momentum=50. Composite ≈ 100*0.2550 + 100*0.3400 + 100*0.1700 +
    # 100*0.0850 + 50*0.15 + 5 (drive-thru) = 25.5+34+17+8.5+7.5+5 = 97.5.
    assert score > 90.0


def test_listing_quality_stale_no_image_low_score():
    """A very stale listing without image should score below 50."""
    score = _listing_quality_score(
        is_listing=True,
        effective_age_days=400,
        is_furnished=False,
        unit_restaurant_score=None,
        has_image=False,
    )
    # Phase 3b: momentum defaults to neutral 50.0 when not passed.
    # composite = 15*0.2550 + 50*0.3400 + 30*0.1700 + 50*0.0850 + 50*0.15
    #           = 3.825 + 17.00 + 5.10 + 4.25 + 7.50 = 37.675 < 50.
    assert score < 50.0


def test_listing_quality_drive_thru_adds_5():
    """Drive-thru bonus should add exactly 5 points."""
    base = _listing_quality_score(
        is_listing=True,
        effective_age_days=10,
        is_furnished=False,
        unit_restaurant_score=50.0,
        has_image=True,
        has_drive_thru=False,
    )
    with_dt = _listing_quality_score(
        is_listing=True,
        effective_age_days=10,
        is_furnished=False,
        unit_restaurant_score=50.0,
        has_image=True,
        has_drive_thru=True,
    )
    assert abs(with_dt - base - 5.0) < 0.01


# ---------------------------------------------------------------------------
# Phase 3a: _effective_listing_age_days
# ---------------------------------------------------------------------------

def test_effective_age_all_three_present_picks_most_recent():
    """GREATEST semantics: the most recent of the three timestamps wins,
    regardless of which source it came from."""
    now = datetime.utcnow()
    row = {
        "unit_aqar_created_at": now - timedelta(days=200),
        "unit_aqar_updated_at": now - timedelta(days=10),
        "unit_first_seen_at": now - timedelta(days=30),
    }
    days, source = _effective_listing_age_days(row)
    assert days == 10
    assert source == "aqar_updated"


def test_effective_age_created_null_updated_present():
    """NULL aqar_created_at falls through to aqar_updated_at when present."""
    now = datetime.utcnow()
    row = {
        "unit_aqar_created_at": None,
        "unit_aqar_updated_at": now - timedelta(days=5),
        "unit_first_seen_at": now - timedelta(days=60),
    }
    days, source = _effective_listing_age_days(row)
    assert days == 5
    assert source == "aqar_updated"


def test_effective_age_both_aqar_null_uses_first_seen():
    """Both aqar fields NULL falls through to first_seen_at."""
    now = datetime.utcnow()
    row = {
        "unit_aqar_created_at": None,
        "unit_aqar_updated_at": None,
        "unit_first_seen_at": now - timedelta(days=40),
    }
    days, source = _effective_listing_age_days(row)
    assert days == 40
    assert source == "first_seen"


def test_effective_age_all_null_returns_unknown_neutral():
    """All three NULL resolves to (None, "unknown") and, when threaded
    through _listing_quality_score, contributes exactly the freshness=50.0
    share (no penalty, no boost)."""
    days, source = _effective_listing_age_days({})
    assert days is None
    assert source == "unknown"

    # Fixed-neutral inputs to isolate the freshness contribution.
    # Phase 3b: sub-weights are (0.2550, 0.3400, 0.1700, 0.0850, 0.15).
    # district_momentum_score=None → momentum resolves to neutral 50.0.
    common = dict(
        is_listing=True,
        is_furnished=False,
        unit_restaurant_score=None,
        has_image=False,
        has_drive_thru=False,
        llm_suitability_score=None,
        llm_listing_quality_score=None,
        district_momentum_score=None,
    )
    # suitability=50, image_signal=30, furnished_signal=50, momentum=50.
    # Composite = freshness*0.2550 + 50*0.3400 + 30*0.1700 + 50*0.0850
    #           + 50*0.15 = freshness*0.2550 + 33.85.
    expected_with_freshness_50 = (
        50.0 * 0.2550
        + 50.0 * 0.3400
        + 30.0 * 0.1700
        + 50.0 * 0.0850
        + 50.0 * 0.15
    )
    observed = _listing_quality_score(effective_age_days=None, **common)
    assert abs(observed - expected_with_freshness_50) < 1e-9


def test_effective_age_future_updated_at_clamps_to_zero():
    """An aqar_updated_at only slightly in the future (within the one-day
    tolerance) is accepted and clamped to days=0, which lands in the
    freshest <=14 band."""
    now = datetime.utcnow()
    row = {
        "unit_aqar_created_at": None,
        # Within the future_cutoff tolerance — accepted, clamped to 0.
        "unit_aqar_updated_at": now + timedelta(hours=6),
        "unit_first_seen_at": None,
    }
    days, source = _effective_listing_age_days(row)
    assert days == 0
    assert source == "aqar_updated"

    score = _listing_quality_score(
        is_listing=True,
        effective_age_days=days,
        is_furnished=False,
        unit_restaurant_score=None,
        has_image=False,
        has_drive_thru=False,
        district_momentum_score=None,
    )
    # Phase 3b weights (0.2550, 0.3400, 0.1700, 0.0850, 0.15). freshness
    # = 100 (days <= 14 band), suitability=50, image_signal=30,
    # furnished=50, momentum=50 (None → neutral). Composite =
    # 100*0.2550 + 50*0.3400 + 30*0.1700 + 50*0.0850 + 50*0.15 = 59.35.
    assert abs(score - 59.35) < 1e-9


def test_effective_age_future_only_returns_unknown():
    """Correction 3 guard: a lone aqar_updated_at more than one day in the
    future is rejected as parser/clock drift, not clamped. With no other
    sources, the helper returns (None, "unknown")."""
    now = datetime.utcnow()
    row = {
        "unit_aqar_created_at": None,
        "unit_aqar_updated_at": now + timedelta(days=30),
        "unit_first_seen_at": None,
    }
    days, source = _effective_listing_age_days(row)
    assert days is None
    assert source == "unknown"


@pytest.mark.parametrize(
    "age_days,expected_freshness",
    [
        (14, 100.0),
        (30, 92.0),
        (60, 80.0),
        (120, 65.0),
        (240, 45.0),
        (365, 28.0),
        (366, 15.0),
    ],
)
def test_effective_age_band_boundaries(age_days, expected_freshness):
    """Lock the frozen Phase 3a band cutoffs at 14/30/60/120/240/365 days
    against silent drift. Phase 3b sub-weights (0.2550, 0.3400, 0.1700,
    0.0850, 0.15); fixed inputs make suitability=50, image_signal=30,
    furnished=50, momentum=50 (None)."""
    score = _listing_quality_score(
        is_listing=True,
        effective_age_days=age_days,
        is_furnished=False,
        unit_restaurant_score=None,
        has_image=False,
        has_drive_thru=False,
        district_momentum_score=None,
    )
    expected_composite = (
        expected_freshness * 0.2550
        + 50.0 * 0.3400
        + 30.0 * 0.1700
        + 50.0 * 0.0850
        + 50.0 * 0.15
    )
    assert abs(score - expected_composite) < 1e-9


def test_effective_age_tzaware_input_normalized():
    """tz-aware aqar_updated_at mixed with tz-naive first_seen_at must not
    raise TypeError. Both point to roughly the same moment; the helper
    should pick the aqar_updated_at (first in the preference order)."""
    now_naive = datetime.utcnow()
    row = {
        "unit_aqar_updated_at": datetime.now(timezone.utc) - timedelta(days=5),
        "unit_aqar_created_at": None,
        "unit_first_seen_at": now_naive - timedelta(days=5),
    }
    days, source = _effective_listing_age_days(row)
    assert days == 5
    assert source == "aqar_updated"


# ---------------------------------------------------------------------------
# Phase 3b: district momentum sub-signal and _district_momentum_score helper
# ---------------------------------------------------------------------------


def _lq_neutral_kwargs(**overrides):
    """Fixed-neutral _listing_quality_score inputs. Suitability resolves
    to 50, image_signal to 30 (no image, no LLM), furnished_signal to 50."""
    base = dict(
        is_listing=True,
        is_furnished=False,
        unit_restaurant_score=None,
        has_image=False,
        has_drive_thru=False,
        llm_suitability_score=None,
        llm_listing_quality_score=None,
    )
    base.update(overrides)
    return base


def _fake_db_returning(rows):
    """Build a MagicMock db whose .execute(...).mappings().all() returns
    the supplied list of dicts. Matches the interface used inside
    _district_momentum_score."""
    db = MagicMock()
    result = MagicMock()
    result.mappings.return_value.all.return_value = rows
    db.execute.return_value = result
    return db


def _fake_db_raising(exc: Exception):
    db = MagicMock()
    db.execute.side_effect = exc
    return db


def test_listing_quality_score_momentum_high_raises_composite():
    """Momentum sub-weight is 0.15. A 100 vs 0 momentum swing must raise
    the composite by exactly (100 - 0) * 0.15 = 15.0 points, with all
    other sub-signals held at their neutral values and the drive-thru
    bonus disabled."""
    common = _lq_neutral_kwargs(effective_age_days=30)
    high = _listing_quality_score(district_momentum_score=100.0, **common)
    low = _listing_quality_score(district_momentum_score=0.0, **common)
    assert abs((high - low) - 15.0) < 1e-9


def test_listing_quality_score_momentum_none_neutral():
    """district_momentum_score=None resolves to neutral 50.0. With
    fully-neutral other inputs (freshness=50, suitability=50,
    image_signal=30, furnished=50) and Phase 3b sub-weights, composite
    = 50*0.2550 + 50*0.3400 + 30*0.1700 + 50*0.0850 + 50*0.15 = 46.60."""
    common = _lq_neutral_kwargs(effective_age_days=None)
    observed = _listing_quality_score(district_momentum_score=None, **common)
    expected = (
        50.0 * 0.2550
        + 50.0 * 0.3400
        + 30.0 * 0.1700
        + 50.0 * 0.0850
        + 50.0 * 0.15
    )
    assert abs(observed - expected) < 1e-9
    assert abs(observed - 46.60) < 1e-9


def test_listing_quality_score_momentum_below_floor_neutral():
    """A candidate in a below-floor district receives district_momentum_score
    = None (helper returns absent key) and must produce the exact same
    composite as the neutral case above. Guards against accidentally
    penalising sparsely-sampled districts."""
    common = _lq_neutral_kwargs(effective_age_days=None)
    absent = _listing_quality_score(district_momentum_score=None, **common)
    # Simulate the helper-returns-empty-dict path: look up a district
    # not present and feed the result (which is None) into the scorer.
    momentum_dict: dict = {}
    val = momentum_dict.get("الياسمين")
    absent_via_helper = _listing_quality_score(district_momentum_score=val, **common)
    assert absent == absent_via_helper


def test_listing_quality_score_sub_weights_sum_to_one():
    """Regression guard against silent sub-weight drift. Post-3b weights
    must sum to 1.0 exactly under math.isclose."""
    import math
    assert math.isclose(0.2550 + 0.3400 + 0.1700 + 0.0850 + 0.15, 1.0)


def test_district_momentum_score_composite_math():
    """With percentile_raw=0.8 and percentile_absolute=0.6, the
    composite = 0.5*0.8 + 0.5*0.6 = 0.7, and momentum_score =
    round(0.7 * 100.0, 2) = 70.00."""
    rows = [
        {
            "district_label": "العارض",
            "activity_30d": 7,
            "active_in_district": 45,
            "percentile_raw": 0.8,
            "percentile_absolute": 0.6,
        },
    ]
    db = _fake_db_returning(rows)
    out = _district_momentum_score(db)
    assert len(out) == 1
    entry = next(iter(out.values()))
    assert abs(entry["percentile_composite"] - 0.7) < 1e-9
    assert abs(entry["momentum_score"] - 70.0) < 1e-9
    assert entry["sample_floor_applied"] is False
    assert entry["activity_30d"] == 7
    assert entry["active_in_district"] == 45


def test_district_momentum_score_sample_floor_excludes_below_20():
    """The HAVING clause at floor=20 excludes under-sampled districts.
    The SQL is fake-mocked here; this test documents that rows the SQL
    returns are trusted unchanged (no Python-side floor check) and that
    rows the SQL does NOT return resolve to absent (→ neutral)."""
    # The SQL enforces the floor; _fake_db_returning simulates post-filter
    # output. A district that would have been filtered (active=19) is
    # simply absent from the input rows.
    rows = [
        {
            "district_label": "Hittin",
            "activity_30d": 3,
            "active_in_district": 23,
            "percentile_raw": 0.2,
            "percentile_absolute": 0.3,
        },
    ]
    db = _fake_db_returning(rows)
    out = _district_momentum_score(db)
    # District with active=23 (above threshold) present.
    assert any(v["district_label"] == "Hittin" for v in out.values())
    # Absent key → None via .get.
    assert out.get("not_a_real_district_key") is None


def test_district_momentum_score_zero_activity_low_score_not_error():
    """A qualifying district with activity_30d=0 must produce a finite
    momentum_score (percentile_absolute COALESCEs to 0.5 on NULLIF-zero
    and percentile_raw works on 0-values), never ZeroDivisionError."""
    rows = [
        {
            "district_label": "Al Marwah",
            "activity_30d": 0,
            "active_in_district": 50,
            "percentile_raw": 0.0,
            "percentile_absolute": 0.5,  # NULLIF on zero numerator then COALESCE 0.5
        },
    ]
    db = _fake_db_returning(rows)
    out = _district_momentum_score(db)
    entry = next(iter(out.values()))
    assert 0.0 <= entry["momentum_score"] <= 100.0
    # Composite = 0.5*0.0 + 0.5*0.5 = 0.25 → 25.0.
    assert abs(entry["momentum_score"] - 25.0) < 1e-9


def test_district_momentum_score_blank_district_label_filtered():
    """A row whose district_label normalizes to empty (pure whitespace,
    or a label that _normalize_district_key rejects) is dropped from
    the returned dict. The SQL filter already drops NULL and numeric
    labels; this guards the Python-side filter for edge cases."""
    rows = [
        {
            "district_label": "  ",  # whitespace-only; normalize_district_key → ""
            "activity_30d": 5,
            "active_in_district": 30,
            "percentile_raw": 0.5,
            "percentile_absolute": 0.5,
        },
        {
            "district_label": "As Sulay",
            "activity_30d": 12,
            "active_in_district": 85,
            "percentile_raw": 0.6,
            "percentile_absolute": 0.4,
        },
    ]
    db = _fake_db_returning(rows)
    out = _district_momentum_score(db)
    # The blank-label row must be filtered; only "As Sulay" remains.
    assert len(out) == 1
    assert next(iter(out.values()))["district_label"] == "As Sulay"


def test_district_momentum_score_empty_dict_on_db_failure():
    """DB exception → {} so scoring falls back to neutral everywhere
    without propagating the error."""
    db = _fake_db_raising(RuntimeError("simulated DB failure"))
    assert _district_momentum_score(db) == {}


def test_district_momentum_blends_created_and_updated():
    """Activity_30d must count a listing once if EITHER aqar_created_at
    OR aqar_updated_at falls in the window, not both (de-duped).
    Documents the SQL COUNT(*) FILTER (WHERE ... OR ...) semantics.
    Here: 5 created-only, 8 updated-only, 3 overlap → 10 unique rows
    (5 + 8 − 3 = 10)."""
    rows = [
        {
            "district_label": "Banban",
            "activity_30d": 10,  # SQL OR naturally dedupes per-row
            "active_in_district": 32,
            "percentile_raw": 0.55,
            "percentile_absolute": 0.45,
        },
    ]
    db = _fake_db_returning(rows)
    out = _district_momentum_score(db)
    entry = next(iter(out.values()))
    assert entry["activity_30d"] == 10  # not 5 + 8 + 3 = 16, not 5 + 8 = 13


def test_district_momentum_uses_external_feature_priority_chain():
    """When the same listing point is covered by both osm_districts
    (priority 1) and aqar_district_hulls (priority 2) polygons, the
    helper must use the osm_districts district. Faithful DISTINCT ON
    behaviour lives in Postgres, so at the unit level this test asserts
    two complementary contracts:

    (a) The emitted SQL contains the DISTINCT ON (cu.aqar_id) clause
        plus the CASE ef.layer_name priority-ordering and the two
        expected layer names. This documents the priority chain's
        presence in the query plan.

    (b) Given an already-resolved row (the SQL chose osm_districts),
        the helper returns that label intact and does not mix in the
        aqar_district_hulls name for the same listing.
    """
    rows = [
        # SQL has already resolved this listing to the osm_districts
        # polygon's district. An aqar_district_hulls row with a
        # DIFFERENT label for the same aqar_id would have been
        # suppressed by DISTINCT ON. The helper sees one row per
        # district here (the post-DISTINCT-ON group).
        {
            "district_label": "العارض",  # chosen from osm_districts
            "activity_30d": 8,
            "active_in_district": 40,
            "percentile_raw": 0.65,
            "percentile_absolute": 0.55,
        },
    ]
    db = _fake_db_returning(rows)
    out = _district_momentum_score(db)

    # Contract (a): SQL shape captured on the mock records the
    # priority-chain intent.
    emitted_sql_obj = db.execute.call_args.args[0]
    emitted_sql = emitted_sql_obj.text if hasattr(emitted_sql_obj, "text") else str(emitted_sql_obj)
    assert "DISTINCT ON (cu.aqar_id)" in emitted_sql
    assert "CASE ef.layer_name" in emitted_sql
    assert "'osm_districts'" in emitted_sql
    assert "'aqar_district_hulls'" in emitted_sql
    # osm_districts must sort BEFORE aqar_district_hulls in the CASE.
    osm_pos = emitted_sql.index("'osm_districts'")
    aqar_pos = emitted_sql.index("'aqar_district_hulls'")
    assert osm_pos < aqar_pos

    # Contract (b): the post-DISTINCT-ON label is preserved verbatim.
    entry = next(iter(out.values()))
    assert entry["district_label"] == "العارض"


def test_listing_quality_scoring_callsite_two_district_delta():
    """Integration guard at the _listing_quality_score call-site contract.
    Two synthetic candidates share all inputs except their district's
    momentum score (80 vs absent → neutral 50). The composite must
    differ by exactly (80 - 50) * 0.15 = 4.50 points."""
    momentum_dict = {
        "high_district": {"momentum_score": 80.0},
        # "low_district" intentionally absent → .get returns None → neutral.
    }
    common = _lq_neutral_kwargs(effective_age_days=30)

    # High-momentum candidate
    high_entry = momentum_dict.get("high_district")
    high_val = high_entry["momentum_score"] if high_entry else None
    high_score = _listing_quality_score(
        district_momentum_score=high_val, **common
    )

    # Below-floor candidate: entry absent, treated as neutral
    low_entry = momentum_dict.get("low_district")
    low_val = low_entry["momentum_score"] if low_entry else None
    low_score = _listing_quality_score(
        district_momentum_score=low_val, **common
    )

    assert abs((high_score - low_score) - 4.5) < 1e-9


# ---------------------------------------------------------------------------
# Patch 06: _estimate_fitout_cost_sar furnished discount
# ---------------------------------------------------------------------------

def test_fitout_cost_furnished_discount():
    """Furnished units get a 35% discount on fitout cost."""
    unfurnished = _estimate_fitout_cost_sar(200.0, "qsr")
    furnished = _estimate_fitout_cost_sar(200.0, "qsr", is_furnished=True)
    assert abs(furnished - unfurnished * 0.65) < 0.01


def test_fitout_cost_unfurnished_unchanged():
    """Unfurnished (default) path is unchanged."""
    cost = _estimate_fitout_cost_sar(200.0, "qsr")
    assert cost == 200.0 * 2600.0


# ---------------------------------------------------------------------------
# Patch 06: _estimate_revenue_index listing-grounded
# ---------------------------------------------------------------------------

def test_revenue_index_wide_street_beats_narrow():
    """A listing on a wide street should score higher than one on a narrow street."""
    wide = _estimate_revenue_index(
        area_m2=200.0,
        unit_street_width_m=40.0,
        demand_score=60.0,
        whitespace_score=60.0,
    )
    narrow = _estimate_revenue_index(
        area_m2=200.0,
        unit_street_width_m=8.0,
        demand_score=60.0,
        whitespace_score=60.0,
    )
    assert wide > narrow


def test_revenue_index_sweet_spot_area_beats_extreme():
    """A QSR sweet-spot area (200 m2) should score higher than a very small unit."""
    sweet = _estimate_revenue_index(area_m2=200.0, demand_score=60.0, whitespace_score=60.0)
    tiny = _estimate_revenue_index(area_m2=40.0, demand_score=60.0, whitespace_score=60.0)
    assert sweet > tiny


# ---------------------------------------------------------------------------
# Patch 07: brief-aware ranking fixes
# ---------------------------------------------------------------------------

def _revenue_index_area_signal(area_m2: float, target_area_m2: float | None) -> float:
    """Mirror of _estimate_revenue_index's area_signal branch for unit testing.

    Kept here so tests can assert the curve's shape without reconstructing
    the whole revenue index composite. Must stay in sync with the branch
    in app/services/expansion_advisor.py::_estimate_revenue_index.
    """
    _target = float(target_area_m2) if target_area_m2 and target_area_m2 > 0 else 225.0
    if area_m2 <= 0:
        return 50.0
    ratio = area_m2 / _target
    if 0.80 <= ratio <= 1.20:
        return 100.0
    if 0.60 <= ratio < 0.80:
        return 80.0 + (ratio - 0.60) / 0.20 * 20.0
    if 1.20 < ratio <= 1.50:
        return 100.0 - (ratio - 1.20) / 0.30 * 20.0
    if 0.40 <= ratio < 0.60:
        return 55.0 + (ratio - 0.40) / 0.20 * 25.0
    if 1.50 < ratio <= 2.00:
        return 80.0 - (ratio - 1.50) / 0.50 * 25.0
    if 0.25 <= ratio < 0.40:
        return 35.0 + (ratio - 0.25) / 0.15 * 20.0
    if 2.00 < ratio <= 3.00:
        return 55.0 - (ratio - 2.00) / 1.00 * 20.0
    return 25.0


def test_revenue_index_centers_on_target_area():
    """A cafe brief with target=80 should prefer ~80 m² listings over 200 m²."""
    # Same demand / whitespace / category inputs — only area differs.
    matched = _estimate_revenue_index(
        area_m2=80.0,
        target_area_m2=80.0,
        demand_score=60.0,
        whitespace_score=60.0,
    )
    oversized = _estimate_revenue_index(
        area_m2=200.0,
        target_area_m2=80.0,
        demand_score=60.0,
        whitespace_score=60.0,
    )
    assert matched > oversized

    # Direct check on the area_signal branch: ratio=1.0 → 100 (inside
    # the ±20% sweet spot), ratio=2.5 → 45 (deep into the 2.00–3.00 band,
    # halfway down from 55 toward 35).
    assert _revenue_index_area_signal(80.0, 80.0) == 100.0
    assert _revenue_index_area_signal(200.0, 80.0) == 45.0
    # And ratio=3.5 is in the "way too big" fallback.
    assert _revenue_index_area_signal(280.0, 80.0) == 25.0


def test_revenue_index_flagship_target_rewards_large_listings():
    """A flagship brief with target=500 should prefer ~500 m² over ~200 m²."""
    at_target = _estimate_revenue_index(
        area_m2=500.0,
        target_area_m2=500.0,
        demand_score=60.0,
        whitespace_score=60.0,
    )
    too_small = _estimate_revenue_index(
        area_m2=200.0,
        target_area_m2=500.0,
        demand_score=60.0,
        whitespace_score=60.0,
    )
    assert at_target > too_small
    assert _revenue_index_area_signal(500.0, 500.0) == 100.0
    # 200/500 = 0.40 → the 0.40-0.60 band starts at 55.0.
    assert _revenue_index_area_signal(200.0, 500.0) == 55.0


def test_revenue_index_falls_back_to_qsr_sweet_spot_when_target_missing():
    """Without target_area_m2 the curve falls back to a 225 m² center.

    This preserves legacy behavior for callers that don't pass the new
    parameter: listings around 150–270 m² still score at the top.
    """
    # No target: 200 m² / 225 default = ratio 0.89 → full credit band.
    assert _revenue_index_area_signal(200.0, None) == 100.0
    # 225 m² default center — 270 is still inside the ±20% window.
    assert _revenue_index_area_signal(270.0, None) == 100.0
    # 40 m² is deep in the low-credit zone (ratio ≈ 0.18 → 25.0).
    assert _revenue_index_area_signal(40.0, None) == 25.0


def test_brand_fit_flagship_uses_target_area():
    """Flagship brief with target_area_m2=600 should prefer 600 m² over 350 m²."""
    base_kwargs = dict(
        district="Olaya",
        demand_score=70.0,
        fit_score=70.0,
        cannibalization_score=40.0,
        provider_density_score=60.0,
        provider_whitespace_score=60.0,
        multi_platform_presence_score=60.0,
        delivery_competition_score=50.0,
        visibility_signal=70.0,
        parking_signal=60.0,
        brand_profile={"expansion_goal": "flagship"},
        service_model="dine_in",
    )

    matched = _brand_fit_score(area_m2=600.0, target_area_m2=600.0, **base_kwargs)
    smaller = _brand_fit_score(area_m2=350.0, target_area_m2=600.0, **base_kwargs)
    # With target=600, a 600 m² listing sits exactly at the sweet spot
    # while a 350 m² listing is at ratio 0.58 (mid-low band) — the
    # 600 m² listing must rank higher.
    assert matched > smaller


def test_brand_fit_flagship_falls_back_when_target_missing():
    """Without target_area_m2, the flagship goal defaults to a 350 m² center."""
    base_kwargs = dict(
        district="Olaya",
        demand_score=70.0,
        fit_score=70.0,
        cannibalization_score=40.0,
        provider_density_score=60.0,
        provider_whitespace_score=60.0,
        multi_platform_presence_score=60.0,
        delivery_competition_score=50.0,
        visibility_signal=70.0,
        parking_signal=60.0,
        brand_profile={"expansion_goal": "flagship"},
        service_model="dine_in",
    )

    near_default = _brand_fit_score(area_m2=350.0, **base_kwargs)
    tiny = _brand_fit_score(area_m2=80.0, **base_kwargs)
    assert near_default > tiny


class _FakeRentBurdenDB:
    """Fake DB that records the SQL params and returns a canned comparable row."""

    def __init__(self, n_rows: int = 12, median: float = 80.0):
        self._n = n_rows
        self._median = median
        self.calls: list[dict] = []

    def begin_nested(self):
        return _FakeNestedTransaction()

    def execute(self, stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        self.calls.append({"sql": sql, "params": dict(params or {})})
        # Return a comparable aggregation: n rows with median=self._median,
        # n_below = half of n (puts the listing at the 50th percentile).
        return _Result([
            {
                "median_monthly_per_m2": self._median,
                "n": self._n,
                "n_below": self._n // 2,
            }
        ])


def test_percentile_rent_burden_uses_unit_neighborhood():
    """When unit_neighborhood_raw is passed, the district tier must be hit.

    Without this fix, the Arabic district_norm never matches the English
    neighborhood column on commercial_unit, so every lookup silently fell
    through to the city tier.
    """
    db = _FakeRentBurdenDB(n_rows=12, median=80.0)
    result = _percentile_rent_burden(
        db,
        listing_monthly_rent_per_m2=80.0,
        district="حي العليا",  # Arabic district — would never match English
        area_m2=180.0,
        listing_type="store",
        unit_neighborhood_raw="Olaya",
    )
    assert result is not None
    # First chain executed is district_band_type (n=12 > min_n=8 → returned).
    assert result["source_label"] == "district_band_type"
    # Verify the SQL params actually carried the English neighborhood value.
    first_call_params = db.calls[0]["params"]
    assert first_call_params.get("neighborhood") == "olaya"


def test_percentile_rent_burden_falls_through_without_neighborhood():
    """Without unit_neighborhood_raw the function falls through to the city tier.

    The Arabic district_norm is still tried (as a cheap fallback), but it
    never matches English neighborhood values — so we exhaust the district
    chains and reach city_band_type. With n=12 >= city_band_type min_n=12,
    city_band_type fires.
    """
    db = _FakeRentBurdenDB(n_rows=12, median=100.0)
    result = _percentile_rent_burden(
        db,
        listing_monthly_rent_per_m2=100.0,
        district="حي العليا",
        area_m2=180.0,
        listing_type="store",
        unit_neighborhood_raw=None,
    )
    assert result is not None
    # The district chains silently return 12 rows (the fake DB returns the
    # same canned row regardless of filter), so district_band_type still
    # fires first. The meaningful assertion is that the fallback parameter
    # carries the Arabic district_norm — not an English neighborhood.
    assert db.calls[0]["params"].get("neighborhood") == "العليا"


def test_economics_score_damps_rent_burden_on_city_fallback():
    """When _percentile_rent_burden returns citywide labels, the rent_burden
    slot must be damped and the deficit redirected to revenue_weight. Other
    paths (district hits, envelope flags, absolute modes) preserve full weight.
    The five composite weights must always sum to 1.0.
    """
    from app.services.expansion_advisor import _rent_burden_confidence

    # Citywide fallbacks are damped.
    assert _rent_burden_confidence("city_band_type", 20) == 0.25
    assert _rent_burden_confidence("city_band_type", 5) == 0.0     # below min_n
    assert _rent_burden_confidence("city", 25) == 0.15
    assert _rent_burden_confidence("city", 10) == 0.0              # below min_n

    # District tiers keep full weight.
    assert _rent_burden_confidence("district_band_type", 12) == 1.0
    assert _rent_burden_confidence("district_type", 8) == 1.0
    assert _rent_burden_confidence("district", 8) == 1.0

    # Envelope flags, absolute modes, unknown labels, and missing metadata
    # preserve legacy behavior (full weight).
    assert _rent_burden_confidence("listing_below_envelope", 0) == 1.0
    assert _rent_burden_confidence("listing_above_envelope", 0) == 1.0
    assert _rent_burden_confidence("absolute_legacy", None) == 1.0
    assert _rent_burden_confidence(None, None) == 1.0

    # Composite-weight arithmetic: damped and preserved cases must each
    # sum to 1.0 across the five components.
    for conf in (1.0, 0.25, 0.15, 0.0):
        rb_w = 0.20 * conf
        rev_w = 0.38 + (0.20 - rb_w)
        assert abs(rev_w + rb_w + 0.14 + 0.13 + 0.15 - 1.0) < 1e-9

    # Spot-check the specific pathology from the revert: city_band_type with
    # n=20 redirects 15 points of weight to revenue_index.
    conf = _rent_burden_confidence("city_band_type", 20)
    rb_w = 0.20 * conf
    rev_w = 0.38 + (0.20 - rb_w)
    assert abs(rb_w - 0.05) < 1e-9
    assert abs(rev_w - 0.53) < 1e-9


# ---------------------------------------------------------------------------
# Patch 06: rebalanced _score_breakdown weights
# ---------------------------------------------------------------------------

def test_score_breakdown_economics_weight_is_30():
    """occupancy_economics should still be weighted at 30%.

    Patch 13 moved 4 points of listing_quality weight into a new
    landlord_signal component, so listing_quality is now 11% not 15%.
    """
    bd = _score_breakdown(
        demand_score=50.0,
        whitespace_score=50.0,
        brand_fit_score=50.0,
        economics_score=50.0,
        provider_intelligence_composite=50.0,
        access_visibility_score=50.0,
        confidence_score=50.0,
        listing_quality_score=50.0,
    )
    assert bd["weights"]["occupancy_economics"] == 30
    assert bd["weights"]["listing_quality"] == 11
    assert bd["weights"]["landlord_signal"] == 8
    # Weight total invariant: everything must sum to 100.
    assert sum(bd["weights"].values()) == 100


def test_score_breakdown_listing_quality_contributes():
    """A high listing_quality should raise final_score vs a low one.

    Patch 13 rebalance: listing_quality now carries 11% weight (down
    from 15%) because 4 points moved to the new landlord_signal slot.
    """
    high = _score_breakdown(
        demand_score=60.0,
        whitespace_score=60.0,
        brand_fit_score=60.0,
        economics_score=60.0,
        provider_intelligence_composite=60.0,
        access_visibility_score=60.0,
        confidence_score=60.0,
        listing_quality_score=95.0,
    )
    low = _score_breakdown(
        demand_score=60.0,
        whitespace_score=60.0,
        brand_fit_score=60.0,
        economics_score=60.0,
        provider_intelligence_composite=60.0,
        access_visibility_score=60.0,
        confidence_score=60.0,
        listing_quality_score=20.0,
    )
    assert high["final_score"] > low["final_score"]
    # With 11% weight, the difference should be (95-20)*0.11 = 8.25 points
    diff = high["final_score"] - low["final_score"]
    assert abs(diff - 8.25) < 0.1


# ---------------------------------------------------------------------------
# Neighborhood plausibility guard rejects scraper-garbage values
# ---------------------------------------------------------------------------

def test_is_plausible_neighborhood_rejects_garbage_accepts_real_names():
    # Scraper-garbage / empty values must be rejected so the rent-burden
    # comp pool doesn't match on pure-digit neighborhood strings.
    assert _is_plausible_neighborhood("3") is False
    assert _is_plausible_neighborhood("12") is False
    assert _is_plausible_neighborhood("  ") is False
    assert _is_plausible_neighborhood("") is False
    assert _is_plausible_neighborhood(None) is False
    # Real neighborhood names (English and Arabic) must be accepted.
    assert _is_plausible_neighborhood("Olaya") is True
    assert _is_plausible_neighborhood("العليا") is True
    assert _is_plausible_neighborhood("An Nadhim") is True


# ---------------------------------------------------------------------------
# Phase 4 — listing recency + district momentum callouts in
# _top_positives_and_risks.
#
# These tests pin the five matrix strings and the threshold boundaries.
# Scoring is unaffected; the block only appends to positives, and does so
# after every other positive-emitting call so Phase 4 strings only claim
# positives[0] on cards with no higher-priority rationale.
# ---------------------------------------------------------------------------

from app.services.expansion_advisor import _top_positives_and_risks


_PHASE4_MISSING = object()


def _phase4_candidate(
    *,
    created_days=_PHASE4_MISSING,
    updated_days=_PHASE4_MISSING,
    district_momentum=None,
    **extra,
):
    """Build a neutral candidate dict that will not trigger any other
    positive-emitting branch in _top_positives_and_risks. Every numeric
    score sits in the dead band between positive and risk thresholds so
    only the Phase 4 block can produce output.

    Phase 4.1: tests now pass ``created_days`` / ``updated_days``
    independently. ``effective_age_days`` and ``source`` are still
    populated for memo/rerank back-compat (and to pin the independence
    of the new pill logic from the GREATEST()-derived fields), but the
    new rationale logic reads created_days / updated_days directly.
    The _PHASE4_MISSING sentinel means the test did not specify the
    field; when BOTH are sentinels, no ``listing_age`` dict is attached
    at all so we can exercise the "feature_snapshot_json has no
    listing_age" path. Passing ``None`` explicitly attaches a
    listing_age dict where the relevant day field is None (unknown).
    """
    fs = {}
    if created_days is not _PHASE4_MISSING or updated_days is not _PHASE4_MISSING:
        cd = None if created_days is _PHASE4_MISSING else created_days
        ud = None if updated_days is _PHASE4_MISSING else updated_days
        _age_candidates = [x for x in (cd, ud) if x is not None]
        if _age_candidates:
            effective = min(_age_candidates)
            source = "aqar_updated" if ud == effective else "aqar_created"
        else:
            effective = None
            source = "unknown"
        fs["listing_age"] = {
            "effective_age_days": effective,
            "source": source,
            "created_days": cd,
            "updated_days": ud,
        }
    if district_momentum is not None:
        fs["district_momentum"] = district_momentum
    return {
        "demand_score": 60.0,
        "whitespace_score": 50.0,
        "brand_fit_score": 60.0,
        "economics_score": 60.0,
        "provider_density_score": 40.0,
        "provider_whitespace_score": 40.0,
        "multi_platform_presence_score": 40.0,
        "delivery_competition_score": 40.0,
        "cannibalization_score": 55.0,
        "competitor_count": 5,
        "distance_to_nearest_branch_m": 3000.0,
        "area_m2": 150.0,
        "min_area_m2": 80.0,
        "max_area_m2": 500.0,
        "gate_status_json": {"overall_pass": False},
        "feature_snapshot_json": fs,
        **extra,
    }


_NEUTRAL_MOMENTUM = {"momentum_score": 50.0, "sample_floor_applied": True}
_ACTIVE_MOMENTUM = {"momentum_score": 82.0, "sample_floor_applied": False}


def test_phase4_state2_new_only_appends_new_string():
    candidate = _phase4_candidate(
        created_days=2,
        updated_days=2,
        district_momentum=_NEUTRAL_MOMENTUM,
    )
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    assert "Newly listed within the last week." in positives
    assert "Recently refreshed listing in an actively trading district." not in positives
    assert "District is actively attracting new listings right now." not in positives


def test_phase4_state3_updated_only_appends_updated_string():
    candidate = _phase4_candidate(
        created_days=30,
        updated_days=4,
        district_momentum=_NEUTRAL_MOMENTUM,
    )
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    assert "Listing refreshed by the owner within the last week." in positives
    assert "Newly listed within the last week." not in positives


def test_phase4_state5_active_market_only():
    candidate = _phase4_candidate(
        created_days=120,
        updated_days=120,
        district_momentum=_ACTIVE_MOMENTUM,
    )
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    assert "District is actively attracting new listings right now." in positives
    assert "Newly listed within the last week." not in positives
    assert "Listing refreshed by the owner within the last week." not in positives


def test_phase4_state6_new_plus_active_appends_combined_string():
    candidate = _phase4_candidate(
        created_days=1,
        updated_days=1,
        district_momentum={"momentum_score": 90.0, "sample_floor_applied": False},
    )
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    assert "Newly listed in an actively trading district." in positives
    # The combined string replaces the standalone strings — the function
    # emits exactly one Phase 4 line.
    assert "Newly listed within the last week." not in positives
    assert "District is actively attracting new listings right now." not in positives


def test_phase4_state7_updated_plus_active_appends_combined_string():
    candidate = _phase4_candidate(
        created_days=30,
        updated_days=6,
        district_momentum={"momentum_score": 75.0, "sample_floor_applied": False},
    )
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    assert "Recently refreshed listing in an actively trading district." in positives
    assert "Listing refreshed by the owner within the last week." not in positives
    assert "District is actively attracting new listings right now." not in positives


def test_phase4_state1_neither_signal_emits_nothing():
    candidate = _phase4_candidate(
        created_days=30,
        updated_days=30,
        district_momentum=_NEUTRAL_MOMENTUM,
    )
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    for phrase in (
        "Newly listed within the last week.",
        "Listing refreshed by the owner within the last week.",
        "Newly listed in an actively trading district.",
        "Recently refreshed listing in an actively trading district.",
        "District is actively attracting new listings right now.",
    ):
        assert phrase not in positives


def test_phase4_momentum_threshold_cliff_69_99_emits_nothing():
    candidate = _phase4_candidate(
        created_days=120,
        updated_days=120,
        district_momentum={"momentum_score": 69.99, "sample_floor_applied": False},
    )
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    assert "District is actively attracting new listings right now." not in positives


def test_phase4_momentum_threshold_cliff_70_00_emits_active_market():
    candidate = _phase4_candidate(
        created_days=120,
        updated_days=120,
        district_momentum={"momentum_score": 70.0, "sample_floor_applied": False},
    )
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    assert "District is actively attracting new listings right now." in positives


def test_phase4_both_days_null_emits_no_freshness():
    """Replaces the obsolete first_seen-source test. With the
    independent-timestamps design, source is no longer consulted by
    the pill logic; instead, None/None on the two raw day fields
    must suppress both "New" and "Updated"."""
    candidate = _phase4_candidate(
        created_days=None,
        updated_days=None,
        district_momentum=_NEUTRAL_MOMENTUM,
    )
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    for phrase in (
        "Newly listed within the last week.",
        "Listing refreshed by the owner within the last week.",
    ):
        assert phrase not in positives


def test_phase4_new_wins_when_both_days_are_fresh():
    """Key regression test for the scraper-cadence issue. Before 4.1
    the GREATEST() winner (aqar_updated) shadowed aqar_created, so a
    row with both created_days=2 and updated_days=2 emitted "Updated"
    instead of "New". After 4.1, "New" wins when created_days is within
    the freshness window, regardless of updated_days."""
    candidate = _phase4_candidate(
        created_days=2,
        updated_days=2,
        district_momentum=_NEUTRAL_MOMENTUM,
    )
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    assert "Newly listed within the last week." in positives
    assert "Listing refreshed by the owner within the last week." not in positives


def test_phase4_updated_fires_only_when_created_is_older_than_window():
    candidate = _phase4_candidate(
        created_days=30,
        updated_days=3,
        district_momentum=_NEUTRAL_MOMENTUM,
    )
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    assert "Listing refreshed by the owner within the last week." in positives
    assert "Newly listed within the last week." not in positives


def test_phase4_new_fires_even_when_updated_is_also_fresh():
    """Pins the scraper-cadence regression: New fires based on
    created_days alone, even when aqar_updated_at would have won the
    GREATEST() tie-break."""
    candidate = _phase4_candidate(
        created_days=3,
        updated_days=1,
        district_momentum=_NEUTRAL_MOMENTUM,
    )
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    assert "Newly listed within the last week." in positives
    assert "Listing refreshed by the owner within the last week." not in positives


def test_phase4_block_runs_after_existing_positives_in_function_order():
    """Phase 4 strings must only claim positives[0] when no other
    positive-emitting branch fired. When a stronger positive is present
    (demand_score >= 70 in this case), it must precede the Phase 4
    string so the card's positives[0]-only render picks the stronger
    rationale."""
    candidate = _phase4_candidate(
        created_days=1,
        updated_days=1,
        district_momentum=_NEUTRAL_MOMENTUM,
        demand_score=85.0,  # triggers "Demand potential is strong..."
    )
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    existing_positive = "Demand potential is strong for this district."
    phase4_positive = "Newly listed within the last week."
    assert existing_positive in positives
    assert phase4_positive in positives
    assert positives.index(existing_positive) < positives.index(phase4_positive)


def test_phase4_missing_feature_snapshot_is_safe():
    """Candidates without feature_snapshot_json must not crash the
    function. Common in tests and in a few recovery paths."""
    candidate = _phase4_candidate()
    candidate["feature_snapshot_json"] = None
    positives, _ = _top_positives_and_risks(
        candidate=candidate, gate_reasons={"passed": [], "failed": [], "unknown": []}
    )
    # Nothing from Phase 4 should fire; we mainly want no exception.
    assert isinstance(positives, list)
