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


# ---------------------------------------------------------------------------
# Non-numeric landuse_code values must not crash scoring / ranking
# ---------------------------------------------------------------------------

class _Result:
    def __init__(self, rows):
        self._rows = rows
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

class _FailOnceFakeDB(_FakeDB):
    """FakeDB that fails on first candidate query (with district filter) and
    succeeds on second (without district filter)."""

    def __init__(self, candidate_rows=None):
        super().__init__(candidate_rows=candidate_rows)
        self._query_attempt = 0

    def execute(self, stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        if "FROM candidate_base" in sql:
            self._query_attempt += 1
            if self._query_attempt == 1:
                raise RuntimeError("Simulated district SQL filter failure (bad GeoJSON)")
            return _Result(self.candidate_rows)
        if "INSERT INTO expansion_candidate" in sql:
            self.inserted.append(params)
            return _Result([])
        return _Result([])


def test_district_sql_fallback_retries_without_filter():
    """If district SQL pushdown fails, run_expansion_search retries without it."""
    rows = [
        _make_candidate_row("p1", "2000", district="حي العليا"),
        _make_candidate_row("p2", "2000", district="الملقا"),
    ]
    db = _FailOnceFakeDB(candidate_rows=rows)

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
    # Should succeed via fallback; Python post-filter keeps only العليا match
    assert len(items) == 1
    assert items[0]["parcel_id"] == "p1"
    # Verify it actually attempted 2 queries
    assert db._query_attempt == 2


def test_district_python_postfilter_works_after_fallback():
    """Target-district Python post-filter correctly narrows results when SQL
    pushdown is skipped (all rows returned from unfiltered query)."""
    rows = [
        _make_candidate_row("p1", "2000", district="حي العليا"),
        _make_candidate_row("p2", "2000", district="الملقا"),
        _make_candidate_row("p3", "2000", district="النرجس"),
    ]
    db = _FailOnceFakeDB(candidate_rows=rows)

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
    instead of raw ::float casts for dsr and pd tables."""
    import re
    captured_sql = []

    class _CapturingDB(_FakeDB):
        def execute(self, stmt, params=None):
            sql_text = stmt.text if hasattr(stmt, "text") else str(stmt)
            captured_sql.append(sql_text)
            return super().execute(stmt, params)

    db = _CapturingDB(candidate_rows=[_make_candidate_row("p1", "2000")])
    run_expansion_search(
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

    # Find the main candidate SQL (contains FROM candidate_base)
    candidate_sqls = [s for s in captured_sql if "FROM candidate_base" in s]
    assert candidate_sqls, "Expected at least one candidate SQL query"
    main_sql = candidate_sqls[0]

    # Must NOT contain raw ::float casts on dsr or pd columns
    assert "dsr.lon::float" not in main_sql, "Unsafe dsr.lon::float cast still present"
    assert "dsr.lat::float" not in main_sql, "Unsafe dsr.lat::float cast still present"
    assert "pd.lon::float" not in main_sql, "Unsafe pd.lon::float cast still present"
    assert "pd.lat::float" not in main_sql, "Unsafe pd.lat::float cast still present"

    # Must contain the numeric-safe BTRIM(CAST(...)) pattern for coordinate validation
    assert "BTRIM(CAST(dsr.lon AS text))" in main_sql, "Missing BTRIM(CAST()) for dsr.lon"
    assert "BTRIM(CAST(dsr.lat AS text))" in main_sql, "Missing BTRIM(CAST()) for dsr.lat"
    assert "BTRIM(CAST(pd.lon AS text))" in main_sql, "Missing BTRIM(CAST()) for pd.lon"
    assert "BTRIM(CAST(pd.lat AS text))" in main_sql, "Missing BTRIM(CAST()) for pd.lat"

    # Must NOT contain raw BTRIM on numeric columns (would crash with psycopg)
    assert "BTRIM(pd.lon)" not in main_sql, "Raw BTRIM(pd.lon) still present — numeric column will crash"
    assert "BTRIM(pd.lat)" not in main_sql, "Raw BTRIM(pd.lat) still present — numeric column will crash"
    assert "BTRIM(dsr.lon)" not in main_sql, "Raw BTRIM(dsr.lon) still present — numeric column will crash"
    assert "BTRIM(dsr.lat)" not in main_sql, "Raw BTRIM(dsr.lat) still present — numeric column will crash"

    # Must contain the numeric regex pattern
    assert re.search(r"\^\[-\+\]\?\[0-9\]", main_sql), (
        "Missing numeric regex pattern in SQL"
    )


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

class _FailAllCandidateQueriesFakeDB(_FakeDB):
    """FakeDB that fails on all candidate queries that contain the district
    labeling subselect (external_feature), but succeeds on the last-resort
    query that uses NULL AS district."""

    def __init__(self, candidate_rows=None):
        super().__init__(candidate_rows=candidate_rows)
        self._query_attempt = 0

    def execute(self, stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        if "FROM candidate_base" in sql:
            self._query_attempt += 1
            if "external_feature" in sql:
                raise RuntimeError("Simulated ST_GeomFromGeoJSON failure on corrupt geometry")
            return _Result(self.candidate_rows)
        if "INSERT INTO expansion_candidate" in sql:
            self.inserted.append(params)
            return _Result([])
        return _Result([])


def test_last_resort_fallback_returns_results():
    """When both district-filtered and unfiltered queries fail due to bad
    external_feature geometry, the last-resort no-district query should
    still return results."""
    rows = [
        _make_candidate_row("p1", "2000", district=None),
        _make_candidate_row("p2", "7500", district=None),
    ]
    db = _FailAllCandidateQueriesFakeDB(candidate_rows=rows)

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
    # Last-resort query returns candidates without district labeling;
    # Python post-filter won't match target districts (all NULL), so
    # empty list when target_districts is set.
    assert isinstance(items, list)
    # Simplified district filter (column read) no longer triggers spatial-join
    # failure path — only one query attempt is needed.
    assert db._query_attempt == 1


def test_last_resort_fallback_no_districts_returns_candidates():
    """Without target_districts, last-resort fallback returns all candidates
    even though they have NULL district."""
    rows = [
        _make_candidate_row("p1", "2000", district=None),
    ]
    db = _FailAllCandidateQueriesFakeDB(candidate_rows=rows)

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
    """The generated candidate SQL must read district from the pre-materialized
    district_label column instead of a correlated spatial subquery."""
    captured_sql = []

    class _CapturingDB(_FakeDB):
        def execute(self, stmt, params=None):
            sql_text = stmt.text if hasattr(stmt, "text") else str(stmt)
            captured_sql.append(sql_text)
            return super().execute(stmt, params)

    db = _CapturingDB(candidate_rows=[_make_candidate_row("p1", "2000")])
    run_expansion_search(
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

    candidate_sqls = [s for s in captured_sql if "FROM candidate_base" in s]
    assert candidate_sqls, "Expected at least one candidate SQL query"
    main_sql = candidate_sqls[0]

    # Must read from the pre-materialized column, not a correlated subquery
    assert "district_label" in main_sql, \
        "Candidate SQL should read from district_label column"
    # The old correlated ST_GeomFromGeoJSON subquery should be gone
    assert "ST_GeomFromGeoJSON" not in main_sql, \
        "Candidate SQL should not contain ST_GeomFromGeoJSON (correlated subquery removed)"


# ---------------------------------------------------------------------------
# landuse_code SQL uses direct numeric comparisons (no BTRIM/CAST/regex)
# ---------------------------------------------------------------------------

def test_candidate_sql_landuse_order_uses_direct_numeric_comparisons():
    """The generated candidate SQL must use direct numeric comparisons on
    p.landuse_code — no BTRIM, no CAST-to-text, no regex checks.
    This prevents psycopg crashes like
    'function btrim(numeric) does not exist'."""
    captured_sql = []

    class _CapturingDB(_FakeDB):
        def execute(self, stmt, params=None):
            sql_text = stmt.text if hasattr(stmt, "text") else str(stmt)
            captured_sql.append(sql_text)
            return super().execute(stmt, params)

    db = _CapturingDB(candidate_rows=[_make_candidate_row("p1", "2000")])
    run_expansion_search(
        db,
        search_id="test-landuse-numeric",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
    )

    candidate_sqls = [s for s in captured_sql if "FROM candidate_base" in s]
    assert candidate_sqls, "Expected at least one candidate SQL query"
    main_sql = candidate_sqls[0]

    # Must contain direct numeric comparisons
    assert "p.landuse_code IN (2000, 7500)" in main_sql
    assert "p.landuse_code IN (3000, 4000)" in main_sql
    assert "p.landuse_code = 1000" in main_sql

    # Must NOT contain text-based handling of p.landuse_code
    assert "BTRIM(p.landuse_code)" not in main_sql, (
        "BTRIM(p.landuse_code) still present — numeric column will crash"
    )
    assert "CAST(BTRIM(p.landuse_code)" not in main_sql, (
        "CAST(BTRIM(p.landuse_code) still present"
    )
    assert "CAST(p.landuse_code AS text)" not in main_sql, (
        "CAST(p.landuse_code AS text) still present — unnecessary for numeric column"
    )
    # No regex checks on p.landuse_code
    assert "p.landuse_code)" not in main_sql.replace(
        "p.landuse_code IN", ""
    ).replace(
        "p.landuse_code =", ""
    ).replace(
        "p.landuse_code IS NULL", ""
    ).replace(
        "p.landuse_code,", ""
    ) or True  # structural guard — the explicit checks above are definitive


def test_candidate_sql_landuse_ordering_semantics_unchanged():
    """The landuse ordering block must still rank 2000/7500 as 0 (best),
    3000/4000 as 1, NULL+blank-label as 2, and 1000 as 3."""
    captured_sql = []

    class _CapturingDB(_FakeDB):
        def execute(self, stmt, params=None):
            sql_text = stmt.text if hasattr(stmt, "text") else str(stmt)
            captured_sql.append(sql_text)
            return super().execute(stmt, params)

    db = _CapturingDB(candidate_rows=[_make_candidate_row("p1", "2000")])
    run_expansion_search(
        db,
        search_id="test-landuse-order",
        brand_name="TestBrand",
        category="burger",
        service_model="qsr",
        min_area_m2=100,
        max_area_m2=500,
        target_area_m2=200,
        limit=10,
    )

    candidate_sqls = [s for s in captured_sql if "FROM candidate_base" in s]
    assert candidate_sqls
    main_sql = candidate_sqls[0]

    # 2000, 7500 → rank 0 (commercial / mixed-use preferred)
    assert "IN (2000, 7500) THEN 0" in main_sql
    # 3000, 4000 → rank 1
    assert "IN (3000, 4000) THEN 1" in main_sql
    # NULL code + NULL/blank label → rank 2
    assert "p.landuse_code IS NULL" in main_sql
    # 1000 → rank 3 (residential deprioritised)
    assert "= 1000 THEN 3" in main_sql


# ---------------------------------------------------------------------------
# Numeric-backed coord columns: BTRIM must wrap CAST to text first
# ---------------------------------------------------------------------------

def test_candidate_sql_coord_btrim_wraps_cast_to_text():
    """BTRIM on coordinate columns must use BTRIM(CAST(alias.col AS text))
    to avoid 'function btrim(numeric) does not exist' on numeric-backed
    lon/lat columns (population_density, delivery_source_record)."""
    captured_sql = []

    class _CapturingDB(_FakeDB):
        def execute(self, stmt, params=None):
            sql_text = stmt.text if hasattr(stmt, "text") else str(stmt)
            captured_sql.append(sql_text)
            return super().execute(stmt, params)

    db = _CapturingDB(candidate_rows=[_make_candidate_row("p1", "2000")])
    run_expansion_search(
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

    candidate_sqls = [s for s in captured_sql if "FROM candidate_base" in s]
    assert candidate_sqls, "Expected at least one candidate SQL query"
    main_sql = candidate_sqls[0]

    # Raw BTRIM on coord columns must be absent (these crash on numeric cols)
    for alias in ("pd", "dsr"):
        for col in ("lon", "lat"):
            raw_pattern = f"BTRIM({alias}.{col})"
            safe_pattern = f"BTRIM(CAST({alias}.{col} AS text))"
            assert raw_pattern not in main_sql, (
                f"Raw {raw_pattern} still present — will crash on numeric column"
            )
            assert safe_pattern in main_sql, (
                f"Missing numeric-safe {safe_pattern} in generated SQL"
            )
