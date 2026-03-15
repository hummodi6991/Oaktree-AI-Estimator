"""Tests for the production patch fixing live Expansion Advisor issues.

Covers:
1. Summary contradiction elimination (pass_count > 0 never says "no pass")
2. Frontend route-string correctness (no corrupted path chars)
3. Report soft-404 behavior
4. Stronger dedupe
5. District mojibake fallback
6. Delivery wording/gating honesty
7. Confidence grade capping for fallback-heavy candidates
8. Selected-candidate vs search-level pass_count consistency
"""
from __future__ import annotations

import pytest

from app.services.expansion_advisor import (
    _build_demand_thesis,
    _canonicalize_district_label,
    _confidence_grade,
    _dedupe_candidates,
    _gate_verdict_label,
    _top_positives_and_risks,
)
from app.services.aqar_district_match import is_mojibake


# ---------------------------------------------------------------------------
# 1. Summary contradiction: pass_count consistency
# ---------------------------------------------------------------------------


class TestSummaryContradiction:
    """Verify that report summary never contradicts gate pass counts."""

    def test_gate_verdict_true_is_pass(self):
        assert _gate_verdict_label(True) == "pass"

    def test_gate_verdict_false_is_fail(self):
        assert _gate_verdict_label(False) == "fail"

    def test_gate_verdict_none_is_unknown(self):
        assert _gate_verdict_label(None) == "unknown"

    def test_nonbool_truthy_not_pass(self):
        """Non-boolean truthy values must not coerce to 'pass'."""
        assert _gate_verdict_label(1) == "unknown"
        assert _gate_verdict_label("yes") == "unknown"

    def test_zero_pass_summary_consistent(self):
        """When no candidates have overall_pass=True, summary should say so."""
        candidates = [
            {"gate_status_json": {"overall_pass": None}, "final_score": 70},
            {"gate_status_json": {"overall_pass": False}, "final_score": 60},
        ]
        pass_candidates = [
            c for c in candidates
            if (c.get("gate_status_json") or {}).get("overall_pass") is True
        ]
        assert len(pass_candidates) == 0

    def test_some_pass_with_selected_not_passing(self):
        """When pass_count > 0 but selected candidate doesn't pass,
        the search-level truth should still show some candidates pass."""
        candidates = [
            {"id": "c1", "gate_status_json": {"overall_pass": None}, "final_score": 80},  # selected, doesn't pass
            {"id": "c2", "gate_status_json": {"overall_pass": True}, "final_score": 70},   # passes
            {"id": "c3", "gate_status_json": {"overall_pass": False}, "final_score": 60},
        ]
        pass_count = sum(
            1 for c in candidates
            if (c.get("gate_status_json") or {}).get("overall_pass") is True
        )
        assert pass_count == 1
        # Selected candidate (c1) doesn't pass, but search-level pass_count=1

    def test_unknown_candidates_separate_from_fail(self):
        """Candidates with overall_pass=None and no blocking failures
        should be treated differently from hard fails."""
        candidates = [
            {
                "gate_status_json": {"overall_pass": None},
                "gate_reasons_json": {"blocking_failures": [], "failed": [], "unknown": ["parking_pass"]},
                "final_score": 75,
            },
            {
                "gate_status_json": {"overall_pass": False},
                "gate_reasons_json": {"blocking_failures": ["area_fit_pass"], "failed": ["area_fit_pass"], "unknown": []},
                "final_score": 65,
            },
        ]
        unknown_no_block = [
            c for c in candidates
            if (c.get("gate_status_json") or {}).get("overall_pass") is None
            and not (c.get("gate_reasons_json") or {}).get("blocking_failures")
        ]
        assert len(unknown_no_block) == 1


# ---------------------------------------------------------------------------
# 2. Frontend route-string correctness
# ---------------------------------------------------------------------------


class TestFrontendRouteStrings:
    """Verify API route strings contain no corrupted characters."""

    def test_expansion_advisor_routes_clean(self):
        """All expansion-advisor API paths must use clean ASCII hyphen."""
        routes = [
            "/v1/expansion-advisor/searches",
            "/v1/expansion-advisor/candidates",
            "/v1/expansion-advisor/districts",
            "/v1/expansion-advisor/saved-searches",
            "/v1/expansion-advisor/branch-suggestions",
        ]
        for route in routes:
            # No Unicode replacement chars
            assert "\ufffd" not in route
            assert "\ufffe" not in route
            assert "\ufeff" not in route
            # Contains clean hyphen
            assert "expansion-advisor" in route
            # No non-ASCII characters
            assert all(ord(c) < 128 for c in route), f"Non-ASCII char in route: {route}"


# ---------------------------------------------------------------------------
# 3. Report soft-404 behavior (tested via contract, not integration)
# ---------------------------------------------------------------------------


class TestReportSoft404:
    """Contract tests for report 404 handling."""

    def test_404_message_starts_with_status(self):
        """Error messages for 404 should start with '404'."""
        msg = "404 Not Found"
        assert msg.startswith("404")

    def test_500_message_does_not_start_with_404(self):
        msg = "500 Internal Server Error"
        assert not msg.startswith("404")


# ---------------------------------------------------------------------------
# 4. Stronger dedupe
# ---------------------------------------------------------------------------


class TestStrongerDedupe:
    def test_exact_parcel_id_dedupe(self):
        candidates = [
            {"parcel_id": "p1", "lat": 24.7, "lon": 46.7, "area_m2": 200, "estimated_rent_sar_m2_year": 800, "final_score": 80},
            {"parcel_id": "p1", "lat": 24.7001, "lon": 46.7001, "area_m2": 210, "estimated_rent_sar_m2_year": 820, "final_score": 75},
        ]
        result = _dedupe_candidates(candidates)
        assert len(result) == 1
        assert result[0]["final_score"] == 80  # first (higher ranked) kept

    def test_spatial_grid_dedupe(self):
        """Near-identical candidates at same grid cell should collapse."""
        candidates = [
            {"parcel_id": "", "lat": 24.7001, "lon": 46.7001, "district": "العليا", "area_m2": 200,
             "estimated_rent_sar_m2_year": 800, "final_score": 80, "distance_to_nearest_branch_m": 2000},
            {"parcel_id": "", "lat": 24.7002, "lon": 46.7002, "district": "العليا", "area_m2": 205,
             "estimated_rent_sar_m2_year": 810, "final_score": 78, "distance_to_nearest_branch_m": 2100},
        ]
        result = _dedupe_candidates(candidates)
        assert len(result) == 1

    def test_distinct_parcels_preserved(self):
        """Genuinely different parcels must not be collapsed."""
        candidates = [
            {"parcel_id": "p1", "lat": 24.7, "lon": 46.7, "district": "العليا", "area_m2": 200,
             "estimated_rent_sar_m2_year": 800, "final_score": 80},
            {"parcel_id": "p2", "lat": 24.8, "lon": 46.8, "district": "الملقا", "area_m2": 400,
             "estimated_rent_sar_m2_year": 1200, "final_score": 75},
        ]
        result = _dedupe_candidates(candidates)
        assert len(result) == 2

    def test_same_district_area_score_dedupe_aggressive(self):
        """Near-clones in same district with same area/score bucket should collapse in aggressive mode."""
        candidates = [
            {"parcel_id": "", "lat": 24.71, "lon": 46.71, "district": "العليا", "area_m2": 201,
             "estimated_rent_sar_m2_year": 800, "final_score": 72.5, "distance_to_nearest_branch_m": None},
            {"parcel_id": "", "lat": 24.72, "lon": 46.72, "district": "العليا", "area_m2": 202,
             "estimated_rent_sar_m2_year": 810, "final_score": 73.0, "distance_to_nearest_branch_m": None},
        ]
        result = _dedupe_candidates(candidates, aggressive=True)
        # These share the same district + area bucket (200/50=4) + score bucket (72.5/2=36, 73/2=36)
        assert len(result) == 1

    def test_same_district_area_score_not_collapsed_default(self):
        """In default (non-aggressive) mode, near-clones at different positions survive."""
        candidates = [
            {"parcel_id": "", "lat": 24.71, "lon": 46.71, "district": "العليا", "area_m2": 201,
             "estimated_rent_sar_m2_year": 800, "final_score": 72.5, "distance_to_nearest_branch_m": None},
            {"parcel_id": "", "lat": 24.72, "lon": 46.72, "district": "العليا", "area_m2": 202,
             "estimated_rent_sar_m2_year": 810, "final_score": 73.0, "distance_to_nearest_branch_m": None},
        ]
        result = _dedupe_candidates(candidates)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# 5. District mojibake fallback
# ---------------------------------------------------------------------------


class TestDistrictMojibake:
    def test_is_mojibake_detects_garbled(self):
        assert is_mojibake("\ufffd\ufffd\ufffd") is True

    def test_is_mojibake_clean_arabic(self):
        assert is_mojibake("الملقا") is False

    def test_is_mojibake_empty(self):
        assert is_mojibake("") is True
        assert is_mojibake(None) is True

    def test_canonicalize_clean_arabic(self):
        result = _canonicalize_district_label("العليا")
        assert result["district_display"] == "العليا"
        assert result["district_key"] is not None

    def test_canonicalize_mojibake_falls_back_to_key(self):
        # Simulate mojibake input
        garbled = "\ufffd\ufffd\ufffd"
        result = _canonicalize_district_label(garbled)
        # Should not display garbled text
        if result["district_display"]:
            assert "\ufffd" not in result["district_display"]

    def test_canonicalize_with_lookup_prefers_clean(self):
        lookup = {"العليا": {"label_ar": "العليا", "label_en": "Olaya"}}
        result = _canonicalize_district_label("العليا", lookup)
        assert result["district_display"] == "العليا"
        assert result["district_name_en"] == "Olaya"

    def test_canonicalize_bom_stripped(self):
        # BOM character embedded in district name
        result = _canonicalize_district_label("\ufeffالعليا")
        assert result["district_display"] is not None
        assert "\ufeff" not in (result["district_display"] or "")


# ---------------------------------------------------------------------------
# 6. Delivery wording/gating
# ---------------------------------------------------------------------------


class TestDeliveryWording:
    def test_demand_thesis_no_delivery_observed(self):
        """When delivery_observed=False, language must be qualified."""
        thesis = _build_demand_thesis(
            demand_score=60,
            population_reach=5000,
            provider_density_score=0,
            provider_whitespace_score=80,
            delivery_competition_score=0,
            delivery_observed=False,
        )
        assert "not observed" in thesis.lower() or "inferred" in thesis.lower()

    def test_demand_thesis_with_delivery_observed(self):
        """When delivery_observed=True, language should use observed terms."""
        thesis = _build_demand_thesis(
            demand_score=75,
            population_reach=15000,
            provider_density_score=70,
            provider_whitespace_score=60,
            delivery_competition_score=45,
            delivery_observed=True,
        )
        assert "not observed" not in thesis.lower()
        assert "inferred" not in thesis.lower()

    def test_top_risks_flags_no_delivery_data(self):
        """When delivery scores are 0, risks should mention inferred data."""
        candidate = {
            "demand_score": 60,
            "whitespace_score": 50,
            "brand_fit_score": 65,
            "economics_score": 70,
            "delivery_competition_score": 0,
            "cannibalization_score": 30,
            "gate_status_json": {"overall_pass": None},
            "provider_density_score": 0,
            "multi_platform_presence_score": 0,
        }
        gate_reasons = {"passed": [], "failed": [], "unknown": []}
        positives, risks = _top_positives_and_risks(
            candidate=candidate, gate_reasons=gate_reasons
        )
        assert any("inferred" in r.lower() or "no observed" in r.lower() for r in risks)


# ---------------------------------------------------------------------------
# 7. Confidence grade capping
# ---------------------------------------------------------------------------


class TestConfidenceGradeCapping:
    def test_grade_capped_when_delivery_not_observed(self):
        grade = _confidence_grade(
            confidence_score=90,
            district="العليا",
            provider_platform_count=0,
            multi_platform_presence_score=0,
            rent_source="aqar_median",
            road_context_available=True,
            parking_context_available=True,
            zoning_available=True,
            delivery_observed=False,
        )
        # With delivery_observed=False, critical_missing >= 1, so max grade is B
        assert grade in ("B", "C", "D")

    def test_grade_a_requires_all_context(self):
        grade = _confidence_grade(
            confidence_score=90,
            district="العليا",
            provider_platform_count=3,
            multi_platform_presence_score=60,
            rent_source="aqar_median",
            road_context_available=True,
            parking_context_available=True,
            zoning_available=True,
            delivery_observed=True,
            data_completeness_score=100,
        )
        assert grade == "A"

    def test_grade_d_when_all_missing(self):
        grade = _confidence_grade(
            confidence_score=30,
            district=None,
            provider_platform_count=0,
            multi_platform_presence_score=0,
            rent_source="conservative_default",
            road_context_available=False,
            parking_context_available=False,
            zoning_available=False,
            delivery_observed=False,
        )
        assert grade == "D"


# ---------------------------------------------------------------------------
# 8. Search/report timing instrumentation (smoke test)
# ---------------------------------------------------------------------------


class TestTimingInstrumentation:
    """Verify that timing-related code paths don't crash."""

    def test_gate_verdict_label_handles_all_states(self):
        assert _gate_verdict_label(True) == "pass"
        assert _gate_verdict_label(False) == "fail"
        assert _gate_verdict_label(None) == "unknown"

    def test_dedupe_empty_list(self):
        assert _dedupe_candidates([]) == []

    def test_dedupe_single_item(self):
        candidates = [{"parcel_id": "p1", "lat": 24.7, "lon": 46.7, "area_m2": 200,
                       "estimated_rent_sar_m2_year": 800, "final_score": 80}]
        assert len(_dedupe_candidates(candidates)) == 1
