from app.services.expansion_advisor import (
    _candidate_gate_status,
    _context_checked,
    _nonnegative_int,
    _parking_evidence_band,
    _road_evidence_band,
)


def test_context_checked_none_is_unavailable():
    assert _context_checked(None) is False


def test_context_checked_zero_is_available():
    assert _context_checked(0) is True


def test_context_checked_positive_is_available():
    assert _context_checked(5) is True


def test_nonnegative_int_clamps_negative():
    assert _nonnegative_int(-3) == 0


def test_nonnegative_int_passes_positive():
    assert _nonnegative_int(4) == 4


def test_nonnegative_int_none_returns_zero():
    assert _nonnegative_int(None) == 0


def test_parking_evidence_band_none_is_unknown():
    assert _parking_evidence_band(None) == "unknown"


def test_parking_evidence_band_zero_is_none_found():
    assert _parking_evidence_band(0) == "none_found"


def test_parking_evidence_band_limited():
    assert _parking_evidence_band(1) == "limited"
    assert _parking_evidence_band(2) == "limited"


def test_parking_evidence_band_moderate():
    assert _parking_evidence_band(3) == "moderate"
    assert _parking_evidence_band(5) == "moderate"


def test_parking_evidence_band_strong():
    assert _parking_evidence_band(6) == "strong"


def test_road_evidence_band_both_none_is_unknown():
    assert _road_evidence_band(None, None) == "unknown"


def test_road_evidence_band_touches_road():
    assert _road_evidence_band(0, True) == "direct_frontage"


def test_road_evidence_band_no_roads():
    assert _road_evidence_band(0, False) == "none_found"


def test_road_evidence_band_limited():
    assert _road_evidence_band(2, False) == "limited"


def test_road_evidence_band_moderate():
    assert _road_evidence_band(4, False) == "moderate"


def test_road_evidence_band_strong():
    assert _road_evidence_band(6, False) == "strong"


def test_candidate_gate_status_exposes_advisory_failures_without_blocking():
    gate_status, reasons = _candidate_gate_status(
        fit_score=78.0,
        area_fit_score=82.0,
        zoning_fit_score=88.0,
        landuse_available=True,
        frontage_score=40.0,  # advisory fail
        access_score=44.0,    # advisory fail
        parking_score=44.0,   # advisory fail
        district="Al Olaya",
        distance_to_nearest_branch_m=3200.0,
        provider_density_score=52.0,
        multi_platform_presence_score=15.0,
        economics_score=58.0,
        payback_band="healthy",
        brand_profile={"primary_channel": "delivery"},
        road_context_available=True,
        parking_context_available=True,
    )

    assert gate_status["overall_pass"] is True
    assert reasons["blocking_failures"] == []
    assert "advisory_failures" in reasons
    assert "frontage_access_pass" in reasons["advisory_failures"]
    assert "parking_pass" in reasons["advisory_failures"]


def test_smoke():
    assert True
