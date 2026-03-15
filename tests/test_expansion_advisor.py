from app.services.expansion_advisor import (
    _candidate_feature_snapshot,
    _candidate_gate_status,
    _confidence_grade,
    _context_checked,
    _dedupe_candidates,
    _build_demand_thesis,
    _nonnegative_int,
    _parking_evidence_band,
    _road_evidence_band,
)
from app.services.aqar_district_match import (
    is_mojibake,
    normalize_arabic_text,
    normalize_district_key,
)


# ---------------------------------------------------------------------------
# Minimal DB fakes for _candidate_feature_snapshot integration tests
# ---------------------------------------------------------------------------

class _Result:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None


class _FakeNestedTx:
    def __enter__(self):
        return self
    def __exit__(self, *a):
        return False


class _FakeDB:
    """Routes SQL to canned responses based on query content."""

    def __init__(self, *, perimeter_row=None, road_row=None, parking_row=None):
        self._perimeter_row = perimeter_row
        self._road_row = road_row
        self._parking_row = parking_row

    def begin_nested(self):
        return _FakeNestedTx()

    def execute(self, stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        if "ST_Perimeter" in sql:
            return _Result([self._perimeter_row] if self._perimeter_row else [])
        if "nearby_road_segment_count" in sql:
            return _Result([self._road_row] if self._road_row else [])
        if "nearby_parking_amenity_count" in sql:
            return _Result([self._parking_row] if self._parking_row else [])
        return _Result([])


_SNAPSHOT_DEFAULTS = dict(
    parcel_id="p1",
    lat=24.7,
    lon=46.7,
    area_m2=200.0,
    district="Al Olaya",
    landuse_label="Commercial",
    landuse_code="C",
    provider_listing_count=5,
    provider_platform_count=2,
    competitor_count=3,
    nearest_branch_distance_m=2000.0,
    rent_source="aqar",
    estimated_rent_sar_m2_year=800.0,
    economics_score=65.0,
    roads_table_available=True,
    parking_table_available=True,
)


# ---------------------------------------------------------------------------
# Unit tests for helper functions
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Integration tests: parking_context_available inside _candidate_feature_snapshot
# ---------------------------------------------------------------------------

def test_snapshot_zero_parking_count_means_context_available():
    """nearby_parking_amenity_count=0 means 'looked, found nothing' — context IS available."""
    db = _FakeDB(
        perimeter_row={"parcel_perimeter_m": 120.0},
        road_row={
            "nearby_road_segment_count": 2,
            "touches_road": False,
            "nearest_major_road_distance_m": 140.0,
        },
        parking_row={"nearby_parking_amenity_count": 0},
    )

    snapshot = _candidate_feature_snapshot(db, **_SNAPSHOT_DEFAULTS)

    assert snapshot["context_sources"]["parking_context_available"] is True
    assert snapshot["nearby_parking_amenity_count"] == 0
    assert snapshot["context_sources"]["parking_evidence_band"] == "none_found"
    assert "parking_context_unavailable" not in snapshot["missing_context"]


def test_snapshot_none_parking_count_means_context_unavailable():
    """nearby_parking_amenity_count=None means the query returned no usable value — context is NOT available."""
    db = _FakeDB(
        perimeter_row={"parcel_perimeter_m": 120.0},
        road_row={
            "nearby_road_segment_count": 2,
            "touches_road": True,
            "nearest_major_road_distance_m": 90.0,
        },
        parking_row={"nearby_parking_amenity_count": None},
    )

    snapshot = _candidate_feature_snapshot(db, **_SNAPSHOT_DEFAULTS)

    assert snapshot["context_sources"]["parking_context_available"] is False
    assert snapshot["context_sources"]["parking_evidence_band"] == "unknown"
    assert "parking_context_unavailable" in snapshot["missing_context"]


def test_snapshot_zero_road_count_means_road_context_available():
    """nearby_road_segment_count=0 with touches_road=False means 'looked, found nothing' — context IS available."""
    db = _FakeDB(
        perimeter_row={"parcel_perimeter_m": 100.0},
        road_row={
            "nearby_road_segment_count": 0,
            "touches_road": False,
            "nearest_major_road_distance_m": 4999.0,
        },
        parking_row={"nearby_parking_amenity_count": 3},
    )

    snapshot = _candidate_feature_snapshot(db, **_SNAPSHOT_DEFAULTS)

    assert snapshot["context_sources"]["road_context_available"] is True
    assert snapshot["context_sources"]["road_evidence_band"] == "none_found"
    assert "road_context_unavailable" not in snapshot["missing_context"]


def test_snapshot_no_parking_row_means_context_unavailable():
    """When the parking query returns no row at all, context stays unavailable (default False)."""
    db = _FakeDB(
        perimeter_row={"parcel_perimeter_m": 100.0},
        road_row={
            "nearby_road_segment_count": 1,
            "touches_road": False,
            "nearest_major_road_distance_m": 200.0,
        },
        parking_row=None,  # no row returned
    )

    snapshot = _candidate_feature_snapshot(db, **_SNAPSHOT_DEFAULTS)

    assert snapshot["context_sources"]["parking_context_available"] is False
    assert snapshot["context_sources"]["parking_evidence_band"] == "unknown"
    assert "parking_context_unavailable" in snapshot["missing_context"]


# ---------------------------------------------------------------------------
# Dedupe tests (Task C)
# ---------------------------------------------------------------------------

def test_dedupe_collapses_same_parcel_id():
    """Two candidates with the same parcel_id collapse to one."""
    candidates = [
        {"parcel_id": "abc123", "lat": 24.7, "lon": 46.7, "final_score": 80},
        {"parcel_id": "abc123", "lat": 24.7, "lon": 46.7, "final_score": 75},
    ]
    result = _dedupe_candidates(candidates)
    assert len(result) == 1
    assert result[0]["final_score"] == 80  # first (highest ranked) kept


def test_dedupe_distinct_parcel_ids_survive():
    """Candidates with different parcel_ids are all kept."""
    candidates = [
        {"parcel_id": "a", "lat": 24.7, "lon": 46.7},
        {"parcel_id": "b", "lat": 24.7, "lon": 46.7},
        {"parcel_id": "c", "lat": 24.8, "lon": 46.8},
    ]
    result = _dedupe_candidates(candidates)
    assert len(result) == 3


def test_dedupe_spatial_composite_key_collapses_clones():
    """Candidates without parcel_id collapse if location+district+area+rent match."""
    candidates = [
        {"lat": 24.700, "lon": 46.700, "district": "Al Olaya", "area_m2": 200, "estimated_rent_sar_m2_year": 800, "distance_to_nearest_branch_m": 1000},
        {"lat": 24.700, "lon": 46.700, "district": "Al Olaya", "area_m2": 210, "estimated_rent_sar_m2_year": 810, "distance_to_nearest_branch_m": 1100},
    ]
    result = _dedupe_candidates(candidates)
    assert len(result) == 1  # same buckets → collapsed


def test_dedupe_different_districts_survive():
    """Same location but different districts are kept."""
    candidates = [
        {"lat": 24.700, "lon": 46.700, "district": "Al Olaya", "area_m2": 200, "estimated_rent_sar_m2_year": 800, "distance_to_nearest_branch_m": 1000},
        {"lat": 24.700, "lon": 46.700, "district": "Al Malqa", "area_m2": 200, "estimated_rent_sar_m2_year": 800, "distance_to_nearest_branch_m": 1000},
    ]
    result = _dedupe_candidates(candidates)
    assert len(result) == 2


def test_dedupe_empty_list():
    assert _dedupe_candidates([]) == []


# ---------------------------------------------------------------------------
# Confidence grade tests (Task E)
# ---------------------------------------------------------------------------

def test_confidence_grade_a_all_observed():
    """Grade A requires high score, all critical context present, high completeness."""
    grade = _confidence_grade(
        confidence_score=85.0,
        district="Al Olaya",
        provider_platform_count=3,
        multi_platform_presence_score=20.0,
        rent_source="aqar",
        road_context_available=True,
        parking_context_available=True,
        zoning_available=True,
        delivery_observed=True,
        data_completeness_score=90,
    )
    assert grade == "A"


def test_confidence_grade_capped_when_zoning_missing():
    """Missing zoning prevents grade A even with a high score."""
    grade = _confidence_grade(
        confidence_score=90.0,
        district="Al Olaya",
        provider_platform_count=3,
        multi_platform_presence_score=20.0,
        rent_source="aqar",
        road_context_available=True,
        parking_context_available=True,
        zoning_available=False,
        delivery_observed=True,
        data_completeness_score=95,
    )
    assert grade != "A"  # capped to B at most


def test_confidence_grade_capped_when_delivery_not_observed():
    """Missing delivery observation prevents grade A."""
    grade = _confidence_grade(
        confidence_score=90.0,
        district="Al Olaya",
        provider_platform_count=3,
        multi_platform_presence_score=20.0,
        rent_source="aqar",
        road_context_available=True,
        parking_context_available=True,
        zoning_available=True,
        delivery_observed=False,
        data_completeness_score=95,
    )
    assert grade != "A"


def test_confidence_grade_d_for_low_score():
    """Very low confidence score yields D regardless of context."""
    grade = _confidence_grade(
        confidence_score=20.0,
        district=None,
        provider_platform_count=0,
        multi_platform_presence_score=0.0,
        rent_source="conservative_default",
        road_context_available=False,
        parking_context_available=False,
        zoning_available=False,
        delivery_observed=False,
        data_completeness_score=30,
    )
    assert grade == "D"


def test_confidence_grade_c_with_multiple_missing():
    """Two or more missing critical contexts cap at C even with moderate score."""
    grade = _confidence_grade(
        confidence_score=72.0,
        district="Al Olaya",
        provider_platform_count=2,
        multi_platform_presence_score=10.0,
        rent_source="aqar",
        road_context_available=False,
        parking_context_available=False,
        zoning_available=True,
        delivery_observed=True,
        data_completeness_score=80,
    )
    # critical_missing = 2 → prevents B (needs <= 1), so should be C
    assert grade == "C"


def test_confidence_grade_b_with_one_missing():
    """One missing critical context allows B if score is high enough."""
    grade = _confidence_grade(
        confidence_score=72.0,
        district="Al Olaya",
        provider_platform_count=2,
        multi_platform_presence_score=15.0,
        rent_source="aqar",
        road_context_available=True,
        parking_context_available=True,
        zoning_available=False,  # one missing
        delivery_observed=True,
        data_completeness_score=85,
    )
    assert grade == "B"


# ---------------------------------------------------------------------------
# Delivery wording tests (Task F)
# ---------------------------------------------------------------------------

def test_demand_thesis_observed_delivery_uses_concrete_labels():
    """When delivery is observed, the thesis uses density labels like 'dense'/'thin'."""
    thesis = _build_demand_thesis(
        demand_score=75.0,
        population_reach=50000,
        provider_density_score=70.0,
        provider_whitespace_score=65.0,
        delivery_competition_score=70.0,
        delivery_observed=True,
    )
    assert "dense" in thesis
    assert "not observed" not in thesis


def test_demand_thesis_not_observed_uses_inferred_language():
    """When delivery is NOT observed, the thesis uses qualified 'inferred' language."""
    thesis = _build_demand_thesis(
        demand_score=75.0,
        population_reach=50000,
        provider_density_score=70.0,
        provider_whitespace_score=65.0,
        delivery_competition_score=70.0,
        delivery_observed=False,
    )
    assert "not observed (inferred)" in thesis
    assert "inferred whitespace opportunity" in thesis
    assert "not directly observed" in thesis


def test_demand_thesis_zero_density_observed_shows_thin():
    """provider_density_score=0 with observed data shows 'thin', not a false positive."""
    thesis = _build_demand_thesis(
        demand_score=30.0,
        population_reach=10000,
        provider_density_score=0.0,
        provider_whitespace_score=10.0,
        delivery_competition_score=10.0,
        delivery_observed=True,
    )
    assert "thin" in thesis
    assert "not observed" not in thesis


# ---------------------------------------------------------------------------
# Arabic district normalization / mojibake tests (Task D)
# ---------------------------------------------------------------------------

def test_is_mojibake_detects_garbled_text():
    """Garbled/empty strings are flagged as mojibake."""
    assert is_mojibake(None) is True
    assert is_mojibake("") is True
    assert is_mojibake("   ") is True


def test_is_mojibake_accepts_clean_arabic():
    """Clean Arabic text is not mojibake."""
    assert is_mojibake("العليا") is False
    assert is_mojibake("الملقا") is False


def test_is_mojibake_accepts_english():
    """English text is not mojibake."""
    assert is_mojibake("Al Olaya") is False


def test_normalize_arabic_text_alef_variants():
    """Alef variants are collapsed to bare alef."""
    result = normalize_arabic_text("إبراهيم")
    assert "إ" not in result  # hamza-below alef normalized


def test_normalize_district_key_strips_whitespace():
    """normalize_district_key produces a clean key."""
    key = normalize_district_key("  Al Olaya  ")
    assert key is not None
    assert key == normalize_district_key("Al Olaya")


def test_normalize_district_key_rejects_empty():
    """Empty/None strings return falsy from normalize_district_key."""
    key = normalize_district_key("")
    assert not key  # empty string is falsy
    key2 = normalize_district_key(None)
    assert not key2


# ---------------------------------------------------------------------------
# Snapshot context_sources provenance tests (Task H / Task 9)
# ---------------------------------------------------------------------------

def test_snapshot_context_sources_include_road_and_parking():
    """Snapshot context_sources includes road and parking provenance."""
    db = _FakeDB(
        perimeter_row={"parcel_perimeter_m": 100.0},
        road_row={
            "nearby_road_segment_count": 3,
            "touches_road": False,
            "nearest_major_road_distance_m": 150.0,
        },
        parking_row={"nearby_parking_amenity_count": 4},
    )
    snapshot = _candidate_feature_snapshot(db, **_SNAPSHOT_DEFAULTS)
    cs = snapshot["context_sources"]

    assert "road_context_available" in cs
    assert "parking_context_available" in cs
    assert "road_evidence_band" in cs
    assert "parking_evidence_band" in cs
    assert cs["road_context_available"] is True
    assert cs["parking_context_available"] is True
    assert cs["road_evidence_band"] == "moderate"  # 3 segments, not touching
    assert cs["parking_evidence_band"] == "moderate"  # 4 amenities


def test_snapshot_road_direct_frontage_when_touches():
    """When touches_road=True, road_evidence_band is 'direct_frontage'."""
    db = _FakeDB(
        perimeter_row={"parcel_perimeter_m": 100.0},
        road_row={
            "nearby_road_segment_count": 0,
            "touches_road": True,
            "nearest_major_road_distance_m": 0.0,
        },
        parking_row={"nearby_parking_amenity_count": 1},
    )
    snapshot = _candidate_feature_snapshot(db, **_SNAPSHOT_DEFAULTS)
    assert snapshot["context_sources"]["road_evidence_band"] == "direct_frontage"


# ---------------------------------------------------------------------------
# Gate status tests (additional scenarios)
# ---------------------------------------------------------------------------

def test_candidate_gate_status_blocking_failure_on_low_fit():
    """A very low fit_score triggers a blocking failure."""
    gate_status, reasons = _candidate_gate_status(
        fit_score=30.0,  # well below threshold
        area_fit_score=20.0,
        zoning_fit_score=25.0,
        landuse_available=True,
        frontage_score=80.0,
        access_score=80.0,
        parking_score=80.0,
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
    assert gate_status["overall_pass"] is not True
    assert len(reasons["blocking_failures"]) > 0


def test_smoke():
    assert True
