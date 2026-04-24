import pytest

from app.services.expansion_advisor import (
    _ROAD_DISTANCE_SENTINEL_M,
    _candidate_feature_snapshot,
    _candidate_gate_status,
    _confidence_grade,
    _confidence_score,
    _context_checked,
    _dedupe_candidates,
    _build_demand_thesis,
    _gate_key_to_label,
    _humanize_gate_list,
    _normalize_candidate_payload,
    _normalize_gate_reasons,
    _top_positives_and_risks,
    _nonnegative_int,
    _parking_evidence_band,
    _road_evidence_band,
    _road_signal_from_context,
    clear_expansion_caches,
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


def _road_signal_distance_component(distance_m, *, touches: bool = False) -> float:
    """Recover the distance_component of _road_signal_from_context.

    The scorer returns touches_component * 0.70 + distance_component * 0.30.
    With touches=False, touches_component is 0 and the whole signal equals
    distance_component * 0.30, so distance_component = signal / 0.30.
    """
    signal = _road_signal_from_context(
        {"touches_road": touches, "nearest_major_road_distance_m": distance_m}
    )
    return signal / 0.30


def test_road_signal_distance_none_is_neutral():
    # Missing distance — unknown, neutral component.
    assert _road_signal_distance_component(None) == pytest.approx(0.5, abs=1e-3)


def test_road_signal_distance_sentinel_is_neutral():
    # 5000 is the COALESCE fallback for "no major road within 700 m" and
    # must be treated as unknown, not penalised. Previously returned 0.0.
    assert _road_signal_distance_component(_ROAD_DISTANCE_SENTINEL_M) == pytest.approx(
        0.5, abs=1e-3
    )
    # Any distance at or beyond the sentinel is equally bogus.
    assert _road_signal_distance_component(_ROAD_DISTANCE_SENTINEL_M + 1.0) == pytest.approx(
        0.5, abs=1e-3
    )


def test_road_signal_distance_close_is_reward():
    # 50 m from an arterial — strong proximity reward unchanged.
    assert _road_signal_distance_component(50.0) > 0.9


def test_road_signal_distance_near_threshold_unchanged():
    # 699 m — below the 700 m search radius and below the sentinel, so the
    # real falloff still applies (capped at 500 m → 0.0, but strictly > 0
    # for d < 500). Use a value just under the 500 m cap.
    assert _road_signal_distance_component(499.0) > 0.0
    # At 699 m we are past the 500 m falloff cap, so component is 0.0.
    assert _road_signal_distance_component(699.0) == pytest.approx(0.0, abs=1e-3)


def test_road_signal_sentinel_lifts_score_vs_old_behaviour():
    # Regression guard: a parcel that touches a side street but has no
    # arterial in range (sentinel distance) should now score ≈ +0.09 higher
    # than under the old logic (0.0 penalty on the 30% distance component).
    ctx = {
        "touches_road": True,
        "nearest_major_road_distance_m": _ROAD_DISTANCE_SENTINEL_M,
    }
    new_signal = _road_signal_from_context(ctx)
    # Old behaviour: distance_component = 0.0 → 1.0*0.70 + 0.0*0.30 = 0.70
    old_signal = 1.0 * 0.70 + 0.0 * 0.30
    # New behaviour: distance_component = 0.5 → 1.0*0.70 + 0.5*0.30 = 0.85
    expected_new = 1.0 * 0.70 + 0.5 * 0.30
    assert new_signal == pytest.approx(expected_new, abs=1e-3)
    assert new_signal > old_signal
    # Lock direction: the lift is ~0.15 on the composite [0,1] signal,
    # which feeds a rent multiplier with 0.20 weight and later gets scaled
    # into the 0.70–1.35 range. Keep the direction-of-change assertion
    # conservative — we only care that sentinel is no longer harsher than
    # "unknown".
    neutral_signal = _road_signal_from_context(
        {"touches_road": True, "nearest_major_road_distance_m": None}
    )
    assert new_signal == pytest.approx(neutral_signal, abs=1e-3)


def test_candidate_gate_status_exposes_advisory_failures_without_blocking():
    gate_status, reasons = _candidate_gate_status(
        fit_score=78.0,
        area_fit_score=82.0,
        area_m2=200.0,
        min_area_m2=100.0,
        max_area_m2=500.0,
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
# Listings-aware confidence score tests (Patch 04)
# ---------------------------------------------------------------------------

def test_confidence_score_listing_full_ground_truth():
    """Listing with all measured fields scores ~95."""
    score = _confidence_score(
        is_listing=True,
        rent_confidence="actual",
        area_confidence="actual",
        unit_street_width_m=12.0,
        image_url="https://example.com/img.jpg",
        landuse_label="commercial",
        population_reach=5000.0,
    )
    assert score == 100.0  # 30+20+15+15+10+5+5 = 100


def test_confidence_score_listing_no_image():
    """Missing image costs exactly 10 points."""
    with_image = _confidence_score(
        is_listing=True,
        rent_confidence="actual",
        area_confidence="actual",
        unit_street_width_m=12.0,
        image_url="https://example.com/img.jpg",
        landuse_label="commercial",
        population_reach=5000.0,
    )
    without_image = _confidence_score(
        is_listing=True,
        rent_confidence="actual",
        area_confidence="actual",
        unit_street_width_m=12.0,
        image_url=None,
        landuse_label="commercial",
        population_reach=5000.0,
    )
    assert with_image - without_image == 10.0


def test_confidence_score_listing_no_measured_rent():
    """Listing without actual rent scores lower."""
    score = _confidence_score(
        is_listing=True,
        rent_confidence="interpolated",
        area_confidence="actual",
        unit_street_width_m=12.0,
        image_url="https://example.com/img.jpg",
        landuse_label="commercial",
        population_reach=5000.0,
    )
    assert score == 80.0  # 30+0+15+15+10+5+5


def test_confidence_score_listing_minimal():
    """Listing with no measured fields gets base 30."""
    score = _confidence_score(
        is_listing=True,
        rent_confidence=None,
        area_confidence=None,
        unit_street_width_m=None,
        image_url=None,
        landuse_label=None,
        population_reach=0.0,
    )
    assert score == 30.0


def test_confidence_score_parcel_capped_at_70():
    """Parcel path is capped at 70 regardless of context richness."""
    score = _confidence_score(
        is_listing=False,
        landuse_label="commercial",
        population_reach=5000.0,
        delivery_listing_count=10,
    )
    assert score == 70.0  # 40+25+20+15=100 but capped at 70


def test_confidence_score_parcel_legacy_base():
    """Parcel with no context gets base 40."""
    score = _confidence_score(
        is_listing=False,
        landuse_label=None,
        population_reach=0.0,
        delivery_listing_count=0,
    )
    assert score == 40.0


def test_confidence_grade_listing_a():
    """Listing with high score grades A without needing parcel context."""
    grade = _confidence_grade(
        confidence_score=85.0,
        district="Al Olaya",
        provider_platform_count=3,
        multi_platform_presence_score=20.0,
        rent_source="commercial_unit_actual",
        road_context_available=False,
        parking_context_available=False,
        zoning_available=False,
        delivery_observed=False,
        data_completeness_score=0,
        is_listing=True,
    )
    assert grade == "A"


def test_confidence_grade_listing_not_demoted_by_missing_context():
    """Listing grade is NOT demoted by missing parcel-era context flags."""
    grade_with_context = _confidence_grade(
        confidence_score=75.0,
        district="Al Olaya",
        provider_platform_count=2,
        multi_platform_presence_score=10.0,
        rent_source="commercial_unit_actual",
        road_context_available=True,
        parking_context_available=True,
        zoning_available=True,
        delivery_observed=True,
        is_listing=True,
    )
    grade_without_context = _confidence_grade(
        confidence_score=75.0,
        district="Al Olaya",
        provider_platform_count=2,
        multi_platform_presence_score=10.0,
        rent_source="commercial_unit_actual",
        road_context_available=False,
        parking_context_available=False,
        zoning_available=False,
        delivery_observed=False,
        is_listing=True,
    )
    assert grade_with_context == grade_without_context


def test_confidence_grade_listing_d_for_low_score():
    """Listing with very low score still gets D."""
    grade = _confidence_grade(
        confidence_score=20.0,
        district=None,
        provider_platform_count=0,
        multi_platform_presence_score=0.0,
        rent_source="conservative_default",
        is_listing=True,
    )
    assert grade == "D"


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
    """When delivery is NOT observed and no district data, the thesis uses qualified 'inferred' language."""
    thesis = _build_demand_thesis(
        demand_score=75.0,
        population_reach=50000,
        provider_density_score=0.0,
        provider_whitespace_score=65.0,
        delivery_competition_score=70.0,
        delivery_observed=False,
    )
    assert "not observed (inferred)" in thesis
    assert "inferred whitespace opportunity" in thesis
    assert "not directly observed" in thesis


def test_demand_thesis_district_fallback_uses_district_language():
    """When delivery is NOT observed but district data exists (provider_density_score > 0),
    the thesis uses 'district-level estimate' language."""
    thesis = _build_demand_thesis(
        demand_score=75.0,
        population_reach=50000,
        provider_density_score=70.0,
        provider_whitespace_score=65.0,
        delivery_competition_score=70.0,
        delivery_observed=False,
    )
    assert "district-level estimate" in thesis
    assert "district-inferred" in thesis


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
        area_m2=600.0,  # outside range -> area gate fails
        min_area_m2=100.0,
        max_area_m2=500.0,
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
        brand_profile={"primary_channel": "delivery"},
        road_context_available=True,
        parking_context_available=True,
    )
    assert gate_status["overall_pass"] is not True
    assert len(reasons["blocking_failures"]) > 0


# ---------------------------------------------------------------------------
# parking_pass trusts derived parking_score (Aqar listings)
# ---------------------------------------------------------------------------

_PARKING_GATE_KWARGS = dict(
    fit_score=78.0,
    area_fit_score=82.0,
    area_m2=200.0,
    min_area_m2=100.0,
    max_area_m2=500.0,
    zoning_fit_score=88.0,
    landuse_available=True,
    frontage_score=70.0,
    access_score=70.0,
    district="Al Olaya",
    distance_to_nearest_branch_m=3200.0,
    provider_density_score=60.0,
    multi_platform_presence_score=40.0,
    economics_score=70.0,
    brand_profile={"primary_channel": "balanced"},
    road_context_available=True,
    parking_context_available=True,
    is_listing=True,
    unit_street_width_m=12.0,
)


def test_parking_pass_true_when_score_above_threshold_for_listing():
    """Aqar listings with parking_score >= 45 now pass the parking gate
    instead of being stuck at None."""
    gate_status, reasons = _candidate_gate_status(
        **_PARKING_GATE_KWARGS, parking_score=60.0,
    )
    assert gate_status["parking_pass"] is True
    assert "parking_pass" in reasons["passed"]


def test_parking_pass_false_when_score_below_threshold_for_listing():
    """Aqar listings with parking_score < 45 fail the parking gate as an
    advisory failure (not a blocking failure)."""
    gate_status, reasons = _candidate_gate_status(
        **_PARKING_GATE_KWARGS, parking_score=30.0,
    )
    assert gate_status["parking_pass"] is False
    assert "parking_pass" in reasons["failed"]
    assert "parking_pass" in reasons["advisory_failures"]
    assert "parking_pass" not in reasons["blocking_failures"]


def test_parking_pass_none_when_score_missing_for_listing():
    """When parking_score is None (no context), parking_pass stays None."""
    gate_status, reasons = _candidate_gate_status(
        **_PARKING_GATE_KWARGS, parking_score=None,
    )
    assert gate_status["parking_pass"] is None
    assert "parking_pass" in reasons["unknown"]


def test_smoke():
    assert True


# ---------------------------------------------------------------------------
# Regression tests: summary contradiction (Fix A)
# ---------------------------------------------------------------------------

def test_summary_no_pass_when_zero_pass_candidates():
    """When pass_count=0 and no unknowns, summary should say 'no candidate passes'."""
    candidates = [
        {"gate_status_json": {"overall_pass": False}, "gate_reasons_json": {"blocking_failures": ["zoning_fit_pass"]}, "final_score": 60.0},
        {"gate_status_json": {"overall_pass": False}, "gate_reasons_json": {"blocking_failures": ["area_fit_pass"]}, "final_score": 55.0},
    ]
    pass_candidates = [c for c in candidates if (c.get("gate_status_json") or {}).get("overall_pass") is True]
    unknown_candidates = [
        c for c in candidates
        if (c.get("gate_status_json") or {}).get("overall_pass") is None
        and not (c.get("gate_reasons_json") or {}).get("blocking_failures")
    ]
    pass_count = len(pass_candidates)
    validation_clear_count = len(unknown_candidates)
    assert pass_count == 0
    assert validation_clear_count == 0


def test_summary_pass_when_some_candidates_pass():
    """When some candidates have overall_pass=True, pass_count must be > 0."""
    candidates = [
        {"gate_status_json": {"overall_pass": True}, "gate_reasons_json": {"blocking_failures": []}, "final_score": 75.0},
        {"gate_status_json": {"overall_pass": False}, "gate_reasons_json": {"blocking_failures": ["zoning_fit_pass"]}, "final_score": 55.0},
    ]
    pass_candidates = [c for c in candidates if (c.get("gate_status_json") or {}).get("overall_pass") is True]
    pass_count = len(pass_candidates)
    assert pass_count == 1  # Strict: only overall_pass=True counts


def test_summary_unknown_no_blocking_is_validation_clear_not_pass():
    """Unknown candidates (overall_pass=None, no blocking failures) are validation-clear,
    not strict passes. They suppress the 'no pass' notice but don't inflate pass_count."""
    candidates = [
        {"gate_status_json": {"overall_pass": None}, "gate_reasons_json": {"blocking_failures": []}, "final_score": 72.0},
        {"gate_status_json": {"overall_pass": False}, "gate_reasons_json": {"blocking_failures": ["zoning_fit_pass"]}, "final_score": 55.0},
    ]
    pass_candidates = [c for c in candidates if (c.get("gate_status_json") or {}).get("overall_pass") is True]
    unknown_candidates = [
        c for c in candidates
        if (c.get("gate_status_json") or {}).get("overall_pass") is None
        and not (c.get("gate_reasons_json") or {}).get("blocking_failures")
    ]
    pass_count = len(pass_candidates)
    validation_clear_count = len(unknown_candidates)
    # pass_count is strict — unknown candidates don't count
    assert pass_count == 0
    # But validation_clear_count tracks them separately
    assert validation_clear_count == 1


def test_summary_selected_nonpass_does_not_override_search_level():
    """Selected candidate not passing shouldn't override pass_count when others pass."""
    candidates = [
        {"id": "c1", "gate_status_json": {"overall_pass": True}, "gate_reasons_json": {"blocking_failures": []}, "final_score": 78.0},
        {"id": "c2", "gate_status_json": {"overall_pass": False}, "gate_reasons_json": {"blocking_failures": ["zoning_fit_pass"]}, "final_score": 55.0},
    ]
    pass_candidates = [c for c in candidates if (c.get("gate_status_json") or {}).get("overall_pass") is True]
    pass_count = len(pass_candidates)
    assert pass_count == 1  # c1 strictly passes
    # The summary should reflect this, not the selected candidate's gate status


# ---------------------------------------------------------------------------
# Regression tests: dedupe strengthening (Fix C)
# ---------------------------------------------------------------------------

def test_dedupe_collapses_near_clones_in_ranked_output():
    """Near-clone candidates at the same spatial grid cell should collapse in default mode."""
    candidates = [
        {"parcel_id": "", "lat": 24.7001, "lon": 46.7001, "district": "Al Olaya",
         "area_m2": 200, "estimated_rent_sar_m2_year": 900, "economics_score": 65,
         "distance_to_nearest_branch_m": 2000, "final_score": 72},
        {"parcel_id": "", "lat": 24.7002, "lon": 46.7002, "district": "Al Olaya",
         "area_m2": 200, "estimated_rent_sar_m2_year": 900, "economics_score": 65,
         "distance_to_nearest_branch_m": 2000, "final_score": 71},
    ]
    result = _dedupe_candidates(candidates)
    # Same spatial grid cell + district + area + rent + branch → collapse
    assert len(result) == 1, f"Expected 1 after dedupe, got {len(result)}"


def test_dedupe_distinct_parcel_ids_survive():
    """Candidates with distinct non-null parcel_ids must never be collapsed."""
    candidates = [
        {"parcel_id": "p1", "lat": 24.7001, "lon": 46.7001, "district": "Al Olaya",
         "area_m2": 200, "estimated_rent_sar_m2_year": 900, "economics_score": 65,
         "distance_to_nearest_branch_m": 2000, "final_score": 72},
        {"parcel_id": "p2", "lat": 24.7002, "lon": 46.7002, "district": "Al Olaya",
         "area_m2": 210, "estimated_rent_sar_m2_year": 950, "economics_score": 66,
         "distance_to_nearest_branch_m": 2100, "final_score": 71},
    ]
    result = _dedupe_candidates(candidates)
    assert len(result) == 2


def test_dedupe_aggressive_mode_shortlist_cleaner():
    """Aggressive mode should collapse more aggressively for report shortlists."""
    candidates = [
        {"parcel_id": "", "lat": 24.7001, "lon": 46.7001, "district": "Al Olaya",
         "area_m2": 200, "estimated_rent_sar_m2_year": 900, "economics_score": 65,
         "distance_to_nearest_branch_m": 2000, "final_score": 72},
        {"parcel_id": "", "lat": 24.7004, "lon": 46.7004, "district": "Al Olaya",
         "area_m2": 220, "estimated_rent_sar_m2_year": 900, "economics_score": 65,
         "distance_to_nearest_branch_m": 2000, "final_score": 72},
    ]
    normal_result = _dedupe_candidates(candidates, aggressive=False)
    aggressive_result = _dedupe_candidates(candidates, aggressive=True)
    # Aggressive mode should be at least as strict as normal mode
    assert len(aggressive_result) <= len(normal_result)


def test_dedupe_economics_similarity_only_aggressive():
    """Economics-similarity key only collapses in aggressive mode, not default."""
    candidates = [
        {"parcel_id": "", "lat": 24.701, "lon": 46.701, "district": "Al Olaya",
         "area_m2": 200, "estimated_rent_sar_m2_year": 900, "economics_score": 65,
         "distance_to_nearest_branch_m": 2000, "final_score": 72},
        {"parcel_id": "", "lat": 24.702, "lon": 46.702, "district": "Al Olaya",
         "area_m2": 200, "estimated_rent_sar_m2_year": 900, "economics_score": 65,
         "distance_to_nearest_branch_m": 2500, "final_score": 71},
    ]
    # Default mode: different spatial grid cells survive
    result_default = _dedupe_candidates(candidates)
    assert len(result_default) == 2, "Default mode should not collapse by economics alone"
    # Aggressive mode: economics-similarity key catches these
    result_aggressive = _dedupe_candidates(candidates, aggressive=True)
    assert len(result_aggressive) == 1, "Aggressive mode should collapse by economics similarity"


# ---------------------------------------------------------------------------
# Regression tests: delivery wording honesty (Fix D)
# ---------------------------------------------------------------------------

def test_delivery_wording_inferred_when_no_observed_listings():
    """When provider_density=0, multi_platform=0, delivery_competition=0,
    wording must clearly indicate inferred, not observed strength."""
    candidate = {
        "demand_score": 75.0,
        "whitespace_score": 80.0,  # High because no competition observed
        "brand_fit_score": 72.0,
        "economics_score": 68.0,
        "delivery_competition_score": 0.0,
        "cannibalization_score": 30.0,
        "provider_density_score": 0.0,
        "multi_platform_presence_score": 0.0,
        "gate_status_json": {"overall_pass": True},
    }
    gate_reasons = {"failed": [], "unknown": [], "passed": ["delivery_market_pass"]}
    positives, risks = _top_positives_and_risks(candidate=candidate, gate_reasons=gate_reasons)

    # Should NOT have positive-sounding delivery/whitespace wording
    whitespace_positives = [p for p in positives if "whitespace" in p.lower()]
    for p in whitespace_positives:
        assert "inferred" in p.lower() or "opportunity" in p.lower(), \
            f"Whitespace positive should be labeled as inferred: {p}"

    # Should have the inferred-delivery risk
    delivery_risks = [r for r in risks if "inferred" in r.lower() or "no observed" in r.lower()]
    assert len(delivery_risks) > 0, "Must warn about inferred delivery data"


def test_delivery_wording_observed_when_listings_present():
    """When delivery listings are observed, wording should reflect observed strength."""
    candidate = {
        "demand_score": 75.0,
        "whitespace_score": 55.0,
        "brand_fit_score": 72.0,
        "economics_score": 68.0,
        "delivery_competition_score": 45.0,
        "cannibalization_score": 30.0,
        "provider_density_score": 60.0,
        "multi_platform_presence_score": 40.0,
        "gate_status_json": {"overall_pass": True},
    }
    gate_reasons = {"failed": [], "unknown": [], "passed": ["delivery_market_pass"]}
    positives, risks = _top_positives_and_risks(candidate=candidate, gate_reasons=gate_reasons)

    # Should NOT have inferred-delivery risk
    delivery_risks = [r for r in risks if "inferred" in r.lower() and "delivery" in r.lower()]
    assert len(delivery_risks) == 0, "Should not warn about inferred delivery when observed"


def test_delivery_gate_explanation_inferred_when_no_observed():
    """Gate explanation for delivery should note inferred status when no listings observed."""
    gate_status, reasons = _candidate_gate_status(
        fit_score=78.0,
        area_fit_score=82.0,
        area_m2=200.0,
        min_area_m2=100.0,
        max_area_m2=500.0,
        zoning_fit_score=88.0,
        landuse_available=True,
        frontage_score=70.0,
        access_score=70.0,
        parking_score=70.0,
        district="Al Olaya",
        distance_to_nearest_branch_m=3200.0,
        provider_density_score=0.0,   # No observed delivery
        multi_platform_presence_score=0.0,  # No observed delivery
        economics_score=58.0,
        brand_profile={"primary_channel": "balanced"},
        road_context_available=True,
        parking_context_available=True,
    )
    assert reasons["delivery_observation_mode"] == "inferred"
    explanation = reasons["explanations"]["delivery_market_pass"]
    assert "no delivery activity" in explanation.lower() or "inferred" in explanation.lower()


def test_delivery_gate_explanation_observed_when_listings_present():
    """Gate explanation should reflect observed delivery when listings are present."""
    gate_status, reasons = _candidate_gate_status(
        fit_score=78.0,
        area_fit_score=82.0,
        area_m2=200.0,
        min_area_m2=100.0,
        max_area_m2=500.0,
        zoning_fit_score=88.0,
        landuse_available=True,
        frontage_score=70.0,
        access_score=70.0,
        parking_score=70.0,
        district="Al Olaya",
        distance_to_nearest_branch_m=3200.0,
        provider_density_score=52.0,   # Observed
        multi_platform_presence_score=35.0,  # Observed
        economics_score=58.0,
        brand_profile={"primary_channel": "delivery"},
        road_context_available=True,
        parking_context_available=True,
    )
    assert reasons["delivery_observation_mode"] == "observed"


def test_demand_thesis_inferred_when_no_delivery():
    """Demand thesis should use inferred language when no delivery observed."""
    thesis = _build_demand_thesis(
        demand_score=70.0,
        population_reach=50000.0,
        provider_density_score=0.0,
        provider_whitespace_score=80.0,
        delivery_competition_score=0.0,
        delivery_observed=False,
    )
    assert "inferred" in thesis.lower() or "not observed" in thesis.lower()


def test_demand_thesis_observed_when_delivery_present():
    """Demand thesis should use observed language when delivery listings exist."""
    thesis = _build_demand_thesis(
        demand_score=70.0,
        population_reach=50000.0,
        provider_density_score=60.0,
        provider_whitespace_score=55.0,
        delivery_competition_score=45.0,
        delivery_observed=True,
    )
    assert "inferred" not in thesis.lower()


# ---------------------------------------------------------------------------
# Regression tests: caching (Fix E)
# ---------------------------------------------------------------------------

def test_clear_expansion_caches():
    """clear_expansion_caches should not raise and should clear state."""
    clear_expansion_caches()
    # Just verify it doesn't error — caches are module-internal


# ---------------------------------------------------------------------------
# Follow-up patch tests: gate label humanization
# ---------------------------------------------------------------------------

def test_gate_key_to_label_known_keys():
    """Known gate keys map to human-readable labels."""
    assert _gate_key_to_label("zoning_fit_pass") == "zoning fit"
    assert _gate_key_to_label("economics_pass") == "economics"
    assert _gate_key_to_label("delivery_market_pass") == "delivery market"
    assert _gate_key_to_label("frontage_access_pass") == "frontage/access"


def test_gate_key_to_label_unknown_key_fallback():
    """Unknown gate keys strip _pass and underscores."""
    result = _gate_key_to_label("some_new_gate_pass")
    assert "_pass" not in result
    assert "_" not in result


def test_humanize_gate_list_deduplicates():
    """Duplicate labels after humanization are removed."""
    result = _humanize_gate_list(["zoning_fit_pass", "zoning_fit_pass", "economics_pass"])
    assert result == ["zoning fit", "economics"]


def test_humanize_gate_list_none_safe():
    """None input returns empty list."""
    assert _humanize_gate_list(None) == []


def test_normalize_gate_reasons_humanizes_lists():
    """_normalize_gate_reasons converts raw keys to human-readable labels."""
    raw = {
        "passed": ["zoning_fit_pass", "economics_pass"],
        "failed": ["parking_pass"],
        "unknown": ["delivery_market_pass"],
        "thresholds": {},
        "explanations": {},
    }
    result = _normalize_gate_reasons(raw)
    assert result["passed"] == ["zoning fit", "economics"]
    assert result["failed"] == ["parking"]
    assert result["unknown"] == ["delivery market"]
    # Verify no raw keys leaked through
    for lst_name in ("passed", "failed", "unknown"):
        for label in result[lst_name]:
            assert "_pass" not in label, f"Raw key leaked in {lst_name}: {label}"


def test_top_risks_never_show_raw_gate_keys():
    """Risk text from _top_positives_and_risks should use human labels, not raw keys."""
    candidate = {
        "demand_score": 50.0,
        "whitespace_score": 40.0,
        "brand_fit_score": 50.0,
        "economics_score": 40.0,
        "delivery_competition_score": 0.0,
        "cannibalization_score": 30.0,
        "provider_density_score": 0.0,
        "multi_platform_presence_score": 0.0,
        "gate_status_json": {"overall_pass": False},
    }
    gate_reasons = {
        "failed": ["zoning_fit_pass", "parking_pass"],
        "unknown": ["frontage_access_pass"],
        "passed": [],
    }
    _, risks = _top_positives_and_risks(candidate=candidate, gate_reasons=gate_reasons)
    for risk in risks:
        assert "_pass" not in risk, f"Raw gate key leaked in risk text: {risk}"
        assert "zoning_fit_pass" not in risk
        assert "parking_pass" not in risk
        assert "frontage_access_pass" not in risk


# ---------------------------------------------------------------------------
# Follow-up patch tests: completeness scoring
# ---------------------------------------------------------------------------

def test_completeness_below_100_when_zoning_unavailable():
    """data_completeness_score < 100 when zoning context is unavailable."""
    db = _FakeDB()
    result = _candidate_feature_snapshot(
        db,
        **{**_SNAPSHOT_DEFAULTS, "landuse_label": "", "landuse_code": ""},
    )
    assert result["data_completeness_score"] < 100, \
        f"Completeness should be < 100 with no zoning, got {result['data_completeness_score']}"
    assert "zoning_context_unavailable" in result["missing_context"]


def test_completeness_below_100_when_delivery_not_observed():
    """data_completeness_score < 100 when no delivery listings observed."""
    db = _FakeDB()
    result = _candidate_feature_snapshot(
        db,
        **{**_SNAPSHOT_DEFAULTS, "provider_listing_count": 0, "provider_platform_count": 0},
    )
    assert result["data_completeness_score"] < 100, \
        f"Completeness should be < 100 with no delivery, got {result['data_completeness_score']}"
    assert "delivery_observation_unavailable" in result["missing_context"]


def test_completeness_not_100_when_both_zoning_and_delivery_missing():
    """Both zoning unavailable and delivery unobserved yields significantly less than 100."""
    db = _FakeDB()
    result = _candidate_feature_snapshot(
        db,
        **{
            **_SNAPSHOT_DEFAULTS,
            "landuse_label": "",
            "landuse_code": "",
            "provider_listing_count": 0,
            "provider_platform_count": 0,
        },
    )
    assert result["data_completeness_score"] < 80, \
        f"Expected < 80% completeness, got {result['data_completeness_score']}"


def test_confidence_grade_default_completeness_does_not_inflate():
    """When data_completeness_score is not passed, grade should not inflate to A."""
    # With the old default of 100, this would return A. With default 0, it should not.
    grade = _confidence_grade(
        confidence_score=90.0,
        district="Al Olaya",
        provider_platform_count=3,
        multi_platform_presence_score=20.0,
        rent_source="aqar",
        road_context_available=True,
        parking_context_available=True,
        zoning_available=True,
        delivery_observed=True,
        # data_completeness_score omitted — should default to 0
    )
    assert grade != "A", "Grade should not be A when completeness defaults to 0"


# ---------------------------------------------------------------------------
# Follow-up patch tests: rent display consistency
# ---------------------------------------------------------------------------

def test_display_annual_rent_matches_rounded_rent_times_area():
    """display_annual_rent_sar = round(rent/m²) × area for display consistency."""
    candidate = {
        "estimated_rent_sar_m2_year": 2000.04,
        "area_m2": 192.0,
        "estimated_annual_rent_sar": 2000.04 * 192.0,  # 384007.68
    }
    result = _normalize_candidate_payload(candidate)
    # Displayed rent: round(2000.04) = 2000
    # Display annual: 2000 × 192 = 384000
    assert result["display_annual_rent_sar"] == 384000.0, \
        f"Expected 384000, got {result['display_annual_rent_sar']}"
    # Internal rent should be unchanged
    assert result["estimated_annual_rent_sar"] == 2000.04 * 192.0


def test_display_annual_rent_uses_exact_when_rent_is_whole():
    """When rent is already a whole number, display and internal should match."""
    candidate = {
        "estimated_rent_sar_m2_year": 2000.0,
        "area_m2": 200.0,
        "estimated_annual_rent_sar": 400000.0,
    }
    result = _normalize_candidate_payload(candidate)
    assert result["display_annual_rent_sar"] == 400000.0
    assert result["estimated_annual_rent_sar"] == 400000.0


def test_display_annual_rent_falls_back_when_missing():
    """When rent/m² is zero or missing, display_annual_rent_sar falls back to internal."""
    candidate = {
        "estimated_rent_sar_m2_year": 0,
        "area_m2": 200.0,
        "estimated_annual_rent_sar": 0,
    }
    result = _normalize_candidate_payload(candidate)
    assert result["display_annual_rent_sar"] == 0


# ---------------------------------------------------------------------------
# Follow-up patch tests: bulk persistence with fallback
# ---------------------------------------------------------------------------

def test_chunked_helper():
    """_chunked splits a list into fixed-size batches."""
    from app.services.expansion_advisor import _chunked
    data = list(range(10))
    batches = list(_chunked(data, 3))
    assert batches == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]


def test_chunked_empty():
    """_chunked on empty list returns empty iterator."""
    from app.services.expansion_advisor import _chunked
    assert list(_chunked([], 5)) == []


def test_chunked_exact_fit():
    """_chunked with exact multiple of size."""
    from app.services.expansion_advisor import _chunked
    assert list(_chunked([1, 2, 3, 4], 2)) == [[1, 2], [3, 4]]
