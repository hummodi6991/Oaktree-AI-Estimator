from app.services.expansion_advisor import (
    _candidate_feature_snapshot,
    _candidate_gate_status,
    _context_checked,
    _nonnegative_int,
    _parking_evidence_band,
    _road_evidence_band,
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


def test_smoke():
    assert True
