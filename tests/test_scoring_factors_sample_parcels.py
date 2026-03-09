"""
5 Riyadh sample parcels demonstrating non-flat scoring from the upgraded
zoning_fit, parking, and commercial_density factors.

Each test constructs a mock DB that returns realistic Riyadh data for a
specific location type, then calls the v2 scoring functions and asserts
the output is genuinely non-flat (i.e., not near 50.0) with meaningful
confidence, rationale, and meta.
"""

from __future__ import annotations

from unittest.mock import MagicMock, call
import pytest

from app.services.restaurant_scoring_factors import (
    ScoredFactor,
    zoning_fit_score,
    parking_availability_score,
    commercial_density_score,
)


# ---------------------------------------------------------------------------
# Helpers — mock DB that returns controlled data per query
# ---------------------------------------------------------------------------

def _mock_mappings_first(data: dict | None):
    """Build a mock result for .mappings().first() returning data."""
    result = MagicMock()
    row = MagicMock()
    if data:
        row.get = lambda k, d=None: data.get(k, d)
        row.__getitem__ = lambda self, k: data[k]
        row.__contains__ = lambda self, k: k in data
    result.mappings.return_value.first.return_value = data and row
    result.mappings.return_value.all.return_value = [row] if data else []
    result.scalar.return_value = data.get("_scalar") if data else 0
    return result


def _mock_mappings_all(rows: list[dict]):
    """Build a mock result for .mappings().all() returning rows."""
    result = MagicMock()
    mapped = []
    for d in rows:
        m = MagicMock()
        m.get = lambda k, default=None, _d=d: _d.get(k, default)
        m.__getitem__ = lambda self, k, _d=d: _d[k]
        mapped.append(m)
    result.mappings.return_value.all.return_value = mapped
    result.mappings.return_value.first.return_value = mapped[0] if mapped else None
    result.scalar.return_value = len(rows)
    return result


def _scalar_result(value):
    result = MagicMock()
    result.scalar.return_value = value
    result.mappings.return_value.first.return_value = None
    result.mappings.return_value.all.return_value = []
    return result


def _build_db(execute_responses: list):
    """Build a mock DB that returns responses in order for successive execute() calls."""
    db = MagicMock()
    db.rollback.return_value = None
    db.execute.side_effect = execute_responses
    return db


# ---------------------------------------------------------------------------
# Sample 1: Commercial zone — Olaya Street (prime commercial corridor)
# ---------------------------------------------------------------------------

class TestSample1_CommercialZone:
    """Olaya St: commercial zoning, good road access, many anchors."""

    def test_zoning_fit(self):
        db = _build_db([
            # Step 1: ArcGIS parcel lookup — commercial
            _mock_mappings_first({
                "landuse_label": "تجاري",  # commercial
                "landuse_code": "1",
                "area_m2": 1200,
                "perimeter_m": 140,
            }),
            # Step 5: road adjacency — primary road nearby
            _mock_mappings_all([
                {"highway": "primary", "distance_m": 25.0},
                {"highway": "service", "distance_m": 80.0},
            ]),
        ])
        result = zoning_fit_score(db, 24.6911, 46.6853)

        assert isinstance(result, ScoredFactor)
        assert result.score >= 90.0, f"Commercial zone should score >=90, got {result.score}"
        assert result.confidence >= 0.8
        assert result.rationale == "commercial_zone"
        assert result.meta["arcgis_label"] == "تجاري"
        assert result.meta["match_source"] == "arcgis_label"
        assert result.meta["road_bonus"] > 0

    def test_parking(self):
        db = _build_db([
            # parcel geometry: 1200m² commercial parcel
            _mock_mappings_first({
                "area_m2": 1200, "perimeter_m": 140, "compactness": 0.77,
            }),
            # road access: primary + service roads
            _mock_mappings_all([
                {"highway": "primary", "distance_m": 25, "name": "Olaya St"},
                {"highway": "service", "distance_m": 60, "name": None},
            ]),
            # parking structures: 2 nearby
            _scalar_result(2),
            # malls nearby: 1
            _scalar_result(1),
            # large buildings: 5
            _scalar_result(5),
        ])
        result = parking_availability_score(db, 24.6911, 46.6853)

        assert isinstance(result, ScoredFactor)
        assert result.score > 60.0, f"Prime commercial area should have decent parking, got {result.score}"
        assert result.confidence >= 0.7
        assert "composite:" in result.rationale

    def test_commercial_density(self):
        db = _build_db([
            # building intensity: 50 buildings, 45 non-residential
            _mock_mappings_first({
                "total_buildings": 50,
                "total_footprint_m2": 40000,
                "non_residential_count": 45,
                "non_residential_footprint_m2": 35000,
            }),
            # demand anchors from overture_buildings: office, hotel classes
            _mock_mappings_all([
                {"class": "commercial", "subtype": "commercial"},
                {"class": "office", "subtype": "commercial"},
                {"class": "hotel", "subtype": "commercial"},
                {"class": "retail", "subtype": "commercial"},
            ]),
            # OSM polygon amenities: schools, shops
            _mock_mappings_all([
                {"amenity": "school", "shop": None, "name": "Al Riyadh School"},
                {"amenity": None, "shop": "supermarket", "name": "Tamimi"},
            ]),
            # commercial parcels: 15 total, 12 commercial
            _mock_mappings_first({
                "total": 15, "commercial_count": 12,
            }),
            # POI ecosystem: 30 total, 5 non-restaurant, 3 sources
            _mock_mappings_first({
                "total_pois": 30, "non_restaurant_pois": 5, "source_diversity": 3,
            }),
        ])
        result = commercial_density_score(db, 24.6911, 46.6853)

        assert isinstance(result, ScoredFactor)
        assert result.score > 65.0, f"Dense commercial corridor should score >65, got {result.score}"
        assert result.confidence >= 0.6


# ---------------------------------------------------------------------------
# Sample 2: Residential zone — Al Malqa (villa district)
# ---------------------------------------------------------------------------

class TestSample2_ResidentialZone:
    """Al Malqa: residential zoning, low commercial activity."""

    def test_zoning_fit(self):
        db = _build_db([
            # ArcGIS: residential parcel
            _mock_mappings_first({
                "landuse_label": "سكني",  # residential
                "landuse_code": "2",
                "area_m2": 500,
                "perimeter_m": 90,
            }),
            # road adjacency: residential road only
            _mock_mappings_all([
                {"highway": "residential", "distance_m": 15.0},
            ]),
        ])
        result = zoning_fit_score(db, 24.8134, 46.6271)

        assert result.score <= 40.0, f"Residential zone should score <=40, got {result.score}"
        assert result.confidence >= 0.8
        assert result.rationale == "residential_zone"

    def test_commercial_density(self):
        db = _build_db([
            # few buildings, mostly residential
            _mock_mappings_first({
                "total_buildings": 8,
                "total_footprint_m2": 3000,
                "non_residential_count": 1,
                "non_residential_footprint_m2": 200,
            }),
            # no commercial anchors
            _mock_mappings_all([
                {"class": "residential", "subtype": "residential"},
            ]),
            # no OSM amenities
            _mock_mappings_all([]),
            # all nearby parcels are residential
            _mock_mappings_first({
                "total": 10, "commercial_count": 0,
            }),
            # sparse POI ecosystem
            _mock_mappings_first({
                "total_pois": 2, "non_restaurant_pois": 0, "source_diversity": 1,
            }),
        ])
        result = commercial_density_score(db, 24.8134, 46.6271)

        assert result.score < 35.0, f"Residential area should have low commercial density, got {result.score}"


# ---------------------------------------------------------------------------
# Sample 3: Mixed-use — King Fahd Road (major corridor)
# ---------------------------------------------------------------------------

class TestSample3_MixedUse:
    """King Fahd Rd: mixed-use zoning near a major highway."""

    def test_zoning_fit(self):
        db = _build_db([
            # ArcGIS: mixed-use
            _mock_mappings_first({
                "landuse_label": "مختلط",  # mixed-use
                "landuse_code": "4",
                "area_m2": 2500,
                "perimeter_m": 200,
            }),
            # primary + service road
            _mock_mappings_all([
                {"highway": "primary", "distance_m": 15.0},
                {"highway": "service", "distance_m": 45.0},
            ]),
        ])
        result = zoning_fit_score(db, 24.7136, 46.6753)

        assert result.score >= 90.0, f"Mixed-use with primary road should score >=90, got {result.score}"
        assert result.confidence >= 0.8
        assert result.rationale == "mixed_use_zone"
        assert result.meta["road_bonus"] > 0

    def test_parking_large_parcel(self):
        db = _build_db([
            # large parcel with good compactness
            _mock_mappings_first({
                "area_m2": 2500, "perimeter_m": 200, "compactness": 0.79,
            }),
            # primary + service roads
            _mock_mappings_all([
                {"highway": "primary", "distance_m": 15, "name": "King Fahd Rd"},
                {"highway": "service", "distance_m": 45, "name": None},
                {"highway": "secondary", "distance_m": 120, "name": None},
            ]),
            # 1 parking structure
            _scalar_result(1),
            # 0 malls
            _scalar_result(0),
            # 3 large buildings
            _scalar_result(3),
        ])
        result = parking_availability_score(db, 24.7136, 46.6753)

        assert result.score > 55.0, f"Large mixed-use parcel should have good parking, got {result.score}"
        assert result.confidence >= 0.4


# ---------------------------------------------------------------------------
# Sample 4: Industrial zone — Second Industrial City
# ---------------------------------------------------------------------------

class TestSample4_IndustrialZone:
    """Industrial City: industrial zoning, very unfavorable for restaurants."""

    def test_zoning_fit(self):
        db = _build_db([
            # ArcGIS: industrial
            _mock_mappings_first({
                "landuse_label": "صناعي",  # industrial
                "landuse_code": "3",
                "area_m2": 5000,
                "perimeter_m": 300,
            }),
            # no nearby roads with restaurant-friendly access
            _mock_mappings_all([
                {"highway": "trunk", "distance_m": 200.0},
            ]),
        ])
        result = zoning_fit_score(db, 24.5689, 46.8320)

        assert result.score <= 30.0, f"Industrial zone should score <=30, got {result.score}"
        assert result.rationale == "industrial_zone"


# ---------------------------------------------------------------------------
# Sample 5: No ArcGIS data — district fallback (60% commercial nearby)
# ---------------------------------------------------------------------------

class TestSample5_DistrictFallback:
    """Location with no direct ArcGIS parcel hit, falling back to district context."""

    def test_zoning_fit_district_fallback(self):
        db = _build_db([
            # Step 1: No direct parcel hit
            _mock_mappings_first(None),
            # Step 2: Legacy parcel table — also no hit (raises exception)
            _scalar_result(None),
            # Step 3: District-level fallback — 60% commercial nearby
            _mock_mappings_all([
                {"landuse_label": "تجاري", "cnt": 6, "commercial_cnt": 6},
                {"landuse_label": "سكني", "cnt": 4, "commercial_cnt": 0},
            ]),
            # Step 5: road adjacency
            _mock_mappings_all([
                {"highway": "secondary", "distance_m": 50.0},
            ]),
        ])
        result = zoning_fit_score(db, 24.7500, 46.7000)

        assert result.score != 50.0, f"District fallback should NOT be flat 50, got {result.score}"
        assert result.score > 50.0, f"60% commercial district should be > 50, got {result.score}"
        assert result.rationale == "district_fallback"
        assert result.confidence < 0.7, "District fallback should have lower confidence"
        assert result.confidence > 0.1, "But not minimal confidence"
        assert result.meta.get("commercial_ratio") == 0.6
        assert result.meta["match_source"] == "district_nearby"


# ---------------------------------------------------------------------------
# Summary assertion — scores span a wide range
# ---------------------------------------------------------------------------

class TestScoreSpread:
    """Verify that the 5 samples produce genuinely different scores."""

    def _zoning_for_label(self, label: str, code: str, road_class: str = "residential"):
        db = _build_db([
            _mock_mappings_first({
                "landuse_label": label, "landuse_code": code,
                "area_m2": 800, "perimeter_m": 120,
            }),
            _mock_mappings_all([
                {"highway": road_class, "distance_m": 50.0},
            ]),
        ])
        return zoning_fit_score(db, 24.7, 46.7)

    def test_score_spread(self):
        commercial = self._zoning_for_label("تجاري", "1", "primary")
        mixed = self._zoning_for_label("مختلط", "4", "secondary")
        residential = self._zoning_for_label("سكني", "2", "residential")
        industrial = self._zoning_for_label("صناعي", "3", "trunk")

        scores = [commercial.score, mixed.score, residential.score, industrial.score]
        spread = max(scores) - min(scores)
        assert spread > 50.0, (
            f"Score spread should be >50 points across zone types, got {spread}: {scores}"
        )
        assert commercial.score > mixed.score > residential.score > industrial.score, (
            f"Expected commercial > mixed > residential > industrial: {scores}"
        )
