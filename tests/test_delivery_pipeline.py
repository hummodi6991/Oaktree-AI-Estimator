"""
Tests for the delivery data pipeline.

Covers:
- Parser normalization and extraction
- Category extraction
- District extraction
- Chain/brand detection
- Confidence assignment
- Location resolution tiers
- Resolver logic
- Record persistence
- Regression: missing lat/lon rows preserved
- Regression: approximate area-only rows survive
"""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from app.delivery.schemas import DeliveryRecord, GeocodeMethod, Platform
from app.delivery.parsers import (
    parse_legacy_record,
    parse_page_content,
    _detect_chain,
    _extract_branch_from_name,
    _extract_district_from_url,
    _extract_district_from_text,
    _estimate_parse_confidence,
)
from app.delivery.location import (
    resolve_location,
    _is_valid_riyadh_coords,
    _normalize_district_name,
    RIYADH_DISTRICT_CENTROIDS,
)
from app.delivery.pipeline import _normalize_name, record_to_row
from app.delivery.resolver import _normalize_for_match as resolver_normalize


# ============================================================================
# Parser tests
# ============================================================================


class TestParseLegacyRecord:
    """Test conversion of legacy scraper dicts to DeliveryRecord."""

    def test_basic_record(self):
        raw = {
            "id": "hungerstation:burger-king-olaya",
            "name": "Burger King - Olaya",
            "source": "hungerstation",
            "source_url": "https://hungerstation.com/riyadh/burger-king-olaya",
            "lat": None,
            "lon": None,
            "category_raw": None,
        }
        rec = parse_legacy_record(raw, "hungerstation")
        assert rec.platform == "hungerstation"
        assert rec.restaurant_name_raw == "Burger King - Olaya"
        assert rec.brand_raw == "Burger King"
        assert rec.branch_raw == "Olaya"
        assert rec.source_url == raw["source_url"]

    def test_record_with_coords(self):
        raw = {
            "id": "talabat:test-restaurant",
            "name": "Test Restaurant",
            "source": "talabat",
            "source_url": "https://talabat.com/riyadh/test",
            "lat": 24.7136,
            "lon": 46.6753,
            "category_raw": "pizza",
        }
        rec = parse_legacy_record(raw, "talabat")
        assert rec.lat == 24.7136
        assert rec.lon == 46.6753
        assert rec.geocode_method == GeocodeMethod.PLATFORM_PAYLOAD
        assert rec.location_confidence == 0.9
        assert rec.cuisine_raw == "pizza"

    def test_record_without_coords_preserved(self):
        """REGRESSION: Records without lat/lon must not be dropped."""
        raw = {
            "id": "jahez:no-coords-restaurant",
            "name": "Mystery Restaurant",
            "source": "jahez",
            "source_url": "https://jahez.net/riyadh/mystery",
            "lat": None,
            "lon": None,
            "category_raw": None,
        }
        rec = parse_legacy_record(raw, "jahez")
        assert rec is not None
        assert rec.restaurant_name_raw == "Mystery Restaurant"
        assert rec.lat is None
        assert rec.lon is None
        assert rec.geocode_method == GeocodeMethod.NONE

    def test_district_extraction_from_url(self):
        raw = {
            "id": "mrsool:test",
            "name": "Some Restaurant",
            "source": "mrsool",
            "source_url": "https://mrsool.co/riyadh/olaya/some-restaurant",
            "lat": None,
            "lon": None,
            "category_raw": None,
        }
        rec = parse_legacy_record(raw, "mrsool")
        assert rec.district_text is not None
        assert "olaya" in rec.district_text.lower()


class TestChainDetection:
    def test_known_chains(self):
        assert _detect_chain("Al Baik - Olaya") == "Al Baik"
        assert _detect_chain("McDonald's Riyadh") == "McDonald's"
        assert _detect_chain("KFC Drive Thru") == "KFC"
        assert _detect_chain("Burger King Al Malaz") == "Burger King"
        assert _detect_chain("هرفي الملز") == "Herfy"
        assert _detect_chain("كودو - الشفاء") == "Kudu"
        assert _detect_chain("البيك - الربوة") == "Al Baik"

    def test_unknown_restaurant(self):
        assert _detect_chain("My Local Restaurant") is None
        assert _detect_chain(None) is None
        assert _detect_chain("") is None

    def test_branch_extraction(self):
        assert _extract_branch_from_name("KFC - Al Olaya", "KFC") == "Al Olaya"
        assert _extract_branch_from_name("KFC (Malaz Branch)", "KFC") == "Malaz Branch"
        assert _extract_branch_from_name("KFC", "KFC") is None
        assert _extract_branch_from_name("Random Restaurant", None) is None


class TestDistrictExtraction:
    def test_from_url(self):
        assert _extract_district_from_url(
            "https://hungerstation.com/riyadh/al-olaya/burger-king"
        ) is not None

    def test_from_text(self):
        result = _extract_district_from_text("Burger King in Olaya district")
        assert result is not None

    def test_no_district(self):
        assert _extract_district_from_url("https://example.com/food") is None
        assert _extract_district_from_text("Just a random restaurant") is None
        assert _extract_district_from_url(None) is None
        assert _extract_district_from_text(None) is None


class TestParseConfidence:
    def test_full_record(self):
        rec = DeliveryRecord(
            platform="hungerstation",
            restaurant_name_raw="Test Restaurant",
            district_text="Olaya",
            lat=24.7,
            lon=46.7,
            cuisine_raw="burger",
            rating=4.5,
            delivery_time_min=30,
            brand_raw="TestChain",
        )
        conf = _estimate_parse_confidence(rec)
        assert conf >= 0.9

    def test_minimal_record(self):
        rec = DeliveryRecord(
            platform="hungerstation",
            restaurant_name_raw="Test",
        )
        conf = _estimate_parse_confidence(rec)
        assert conf == 0.25

    def test_empty_record(self):
        rec = DeliveryRecord(platform="hungerstation")
        conf = _estimate_parse_confidence(rec)
        assert conf == 0.0


class TestCategoryExtraction:
    def test_normalize_category_from_cuisine(self):
        from app.services.restaurant_categories import normalize_category

        assert normalize_category("burger") == "burger"
        assert normalize_category("pizza") == "pizza"
        assert normalize_category("shawarma") == "traditional"
        assert normalize_category("sushi") == "asian"
        assert normalize_category("coffee") == "coffee_bakery"
        assert normalize_category(None) == "international"
        assert normalize_category("unknown_type") == "international"


# ============================================================================
# Location resolution tests
# ============================================================================


class TestLocationResolution:
    def test_valid_riyadh_coords(self):
        assert _is_valid_riyadh_coords(24.7136, 46.6753)
        assert not _is_valid_riyadh_coords(None, 46.0)
        assert not _is_valid_riyadh_coords(24.7, None)
        assert not _is_valid_riyadh_coords(0.0, 0.0)
        assert not _is_valid_riyadh_coords(40.0, 46.7)  # Not Riyadh

    def test_tier_a_direct_coords(self):
        rec = DeliveryRecord(
            platform="hungerstation",
            lat=24.7136,
            lon=46.6753,
        )
        result = resolve_location(rec, db=None)
        assert result.geocode_method == GeocodeMethod.PLATFORM_PAYLOAD
        assert result.location_confidence == 0.9

    def test_tier_d_district_centroid(self):
        rec = DeliveryRecord(
            platform="hungerstation",
            restaurant_name_raw="Some Restaurant",
            district_text="Olaya",
        )
        result = resolve_location(rec, db=None)
        assert result.lat is not None
        assert result.lon is not None
        assert result.geocode_method == GeocodeMethod.DISTRICT_CENTROID
        assert result.location_confidence == 0.3

    def test_no_location_still_preserved(self):
        """Records with no location data should still be returned."""
        rec = DeliveryRecord(
            platform="hungerstation",
            restaurant_name_raw="Unknown Place",
        )
        result = resolve_location(rec, db=None)
        assert result is not None
        assert result.lat is None
        assert result.location_confidence == 0.0

    def test_district_centroids_are_in_riyadh(self):
        """All district centroids must be valid Riyadh coordinates."""
        for name, (lat, lon) in RIYADH_DISTRICT_CENTROIDS.items():
            assert _is_valid_riyadh_coords(lat, lon), (
                f"District {name} has invalid coords: ({lat}, {lon})"
            )

    def test_normalize_district_name(self):
        assert _normalize_district_name("Al-Olaya") == "al olaya"
        assert _normalize_district_name("al_malaz") == "al malaz"
        assert _normalize_district_name(None) is None


# ============================================================================
# Pipeline tests
# ============================================================================


class TestPipelineHelpers:
    def test_normalize_name(self):
        assert _normalize_name("Burger King - Delivery") == "Burger King"
        assert _normalize_name("  Test  ") == "Test"
        assert _normalize_name("مطعم البيت - توصيل") == "مطعم البيت"
        assert _normalize_name(None) is None
        assert _normalize_name("") is None
        # "Restaurant" suffix is stripped
        assert _normalize_name("Olaya Restaurant") == "Olaya"

    def test_record_to_row(self):
        rec = DeliveryRecord(
            platform="hungerstation",
            restaurant_name_raw="Test Burger",
            source_url="https://example.com",
            lat=24.7136,
            lon=46.6753,
            cuisine_raw="burger",
            rating=4.5,
            delivery_time_min=30,
        )
        row = record_to_row(rec, run_id=1)
        assert row.platform == "hungerstation"
        assert row.restaurant_name_raw == "Test Burger"
        assert row.ingest_run_id == 1
        assert row.category_raw == "burger"  # normalized


class TestResolverNormalization:
    def test_normalize_for_match(self):
        assert resolver_normalize("Burger King") == "burger king"
        # Punctuation replaced with space, then collapsed
        assert resolver_normalize("Al-Baik (Olaya)") == "al baik olaya"
        assert resolver_normalize(None) == ""
        assert resolver_normalize("") == ""


# ============================================================================
# HTML parser tests
# ============================================================================


class TestPageContentParser:
    def test_json_ld_restaurant(self):
        html = """
        <html>
        <head>
        <script type="application/ld+json">
        {
            "@type": "Restaurant",
            "name": "Test Restaurant",
            "servesCuisine": "Italian",
            "address": {"streetAddress": "123 Main St", "addressLocality": "Olaya"},
            "geo": {"latitude": "24.7136", "longitude": "46.6753"},
            "aggregateRating": {"ratingValue": "4.5", "reviewCount": "120"},
            "telephone": "+966111234567"
        }
        </script>
        </head>
        <body></body>
        </html>
        """
        rec = parse_page_content(html, "https://example.com/restaurant/test", "hungerstation")
        assert rec is not None
        assert rec.restaurant_name_raw == "Test Restaurant"
        assert rec.cuisine_raw == "Italian"
        assert rec.lat == 24.7136
        assert rec.lon == 46.6753
        assert rec.rating == 4.5
        assert rec.rating_count == 120
        assert rec.phone_raw == "+966111234567"
        assert rec.geocode_method == GeocodeMethod.JSON_LD

    def test_og_title_fallback(self):
        html = """
        <html>
        <head>
        <meta property="og:title" content="Al Baik Riyadh" />
        </head>
        <body></body>
        </html>
        """
        rec = parse_page_content(html, "https://example.com/riyadh/olaya/test", "talabat")
        assert rec is not None
        assert rec.restaurant_name_raw == "Al Baik Riyadh"
        assert rec.brand_raw == "Al Baik"

    def test_no_data_returns_none(self):
        html = "<html><body>Nothing useful here</body></html>"
        rec = parse_page_content(html, "https://example.com/about", "talabat")
        assert rec is None


# ============================================================================
# Regression tests
# ============================================================================


class TestRegressions:
    def test_missing_lat_lon_not_discarded(self):
        """Records without coordinates must NOT be silently discarded."""
        raw = {
            "id": "keeta:no-location",
            "name": "Cloud Kitchen Riyadh",
            "source": "keeta",
            "source_url": "https://keeta.com/riyadh/cloud-kitchen",
            "lat": None,
            "lon": None,
            "category_raw": "chicken",
        }
        rec = parse_legacy_record(raw, "keeta")
        assert rec is not None
        assert rec.restaurant_name_raw == "Cloud Kitchen Riyadh"
        assert rec.cuisine_raw == "chicken"

        # The record should be convertible to a DB row
        row = record_to_row(rec, run_id=1)
        assert row.restaurant_name_raw == "Cloud Kitchen Riyadh"
        assert row.lat is None
        assert row.lon is None

    def test_approximate_area_rows_survive(self):
        """District-only records should get centroid coords and survive."""
        rec = DeliveryRecord(
            platform="mrsool",
            restaurant_name_raw="Local Shawarma",
            district_text="Olaya",
        )
        result = resolve_location(rec, db=None)
        assert result.lat is not None
        assert result.lon is not None
        assert result.location_confidence == 0.3
        assert result.geocode_method == GeocodeMethod.DISTRICT_CENTROID

        # Should be storable
        row = record_to_row(result, run_id=1)
        assert row.lat is not None
        assert row.location_confidence == 0.3

    def test_low_confidence_marked_correctly(self):
        """District-centroid records must have low confidence."""
        rec = DeliveryRecord(
            platform="hungerstation",
            restaurant_name_raw="Test",
            district_text="Malaz",
        )
        result = resolve_location(rec, db=None)
        assert result.location_confidence < 0.5
        assert result.geocode_method == GeocodeMethod.DISTRICT_CENTROID

    def test_platform_enum_values(self):
        """All platforms in the enum must be valid."""
        platforms = [p.value for p in Platform]
        assert "hungerstation" in platforms
        assert "jahez" in platforms
        assert "keeta" in platforms
        assert "talabat" in platforms
        assert "mrsool" in platforms
        assert "toyou" in platforms


# ============================================================================
# Integration-style tests (using fixtures, no real DB)
# ============================================================================


class TestIntegrationFixtures:
    """Test with representative platform payload fixtures."""

    def test_hungerstation_sitemap_record(self):
        """Simulate a HungerStation sitemap-scraped record."""
        raw = {
            "id": "hungerstation:al-baik-olaya-riyadh",
            "name": "Al Baik Olaya Riyadh",
            "source": "hungerstation",
            "source_url": "https://hungerstation.com/riyadh/al-baik-olaya-riyadh",
            "lat": None,
            "lon": None,
            "category_raw": None,
        }
        rec = parse_legacy_record(raw, "hungerstation")

        assert rec.brand_raw == "Al Baik"
        assert rec.district_text is not None
        assert "olaya" in rec.district_text.lower()

        # Location resolve should give district centroid
        resolved = resolve_location(rec, db=None)
        assert resolved.lat is not None
        assert resolved.lon is not None
        assert resolved.geocode_method == GeocodeMethod.DISTRICT_CENTROID

    def test_talabat_record_with_coords(self):
        """Simulate a Talabat record that has coordinates."""
        raw = {
            "id": "talabat:pizza-hut-malaz",
            "name": "Pizza Hut - Al Malaz",
            "source": "talabat",
            "source_url": "https://talabat.com/saudi-arabia/riyadh/pizza-hut-malaz",
            "lat": 24.6651,
            "lon": 46.7218,
            "category_raw": "pizza",
        }
        rec = parse_legacy_record(raw, "talabat")

        assert rec.brand_raw == "Pizza Hut"
        assert rec.branch_raw == "Al Malaz"
        assert rec.lat == 24.6651
        assert rec.lon == 46.7218
        assert rec.location_confidence == 0.9

        # Should not change coords during resolve
        resolved = resolve_location(rec, db=None)
        assert resolved.lat == 24.6651
        assert resolved.lon == 46.7218
