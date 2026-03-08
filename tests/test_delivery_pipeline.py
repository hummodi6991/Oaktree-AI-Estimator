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
from app.delivery.models import DeliveryIngestRun
from app.delivery.pipeline import _normalize_name, record_to_row
from app.delivery.resolver import _normalize_for_match as resolver_normalize
from app.delivery.location import _normalize_name_for_sql


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


# ============================================================================
# Name normalization consistency tests
# ============================================================================


class TestNameNormalizationConsistency:
    """Verify Python and SQL-side normalization produce the same results."""

    def test_punctuation_stripped_consistently(self):
        """Al-Baik (Olaya) must normalize identically in Python and SQL paths."""
        py_result = resolver_normalize("Al-Baik (Olaya)")
        sql_result = _normalize_name_for_sql("Al-Baik (Olaya)")
        assert py_result == sql_result == "al baik olaya"

    def test_arabic_preserved(self):
        """Arabic characters must survive normalization."""
        py_result = resolver_normalize("البيك - الربوة")
        sql_result = _normalize_name_for_sql("البيك - الربوة")
        assert py_result == sql_result
        assert "البيك" in py_result
        assert "الربوة" in py_result

    def test_apostrophe_stripped(self):
        """Apostrophes (McDonald's) must be normalized consistently.
        The apostrophe becomes a space, then gets collapsed."""
        py_result = resolver_normalize("McDonald's Riyadh")
        sql_result = _normalize_name_for_sql("McDonald's Riyadh")
        assert py_result == sql_result == "mcdonald s riyadh"

    def test_empty_and_none(self):
        assert resolver_normalize(None) == ""
        assert resolver_normalize("") == ""
        assert _normalize_name_for_sql("") == ""


# ============================================================================
# Confidence gating tests
# ============================================================================


class TestConfidenceGating:
    """Verify that low-precision rows are properly gated."""

    def test_district_centroid_below_parcel_threshold(self):
        """District centroid records (0.3) must not pass the 0.7 parcel gate."""
        from app.delivery.features import PARCEL_MIN_CONFIDENCE

        rec = DeliveryRecord(
            platform="hungerstation",
            restaurant_name_raw="Test",
            district_text="Olaya",
        )
        resolved = resolve_location(rec, db=None)
        assert resolved.location_confidence < PARCEL_MIN_CONFIDENCE
        assert resolved.geocode_method == GeocodeMethod.DISTRICT_CENTROID

    def test_platform_payload_above_parcel_threshold(self):
        """Direct platform coordinates (0.9) must pass the 0.7 parcel gate."""
        from app.delivery.features import PARCEL_MIN_CONFIDENCE

        rec = DeliveryRecord(
            platform="hungerstation",
            lat=24.7136,
            lon=46.6753,
        )
        resolved = resolve_location(rec, db=None)
        assert resolved.location_confidence >= PARCEL_MIN_CONFIDENCE
        assert resolved.geocode_method == GeocodeMethod.PLATFORM_PAYLOAD

    def test_poi_match_excluded_from_first_party_methods(self):
        """POI_MATCH coordinates are borrowed and not first-party."""
        first_party = {"platform_payload", "json_ld", "address_geocode"}
        assert GeocodeMethod.POI_MATCH.value not in first_party
        assert GeocodeMethod.DISTRICT_CENTROID.value not in first_party


# ============================================================================
# Production robustness tests
# ============================================================================


class TestScraperTransportErrorRollback:
    """Scraper transport errors must cause a rollback, not poison the session."""

    def test_scraper_exception_triggers_rollback(self):
        """When a scraper raises a transport error mid-stream, the pipeline
        must rollback the session and mark the run as failed."""
        from app.delivery.pipeline import run_platform_scrape

        def _exploding_scraper(max_pages=200):
            yield {
                "id": "test:ok-record",
                "name": "Good Record",
                "source": "testplatform",
                "source_url": "https://example.com/ok",
                "lat": None,
                "lon": None,
                "category_raw": None,
            }
            raise ConnectionError(
                "peer closed connection without sending complete message body"
            )

        mock_registry = {
            "testplatform": {
                "fn": _exploding_scraper,
                "source": "testplatform",
                "label": "Test",
                "url": "https://example.com",
            }
        }

        mock_db = MagicMock()
        mock_db.query.return_value.filter_by.return_value.first.return_value = None

        # Make flush assign an id to any DeliveryIngestRun added
        added_objects = []
        def fake_add(obj):
            added_objects.append(obj)
            if isinstance(obj, DeliveryIngestRun) and obj.id is None:
                obj.id = 99
        mock_db.add.side_effect = fake_add
        mock_db.flush.return_value = None

        # For _safe_finalize_run's session.get()
        run_obj = DeliveryIngestRun(id=99, platform="testplatform", status="running")
        mock_db.get.return_value = run_obj

        with patch("app.connectors.delivery_platforms.SCRAPER_REGISTRY", mock_registry):
            result = run_platform_scrape(
                mock_db, "testplatform", max_pages=5, run_resolver=False
            )

        # Session must have been rolled back
        assert mock_db.rollback.called
        # Result must report the error
        assert any("scrape" in e.get("phase", "") for e in result.get("errors", []))

    def test_transport_error_does_not_raise(self):
        """The pipeline must catch transport errors and not propagate them."""
        from app.delivery.pipeline import run_platform_scrape

        def _exploding_scraper(max_pages=200):
            raise ConnectionError("incomplete chunked read")

        mock_registry = {
            "boom": {
                "fn": _exploding_scraper,
                "source": "boom",
                "label": "Boom",
                "url": "https://example.com",
            }
        }

        mock_db = MagicMock()
        def fake_add(obj):
            if isinstance(obj, DeliveryIngestRun) and obj.id is None:
                obj.id = 1
        mock_db.add.side_effect = fake_add
        mock_db.flush.return_value = None
        run = DeliveryIngestRun(id=1, platform="boom", status="running")
        mock_db.get.return_value = run

        with patch("app.connectors.delivery_platforms.SCRAPER_REGISTRY", mock_registry):
            # Must not raise
            result = run_platform_scrape(
                mock_db, "boom", max_pages=1, run_resolver=False
            )

        assert "errors" in result
        assert len(result["errors"]) > 0


class TestResolverDBExceptionRollback:
    """Resolver DB exceptions must be caught and rolled back cleanly."""

    def test_resolver_failure_does_not_crash_pipeline(self):
        """If the resolver raises (e.g. AdminShutdown), the pipeline must
        rollback and continue — not propagate PendingRollbackError."""
        from app.delivery.pipeline import run_platform_scrape

        def _ok_scraper(max_pages=200):
            # yields nothing — just want to test resolver path
            return iter([])

        mock_registry = {
            "safe": {
                "fn": _ok_scraper,
                "source": "safe",
                "label": "Safe",
                "url": "https://example.com",
            }
        }

        mock_db = MagicMock()
        def fake_add(obj):
            if isinstance(obj, DeliveryIngestRun) and obj.id is None:
                obj.id = 42
        mock_db.add.side_effect = fake_add
        mock_db.flush.return_value = None
        run = DeliveryIngestRun(id=42, platform="safe", status="running")
        mock_db.get.return_value = run

        def _resolver_boom(db, run_id):
            raise Exception("terminating connection due to administrator command")

        with patch("app.connectors.delivery_platforms.SCRAPER_REGISTRY", mock_registry):
            with patch("app.delivery.resolver.resolve_run", _resolver_boom):
                result = run_platform_scrape(
                    mock_db, "safe", max_pages=1, run_resolver=True
                )

        # Rollback must have been called after resolver failure
        assert mock_db.rollback.called
        assert any(
            "resolve" in e.get("phase", "") for e in result.get("errors", [])
        )


def _mock_session_factory():
    """Create a mock session factory that returns mock sessions with
    proper run objects for pipeline testing."""
    sessions = []

    def factory():
        s = MagicMock()
        s.flush.return_value = None
        next_id = len(sessions) + 1

        def fake_add(obj):
            if isinstance(obj, DeliveryIngestRun) and obj.id is None:
                obj.id = next_id
        s.add.side_effect = fake_add

        run = DeliveryIngestRun(
            id=next_id,
            platform="test",
            status="running",
        )
        s.get.return_value = run
        sessions.append(s)
        return s

    return factory, sessions


def _patch_session_local(mock_factory):
    """Patch SessionLocal by injecting a mock module into sys.modules."""
    import sys
    import types
    mock_mod = types.ModuleType("app.db.session")
    mock_mod.SessionLocal = mock_factory
    return patch.dict(sys.modules, {"app.db.session": mock_mod})


class TestPlatformIsolation:
    """Failure in one platform must not poison subsequent platforms."""

    def test_one_platform_failure_does_not_block_next(self):
        """When platform A fails, platform B must still run successfully
        (each gets its own session)."""
        from app.delivery.pipeline import run_all_platforms

        call_log = []

        def _failing_scraper(max_pages=200):
            call_log.append("fail_called")
            raise ConnectionError("boom")

        def _ok_scraper(max_pages=200):
            call_log.append("ok_called")
            return iter([])

        mock_registry = {
            "platform_a": {
                "fn": _failing_scraper,
                "source": "platform_a",
                "label": "A",
                "url": "https://a.com",
            },
            "platform_b": {
                "fn": _ok_scraper,
                "source": "platform_b",
                "label": "B",
                "url": "https://b.com",
            },
        }

        factory, mock_sessions = _mock_session_factory()

        with patch("app.connectors.delivery_platforms.SCRAPER_REGISTRY", mock_registry):
            with _patch_session_local(factory):
                results = run_all_platforms(
                    platforms=["platform_a", "platform_b"],
                    run_resolver=False,
                )

        # Both platforms must have been attempted
        assert "fail_called" in call_log
        assert "ok_called" in call_log

        # Each platform got its own session
        assert len(mock_sessions) >= 2

        # Results must contain entries for both platforms
        assert len(results) == 2

    def test_fresh_session_per_platform(self):
        """Verify that run_all_platforms creates a separate session for each
        platform, not reusing a single shared session."""
        from app.delivery.pipeline import run_all_platforms

        def _noop_scraper(max_pages=200):
            return iter([])

        mock_registry = {
            "p1": {"fn": _noop_scraper, "source": "p1", "label": "P1", "url": "https://p1.com"},
            "p2": {"fn": _noop_scraper, "source": "p2", "label": "P2", "url": "https://p2.com"},
        }

        factory, mock_sessions = _mock_session_factory()

        with patch("app.connectors.delivery_platforms.SCRAPER_REGISTRY", mock_registry):
            with _patch_session_local(factory):
                run_all_platforms(
                    platforms=["p1", "p2"],
                    run_resolver=False,
                )

        # Two distinct sessions must have been created
        assert len(mock_sessions) >= 2
        assert mock_sessions[0] is not mock_sessions[1]


class TestPlatformFiltering:
    """Platform filtering from CLI/workflow must restrict execution."""

    def test_cli_platform_comma_separated(self):
        """--platform hungerstation,jahez should produce a two-element list."""
        platform_str = "hungerstation,jahez"
        platform_list = [p.strip() for p in platform_str.split(",") if p.strip()]
        assert platform_list == ["hungerstation", "jahez"]

    def test_cli_single_platform(self):
        """--platform hungerstation should produce a one-element list."""
        platform_str = "hungerstation"
        platform_list = [p.strip() for p in platform_str.split(",") if p.strip()]
        assert platform_list == ["hungerstation"]

    def test_run_all_platforms_respects_filter(self):
        """run_all_platforms(platforms=[...]) must only run those platforms."""
        from app.delivery.pipeline import run_all_platforms

        called_platforms = []

        def _tracking_scraper(platform_name):
            def _scraper(max_pages=200):
                called_platforms.append(platform_name)
                return iter([])
            return _scraper

        mock_registry = {
            "alpha": {"fn": _tracking_scraper("alpha"), "source": "alpha", "label": "A", "url": "https://a.com"},
            "beta": {"fn": _tracking_scraper("beta"), "source": "beta", "label": "B", "url": "https://b.com"},
            "gamma": {"fn": _tracking_scraper("gamma"), "source": "gamma", "label": "G", "url": "https://g.com"},
        }

        factory, _ = _mock_session_factory()

        with patch("app.connectors.delivery_platforms.SCRAPER_REGISTRY", mock_registry):
            with _patch_session_local(factory):
                run_all_platforms(
                    platforms=["alpha", "gamma"],
                    run_resolver=False,
                )

        assert "alpha" in called_platforms
        assert "gamma" in called_platforms
        assert "beta" not in called_platforms
