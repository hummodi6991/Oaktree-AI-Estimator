"""Tests for the Bayut scraper — parser, type-mapping, price conversion,
upsert shape, and classifier portability.

PR4 of the multi-portal listings series. The parser reads a structured
JSON blob embedded in Bayut's detail-page HTML (Next.js
``__NEXT_DATA__``) and projects fields onto a ``BayutDetailPayload``
dataclass; the upsert layer mirrors Aqar's SELECT-then-(INSERT or
UPDATE) shape but sets ``platform='bayut'`` and prefixes the primary
key as ``aqar_id='bayut:<id>'``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest


FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE_HTML = (FIXTURES / "bayut_sample_detail.html").read_text(encoding="utf-8")
SAMPLE_URL = "https://www.bayut.sa/en/property/details-87990074.html"


def _load_next_data(html: str) -> dict[str, Any]:
    """Return the parsed __NEXT_DATA__ JSON from the fixture."""
    start = html.index('<script id="__NEXT_DATA__"')
    open_brace = html.index("{", start)
    end = html.index("</script>", open_brace)
    return json.loads(html[open_brace:end])


def _swap_property_type(html: str, new_en: str) -> str:
    data = _load_next_data(html)
    data["props"]["pageProps"]["property"]["propertyType"]["en"] = new_en
    return _rewrite_next_data(html, data)


def _swap_rent_frequency(html: str, new_frequency: str) -> str:
    data = _load_next_data(html)
    data["props"]["pageProps"]["property"]["rentFrequency"] = new_frequency
    return _rewrite_next_data(html, data)


def _rewrite_next_data(html: str, data: dict[str, Any]) -> str:
    prefix = '<script id="__NEXT_DATA__" type="application/json">'
    suffix = "</script>"
    start = html.index(prefix) + len(prefix)
    end = html.index(suffix, start)
    return html[:start] + "\n" + json.dumps(data) + "\n" + html[end:]


class TestBayutDetailParser:
    """End-to-end parse of the fixture detail page."""

    def setup_method(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        self.payload = parse_detail_html(SAMPLE_HTML, SAMPLE_URL)

    def test_payload_is_not_none(self) -> None:
        assert self.payload is not None

    def test_platform_listing_id(self) -> None:
        assert self.payload.platform_listing_id == "87990074"

    def test_title_is_non_empty(self) -> None:
        assert self.payload.title
        assert "Olaya" in self.payload.title

    def test_description_is_non_empty(self) -> None:
        assert self.payload.description

    def test_image_url(self) -> None:
        assert self.payload.image_url
        assert self.payload.image_url.startswith("https://")

    def test_lat_lon_in_riyadh(self) -> None:
        assert self.payload.lat == pytest.approx(24.6919, abs=0.0005)
        assert self.payload.lon == pytest.approx(46.6856, abs=0.0005)
        # Sanity: roughly Riyadh.
        assert 24.4 <= self.payload.lat <= 25.1
        assert 46.4 <= self.payload.lon <= 47.0

    def test_area_sqm_positive(self) -> None:
        assert self.payload.area_sqm is not None
        assert self.payload.area_sqm > 0
        # Fixture explicitly carries area=180.0 m².
        assert self.payload.area_sqm == Decimal("180.00")

    def test_price_sar_annual_is_monthly_times_twelve(self) -> None:
        # Fixture price=4990 monthly → 59880 annual.
        assert self.payload.price_sar_annual == Decimal("59880.00")
        # Sanity: annual range should be in the thousands-to-millions.
        assert 10_000 <= float(self.payload.price_sar_annual) <= 50_000_000

    def test_rega_advertisement_license_is_11_digit_numeric(self) -> None:
        assert self.payload.aqar_advertisement_license is not None
        assert self.payload.aqar_advertisement_license.isdigit()
        assert len(self.payload.aqar_advertisement_license) == 11

    def test_aqar_listing_source_is_bayut(self) -> None:
        assert self.payload.aqar_listing_source == "Bayut"

    def test_contact_phone_non_empty(self) -> None:
        assert self.payload.contact_phone is not None
        assert self.payload.contact_phone.startswith("+966")

    def test_listing_type_is_store(self) -> None:
        assert self.payload.listing_type == "store"

    def test_property_type_constant(self) -> None:
        assert self.payload.property_type == "Commercial"

    def test_aqar_detail_scraped_at(self) -> None:
        assert isinstance(self.payload.aqar_detail_scraped_at, datetime)

    def test_aqar_created_at_parsed(self) -> None:
        assert self.payload.aqar_created_at is not None
        assert self.payload.aqar_created_at.year == 2026

    def test_aqar_updated_at_parsed(self) -> None:
        assert self.payload.aqar_updated_at is not None
        assert self.payload.aqar_updated_at.year == 2026


class TestPropertyTypeFiltering:
    """Reject residential listings that leak into commercial-rent pages."""

    def test_apartment_returns_none(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _swap_property_type(SAMPLE_HTML, "Apartment")
        assert parse_detail_html(html, SAMPLE_URL) is None

    def test_villa_returns_none(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _swap_property_type(SAMPLE_HTML, "Villa")
        assert parse_detail_html(html, SAMPLE_URL) is None

    def test_office_returns_none(self) -> None:
        # Office is structurally commercial but not in the accepted set
        # (shops + showrooms only for v1 per the investigation report).
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _swap_property_type(SAMPLE_HTML, "Office")
        assert parse_detail_html(html, SAMPLE_URL) is None

    def test_shop_returns_valid_payload(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _swap_property_type(SAMPLE_HTML, "Shop")
        payload = parse_detail_html(html, SAMPLE_URL)
        assert payload is not None
        assert payload.listing_type == "store"

    def test_showroom_returns_valid_payload(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _swap_property_type(SAMPLE_HTML, "Showroom")
        payload = parse_detail_html(html, SAMPLE_URL)
        assert payload is not None
        assert payload.listing_type == "showroom"

    def test_retail_shop_returns_valid_payload(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _swap_property_type(SAMPLE_HTML, "Retail Shop")
        payload = parse_detail_html(html, SAMPLE_URL)
        assert payload is not None
        assert payload.listing_type == "store"


class TestListingTypeMapping:
    """Property-type → listing_type enum mapping."""

    def test_shop_maps_to_store(self) -> None:
        from app.ingest.bayut.detail_scraper import _map_bayut_listing_type

        assert _map_bayut_listing_type("Shop") == "store"

    def test_retail_shop_maps_to_store(self) -> None:
        from app.ingest.bayut.detail_scraper import _map_bayut_listing_type

        assert _map_bayut_listing_type("Retail Shop") == "store"

    def test_commercial_shop_maps_to_store(self) -> None:
        from app.ingest.bayut.detail_scraper import _map_bayut_listing_type

        assert _map_bayut_listing_type("Commercial Shop") == "store"

    def test_showroom_maps_to_showroom(self) -> None:
        from app.ingest.bayut.detail_scraper import _map_bayut_listing_type

        assert _map_bayut_listing_type("Showroom") == "showroom"

    def test_apartment_maps_to_none(self) -> None:
        from app.ingest.bayut.detail_scraper import _map_bayut_listing_type

        assert _map_bayut_listing_type("Apartment") is None

    def test_office_maps_to_none(self) -> None:
        from app.ingest.bayut.detail_scraper import _map_bayut_listing_type

        assert _map_bayut_listing_type("Office") is None

    def test_none_input_maps_to_none(self) -> None:
        from app.ingest.bayut.detail_scraper import _map_bayut_listing_type

        assert _map_bayut_listing_type(None) is None
        assert _map_bayut_listing_type("") is None


class TestPriceConversion:
    """Monthly → annual conversion + defensive frequency check."""

    def test_monthly_price_4990_becomes_annual_59880(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        payload = parse_detail_html(SAMPLE_HTML, SAMPLE_URL)
        assert payload is not None
        assert payload.price_sar_annual == Decimal("59880.00")

    def test_yearly_rent_frequency_returns_none(self) -> None:
        # Defensive skip: if Bayut ever publishes annual listings, we don't
        # silently underprice by 12× — the parser logs and returns None.
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _swap_rent_frequency(SAMPLE_HTML, "yearly")
        assert parse_detail_html(html, SAMPLE_URL) is None

    def test_missing_rent_frequency_returns_none(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        # Drop rentFrequency entirely.
        data = _load_next_data(SAMPLE_HTML)
        del data["props"]["pageProps"]["property"]["rentFrequency"]
        html = _rewrite_next_data(SAMPLE_HTML, data)
        assert parse_detail_html(html, SAMPLE_URL) is None

    def test_daily_rent_frequency_returns_none(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _swap_rent_frequency(SAMPLE_HTML, "daily")
        assert parse_detail_html(html, SAMPLE_URL) is None


class _FakeResult:
    def __init__(self, scalar_value: Any = None, rowcount: int = 0) -> None:
        self._scalar = scalar_value
        self.rowcount = rowcount

    def scalar(self) -> Any:
        return self._scalar

    def first(self) -> Any:
        return self._scalar


class _FakeDB:
    """Records ``execute`` calls and returns canned results in order."""

    def __init__(self, results: list[_FakeResult]) -> None:
        self._results = list(results)
        self.calls: list[tuple[str, dict]] = []

    def execute(self, stmt, params=None):
        self.calls.append((str(stmt), dict(params or {})))
        return self._results.pop(0)

    def commit(self) -> None:
        pass


_BASE_BAYUT_LISTING: dict[str, Any] = {
    "aqar_id": "bayut:87990074",
    "platform": "bayut",
    "platform_listing_id": "87990074",
    "title": "Shop for rent in Al Olaya, Riyadh",
    "description": "Ground-floor commercial shop.",
    "neighborhood": "Al Olaya",
    "listing_url": SAMPLE_URL,
    "image_url": "https://images.bayut.sa/thumbnails/87990074-800x600.webp",
    "price_sar_annual": Decimal("59880.00"),
    "area_sqm": Decimal("180.00"),
    "lat": 24.6919,
    "lon": 46.6856,
    "contact_phone": "+966XXXXXXXXX",
    "listing_type": "store",
    "property_type": "Commercial",
    "aqar_advertisement_license": "12000037280",
    "aqar_listing_source": "Bayut",
    "restaurant_score": 25,
    "restaurant_suitable": True,
    "restaurant_signals": [],
    # NULL on Bayut rows.
    "street_width_m": None,
    "num_floors": None,
    "has_mezzanine": None,
    "has_drive_thru": None,
    "facade_direction": None,
    "is_furnished": None,
    "apartments_count": None,
    "num_rooms": None,
}


class TestUpsertBayutListing:
    """Insert and update branches set platform / prefixed PK correctly."""

    def test_insert_branch_sets_bayut_platform_columns(self) -> None:
        from scripts.scrape_bayut import upsert_bayut_listing

        select_result = _FakeResult(scalar_value=None)
        select_result.first = lambda: None  # type: ignore[assignment]
        db = _FakeDB([select_result, _FakeResult(rowcount=1)])

        action = upsert_bayut_listing(db, dict(_BASE_BAYUT_LISTING))

        assert action == "insert"
        assert len(db.calls) == 2
        insert_sql, insert_params = db.calls[1]
        assert "INSERT INTO commercial_unit" in insert_sql
        assert ":platform" in insert_sql
        assert ":platform_listing_id" in insert_sql
        assert insert_params["platform"] == "bayut"
        assert insert_params["platform_listing_id"] == "87990074"
        assert insert_params["aqar_id"] == "bayut:87990074"
        assert insert_params["aqar_listing_source"] == "Bayut"

    def test_update_branch_sets_bayut_platform_columns(self) -> None:
        from scripts.scrape_bayut import upsert_bayut_listing

        select_result = _FakeResult(scalar_value=("bayut:87990074",))
        select_result.first = lambda: ("bayut:87990074",)  # type: ignore[assignment]
        db = _FakeDB([select_result, _FakeResult(rowcount=1)])

        action = upsert_bayut_listing(db, dict(_BASE_BAYUT_LISTING))

        assert action == "update"
        assert len(db.calls) == 2
        update_sql, update_params = db.calls[1]
        assert "UPDATE commercial_unit SET" in update_sql
        assert "platform = :platform" in update_sql
        assert "platform_listing_id = :platform_listing_id" in update_sql
        assert update_params["platform"] == "bayut"
        assert update_params["platform_listing_id"] == "87990074"
        assert update_params["aqar_id"] == "bayut:87990074"

    def test_listing_params_prefixes_aqar_id(self) -> None:
        from scripts.scrape_bayut import _bayut_listing_params

        params = _bayut_listing_params({
            "platform_listing_id": "12345678",
            "title": "test",
        })
        assert params["platform"] == "bayut"
        assert params["platform_listing_id"] == "12345678"
        assert params["aqar_id"] == "bayut:12345678"
        assert params["aqar_listing_source"] == "Bayut"

    def test_listing_params_carries_rega_license(self) -> None:
        from scripts.scrape_bayut import _bayut_listing_params

        params = _bayut_listing_params({
            "platform_listing_id": "12345678",
            "aqar_advertisement_license": "12000037280",
        })
        assert params["aqar_advertisement_license"] == "12000037280"


class TestSuitabilityClassifierPortability:
    """Bayut listings (lacking street_width_m / has_drive_thru) must
    still flow cleanly through the existing classifier."""

    def test_classifier_handles_bayut_shape_dict(self) -> None:
        from scripts.scrape_aqar import classify_restaurant_suitability

        listing: dict[str, Any] = {
            "title": "Shop for rent in Al Olaya, Riyadh",
            "description": "Ground-floor commercial shop with wide street frontage.",
            "listing_type": "store",
            "area_sqm": 180.0,
            # No street_width_m, no has_drive_thru — Bayut doesn't expose them.
        }

        result = classify_restaurant_suitability(listing)

        assert "restaurant_suitable" in result
        assert isinstance(result["restaurant_suitable"], bool)
        assert "restaurant_score" in result
        assert isinstance(result["restaurant_score"], int)
        # Bayut shop with no drive-thru/mezzanine is still a valid candidate
        # — the structural F&B gate accepts listing_type=store.
        assert result["restaurant_suitable"] is True

    def test_classifier_rejects_residential_keywords(self) -> None:
        from scripts.scrape_aqar import classify_restaurant_suitability

        # Even if a listing slips through as listing_type=store, residential
        # keywords in the title/description must still mark it unsuitable.
        listing: dict[str, Any] = {
            "title": "Apartment for rent in Riyadh",
            "description": "Furnished studio apartment.",
            "listing_type": "store",
            "area_sqm": 80.0,
        }

        result = classify_restaurant_suitability(listing)

        assert result["restaurant_suitable"] is False
