"""Tests for the Bayut scraper — parser, type-mapping, price conversion,
upsert shape, and classifier portability.

PR4 of the multi-portal listings series. The parser reads a two-payload
mix from Bayut's detail-page HTML (JSON-LD ``@graph[RealEstateListing]``
+ a Nuxt-style ``window.state`` blob at ``state.property.data``) and
projects fields onto a ``BayutDetailPayload`` dataclass; the upsert
layer mirrors Aqar's SELECT-then-(INSERT or UPDATE) shape but sets
``platform='bayut'`` and prefixes the primary key as
``aqar_id='bayut:<id>'``.

The two committed fixtures are real Bayut captures with PII redacted:
``bayut_real_showroom.html`` (listing 87825483 — yearly, 120 000 SAR/yr,
883 m²), which must parse, and ``bayut_real_office.html`` (listing
87772614 — monthly Office), which must now be **rejected**. Office was
dropped from the Bayut accept-set because its inventory is
F&B-unsuitable; the office fixture is retained as the regression guard
proving the rejection.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest


FIXTURES = Path(__file__).parent / "fixtures"
SHOWROOM_HTML = (FIXTURES / "bayut_real_showroom.html").read_text(encoding="utf-8")
OFFICE_HTML = (FIXTURES / "bayut_real_office.html").read_text(encoding="utf-8")

SHOWROOM_URL = "https://www.bayut.sa/en/property/details-87825483.html"
OFFICE_URL = "https://www.bayut.sa/en/property/details-87772614.html"


# ---------------------------------------------------------------------------
# Fixture-mutation helpers — operate on the JSON-LD + window.state shapes
# the parser actually reads from real Bayut, not the v1 synthetic
# __NEXT_DATA__.
# ---------------------------------------------------------------------------


_LD_TAG_RE = re.compile(
    r'(<script[^>]*type="application/ld\+json"[^>]*>)(.*?)(</script>)',
    re.DOTALL,
)
_WINDOW_STATE_RE = re.compile(r"window\.state\s*=\s*")


def _parse_jsonld(html: str) -> dict[str, Any]:
    match = _LD_TAG_RE.search(html)
    assert match, "fixture must contain a JSON-LD <script> block"
    return json.loads(match.group(2).strip())


def _rewrite_jsonld(html: str, data: dict[str, Any]) -> str:
    match = _LD_TAG_RE.search(html)
    assert match
    new_body = json.dumps(data)
    return html[: match.start(2)] + new_body + html[match.end(2):]


def _parse_state(html: str) -> dict[str, Any]:
    m = _WINDOW_STATE_RE.search(html)
    assert m, "fixture must contain a window.state assignment"
    decoder = json.JSONDecoder()
    state, end_offset = decoder.raw_decode(html, m.end())
    return state, m.end(), end_offset


def _rewrite_state(html: str, state: dict[str, Any]) -> str:
    _, start, end = _parse_state(html)
    return html[:start] + json.dumps(state) + html[end:]


def _swap_accommodation_category(html: str, new_value: str) -> str:
    """Set both JSON-LD mainEntity.accommodationCategory and
    window.state.property.data.category[1].nameSingular to ``new_value``."""
    ld = _parse_jsonld(html)
    listing = ld["@graph"][0]
    listing["mainEntity"]["accommodationCategory"] = new_value
    html = _rewrite_jsonld(html, ld)

    state, _, _ = _parse_state(html)
    state["property"]["data"]["category"][1]["nameSingular"] = new_value
    state["property"]["data"]["category"][1]["name"] = new_value
    return _rewrite_state(html, state)


def _set_rent_frequency(html: str, new_value: str | None) -> str:
    """Mutate ``window.state.property.data.rentFrequency``. ``None``
    deletes the key."""
    state, _, _ = _parse_state(html)
    if new_value is None:
        state["property"]["data"].pop("rentFrequency", None)
    else:
        state["property"]["data"]["rentFrequency"] = new_value
    html = _rewrite_state(html, state)
    # Also strip the JSON-LD priceSpecification.unitText fallback so the
    # rentFrequency branch genuinely tests the missing path.
    if new_value is None:
        ld = _parse_jsonld(html)
        listing = ld["@graph"][0]
        spec = listing.get("mainEntity", {}).get("priceSpecification")
        if isinstance(spec, dict) and "unitText" in spec:
            spec.pop("unitText", None)
        html = _rewrite_jsonld(html, ld)
    return html


# ---------------------------------------------------------------------------
# TestRealShowroomFixture
# ---------------------------------------------------------------------------


class TestRealShowroomFixture:
    """End-to-end parse of the real Showroom (yearly) detail page."""

    @classmethod
    def setup_class(cls) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        cls.payload = parse_detail_html(SHOWROOM_HTML, SHOWROOM_URL)

    def test_payload_is_not_none(self) -> None:
        assert self.payload is not None

    def test_platform_listing_id(self) -> None:
        assert self.payload.platform_listing_id == "87825483"

    def test_accommodation_category_was_showroom(self) -> None:
        assert self.payload.raw_property_type == "Showroom"

    def test_listing_type_maps_to_showroom(self) -> None:
        assert self.payload.listing_type == "showroom"

    def test_permit_number(self) -> None:
        # 10-digit Saudi REGA FAL permit, format-compatible with Aqar.
        assert self.payload.aqar_advertisement_license == "7200772869"
        assert self.payload.aqar_advertisement_license.isdigit()
        assert len(self.payload.aqar_advertisement_license) == 10

    def test_yearly_price_no_multiplier(self) -> None:
        # window.state.price = 120000 ; rentFrequency = yearly →
        # price_sar_annual = 120000 (no × 12).
        assert self.payload.price_sar_annual == Decimal("120000.00")

    def test_lat_lon_in_riyadh_bbox(self) -> None:
        assert self.payload.lat == pytest.approx(24.896, abs=0.01)
        assert self.payload.lon == pytest.approx(46.613, abs=0.01)
        assert 24.4 <= self.payload.lat <= 25.1
        assert 46.4 <= self.payload.lon <= 47.0

    def test_area_sqm(self) -> None:
        assert self.payload.area_sqm == Decimal("883.00")
        assert self.payload.area_sqm > 0

    def test_listing_source(self) -> None:
        assert self.payload.aqar_listing_source == "Bayut"

    def test_contact_phone_preserves_redaction(self) -> None:
        assert self.payload.contact_phone == "+966XXXXXXXXX"

    def test_neighborhood_is_al_arid(self) -> None:
        # Deepest level in window.state.location chain (level 3).
        assert self.payload.neighborhood == "Al Arid"

    def test_image_url(self) -> None:
        assert self.payload.image_url
        assert self.payload.image_url.startswith("https://images.bayut.sa/")

    def test_created_at_parsed(self) -> None:
        assert self.payload.aqar_created_at is not None
        assert self.payload.aqar_created_at.year == 2026


# ---------------------------------------------------------------------------
# TestRealOfficeFixture
# ---------------------------------------------------------------------------


class TestRealOfficeFixture:
    """Regression fixture proving the real Office (monthly) detail page is
    **rejected**.

    Office was dropped from the Bayut accept-set: Bayut's office
    inventory is F&B-unsuitable, so the PR4 ``Office → store`` mapping
    only fed unsuitable noise into candidate_location. This fixture now
    pins the reverse of its original behavior — an Office page must
    route through the same parser-time rejection path as Warehouse /
    Commercial Building / Complex, yielding ``None`` (no payload, no
    ``commercial_unit`` insert, no ``store`` row).
    """

    @classmethod
    def setup_class(cls) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        cls.payload = parse_detail_html(OFFICE_HTML, OFFICE_URL)

    def test_office_is_rejected(self) -> None:
        # Office is no longer in the accept-set; parse_detail_html returns
        # None via the same path that rejects Warehouse / Complex.
        assert self.payload is None


# ---------------------------------------------------------------------------
# TestPropertyTypeFiltering
# ---------------------------------------------------------------------------


class TestPropertyTypeFiltering:
    """Mutates the Showroom fixture's accommodationCategory and asserts
    the accept-set. Anything outside {Showroom} — Office now included —
    returns None."""

    @pytest.mark.parametrize("category", [
        "Apartment", "Villa", "Floor", "Townhouse",
        "Office", "Warehouse", "Commercial Building", "Complex",
    ])
    def test_rejected_categories_return_none(self, category: str) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _swap_accommodation_category(SHOWROOM_HTML, category)
        assert parse_detail_html(html, SHOWROOM_URL) is None

    def test_showroom_returns_valid_payload(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _swap_accommodation_category(SHOWROOM_HTML, "Showroom")
        payload = parse_detail_html(html, SHOWROOM_URL)
        assert payload is not None
        assert payload.listing_type == "showroom"

    def test_office_is_rejected(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        # Office was dropped from the accept-set; even with a resolvable
        # price path it must reject (returns None) like any other
        # non-Showroom category.
        html = _swap_accommodation_category(SHOWROOM_HTML, "Office")
        assert parse_detail_html(html, SHOWROOM_URL) is None


# ---------------------------------------------------------------------------
# TestRentFrequencyBranching
# ---------------------------------------------------------------------------


class TestRentFrequencyBranching:
    """rentFrequency branches: yearly = identity, monthly = ×12, else None."""

    def test_yearly_keeps_price(self) -> None:
        # Real Showroom fixture is yearly @ 120000.
        from app.ingest.bayut.detail_scraper import parse_detail_html

        payload = parse_detail_html(SHOWROOM_HTML, SHOWROOM_URL)
        assert payload is not None
        assert payload.price_sar_annual == Decimal("120000.00")

    def test_monthly_multiplies_by_twelve(self) -> None:
        # Mutate Showroom (price=120000) into a monthly variant → 120000 × 12.
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _set_rent_frequency(SHOWROOM_HTML, "monthly")
        payload = parse_detail_html(html, SHOWROOM_URL)
        assert payload is not None
        assert payload.price_sar_annual == Decimal("1440000.00")

    def test_daily_returns_none(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _set_rent_frequency(SHOWROOM_HTML, "daily")
        assert parse_detail_html(html, SHOWROOM_URL) is None

    def test_weekly_returns_none(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _set_rent_frequency(SHOWROOM_HTML, "weekly")
        assert parse_detail_html(html, SHOWROOM_URL) is None

    def test_missing_returns_none(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _set_rent_frequency(SHOWROOM_HTML, None)
        assert parse_detail_html(html, SHOWROOM_URL) is None


# ---------------------------------------------------------------------------
# TestListingTypeMapping
# ---------------------------------------------------------------------------


class TestListingTypeMapping:
    """accommodationCategory → listing_type enum mapping."""

    def test_showroom_maps_to_showroom(self) -> None:
        from app.ingest.bayut.detail_scraper import _map_bayut_listing_type

        assert _map_bayut_listing_type("Showroom") == "showroom"

    def test_office_maps_to_none(self) -> None:
        # Office was removed from the type map — no longer maps to store.
        from app.ingest.bayut.detail_scraper import _map_bayut_listing_type

        assert _map_bayut_listing_type("Office") is None

    @pytest.mark.parametrize("category", [
        "Apartment", "Villa", "Floor", "Townhouse",
        "Office", "Warehouse", "Commercial Building", "Complex", "Shop",
    ])
    def test_other_categories_map_to_none(self, category: str) -> None:
        from app.ingest.bayut.detail_scraper import _map_bayut_listing_type

        assert _map_bayut_listing_type(category) is None

    def test_none_input_maps_to_none(self) -> None:
        from app.ingest.bayut.detail_scraper import _map_bayut_listing_type

        assert _map_bayut_listing_type(None) is None
        assert _map_bayut_listing_type("") is None


# ---------------------------------------------------------------------------
# TestUpsertBayutListing
# ---------------------------------------------------------------------------


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
    "aqar_id": "bayut:87825483",
    "platform": "bayut",
    "platform_listing_id": "87825483",
    "title": "Exhibition Building For Rent in Al Arid, Riyadh",
    "description": "Discover an excellent opportunity to rent an Exhibition Building.",
    "neighborhood": "Al Arid",
    "listing_url": SHOWROOM_URL,
    "image_url": "https://images.bayut.sa/thumbnails/8094107-800x600.jpeg",
    "price_sar_annual": Decimal("120000.00"),
    "area_sqm": Decimal("883.00"),
    "lat": 24.896259504116,
    "lon": 46.613181703661,
    "contact_phone": "+966XXXXXXXXX",
    "listing_type": "showroom",
    "property_type": "Commercial",
    "aqar_advertisement_license": "7200772869",
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
        assert insert_params["platform_listing_id"] == "87825483"
        assert insert_params["aqar_id"] == "bayut:87825483"
        assert insert_params["aqar_listing_source"] == "Bayut"
        assert insert_params["aqar_advertisement_license"] == "7200772869"

    def test_update_branch_sets_bayut_platform_columns(self) -> None:
        from scripts.scrape_bayut import upsert_bayut_listing

        select_result = _FakeResult(scalar_value=("bayut:87825483",))
        select_result.first = lambda: ("bayut:87825483",)  # type: ignore[assignment]
        db = _FakeDB([select_result, _FakeResult(rowcount=1)])

        action = upsert_bayut_listing(db, dict(_BASE_BAYUT_LISTING))

        assert action == "update"
        assert len(db.calls) == 2
        update_sql, update_params = db.calls[1]
        assert "UPDATE commercial_unit SET" in update_sql
        assert "platform = :platform" in update_sql
        assert "platform_listing_id = :platform_listing_id" in update_sql
        assert update_params["platform"] == "bayut"
        assert update_params["platform_listing_id"] == "87825483"
        assert update_params["aqar_id"] == "bayut:87825483"

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

    def test_listing_params_carries_permit_number(self) -> None:
        from scripts.scrape_bayut import _bayut_listing_params

        params = _bayut_listing_params({
            "platform_listing_id": "12345678",
            "aqar_advertisement_license": "7200772869",
        })
        assert params["aqar_advertisement_license"] == "7200772869"


# ---------------------------------------------------------------------------
# TestSuitabilityClassifierPortability
# ---------------------------------------------------------------------------


class TestSuitabilityClassifierPortability:
    """Bayut listings (lacking street_width_m / has_drive_thru) must
    still flow cleanly through the existing classifier."""

    def test_classifier_handles_bayut_shape_dict(self) -> None:
        from scripts.scrape_aqar import classify_restaurant_suitability

        listing: dict[str, Any] = {
            "title": "Exhibition Building For Rent in Al Arid, Riyadh",
            "description": "Discover an opportunity to rent a commercial showroom.",
            "listing_type": "showroom",
            "area_sqm": 883.0,
            # No street_width_m, no has_drive_thru — Bayut doesn't expose them.
        }

        result = classify_restaurant_suitability(listing)

        assert "restaurant_suitable" in result
        assert isinstance(result["restaurant_suitable"], bool)
        assert "restaurant_score" in result
        assert isinstance(result["restaurant_score"], int)
        # Bayut showroom with no drive-thru/mezzanine is still a valid
        # candidate — the structural F&B gate accepts listing_type=showroom.
        assert result["restaurant_suitable"] is True

    def test_classifier_rejects_residential_keywords(self) -> None:
        from scripts.scrape_aqar import classify_restaurant_suitability

        listing: dict[str, Any] = {
            "title": "Apartment for rent in Riyadh",
            "description": "Furnished studio apartment.",
            "listing_type": "store",
            "area_sqm": 80.0,
        }

        result = classify_restaurant_suitability(listing)

        assert result["restaurant_suitable"] is False


# ---------------------------------------------------------------------------
# TestApartmentRejectionAgainstRealBayut
# ---------------------------------------------------------------------------


class TestApartmentRejectionAgainstRealBayut:
    """Proves the accommodationCategory filter rejects residential
    pollution against a real Bayut payload shape — the bytes-level
    failure mode the v1 synthetic fixture couldn't simulate."""

    def test_apartment_against_real_payload_returns_none(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _swap_accommodation_category(SHOWROOM_HTML, "Apartment")
        assert parse_detail_html(html, SHOWROOM_URL) is None

    def test_villa_against_real_payload_returns_none(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _swap_accommodation_category(SHOWROOM_HTML, "Villa")
        assert parse_detail_html(html, SHOWROOM_URL) is None

    def test_floor_against_real_payload_returns_none(self) -> None:
        from app.ingest.bayut.detail_scraper import parse_detail_html

        html = _swap_accommodation_category(SHOWROOM_HTML, "Floor")
        assert parse_detail_html(html, SHOWROOM_URL) is None
