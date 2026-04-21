"""Tests for ``app.ingest.aqar.detail_scraper``.

Fixtures under ``tests/ingest/aqar/fixtures/`` are representative Aqar
detail-page HTML snapshots covering the four listing types plus two
edge cases: a listing with missing license/deed fields (common for
private landlord posts) and a fully-Arabic page with a relative "Last
Update" value.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from app.ingest.aqar.detail_scraper import (
    AqarDetailPayload,
    fetch_listing_detail,
    parse_detail_html,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures"
ANCHOR = datetime(2026, 4, 21, 12, 0, 0, tzinfo=timezone.utc)


def _load(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Warehouse — the canonical example from the Phase 2 spec
# ---------------------------------------------------------------------------


def test_parse_warehouse_en_spec_example():
    payload = parse_detail_html(_load("warehouse_en.html"), ANCHOR)

    assert payload is not None
    assert payload.aqar_created_at == datetime(2026, 1, 21, tzinfo=timezone.utc)
    # "1 minute ago"
    assert payload.aqar_updated_at == ANCHOR - timedelta(minutes=1)
    assert payload.aqar_views == 516
    assert payload.aqar_listing_source == "REGA"
    assert payload.aqar_advertisement_license == "7200846411"
    assert payload.aqar_license_expiry == date(2026, 10, 24)
    assert payload.aqar_plan_parcel == "4027 - مستودع / 199"
    assert payload.aqar_area_deed == Decimal("1477.41")
    assert payload.aqar_detail_scraped_at == ANCHOR


# ---------------------------------------------------------------------------
# Store — different DOM shape (spans not divs) and thousands-separator views
# ---------------------------------------------------------------------------


def test_parse_store_en_handles_span_layout_and_thousands_views():
    payload = parse_detail_html(_load("store_en.html"), ANCHOR)

    assert payload is not None
    assert payload.aqar_created_at == datetime(2026, 3, 5, tzinfo=timezone.utc)
    assert payload.aqar_updated_at == ANCHOR - timedelta(hours=3)
    # "1,284" must decode to 1284, not crash on the comma.
    assert payload.aqar_views == 1284
    assert payload.aqar_advertisement_license == "7200999123"
    assert payload.aqar_license_expiry == date(2027, 3, 5)
    assert payload.aqar_area_deed == Decimal("84.50")


# ---------------------------------------------------------------------------
# Building — Arabic labels, Arabic-Indic digits, dual "two days"
# ---------------------------------------------------------------------------


def test_parse_building_ar_handles_arabic_labels_and_dual():
    payload = parse_detail_html(_load("building_ar.html"), ANCHOR)

    assert payload is not None
    assert payload.aqar_created_at == datetime(2025, 12, 12, tzinfo=timezone.utc)
    # "منذ يومين" — dual form, 2 days without a leading 2.
    assert payload.aqar_updated_at == ANCHOR - timedelta(days=2)
    # Arabic-Indic "٣٢٥" must normalize to 325.
    assert payload.aqar_views == 325
    assert payload.aqar_advertisement_license == "7200123456"
    assert payload.aqar_license_expiry == date(2026, 12, 12)
    assert payload.aqar_plan_parcel == "3010 / 77"
    assert payload.aqar_area_deed == Decimal("620.00")


# ---------------------------------------------------------------------------
# Showroom — missing license / deed fields (private landlord post)
# ---------------------------------------------------------------------------


def test_parse_showroom_missing_license_fields_none_not_error():
    payload = parse_detail_html(_load("showroom_missing_license.html"), ANCHOR)

    assert payload is not None
    # Fields that ARE present still parse correctly.
    assert payload.aqar_created_at == datetime(2026, 4, 17, tzinfo=timezone.utc)
    assert payload.aqar_updated_at == ANCHOR - timedelta(days=5)
    assert payload.aqar_views == 42
    # Fields that are missing come back as None, not raise.
    assert payload.aqar_advertisement_license is None
    assert payload.aqar_license_expiry is None
    assert payload.aqar_plan_parcel is None
    assert payload.aqar_area_deed is None
    assert payload.aqar_listing_source is None


# ---------------------------------------------------------------------------
# Warehouse (Arabic) — Arabic-Indic digits in dates AND a relative time
# ---------------------------------------------------------------------------


def test_parse_warehouse_ar_relative_normalizes_arabic_digits_in_dates():
    payload = parse_detail_html(_load("warehouse_ar_relative.html"), ANCHOR)

    assert payload is not None
    # "٢١/٠١/٢٠٢٦" must decode to 2026-01-21.
    assert payload.aqar_created_at == datetime(2026, 1, 21, tzinfo=timezone.utc)
    # "منذ 3 أيام"
    assert payload.aqar_updated_at == ANCHOR - timedelta(days=3)
    # "٢٤/١٠/٢٠٢٦"
    assert payload.aqar_license_expiry == date(2026, 10, 24)
    assert payload.aqar_advertisement_license == "7200846411"
    # Arabic preserves Arabic script in the plan/parcel value.
    assert payload.aqar_plan_parcel == "4027 - مستودع / 199"


# ---------------------------------------------------------------------------
# Structure change — Info block absent → return None, not exception
# ---------------------------------------------------------------------------


def test_parse_returns_none_when_info_block_is_absent(caplog):
    html = "<html><body><h1>Some Aqar page without the Info block</h1></body></html>"
    payload = parse_detail_html(html, ANCHOR)
    assert payload is None


def test_parse_returns_none_on_empty_document():
    payload = parse_detail_html("", ANCHOR)
    assert payload is None


# ---------------------------------------------------------------------------
# Payload contract
# ---------------------------------------------------------------------------


def test_payload_dataclass_has_all_nine_fields():
    payload = parse_detail_html(_load("warehouse_en.html"), ANCHOR)
    assert payload is not None
    for name in (
        "aqar_created_at",
        "aqar_updated_at",
        "aqar_views",
        "aqar_advertisement_license",
        "aqar_license_expiry",
        "aqar_plan_parcel",
        "aqar_area_deed",
        "aqar_listing_source",
        "aqar_detail_scraped_at",
    ):
        assert hasattr(payload, name), f"AqarDetailPayload is missing {name}"


# ---------------------------------------------------------------------------
# Fetcher — 404 / 5xx retry / structure-change behavior
# ---------------------------------------------------------------------------


def _mock_response(status_code: int, text: str = ""):
    mock = MagicMock()
    mock.status_code = status_code
    mock.text = text
    return mock


def test_fetch_returns_none_on_404():
    session = MagicMock()
    session.get.return_value = _mock_response(404)

    result = fetch_listing_detail("6556192", "https://example/6556192", session)
    assert result is None
    assert session.get.call_count == 1  # no retry on 404


def test_fetch_parses_200_response():
    session = MagicMock()
    session.get.return_value = _mock_response(200, _load("warehouse_en.html"))

    result = fetch_listing_detail("6556192", "https://example/6556192", session)
    assert isinstance(result, AqarDetailPayload)
    assert result.aqar_views == 516


def test_fetch_retries_on_5xx_then_succeeds(monkeypatch):
    monkeypatch.setattr("app.ingest.aqar.detail_scraper.time.sleep", lambda _: None)
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(502),
        _mock_response(503),
        _mock_response(200, _load("warehouse_en.html")),
    ]

    result = fetch_listing_detail(
        "6556192", "https://example/6556192", session, max_retries=3
    )
    assert isinstance(result, AqarDetailPayload)
    assert session.get.call_count == 3


def test_fetch_gives_up_after_5xx_retries_exhausted(monkeypatch):
    monkeypatch.setattr("app.ingest.aqar.detail_scraper.time.sleep", lambda _: None)
    session = MagicMock()
    session.get.return_value = _mock_response(503)

    result = fetch_listing_detail(
        "6556192", "https://example/6556192", session, max_retries=3
    )
    assert result is None
    assert session.get.call_count == 3


def test_fetch_retries_on_network_error_then_succeeds(monkeypatch):
    monkeypatch.setattr("app.ingest.aqar.detail_scraper.time.sleep", lambda _: None)
    session = MagicMock()
    session.get.side_effect = [
        requests.ConnectionError("boom"),
        _mock_response(200, _load("warehouse_en.html")),
    ]

    result = fetch_listing_detail(
        "6556192", "https://example/6556192", session, max_retries=3
    )
    assert isinstance(result, AqarDetailPayload)


def test_fetch_returns_none_when_info_block_missing():
    session = MagicMock()
    session.get.return_value = _mock_response(
        200, "<html><body>not an aqar page</body></html>"
    )

    result = fetch_listing_detail("6556192", "https://example/6556192", session)
    assert result is None


def test_fetch_sets_detail_scraped_at_near_now(monkeypatch):
    session = MagicMock()
    session.get.return_value = _mock_response(200, _load("warehouse_en.html"))

    before = datetime.now(timezone.utc)
    result = fetch_listing_detail("6556192", "https://example/6556192", session)
    after = datetime.now(timezone.utc)

    assert isinstance(result, AqarDetailPayload)
    assert before <= result.aqar_detail_scraped_at <= after


# ---------------------------------------------------------------------------
# Regression harness — every fixture must parse without exceptions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fixture", sorted(FIXTURE_DIR.glob("*.html")))
def test_every_fixture_parses_to_payload(fixture):
    payload = parse_detail_html(fixture.read_text(encoding="utf-8"), ANCHOR)
    assert payload is not None, f"fixture {fixture.name} failed to parse"
    # Every fixture has Created At and Views in it — they must land.
    assert payload.aqar_created_at is not None
    assert payload.aqar_views is not None
