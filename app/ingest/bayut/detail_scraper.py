"""Bayut.sa listing detail-page scraper — JSON-blob extraction.

Bayut renders every commercial-rent detail page as a Next.js app and
embeds a structured JSON blob inside a ``<script>`` tag (``__NEXT_DATA__``)
that exposes every field the writer needs without depending on rendered
HTML. This parser pulls the blob out and projects the fields onto a
``BayutDetailPayload`` dataclass that mirrors the shape Aqar uses, so the
``upsert_bayut_listing`` writer can reuse the same column projection
``_listing_params`` pattern Aqar uses.

Conventions:
  * Pure HTTP — no Playwright, no JS rendering. The JSON blob is
    server-rendered into the HTML so a plain ``requests.get`` is
    sufficient.
  * BeautifulSoup with lxml for the script-tag extraction; ``json.loads``
    for the actual data.
  * Property-type filter (Shop / Showroom / Retail Shop / Commercial
    Shop) applied at parser-time — Bayut's ``/shops-for-rent/`` URL
    mixes in residential listings and we reject them by structured
    ``propertyType.en`` field.
  * Price conversion — Bayut always publishes commercial rents as
    monthly (``rentFrequency: "monthly"``); the writer multiplies by 12
    to land an annual figure in ``commercial_unit.price_sar_annual``.
  * REGA dedup key — ``rega_license_info_fal_license_number`` is the
    per-listing FAL advertisement license (Aqar's
    ``aqar_advertisement_license`` equivalent). The agency-level
    ``brokerage_and_marketing_license_number`` is intentionally NOT
    used for dedup.
  * Returns ``None`` (does not raise) on malformed/empty input or on
    rejected property types — same shape as Aqar's ``parse_detail_html``.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


# Property types acceptable as commercial-rent restaurant candidates.
# Bayut's /shops-for-rent/ URL mixes residential listings (Apartment,
# Villa, etc.) into the listing pages, so the parser filters on the
# structured propertyType.en value rather than trusting the URL slug.
_BAYUT_ACCEPTED_PROPERTY_TYPES = frozenset({
    "Shop",
    "Showroom",
    "Retail Shop",
    "Commercial Shop",
})


# Mapping from Bayut's accepted propertyType.en strings to the
# commercial_unit.listing_type enum the rest of the pipeline uses. The
# downstream classify_restaurant_suitability gates F&B-compatibility on
# {store, showroom}, so both shop variants land as 'store'.
_BAYUT_LISTING_TYPE_MAP: dict[str, str] = {
    "Shop": "store",
    "Retail Shop": "store",
    "Commercial Shop": "store",
    "Showroom": "showroom",
}


# Bayut's commercial-rent listings are always published as monthly rent.
# A future schema drift (e.g. some yearly listings) would silently
# corrupt price_sar_annual by 12× if we defaulted; the parser instead
# skips the listing and logs a warning so the operator notices.
_EXPECTED_RENT_FREQUENCY = "monthly"


SQFT_TO_SQM = Decimal("0.092903")


@dataclass
class BayutDetailPayload:
    """Structured result of parsing a Bayut listing detail page.

    Field names align with commercial_unit columns so the upsert layer
    can splat the dataclass into _bayut_listing_params with minimal
    plumbing.
    """

    platform_listing_id: str
    listing_url: str
    title: str | None
    description: str | None
    image_url: str | None
    lat: float | None
    lon: float | None
    area_sqm: Decimal | None
    price_sar_annual: Decimal | None
    contact_phone: str | None
    listing_type: str | None
    property_type: str
    aqar_advertisement_license: str | None
    aqar_listing_source: str
    aqar_created_at: datetime | None
    aqar_updated_at: datetime | None
    aqar_detail_scraped_at: datetime
    neighborhood: str | None = None
    raw_property_type: str | None = None
    # Carries unmapped extras for diagnostics/debugging — never persisted.
    extras: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Top-level parser
# ---------------------------------------------------------------------------


def parse_detail_html(
    html: str,
    listing_url: str,
    fetched_at: datetime | None = None,
) -> BayutDetailPayload | None:
    """Parse a Bayut listing detail page into a ``BayutDetailPayload``.

    Returns ``None`` when the page is malformed, the JSON blob is
    missing, the listing's ``propertyType.en`` is outside the accepted
    commercial-rent set, or any defensive guard trips (e.g. the
    ``rentFrequency`` is not the expected monthly cadence).
    """
    if not html:
        return None

    fetched_at = fetched_at or datetime.now(timezone.utc)

    listing_json = _extract_listing_json(html)
    if not listing_json:
        logger.warning(
            "Bayut detail page missing structured JSON blob — possible "
            "structure change at %s", listing_url,
        )
        return None

    property_type_raw = _extract_property_type_en(listing_json)
    if property_type_raw not in _BAYUT_ACCEPTED_PROPERTY_TYPES:
        logger.info(
            "Bayut listing skipped: propertyType=%r not in accepted set (%s)",
            property_type_raw, listing_url,
        )
        return None

    rent_frequency = _extract_rent_frequency(listing_json)
    if rent_frequency != _EXPECTED_RENT_FREQUENCY:
        logger.warning(
            "Bayut listing %s has rentFrequency=%r (expected %r); "
            "skipping rather than guessing the annualization factor",
            listing_url, rent_frequency, _EXPECTED_RENT_FREQUENCY,
        )
        return None

    listing_id = _extract_id(listing_json)
    if not listing_id:
        logger.warning("Bayut listing missing id at %s", listing_url)
        return None

    listing_type = _map_bayut_listing_type(property_type_raw)
    if listing_type is None:
        # Defensive: filter+map agree on the accepted set.
        return None

    soup = BeautifulSoup(html, "lxml")

    return BayutDetailPayload(
        platform_listing_id=str(listing_id),
        listing_url=listing_url,
        title=_extract_title(listing_json, soup),
        description=_extract_description(listing_json, soup),
        image_url=_extract_image_url(listing_json, soup),
        lat=_extract_lat(listing_json),
        lon=_extract_lon(listing_json),
        area_sqm=_extract_area_sqm(listing_json),
        price_sar_annual=_extract_price_sar_annual(listing_json),
        contact_phone=_extract_contact_phone(listing_json),
        listing_type=listing_type,
        property_type="Commercial",
        aqar_advertisement_license=_extract_rega_license(listing_json),
        aqar_listing_source="Bayut",
        aqar_created_at=_extract_iso_datetime(listing_json, ("createdAt", "posted", "datePosted")),
        aqar_updated_at=_extract_iso_datetime(listing_json, ("dateModified", "updatedAt", "lastUpdated")),
        aqar_detail_scraped_at=fetched_at,
        neighborhood=_extract_neighborhood(listing_json),
        raw_property_type=property_type_raw,
    )


def _map_bayut_listing_type(property_type: str | None) -> str | None:
    """Map a Bayut ``propertyType.en`` value onto our listing_type enum."""
    if not property_type:
        return None
    return _BAYUT_LISTING_TYPE_MAP.get(property_type)


# ---------------------------------------------------------------------------
# JSON blob extraction
# ---------------------------------------------------------------------------


_NEXT_DATA_PATTERNS = (
    re.compile(r'window\.__NEXT_DATA__\s*=\s*(\{.*?\});', re.DOTALL),
    re.compile(r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\});', re.DOTALL),
    re.compile(r'window\.__data__\s*=\s*(\{.*?\});', re.DOTALL),
)


def _extract_listing_json(html: str) -> dict[str, Any] | None:
    """Pull the listing's JSON record out of the detail-page HTML.

    Bayut's detail pages embed the listing model in one of three
    server-rendered shapes. Each is tried in order:

    1. ``<script id="__NEXT_DATA__" type="application/json">`` — the
       Next.js standard. Most stable, this is the preferred path.
    2. ``<script type="application/ld+json">`` — JSON-LD product
       metadata; thinner than __NEXT_DATA__ but covers id/price/url.
    3. Inline ``window.__NEXT_DATA__ = {...};`` assignment — fallback
       for older render paths.

    Returns the inner ``property`` object (the per-listing record) when
    locatable, else ``None``.
    """
    soup = BeautifulSoup(html, "lxml")

    # Path 1: __NEXT_DATA__ script tag.
    next_tag = soup.find("script", id="__NEXT_DATA__")
    if next_tag and next_tag.string:
        try:
            payload = json.loads(next_tag.string)
        except json.JSONDecodeError as exc:
            logger.warning("Bayut __NEXT_DATA__ JSON decode error: %s", exc)
            payload = None
        if payload:
            listing = _find_listing_record(payload)
            if listing:
                return listing

    # Path 2: JSON-LD product blocks.
    for ld in soup.find_all("script", attrs={"type": "application/ld+json"}):
        if not ld.string:
            continue
        try:
            data = json.loads(ld.string)
        except json.JSONDecodeError:
            continue
        listing = _find_listing_record(data)
        if listing:
            return listing

    # Path 3: inline window assignment (legacy / probe-captured shape).
    for pattern in _NEXT_DATA_PATTERNS:
        match = pattern.search(html)
        if not match:
            continue
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        listing = _find_listing_record(payload)
        if listing:
            return listing

    return None


_LISTING_RECORD_HINT_KEYS = (
    "rega_license_info_fal_license_number",
    "rentFrequency",
    "propertyType",
    "externalID",
    "referenceNumber",
)


def _find_listing_record(payload: Any) -> dict[str, Any] | None:
    """DFS through the JSON tree looking for the per-listing record.

    Different Bayut render paths nest the listing under different keys
    (``props.pageProps.property``, ``data.property``, ``listing``,
    etc.). Rather than hard-code one path, we walk the tree and pick
    the first dict that holds at least two of the hint keys.
    """
    if isinstance(payload, dict):
        hits = sum(1 for k in _LISTING_RECORD_HINT_KEYS if k in payload)
        if hits >= 2:
            return payload
        for value in payload.values():
            found = _find_listing_record(value)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_listing_record(item)
            if found:
                return found
    return None


# ---------------------------------------------------------------------------
# Field extraction helpers
# ---------------------------------------------------------------------------


def _extract_id(listing: dict[str, Any]) -> str | None:
    """Bayut's stable per-listing ID lives under one of several keys."""
    for key in ("externalID", "id", "referenceNumber"):
        value = listing.get(key)
        if value:
            return str(value)
    return None


def _extract_property_type_en(listing: dict[str, Any]) -> str | None:
    """Read ``propertyType.en`` (the English label used for filtering)."""
    raw = listing.get("propertyType") or listing.get("category")
    if isinstance(raw, dict):
        # Bayut nests {en: "Shop", ar: "...", slug: "..."}.
        return raw.get("en") or raw.get("label") or raw.get("name")
    if isinstance(raw, list) and raw:
        # /category/ shape exposes a path; the leaf is the most specific
        # propertyType (Shop / Showroom / etc.).
        leaf = raw[-1]
        if isinstance(leaf, dict):
            return leaf.get("name_l1") or leaf.get("name")
        return str(leaf)
    if isinstance(raw, str):
        return raw
    return None


def _extract_rent_frequency(listing: dict[str, Any]) -> str | None:
    """Read ``rentFrequency`` (Bayut publishes monthly for commercial)."""
    value = listing.get("rentFrequency") or listing.get("rent_frequency")
    return value.lower() if isinstance(value, str) else None


def _extract_title(listing: dict[str, Any], soup: BeautifulSoup) -> str | None:
    """Prefer ``og:title`` meta; fall back to JSON title field."""
    meta_title = _meta_content(soup, "og:title")
    if meta_title:
        return meta_title.strip()
    for key in ("title", "name"):
        value = listing.get(key)
        if value:
            return str(value).strip()
    return None


def _extract_description(listing: dict[str, Any], soup: BeautifulSoup) -> str | None:
    meta_desc = _meta_content(soup, "og:description") or _meta_name_content(soup, "description")
    if meta_desc:
        return meta_desc.strip()
    for key in ("description", "shortDescription"):
        value = listing.get(key)
        if value:
            return str(value).strip()
    return None


def _extract_image_url(listing: dict[str, Any], soup: BeautifulSoup) -> str | None:
    meta_image = _meta_content(soup, "og:image")
    if meta_image:
        return meta_image
    cover = listing.get("coverPhoto") or listing.get("image")
    if isinstance(cover, dict):
        return cover.get("url") or cover.get("src")
    if isinstance(cover, str):
        return cover
    photos = listing.get("photos") or listing.get("images")
    if isinstance(photos, list) and photos:
        first = photos[0]
        if isinstance(first, dict):
            return first.get("url") or first.get("src")
        if isinstance(first, str):
            return first
    return None


def _extract_lat(listing: dict[str, Any]) -> float | None:
    return _extract_coordinate(listing, ("lat", "latitude"))


def _extract_lon(listing: dict[str, Any]) -> float | None:
    return _extract_coordinate(listing, ("lng", "lon", "longitude"))


def _extract_coordinate(listing: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    """Read lat/lng from the ``geography`` block or top-level keys.

    Bayut nests coordinates under ``geography: {lat, lng}`` in
    __NEXT_DATA__ payloads, but the legacy probe shape exposed top-level
    ``latitude``/``longitude`` keys. Both are supported; ``geography``
    wins when both are present because it's the structured field Bayut
    treats as authoritative.
    """
    geo = listing.get("geography")
    if isinstance(geo, dict):
        for key in keys:
            value = geo.get(key)
            coerced = _coerce_float(value)
            if coerced is not None:
                return coerced
    for key in keys:
        coerced = _coerce_float(listing.get(key))
        if coerced is not None:
            return coerced
    return None


# The authoritative area key. Per the PR4 investigation: Bayut's
# detail-page JSON exposes 3-5 area-like keys per listing, but only one
# is the canonical "total area" the rest are derived from. The captured
# fixture exposes:
#
#   "area": 1234.0          → m², canonical total area (what we want)
#   "areaSquareFeet": 13282 → sqft derived from area × 10.7639
#   "floorArea": 1234.0     → typically equal to area for retail units
#   "size": "1234 m²"       → string form of area
#
# We prefer the bare numeric `area` key when present. When only
# `areaSquareFeet` is exposed, we convert × 0.092903. We deliberately do
# NOT trust `size` (string form) — too easy to corrupt via stray
# punctuation in upstream content.
def _extract_area_sqm(listing: dict[str, Any]) -> Decimal | None:
    """Pull the canonical total-area in m² from the listing blob.

    Reads ``area`` (m², canonical), then falls back to
    ``areaSquareFeet`` (with × 0.092903 conversion). Anything else is
    treated as derived data we don't trust.
    """
    raw = listing.get("area")
    if isinstance(raw, (int, float)) and raw > 0:
        return _decimal_round(Decimal(str(raw)))

    raw_sqft = listing.get("areaSquareFeet") or listing.get("area_sqft")
    if isinstance(raw_sqft, (int, float)) and raw_sqft > 0:
        return _decimal_round(Decimal(str(raw_sqft)) * SQFT_TO_SQM)

    # Last-chance string parse on the numeric content of ``size`` so the
    # parser doesn't return None on listings that only expose the
    # human-formatted total. Bounded to digits/decimal, no unit
    # interpretation.
    size_str = listing.get("size")
    if isinstance(size_str, str):
        match = re.search(r"(\d+(?:\.\d+)?)", size_str)
        if match:
            try:
                value = Decimal(match.group(1))
                if value > 0:
                    return _decimal_round(value)
            except InvalidOperation:
                pass
    return None


def _extract_price_sar_annual(listing: dict[str, Any]) -> Decimal | None:
    """Annual rent in SAR — monthly Bayut price × 12.

    The frequency guard runs before this call, so we only get here when
    ``rentFrequency == 'monthly'``. Hardcoding the 12 multiplier is
    intentional — a dynamic check would underprice by 12× if the field
    ever went missing on a future Bayut render.
    """
    raw = listing.get("price")
    if isinstance(raw, (int, float)) and raw > 0:
        return _decimal_round(Decimal(str(raw)) * 12)
    if isinstance(raw, str):
        match = re.search(r"\d+(?:[\.,]\d+)?", raw.replace(",", ""))
        if match:
            try:
                value = Decimal(match.group(0))
                if value > 0:
                    return _decimal_round(value * 12)
            except InvalidOperation:
                return None
    return None


def _extract_rega_license(listing: dict[str, Any]) -> str | None:
    """Per-listing FAL advertisement license — the cross-portal dedup key.

    NOT to be confused with ``brokerage_and_marketing_license_number``,
    which is the agency-level license and identical across hundreds of
    listings from the same broker (useless for dedup).
    """
    value = listing.get("rega_license_info_fal_license_number")
    if not value:
        # Some payloads nest the FAL number under a sub-object.
        info = listing.get("rega_license_info")
        if isinstance(info, dict):
            value = info.get("fal_license_number") or info.get("ad_license_number")
    if value is None:
        return None
    cleaned = re.sub(r"\s+", "", str(value)).strip()
    return cleaned or None


def _extract_contact_phone(listing: dict[str, Any]) -> str | None:
    """First non-empty in priority order: phone → mobileNumbers[0] → whatsapp.

    Bayut nests contact details under an ``agent`` or ``phoneNumber``
    block. We accept either shape.
    """
    candidates: list[Any] = []

    agent = listing.get("phoneNumber") or listing.get("agent") or {}
    if isinstance(agent, dict):
        candidates.append(agent.get("phone"))
        mobile = agent.get("mobileNumbers") or agent.get("mobile")
        if isinstance(mobile, list) and mobile:
            candidates.append(mobile[0])
        elif isinstance(mobile, str):
            candidates.append(mobile)
        candidates.append(agent.get("whatsapp"))

    # Top-level fallbacks.
    candidates.extend([
        listing.get("phone"),
        listing.get("mobile"),
        listing.get("whatsapp"),
    ])

    for candidate in candidates:
        if candidate:
            cleaned = str(candidate).strip()
            if cleaned:
                return cleaned
    return None


def _extract_neighborhood(listing: dict[str, Any]) -> str | None:
    """Pull a human-readable neighborhood label from the location tree.

    Bayut exposes ``location: [{level, name}, ...]`` where the deepest
    non-city level (typically level 2 or 3) is the neighborhood.
    """
    location = listing.get("location") or listing.get("neighborhood")
    if isinstance(location, list) and location:
        # The leaf is the most-specific level.
        for level in reversed(location):
            if isinstance(level, dict):
                name = level.get("name") or level.get("name_l1")
                if name and name.lower() not in ("saudi arabia", "riyadh"):
                    return str(name).strip()
    if isinstance(location, str):
        return location.strip() or None
    return None


def _extract_iso_datetime(listing: dict[str, Any], keys: tuple[str, ...]) -> datetime | None:
    for key in keys:
        value = listing.get(key)
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # Bayut sometimes exposes Unix epoch seconds for dateModified.
        try:
            return datetime.fromtimestamp(int(value), tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        # ISO 8601 (with or without timezone).
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        # Date-only ``YYYY-MM-DD`` (Bayut's "posted" field).
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d")
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------


def _meta_content(soup: BeautifulSoup, prop: str) -> str | None:
    tag = soup.find("meta", attrs={"property": prop})
    if tag and tag.get("content"):
        return tag["content"]
    return None


def _meta_name_content(soup: BeautifulSoup, name: str) -> str | None:
    tag = soup.find("meta", attrs={"name": name})
    if tag and tag.get("content"):
        return tag["content"]
    return None


def _coerce_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _decimal_round(value: Decimal) -> Decimal:
    # Two decimal places is the consistent precision the rest of the
    # commercial_unit columns use (price_per_sqm, area_sqm). Quantize
    # here so the upstream comparison/test math is stable.
    return value.quantize(Decimal("0.01"))
