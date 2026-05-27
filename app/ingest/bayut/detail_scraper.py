"""Bayut.sa listing detail-page parser.

Bayut detail pages embed listing data in **two complementary payloads**.
The marketing-shape fields (name, description, geo, floorSize, price,
realEstateAgent telephone) live in a JSON-LD ``<script
type="application/ld+json">`` block under
``@graph[…].mainEntity``. The operational fields (REGA permit number,
rentFrequency, the Bayut category tree) live in a Nuxt-style hydration
blob assigned to ``window.state`` inside a bare ``<script>`` tag at the
bottom of the page; the per-listing record sits at
``window.state.property.data``. The parser reads both and combines
them.

Conventions:
  * Pure HTTP — no Playwright, no JS rendering. Both payloads are
    server-rendered into the HTML so a plain ``requests.get`` is
    sufficient.
  * BeautifulSoup with lxml for the JSON-LD ``<script>`` lookup.
    ``window.state`` is extracted via ``json.JSONDecoder().raw_decode``
    — a non-greedy regex would mis-balance on nested objects in the
    155 KB hydration blob.
  * Property-type filter (``Showroom`` and ``Office`` only) is applied
    at parser-time. Bayut's ``/en/to-rent/commercial/`` URL also
    exposes Warehouses, Commercial Buildings, and Complexes; the
    filter restricts PR4 v1 to F&B-compatible accommodations.
  * Price conversion branches on ``rentFrequency`` — Bayut publishes
    both ``yearly`` and ``monthly`` commercial listings:
      * ``yearly``  → ``price_sar_annual = price`` (no multiplier)
      * ``monthly`` → ``price_sar_annual = price * 12``
      * anything else (incl. missing) → return ``None``, log warning
  * REGA dedup key — ``permitNumber`` (Saudi REGA 10-digit FAL
    advertisement permit). The agency-level
    ``brokerage_and_marketing_license_number`` and the parcel-level
    ``rega_additional_info_deed_number`` are intentionally NOT used.
  * Returns ``None`` (does not raise) on malformed/empty input or on
    rejected property types — same shape as Aqar's
    ``parse_detail_html``.
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


# Accommodation categories acceptable as commercial-rent restaurant
# candidates. Bayut's commercial taxonomy is {Showroom, Office,
# Warehouse, Commercial Building, Complex}; v1 restricts to the two
# that are structurally F&B-compatible. Anything else — including
# residential leaks (Apartment, Villa, Floor, Townhouse) — gets
# rejected at parser time.
_BAYUT_ACCEPTED_PROPERTY_TYPES = frozenset({"Showroom", "Office"})


# Mapping from Bayut's accepted ``accommodationCategory`` strings to the
# commercial_unit.listing_type enum the rest of the pipeline uses.
# Aqar's classifier gates F&B-compatibility on {store, showroom}, so
# Office (a serviced/leased commercial unit) maps onto ``store``.
_BAYUT_LISTING_TYPE_MAP: dict[str, str] = {
    "Showroom": "showroom",
    "Office": "store",
}


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

    Returns ``None`` when the page is malformed, either payload is
    missing, the listing's accommodationCategory is outside the
    accepted commercial-rent set, or the rent-frequency branch can't
    annualize the price (missing or unrecognized cadence).
    """
    if not html:
        return None

    fetched_at = fetched_at or datetime.now(timezone.utc)

    ld_listing = _extract_jsonld_listing(html)
    state_listing = _extract_state_listing(html)

    if ld_listing is None and state_listing is None:
        logger.warning(
            "Bayut detail page missing both JSON-LD and window.state — "
            "possible structure change at %s",
            listing_url,
        )
        return None

    property_type = _extract_accommodation_category(ld_listing, state_listing)
    if property_type not in _BAYUT_ACCEPTED_PROPERTY_TYPES:
        logger.info(
            "Bayut listing skipped: accommodationCategory=%r not in accepted set (%s)",
            property_type, listing_url,
        )
        return None

    listing_type = _map_bayut_listing_type(property_type)
    if listing_type is None:
        return None

    price_sar_annual = _extract_price_sar_annual(state_listing, ld_listing, listing_url)
    if price_sar_annual is None:
        # Either price or rentFrequency was unusable; the helper logs the
        # specific reason.
        return None

    listing_id = _extract_id(state_listing, ld_listing, listing_url)
    if not listing_id:
        logger.warning("Bayut listing missing id at %s", listing_url)
        return None

    return BayutDetailPayload(
        platform_listing_id=listing_id,
        listing_url=listing_url,
        title=_extract_title(ld_listing, state_listing),
        description=_extract_description(ld_listing, state_listing),
        image_url=_extract_image_url(ld_listing, state_listing),
        lat=_extract_lat(ld_listing, state_listing),
        lon=_extract_lon(ld_listing, state_listing),
        area_sqm=_extract_area_sqm(ld_listing, state_listing),
        price_sar_annual=price_sar_annual,
        contact_phone=_extract_contact_phone(state_listing, ld_listing),
        listing_type=listing_type,
        property_type="Commercial",
        aqar_advertisement_license=_extract_permit_number(state_listing),
        aqar_listing_source="Bayut",
        aqar_created_at=_extract_created_at(ld_listing, state_listing),
        aqar_updated_at=_extract_updated_at(state_listing, ld_listing),
        aqar_detail_scraped_at=fetched_at,
        neighborhood=_extract_neighborhood(state_listing, ld_listing),
        raw_property_type=property_type,
    )


def _map_bayut_listing_type(property_type: str | None) -> str | None:
    """Map a Bayut ``accommodationCategory`` value onto our enum."""
    if not property_type:
        return None
    return _BAYUT_LISTING_TYPE_MAP.get(property_type)


# ---------------------------------------------------------------------------
# JSON-LD extraction
# ---------------------------------------------------------------------------


def _extract_jsonld_listing(html: str) -> dict[str, Any] | None:
    """Pull the ``RealEstateListing`` record out of the JSON-LD block.

    Real Bayut detail pages serve exactly one ``<script
    type="application/ld+json">`` with ``@context: schema.org`` and a
    two-element ``@graph`` containing a ``RealEstateListing`` and an
    ``Organization``. We return the ``RealEstateListing`` element.
    """
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        if not tag.string:
            continue
        try:
            payload = json.loads(tag.string)
        except json.JSONDecodeError as exc:
            logger.warning("Bayut JSON-LD decode error: %s", exc)
            continue
        listing = _find_realestate_listing(payload)
        if listing is not None:
            return listing
    return None


def _find_realestate_listing(payload: Any) -> dict[str, Any] | None:
    """Walk a JSON-LD payload to find the per-listing record.

    The actual shape on Bayut is ``{"@context": ..., "@graph": [
    {"@type": "RealEstateListing", ...}, {"@type": "Organization",
    ...}]}``. We accept either the listing at ``@graph[i]`` or any
    nested location to keep the parser robust against future shape
    drift.
    """
    if isinstance(payload, dict):
        types = payload.get("@type")
        if types == "RealEstateListing" or (
            isinstance(types, list) and "RealEstateListing" in types
        ):
            return payload
        for value in payload.values():
            found = _find_realestate_listing(value)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_realestate_listing(item)
            if found is not None:
                return found
    return None


# ---------------------------------------------------------------------------
# window.state extraction
# ---------------------------------------------------------------------------


_WINDOW_STATE_RE = re.compile(r"window\.state\s*=\s*")


def _extract_state_listing(html: str) -> dict[str, Any] | None:
    """Pull ``state.property.data`` out of the inline ``window.state`` blob.

    ``window.state`` is a ~155 KB Nuxt-style hydration blob assigned in
    a bare ``<script>`` tag (no ``type``, no ``id``, no ``src``). The
    per-listing record lives at the fixed path
    ``state.property.data``. We use ``json.JSONDecoder().raw_decode``
    rather than a non-greedy regex because the latter mis-balances on
    nested objects.
    """
    match = _WINDOW_STATE_RE.search(html)
    if match is None:
        return None
    decoder = json.JSONDecoder()
    try:
        state, _ = decoder.raw_decode(html, match.end())
    except json.JSONDecodeError as exc:
        logger.warning("Bayut window.state decode error: %s", exc)
        return None
    if not isinstance(state, dict):
        return None

    property_block = state.get("property")
    if isinstance(property_block, dict):
        data = property_block.get("data")
        if isinstance(data, dict) and data:
            return data

    return _find_state_listing(state)


_STATE_HINT_KEYS = ("permitNumber", "rentFrequency", "externalID", "geography")


def _find_state_listing(node: Any) -> dict[str, Any] | None:
    """Fallback DFS used only when ``state.property.data`` is empty.

    Matches any dict that exposes at least three of the hint keys —
    enough signal that a future shape drift still resolves to the
    listing record rather than to a recommendation summary card.
    """
    if isinstance(node, dict):
        hits = sum(1 for k in _STATE_HINT_KEYS if k in node)
        if hits >= 3:
            return node
        for value in node.values():
            found = _find_state_listing(value)
            if found is not None:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _find_state_listing(item)
            if found is not None:
                return found
    return None


# ---------------------------------------------------------------------------
# Field extractors — read from JSON-LD primarily, window.state as fallback
# ---------------------------------------------------------------------------


def _extract_id(
    state_listing: dict[str, Any] | None,
    ld_listing: dict[str, Any] | None,
    listing_url: str,
) -> str | None:
    if state_listing is not None:
        for key in ("externalID", "referenceNumber"):
            value = state_listing.get(key)
            if value:
                return str(value)
    if ld_listing is not None:
        url = ld_listing.get("url")
        if isinstance(url, str):
            match = re.search(r"/details-(\d+)\.html", url)
            if match:
                return match.group(1)
    match = re.search(r"/details-(\d+)\.html", listing_url)
    if match:
        return match.group(1)
    return None


def _extract_accommodation_category(
    ld_listing: dict[str, Any] | None,
    state_listing: dict[str, Any] | None,
) -> str | None:
    """Return the structured English accommodationCategory.

    Prefers JSON-LD ``mainEntity.accommodationCategory`` because it's
    the schema.org-declared field. Falls back to ``window.state``'s
    ``category[1].nameSingular`` — Bayut populates ``category`` as a
    two-element array ``[{Commercial parent}, {specific
    accommodation}]``; the second entry's ``nameSingular`` carries the
    same value (e.g. ``"Showroom"``, ``"Office"``).
    """
    if ld_listing is not None:
        main_entity = ld_listing.get("mainEntity")
        if isinstance(main_entity, dict):
            value = main_entity.get("accommodationCategory")
            if isinstance(value, str):
                return value
        # Some legacy shapes expose it at the listing root.
        value = ld_listing.get("accommodationCategory")
        if isinstance(value, str):
            return value

    if state_listing is not None:
        category = state_listing.get("category")
        if isinstance(category, list) and len(category) >= 2:
            specific = category[1]
            if isinstance(specific, dict):
                value = specific.get("nameSingular") or specific.get("name")
                if isinstance(value, str):
                    return value

    return None


def _extract_title(
    ld_listing: dict[str, Any] | None,
    state_listing: dict[str, Any] | None,
) -> str | None:
    if state_listing is not None:
        value = state_listing.get("title")
        if isinstance(value, str) and value.strip():
            return value.strip()
    if ld_listing is not None:
        value = ld_listing.get("name")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_description(
    ld_listing: dict[str, Any] | None,
    state_listing: dict[str, Any] | None,
) -> str | None:
    if ld_listing is not None:
        main_entity = ld_listing.get("mainEntity")
        if isinstance(main_entity, dict):
            value = main_entity.get("description")
            if isinstance(value, str) and value.strip():
                return value.strip()
        value = ld_listing.get("description")
        if isinstance(value, str) and value.strip():
            return value.strip()
    if state_listing is not None:
        value = state_listing.get("description")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_image_url(
    ld_listing: dict[str, Any] | None,
    state_listing: dict[str, Any] | None,
) -> str | None:
    if ld_listing is not None:
        main_entity = ld_listing.get("mainEntity")
        if isinstance(main_entity, dict):
            images = main_entity.get("image")
            if isinstance(images, list) and images:
                first = images[0]
                if isinstance(first, str):
                    return first
            if isinstance(images, str):
                return images
        top = ld_listing.get("image")
        if isinstance(top, str):
            return top
    if state_listing is not None:
        cover = state_listing.get("coverPhoto")
        if isinstance(cover, dict):
            uuid = cover.get("uuid")
            if isinstance(uuid, str) and uuid:
                # Bayut serves photos at https://images.bayut.sa/thumbnails/<id>-<size>.jpeg
                ext_id = cover.get("externalID")
                if ext_id:
                    return f"https://images.bayut.sa/thumbnails/{ext_id}-800x600.jpeg"
    return None


def _extract_lat(
    ld_listing: dict[str, Any] | None,
    state_listing: dict[str, Any] | None,
) -> float | None:
    if ld_listing is not None:
        main_entity = ld_listing.get("mainEntity")
        if isinstance(main_entity, dict):
            geo = main_entity.get("geo")
            if isinstance(geo, dict):
                coerced = _coerce_float(geo.get("latitude"))
                if coerced is not None:
                    return coerced
    if state_listing is not None:
        geo = state_listing.get("geography")
        if isinstance(geo, dict):
            coerced = _coerce_float(geo.get("lat"))
            if coerced is not None:
                return coerced
    return None


def _extract_lon(
    ld_listing: dict[str, Any] | None,
    state_listing: dict[str, Any] | None,
) -> float | None:
    if ld_listing is not None:
        main_entity = ld_listing.get("mainEntity")
        if isinstance(main_entity, dict):
            geo = main_entity.get("geo")
            if isinstance(geo, dict):
                coerced = _coerce_float(geo.get("longitude"))
                if coerced is not None:
                    return coerced
    if state_listing is not None:
        geo = state_listing.get("geography")
        if isinstance(geo, dict):
            coerced = _coerce_float(geo.get("lng"))
            if coerced is not None:
                return coerced
    return None


def _extract_area_sqm(
    ld_listing: dict[str, Any] | None,
    state_listing: dict[str, Any] | None,
) -> Decimal | None:
    """Read total floor area in m² — JSON-LD ``floorSize.value`` is the
    canonical source, ``window.state.area`` is the fallback.

    Bayut's JSON-LD reports ``floorSize: {value: "883", unitText:
    "SQM"}`` — the value is a string carrying an integer count of
    square metres. The fallback ``window.state.property.data.area`` is
    a bare integer.
    """
    if ld_listing is not None:
        main_entity = ld_listing.get("mainEntity")
        if isinstance(main_entity, dict):
            floor_size = main_entity.get("floorSize")
            if isinstance(floor_size, dict):
                raw = floor_size.get("value")
                parsed = _parse_decimal(raw)
                if parsed is not None and parsed > 0:
                    return _decimal_round(parsed)
    if state_listing is not None:
        raw = state_listing.get("area")
        parsed = _parse_decimal(raw)
        if parsed is not None and parsed > 0:
            return _decimal_round(parsed)
    return None


def _extract_price_sar_annual(
    state_listing: dict[str, Any] | None,
    ld_listing: dict[str, Any] | None,
    listing_url: str,
) -> Decimal | None:
    """Annual rent in SAR, branched on ``rentFrequency``.

    Bayut publishes both ``yearly`` and ``monthly`` commercial rents.
    ``yearly`` listings keep ``price`` as-is; ``monthly`` listings get
    ``× 12``. Anything else — missing, ``"daily"``, ``"weekly"`` —
    returns ``None`` so the writer doesn't silently mis-annualize.
    """
    frequency, price = _read_frequency_and_price(state_listing, ld_listing)
    if frequency is None or price is None:
        logger.warning(
            "Bayut listing %s missing price or rentFrequency (price=%r, "
            "frequency=%r); skipping rather than guessing",
            listing_url, price, frequency,
        )
        return None

    if frequency == "yearly":
        return _decimal_round(price)
    if frequency == "monthly":
        return _decimal_round(price * 12)

    logger.warning(
        "Bayut listing %s has unsupported rentFrequency=%r; skipping",
        listing_url, frequency,
    )
    return None


def _read_frequency_and_price(
    state_listing: dict[str, Any] | None,
    ld_listing: dict[str, Any] | None,
) -> tuple[str | None, Decimal | None]:
    frequency: str | None = None
    price: Decimal | None = None

    if state_listing is not None:
        raw_freq = state_listing.get("rentFrequency")
        if isinstance(raw_freq, str):
            frequency = raw_freq.lower().strip() or None
        price = _parse_decimal(state_listing.get("price"))

    if frequency is None or price is None:
        # JSON-LD secondary path: priceSpecification carries both.
        if ld_listing is not None:
            main_entity = ld_listing.get("mainEntity")
            if isinstance(main_entity, dict):
                spec = main_entity.get("priceSpecification")
                if isinstance(spec, dict):
                    if frequency is None:
                        raw = spec.get("unitText")
                        if isinstance(raw, str):
                            frequency = raw.lower().strip() or None
                    if price is None:
                        price = _parse_decimal(spec.get("price"))

    return frequency, price


def _extract_permit_number(state_listing: dict[str, Any] | None) -> str | None:
    """Per-listing FAL advertisement permit — the cross-portal dedup key.

    Bayut exposes it at ``window.state.property.data.permitNumber`` (a
    10-digit Saudi REGA number, format-compatible with Aqar's
    ``aqar_advertisement_license``). The agency-level
    ``brokerage_and_marketing_license_number`` and the parcel-level
    ``rega_additional_info_deed_number`` (both in ``extraFields``) are
    intentionally NOT used.
    """
    if state_listing is None:
        return None
    value = state_listing.get("permitNumber")
    if value is None:
        extras = state_listing.get("extraFields")
        if isinstance(extras, dict):
            value = extras.get("rega_license_info_ad_license_number")
    if value is None:
        return None
    cleaned = re.sub(r"\s+", "", str(value)).strip()
    return cleaned or None


def _extract_contact_phone(
    state_listing: dict[str, Any] | None,
    ld_listing: dict[str, Any] | None,
) -> str | None:
    """First non-empty in priority order: mobileNumbers[0] → phoneNumbers[0]
    → whatsapp → JSON-LD ``realEstateAgent.telephone``.
    """
    candidates: list[Any] = []
    if state_listing is not None:
        phone = state_listing.get("phoneNumber")
        if isinstance(phone, dict):
            mobiles = phone.get("mobileNumbers")
            if isinstance(mobiles, list) and mobiles:
                candidates.append(mobiles[0])
            phones = phone.get("phoneNumbers")
            if isinstance(phones, list) and phones:
                candidates.append(phones[0])
            candidates.append(phone.get("proxyMobile"))
            candidates.append(phone.get("whatsapp"))
        candidates.append(state_listing.get("mobilePhoneNumber"))
        candidates.append(state_listing.get("primaryPhoneNumber"))

    if ld_listing is not None:
        main_entity = ld_listing.get("mainEntity")
        if isinstance(main_entity, dict):
            agent = main_entity.get("realEstateAgent")
            if isinstance(agent, dict):
                candidates.append(agent.get("telephone"))

    for candidate in candidates:
        if candidate:
            cleaned = str(candidate).strip()
            if cleaned:
                return cleaned
    return None


def _extract_neighborhood(
    state_listing: dict[str, Any] | None,
    ld_listing: dict[str, Any] | None,
) -> str | None:
    """Deepest level in the ``window.state`` location chain.

    Bayut populates ``location`` as a 4-element ladder
    ``[KSA, Riyadh, <Area>, <Neighborhood>]`` ordered by ``level``;
    the deepest entry is the most specific. Falls back to JSON-LD
    ``mainEntity.address.addressLocality`` (which is the area, not the
    neighborhood — coarser but stable).
    """
    if state_listing is not None:
        location = state_listing.get("location")
        if isinstance(location, list) and location:
            best_level = -1
            best_name: str | None = None
            for entry in location:
                if not isinstance(entry, dict):
                    continue
                level = entry.get("level")
                name = entry.get("name")
                if isinstance(level, int) and isinstance(name, str) and level > best_level:
                    if name.lower() not in ("ksa", "saudi arabia"):
                        best_level = level
                        best_name = name
            if best_name:
                return best_name.strip() or None

    if ld_listing is not None:
        main_entity = ld_listing.get("mainEntity")
        if isinstance(main_entity, dict):
            address = main_entity.get("address")
            if isinstance(address, dict):
                value = address.get("addressLocality")
                if isinstance(value, str) and value.strip():
                    return value.strip()

    return None


def _extract_created_at(
    ld_listing: dict[str, Any] | None,
    state_listing: dict[str, Any] | None,
) -> datetime | None:
    """JSON-LD ``datePosted`` (ISO date) → ``window.state.createdAt`` (Unix)."""
    if ld_listing is not None:
        parsed = _parse_iso_datetime(ld_listing.get("datePosted"))
        if parsed is not None:
            return parsed
    if state_listing is not None:
        parsed = _parse_unix_timestamp(state_listing.get("createdAt"))
        if parsed is not None:
            return parsed
    return None


def _extract_updated_at(
    state_listing: dict[str, Any] | None,
    ld_listing: dict[str, Any] | None,
) -> datetime | None:
    """``window.state.updatedAt`` (Unix epoch) → JSON-LD ``dateModified``
    (often missing on real Bayut). The state path wins because it's
    populated and JSON-LD's ``dateModified`` is empirically absent.
    """
    if state_listing is not None:
        parsed = _parse_unix_timestamp(state_listing.get("updatedAt"))
        if parsed is not None:
            return parsed
        parsed = _parse_unix_timestamp(state_listing.get("touchedAt"))
        if parsed is not None:
            return parsed
    if ld_listing is not None:
        parsed = _parse_iso_datetime(ld_listing.get("dateModified"))
        if parsed is not None:
            return parsed
    return None


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------


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


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if not text:
            return None
        try:
            return Decimal(text)
        except InvalidOperation:
            return None
    return None


def _decimal_round(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _parse_unix_timestamp(value: Any) -> datetime | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return None
    return None
