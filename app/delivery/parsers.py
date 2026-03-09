"""
Per-platform parsers for delivery data.

Each parser converts raw scraper output (dict or HTML content) into
structured ``DeliveryRecord`` instances.  The ``parse_legacy_record``
function handles the existing scraper output format (name + URL dicts).

Enhanced parsers can extract richer data from page HTML or JSON payloads
when available.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from app.delivery.schemas import DeliveryRecord, GeocodeMethod, Platform

logger = logging.getLogger(__name__)

# Known Riyadh district names for extraction from URLs and text
RIYADH_DISTRICTS = [
    "al-olaya", "olaya", "العليا",
    "al-malaz", "malaz", "الملز",
    "al-murabba", "murabba", "المربع",
    "al-naseem", "naseem", "النسيم",
    "al-rawdah", "rawdah", "الروضة",
    "al-sulaymaniyah", "sulaymaniyah", "السليمانية",
    "al-shifa", "shifa", "الشفاء",
    "al-batha", "batha", "البطحاء",
    "al-worood", "worood", "الورود",
    "al-nakheel", "nakheel", "النخيل",
    "al-hamra", "hamra", "الحمراء",
    "al-yasmin", "yasmin", "الياسمين",
    "al-narjis", "narjis", "النرجس",
    "al-aqiq", "aqiq", "العقيق",
    "al-sahafah", "sahafah", "الصحافة",
    "hittin", "حطين",
    "al-takhassusi", "takhassusi", "التخصصي",
    "al-rabwah", "rabwah", "الربوة",
    "al-khaleej", "khaleej", "الخليج",
    "al-wadi", "wadi", "الوادي",
    "al-ghadir", "ghadir", "الغدير",
    "al-arid", "arid", "العارض",
    "al-qayrawan", "qayrawan", "القيروان",
    "al-rimal", "rimal", "الرمال",
    "tuwaiq", "طويق",
    "al-dar-al-baida", "dar-al-baida", "الدار البيضاء",
    "irqah", "عرقة",
    "al-shuhada", "shuhada", "الشهداء",
    "al-suwaidi", "suwaidi", "السويدي",
    "al-aziziyah", "aziziyah", "العزيزية",
    "al-khuzama", "khuzama", "الخزامى",
    "al-munsiyah", "munsiyah", "المنسية",
    "al-andalus", "andalus", "الأندلس",
]

# Known chain brands in Saudi Arabia
KNOWN_CHAINS = {
    "al baik": "Al Baik",
    "albaik": "Al Baik",
    "البيك": "Al Baik",
    "mcdonalds": "McDonald's",
    "mcdonald's": "McDonald's",
    "ماكدونالدز": "McDonald's",
    "burger king": "Burger King",
    "برجر كنج": "Burger King",
    "kfc": "KFC",
    "كنتاكي": "KFC",
    "hardees": "Hardee's",
    "hardee's": "Hardee's",
    "هرفي": "Herfy",
    "herfy": "Herfy",
    "kudu": "Kudu",
    "كودو": "Kudu",
    "pizza hut": "Pizza Hut",
    "بيتزا هت": "Pizza Hut",
    "dominos": "Domino's",
    "domino's": "Domino's",
    "دومينوز": "Domino's",
    "starbucks": "Starbucks",
    "ستاربكس": "Starbucks",
    "shawarmer": "Shawarmer",
    "شاورمر": "Shawarmer",
    "maestro pizza": "Maestro Pizza",
    "little caesars": "Little Caesars",
    "papa johns": "Papa John's",
    "papa john's": "Papa John's",
    "بابا جونز": "Papa John's",
    "subway": "Subway",
    "صب واي": "Subway",
    "baskin robbins": "Baskin Robbins",
    "dunkin": "Dunkin'",
    "dunkin'": "Dunkin'",
    "tim hortons": "Tim Hortons",
    "popeyes": "Popeyes",
    "texas chicken": "Texas Chicken",
    "chili's": "Chili's",
    "applebee's": "Applebee's",
    "the cheesecake factory": "The Cheesecake Factory",
    "shrimpy": "Shrimpy",
    "barns": "Barns",
    "بارنز": "Barns",
}


def _extract_district_from_url(url: str) -> str | None:
    """Try to extract a Riyadh district name from a URL path."""
    if not url:
        return None
    path = urlparse(url).path.lower()
    for district in RIYADH_DISTRICTS:
        if district.replace(" ", "-") in path or district in path:
            return district.replace("al-", "Al ").replace("-", " ").title()
    return None


def _extract_district_from_text(text_val: str) -> str | None:
    """Try to extract district from arbitrary text (name, address)."""
    if not text_val:
        return None
    lower = text_val.lower()
    for district in RIYADH_DISTRICTS:
        if district in lower:
            return district.replace("al-", "Al ").replace("-", " ").title()
    return None


def _detect_chain(name: str | None) -> str | None:
    """Detect if a restaurant name matches a known chain brand."""
    if not name:
        return None
    lower = name.lower().strip()
    for pattern, chain in KNOWN_CHAINS.items():
        if pattern in lower:
            return chain
    return None


def _extract_branch_from_name(name: str, chain: str | None) -> str | None:
    """Extract branch/location qualifier from name if a chain is detected."""
    if not chain or not name:
        return None
    # Common patterns: "McDonald's - Al Olaya", "KFC (Malaz Branch)"
    for sep in [" - ", " – ", " — ", " | "]:
        if sep in name:
            parts = name.split(sep)
            for part in parts[1:]:
                stripped = part.strip()
                if stripped and stripped.lower() != chain.lower():
                    return stripped
    branch_match = re.search(r"\(([^)]+)\)", name)
    if branch_match:
        return branch_match.group(1).strip()
    return None


def _estimate_parse_confidence(rec: DeliveryRecord) -> float:
    """Estimate how much useful data was extracted."""
    score = 0.0
    if rec.restaurant_name_raw:
        score += 0.25
    if rec.district_text or rec.area_text:
        score += 0.15
    if rec.lat and rec.lon:
        score += 0.20
    if rec.cuisine_raw or rec.category_raw:
        score += 0.15
    if rec.rating is not None:
        score += 0.10
    if rec.delivery_time_min or rec.delivery_fee is not None:
        score += 0.10
    if rec.brand_raw:
        score += 0.05
    return min(1.0, score)


def parse_legacy_record(raw: dict[str, Any], platform: str) -> DeliveryRecord:
    """
    Parse a legacy scraper output dict into a DeliveryRecord.

    The scrapers yield dicts with at minimum: {id, name, source, source_url}
    plus optional HTML-extracted fields: lat, lon, category_raw, rating,
    rating_count, address_raw, district_text, phone_raw.

    This parser extracts as much additional value as possible.
    """
    name = raw.get("name", "")
    source_url = raw.get("source_url", "")
    listing_id = raw.get("id", "")

    # Extract district — prefer HTML-extracted value, fall back to URL/name
    district = raw.get("district_text")
    if not district:
        district = _extract_district_from_url(source_url)
    if not district:
        district = _extract_district_from_text(name)

    # Detect chain brand
    chain = _detect_chain(name)
    branch = _extract_branch_from_name(name, chain)

    # Coordinates
    lat = raw.get("lat")
    lon = raw.get("lon")
    geocode_method = GeocodeMethod.NONE
    location_confidence = 0.0
    if lat is not None and lon is not None:
        geocode_method = GeocodeMethod.PLATFORM_PAYLOAD
        location_confidence = 0.9

    record = DeliveryRecord(
        platform=platform,
        source_listing_id=listing_id,
        source_url=source_url,
        restaurant_name_raw=name,
        cuisine_raw=raw.get("category_raw"),
        category_raw=raw.get("category_raw"),
        lat=lat,
        lon=lon,
        geocode_method=geocode_method,
        location_confidence=location_confidence,
        district_text=district,
        address_raw=raw.get("address_raw"),
        brand_raw=chain,
        branch_raw=branch,
        phone_raw=raw.get("phone_raw"),
        rating=raw.get("rating"),
        rating_count=raw.get("rating_count"),
        raw_payload=raw,
    )

    record.parse_confidence = _estimate_parse_confidence(record)
    return record


def parse_page_content(
    html: str,
    url: str,
    platform: str,
) -> DeliveryRecord | None:
    """
    Enhanced parser: extract structured data from HTML page content.

    Attempts to extract JSON-LD, Open Graph, and meta data from the page.
    Returns None if the page has no usable restaurant data.
    """
    import re as _re

    record = DeliveryRecord(
        platform=platform,
        source_url=url,
    )

    # Try JSON-LD
    json_ld_blocks = _extract_json_ld(html)
    for block in json_ld_blocks:
        block_type = block.get("@type", "")
        if block_type in ("Restaurant", "FoodEstablishment", "LocalBusiness"):
            record.restaurant_name_raw = block.get("name")
            addr = block.get("address", {})
            if isinstance(addr, dict):
                record.address_raw = addr.get("streetAddress")
                record.district_text = addr.get("addressLocality")
            geo = block.get("geo", {})
            if isinstance(geo, dict):
                try:
                    record.lat = float(geo.get("latitude", 0))
                    record.lon = float(geo.get("longitude", 0))
                    if record.lat and record.lon:
                        record.geocode_method = GeocodeMethod.JSON_LD
                        record.location_confidence = 0.85
                except (ValueError, TypeError):
                    pass
            cuisine = block.get("servesCuisine")
            if cuisine:
                record.cuisine_raw = (
                    cuisine if isinstance(cuisine, str) else ", ".join(cuisine)
                )
            agg_rating = block.get("aggregateRating", {})
            if isinstance(agg_rating, dict):
                try:
                    record.rating = float(agg_rating.get("ratingValue", 0))
                    record.rating_count = int(agg_rating.get("reviewCount", 0))
                except (ValueError, TypeError):
                    pass
            record.phone_raw = block.get("telephone")
            record.website_raw = block.get("url")
            record.menu_url = block.get("hasMenu", {}).get("url") if isinstance(
                block.get("hasMenu"), dict
            ) else block.get("hasMenu")
            break

    # Try Open Graph / meta tags
    if not record.restaurant_name_raw:
        og_title = _re.search(
            r'<meta\s+property=["\']og:title["\'][^>]*content=["\']([^"\']+)',
            html,
            _re.IGNORECASE,
        )
        if og_title:
            record.restaurant_name_raw = og_title.group(1).strip()

    # Extract district from URL if not found
    if not record.district_text:
        record.district_text = _extract_district_from_url(url)
    if not record.district_text and record.restaurant_name_raw:
        record.district_text = _extract_district_from_text(
            record.restaurant_name_raw
        )

    # Detect chain
    record.brand_raw = _detect_chain(record.restaurant_name_raw)
    record.branch_raw = _extract_branch_from_name(
        record.restaurant_name_raw or "", record.brand_raw
    )

    if not record.restaurant_name_raw:
        return None

    record.parse_confidence = _estimate_parse_confidence(record)
    return record


def _extract_json_ld(html: str) -> list[dict]:
    """Extract JSON-LD blocks from HTML."""
    results = []
    for m in re.finditer(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.DOTALL | re.IGNORECASE,
    ):
        try:
            obj = json.loads(m.group(1))
            if isinstance(obj, list):
                results.extend(obj)
            else:
                results.append(obj)
        except json.JSONDecodeError:
            pass
    return results
