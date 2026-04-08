#!/usr/bin/env python3
"""Aqar.fm crawler: fetch Riyadh commercial-for-rent listings by area/neighborhood."""

import argparse
import json
import logging
import os
import random
import re
import time
from decimal import Decimal

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

AREAS = [
    "north-of-riyadh",
    "south-of-riyadh",
    "east-of-riyadh",
    "west-of-riyadh",
    "center-of-riyadh",
]

LISTING_TYPES = {
    "store": "store-for-rent",
    "showroom": "showroom-for-rent",
    "warehouse": "warehouse-for-rent",
    "building": "building-for-rent",
}

_BASE_TMPL = "https://sa.aqar.fm/en/{listing_type}/riyadh"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
]


def _get(url: str) -> requests.Response:
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp


# ---------------------------------------------------------------------------
# Phase 1: area → neighborhood list
# ---------------------------------------------------------------------------


def fetch_area(area: str, listing_type: str = "store-for-rent") -> list[dict]:
    base = _BASE_TMPL.format(listing_type=listing_type)
    url = f"{base}/{area}"
    resp = _get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    results = []
    for link in soup.select("a[href]"):
        href = link.get("href", "")
        if f"/en/{listing_type}/riyadh/" not in href or href.endswith(f"/{area}"):
            continue
        # Skip listing card links — they contain description text, not neighborhood names
        if _CARD_HREF_RE.search(href):
            continue
        text = link.get_text(strip=True)
        # Skip filter links like "#more than 5m", "#more than 10m"
        if text.startswith("#"):
            continue
        # Skip links whose text is suspiciously long (likely card/description text)
        if len(text) > 120:
            continue
        count = 0
        if "(" in text and text.endswith(")"):
            parts = text.rsplit("(", 1)
            text = parts[0].strip()
            count = int(parts[1].rstrip(")").replace(",", "") or 0)
        if text:
            slug = href.rstrip("/").rsplit("/", 1)[-1]
            full_url = href if href.startswith("http") else f"https://sa.aqar.fm{href}"
            results.append({"neighborhood": text, "slug": slug, "url": full_url, "count": count})
    return results


# ---------------------------------------------------------------------------
# Phase 2: neighborhood page → listing cards
# ---------------------------------------------------------------------------

_CARD_HREF_RE = re.compile(r"(?:store|showroom|warehouse|building)-for-rent-(\d+)")
_AREA_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*m²")
_STREET_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*m\b")


def _parse_listing_from_card(card_link, neighborhood: str) -> dict | None:
    """Parse a listing from an <a> card link matching the Aqar HTML structure.

    ``card_link`` is an <a> tag whose href contains ``store-for-rent-NNNN``.
    """
    href = card_link.get("href", "")
    m = _CARD_HREF_RE.search(href)
    if not m:
        return None
    aqar_id = m.group(1)
    full_url = href if href.startswith("http") else f"https://sa.aqar.fm{href}"

    # Title: <span class="line-clamp-1">
    title = ""
    title_el = card_link.find("span", class_=re.compile(r"line-clamp-1"))
    if title_el:
        title = title_el.get_text(strip=True)

    # Price: text near <span class="sar">, extract digits
    price = None
    sar_el = card_link.find("span", class_="sar")
    if sar_el and sar_el.parent:
        price_text = sar_el.parent.get_text(" ", strip=True)
        digits = re.sub(r"[^\d]", "", price_text)
        if digits:
            price = int(digits)

    # Area and street width: find <span> elements after area.svg / street.svg icons
    area_sqm = None
    street_width = None

    for img_el in card_link.find_all("img", src=True):
        src = img_el.get("src", "")
        # Find the next sibling <span> with the value
        sibling = img_el.find_next("span")
        if not sibling:
            continue
        span_text = sibling.get_text(strip=True)

        if "area.svg" in src:
            am = _AREA_RE.search(span_text)
            if am:
                area_sqm = float(am.group(1).replace(",", ""))
        elif "street.svg" in src:
            sm = _STREET_RE.search(span_text)
            if sm:
                street_width = float(sm.group(1).replace(",", ""))

    # Neighborhood: extract from a dedicated location element in the card
    # rather than relying solely on the page-level neighborhood parameter,
    # which can pick up description text from card links on the area page.
    card_neighborhood = None
    for loc_img in card_link.find_all("img", src=True):
        if "location" in loc_img.get("src", ""):
            loc_span = loc_img.find_next("span")
            if loc_span:
                loc_text = loc_span.get_text(strip=True)
                if loc_text:
                    card_neighborhood = loc_text
            break
    # Fall back to page-level neighborhood, but guard against description
    # text that was accidentally captured as a neighborhood name.
    resolved_neighborhood = card_neighborhood or neighborhood
    if resolved_neighborhood and len(resolved_neighborhood) > 120:
        resolved_neighborhood = neighborhood if card_neighborhood else None

    # Description: hidden lg:block div
    desc = ""
    desc_el = card_link.find("div", class_=re.compile(r"hidden\s+lg:block|lg:block\s+hidden"))
    if desc_el:
        desc = desc_el.get_text(strip=True)[:200]

    # Image: <img> with src containing images.aqar.fm or props/
    img = None
    for img_tag in card_link.find_all("img", src=True):
        img_src = img_tag["src"]
        if "images.aqar.fm" in img_src or "props/" in img_src:
            img = img_src
            if img.startswith("//"):
                img = "https:" + img
            break

    return {
        "aqar_id": aqar_id,
        "title": title,
        "price_sar_annual": price,
        "area_sqm": area_sqm,
        "street_width_m": street_width,
        "description": desc,
        "neighborhood": resolved_neighborhood,
        "image_url": img,
        "listing_url": full_url,
    }


def _find_next_page_url(soup: BeautifulSoup, current_url: str) -> str | None:
    """Find the 'next page' link from pagination."""
    # Look for rel="next" or aria-label="Next" or text ">"/"Next"
    nxt = soup.find("a", rel="next")
    if not nxt:
        nxt = soup.find("a", attrs={"aria-label": re.compile(r"next", re.I)})
    if not nxt:
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True)
            if txt in (">", "›", "Next", "التالي"):
                nxt = a
                break
    if not nxt:
        # Try ?page=N+1 pattern
        m = re.search(r"[?&]page=(\d+)", current_url)
        cur_page = int(m.group(1)) if m else 1
        next_candidate = f"?page={cur_page + 1}" if "?" not in current_url else re.sub(
            r"page=\d+", f"page={cur_page + 1}", current_url
        )
        # Only return if we saw pagination indicators
        if soup.find(class_=re.compile(r"pagination|pager", re.I)):
            return next_candidate if next_candidate != current_url else None
        return None
    href = nxt.get("href", "")
    if not href or href == "#":
        return None
    return href if href.startswith("http") else f"https://sa.aqar.fm{href}"


def fetch_neighborhood_listings(neighborhood_url: str, neighborhood_name: str, max_pages: int = 10) -> list[dict]:
    """Fetch all listing cards from a neighborhood page, following pagination."""
    all_listings = []
    url = neighborhood_url
    seen_ids: set[str] = set()

    for page in range(1, max_pages + 1):
        if not url:
            break
        if page > 1:
            time.sleep(2)
        print(f"    [page {page}] {url}")
        resp = _get(url)
        html = resp.text

        listings_on_page = []

        soup = BeautifulSoup(html, "html.parser")
        card_links = soup.find_all("a", href=_CARD_HREF_RE)
        for card_link in card_links:
            parsed = _parse_listing_from_card(card_link, neighborhood_name)
            if parsed and parsed["aqar_id"] not in seen_ids:
                seen_ids.add(parsed["aqar_id"])
                listings_on_page.append(parsed)

        if not listings_on_page:
            break

        all_listings.extend(listings_on_page)

        # Find next page
        url = _find_next_page_url(soup, url)

    return all_listings


# ---------------------------------------------------------------------------
# Phase 3: listing detail page → extra fields
# ---------------------------------------------------------------------------

_MEZZANINE_RE = re.compile(r"mezzanine|ميزانين|ميزنين|طابق علوي|دور علوي", re.I)
_DRIVE_THRU_RE = re.compile(r"drive.?thr(?:u|ough)|درايف ثرو|طلبات سيارات|خدمة سيارات", re.I)
_FACADE_RE = re.compile(
    r"(?:fac(?:ing|ade)|direction|واجهة|اتجاه)[:\s]*(north|south|east|west|شمال|جنوب|شرق|غرب)",
    re.I,
)
_FLOORS_RE = re.compile(r"(?:floors?|stories|طوابق|أدوار|ادوار|طابق)[:\s]*(\d+)", re.I)
_FLOORS_RE2 = re.compile(r"(\d+)\s*(?:floors?|stories|طوابق|أدوار|ادوار)", re.I)
_PHONE_RE = re.compile(r"(?:\+966|05|5)\d[\d\s\-]{7,10}")


def _search_text_bool(pattern: re.Pattern, *texts: str) -> bool:
    return any(pattern.search(t) for t in texts if t)


def _search_text_match(pattern: re.Pattern, *texts: str) -> re.Match | None:
    for t in texts:
        if t:
            m = pattern.search(t)
            if m:
                return m
    return None


def _extract_property_type(soup: BeautifulSoup) -> str | None:
    """Extract the 'Property Type' field from the Listing Details section.

    Aqar only sets this field for residential listings. Offices and
    commercial buildings typically leave it blank.

    Returns the property type string (e.g. 'Residential') or None.
    """
    # Strategy 1: Find any element whose text contains exactly "Property Type"
    for label_el in soup.find_all(string=lambda s: s and "Property Type" in s):
        parent = label_el.parent
        if parent is None:
            continue

        next_sib = parent.find_next_sibling()
        if next_sib and next_sib.get_text(strip=True):
            value = next_sib.get_text(strip=True)
            if value and value != "Property Type":
                return value

        if parent.parent:
            next_sib = parent.parent.find_next_sibling()
            if next_sib and next_sib.get_text(strip=True):
                value = next_sib.get_text(strip=True)
                if value and value != "Property Type":
                    return value

        parent_text = parent.get_text(separator="|", strip=True)
        parts = [p.strip() for p in parent_text.split("|") if p.strip()]
        if "Property Type" in parts:
            idx = parts.index("Property Type")
            if idx + 1 < len(parts):
                return parts[idx + 1]

    # Strategy 2: Regex on the raw HTML as a fallback
    html_str = str(soup)
    match = re.search(
        r"Property Type[^<>]*</[^>]+>\s*<[^>]+>([A-Za-z]+)",
        html_str,
    )
    if match:
        return match.group(1).strip()

    # Strategy 3: Arabic equivalent (نوع العقار)
    for label_el in soup.find_all(string=lambda s: s and "نوع العقار" in s):
        parent = label_el.parent
        if parent is None:
            continue
        next_sib = parent.find_next_sibling()
        if next_sib and next_sib.get_text(strip=True):
            return next_sib.get_text(strip=True)

    return None


def _extract_is_furnished(soup: BeautifulSoup) -> bool:
    """Detect whether the Aqar 'Features' section includes 'Furnished'.

    The Furnished feature is a strong negative signal for F&B because
    operators want unfurnished shells (they install their own kitchen,
    equipment, fixtures).

    Returns True if 'Furnished' is found, False otherwise.
    """
    for el in soup.find_all(string=lambda s: s and s.strip() == "Furnished"):
        return True

    for el in soup.find_all(string=lambda s: s and "furnished" in s.lower()):
        text = el.strip().lower()
        if "unfurnished" in text:
            continue
        if text in ("furnished", "fully furnished") or text.startswith("furnished"):
            return True

    # Arabic equivalent (مفروش / مفروشة)
    for el in soup.find_all(string=lambda s: s and ("مفروش" in s or "مفروشة" in s)):
        return True

    return False


def _extract_description(soup: BeautifulSoup) -> str | None:
    """Extract the actual ad body description from an Aqar listing detail page.

    The description sits between the price heading (### NNN §/annually or
    ### NNN §/سنوي) and the "Listing Details" / "تفاصيل الإعلان" heading.

    Strategy: find the price-containing element, then walk forward collecting
    text until we hit the Listing Details heading. Return the accumulated text
    if it's substantial enough.
    """
    # Markers that indicate we've left the description area
    STOP_MARKERS = (
        "Listing Details",
        "تفاصيل الإعلان",
        "Property Type",
        "نوع العقار",
        "Street direction",
        "الواجهة",
        "Street width",
        "عرض الشارع",
        "Apartments",
        "عدد الشقق",
        "Advertisement License",
        "رخصة الإعلان",
        "Plan and Parcel",
        "المخطط",
        "Created At",
        "تاريخ الإضافة",
        "View more",
        "عرض المزيد",
    )

    # Markers that indicate the price element (description starts after this)
    PRICE_RE = re.compile(r"[§﷼]\s*\d|annually|سنوي|/yr|/year")

    # Strategy 1: Find any element whose text matches a price pattern,
    # then walk through its next siblings collecting substantial text blocks.
    price_el = None
    for el in soup.find_all(["h3", "h2", "div", "span", "p"]):
        el_text = el.get_text(strip=True) if hasattr(el, "get_text") else ""
        if el_text and PRICE_RE.search(el_text) and len(el_text) < 100:
            price_el = el
            break

    if price_el is None:
        # Fallback: find any string node with price marker
        price_str = soup.find(string=lambda s: s and PRICE_RE.search(s) and len(s) < 100)
        if price_str:
            price_el = price_str.parent

    if price_el is None:
        return None

    # Walk forward from the price element collecting text
    collected_parts: list[str] = []
    current = price_el

    # Try walking through next siblings of price element AND its parents
    for _ in range(40):  # max 40 hops to prevent infinite loops
        current = current.find_next() if current else None
        if current is None:
            break

        cur_text = current.get_text(separator=" ", strip=True) if hasattr(current, "get_text") else str(current).strip()
        if not cur_text:
            continue

        # Stop if we hit a marker indicating we've left the description
        if any(marker in cur_text for marker in STOP_MARKERS):
            break

        # Skip very short fragments (likely UI noise)
        if len(cur_text) < 15:
            continue

        # Skip if this looks like an image alt text or navigation
        if cur_text.startswith("صورة") or cur_text.startswith("image"):
            continue

        # Avoid duplicates (BeautifulSoup walks return parent + child text)
        if collected_parts and cur_text in collected_parts[-1]:
            continue
        if collected_parts and collected_parts[-1] in cur_text:
            collected_parts[-1] = cur_text  # prefer the more complete text
            continue

        collected_parts.append(cur_text)

        # If we have a substantial chunk already, that's enough
        if sum(len(p) for p in collected_parts) > 300:
            break

    if not collected_parts:
        return None

    description = " ".join(collected_parts).strip()
    if len(description) < 20:
        return None

    return description[:2000]


def _parse_apartment_count(value: str) -> int | None:
    """Parse an apartments count value. Returns int, or None if not numeric.

    'None', 'لا يوجد', '0', '' → None (treated as no apartments)
    '7' → 7
    '٧' → 7 (Arabic numerals)
    """
    if not value:
        return None

    v = value.strip().lower()
    if v in ("none", "لا يوجد", "بدون", "0", "صفر", "no", "n/a"):
        return None

    # Convert Arabic numerals to ASCII
    arabic_to_ascii = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
    v = v.translate(arabic_to_ascii)

    # Extract first number from string
    match = re.search(r"\d+", v)
    if match:
        try:
            count = int(match.group())
            return count if count > 0 else None
        except ValueError:
            return None

    return None


def _extract_apartments_count(soup: BeautifulSoup) -> int | None:
    """Extract the 'Apartments' field from the Listing Details section.

    Aqar lists this as 'Apartments' (English) or 'عدد الشقق' (Arabic).
    The value is either a number ('7') or 'None' / 'لا يوجد' / blank.

    A value of 2 or more is a strong residential signal — multi-unit
    residential buildings are not suitable for F&B.

    Returns the integer count, or None if not found / not numeric / 'None'.
    """
    label_variants = ("Apartments", "عدد الشقق")

    for label_text in label_variants:
        for label_el in soup.find_all(string=lambda s: s and label_text in s):
            parent = label_el.parent
            if parent is None:
                continue

            # Try next sibling
            next_sib = parent.find_next_sibling()
            if next_sib:
                value_text = next_sib.get_text(strip=True)
                count = _parse_apartment_count(value_text)
                if count is not None:
                    return count

            # Try parent's next sibling
            if parent.parent:
                next_sib = parent.parent.find_next_sibling()
                if next_sib:
                    value_text = next_sib.get_text(strip=True)
                    count = _parse_apartment_count(value_text)
                    if count is not None:
                        return count

            # Try parsing the parent's combined text after the label
            parent_text = parent.get_text(separator="|", strip=True)
            parts = [p.strip() for p in parent_text.split("|") if p.strip()]
            if label_text in parts:
                idx = parts.index(label_text)
                if idx + 1 < len(parts):
                    count = _parse_apartment_count(parts[idx + 1])
                    if count is not None:
                        return count

    return None


def _extract_num_rooms(soup: BeautifulSoup) -> int | None:
    """Extract the 'Rooms' field from the Listing Details section.

    Aqar lists this as 'Rooms' (English) or 'عدد الغرف' (Arabic).
    The value is a number ('18', '20', '6') or 'None' / blank.

    A value of 6 or more on a building listing is a strong non-F&B signal:
      - Residential apartment buildings (bedrooms counted as rooms)
      - Multi-office commercial buildings
      - Clinics / hotels / mixed-use
    None of these are suitable for a QSR storefront conversion.

    Returns the integer count, or None if not found / not numeric / 'None'.
    """
    label_variants = ("Rooms", "عدد الغرف", "الغرف")

    for label_text in label_variants:
        for label_el in soup.find_all(string=lambda s: s and label_text in s):
            # Skip false positives where the label is embedded in a longer string
            parent_text = label_el.parent.get_text(strip=True) if label_el.parent else ""
            if len(parent_text) > len(label_text) + 10:
                continue

            parent = label_el.parent
            if parent is None:
                continue

            # Try next sibling
            next_sib = parent.find_next_sibling()
            if next_sib:
                value_text = next_sib.get_text(strip=True)
                count = _parse_apartment_count(value_text)
                if count is not None:
                    return count

            # Try parent's next sibling
            if parent.parent:
                next_sib = parent.parent.find_next_sibling()
                if next_sib:
                    value_text = next_sib.get_text(strip=True)
                    count = _parse_apartment_count(value_text)
                    if count is not None:
                        return count

            # Try parsing the parent's combined text after the label
            parent_combined = parent.get_text(separator="|", strip=True)
            parts = [p.strip() for p in parent_combined.split("|") if p.strip()]
            if label_text in parts:
                idx = parts.index(label_text)
                if idx + 1 < len(parts):
                    count = _parse_apartment_count(parts[idx + 1])
                    if count is not None:
                        return count

    return None


# Bedroom-related keywords. Presence of ANY of these in a description is a
# near-100% precision residential signal — commercial F&B shells, offices,
# warehouses, and showrooms never describe themselves with bedroom counts.
_BEDROOM_KEYWORDS = (
    # English
    "bedroom",
    "bedrooms",
    "master bedroom",
    # Arabic
    "غرفة نوم",
    "غرف نوم",
    "غرفة ماستر",
    "ماستر",
)


def _has_bedroom_keywords(description: str | None) -> bool:
    """Return True if the description contains any bedroom-related keyword.

    Bedrooms are a near-perfect residential signal — they never appear in
    commercial real estate vocabulary. A single match is sufficient.

    Case-insensitive for English; Arabic is case-less so direct match works.
    """
    if not description:
        return False
    desc_lower = description.lower()
    return any(kw in desc_lower for kw in _BEDROOM_KEYWORDS)


def _extract_detail_from_html(html: str) -> dict:
    """Extract detail fields by parsing the detail page HTML."""
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    phone = None
    # Look for tel: links first
    tel_link = soup.find("a", href=re.compile(r"^tel:"))
    if tel_link:
        phone = tel_link["href"].replace("tel:", "").strip()
    if not phone:
        pm = _PHONE_RE.search(page_text)
        phone = pm.group(0).strip() if pm else None

    # Extract the actual ad description (not metadata)
    desc = _extract_description(soup) or ""

    facade = None
    fm = _FACADE_RE.search(page_text)
    if fm:
        facade = fm.group(1)

    num_floors = None
    flm = _FLOORS_RE.search(page_text) or _FLOORS_RE2.search(page_text)
    if flm:
        num_floors = int(flm.group(1))

    return {
        "full_description": desc,
        "has_mezzanine": _search_text_bool(_MEZZANINE_RE, page_text),
        "has_drive_thru": _search_text_bool(_DRIVE_THRU_RE, page_text),
        "facade_direction": facade,
        "contact_phone": phone,
        "num_floors": num_floors,
        "property_type": _extract_property_type(soup),
        "is_furnished": _extract_is_furnished(soup),
        "apartments_count": _extract_apartments_count(soup),
        "num_rooms": _extract_num_rooms(soup),
    }


def fetch_listing_detail(listing: dict) -> dict:
    """Fetch a listing's detail page and merge extra fields into the listing dict."""
    url = listing["listing_url"]
    try:
        resp = _get(url)
    except requests.RequestException as e:
        print(f"      WARN: failed to fetch detail {url}: {e}")
        return listing

    html = resp.text
    detail = _extract_detail_from_html(html)

    # Upgrade the truncated description to full
    if detail["full_description"]:
        listing["description"] = detail["full_description"]
    listing["has_mezzanine"] = detail["has_mezzanine"]
    listing["has_drive_thru"] = detail["has_drive_thru"]
    listing["facade_direction"] = detail["facade_direction"]
    listing["contact_phone"] = detail["contact_phone"]
    listing["num_floors"] = detail["num_floors"]

    # Extract Aqar's structured Property Type field (set on residential listings)
    if detail["property_type"]:
        listing["property_type"] = detail["property_type"]

    # Extract Furnished feature flag (negative signal for F&B)
    listing["is_furnished"] = detail["is_furnished"]

    # Extract apartments count (>=2 on a building = residential)
    listing["apartments_count"] = detail["apartments_count"]

    # Extract rooms count (>=6 on a building = multi-room residential/office)
    listing["num_rooms"] = detail["num_rooms"]

    return listing


# ---------------------------------------------------------------------------
# Phase 4: geocode neighborhoods via Google Maps + DB cache
# ---------------------------------------------------------------------------

# Riyadh bounding box for validation
_RIYADH_LAT_MIN, _RIYADH_LAT_MAX = 24.4, 25.1
_RIYADH_LON_MIN, _RIYADH_LON_MAX = 46.4, 47.0

GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def _get_db_session():
    """Create a DB session using the app's connection pattern."""
    from app.db.session import SessionLocal

    return SessionLocal()


def _purge_null_geocode_cache(db) -> int:
    """Delete geocode_cache rows where lat IS NULL (poisoned entries from failed queries).

    Returns the number of rows deleted.
    """
    from sqlalchemy import text as sa_text

    result = db.execute(sa_text("DELETE FROM geocode_cache WHERE lat IS NULL"))
    db.commit()
    deleted = result.rowcount
    if deleted:
        print(f"  Purged {deleted} poisoned geocode_cache rows (lat IS NULL)")
    return deleted


def _check_geocode_cache(db, query: str) -> dict | None:
    """Look up a geocode query in the cache table. Returns dict or None."""
    from sqlalchemy import text as sa_text

    row = db.execute(
        sa_text("SELECT lat, lon, formatted_address, raw FROM geocode_cache WHERE query = :q"),
        {"q": query},
    ).mappings().first()
    if row:
        return {
            "lat": float(row["lat"]) if row["lat"] is not None else None,
            "lon": float(row["lon"]) if row["lon"] is not None else None,
            "formatted_address": row["formatted_address"],
            "source": "cache",
        }
    return None


def _store_geocode_cache(db, query: str, lat: float | None, lon: float | None,
                         formatted_address: str | None, raw: dict) -> None:
    """Insert or update a geocode result in the cache table."""
    from sqlalchemy import text as sa_text

    db.execute(
        sa_text(
            "INSERT INTO geocode_cache (query, lat, lon, formatted_address, raw) "
            "VALUES (:q, :lat, :lon, :addr, CAST(:raw AS jsonb)) "
            "ON CONFLICT (query) DO UPDATE SET lat=:lat, lon=:lon, "
            "formatted_address=:addr, raw=CAST(:raw AS jsonb)"
        ),
        {
            "q": query,
            "lat": Decimal(str(lat)) if lat is not None else None,
            "lon": Decimal(str(lon)) if lon is not None else None,
            "addr": formatted_address,
            "raw": json.dumps(raw),
        },
    )
    db.commit()


def _validate_riyadh_coords(lat: float, lon: float) -> bool:
    """Check that coordinates fall within the Riyadh bounding box."""
    return (_RIYADH_LAT_MIN <= lat <= _RIYADH_LAT_MAX
            and _RIYADH_LON_MIN <= lon <= _RIYADH_LON_MAX)


def geocode_neighborhood(neighborhood_name: str, api_key: str, db=None) -> dict | None:
    """Geocode a neighborhood name. Uses DB cache, falls back to Google Maps API.

    Returns dict with lat, lon, formatted_address, source or None if invalid/failed.
    """
    query = f"{neighborhood_name}, Riyadh, Saudi Arabia"

    # Check cache first
    if db is not None:
        cached = _check_geocode_cache(db, query)
        if cached:
            print(f"      geocode (cache): {neighborhood_name} → {cached['lat']}, {cached['lon']}")
            return cached

    # Call Google Maps Geocoding API
    try:
        resp = requests.get(
            GEOCODE_URL,
            params={
                "address": query,
                "key": api_key,
                "components": "country:SA",
                "bounds": "24.5,46.2|25.1,47.3",
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"      WARN: geocode API error for '{query}': {e}")
        return None

    if data.get("status") != "OK" or not data.get("results"):
        print(f"      WARN: geocode no results for '{query}' (status={data.get('status')})")
        # Cache the miss to avoid re-querying
        if db is not None:
            _store_geocode_cache(db, query, None, None, None, data)
        return None

    result = data["results"][0]
    loc = result["geometry"]["location"]
    lat, lon = loc["lat"], loc["lng"]
    formatted_address = result.get("formatted_address", "")

    # Cache regardless of validation (the raw data is still useful)
    if db is not None:
        _store_geocode_cache(db, query, lat, lon, formatted_address, data)

    # Validate within Riyadh bounds
    if not _validate_riyadh_coords(lat, lon):
        print(f"      WARN: geocode out of Riyadh bounds: {neighborhood_name} → {lat}, {lon}")
        return None

    print(f"      geocode (API): {neighborhood_name} → {lat}, {lon}")
    return {"lat": lat, "lon": lon, "formatted_address": formatted_address, "source": "api"}


# ---------------------------------------------------------------------------
# Phase 5: restaurant suitability classification
# ---------------------------------------------------------------------------

_STRONG_POSITIVE_KW = [
    "مطعم", "restaurant", "كافيه", "café", "cafe", "drive-thru", "drive thru",
    "درايف ثرو", "مطبخ", "kitchen",
]
_MODERATE_POSITIVE_KW = [
    "محل تجاري", "معرض", "أرضي", "ground floor", "واجهة", "facade", "frontage",
]
_NEGATIVE_KW = [
    "مستودع", "warehouse", "مكتب إداري", "administrative office", "عيادة", "clinic",
]

_STRONG_POS_RE = re.compile("|".join(re.escape(k) for k in _STRONG_POSITIVE_KW), re.I)
_MOD_POS_RE = re.compile("|".join(re.escape(k) for k in _MODERATE_POSITIVE_KW), re.I)
_NEG_RE = re.compile("|".join(re.escape(k) for k in _NEGATIVE_KW), re.I)

_SUITABILITY_THRESHOLD = 25

# Listing types that can plausibly host an F&B operator. Any other
# listing_type (warehouse, building, land, rest_house, farm, etc.) is
# structurally not an F&B candidate regardless of area, rent, or other
# signals — hard-excluded from restaurant_suitable.
_FNB_COMPATIBLE_LISTING_TYPES: frozenset[str] = frozenset({"store", "showroom"})


def _is_fnb_compatible_listing_type(listing_type: str | None) -> bool:
    if not listing_type:
        return False
    return listing_type.strip().lower() in _FNB_COMPATIBLE_LISTING_TYPES


def classify_restaurant_suitability(listing: dict) -> dict:
    """Score a listing for restaurant suitability based on keywords and physical traits.

    Mutates listing in-place, adding restaurant_score, restaurant_suitable, and
    restaurant_signals fields. Returns the listing.
    """
    # Hard gate: listing must be a structurally F&B-compatible product type.
    # This runs before all other checks — warehouses, full buildings, land,
    # etc. are not F&B candidates regardless of their area or rent signals.
    if not _is_fnb_compatible_listing_type(listing.get("listing_type")):
        listing["restaurant_score"] = 0
        listing["restaurant_suitable"] = False
        listing["restaurant_signals"] = [
            f"REJECTED listing_type_not_fnb_compatible: "
            f"'{listing.get('listing_type')}'"
        ]
        return listing

    score = 0
    signals: list[str] = []

    # ── Residential listing detection (negative classification) ──
    # Aqar's building-for-rent category mixes commercial buildings with
    # residential buildings (apartments, accommodation, furnished units).
    # These are not suitable for F&B expansion and must be excluded.
    title = listing.get("title", "") or ""
    desc = listing.get("description", "") or ""
    combined_text = f"{title} {desc}".lower()

    # Arabic residential keywords
    _RESIDENTIAL_AR_KEYWORDS = [
        "شقة", "شقق", "سكني", "سكنية", "للسكن",
        "دور سكني", "دور للسكن", "غرفة نوم", "غرف نوم",
        "مفروشة", "مفروش", "عوائل", "عزاب",
        "شقق مفروشة", "سكن طالبات", "سكن موظفات", "سكن عمال",
        "استوديو", "ستوديو",
        "عمارة سكنية", "مبنى سكني", "بناء سكني",
        "فلة", "فيلا", "دوبلكس",
    ]

    # English residential keywords
    _RESIDENTIAL_EN_KEYWORDS = [
        "apartment", "apartments", "residential",
        "accommodation", "accomodation",
        "housing", "furnished",
        "studio", "bedroom", "bedrooms",
        "women's accommodation", "womens accommodation",
        "employees accommodation", "students accommodation",
        "workers housing", "staff housing",
        "villa", "duplex", "townhouse",
        "single family", "single-family",
        "for living", "for residence",
    ]

    is_residential = False
    matched_keyword = None

    # Check Arabic keywords (substring match in original case)
    title_desc_raw = f"{title} {desc}"
    for kw in _RESIDENTIAL_AR_KEYWORDS:
        if kw in title_desc_raw:
            is_residential = True
            matched_keyword = kw
            break

    # Check English keywords (lowercase match)
    if not is_residential:
        for kw in _RESIDENTIAL_EN_KEYWORDS:
            if kw in combined_text:
                is_residential = True
                matched_keyword = kw
                break

    if is_residential:
        listing["restaurant_score"] = 0
        listing["restaurant_suitable"] = False
        listing["restaurant_signals"] = [f"REJECTED residential: matched '{matched_keyword}'"]
        return listing

    # All listings from the scraper are commercial spaces,
    # so they get a base score reflecting inherent restaurant potential.
    score += 15
    signals.append("+15 commercial_retail_base")

    title = listing.get("title", "") or ""
    desc = listing.get("description", "") or ""
    combined = f"{title} {desc}"

    # --- Keyword scoring ---
    strong_hits = _STRONG_POS_RE.findall(combined)
    if strong_hits:
        score += 30
        signals.append(f"+30 strong_kw({','.join(set(strong_hits))})")

    mod_hits = _MOD_POS_RE.findall(combined)
    if mod_hits:
        score += 15
        signals.append(f"+15 moderate_kw({','.join(set(mod_hits))})")

    neg_hits = _NEG_RE.findall(combined)
    if neg_hits:
        score -= 20
        signals.append(f"-20 negative_kw({','.join(set(neg_hits))})")

    # --- Physical characteristic scoring ---
    area = listing.get("area_sqm")
    if area is not None:
        if 40 <= area <= 500:
            score += 10
            signals.append(f"+10 area_ok({area}sqm)")
        if area < 15 or area > 2000:
            score -= 20
            signals.append(f"-20 area_extreme({area}sqm)")

    street_w = listing.get("street_width_m")
    if street_w is not None and street_w >= 20:
        score += 5
        signals.append(f"+5 wide_street({street_w}m)")

    if listing.get("has_drive_thru"):
        score += 20
        signals.append("+20 has_drive_thru")

    if listing.get("has_mezzanine"):
        score += 5
        signals.append("+5 has_mezzanine")

    listing["restaurant_score"] = score
    listing["restaurant_suitable"] = True  # Commercial listing — passed residential rejection
    listing["restaurant_signals"] = signals
    return listing


# ---------------------------------------------------------------------------
# Phase 6: upsert listings into commercial_unit table
# ---------------------------------------------------------------------------

_STALE_DAYS = 28


def _compute_price_per_sqm(listing: dict) -> float | None:
    """Derive price per sqm from annual price and area."""
    price = listing.get("price_sar_annual")
    area = listing.get("area_sqm")
    if price and area and area > 0:
        return round(float(price) / float(area), 2)
    return None


def upsert_listing(db, listing: dict) -> str:
    """Insert or update a single listing in commercial_unit. Returns 'insert' or 'update'."""
    from sqlalchemy import text as sa_text

    aqar_id = listing["aqar_id"]
    listing["price_per_sqm"] = _compute_price_per_sqm(listing)

    existing = db.execute(
        sa_text("SELECT aqar_id FROM commercial_unit WHERE aqar_id = :id"),
        {"id": aqar_id},
    ).first()

    if existing:
        db.execute(
            sa_text(
                "UPDATE commercial_unit SET "
                "title = :title, description = :description, "
                "price_sar_annual = :price_sar_annual, price_per_sqm = :price_per_sqm, "
                "area_sqm = :area_sqm, street_width_m = :street_width_m, "
                "num_floors = :num_floors, has_mezzanine = :has_mezzanine, "
                "has_drive_thru = :has_drive_thru, facade_direction = :facade_direction, "
                "contact_phone = :contact_phone, "
                "listing_type = :listing_type, "
                "property_type = :property_type, "
                "is_furnished = :is_furnished, "
                "apartments_count = :apartments_count, "
                "num_rooms = :num_rooms, "
                "lat = COALESCE(:lat, commercial_unit.lat), "
                "lon = COALESCE(:lon, commercial_unit.lon), "
                "image_url = :image_url, listing_url = :listing_url, "
                "restaurant_score = :restaurant_score, "
                "restaurant_suitable = :restaurant_suitable, "
                "restaurant_signals = :restaurant_signals, "
                "status = 'active', last_seen_at = now() "
                "WHERE aqar_id = :aqar_id"
            ),
            _listing_params(listing),
        )
        return "update"
    else:
        db.execute(
            sa_text(
                "INSERT INTO commercial_unit "
                "(aqar_id, title, description, neighborhood, listing_url, image_url, "
                "price_sar_annual, price_per_sqm, area_sqm, street_width_m, "
                "num_floors, has_mezzanine, has_drive_thru, facade_direction, "
                "contact_phone, listing_type, property_type, is_furnished, apartments_count, num_rooms, lat, lon, "
                "restaurant_score, restaurant_suitable, restaurant_signals, "
                "status, first_seen_at, last_seen_at) "
                "VALUES (:aqar_id, :title, :description, :neighborhood, :listing_url, :image_url, "
                ":price_sar_annual, :price_per_sqm, :area_sqm, :street_width_m, "
                ":num_floors, :has_mezzanine, :has_drive_thru, :facade_direction, "
                ":contact_phone, :listing_type, :property_type, :is_furnished, :apartments_count, :num_rooms, :lat, :lon, "
                ":restaurant_score, :restaurant_suitable, :restaurant_signals, "
                "'active', now(), now())"
            ),
            _listing_params(listing),
        )
        return "insert"


def _listing_params(listing: dict) -> dict:
    """Build parameter dict for SQL statements from a listing dict."""
    return {
        "aqar_id": listing.get("aqar_id"),
        "title": listing.get("title"),
        "description": listing.get("description"),
        "neighborhood": listing.get("neighborhood"),
        "listing_url": listing.get("listing_url"),
        "image_url": listing.get("image_url"),
        "price_sar_annual": Decimal(str(listing["price_sar_annual"])) if listing.get("price_sar_annual") else None,
        "price_per_sqm": Decimal(str(listing["price_per_sqm"])) if listing.get("price_per_sqm") else None,
        "area_sqm": Decimal(str(listing["area_sqm"])) if listing.get("area_sqm") else None,
        "street_width_m": Decimal(str(listing["street_width_m"])) if listing.get("street_width_m") else None,
        "num_floors": listing.get("num_floors"),
        "has_mezzanine": listing.get("has_mezzanine"),
        "has_drive_thru": listing.get("has_drive_thru"),
        "facade_direction": listing.get("facade_direction"),
        "contact_phone": listing.get("contact_phone"),
        "listing_type": listing.get("listing_type"),
        "property_type": listing.get("property_type"),
        "is_furnished": listing.get("is_furnished"),
        "apartments_count": listing.get("apartments_count"),
        "num_rooms": listing.get("num_rooms"),
        "lat": Decimal(str(listing["lat"])) if listing.get("lat") else None,
        "lon": Decimal(str(listing["lon"])) if listing.get("lon") else None,
        "restaurant_score": listing.get("restaurant_score"),
        "restaurant_suitable": listing.get("restaurant_suitable"),
        "restaurant_signals": json.dumps(listing.get("restaurant_signals", [])),
    }


def mark_stale_listings(db, type_keys: list[str] | None = None) -> int:
    """Mark listings not seen in 28+ days as stale. Returns count of rows updated.

    When ``type_keys`` is given (e.g. ``["store", "showroom"]``), only listings
    whose ``listing_type`` matches one of the crawled types are eligible for
    stale-marking.  This prevents marking store listings as stale when only
    showroom pages were crawled (and vice-versa).
    """
    from sqlalchemy import text as sa_text

    where = (
        "status = 'active' "
        "AND last_seen_at < now() - make_interval(days => :days)"
    )
    params: dict = {"days": _STALE_DAYS}

    if type_keys:
        placeholders = []
        for i, tk in enumerate(type_keys):
            key = f"lt_{i}"
            params[key] = tk
            placeholders.append(f":{key}")
        where += f" AND listing_type IN ({', '.join(placeholders)})"

    result = db.execute(
        sa_text(f"UPDATE commercial_unit SET status = 'stale' WHERE {where}"),
        params,
    )
    return result.rowcount


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _listing_already_exists(aqar_id: str, db) -> bool:
    """Check if this listing already exists and was seen recently."""
    from sqlalchemy import text as sa_text

    result = db.execute(
        sa_text("""
            SELECT 1 FROM commercial_unit
            WHERE aqar_id = :aqar_id
              AND status = 'active'
              AND last_seen_at > NOW() - INTERVAL '7 days'
            LIMIT 1
        """),
        {"aqar_id": aqar_id},
    ).first()
    return result is not None


def _touch_last_seen(aqar_id: str, db) -> None:
    """Update last_seen_at for an already-existing listing."""
    from sqlalchemy import text as sa_text

    db.execute(
        sa_text("""
            UPDATE commercial_unit
            SET last_seen_at = NOW()
            WHERE aqar_id = :aqar_id
        """),
        {"aqar_id": aqar_id},
    )


_CITY_LEVEL_FALLBACK_THRESHOLD = 50


def main():
    parser = argparse.ArgumentParser(description="Crawl Aqar.fm Riyadh commercial-for-rent listings")
    parser.add_argument("--area", choices=AREAS, help="Limit to a single area")
    parser.add_argument("--neighborhood", help="Limit to a single neighborhood slug (e.g. al-olaya)")
    parser.add_argument("--max-pages", type=int, default=500,
                        help="Max pages per neighborhood (default: 500)")
    parser.add_argument("--list-only", action="store_true", help="Only list neighborhoods, don't crawl listings")
    parser.add_argument("--no-detail", action="store_true", help="Skip fetching individual listing detail pages")
    parser.add_argument("--skip-geocode", action="store_true", help="Skip geocoding neighborhoods")
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing to DB")
    parser.add_argument(
        "--listing-type",
        choices=["all", "store", "showroom", "warehouse", "building"],
        default="all",
        help="Which listing types to crawl (default: all)",
    )
    parser.add_argument(
        "--resume", action="store_true", default=True,
        help="Skip listings already in DB with last_seen_at < 7 days (default: True)",
    )
    parser.add_argument(
        "--no-resume", dest="resume", action="store_false",
        help="Force re-scrape all listings even if already in DB",
    )
    args = parser.parse_args()

    api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "")
    do_geocode = not args.skip_geocode and bool(api_key)
    if not args.skip_geocode and not api_key:
        print("WARN: GOOGLE_MAPS_API_KEY not set, skipping geocoding")

    db = None
    need_db = do_geocode or not args.dry_run or args.resume
    if need_db:
        try:
            db = _get_db_session()
        except Exception as e:
            print(f"WARN: could not connect to DB: {e}")
            if not args.dry_run:
                print("ERROR: DB required for persistence. Use --dry-run to skip DB writes.")
                return

    areas = [args.area] if args.area else AREAS

    if args.listing_type == "all":
        type_keys = list(LISTING_TYPES.keys())
    else:
        type_keys = [args.listing_type]

    try:
        for type_key in type_keys:
            listing_type = LISTING_TYPES[type_key]
            print(f"\n{'=' * 60}")
            print(f"  Listing type: {listing_type}")
            print(f"{'=' * 60}")

            stats = {"scraped": 0, "skipped_existing": 0, "failed": 0,
                     "insert": 0, "update": 0, "total": 0}
            type_total_from_areas = 0
            seen_aqar_ids: set[str] = set()  # For dedup across area + city-level

            for i, area in enumerate(areas):
                if i > 0:
                    time.sleep(2)
                print(f"\n=== {area} ({listing_type}) ===")
                try:
                    neighborhoods = fetch_area(area, listing_type=listing_type)
                except requests.RequestException as e:
                    logger.warning("Area URL %s/%s returned error: %s, skipping area", listing_type, area, e)
                    continue

                if args.neighborhood:
                    neighborhoods = [n for n in neighborhoods if n["slug"] == args.neighborhood]
                    if not neighborhoods:
                        print(f"  Neighborhood '{args.neighborhood}' not found in {area}")
                        continue

                for row in neighborhoods:
                    print(f"  {row['neighborhood']:30s}  {row['count']:>5d}  {row['url']}")

                if args.list_only:
                    continue

                # Geocode each neighborhood (once per neighborhood, not per listing)
                geo_cache: dict[str, dict | None] = {}
                if do_geocode:
                    _purge_null_geocode_cache(db)
                    for nbr in neighborhoods:
                        name = nbr["neighborhood"]
                        if name not in geo_cache:
                            geo_cache[name] = geocode_neighborhood(name, api_key, db)

                for j, nbr in enumerate(neighborhoods):
                    if j > 0 or i > 0:
                        time.sleep(2)
                    print(f"\n  --- Crawling: {nbr['neighborhood']} ---")
                    listings = fetch_neighborhood_listings(nbr["url"], nbr["neighborhood"], max_pages=args.max_pages)
                    print(f"  Found {len(listings)} listings")

                    geo = geo_cache.get(nbr["neighborhood"])

                    for k, listing in enumerate(listings):
                        aqar_id = listing["aqar_id"]
                        seen_aqar_ids.add(aqar_id)

                        # Resumability: skip already-persisted listings
                        if args.resume and db and _listing_already_exists(aqar_id, db):
                            _touch_last_seen(aqar_id, db)
                            stats["skipped_existing"] += 1
                            _log_progress(stats, type_key, j, args.max_pages)
                            continue

                        if not args.no_detail:
                            time.sleep(random.uniform(2, 3))
                            print(f"    [{k + 1}/{len(listings)}] fetching detail: {listing['listing_url']}")
                            fetch_listing_detail(listing)
                        # Attach geocode to each listing
                        if geo:
                            listing["lat"] = geo["lat"]
                            listing["lon"] = geo["lon"]
                        # Tag listing type from crawl config
                        listing["listing_type"] = type_key
                        # Classify restaurant suitability
                        classify_restaurant_suitability(listing)
                        # Compute price_per_sqm
                        listing["price_per_sqm"] = _compute_price_per_sqm(listing)

                        stats["total"] += 1
                        stats["scraped"] += 1

                        if args.dry_run:
                            print(f"    [DRY-RUN] {listing}")
                        else:
                            action = upsert_listing(db, listing)
                            stats[action] += 1
                            print(f"    [{action}] {aqar_id}  score={listing.get('restaurant_score')}"
                                  f"  suitable={listing.get('restaurant_suitable')}"
                                  f"  price_per_sqm={listing.get('price_per_sqm')}")

                        _log_progress(stats, type_key, j, args.max_pages)

                    type_total_from_areas += len(listings)

                    # Commit after each neighborhood batch
                    if db and not args.dry_run:
                        db.commit()

            # ── City-level fallback ──────────────────────────────────────
            # If area-level pages yielded few listings, try the city-level
            # URL as a catch-all sweep (deduplicating by aqar_id).
            if not args.list_only and type_total_from_areas < _CITY_LEVEL_FALLBACK_THRESHOLD:
                city_url = _BASE_TMPL.format(listing_type=listing_type)
                logger.info(
                    "City-level fallback for %s: area pages yielded only %d listings, trying %s",
                    listing_type, type_total_from_areas, city_url,
                )
                try:
                    city_listings = fetch_neighborhood_listings(city_url, "Riyadh", max_pages=args.max_pages)
                except Exception as e:
                    logger.warning(
                        "City-level fallback failed for %s (%s): %s — skipping",
                        listing_type, city_url, e,
                    )
                    city_listings = []
                logger.info("City-level fallback found %d listings for %s", len(city_listings), listing_type)

                for k, listing in enumerate(city_listings):
                    aqar_id = listing["aqar_id"]
                    if aqar_id in seen_aqar_ids:
                        continue  # Already scraped from area pages
                    seen_aqar_ids.add(aqar_id)

                    if args.resume and db and _listing_already_exists(aqar_id, db):
                        _touch_last_seen(aqar_id, db)
                        stats["skipped_existing"] += 1
                        _log_progress(stats, type_key, 0, args.max_pages)
                        continue

                    if not args.no_detail:
                        time.sleep(random.uniform(2, 3))
                        fetch_listing_detail(listing)

                    listing["listing_type"] = type_key
                    classify_restaurant_suitability(listing)
                    listing["price_per_sqm"] = _compute_price_per_sqm(listing)
                    stats["total"] += 1
                    stats["scraped"] += 1

                    if args.dry_run:
                        print(f"    [DRY-RUN city] {listing}")
                    else:
                        action = upsert_listing(db, listing)
                        stats[action] += 1

                    _log_progress(stats, type_key, 0, args.max_pages)

                if db and not args.dry_run:
                    db.commit()

            logger.info(
                "Type %s done: scraped=%d, skipped_existing=%d, insert=%d, update=%d",
                type_key, stats["scraped"], stats["skipped_existing"],
                stats["insert"], stats["update"],
            )

        # Mark stale listings after full scrape
        if db and not args.dry_run:
            stale_count = mark_stale_listings(db, type_keys=type_keys)
            db.commit()
            if stale_count:
                print(f"\nMarked {stale_count} listings as stale (not seen in {_STALE_DAYS}+ days)")

        print(f"\n=== Done ===")
    finally:
        if db is not None:
            db.close()


def _log_progress(stats: dict, listing_type: str, current_page: int, max_pages: int) -> None:
    """Log progress every 100 listings processed."""
    processed = stats["scraped"] + stats["skipped_existing"]
    if processed > 0 and processed % 100 == 0:
        logger.info(
            "Progress [%s]: scraped=%d, skipped_existing=%d, failed=%d, page=%d/%d",
            listing_type,
            stats["scraped"],
            stats["skipped_existing"],
            stats["failed"],
            current_page,
            max_pages,
        )


if __name__ == "__main__":
    main()
