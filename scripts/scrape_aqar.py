#!/usr/bin/env python3
"""Aqar.fm crawler: fetch Riyadh store-for-rent listings by area/neighborhood."""

import argparse
import json
import os
import random
import re
import time
from decimal import Decimal

import requests
from bs4 import BeautifulSoup

AREAS = [
    "north-of-riyadh",
    "south-of-riyadh",
    "east-of-riyadh",
    "west-of-riyadh",
    "center-of-riyadh",
]

BASE = "https://sa.aqar.fm/en/store-for-rent/riyadh"

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


def fetch_area(area: str) -> list[dict]:
    url = f"{BASE}/{area}"
    resp = _get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    results = []
    for link in soup.select("a[href]"):
        href = link.get("href", "")
        if "/en/store-for-rent/riyadh/" not in href or href.endswith(f"/{area}"):
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

_CARD_HREF_RE = re.compile(r"store-for-rent-(\d+)")
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

    # Full description: look for description container
    desc = ""
    for sel in ("div[class*='desc']", "div[class*='content']", "div[class*='body']", "p[class*='desc']"):
        el = soup.select_one(sel)
        if el:
            desc = el.get_text(" ", strip=True)
            if len(desc) > 50:
                break
    if not desc:
        # Fallback: longest <p> on the page
        longest = ""
        for p in soup.find_all("p"):
            t = p.get_text(" ", strip=True)
            if len(t) > len(longest):
                longest = t
        desc = longest

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
            params={"address": query, "key": api_key},
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

_SUITABILITY_THRESHOLD = 40


def classify_restaurant_suitability(listing: dict) -> dict:
    """Score a listing for restaurant suitability based on keywords and physical traits.

    Mutates listing in-place, adding restaurant_score, restaurant_suitable, and
    restaurant_signals fields. Returns the listing.
    """
    score = 0
    signals: list[str] = []

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
    listing["restaurant_suitable"] = score >= _SUITABILITY_THRESHOLD
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
                "lat = :lat, lon = :lon, "
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
                "contact_phone, lat, lon, "
                "restaurant_score, restaurant_suitable, restaurant_signals, "
                "status, first_seen_at, last_seen_at) "
                "VALUES (:aqar_id, :title, :description, :neighborhood, :listing_url, :image_url, "
                ":price_sar_annual, :price_per_sqm, :area_sqm, :street_width_m, "
                ":num_floors, :has_mezzanine, :has_drive_thru, :facade_direction, "
                ":contact_phone, :lat, :lon, "
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
        "lat": Decimal(str(listing["lat"])) if listing.get("lat") else None,
        "lon": Decimal(str(listing["lon"])) if listing.get("lon") else None,
        "restaurant_score": listing.get("restaurant_score"),
        "restaurant_suitable": listing.get("restaurant_suitable"),
        "restaurant_signals": json.dumps(listing.get("restaurant_signals", [])),
    }


def mark_stale_listings(db) -> int:
    """Mark listings not seen in 28+ days as stale. Returns count of rows updated."""
    from sqlalchemy import text as sa_text

    result = db.execute(
        sa_text(
            "UPDATE commercial_unit SET status = 'stale' "
            "WHERE status = 'active' "
            "AND last_seen_at < now() - make_interval(days => :days)"
        ),
        {"days": _STALE_DAYS},
    )
    return result.rowcount


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Crawl Aqar.fm Riyadh store-for-rent listings")
    parser.add_argument("--area", choices=AREAS, help="Limit to a single area")
    parser.add_argument("--neighborhood", help="Limit to a single neighborhood slug (e.g. al-olaya)")
    parser.add_argument("--max-pages", type=int, default=10, help="Max pages per neighborhood (default: 10)")
    parser.add_argument("--list-only", action="store_true", help="Only list neighborhoods, don't crawl listings")
    parser.add_argument("--no-detail", action="store_true", help="Skip fetching individual listing detail pages")
    parser.add_argument("--skip-geocode", action="store_true", help="Skip geocoding neighborhoods")
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing to DB")
    args = parser.parse_args()

    api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "")
    do_geocode = not args.skip_geocode and bool(api_key)
    if not args.skip_geocode and not api_key:
        print("WARN: GOOGLE_MAPS_API_KEY not set, skipping geocoding")

    db = None
    need_db = do_geocode or not args.dry_run
    if need_db:
        try:
            db = _get_db_session()
        except Exception as e:
            print(f"WARN: could not connect to DB: {e}")
            if not args.dry_run:
                print("ERROR: DB required for persistence. Use --dry-run to skip DB writes.")
                return

    areas = [args.area] if args.area else AREAS
    stats = {"insert": 0, "update": 0, "total": 0}

    try:
        for i, area in enumerate(areas):
            if i > 0:
                time.sleep(2)
            print(f"\n=== {area} ===")
            neighborhoods = fetch_area(area)

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
                    if not args.no_detail:
                        time.sleep(random.uniform(2, 3))
                        print(f"    [{k + 1}/{len(listings)}] fetching detail: {listing['listing_url']}")
                        fetch_listing_detail(listing)
                    # Attach geocode to each listing
                    if geo:
                        listing["lat"] = geo["lat"]
                        listing["lon"] = geo["lon"]
                    # Classify restaurant suitability
                    classify_restaurant_suitability(listing)
                    # Compute price_per_sqm
                    listing["price_per_sqm"] = _compute_price_per_sqm(listing)

                    stats["total"] += 1

                    if args.dry_run:
                        print(f"    [DRY-RUN] {listing}")
                    else:
                        action = upsert_listing(db, listing)
                        stats[action] += 1
                        print(f"    [{action}] {listing['aqar_id']}  score={listing.get('restaurant_score')}"
                              f"  suitable={listing.get('restaurant_suitable')}"
                              f"  price_per_sqm={listing.get('price_per_sqm')}")

                # Commit after each neighborhood batch
                if db and not args.dry_run:
                    db.commit()

        # Mark stale listings after full scrape
        if db and not args.dry_run:
            stale_count = mark_stale_listings(db)
            db.commit()
            if stale_count:
                print(f"\nMarked {stale_count} listings as stale (not seen in {_STALE_DAYS}+ days)")

        print(f"\n=== Summary: {stats['total']} processed, "
              f"{stats['insert']} inserted, {stats['update']} updated ===")
    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    main()
