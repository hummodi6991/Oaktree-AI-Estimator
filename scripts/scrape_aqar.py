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


def _extract_next_data(html: str) -> dict | None:
    """Extract __NEXT_DATA__ JSON embedded by Next.js, if present."""
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("script", id="__NEXT_DATA__")
    if tag and tag.string:
        try:
            return json.loads(tag.string)
        except json.JSONDecodeError:
            pass
    return None


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
        text = link.get_text(strip=True)
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

_ID_RE = re.compile(r"/(\d+)(?:\?|$|/)")


def _parse_listing_from_next_data(item: dict) -> dict | None:
    """Parse a single listing from __NEXT_DATA__ props structure."""
    aqar_id = str(item.get("id", ""))
    if not aqar_id:
        return None
    title = item.get("title", "")
    price = item.get("price")
    area_sqm = None
    street_width = None
    img = None

    # Attributes may live under 'attributes', 'details', or flat keys
    for attr in item.get("attributes", []):
        slug = attr.get("slug", "")
        val = attr.get("value") or attr.get("formatted_value")
        if slug in ("space", "area", "size") and val is not None:
            try:
                area_sqm = float(str(val).replace(",", ""))
            except ValueError:
                pass
        if slug in ("street_width", "street-width") and val is not None:
            try:
                street_width = float(str(val).replace(",", ""))
            except ValueError:
                pass

    # Fallback flat keys
    if area_sqm is None and item.get("area"):
        try:
            area_sqm = float(str(item["area"]).replace(",", ""))
        except (ValueError, TypeError):
            pass

    imgs = item.get("images") or item.get("imgs") or []
    if imgs:
        first = imgs[0]
        img = first if isinstance(first, str) else first.get("url") or first.get("image")

    return {
        "aqar_id": aqar_id,
        "title": title,
        "price_sar_annual": int(price) if price else None,
        "area_sqm": area_sqm,
        "street_width_m": street_width,
        "description": (item.get("description") or item.get("content") or "")[:200],
        "neighborhood": item.get("location", {}).get("name", "") if isinstance(item.get("location"), dict) else "",
        "image_url": img,
        "listing_url": f"https://sa.aqar.fm/en/{aqar_id}",
    }


def _parse_listing_from_card(card, neighborhood: str) -> dict | None:
    """Parse a listing card from raw HTML."""
    link = card.find("a", href=True)
    if not link:
        return None
    href = link["href"]
    m = _ID_RE.search(href)
    aqar_id = m.group(1) if m else href.rstrip("/").rsplit("/", 1)[-1]
    full_url = href if href.startswith("http") else f"https://sa.aqar.fm{href}"

    title = ""
    title_el = card.find(["h2", "h3"]) or link
    if title_el:
        title = title_el.get_text(strip=True)

    price = None
    price_el = card.find(string=re.compile(r"[\d,]+\s*(?:SAR|ريال|SR)")) or card.find(
        string=re.compile(r"(?:SAR|ريال|SR)\s*[\d,]+")
    )
    if price_el:
        digits = re.sub(r"[^\d]", "", price_el.strip())
        if digits:
            price = int(digits)

    area_sqm = None
    area_el = card.find(string=re.compile(r"[\d,.]+\s*(?:sqm|م²|m²|متر)", re.I))
    if area_el:
        am = re.search(r"([\d,.]+)", area_el)
        if am:
            area_sqm = float(am.group(1).replace(",", ""))

    street_width = None
    sw_el = card.find(string=re.compile(r"(?:street|width|شارع|عرض)", re.I))
    if sw_el:
        sm = re.search(r"([\d,.]+)\s*m", sw_el, re.I)
        if sm:
            street_width = float(sm.group(1).replace(",", ""))

    desc = ""
    desc_el = card.find("p") or card.find(class_=re.compile(r"desc|snippet|body", re.I))
    if desc_el:
        desc = desc_el.get_text(strip=True)[:200]

    img = None
    img_el = card.find("img", src=True)
    if img_el:
        img = img_el["src"]
        if img.startswith("//"):
            img = "https:" + img

    return {
        "aqar_id": aqar_id,
        "title": title,
        "price_sar_annual": price,
        "area_sqm": area_sqm,
        "street_width_m": street_width,
        "description": desc,
        "neighborhood": neighborhood,
        "image_url": img,
        "listing_url": full_url,
    }


def _extract_listings_next(data: dict) -> list[dict]:
    """Walk __NEXT_DATA__ to find the listings array."""
    props = data.get("props", {}).get("pageProps", {})
    # Try common key names
    for key in ("listings", "ads", "results", "items", "data"):
        items = props.get(key)
        if isinstance(items, list) and items:
            return items
    # Recurse one level into sub-dicts
    for v in props.values():
        if isinstance(v, dict):
            for key in ("listings", "ads", "results", "items", "data"):
                items = v.get(key)
                if isinstance(items, list) and items:
                    return items
    return []


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

        # Strategy 1: __NEXT_DATA__
        ndata = _extract_next_data(html)
        if ndata:
            for item in _extract_listings_next(ndata):
                parsed = _parse_listing_from_next_data(item)
                if parsed and parsed["aqar_id"] not in seen_ids:
                    if not parsed["neighborhood"]:
                        parsed["neighborhood"] = neighborhood_name
                    seen_ids.add(parsed["aqar_id"])
                    listings_on_page.append(parsed)

        # Strategy 2: HTML card fallback
        if not listings_on_page:
            soup = BeautifulSoup(html, "html.parser")
            cards = soup.select("[class*='listing'], [class*='card'], [class*='adCard'], article")
            for card in cards:
                parsed = _parse_listing_from_card(card, neighborhood_name)
                if parsed and parsed["aqar_id"] not in seen_ids:
                    seen_ids.add(parsed["aqar_id"])
                    listings_on_page.append(parsed)

        if not listings_on_page:
            break

        all_listings.extend(listings_on_page)

        # Find next page
        soup = BeautifulSoup(html, "html.parser") if not ndata else BeautifulSoup(html, "html.parser")
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


def _extract_detail_from_next_data(data: dict) -> dict:
    """Extract detail fields from a detail page's __NEXT_DATA__."""
    props = data.get("props", {}).get("pageProps", {})
    # The listing object may be under various keys
    item = {}
    for key in ("listing", "ad", "post", "data"):
        if isinstance(props.get(key), dict):
            item = props[key]
            break
    if not item:
        item = props

    desc = item.get("description") or item.get("content") or item.get("body") or ""
    title = item.get("title") or ""
    combined = f"{title} {desc}"

    # Phone: may be in a dedicated field or in description
    phone = item.get("phone") or item.get("contact_phone") or item.get("mobile")
    if not phone:
        pm = _PHONE_RE.search(combined)
        phone = pm.group(0).strip() if pm else None

    # Facade: check attributes first, then text
    facade = None
    for attr in item.get("attributes", []):
        slug = attr.get("slug", "")
        if slug in ("facade", "direction", "facing", "facade_direction"):
            facade = str(attr.get("value") or attr.get("formatted_value") or "")
            break
    if not facade:
        fm = _FACADE_RE.search(combined)
        facade = fm.group(1) if fm else None

    # Floors: check attributes then text
    num_floors = None
    for attr in item.get("attributes", []):
        slug = attr.get("slug", "")
        if slug in ("floors", "num_floors", "stories", "number_of_floors"):
            try:
                num_floors = int(attr.get("value") or attr.get("formatted_value") or 0)
            except (ValueError, TypeError):
                pass
            break
    if num_floors is None:
        flm = _search_text_match(_FLOORS_RE, combined) or _search_text_match(_FLOORS_RE2, combined)
        if flm:
            num_floors = int(flm.group(1))

    return {
        "full_description": desc,
        "has_mezzanine": _search_text_bool(_MEZZANINE_RE, combined),
        "has_drive_thru": _search_text_bool(_DRIVE_THRU_RE, combined),
        "facade_direction": facade,
        "contact_phone": phone,
        "num_floors": num_floors,
    }


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
    ndata = _extract_next_data(html)
    if ndata:
        detail = _extract_detail_from_next_data(ndata)
    else:
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
            "VALUES (:q, :lat, :lon, :addr, :raw) "
            "ON CONFLICT (query) DO UPDATE SET lat=:lat, lon=:lon, "
            "formatted_address=:addr, raw=:raw"
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
    args = parser.parse_args()

    api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "")
    do_geocode = not args.skip_geocode and bool(api_key)
    if not args.skip_geocode and not api_key:
        print("WARN: GOOGLE_MAPS_API_KEY not set, skipping geocoding")

    db = None
    if do_geocode:
        try:
            db = _get_db_session()
        except Exception as e:
            print(f"WARN: could not connect to DB for geocode cache: {e}")

    areas = [args.area] if args.area else AREAS

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
                    print(f"    {listing}")
    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    main()
