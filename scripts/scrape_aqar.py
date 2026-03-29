#!/usr/bin/env python3
"""Aqar.fm crawler: fetch Riyadh store-for-rent listings by area/neighborhood."""

import argparse
import json
import random
import re
import time

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
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Crawl Aqar.fm Riyadh store-for-rent listings")
    parser.add_argument("--area", choices=AREAS, help="Limit to a single area")
    parser.add_argument("--neighborhood", help="Limit to a single neighborhood slug (e.g. al-olaya)")
    parser.add_argument("--max-pages", type=int, default=10, help="Max pages per neighborhood (default: 10)")
    parser.add_argument("--list-only", action="store_true", help="Only list neighborhoods, don't crawl listings")
    args = parser.parse_args()

    areas = [args.area] if args.area else AREAS

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

        for j, nbr in enumerate(neighborhoods):
            if j > 0 or i > 0:
                time.sleep(2)
            print(f"\n  --- Crawling: {nbr['neighborhood']} ---")
            listings = fetch_neighborhood_listings(nbr["url"], nbr["neighborhood"], max_pages=args.max_pages)
            print(f"  Found {len(listings)} listings")
            for listing in listings:
                print(f"    {listing}")


if __name__ == "__main__":
    main()
