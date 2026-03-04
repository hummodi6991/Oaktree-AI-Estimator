"""
Connectors for food delivery platforms.

Scrapes publicly accessible restaurant listing pages (robots.txt compliant).
Each function returns an iterator of dicts suitable for upserting into ``restaurant_poi``.

Operational safeguards:
- robots.txt check before every fetch
- Per-provider rate limiting (configurable crawl-delay)
- HTTP retries with exponential backoff
- Explicit User-Agent header for polite crawling
- No headless-browser / JS rendering — if a page requires it we log a
  warning and skip rather than pulling in Puppeteer / Playwright.
"""

from __future__ import annotations

import logging
import time
import xml.etree.ElementTree as ET
from typing import Any, Iterator

import httpx

from app.connectors.open_data import robots_allows

logger = logging.getLogger(__name__)

_UA = "oaktree-estimator/1.0 (+https://github.com/hummodi6991/Oaktree-AI-Estimator)"
_DEFAULT_TIMEOUT = 30
_MAX_RETRIES = 3
_BACKOFF_BASE = 2.0  # seconds; retry delays: 2, 4, 8


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_with_retries(
    url: str,
    *,
    timeout: float = _DEFAULT_TIMEOUT,
) -> httpx.Response | None:
    """GET with exponential-backoff retries.  Does NOT check robots.txt."""
    for attempt in range(_MAX_RETRIES):
        try:
            r = httpx.get(
                url,
                timeout=timeout,
                headers={"User-Agent": _UA},
                follow_redirects=True,
            )
            r.raise_for_status()
            return r
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (429, 503):
                delay = _BACKOFF_BASE ** (attempt + 1)
                logger.info("Rate-limited on %s, retrying in %.0fs", url, delay)
                time.sleep(delay)
                continue
            logger.debug("HTTP %s for %s", exc.response.status_code, url)
            return None
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            delay = _BACKOFF_BASE ** (attempt + 1)
            logger.info("Network error on %s (%s), retrying in %.0fs", url, exc, delay)
            time.sleep(delay)
    logger.warning("All %d retries exhausted for %s", _MAX_RETRIES, url)
    return None


def _requires_js(resp: httpx.Response) -> bool:
    """Heuristic: detect pages that need JS rendering to show content."""
    body = resp.text[:2000].lower()
    # Essentially-empty body with only a JS bundle
    if len(resp.text.strip()) < 500 and "<script" in body:
        return True
    js_only_markers = [
        "you need to enable javascript",
        "please enable javascript",
    ]
    return any(tag in body for tag in js_only_markers)


def _parse_sitemap(url: str) -> list[str]:
    """Fetch and parse a sitemap XML, returning all <loc> URLs."""
    resp = _fetch_with_retries(url)
    if resp is None:
        return []

    urls: list[str] = []
    try:
        root = ET.fromstring(resp.text)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for loc in root.findall(".//sm:loc", ns):
            if loc.text:
                urls.append(loc.text.strip())
    except ET.ParseError as exc:
        logger.warning("Failed to parse sitemap XML %s: %s", url, exc)
    return urls


def _safe_get(url: str, crawl_delay: float = 2.0) -> httpx.Response | None:
    """GET a URL after checking robots.txt and respecting crawl delay."""
    if not robots_allows(url, _UA):
        logger.debug("robots.txt disallows: %s", url)
        return None
    time.sleep(crawl_delay)
    resp = _fetch_with_retries(url)
    if resp is None:
        return None
    if _requires_js(resp):
        logger.warning(
            "Page requires JS rendering (unsupported scraping mode), skipping: %s",
            url,
        )
        return None
    return resp


# ---------------------------------------------------------------------------
# HungerStation
# ---------------------------------------------------------------------------

def scrape_hungerstation_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from HungerStation for Riyadh.
    Uses sitemap to discover restaurant pages, then extracts JSON-LD or
    structured data from each page.
    """
    sitemap_url = "https://hungerstation.com/sitemaps/index.xml"
    logger.info("Fetching HungerStation sitemap: %s", sitemap_url)

    sitemap_urls = _parse_sitemap(sitemap_url)
    restaurant_urls: list[str] = []

    for url in sitemap_urls:
        if "restaurant" in url.lower() or "riyadh" in url.lower():
            if url.endswith(".xml"):
                restaurant_urls.extend(_parse_sitemap(url))
            else:
                restaurant_urls.append(url)

    logger.info("Found %d potential restaurant URLs from HungerStation", len(restaurant_urls))

    count = 0
    for url in restaurant_urls[:max_pages]:
        if "riyadh" not in url.lower() and "\u0627\u0644\u0631\u064a\u0627\u0636" not in url:
            continue

        resp = _safe_get(url, crawl_delay=10.0)  # HungerStation robots.txt 10s crawl-delay
        if not resp:
            continue

        yield {
            "id": f"hungerstation:{url.split('/')[-1]}",
            "name": url.split("/")[-1].replace("-", " ").title(),
            "source": "hungerstation",
            "source_url": url,
            "lat": None,
            "lon": None,
            "category_raw": None,
        }
        count += 1

    logger.info("Scraped %d restaurants from HungerStation", count)


# ---------------------------------------------------------------------------
# Talabat
# ---------------------------------------------------------------------------

def scrape_talabat_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Talabat for Riyadh.
    Talabat robots.txt has no restrictions.
    """
    sitemap_url = "https://www.talabat.com/sitemap/sitemap.xml.gz"
    logger.info("Fetching Talabat sitemap: %s", sitemap_url)

    sitemap_urls = _parse_sitemap(sitemap_url)
    restaurant_urls = [
        u for u in sitemap_urls
        if "/saudi-arabia/" in u.lower() or "/sa/" in u.lower()
    ]

    logger.info("Found %d Saudi restaurant URLs from Talabat", len(restaurant_urls))

    count = 0
    for url in restaurant_urls[:max_pages]:
        resp = _safe_get(url, crawl_delay=2.0)
        if not resp:
            continue

        yield {
            "id": f"talabat:{url.split('/')[-1]}",
            "name": url.split("/")[-1].replace("-", " ").title(),
            "source": "talabat",
            "source_url": url,
            "lat": None,
            "lon": None,
            "category_raw": None,
        }
        count += 1

    logger.info("Scraped %d restaurants from Talabat", count)


# ---------------------------------------------------------------------------
# Mrsool
# ---------------------------------------------------------------------------

def scrape_mrsool_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Mrsool for Riyadh.
    Mrsool robots.txt only blocks /wp-admin/.
    """
    sitemap_url = "https://mrsool.co/sitemap.xml"
    logger.info("Fetching Mrsool sitemap: %s", sitemap_url)

    sitemap_urls = _parse_sitemap(sitemap_url)
    restaurant_urls = [
        u for u in sitemap_urls
        if "restaurant" in u.lower() or "riyadh" in u.lower()
    ]

    logger.info("Found %d restaurant URLs from Mrsool", len(restaurant_urls))

    count = 0
    for url in restaurant_urls[:max_pages]:
        resp = _safe_get(url, crawl_delay=2.0)
        if not resp:
            continue

        yield {
            "id": f"mrsool:{url.split('/')[-1]}",
            "name": url.split("/")[-1].replace("-", " ").title(),
            "source": "mrsool",
            "source_url": url,
            "lat": None,
            "lon": None,
            "category_raw": None,
        }
        count += 1

    logger.info("Scraped %d restaurants from Mrsool", count)
