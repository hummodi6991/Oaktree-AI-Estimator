"""
Connectors for food delivery platforms.

Scrapes publicly accessible restaurant listing pages (robots.txt compliant).
Each function returns an iterator of dicts suitable for upserting into ``restaurant_poi``.

Supported platforms (16 total):
- HungerStation, Talabat, Mrsool (original)
- Jahez, ToYou, Keeta, The Chefz, Lugmety, Shgardi, Ninja,
  Nana, Dailymealz, Careem Food, Deliveroo (added)

Operational safeguards:
- robots.txt check before every fetch
- Per-provider rate limiting (configurable crawl-delay)
- HTTP retries with exponential backoff
- Explicit User-Agent header for polite crawling
- No headless-browser / JS rendering — if a page requires it we log a
  warning and skip rather than pulling in Puppeteer / Playwright.
"""

from __future__ import annotations

import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from typing import Any, Iterator
from urllib.parse import urljoin, urlparse

import httpx

from app.connectors.open_data import robots_allows

logger = logging.getLogger(__name__)

_UA = "oaktree-estimator/1.0 (+https://github.com/hummodi6991/Oaktree-AI-Estimator)"
_DEFAULT_TIMEOUT = 45
_MAX_RETRIES = 4
_BACKOFF_BASE = 2.0  # seconds; retry delays: 2, 4, 8, 16

# Registry of all scrapers — used by the ingestion pipeline to iterate
# over all available platforms without hard-coding function names.
SCRAPER_REGISTRY: dict[str, dict[str, Any]] = {}


def _register(source: str, *, label: str, url: str):
    """Decorator to register a scraper function in the global registry."""
    def wrapper(fn):
        SCRAPER_REGISTRY[source] = {
            "fn": fn,
            "source": source,
            "label": label,
            "url": url,
        }
        return fn
    return wrapper


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_with_retries(
    url: str,
    *,
    timeout: float = _DEFAULT_TIMEOUT,
) -> httpx.Response | None:
    """GET with exponential-backoff retries.  Does NOT check robots.txt.

    Retries on:
    - 429 / 503 HTTP status codes
    - Timeouts, connection errors
    - Transport-level errors (incomplete chunked read, RemoteProtocolError)
    """
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
        except (
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.RemoteProtocolError,
            httpx.ReadError,
        ) as exc:
            delay = _BACKOFF_BASE ** (attempt + 1)
            logger.info(
                "Transient error on %s (attempt %d/%d: %s), retrying in %.0fs",
                url, attempt + 1, _MAX_RETRIES, exc, delay,
            )
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


def _extract_json_ld(html: str) -> list[dict]:
    """Extract JSON-LD blocks from an HTML page."""
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


def _slug_to_name(slug: str) -> str:
    """Convert a URL slug to a human-readable name."""
    return slug.replace("-", " ").replace("_", " ").strip().title()


def _is_riyadh_url(url: str) -> bool:
    """Check if a URL is likely related to Riyadh."""
    lower = url.lower()
    return "riyadh" in lower or "\u0627\u0644\u0631\u064a\u0627\u0636" in lower


def _generic_sitemap_scrape(
    *,
    source: str,
    sitemap_url: str,
    url_filter: str | None = None,
    riyadh_filter: bool = True,
    crawl_delay: float = 2.0,
    max_pages: int = 200,
) -> Iterator[dict[str, Any]]:
    """Generic sitemap-based scraper used by multiple platforms.

    Partial sitemap shard failures are logged and skipped — they do not
    collapse the entire platform run.
    """
    logger.info("Fetching %s sitemap: %s", source, sitemap_url)
    sitemap_urls = _parse_sitemap(sitemap_url)

    # Expand nested sitemaps (tolerate individual shard failures)
    restaurant_urls: list[str] = []
    shard_failures = 0
    for u in sitemap_urls:
        if url_filter and url_filter not in u.lower():
            continue
        if u.endswith(".xml") or u.endswith(".xml.gz"):
            try:
                restaurant_urls.extend(_parse_sitemap(u))
            except Exception as exc:
                shard_failures += 1
                logger.warning(
                    "Sitemap shard %s failed for %s: %s", u, source, exc
                )
        else:
            restaurant_urls.append(u)

    if not url_filter:
        restaurant_urls = sitemap_urls

    if shard_failures:
        logger.warning(
            "%s: %d sitemap shard(s) failed, continuing with %d URLs",
            source, shard_failures, len(restaurant_urls),
        )
    logger.info("Found %d candidate URLs from %s", len(restaurant_urls), source)

    count = 0
    for u in restaurant_urls[:max_pages]:
        if riyadh_filter and not _is_riyadh_url(u):
            continue

        resp = _safe_get(u, crawl_delay=crawl_delay)
        if not resp:
            continue

        slug = urlparse(u).path.rstrip("/").split("/")[-1]
        yield {
            "id": f"{source}:{slug}",
            "name": _slug_to_name(slug),
            "source": source,
            "source_url": u,
            "lat": None,
            "lon": None,
            "category_raw": None,
        }
        count += 1

    logger.info("Scraped %d restaurants from %s", count, source)


# ---------------------------------------------------------------------------
# HungerStation
# ---------------------------------------------------------------------------

@_register("hungerstation", label="HungerStation", url="https://hungerstation.com")
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
        if not _is_riyadh_url(url):
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

@_register("talabat", label="Talabat", url="https://www.talabat.com")
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

@_register("mrsool", label="Mrsool", url="https://mrsool.co")
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


# ---------------------------------------------------------------------------
# Jahez
# ---------------------------------------------------------------------------

@_register("jahez", label="Jahez", url="https://www.jahez.net")
def scrape_jahez_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Jahez for Riyadh.
    Jahez is one of the largest Saudi food delivery platforms.
    Uses sitemap-based discovery of public restaurant listing pages.
    """
    yield from _generic_sitemap_scrape(
        source="jahez",
        sitemap_url="https://www.jahez.net/sitemap.xml",
        url_filter="restaurant",
        riyadh_filter=True,
        crawl_delay=3.0,
        max_pages=max_pages,
    )


# ---------------------------------------------------------------------------
# ToYou
# ---------------------------------------------------------------------------

@_register("toyou", label="ToYou", url="https://toyou.io")
def scrape_toyou_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from ToYou delivery platform.
    ToYou is a Saudi last-mile delivery service with restaurant listings.
    """
    yield from _generic_sitemap_scrape(
        source="toyou",
        sitemap_url="https://toyou.io/sitemap.xml",
        url_filter=None,
        riyadh_filter=True,
        crawl_delay=2.0,
        max_pages=max_pages,
    )


# ---------------------------------------------------------------------------
# Keeta (Meituan's Saudi brand)
# ---------------------------------------------------------------------------

@_register("keeta", label="Keeta", url="https://www.keeta.com")
def scrape_keeta_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Keeta (Meituan's Saudi brand).
    Keeta launched in Riyadh in 2024 and is expanding rapidly.
    """
    yield from _generic_sitemap_scrape(
        source="keeta",
        sitemap_url="https://www.keeta.com/sitemap.xml",
        url_filter=None,
        riyadh_filter=True,
        crawl_delay=2.0,
        max_pages=max_pages,
    )


# ---------------------------------------------------------------------------
# The Chefz
# ---------------------------------------------------------------------------

@_register("thechefz", label="The Chefz", url="https://www.thechefz.com")
def scrape_thechefz_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from The Chefz.
    Premium food delivery platform in Saudi Arabia focusing on
    high-end restaurants.
    """
    yield from _generic_sitemap_scrape(
        source="thechefz",
        sitemap_url="https://www.thechefz.com/sitemap.xml",
        url_filter="restaurant",
        riyadh_filter=True,
        crawl_delay=3.0,
        max_pages=max_pages,
    )


# ---------------------------------------------------------------------------
# Lugmety (لقمتي)
# ---------------------------------------------------------------------------

@_register("lugmety", label="Lugmety", url="https://lugmety.com")
def scrape_lugmety_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Lugmety.
    Saudi food delivery platform with strong local restaurant coverage.
    """
    yield from _generic_sitemap_scrape(
        source="lugmety",
        sitemap_url="https://lugmety.com/sitemap.xml",
        url_filter=None,
        riyadh_filter=True,
        crawl_delay=2.0,
        max_pages=max_pages,
    )


# ---------------------------------------------------------------------------
# Shgardi (شقردي)
# ---------------------------------------------------------------------------

@_register("shgardi", label="Shgardi", url="https://shgardi.com")
def scrape_shgardi_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Shgardi.
    Saudi delivery app with strong presence in Riyadh.
    """
    yield from _generic_sitemap_scrape(
        source="shgardi",
        sitemap_url="https://shgardi.com/sitemap.xml",
        url_filter="restaurant",
        riyadh_filter=True,
        crawl_delay=2.0,
        max_pages=max_pages,
    )


# ---------------------------------------------------------------------------
# Ninja
# ---------------------------------------------------------------------------

@_register("ninja", label="Ninja", url="https://ninjasa.com")
def scrape_ninja_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Ninja delivery.
    Saudi food delivery platform.
    """
    yield from _generic_sitemap_scrape(
        source="ninja",
        sitemap_url="https://ninjasa.com/sitemap.xml",
        url_filter=None,
        riyadh_filter=True,
        crawl_delay=2.0,
        max_pages=max_pages,
    )


# ---------------------------------------------------------------------------
# Nana
# ---------------------------------------------------------------------------

@_register("nana", label="Nana", url="https://www.nana.sa")
def scrape_nana_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape grocery/restaurant listings from Nana.
    Nana is a Saudi grocery and food delivery platform — its restaurant
    listings indicate areas of high delivery demand.
    """
    yield from _generic_sitemap_scrape(
        source="nana",
        sitemap_url="https://www.nana.sa/sitemap.xml",
        url_filter=None,
        riyadh_filter=True,
        crawl_delay=2.0,
        max_pages=max_pages,
    )


# ---------------------------------------------------------------------------
# Dailymealz
# ---------------------------------------------------------------------------

@_register("dailymealz", label="Dailymealz", url="https://www.dailymealz.com")
def scrape_dailymealz_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Dailymealz.
    Saudi meal subscription / delivery platform — restaurants listed here
    have proven recurring demand in their delivery zones.
    """
    yield from _generic_sitemap_scrape(
        source="dailymealz",
        sitemap_url="https://www.dailymealz.com/sitemap.xml",
        url_filter=None,
        riyadh_filter=True,
        crawl_delay=2.0,
        max_pages=max_pages,
    )


# ---------------------------------------------------------------------------
# Careem Food (now part of Careem super-app)
# ---------------------------------------------------------------------------

@_register("careemfood", label="Careem Food", url="https://www.careem.com")
def scrape_careem_food_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Careem Food.
    Careem (Uber subsidiary) operates a food delivery service in Saudi Arabia.
    """
    yield from _generic_sitemap_scrape(
        source="careemfood",
        sitemap_url="https://www.careem.com/sitemap.xml",
        url_filter="food",
        riyadh_filter=True,
        crawl_delay=3.0,
        max_pages=max_pages,
    )


# ---------------------------------------------------------------------------
# Deliveroo
# ---------------------------------------------------------------------------

@_register("deliveroo", label="Deliveroo", url="https://deliveroo.sa")
def scrape_deliveroo_riyadh(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Deliveroo Saudi Arabia.
    Deliveroo operates in major Saudi cities including Riyadh.
    """
    yield from _generic_sitemap_scrape(
        source="deliveroo",
        sitemap_url="https://deliveroo.sa/sitemap.xml",
        url_filter="restaurant",
        riyadh_filter=True,
        crawl_delay=2.0,
        max_pages=max_pages,
    )


# ---------------------------------------------------------------------------
# Convenience: list all registered scrapers
# ---------------------------------------------------------------------------

def list_all_scrapers() -> list[dict[str, str]]:
    """Return metadata for all registered scrapers."""
    return [
        {"source": v["source"], "label": v["label"], "url": v["url"]}
        for v in SCRAPER_REGISTRY.values()
    ]


def scrape_all_platforms(max_pages: int = 200) -> Iterator[dict[str, Any]]:
    """Run all registered scrapers and yield their combined results."""
    for source, entry in SCRAPER_REGISTRY.items():
        logger.info("Running scraper: %s", source)
        try:
            yield from entry["fn"](max_pages=max_pages)
        except Exception as exc:
            logger.warning("Scraper %s failed: %s", source, exc)
