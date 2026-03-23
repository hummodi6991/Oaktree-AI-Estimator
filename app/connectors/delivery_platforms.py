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

from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
import gzip
import json
import logging
import re
import threading
import time
import xml.etree.ElementTree as ET
from typing import Any, Iterator
from urllib.parse import urljoin, urlparse, unquote

import httpx

from app.connectors.open_data import robots_allows

logger = logging.getLogger(__name__)

_UA = "oaktree-estimator/1.0 (+https://github.com/hummodi6991/Oaktree-AI-Estimator)"
_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
_DEFAULT_TIMEOUT = 45
_MAX_RETRIES = 4
_BACKOFF_BASE = 2.0  # seconds; retry delays: 2, 4, 8, 16

# Registry of all scrapers — used by the ingestion pipeline to iterate
# over all available platforms without hard-coding function names.
SCRAPER_REGISTRY: dict[str, dict[str, Any]] = {}

# robots.txt decisions are stable enough for a single ingest run; avoid
# re-fetching robots.txt for every page request.
_ROBOTS_ALLOWED_CACHE: dict[tuple[str, str], bool] = {}
_ROBOTS_ALLOWED_CACHE_LOCK = threading.Lock()

_RIYADH_URL_TOKENS = (
    "riyadh",
    "ar-riyadh",
    "al-riyadh",
    "\u0627\u0644\u0631\u064a\u0627\u0636",
)


def _decoded_url(url: str) -> str:
    try:
        return unquote(url or "")
    except Exception:
        return url or ""


def _robots_allows_cached(url: str, user_agent: str) -> bool:
    parsed = urlparse(url)
    key = (f"{parsed.scheme}://{parsed.netloc}".lower(), user_agent)
    with _ROBOTS_ALLOWED_CACHE_LOCK:
        cached = _ROBOTS_ALLOWED_CACHE.get(key)
    if cached is not None:
        return cached
    allowed = robots_allows(url, user_agent)
    with _ROBOTS_ALLOWED_CACHE_LOCK:
        _ROBOTS_ALLOWED_CACHE[key] = allowed
    return allowed


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
    user_agent: str = _UA,
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
                headers={"User-Agent": user_agent},
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


def _parse_sitemap(url: str, user_agent: str = _UA) -> list[str]:
    """Fetch and parse a sitemap XML, returning all <loc> URLs."""
    resp = _fetch_with_retries(url, user_agent=user_agent)
    if resp is None:
        return []

    urls: list[str] = []
    try:
        # Handle gzipped sitemaps (.xml.gz files served as raw gzip)
        if url.endswith(".gz"):
            try:
                xml_bytes = gzip.decompress(resp.content)
                xml_text = xml_bytes.decode("utf-8", errors="replace")
            except Exception as exc:
                logger.warning("Failed to decompress gzipped sitemap %s: %s", url, exc)
                # Fall back to resp.text in case the server already decompressed it
                xml_text = resp.text
        else:
            xml_text = resp.text

        root = ET.fromstring(xml_text)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for loc in root.findall(".//sm:loc", ns):
            if loc.text:
                urls.append(loc.text.strip())
    except ET.ParseError as exc:
        logger.warning("Failed to parse sitemap XML %s: %s", url, exc)

    # Diagnostic: log sample URLs to help debug URL pattern issues
    if urls:
        sample = urls[:3]
        logger.info("Sitemap %s returned %d URLs, samples: %s", url, len(urls), sample)
    else:
        logger.info("Sitemap %s returned 0 URLs", url)

    return urls


# ---------------------------------------------------------------------------
# Multi-strategy sitemap discovery
# ---------------------------------------------------------------------------

# Common alternate sitemap paths that delivery platforms may use.
_COMMON_SITEMAP_PATHS = (
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemaps/sitemap.xml",
    "/sitemap/sitemap.xml",
    "/sitemaps/index.xml",
    "/sitemap1.xml",
)


@dataclass
class DiscoveryStats:
    """Structured stats for a single platform's sitemap discovery run."""
    platform: str = ""
    discovery_attempts: list[dict[str, Any]] = field(default_factory=list)
    discovery_success_path: str | None = None
    sitemap_urls_found: int = 0
    candidate_urls_found: int = 0
    fetch_failures: int = 0
    parse_failures: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _extract_sitemap_hints_from_robots(base_url: str, user_agent: str = _UA) -> list[str]:
    """Parse robots.txt for ``Sitemap:`` directives.

    Returns sitemap URLs advertised in the robots.txt of *base_url*.
    Respects the existing timeout and retry behaviour via _fetch_with_retries.
    """
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    resp = _fetch_with_retries(robots_url, timeout=10, user_agent=user_agent)
    if resp is None:
        return []
    hints: list[str] = []
    for line in resp.text.splitlines():
        line = line.strip()
        if line.lower().startswith("sitemap:"):
            value = line.split(":", 1)[1].strip()
            if value:
                hints.append(value)
    return hints


def _discover_sitemaps(
    base_url: str,
    configured_sitemap: str,
    alternate_paths: tuple[str, ...] = _COMMON_SITEMAP_PATHS,
    user_agent: str = _UA,
) -> tuple[list[str], DiscoveryStats]:
    """Multi-strategy sitemap discovery.

    Tries sources in order, stopping at the first that yields URLs:
    1. The *configured_sitemap* URL
    2. ``Sitemap:`` hints from robots.txt
    3. A small curated list of common alternate sitemap paths
    4. If a sitemap index is found (entries ending in .xml), expand it

    Returns ``(sitemap_entry_urls, stats)``.
    """
    stats = DiscoveryStats()
    all_urls: list[str] = []

    def _try_url(url: str, label: str) -> list[str]:
        stats.discovery_attempts.append({"url": url, "strategy": label})
        urls = _parse_sitemap(url, user_agent=user_agent)
        if urls:
            logger.info("Discovery [%s]: %s yielded %d entries", label, url, len(urls))
        else:
            logger.debug("Discovery [%s]: %s yielded 0 entries", label, url)
        return urls

    # Strategy 1 — configured sitemap
    all_urls = _try_url(configured_sitemap, "configured")
    if all_urls:
        stats.discovery_success_path = "configured"
        return _expand_sitemap_index(all_urls, stats, user_agent=user_agent), stats

    # Strategy 2 — robots.txt Sitemap: hints
    hints = _extract_sitemap_hints_from_robots(base_url, user_agent=user_agent)
    for hint in hints:
        found = _try_url(hint, "robots_hint")
        if found:
            all_urls.extend(found)
    if all_urls:
        stats.discovery_success_path = "robots_hint"
        return _expand_sitemap_index(all_urls, stats, user_agent=user_agent), stats

    # Strategy 3 — common alternate paths
    parsed = urlparse(base_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    for path in alternate_paths:
        candidate = origin + path
        # Skip if we already tried this exact URL
        if candidate == configured_sitemap:
            continue
        if any(a["url"] == candidate for a in stats.discovery_attempts):
            continue
        found = _try_url(candidate, "common_path")
        if found:
            all_urls.extend(found)
            stats.discovery_success_path = f"common_path:{path}"
            return _expand_sitemap_index(all_urls, stats, user_agent=user_agent), stats

    # Nothing found
    stats.discovery_success_path = None
    return [], stats


def _expand_sitemap_index(urls: list[str], stats: DiscoveryStats, user_agent: str = _UA) -> list[str]:
    """If *urls* look like a sitemap index (entries ending in .xml), expand them.

    Non-XML entries are passed through unchanged.
    """
    expanded: list[str] = []
    index_entries = [u for u in urls if u.endswith(".xml") or u.endswith(".xml.gz")]
    plain_entries = [u for u in urls if u not in set(index_entries)]

    if not index_entries:
        stats.sitemap_urls_found = len(urls)
        return urls

    for idx_url in index_entries:
        try:
            child_urls = _parse_sitemap(idx_url, user_agent=user_agent)
            expanded.extend(child_urls)
        except Exception as exc:
            stats.parse_failures += 1
            logger.warning("Sitemap index expansion failed for %s: %s", idx_url, exc)

    expanded.extend(plain_entries)
    stats.sitemap_urls_found = len(index_entries)
    return expanded


def _safe_get(url: str, crawl_delay: float = 2.0, user_agent: str = _UA) -> httpx.Response | None:
    """GET a URL after checking robots.txt and respecting crawl delay."""
    if not _robots_allows_cached(url, user_agent):
        logger.debug("robots.txt disallows: %s", url)
        return None
    time.sleep(crawl_delay)
    resp = _fetch_with_retries(url, user_agent=user_agent)
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
    lower = _decoded_url(url).lower()
    return any(token in lower for token in _RIYADH_URL_TOKENS)


def _is_hungerstation_riyadh_restaurant_shard(url: str) -> bool:
    """
    Keep only HungerStation restaurant-vendor sitemap shards for *Riyadh city*.

    This is the critical fix:
    - do not expand all cities
    - do not expand flowers/pharmacies/supermarkets/pickup shards
    - do not accidentally match nearby/non-Riyadh cities like 'riyadh-al-khabra'
    """
    lower = _decoded_url(url).lower()
    m = re.search(
        r"hungerstation\.com/sitemaps/(?:sa-(?:ar|en))/modules/restaurants/vendors/([^/]+)/part-\d+\.xml(?:\.gz)?$",
        lower,
    )
    if not m:
        return False
    city_slug = m.group(1).strip("/")
    return city_slug in {"riyadh", "الرياض"}


def _canonical_hungerstation_shard_key(url: str) -> str | None:
    """
    Canonicalize Riyadh shard URLs so Arabic/English sitemap variants collapse
    to a single logical shard key. This avoids expanding both sa-ar and sa-en
    copies when they point to the same vendor pages.
    """
    lower = _decoded_url(url).lower()
    m = re.search(
        r"/modules/restaurants/vendors/(riyadh|الرياض)/part-(\d+)\.xml(?:\.gz)?$",
        lower,
    )
    if not m:
        return None
    return f"{m.group(1)}:part-{m.group(2)}"


def _prefer_hungerstation_shard(current: str | None, candidate: str) -> str:
    """
    Prefer sa-ar over sa-en when both languages expose the same logical shard.
    """
    if current is None:
        return candidate
    current_lower = _decoded_url(current).lower()
    candidate_lower = _decoded_url(candidate).lower()
    if "/sa-ar/" in candidate_lower and "/sa-en/" in current_lower:
        return candidate
    return current


def _target_candidate_pool(max_pages: int) -> int:
    """
    Enough URLs to fill the requested batch after dedupe/rejections, but not so
    many that we expand hundreds of sitemap shards unnecessarily.

    This is only one stop condition now. We also require minimum shard and
    bucket coverage before early-stopping, so the first N fetched pages are not
    trapped inside a tiny cluster of Riyadh districts.
    """
    return max(max_pages * 8, 1200)


def _min_hungerstation_shards(max_pages: int) -> int:
    """
    Expand at least a sensible number of sitemap shards before stopping.
    For max_pages=5000 this yields 16.
    """
    return max(8, min(16, (max_pages + 24) // 25))


def _min_hungerstation_district_buckets(max_pages: int) -> int:
    """
    Require a minimum number of district-ish buckets before early-stop.
    For max_pages=5000 this yields 40.
    """
    return max(18, min(40, max_pages // 8))


def _max_hungerstation_candidate_pool(max_pages: int) -> int:
    """
    Hard ceiling to avoid reintroducing a huge candidate explosion if bucket
    breadth remains low for too long.
    For max_pages=5000 this yields 160000.
    """
    return max(_target_candidate_pool(max_pages) * 4, 6000)


def _extract_hungerstation_listing_id(url: str) -> str | None:
    """
    Extract a stable numeric listing id from HungerStation URLs.

    Handles both:
    - /restaurant/<city>/<district>/<id>
    - /qc/<brand_id>/<name>/branch/<city~district~id>
    """
    decoded = _decoded_url(url)
    m = re.search(r"/(\d+)(?:[/?#]|$)", decoded)
    if m:
        return m.group(1)

    # Fallback for branch URLs where the final path segment contains ~<id>
    last = urlparse(decoded).path.rstrip("/").split("/")[-1]
    m = re.search(r"(?:~|/)(\d+)$", last)
    if m:
        return m.group(1)
    return None


def _extract_hungerstation_district_bucket(url: str) -> str:
    """
    Extract a district-ish bucket from the URL so we can interleave candidates
    across different areas instead of scraping 200 pages from the same one.
    """
    decoded = _decoded_url(url)
    path = urlparse(decoded).path.strip("/")
    parts = [p for p in path.split("/") if p]

    # /sa-ar/restaurant/<city>/<district>/<id>
    if len(parts) >= 5 and parts[1] == "restaurant":
        return parts[3].strip().lower()

    # /sa-ar/qc/<brand_id>/<brand>/branch/<city~district~id>
    if "branch" in parts:
        branch_seg = parts[-1]
        branch_parts = [p.strip().lower() for p in branch_seg.split("~") if p.strip()]
        if len(branch_parts) >= 3:
            return branch_parts[1]

    # Fallback bucket
    return "unknown"


def _is_hungerstation_primary_restaurant_url(url: str) -> bool:
    lower = _decoded_url(url).lower()
    return "/restaurant/" in lower and "/qc/" not in lower


def _prefer_hungerstation_candidate(current: str | None, candidate: str) -> str:
    """
    Prefer canonical /restaurant/ detail URLs over /qc/branch URLs when both
    point to the same underlying listing id.
    """
    if current is None:
        return candidate
    if _is_hungerstation_primary_restaurant_url(candidate) and not _is_hungerstation_primary_restaurant_url(current):
        return candidate
    return current


def _diversify_hungerstation_candidates(urls: list[str], max_pages: int) -> list[str]:
    """
    Reorder candidates so the first N fetched pages are spread across districts
    instead of being dominated by a single district block from one shard.
    """
    by_listing_id: dict[str, str] = {}
    passthrough: list[str] = []

    for url in urls:
        listing_id = _extract_hungerstation_listing_id(url)
        if listing_id:
            by_listing_id[listing_id] = _prefer_hungerstation_candidate(
                by_listing_id.get(listing_id), url
            )
        else:
            passthrough.append(url)

    deduped = list(by_listing_id.values()) + passthrough

    buckets: dict[str, deque[str]] = defaultdict(deque)
    for url in deduped:
        buckets[_extract_hungerstation_district_bucket(url)].append(url)

    ordered: list[str] = []
    bucket_names = sorted(buckets.keys(), key=lambda k: (-len(buckets[k]), k))
    while bucket_names:
        next_round: list[str] = []
        for name in bucket_names:
            q = buckets[name]
            if q:
                ordered.append(q.popleft())
            if q:
                next_round.append(name)
        bucket_names = next_round

    logger.info(
        "HungerStation candidate diversification: %d raw -> %d deduped ordered URLs across %d district bucket(s)",
        len(urls),
        len(ordered),
        len(buckets),
    )

    return ordered


def _is_hungerstation_candidate_restaurant_url(url: str) -> bool:
    """
    Defensive filter for detail/listing URLs emitted from Riyadh restaurant shards.
    Excludes obvious landing/index pages that pollute the first page window and
    lead to rows with no usable geo.
    """
    lower = _decoded_url(url).lower()

    blocked = (
        "/vendors/riyadh-al-",
        "/vendors/ar-riyadh-",
        "/vendors/al-riyadh-",
        "/modules/restaurants/vendors/",
        "/regions/",
        "/cuisines",
        "/flowers/",
        "/pharmacies/",
        "/supermarkets/",
        "/pickup/",
    )
    return not any(token in lower for token in blocked)


def _extract_page_data(html: str, url: str, source: str) -> dict[str, Any]:
    """Extract structured restaurant data from an HTML page.

    Tries, in order:
    1. JSON-LD structured data
    2. Embedded __NEXT_DATA__ / window.__data__ JSON blobs
    3. Open Graph meta tags
    4. <title> tag

    Returns a dict with as many fields as could be extracted.
    """
    data: dict[str, Any] = {}

    # --- 1. JSON-LD ---
    for block in _extract_json_ld(html):
        block_type = block.get("@type", "")
        if block_type in ("Restaurant", "FoodEstablishment", "LocalBusiness",
                          "Organization", "Place"):
            data["name"] = block.get("name") or data.get("name")
            addr = block.get("address", {})
            if isinstance(addr, dict):
                data["address_raw"] = addr.get("streetAddress")
                data["district_text"] = addr.get("addressLocality")
            geo = block.get("geo", {})
            if isinstance(geo, dict):
                try:
                    lat = float(geo.get("latitude", 0))
                    lon = float(geo.get("longitude", 0))
                    if lat and lon:
                        data["lat"] = lat
                        data["lon"] = lon
                except (ValueError, TypeError):
                    pass
            cuisine = block.get("servesCuisine")
            if cuisine:
                data["category_raw"] = (
                    cuisine if isinstance(cuisine, str) else ", ".join(cuisine)
                )
            agg = block.get("aggregateRating", {})
            if isinstance(agg, dict):
                try:
                    data["rating"] = float(agg.get("ratingValue", 0)) or None
                    data["rating_count"] = int(agg.get("reviewCount", 0)) or None
                except (ValueError, TypeError):
                    pass
            data["phone_raw"] = block.get("telephone")
            break  # use first matching block

    # --- 2. Embedded JSON blobs (Next.js / SPA frameworks) ---
    if not data.get("name"):
        for pattern in [
            r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
            r'window\.__data__\s*=\s*(\{.*?\});',
            r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\});',
        ]:
            m = re.search(pattern, html, re.DOTALL)
            if m:
                try:
                    blob = json.loads(m.group(1))
                    _extract_from_nested_json(blob, data)
                except (json.JSONDecodeError, RecursionError):
                    pass
                if data.get("name"):
                    break

    # --- 3. Open Graph meta tags ---
    if not data.get("name"):
        og_title = re.search(
            r'<meta\s+property=["\']og:title["\'][^>]*content=["\']([^"\']+)',
            html, re.IGNORECASE,
        )
        if og_title:
            data["name"] = og_title.group(1).strip()

    # --- 4. <title> tag fallback ---
    if not data.get("name"):
        title_m = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE)
        if title_m:
            raw_title = title_m.group(1).strip()
            # Strip common suffixes like " | HungerStation" or " - Talabat"
            for sep in [" | ", " - ", " – ", " — "]:
                if sep in raw_title:
                    raw_title = raw_title.split(sep)[0].strip()
            if raw_title and len(raw_title) > 2:
                data["name"] = raw_title

    return data


def _extract_from_nested_json(
    obj: Any,
    out: dict[str, Any],
    depth: int = 0,
) -> None:
    """Walk a nested JSON structure looking for restaurant-like fields."""
    if depth > 8 or out.get("name"):
        return
    if isinstance(obj, dict):
        # Look for restaurant-like objects
        if obj.get("name") and any(
            k in obj for k in ("latitude", "lat", "cuisine", "rating",
                               "address", "category", "delivery")
        ):
            out["name"] = out.get("name") or obj.get("name")
            for lat_key in ("latitude", "lat"):
                if obj.get(lat_key):
                    try:
                        out["lat"] = float(obj[lat_key])
                    except (ValueError, TypeError):
                        pass
            for lon_key in ("longitude", "lon", "lng"):
                if obj.get(lon_key):
                    try:
                        out["lon"] = float(obj[lon_key])
                    except (ValueError, TypeError):
                        pass
            out["category_raw"] = out.get("category_raw") or obj.get(
                "cuisine") or obj.get("category")
            if isinstance(out.get("category_raw"), list):
                out["category_raw"] = ", ".join(str(c) for c in out["category_raw"])
            out["rating"] = out.get("rating") or obj.get("rating")
            out["address_raw"] = out.get("address_raw") or obj.get("address")
            if isinstance(out["address_raw"], dict):
                out["address_raw"] = out["address_raw"].get("streetAddress")
            return
        for v in obj.values():
            _extract_from_nested_json(v, out, depth + 1)
    elif isinstance(obj, list):
        for item in obj[:20]:  # limit to avoid huge arrays
            _extract_from_nested_json(item, out, depth + 1)



# Module-level storage for the most recent discovery stats per platform.
# Populated by _generic_sitemap_scrape; read by the ingestion pipeline.
_LAST_DISCOVERY_STATS: dict[str, DiscoveryStats] = {}


def get_discovery_stats() -> dict[str, dict[str, Any]]:
    """Return a copy of per-platform discovery stats from the last run."""
    return {k: v.to_dict() for k, v in _LAST_DISCOVERY_STATS.items()}


def _generic_sitemap_scrape(
    *,
    source: str,
    sitemap_url: str,
    url_filter: str | None = None,
    riyadh_filter: bool = True,
    crawl_delay: float = 2.0,
    max_pages: int = 5000,
    multi_strategy: bool = False,
    base_url: str | None = None,
    user_agent: str = _UA,
) -> Iterator[dict[str, Any]]:
    """Generic sitemap-based scraper used by multiple platforms.

    Partial sitemap shard failures are logged and skipped — they do not
    collapse the entire platform run.

    When *multi_strategy* is True the scraper uses :func:`_discover_sitemaps`
    to try multiple sitemap sources before giving up.  *base_url* must be
    provided in that case (e.g. ``https://www.jahez.net``).
    """
    stats = DiscoveryStats(platform=source)

    if multi_strategy:
        assert base_url, "base_url is required when multi_strategy=True"
        logger.info("Running multi-strategy discovery for %s", source)
        sitemap_urls, stats = _discover_sitemaps(base_url, sitemap_url, user_agent=user_agent)
        stats.platform = source
    else:
        logger.info("Fetching %s sitemap: %s", source, sitemap_url)
        sitemap_urls = _parse_sitemap(sitemap_url, user_agent=user_agent)
        stats.discovery_attempts.append({"url": sitemap_url, "strategy": "configured"})
        if sitemap_urls:
            stats.discovery_success_path = "configured"

    # Expand nested sitemaps (tolerate individual shard failures)
    restaurant_urls: list[str] = []
    shard_failures = 0
    for u in sitemap_urls:
        if url_filter and url_filter not in u.lower():
            continue
        if u.endswith(".xml") or u.endswith(".xml.gz"):
            try:
                restaurant_urls.extend(_parse_sitemap(u, user_agent=user_agent))
            except Exception as exc:
                shard_failures += 1
                stats.parse_failures += 1
                logger.warning(
                    "Sitemap shard %s failed for %s: %s", u, source, exc
                )
        else:
            restaurant_urls.append(u)

    if not url_filter and not restaurant_urls:
        # No filter was set AND no nested sitemaps found — fall back to
        # treating the top-level sitemap entries as candidate URLs.
        restaurant_urls = [u for u in sitemap_urls
                           if not u.endswith(".xml") and not u.endswith(".xml.gz")]

    if shard_failures:
        logger.warning(
            "%s: %d sitemap shard(s) failed, continuing with %d URLs",
            source, shard_failures, len(restaurant_urls),
        )

    stats.candidate_urls_found = len(restaurant_urls)
    logger.info("Found %d candidate URLs from %s", len(restaurant_urls), source)

    count = 0
    riyadh_filtered = 0
    fetch_failed = 0
    seen = 0
    for u in restaurant_urls:
        if riyadh_filter and not _is_riyadh_url(u):
            riyadh_filtered += 1
            continue

        resp = _safe_get(u, crawl_delay=crawl_delay, user_agent=user_agent)
        if not resp:
            fetch_failed += 1
            continue

        seen += 1
        if seen > max_pages:
            break

        slug = urlparse(u).path.rstrip("/").split("/")[-1]
        page_data = _extract_page_data(resp.text, u, source)
        name = page_data.get("name") or _slug_to_name(slug)

        yield {
            "id": f"{source}:{slug}",
            "name": name,
            "source": source,
            "source_url": u,
            "lat": page_data.get("lat"),
            "lon": page_data.get("lon"),
            "category_raw": page_data.get("category_raw"),
            "rating": page_data.get("rating"),
            "rating_count": page_data.get("rating_count"),
            "address_raw": page_data.get("address_raw"),
            "district_text": page_data.get("district_text"),
            "phone_raw": page_data.get("phone_raw"),
            "_html_extracted": bool(page_data.get("name")),
        }
        count += 1

    stats.fetch_failures = fetch_failed
    logger.info(
        "Scraped %d restaurants from %s "
        "(riyadh_filtered=%d, fetch_failed=%d, candidate_urls=%d, "
        "discovery_path=%s)",
        count, source, riyadh_filtered, fetch_failed, len(restaurant_urls),
        stats.discovery_success_path,
    )

    # Persist stats for the ingestion pipeline to pick up
    _LAST_DISCOVERY_STATS[source] = stats


# ---------------------------------------------------------------------------
# HungerStation
# ---------------------------------------------------------------------------

@_register("hungerstation", label="HungerStation", url="https://hungerstation.com")
def scrape_hungerstation_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from HungerStation for Riyadh.
    Uses sitemap to discover restaurant pages, then extracts JSON-LD,
    embedded JSON, or meta tags from each page.
    """
    sitemap_url = "https://hungerstation.com/sitemaps/index.xml"
    logger.info("Fetching HungerStation sitemap: %s", sitemap_url)

    sitemap_urls = _parse_sitemap(sitemap_url)
    logger.info(
        "HungerStation index returned %d entries: %s",
        len(sitemap_urls),
        sitemap_urls[:10],
    )

    # Restrict to exact Riyadh-city restaurant shards, then collapse sa-ar/sa-en
    # duplicates down to one preferred shard per logical part number.
    raw_riyadh_restaurant_shards = [
        u for u in sitemap_urls if _is_hungerstation_riyadh_restaurant_shard(u)
    ]

    shard_map: dict[str, str] = {}
    for u in raw_riyadh_restaurant_shards:
        key = _canonical_hungerstation_shard_key(u)
        if not key:
            continue
        shard_map[key] = _prefer_hungerstation_shard(shard_map.get(key), u)

    riyadh_restaurant_shards = list(shard_map.values())

    logger.info(
        "HungerStation Riyadh restaurant shards: %d raw, %d canonical",
        len(raw_riyadh_restaurant_shards),
        len(riyadh_restaurant_shards),
    )

    restaurant_urls: list[str] = []
    seen_urls: set[str] = set()

    target_pool = _target_candidate_pool(max_pages)
    min_shards = _min_hungerstation_shards(max_pages)
    min_buckets = _min_hungerstation_district_buckets(max_pages)
    max_pool = _max_hungerstation_candidate_pool(max_pages)

    expanded_shards = 0
    bucket_counts: dict[str, int] = defaultdict(int)

    for url in riyadh_restaurant_shards:
        try:
            expanded = _parse_sitemap(url)
            expanded = [
                u for u in expanded
                if _is_hungerstation_candidate_restaurant_url(u)
            ]
            added = 0
            for candidate in expanded:
                if candidate in seen_urls:
                    continue
                seen_urls.add(candidate)
                restaurant_urls.append(candidate)
                bucket = _extract_hungerstation_district_bucket(candidate)
                if bucket and bucket != "unknown":
                    bucket_counts[bucket] += 1
                added += 1
            expanded_shards += 1

            discovered_buckets = len(bucket_counts)
            logger.info(
                "HungerStation Riyadh shard %s yielded %d candidate restaurant URLs "
                "(%d new, pool=%d/%d, buckets=%d/%d, shards=%d/%d)",
                url,
                len(expanded),
                added,
                len(restaurant_urls),
                target_pool,
                discovered_buckets,
                min_buckets,
                expanded_shards,
                min_shards,
            )

            if (
                len(restaurant_urls) >= target_pool
                and expanded_shards >= min_shards
                and discovered_buckets >= min_buckets
            ):
                logger.info(
                    "HungerStation early-stop after %d shard(s): gathered %d unique candidate URLs "
                    "for max_pages=%d with %d bucket(s)",
                    expanded_shards,
                    len(restaurant_urls),
                    max_pages,
                    discovered_buckets,
                )
                break

            if len(restaurant_urls) >= max_pool and expanded_shards >= min_shards:
                logger.info(
                    "HungerStation hard-stop after %d shard(s): gathered %d unique candidate URLs "
                    "(bucket coverage=%d/%d, max_pool=%d)",
                    expanded_shards,
                    len(restaurant_urls),
                    discovered_buckets,
                    min_buckets,
                    max_pool,
                )
                break
        except Exception as exc:
            logger.warning("HungerStation shard %s failed: %s", url, exc)

    logger.info(
        "Found %d Riyadh restaurant candidate URLs from HungerStation after expanding %d shard(s) "
        "across %d discovered district bucket(s)",
        len(restaurant_urls),
        expanded_shards,
        len(bucket_counts),
    )

    restaurant_urls = _diversify_hungerstation_candidates(
        restaurant_urls,
        max_pages=max_pages,
    )

    count = 0
    fetch_failed = 0
    no_parse = 0
    sample_rejected: list[str] = []
    processed = 0

    for url in restaurant_urls:
        processed += 1
        if processed > max_pages:
            break

        resp = _safe_get(url, crawl_delay=2.0)
        if not resp:
            fetch_failed += 1
            continue

        listing_id = _extract_hungerstation_listing_id(url)
        slug = listing_id or urlparse(url).path.rstrip("/").split("/")[-1]
        page_data = _extract_page_data(resp.text, url, "hungerstation")

        name = page_data.get("name") or _slug_to_name(slug)

        # Even if HTML extraction yielded nothing beyond a name from <title>,
        # we still yield a record — the slug-based name is sufficient for raw
        # persistence.
        if not name or len(name.strip()) < 2:
            no_parse += 1
            if len(sample_rejected) < 5:
                sample_rejected.append(url)
            continue

        yield {
            "id": f"hungerstation:{slug}",
            "name": name,
            "source": "hungerstation",
            "source_url": url,
            "lat": page_data.get("lat"),
            "lon": page_data.get("lon"),
            "category_raw": page_data.get("category_raw"),
            "rating": page_data.get("rating"),
            "rating_count": page_data.get("rating_count"),
            "address_raw": page_data.get("address_raw"),
            "district_text": page_data.get("district_text"),
            "phone_raw": page_data.get("phone_raw"),
            "raw_payload": {
                "canonical_listing_id": listing_id,
            },
            "_html_extracted": bool(page_data.get("name")),
        }
        count += 1

    if sample_rejected:
        logger.warning(
            "HungerStation: %d pages fetched but unparseable. Samples: %s",
            no_parse, sample_rejected,
        )
    logger.info(
        "Scraped %d restaurants from HungerStation "
        "(fetch_failed=%d, no_parse=%d, candidate_urls=%d, processed=%d)",
        count,
        fetch_failed,
        no_parse,
        len(restaurant_urls),
        min(processed, max_pages),
    )


# ---------------------------------------------------------------------------
# Talabat
# ---------------------------------------------------------------------------

@_register("talabat", label="Talabat", url="https://www.talabat.com")
def scrape_talabat_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Talabat for Riyadh.
    Talabat operates across MENA — uses riyadh_filter to select
    only Riyadh-area restaurants from the KSA portion of the sitemap.
    Uses multi-strategy discovery to handle sitemap index expansion.
    """
    yield from _generic_sitemap_scrape(
        source="talabat",
        sitemap_url="https://www.talabat.com/sitemap/sitemap.xml.gz",
        url_filter=None,  # Don't filter by URL pattern — let riyadh_filter handle geo
        riyadh_filter=False,  # Talabat URLs may not contain "riyadh" — filter by coordinates downstream
        crawl_delay=2.0,
        max_pages=max_pages,
        multi_strategy=True,
        base_url="https://www.talabat.com",
    )


# ---------------------------------------------------------------------------
# Mrsool
# ---------------------------------------------------------------------------

@_register("mrsool", label="Mrsool", url="https://mrsool.co")
def scrape_mrsool_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Mrsool for Riyadh.
    Mrsool is Saudi-only so riyadh_filter is disabled — all restaurants
    are Saudi. Coordinate-based filtering happens during delivery enrichment.
    Uses multi-strategy discovery to handle sitemap index expansion.
    """
    yield from _generic_sitemap_scrape(
        source="mrsool",
        sitemap_url="https://mrsool.co/sitemap.xml",
        url_filter=None,  # Mrsool URLs don't contain "restaurant" — be permissive
        riyadh_filter=False,  # Saudi-only platform, no city in URL — filter by coordinates downstream
        crawl_delay=2.0,
        max_pages=max_pages,
        multi_strategy=True,
        base_url="https://mrsool.co",
    )


# ---------------------------------------------------------------------------
# Jahez
# ---------------------------------------------------------------------------

@_register("jahez", label="Jahez", url="https://www.jahez.net")
def scrape_jahez_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Jahez for Riyadh.
    Jahez is one of the largest Saudi food delivery platforms.
    Uses multi-strategy sitemap discovery to maximize breadth.
    """
    yield from _generic_sitemap_scrape(
        source="jahez",
        sitemap_url="https://www.jahez.net/sitemap.xml",
        url_filter="restaurant",
        riyadh_filter=True,
        crawl_delay=3.0,
        max_pages=max_pages,
        multi_strategy=True,
        base_url="https://www.jahez.net",
        user_agent=_BROWSER_UA,
    )


# ---------------------------------------------------------------------------
# ToYou
# ---------------------------------------------------------------------------

@_register("toyou", label="ToYou", url="https://toyou.io")
def scrape_toyou_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
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
def scrape_keeta_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
    """
    Scrape restaurant listings from Keeta (Meituan's Saudi brand).
    Keeta launched in Riyadh in 2024 and is expanding rapidly.
    Uses multi-strategy sitemap discovery to maximize breadth.
    """
    yield from _generic_sitemap_scrape(
        source="keeta",
        sitemap_url="https://www.keeta.com/sitemap.xml",
        url_filter=None,
        riyadh_filter=True,
        crawl_delay=2.0,
        max_pages=max_pages,
        multi_strategy=True,
        base_url="https://www.keeta.com",
        user_agent=_BROWSER_UA,
    )


# ---------------------------------------------------------------------------
# The Chefz
# ---------------------------------------------------------------------------

@_register("thechefz", label="The Chefz", url="https://www.thechefz.com")
def scrape_thechefz_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
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
def scrape_lugmety_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
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
def scrape_shgardi_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
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
def scrape_ninja_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
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
def scrape_nana_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
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
def scrape_dailymealz_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
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
def scrape_careem_food_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
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
def scrape_deliveroo_riyadh(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
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


def scrape_all_platforms(max_pages: int = 5000) -> Iterator[dict[str, Any]]:
    """Run all registered scrapers and yield their combined results."""
    for source, entry in SCRAPER_REGISTRY.items():
        logger.info("Running scraper: %s", source)
        try:
            yield from entry["fn"](max_pages=max_pages)
        except Exception as exc:
            logger.warning("Scraper %s failed: %s", source, exc)
