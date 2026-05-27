"""Bayut.sa pagination helper — discovers listing IDs from the commercial index.

Bayut renders ``/en/to-rent/commercial/<city>/`` as a single
server-side HTML grid covering all commercial accommodation categories
(Showroom, Office, Warehouse, Commercial Building, Complex). The
parser-level ``accommodationCategory`` filter restricts the writer's
pool to {Showroom, Office}. There is no per-category landing page on
real Bayut; the v1 ``/{slug}-for-rent/`` URLs returned a generic
landing carousel of mostly residential featured listings.

This module walks ``?page=N`` pagination, extracts unique listing IDs
per page via the canonical ``/en/property/details-<id>.html`` anchor
pattern, and stops when a page yields zero new IDs or returns
non-200.
"""

from __future__ import annotations

import logging
import random
import re
import time
from typing import Iterator

import requests

logger = logging.getLogger(__name__)


_BAYUT_BASE = "https://www.bayut.sa"

_LISTING_HREF_RE = re.compile(r"/en/property/details-(\d+)\.html")


USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
)


def fetch_commercial_listing_ids(
    session: requests.Session,
    city: str = "riyadh",
    max_pages: int = 20,
    *,
    rate_limit: float = 1.0,
    timeout: float = 30.0,
) -> Iterator[tuple[str, str]]:
    """Yield ``(listing_id, listing_url)`` tuples for commercial-for-rent in ``city``.

    Walks pages 1..``max_pages``. Stops early on:
      * HTTP non-200 from the index page
      * a page that yields zero new (previously-unseen) listing IDs

    Each subsequent page fetch is preceded by ``time.sleep(rate_limit)``
    to honour the conservative 1 req/sec default. Riyadh commercial
    inventory is small (~hundreds of listings), so ``max_pages=20``
    provides a ~500-listing ceiling well above realistic volume.
    """
    seen: set[str] = set()
    base_url = f"{_BAYUT_BASE}/en/to-rent/commercial/{city}/"

    for page_num in range(1, max_pages + 1):
        if page_num > 1:
            time.sleep(rate_limit)

        page_url = base_url if page_num == 1 else f"{base_url}?page={page_num}"
        headers = {"User-Agent": random.choice(USER_AGENTS)}

        try:
            resp = session.get(page_url, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            logger.warning(
                "Bayut commercial index fetch error (page %d): %s — stopping",
                page_num, exc,
            )
            return

        if resp.status_code != 200:
            logger.warning(
                "Bayut commercial index page %d returned HTTP %d — stopping",
                page_num, resp.status_code,
            )
            return

        new_ids = _extract_listing_ids(resp.text, seen)
        if not new_ids:
            logger.info(
                "Bayut commercial index exhausted at page %d (no new IDs)",
                page_num,
            )
            return

        for listing_id in new_ids:
            seen.add(listing_id)
            url = f"{_BAYUT_BASE}/en/property/details-{listing_id}.html"
            yield listing_id, url


def _extract_listing_ids(html: str, already_seen: set[str]) -> list[str]:
    """Return new (not in ``already_seen``) listing IDs in page order."""
    found: list[str] = []
    seen_this_page: set[str] = set()
    for match in _LISTING_HREF_RE.finditer(html):
        listing_id = match.group(1)
        if listing_id in already_seen or listing_id in seen_this_page:
            continue
        seen_this_page.add(listing_id)
        found.append(listing_id)
    return found
