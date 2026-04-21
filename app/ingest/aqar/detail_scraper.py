"""Aqar.fm listing detail-page scraper — Info-block extraction.

Every Aqar listing detail page exposes a rich "Info" block (rendered in
server-side HTML — no JS needed) with fields we don't capture from the
list page:

  Created At:              21/01/2026
  Last Update:             1 minute ago
  Views:                   516
  Listing Source:          REGA
  Advertisement License:   7200846411
  License Expiry Date:     24/10/2026
  Plan and Parcel:         4027 - مستودع / 199
  Area as per Deed:        1477.41
  ID:                      6556192

This module turns an Aqar detail URL into an ``AqarDetailPayload`` with
the nine fields the migration added. The parser is split from the
fetcher (``parse_detail_html`` vs ``fetch_listing_detail``) so the
orchestration in ``scripts/scrape_aqar.py`` can reuse HTML it has
already fetched and the backfill script can drive the fetcher directly.

Conventions:
  * Pure HTTP — no Playwright, no JS rendering.
  * Same User-Agent rotation and BeautifulSoup layer the rest of the
    scraper already uses.
  * 5xx / transient network errors retry with exponential backoff; 404
    (listing removed) returns ``None``; structure-change (Info block
    absent) logs a warning and returns ``None``.
"""

from __future__ import annotations

import logging
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal, InvalidOperation
from typing import Iterable

import requests
from bs4 import BeautifulSoup, Tag

from app.ingest.aqar.relative_time import parse_relative_time

logger = logging.getLogger(__name__)


USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
)


# ---------------------------------------------------------------------------
# Label catalog — English ⟷ Arabic variants for each Info-block field
# ---------------------------------------------------------------------------

_CREATED_AT_LABELS = ("Created At", "تاريخ الإضافة")
_LAST_UPDATE_LABELS = ("Last Update", "آخر تحديث")
_VIEWS_LABELS = ("Views", "المشاهدات", "عدد المشاهدات")
_LISTING_SOURCE_LABELS = ("Listing Source", "مصدر الإعلان")
_AD_LICENSE_LABELS = ("Advertisement License", "رخصة الإعلان", "رقم رخصة الإعلان")
_LICENSE_EXPIRY_LABELS = (
    "License Expiry Date",
    "License Expiry",
    "تاريخ انتهاء الترخيص",
    "تاريخ انتهاء الرخصة",
)
_PLAN_PARCEL_LABELS = ("Plan and Parcel", "Plan & Parcel", "المخطط", "المخطط والقطعة")
_AREA_DEED_LABELS = ("Area as per Deed", "المساحة حسب الصك", "مساحة الصك")

# Any one of these label hits tells us the Info block is present. If
# NONE are found we treat the page as a structure change (or a Cloudflare
# interstitial) and return None.
_INFO_BLOCK_MARKERS: tuple[str, ...] = (
    _CREATED_AT_LABELS
    + _AD_LICENSE_LABELS
    + _LICENSE_EXPIRY_LABELS
    + _PLAN_PARCEL_LABELS
    + _AREA_DEED_LABELS
)


_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

_DATE_RE = re.compile(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})")
_INT_RE = re.compile(r"-?\d[\d,]*")
_DECIMAL_RE = re.compile(r"-?\d[\d,]*\.?\d*")


# ---------------------------------------------------------------------------
# Payload
# ---------------------------------------------------------------------------


@dataclass
class AqarDetailPayload:
    """Structured result of parsing an Aqar listing detail page."""

    aqar_created_at: datetime | None
    aqar_updated_at: datetime | None
    aqar_views: int | None
    aqar_advertisement_license: str | None
    aqar_license_expiry: date | None
    aqar_plan_parcel: str | None
    aqar_area_deed: Decimal | None
    aqar_listing_source: str | None
    aqar_detail_scraped_at: datetime


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------


def fetch_listing_detail(
    aqar_id: str,
    listing_url: str,
    session: requests.Session,
    *,
    max_retries: int = 3,
    timeout: int = 15,
) -> AqarDetailPayload | None:
    """Fetch the Aqar detail page for ``aqar_id`` and parse the Info block.

    Returns an ``AqarDetailPayload`` on success, or ``None`` if:
      * The listing 404s (removed from Aqar).
      * The page loads but the Info block is missing (structure change).
      * All retry attempts exhaust on 5xx / network errors.

    The caller is responsible for rate-limiting between consecutive
    invocations; this function does not sleep on success.
    """
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    last_exc: Exception | None = None

    for attempt in range(max_retries):
        fetched_at = datetime.now(timezone.utc)
        try:
            resp = session.get(listing_url, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning(
                "Aqar detail fetch network error for aqar_id=%s (attempt %d/%d): %s",
                aqar_id, attempt + 1, max_retries, exc,
            )
            _backoff_sleep(attempt)
            continue

        if resp.status_code == 404:
            logger.info("Aqar listing %s returned 404 (removed)", aqar_id)
            return None

        if 500 <= resp.status_code < 600:
            logger.warning(
                "Aqar detail fetch 5xx for aqar_id=%s (attempt %d/%d): %s",
                aqar_id, attempt + 1, max_retries, resp.status_code,
            )
            _backoff_sleep(attempt)
            continue

        if resp.status_code != 200:
            logger.warning(
                "Aqar detail fetch non-200 for aqar_id=%s: %s",
                aqar_id, resp.status_code,
            )
            return None

        return parse_detail_html(resp.text, fetched_at)

    logger.warning(
        "Aqar detail fetch exhausted retries for aqar_id=%s (last error: %s)",
        aqar_id, last_exc,
    )
    return None


def _backoff_sleep(attempt: int) -> None:
    # 2s, 4s, 8s — short enough to be polite, long enough to ride out a
    # typical Aqar edge-server hiccup.
    delay = min(2 ** (attempt + 1), 8)
    time.sleep(delay)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def parse_detail_html(html: str, fetched_at: datetime) -> AqarDetailPayload | None:
    """Parse an Aqar listing detail page into an ``AqarDetailPayload``.

    Returns ``None`` if none of the known Info-block labels can be found;
    callers should treat that as a structure change and alarm on it.
    """
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    if not any(marker in page_text for marker in _INFO_BLOCK_MARKERS):
        logger.warning(
            "Aqar detail page missing Info block — possible structure change"
        )
        return None

    created_at_date = _extract_date(soup, _CREATED_AT_LABELS)
    created_at_dt = (
        datetime.combine(created_at_date, dt_time(0, 0, 0), tzinfo=timezone.utc)
        if created_at_date
        else None
    )

    last_update_raw = _extract_label_value(soup, _LAST_UPDATE_LABELS)
    updated_at_dt = (
        parse_relative_time(last_update_raw, fetched_at) if last_update_raw else None
    )

    views = _extract_int(soup, _VIEWS_LABELS)
    ad_license = _extract_label_value(soup, _AD_LICENSE_LABELS)
    license_expiry = _extract_date(soup, _LICENSE_EXPIRY_LABELS)
    plan_parcel = _extract_label_value(soup, _PLAN_PARCEL_LABELS)
    area_deed = _extract_decimal(soup, _AREA_DEED_LABELS)
    listing_source = _extract_label_value(soup, _LISTING_SOURCE_LABELS)

    return AqarDetailPayload(
        aqar_created_at=created_at_dt,
        aqar_updated_at=updated_at_dt,
        aqar_views=views,
        aqar_advertisement_license=_normalize_license(ad_license),
        aqar_license_expiry=license_expiry,
        aqar_plan_parcel=_clean_whitespace(plan_parcel),
        aqar_area_deed=area_deed,
        aqar_listing_source=_clean_whitespace(listing_source),
        aqar_detail_scraped_at=fetched_at,
    )


# ---------------------------------------------------------------------------
# Label/value extraction helpers
# ---------------------------------------------------------------------------


def _extract_label_value(soup: BeautifulSoup, labels: Iterable[str]) -> str | None:
    """Find a label string in the document and return the neighbouring value.

    Matches any of ``labels`` and tries three recovery strategies in
    order — the next sibling, the parent's next sibling, then the
    parent's split text. This mirrors the approach
    ``scripts/scrape_aqar.py::_extract_property_type`` takes for the
    list-page "Property Type" field, which is the same rendering
    pattern Aqar uses in the Info block.
    """
    for label_text in labels:
        for label_el in soup.find_all(
            string=lambda s, lt=label_text: s and lt in s and len(s.strip()) <= len(lt) + 20
        ):
            parent = label_el.parent
            if parent is None:
                continue

            # Strategy 1: immediate next sibling element
            value = _sibling_text(parent, label_text)
            if value:
                return value

            # Strategy 2: parent's next sibling
            if parent.parent is not None:
                value = _sibling_text(parent.parent, label_text)
                if value:
                    return value

            # Strategy 3: split parent combined text around the label
            value = _split_parent_text(parent, label_text)
            if value:
                return value

    return None


def _sibling_text(element: Tag, label_text: str) -> str | None:
    """Return the trimmed text of the first non-empty following sibling."""
    sib = element.find_next_sibling()
    while sib is not None:
        if isinstance(sib, Tag):
            txt = sib.get_text(" ", strip=True)
        else:
            txt = str(sib).strip()
        if txt and txt != label_text and label_text not in txt[: len(label_text) + 1]:
            return txt
        sib = sib.find_next_sibling() if hasattr(sib, "find_next_sibling") else None
    return None


def _split_parent_text(parent: Tag, label_text: str) -> str | None:
    """Extract ``label_text``'s neighbour from the parent's joined text.

    Labels and values often live in sibling elements inside the same
    parent; joining children with a separator and splitting around the
    label pulls the value out even when the DOM shape varies.
    """
    combined = parent.get_text(separator="|", strip=True)
    parts = [p.strip() for p in combined.split("|") if p.strip()]
    for i, part in enumerate(parts):
        if part == label_text and i + 1 < len(parts):
            return parts[i + 1]
    return None


def _extract_date(
    soup: BeautifulSoup, labels: Iterable[str]
) -> date | None:
    raw = _extract_label_value(soup, labels)
    return _parse_dd_mm_yyyy(raw) if raw else None


def _extract_int(soup: BeautifulSoup, labels: Iterable[str]) -> int | None:
    raw = _extract_label_value(soup, labels)
    if not raw:
        return None
    normalized = raw.translate(_ARABIC_DIGITS)
    m = _INT_RE.search(normalized)
    if not m:
        return None
    try:
        return int(m.group(0).replace(",", ""))
    except ValueError:
        return None


def _extract_decimal(soup: BeautifulSoup, labels: Iterable[str]) -> Decimal | None:
    raw = _extract_label_value(soup, labels)
    if not raw:
        return None
    normalized = raw.translate(_ARABIC_DIGITS)
    m = _DECIMAL_RE.search(normalized)
    if not m:
        return None
    # Aqar uses ``.`` as decimal separator for "Area as per Deed" (the
    # 1477.41 example in the spec); commas only appear as thousands
    # grouping on this specific field, so strip them unconditionally.
    token = m.group(0).replace(",", "")
    try:
        return Decimal(token)
    except InvalidOperation:
        return None


def _parse_dd_mm_yyyy(text: str) -> date | None:
    if not text:
        return None
    normalized = text.translate(_ARABIC_DIGITS)
    m = _DATE_RE.search(normalized)
    if not m:
        return None
    day, month, year = (int(x) for x in m.groups())
    try:
        return date(year, month, day)
    except ValueError:
        logger.warning("Aqar detail date out of range: %r", text)
        return None


def _clean_whitespace(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned or None


def _normalize_license(value: str | None) -> str | None:
    """License numbers may arrive with embedded spaces — collapse them."""
    if value is None:
        return None
    cleaned = re.sub(r"\s+", "", value).strip()
    return cleaned or None
