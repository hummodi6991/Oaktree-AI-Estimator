#!/usr/bin/env python3
"""Bayut.sa crawler — fetch Riyadh commercial-for-rent Showrooms and Offices.

PR4 of the multi-portal listings series. Writes to ``commercial_unit``
with ``platform='bayut'``, ``platform_listing_id=<bayut-id>``, and a
prefixed primary key ``aqar_id=f"bayut:{bayut-id}"`` (Option α from the
design doc — the column name is a legacy of the Aqar-only era; PR4 keeps
the existing schema and namespaces the values).

URL: ``https://www.bayut.sa/en/to-rent/commercial/riyadh/?page=N`` —
one index covers all commercial accommodation categories (Showroom,
Office, Warehouse, Commercial Building, Complex). The parser's
``accommodationCategory`` filter restricts the writer's pool to
{Showroom, Office} — the only F&B-compatible categories in PR4 v1.

The script mirrors ``scripts/scrape_aqar.py``:
  * Same HTTP retry policy (429 + 5xx, backoff_factor=0.5, total=5).
  * Same User-Agent rotation pattern, shared cookie jar.
  * Reuses ``classify_restaurant_suitability`` from scrape_aqar
    (do NOT duplicate — investigation noted Bayut rows lack a few
    optional structural fields and the classifier handles those
    gracefully via ``.get()`` calls).
  * Same SELECT-then-(INSERT or UPDATE) upsert shape, gated by the
    50% coverage floor on the immediate-close sweep.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import time
from datetime import datetime, timezone
from decimal import Decimal

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.ingest.bayut.detail_scraper import (
    BayutDetailPayload,
    parse_detail_html,
)
from app.ingest.bayut.list_scraper import (
    USER_AGENTS,
    fetch_commercial_listing_ids,
)
from scripts.scrape_aqar import classify_restaurant_suitability

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# Riyadh bounding box — same constants Aqar enforces. Listings whose
# parsed lat/lon land outside this box get skipped at write time.
RIYADH_LAT_MIN, RIYADH_LAT_MAX = 24.4, 25.1
RIYADH_LON_MIN, RIYADH_LON_MAX = 46.4, 47.0

_BAYUT_HTTP_TIMEOUT = (10.0, 30.0)
_DETAIL_FETCH_RETRY_BUDGET = 5
_STALE_DAYS = 28
_IMMEDIATE_CLOSE_MIN_COVERAGE = 0.5


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------


def _build_bayut_session() -> requests.Session:
    """Shared session with retry on 429 + 5xx, persistent cookie jar."""
    retry = Retry(
        total=_DETAIL_FETCH_RETRY_BUDGET,
        connect=_DETAIL_FETCH_RETRY_BUDGET,
        read=_DETAIL_FETCH_RETRY_BUDGET,
        status=_DETAIL_FETCH_RETRY_BUDGET,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _fetch_detail_html(
    session: requests.Session,
    url: str,
    timeout: tuple[float, float] = _BAYUT_HTTP_TIMEOUT,
) -> str | None:
    """Fetch a Bayut detail page. Returns the HTML or ``None`` on failure."""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        resp = session.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        logger.warning("Bayut detail fetch error for %s: %s", url, exc)
        return None
    if resp.status_code != 200:
        logger.warning("Bayut detail fetch non-200 (%d) for %s", resp.status_code, url)
        return None
    return resp.text


# ---------------------------------------------------------------------------
# Riyadh bounding box guard
# ---------------------------------------------------------------------------


def _within_riyadh(lat: float | None, lon: float | None) -> bool:
    if lat is None or lon is None:
        return False
    return (
        RIYADH_LAT_MIN <= lat <= RIYADH_LAT_MAX
        and RIYADH_LON_MIN <= lon <= RIYADH_LON_MAX
    )


# ---------------------------------------------------------------------------
# Payload → listing dict
# ---------------------------------------------------------------------------


def _payload_to_listing(payload: BayutDetailPayload) -> dict:
    """Project a ``BayutDetailPayload`` onto the listing-dict shape the
    classifier and upsert layer expect (mirrors Aqar's listing dict)."""
    return {
        "aqar_id": f"bayut:{payload.platform_listing_id}",
        "platform": "bayut",
        "platform_listing_id": payload.platform_listing_id,
        "title": payload.title,
        "description": payload.description,
        "neighborhood": payload.neighborhood,
        "listing_url": payload.listing_url,
        "image_url": payload.image_url,
        "price_sar_annual": payload.price_sar_annual,
        "area_sqm": payload.area_sqm,
        "lat": payload.lat,
        "lon": payload.lon,
        "contact_phone": payload.contact_phone,
        "listing_type": payload.listing_type,
        "property_type": payload.property_type,
        "aqar_advertisement_license": payload.aqar_advertisement_license,
        "aqar_listing_source": payload.aqar_listing_source,
        "aqar_created_at": payload.aqar_created_at,
        "aqar_updated_at": payload.aqar_updated_at,
        "aqar_detail_scraped_at": payload.aqar_detail_scraped_at,
        # Fields that are NULL on Bayut rows (acceptable per the
        # investigation report) — set explicitly so _bayut_listing_params
        # always finds them via .get().
        "street_width_m": None,
        "num_floors": None,
        "has_mezzanine": None,
        "has_drive_thru": None,
        "facade_direction": None,
        "is_furnished": None,
        "apartments_count": None,
        "num_rooms": None,
        "aqar_license_expiry": None,
        "aqar_plan_parcel": None,
        "aqar_area_deed": None,
        "aqar_views": None,
    }


def _compute_price_per_sqm(listing: dict) -> Decimal | None:
    """Derive price per sqm from annual price and area."""
    price = listing.get("price_sar_annual")
    area = listing.get("area_sqm")
    if price and area and float(area) > 0:
        return (Decimal(str(price)) / Decimal(str(area))).quantize(Decimal("0.01"))
    return None


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------


def upsert_bayut_listing(db, listing: dict) -> str:
    """Insert or update a single Bayut listing in commercial_unit.

    Mirrors Aqar's SELECT-then-(INSERT or UPDATE) shape. Returns
    ``'insert'`` or ``'update'``.
    """
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
                "llm_suitability_verdict = COALESCE(:llm_suitability_verdict, commercial_unit.llm_suitability_verdict), "
                "llm_suitability_score = COALESCE(:llm_suitability_score, commercial_unit.llm_suitability_score), "
                "llm_listing_quality_score = COALESCE(:llm_listing_quality_score, commercial_unit.llm_listing_quality_score), "
                "llm_landlord_signal_score = COALESCE(:llm_landlord_signal_score, commercial_unit.llm_landlord_signal_score), "
                "llm_reasoning = COALESCE(:llm_reasoning, commercial_unit.llm_reasoning), "
                "llm_classified_at = COALESCE(:llm_classified_at, commercial_unit.llm_classified_at), "
                "llm_classifier_version = COALESCE(:llm_classifier_version, commercial_unit.llm_classifier_version), "
                "aqar_created_at = COALESCE(:aqar_created_at, commercial_unit.aqar_created_at), "
                "aqar_updated_at = COALESCE(:aqar_updated_at, commercial_unit.aqar_updated_at), "
                "aqar_views = COALESCE(:aqar_views, commercial_unit.aqar_views), "
                "aqar_advertisement_license = COALESCE(:aqar_advertisement_license, commercial_unit.aqar_advertisement_license), "
                "aqar_license_expiry = COALESCE(:aqar_license_expiry, commercial_unit.aqar_license_expiry), "
                "aqar_plan_parcel = COALESCE(:aqar_plan_parcel, commercial_unit.aqar_plan_parcel), "
                "aqar_area_deed = COALESCE(:aqar_area_deed, commercial_unit.aqar_area_deed), "
                "aqar_listing_source = COALESCE(:aqar_listing_source, commercial_unit.aqar_listing_source), "
                "aqar_detail_scraped_at = COALESCE(:aqar_detail_scraped_at, commercial_unit.aqar_detail_scraped_at), "
                "platform = :platform, "
                "platform_listing_id = :platform_listing_id, "
                "status = 'active', last_seen_at = now() "
                "WHERE aqar_id = :aqar_id"
            ),
            _bayut_listing_params(listing),
        )
        return "update"

    db.execute(
        sa_text(
            "INSERT INTO commercial_unit "
            "(aqar_id, title, description, neighborhood, listing_url, image_url, "
            "price_sar_annual, price_per_sqm, area_sqm, street_width_m, "
            "num_floors, has_mezzanine, has_drive_thru, facade_direction, "
            "contact_phone, listing_type, property_type, is_furnished, apartments_count, num_rooms, lat, lon, "
            "restaurant_score, restaurant_suitable, restaurant_signals, "
            "llm_suitability_verdict, llm_suitability_score, "
            "llm_listing_quality_score, llm_landlord_signal_score, "
            "llm_reasoning, llm_classified_at, llm_classifier_version, "
            "aqar_created_at, aqar_updated_at, aqar_views, "
            "aqar_advertisement_license, aqar_license_expiry, aqar_plan_parcel, "
            "aqar_area_deed, aqar_listing_source, aqar_detail_scraped_at, "
            "platform, platform_listing_id, "
            "status, first_seen_at, last_seen_at) "
            "VALUES (:aqar_id, :title, :description, :neighborhood, :listing_url, :image_url, "
            ":price_sar_annual, :price_per_sqm, :area_sqm, :street_width_m, "
            ":num_floors, :has_mezzanine, :has_drive_thru, :facade_direction, "
            ":contact_phone, :listing_type, :property_type, :is_furnished, :apartments_count, :num_rooms, :lat, :lon, "
            ":restaurant_score, :restaurant_suitable, :restaurant_signals, "
            ":llm_suitability_verdict, :llm_suitability_score, "
            ":llm_listing_quality_score, :llm_landlord_signal_score, "
            ":llm_reasoning, :llm_classified_at, :llm_classifier_version, "
            ":aqar_created_at, :aqar_updated_at, :aqar_views, "
            ":aqar_advertisement_license, :aqar_license_expiry, :aqar_plan_parcel, "
            ":aqar_area_deed, :aqar_listing_source, :aqar_detail_scraped_at, "
            ":platform, :platform_listing_id, "
            "'active', now(), now())"
        ),
        _bayut_listing_params(listing),
    )
    return "insert"


def _bayut_listing_params(listing: dict) -> dict:
    """Build parameter dict for SQL statements from a Bayut listing dict."""
    platform_listing_id = listing.get("platform_listing_id")
    aqar_id = listing.get("aqar_id") or (
        f"bayut:{platform_listing_id}" if platform_listing_id else None
    )
    return {
        "aqar_id": aqar_id,
        "platform": "bayut",
        "platform_listing_id": platform_listing_id,
        "title": listing.get("title"),
        "description": listing.get("description"),
        "neighborhood": listing.get("neighborhood"),
        "listing_url": listing.get("listing_url"),
        "image_url": listing.get("image_url"),
        "price_sar_annual": _to_decimal(listing.get("price_sar_annual")),
        "price_per_sqm": _to_decimal(listing.get("price_per_sqm")),
        "area_sqm": _to_decimal(listing.get("area_sqm")),
        "street_width_m": _to_decimal(listing.get("street_width_m")),
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
        "lat": _to_decimal(listing.get("lat")),
        "lon": _to_decimal(listing.get("lon")),
        "restaurant_score": listing.get("restaurant_score"),
        "restaurant_suitable": listing.get("restaurant_suitable"),
        "restaurant_signals": json.dumps(listing.get("restaurant_signals", [])),
        "llm_suitability_verdict": listing.get("llm_suitability_verdict"),
        "llm_suitability_score": listing.get("llm_suitability_score"),
        "llm_listing_quality_score": listing.get("llm_listing_quality_score"),
        "llm_landlord_signal_score": listing.get("llm_landlord_signal_score"),
        "llm_reasoning": listing.get("llm_reasoning"),
        "llm_classified_at": listing.get("llm_classified_at"),
        "llm_classifier_version": listing.get("llm_classifier_version"),
        "aqar_created_at": listing.get("aqar_created_at"),
        "aqar_updated_at": listing.get("aqar_updated_at"),
        "aqar_views": listing.get("aqar_views"),
        "aqar_advertisement_license": listing.get("aqar_advertisement_license"),
        "aqar_license_expiry": listing.get("aqar_license_expiry"),
        "aqar_plan_parcel": listing.get("aqar_plan_parcel"),
        "aqar_area_deed": _to_decimal(listing.get("aqar_area_deed")),
        "aqar_listing_source": listing.get("aqar_listing_source") or "Bayut",
        "aqar_detail_scraped_at": listing.get("aqar_detail_scraped_at"),
    }


def _to_decimal(value) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Stale-handling sweep — Bayut-scoped analogue of Aqar's mark_unseen_listings_closed
# ---------------------------------------------------------------------------


def mark_bayut_unseen_listings_closed(
    db,
    seen_aqar_ids: set[str],
    min_coverage: float = _IMMEDIATE_CLOSE_MIN_COVERAGE,
) -> int:
    """Flip Bayut listings not seen in the current scrape to ``stale``.

    Coverage-guarded: requires the scrape to have seen at least 50% of
    the currently-active Bayut pool before any same-day closure is
    trusted. A broken crawl must not nuke the whole pool — when below
    the coverage floor, the 28-day ``mark_stale_listings`` safety net
    handles cleanup instead.

    Returns the number of rows flipped to ``'stale'``, or ``-1`` when
    the coverage guard tripped.
    """
    from sqlalchemy import text as sa_text

    active_count = db.execute(
        sa_text(
            "SELECT COUNT(*) FROM commercial_unit "
            "WHERE status = 'active' AND platform = 'bayut'"
        ),
    ).scalar() or 0

    if active_count == 0:
        return 0

    coverage = len(seen_aqar_ids) / active_count
    if coverage < min_coverage:
        logger.warning(
            "Skipping Bayut immediate-close sweep: only %d of %d currently-active "
            "Bayut listings were seen (%.1f%% < %.0f%% coverage floor). Falling "
            "back to the %d-day stale marker.",
            len(seen_aqar_ids),
            active_count,
            coverage * 100,
            min_coverage * 100,
            _STALE_DAYS,
        )
        return -1

    result = db.execute(
        sa_text(
            "UPDATE commercial_unit SET status = 'stale' "
            "WHERE status = 'active' "
            "  AND platform = 'bayut' "
            "  AND aqar_id <> ALL(:seen_ids)"
        ),
        {"seen_ids": list(seen_aqar_ids)},
    )
    return result.rowcount


def mark_bayut_stale_listings(db) -> int:
    """Mark Bayut listings not seen in 28+ days as stale."""
    from sqlalchemy import text as sa_text

    result = db.execute(
        sa_text(
            "UPDATE commercial_unit SET status = 'stale' "
            "WHERE status = 'active' "
            "  AND platform = 'bayut' "
            "  AND last_seen_at < now() - make_interval(days => :days)"
        ),
        {"days": _STALE_DAYS},
    )
    return result.rowcount


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _get_db_session():
    from app.db.session import SessionLocal

    return SessionLocal()


def _listing_already_exists(aqar_id: str, db) -> bool:
    from sqlalchemy import text as sa_text

    result = db.execute(
        sa_text(
            "SELECT 1 FROM commercial_unit "
            "WHERE aqar_id = :aqar_id "
            "  AND status = 'active' "
            "  AND last_seen_at > NOW() - INTERVAL '7 days' "
            "LIMIT 1"
        ),
        {"aqar_id": aqar_id},
    ).first()
    return result is not None


def _touch_last_seen(aqar_id: str, db) -> None:
    from sqlalchemy import text as sa_text

    db.execute(
        sa_text(
            "UPDATE commercial_unit SET last_seen_at = NOW() "
            "WHERE aqar_id = :aqar_id"
        ),
        {"aqar_id": aqar_id},
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crawl Bayut.sa Riyadh commercial-for-rent Showrooms and Offices",
    )
    parser.add_argument("--max-pages", type=int, default=20,
                        help="Max pages on the commercial index (default: 20)")
    parser.add_argument("--no-detail", action="store_true",
                        help="Skip fetching individual listing detail pages")
    parser.add_argument("--skip-geocode", action="store_true", default=True,
                        help="Skip geocoding (default: True — Bayut exposes coords in JSON)")
    parser.add_argument("--no-skip-geocode", dest="skip_geocode", action="store_false",
                        help="Override the default skip-geocode behaviour")
    parser.add_argument("--rate-limit", type=float, default=1.0,
                        help="Seconds to sleep between requests (default: 1.0)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print results without writing to DB")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Skip listings already in DB with last_seen_at < 7 days")
    parser.add_argument("--no-resume", dest="resume", action="store_false")
    args = parser.parse_args()

    db = None
    if not args.dry_run or args.resume:
        try:
            db = _get_db_session()
        except Exception as exc:
            logger.warning("Could not connect to DB: %s", exc)
            if not args.dry_run:
                logger.error(
                    "DB required for persistence. Use --dry-run to skip DB writes."
                )
                return

    session = _build_bayut_session()
    stats = {
        "discovered": 0,
        "scraped": 0,
        "skipped_existing": 0,
        "skipped_outside_bbox": 0,
        "skipped_wrong_property_type": 0,
        "skipped_missing_field": 0,
        "skipped_not_suitable": 0,
        "errors": 0,
        "insert": 0,
        "update": 0,
    }
    seen_aqar_ids: set[str] = set()

    try:
        logger.info("=" * 60)
        logger.info("  Bayut commercial — Riyadh")
        logger.info("=" * 60)

        for listing_id, listing_url in fetch_commercial_listing_ids(
            session, city="riyadh", max_pages=args.max_pages,
            rate_limit=args.rate_limit,
        ):
            stats["discovered"] += 1
            aqar_id = f"bayut:{listing_id}"
            seen_aqar_ids.add(aqar_id)

            # Resume gate — skip listings we've already touched in the last 7d.
            if args.resume and db is not None and _listing_already_exists(aqar_id, db):
                _touch_last_seen(aqar_id, db)
                stats["skipped_existing"] += 1
                logger.info("scraped: skipped_existing %s", aqar_id)
                continue

            if args.no_detail:
                logger.info("scraped: no_detail %s", aqar_id)
                continue

            time.sleep(args.rate_limit)
            html = _fetch_detail_html(session, listing_url)
            if html is None:
                stats["errors"] += 1
                logger.info("error: fetch_failed %s", aqar_id)
                continue

            fetched_at = datetime.now(timezone.utc)
            payload = parse_detail_html(html, listing_url, fetched_at)
            if payload is None:
                # The parser logs the specific reason (wrong accommodation
                # category, missing payload, unsupported rentFrequency).
                stats["skipped_wrong_property_type"] += 1
                logger.info("skipped: parser_rejected %s", aqar_id)
                continue

            if not _within_riyadh(payload.lat, payload.lon):
                stats["skipped_outside_bbox"] += 1
                logger.info(
                    "skipped: outside_bbox %s lat=%s lon=%s",
                    aqar_id, payload.lat, payload.lon,
                )
                continue

            if payload.area_sqm is None or payload.area_sqm <= 0:
                stats["skipped_missing_field"] += 1
                logger.info("skipped: missing_required_field:area_sqm %s", aqar_id)
                continue

            if payload.price_sar_annual is None or payload.price_sar_annual <= 0:
                stats["skipped_missing_field"] += 1
                logger.info("skipped: missing_required_field:price_sar_annual %s", aqar_id)
                continue

            listing = _payload_to_listing(payload)
            classify_restaurant_suitability(listing)
            listing["price_per_sqm"] = _compute_price_per_sqm(listing)

            stats["scraped"] += 1

            if args.dry_run:
                logger.info("[DRY-RUN] %s %s", aqar_id, listing)
                continue

            try:
                action = upsert_bayut_listing(db, listing)
                stats[action] += 1
                logger.info(
                    "upserted: %s %s score=%s suitable=%s",
                    action, aqar_id,
                    listing.get("restaurant_score"),
                    listing.get("restaurant_suitable"),
                )
            except Exception as exc:
                stats["errors"] += 1
                logger.exception("error: upsert_failed %s: %s", aqar_id, exc)
                db.rollback()
                continue

            # Commit every 50 listings to keep transactions small.
            if (stats["insert"] + stats["update"]) % 50 == 0:
                db.commit()

        if db and not args.dry_run:
            db.commit()

        # Stale-handling sweeps — Bayut-scoped to avoid touching Aqar rows.
        if db is not None and not args.dry_run:
            closed = mark_bayut_unseen_listings_closed(db, seen_aqar_ids)
            db.commit()
            if closed > 0:
                logger.info("Marked %d Bayut listings as closed (not seen in this run)", closed)

            stale = mark_bayut_stale_listings(db)
            db.commit()
            if stale:
                logger.info("Marked %d Bayut listings as stale (28+ days unseen)", stale)

        logger.info(
            "Bayut scrape summary: discovered=%d scraped=%d skipped_existing=%d "
            "skipped_outside_bbox=%d skipped_wrong_property_type=%d "
            "skipped_missing_field=%d insert=%d update=%d errors=%d",
            stats["discovered"], stats["scraped"], stats["skipped_existing"],
            stats["skipped_outside_bbox"], stats["skipped_wrong_property_type"],
            stats["skipped_missing_field"],
            stats["insert"], stats["update"], stats["errors"],
        )
    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    main()
