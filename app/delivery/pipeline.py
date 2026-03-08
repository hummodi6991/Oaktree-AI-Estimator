"""
Delivery scraper pipeline orchestration.

Architecture:
1. Platform fetcher  — fetch page/API data (existing scrapers)
2. Parser            — extract structured DeliveryRecord from raw data
3. Raw writer        — persist into delivery_source_record
4. Resolver hook     — optionally match to restaurant_poi
5. Metrics/logging   — emit ingest run stats

The pipeline wraps the existing SCRAPER_REGISTRY scrapers but also
supports enhanced parsers that produce richer DeliveryRecord output.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Iterator

from sqlalchemy.orm import Session

from app.delivery.models import DeliveryIngestRun, DeliverySourceRecord
from app.delivery.schemas import DeliveryRecord, Platform
from app.delivery.parsers import parse_legacy_record, parse_page_content
from app.delivery.location import resolve_location
from app.services.restaurant_categories import normalize_category

logger = logging.getLogger(__name__)


def _normalize_name(raw: str | None) -> str | None:
    """Basic restaurant name normalization."""
    if not raw:
        return None
    name = raw.strip()
    # Remove common suffixes / noise
    for suffix in [" - Delivery", " - توصيل", " Restaurant", " مطعم"]:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
    # Collapse whitespace
    name = " ".join(name.split())
    return name if name else None


def record_to_row(rec: DeliveryRecord, run_id: int) -> DeliverySourceRecord:
    """Convert a DeliveryRecord pydantic model to an ORM row."""
    normalized_name = _normalize_name(rec.restaurant_name_raw)
    normalized_cat = normalize_category(rec.cuisine_raw or rec.category_raw)

    return DeliverySourceRecord(
        platform=rec.platform,
        source_listing_id=rec.source_listing_id,
        source_url=rec.source_url,
        scraped_at=rec.scraped_at,
        city=rec.city,
        district_text=rec.district_text,
        area_text=rec.area_text,
        address_raw=rec.address_raw,
        lat=rec.lat,
        lon=rec.lon,
        geocode_method=rec.geocode_method,
        location_confidence=rec.location_confidence,
        restaurant_name_raw=rec.restaurant_name_raw,
        restaurant_name_normalized=normalized_name or rec.restaurant_name_normalized,
        brand_raw=rec.brand_raw,
        branch_raw=rec.branch_raw,
        cuisine_raw=rec.cuisine_raw,
        category_raw=normalized_cat,
        category_confidence=rec.category_confidence,
        price_band_raw=rec.price_band_raw,
        rating=rec.rating,
        rating_count=rec.rating_count,
        delivery_time_min=rec.delivery_time_min,
        delivery_fee=rec.delivery_fee,
        minimum_order=rec.minimum_order,
        promo_text=rec.promo_text,
        availability_text=rec.availability_text,
        is_open_now_raw=rec.is_open_now_raw,
        phone_raw=rec.phone_raw,
        website_raw=rec.website_raw,
        menu_url=rec.menu_url,
        raw_payload=rec.raw_payload,
        ingest_run_id=run_id,
        parse_confidence=rec.parse_confidence,
    )


def run_platform_scrape(
    db: Session,
    platform: str,
    *,
    max_pages: int = 200,
    run_resolver: bool = True,
) -> dict[str, Any]:
    """
    Run a single platform scrape through the full pipeline.

    Steps:
    1. Create an ingest run record
    2. Run the scraper (legacy or enhanced)
    3. Parse each result into a DeliveryRecord
    4. Attempt location resolution
    5. Persist to delivery_source_record
    6. Optionally run entity resolver
    7. Finalize ingest run stats
    """
    from app.connectors.delivery_platforms import SCRAPER_REGISTRY

    if platform not in SCRAPER_REGISTRY:
        raise ValueError(f"Unknown platform: {platform}")

    # 1. Create ingest run
    run = DeliveryIngestRun(
        platform=platform,
        started_at=datetime.now(timezone.utc),
        status="running",
    )
    db.add(run)
    db.flush()  # get run.id
    run_id = run.id

    stats = {
        "rows_scraped": 0,
        "rows_parsed": 0,
        "rows_inserted": 0,
        "rows_skipped": 0,
        "errors": [],
    }

    # 2. Run scraper
    scraper_fn = SCRAPER_REGISTRY[platform]["fn"]
    try:
        for raw_dict in scraper_fn(max_pages=max_pages):
            stats["rows_scraped"] += 1

            # 3. Parse into DeliveryRecord
            try:
                record = parse_legacy_record(raw_dict, platform)
                stats["rows_parsed"] += 1
            except Exception as exc:
                stats["errors"].append(
                    {"phase": "parse", "error": str(exc)[:200]}
                )
                stats["rows_skipped"] += 1
                continue

            # 4. Location resolution
            record = resolve_location(record, db)

            # 5. Persist
            row = record_to_row(record, run_id)
            db.add(row)
            stats["rows_inserted"] += 1

            if stats["rows_inserted"] % 100 == 0:
                db.flush()

    except Exception as exc:
        logger.error("Scraper %s failed: %s", platform, exc)
        stats["errors"].append({"phase": "scrape", "error": str(exc)[:500]})

    # 6. Optionally run resolver
    matched = 0
    if run_resolver:
        try:
            from app.delivery.resolver import resolve_run
            matched = resolve_run(db, run_id)
        except Exception as exc:
            logger.warning("Resolver failed for run %d: %s", run_id, exc)
            stats["errors"].append({"phase": "resolve", "error": str(exc)[:200]})

    # 7. Finalize
    run.finished_at = datetime.now(timezone.utc)
    run.status = "completed" if not stats["errors"] else "completed_with_errors"
    run.rows_scraped = stats["rows_scraped"]
    run.rows_parsed = stats["rows_parsed"]
    run.rows_inserted = stats["rows_inserted"]
    run.rows_skipped = stats["rows_skipped"]
    run.rows_matched = matched
    run.error_summary = {"errors": stats["errors"]} if stats["errors"] else None

    db.commit()
    logger.info(
        "Platform %s ingest complete: scraped=%d parsed=%d inserted=%d matched=%d",
        platform,
        stats["rows_scraped"],
        stats["rows_parsed"],
        stats["rows_inserted"],
        matched,
    )
    return {
        "run_id": run_id,
        "platform": platform,
        **stats,
        "rows_matched": matched,
    }


def run_all_platforms(
    db: Session,
    *,
    max_pages: int = 200,
    platforms: list[str] | None = None,
    run_resolver: bool = True,
) -> list[dict[str, Any]]:
    """Run scrape pipeline for all (or specified) platforms."""
    from app.connectors.delivery_platforms import SCRAPER_REGISTRY

    results = []
    target_platforms = platforms or list(SCRAPER_REGISTRY.keys())

    for platform in target_platforms:
        try:
            result = run_platform_scrape(
                db, platform, max_pages=max_pages, run_resolver=run_resolver
            )
            results.append(result)
        except Exception as exc:
            logger.error("Pipeline failed for %s: %s", platform, exc)
            results.append({"platform": platform, "error": str(exc)})

    return results
