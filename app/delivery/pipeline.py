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

Production-robustness:
- Each platform runs in its own DB session (fresh SessionLocal)
- Scraper/resolver failures trigger immediate rollback
- One platform failure never poisons the next
- Ingest run status is always finalized, even on failure
"""

from __future__ import annotations

import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Iterator

from sqlalchemy import text as sa_text
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


def _safe_rollback(db: Session) -> None:
    """Rollback the session, ignoring errors if the connection is dead."""
    try:
        db.rollback()
    except Exception:
        pass


def _safe_finalize_run(
    db: Session,
    run_id: int,
    stats: dict[str, Any],
    matched: int,
    error: str | None,
) -> None:
    """Persist ingest_run status even after failures.

    Uses a fresh session to avoid poisoned-session issues.  If the original
    session is still healthy we use it; otherwise we open a new one.
    """
    status = "failed" if error else (
        "completed_with_errors" if stats.get("errors") else "completed"
    )
    error_summary = None
    if error:
        error_summary = {"fatal": error[:2000]}
    elif stats.get("errors"):
        error_summary = {"errors": stats["errors"]}

    def _do_finalize(session: Session) -> None:
        run = session.get(DeliveryIngestRun, run_id)
        if not run:
            return
        run.finished_at = datetime.now(timezone.utc)
        run.status = status
        run.rows_scraped = stats.get("rows_scraped", 0)
        run.rows_parsed = stats.get("rows_parsed", 0)
        run.rows_inserted = stats.get("rows_inserted", 0)
        run.rows_updated = stats.get("rows_updated", 0)
        run.rows_skipped = stats.get("rows_skipped", 0)
        run.rows_matched = matched
        run.error_summary = error_summary
        session.commit()

    # Try on the existing session first
    try:
        _do_finalize(db)
        return
    except Exception:
        _safe_rollback(db)

    # Fallback: open a brand-new session
    try:
        from app.db.session import SessionLocal
        fallback = SessionLocal()
        try:
            _do_finalize(fallback)
        finally:
            fallback.close()
    except Exception as exc:
        logger.error(
            "Could not finalize ingest run %d even with fallback session: %s",
            run_id, exc,
        )


def run_platform_scrape(
    db: Session,
    platform: str,
    *,
    max_pages: int = 5000,
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

    On any unrecoverable error the session is rolled back and the ingest
    run is marked ``failed`` with the root exception recorded.
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

    stats: dict[str, Any] = {
        "rows_scraped": 0,
        "rows_parsed": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_skipped": 0,
        "rows_html_extracted": 0,
        "rows_with_coords": 0,
        "rows_with_name": 0,
        "errors": [],
        "rejection_reasons": {},
        "sample_rejected_urls": [],
    }
    matched = 0
    fatal_error: str | None = None

    # 2. Run scraper
    scraper_fn = SCRAPER_REGISTRY[platform]["fn"]
    try:
        for raw_dict in scraper_fn(max_pages=max_pages):
            stats["rows_scraped"] += 1

            # Track HTML extraction success
            if raw_dict.get("_html_extracted"):
                stats["rows_html_extracted"] += 1

            # 3. Parse into DeliveryRecord
            try:
                record = parse_legacy_record(raw_dict, platform)
                stats["rows_parsed"] += 1
            except Exception as exc:
                reason = f"parse_error: {str(exc)[:100]}"
                stats["errors"].append(
                    {"phase": "parse", "error": str(exc)[:200]}
                )
                stats["rows_skipped"] += 1
                stats["rejection_reasons"][reason] = (
                    stats["rejection_reasons"].get(reason, 0) + 1
                )
                if len(stats["sample_rejected_urls"]) < 10:
                    stats["sample_rejected_urls"].append({
                        "url": raw_dict.get("source_url", "?"),
                        "reason": reason,
                    })
                continue

            # Track data quality
            if record.restaurant_name_raw:
                stats["rows_with_name"] += 1

            # 4. Location resolution
            record = resolve_location(record, db)

            if record.lat is not None and record.lon is not None:
                stats["rows_with_coords"] += 1

            # 5. Persist (upsert: update if same platform+listing exists)
            # Relaxed persistence: always store, even if fields are sparse.
            # Low-confidence records are stored with parse_confidence reflecting
            # their quality, instead of being dropped.
            row = record_to_row(record, run_id)
            if row.source_listing_id:
                existing = (
                    db.query(DeliverySourceRecord)
                    .filter_by(
                        platform=row.platform,
                        source_listing_id=row.source_listing_id,
                    )
                    .first()
                )
                if existing:
                    # Update existing record with fresh data
                    existing.ingest_run_id = run_id
                    existing.scraped_at = row.scraped_at
                    existing.lat = row.lat
                    existing.lon = row.lon
                    existing.geocode_method = row.geocode_method
                    existing.location_confidence = row.location_confidence
                    existing.restaurant_name_raw = row.restaurant_name_raw
                    existing.restaurant_name_normalized = row.restaurant_name_normalized
                    existing.brand_raw = row.brand_raw
                    existing.branch_raw = row.branch_raw
                    existing.cuisine_raw = row.cuisine_raw
                    existing.category_raw = row.category_raw
                    existing.rating = row.rating
                    existing.rating_count = row.rating_count
                    existing.district_text = row.district_text
                    existing.address_raw = row.address_raw
                    existing.phone_raw = row.phone_raw
                    existing.parse_confidence = row.parse_confidence
                    existing.entity_resolution_status = "pending"
                    stats["rows_updated"] += 1
                else:
                    db.add(row)
                    stats["rows_inserted"] += 1
            else:
                db.add(row)
                stats["rows_inserted"] += 1

            total_rows = stats["rows_inserted"] + stats["rows_updated"]
            if total_rows % 100 == 0:
                db.flush()

    except Exception as exc:
        fatal_error = f"scraper error: {exc}"
        logger.error(
            "Platform %s scraper failed (root cause): %s\n%s",
            platform, exc, traceback.format_exc(),
        )
        stats["errors"].append({"phase": "scrape", "error": str(exc)[:500]})
        # Rollback to clear the broken transaction
        _safe_rollback(db)

    # 6. Optionally run resolver (only if scraper succeeded)
    if run_resolver and not fatal_error:
        try:
            from app.delivery.resolver import resolve_run
            matched = resolve_run(db, run_id)
        except Exception as exc:
            logger.warning("Resolver failed for run %d: %s", run_id, exc)
            stats["errors"].append({"phase": "resolve", "error": str(exc)[:200]})
            _safe_rollback(db)

    # 7. Finalize — always write run status, even on failure
    _safe_finalize_run(db, run_id, stats, matched, fatal_error)

    _log_platform_summary(platform, run_id, stats, matched, fatal_error)

    return {
        "run_id": run_id,
        "platform": platform,
        **stats,
        "rows_matched": matched,
    }


def _log_platform_summary(
    platform: str,
    run_id: int,
    stats: dict[str, Any],
    matched: int,
    fatal_error: str | None,
) -> None:
    """Emit a structured per-platform summary with diagnostics."""
    status = "FAILED" if fatal_error else (
        "ERRORS" if stats.get("errors") else "OK"
    )
    logger.info(
        "=== Platform %-16s | run_id=%-5d | status=%-7s | "
        "scraped=%-5d parsed=%-5d inserted=%-5d updated=%-5d "
        "skipped=%-5d matched=%-5d | "
        "html_extracted=%-5d with_coords=%-5d with_name=%-5d ===",
        platform,
        run_id,
        status,
        stats.get("rows_scraped", 0),
        stats.get("rows_parsed", 0),
        stats.get("rows_inserted", 0),
        stats.get("rows_updated", 0),
        stats.get("rows_skipped", 0),
        matched,
        stats.get("rows_html_extracted", 0),
        stats.get("rows_with_coords", 0),
        stats.get("rows_with_name", 0),
    )
    if fatal_error:
        logger.error("  Root error for %s: %s", platform, fatal_error)
    if stats.get("rejection_reasons"):
        logger.info(
            "  Rejection reasons for %s: %s", platform, stats["rejection_reasons"],
        )
    if stats.get("sample_rejected_urls"):
        logger.info(
            "  Sample rejected URLs for %s: %s",
            platform,
            stats["sample_rejected_urls"][:5],
        )


def run_all_platforms(
    db: Session | None = None,
    *,
    max_pages: int = 5000,
    platforms: list[str] | None = None,
    run_resolver: bool = True,
) -> list[dict[str, Any]]:
    """Run scrape pipeline for all (or specified) platforms.

    Each platform gets its own DB session so that a failure (including
    connection-level errors like AdminShutdown) in one platform cannot
    leave a poisoned session for the next platform.

    The ``db`` parameter is accepted for API compatibility but is not
    used — each platform opens its own fresh session.
    """
    from app.connectors.delivery_platforms import SCRAPER_REGISTRY
    from app.db.session import SessionLocal

    results: list[dict[str, Any]] = []
    target_platforms = platforms or list(SCRAPER_REGISTRY.keys())

    logger.info(
        "Delivery pipeline starting for %d platform(s): %s",
        len(target_platforms),
        ", ".join(target_platforms),
    )

    for platform in target_platforms:
        # Fresh session per platform — isolation guarantee
        platform_db = SessionLocal()
        try:
            result = run_platform_scrape(
                platform_db, platform, max_pages=max_pages, run_resolver=run_resolver
            )
            results.append(result)
        except Exception as exc:
            logger.error("Pipeline failed for %s: %s", platform, exc)
            _safe_rollback(platform_db)
            results.append({"platform": platform, "error": str(exc)})
        finally:
            platform_db.close()

    # Final summary
    ok = sum(1 for r in results if "error" not in r and not r.get("errors"))
    failed = len(results) - ok
    total_inserted = sum(r.get("rows_inserted", 0) for r in results)
    total_matched = sum(r.get("rows_matched", 0) for r in results)
    logger.info(
        "=== Delivery pipeline complete: %d/%d platforms OK, "
        "%d total inserted, %d total matched ===",
        ok, len(results), total_inserted, total_matched,
    )

    return results
