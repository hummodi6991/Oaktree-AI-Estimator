"""
Data quality observability and metrics for delivery data.

Provides visibility into data quality across platforms:
- Rows per platform
- % with exact coords / district only / no location
- % matched to restaurant_poi
- % with category, rating, delivery time
- Top parsing failures
- Freshness by platform
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def delivery_data_quality_report(db: Session) -> dict[str, Any]:
    """
    Generate a comprehensive data quality report for delivery records.
    """
    report: dict[str, Any] = {}

    # Per-platform stats
    try:
        rows = db.execute(
            text("""
                SELECT
                    platform,
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE lat IS NOT NULL AND lon IS NOT NULL
                                     AND location_confidence >= 0.7) as exact_coords,
                    COUNT(*) FILTER (WHERE district_text IS NOT NULL
                                     AND location_confidence < 0.7) as district_only,
                    COUNT(*) FILTER (WHERE lat IS NULL AND district_text IS NULL) as no_location,
                    COUNT(*) FILTER (WHERE entity_resolution_status = 'matched') as matched,
                    COUNT(*) FILTER (WHERE entity_resolution_status = 'unmatched') as unmatched,
                    COUNT(*) FILTER (WHERE entity_resolution_status = 'pending') as pending,
                    COUNT(*) FILTER (WHERE category_raw IS NOT NULL) as has_category,
                    COUNT(*) FILTER (WHERE rating IS NOT NULL) as has_rating,
                    COUNT(*) FILTER (WHERE delivery_time_min IS NOT NULL) as has_delivery_time,
                    COUNT(*) FILTER (WHERE delivery_fee IS NOT NULL) as has_delivery_fee,
                    COUNT(*) FILTER (WHERE brand_raw IS NOT NULL) as has_brand,
                    MAX(scraped_at) as last_scraped,
                    AVG(parse_confidence) as avg_parse_confidence
                FROM delivery_source_record
                GROUP BY platform
                ORDER BY total DESC
            """),
        ).fetchall()

        platforms = []
        for r in rows:
            total = r[1] or 1
            platforms.append({
                "platform": r[0],
                "total": r[1],
                "pct_exact_coords": round(100 * r[2] / total, 1),
                "pct_district_only": round(100 * r[3] / total, 1),
                "pct_no_location": round(100 * r[4] / total, 1),
                "pct_matched": round(100 * r[5] / total, 1),
                "pct_unmatched": round(100 * r[6] / total, 1),
                "pct_pending": round(100 * r[7] / total, 1),
                "pct_has_category": round(100 * r[8] / total, 1),
                "pct_has_rating": round(100 * r[9] / total, 1),
                "pct_has_delivery_time": round(100 * r[10] / total, 1),
                "pct_has_delivery_fee": round(100 * r[11] / total, 1),
                "pct_has_brand": round(100 * r[12] / total, 1),
                "last_scraped": str(r[13]) if r[13] else None,
                "avg_parse_confidence": round(float(r[14]), 3) if r[14] else 0,
            })

        report["platforms"] = platforms
    except Exception as exc:
        logger.warning("Platform stats query failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        report["platforms"] = []

    # Global totals
    try:
        totals = db.execute(
            text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(DISTINCT platform) as platform_count,
                    COUNT(DISTINCT restaurant_name_normalized) as unique_names,
                    COUNT(DISTINCT district_text)
                        FILTER (WHERE district_text IS NOT NULL) as districts_covered,
                    COUNT(*) FILTER (WHERE entity_resolution_status = 'matched') as total_matched,
                    COUNT(*) FILTER (WHERE entity_resolution_status = 'unmatched') as total_unmatched,
                    COUNT(*) FILTER (WHERE entity_resolution_status = 'pending') as total_pending
                FROM delivery_source_record
            """),
        ).first()

        if totals:
            report["totals"] = {
                "total_records": totals[0],
                "platform_count": totals[1],
                "unique_restaurant_names": totals[2],
                "districts_covered": totals[3],
                "total_matched": totals[4],
                "total_unmatched": totals[5],
                "total_pending": totals[6],
            }
    except Exception as exc:
        logger.warning("Totals query failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        report["totals"] = {}

    # Top duplicate names
    try:
        dupes = db.execute(
            text("""
                SELECT restaurant_name_normalized, COUNT(*) as cnt,
                       COUNT(DISTINCT platform) as platforms
                FROM delivery_source_record
                WHERE restaurant_name_normalized IS NOT NULL
                GROUP BY restaurant_name_normalized
                HAVING COUNT(*) > 3
                ORDER BY cnt DESC
                LIMIT 20
            """),
        ).fetchall()

        report["top_duplicates"] = [
            {"name": r[0], "count": r[1], "platforms": r[2]}
            for r in dupes
        ]
    except Exception as exc:
        logger.debug("Duplicates query failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        report["top_duplicates"] = []

    # Recent ingest runs
    try:
        runs = db.execute(
            text("""
                SELECT id, platform, started_at, finished_at, status,
                       rows_scraped, rows_parsed, rows_inserted,
                       rows_skipped, rows_matched
                FROM delivery_ingest_run
                ORDER BY started_at DESC
                LIMIT 20
            """),
        ).fetchall()

        report["recent_runs"] = [
            {
                "run_id": r[0],
                "platform": r[1],
                "started_at": str(r[2]) if r[2] else None,
                "finished_at": str(r[3]) if r[3] else None,
                "status": r[4],
                "rows_scraped": r[5],
                "rows_parsed": r[6],
                "rows_inserted": r[7],
                "rows_skipped": r[8],
                "rows_matched": r[9],
            }
            for r in runs
        ]
    except Exception as exc:
        logger.debug("Ingest runs query failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        report["recent_runs"] = []

    return report
