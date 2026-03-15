"""Expansion Advisor — Delivery Marketplace Ingestion.

End-to-end pipeline: scrapes fresh delivery data from configured platforms,
runs entity resolution, then normalizes resolved records into
expansion_delivery_market for the Expansion Advisor service.

Only Riyadh rows are kept in the normalized output.
Fails loudly if requested platforms produce zero useful rows
(unless ALLOW_EMPTY_DELIVERY_INGEST=true).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from sqlalchemy import text

from app.ingest.expansion_advisor_common import (
    RIYADH_BBOX,
    get_session,
    log_table_counts,
    table_exists,
    validate_db_env,
    write_stats,
)

logger = logging.getLogger("expansion_advisor.delivery")

DEFAULT_PLATFORMS = "hungerstation,jahez,keeta,talabat,mrsool"


def _normalize_delivery_records(db, platforms: list[str], allow_empty: bool) -> dict:
    """Copy/normalize delivery_source_record rows into expansion_delivery_market.

    Only keeps Riyadh-geolocated rows with valid coordinates.
    """
    if not table_exists(db, "delivery_source_record"):
        msg = "delivery_source_record table does not exist"
        if allow_empty:
            logger.warning(msg)
            return {"inserted": 0, "platforms": platforms, "skipped_reason": msg}
        logger.error(msg)
        sys.exit(1)

    # Clear existing rows for requested platforms
    for platform in platforms:
        db.execute(
            text("DELETE FROM expansion_delivery_market WHERE city = 'riyadh' AND platform = :p"),
            {"p": platform},
        )
    db.commit()

    bbox = RIYADH_BBOX
    platform_list = ", ".join(f"'{p}'" for p in platforms)

    insert_sql = text(f"""
        INSERT INTO expansion_delivery_market (
            city, platform, branch_name, brand_name, category, geom,
            district, rating, rating_count, min_order_sar, delivery_fee_sar,
            eta_minutes, is_open_now, supports_late_night,
            source_record_id, resolved_restaurant_poi_id, scraped_at
        )
        SELECT
            'riyadh',
            dsr.platform,
            COALESCE(dsr.branch_raw, dsr.restaurant_name_raw),
            COALESCE(dsr.brand_raw, dsr.restaurant_name_normalized, dsr.restaurant_name_raw),
            COALESCE(dsr.category_raw, dsr.cuisine_raw, 'unknown'),
            ST_SetSRID(ST_MakePoint(
                CAST(BTRIM(CAST(dsr.lon AS text)) AS double precision),
                CAST(BTRIM(CAST(dsr.lat AS text)) AS double precision)
            ), 4326),
            dsr.district_text,
            dsr.rating,
            dsr.rating_count,
            dsr.minimum_order,
            dsr.delivery_fee,
            dsr.delivery_time_min,
            dsr.is_open_now_raw,
            -- supports_late_night: heuristic from availability text (approximation)
            CASE
                WHEN lower(COALESCE(dsr.availability_text, '')) LIKE '%late%' THEN TRUE
                WHEN lower(COALESCE(dsr.availability_text, '')) LIKE '%24%' THEN TRUE
                WHEN lower(COALESCE(dsr.availability_text, '')) LIKE '%night%' THEN TRUE
                ELSE NULL
            END,
            dsr.id,
            dsr.matched_restaurant_poi_id,
            dsr.scraped_at
        FROM delivery_source_record dsr
        WHERE dsr.lat IS NOT NULL
          AND dsr.lon IS NOT NULL
          AND BTRIM(CAST(dsr.lon AS text)) ~ '^[-+]?[0-9]*\\.?[0-9]+$'
          AND BTRIM(CAST(dsr.lat AS text)) ~ '^[-+]?[0-9]*\\.?[0-9]+$'
          AND CAST(BTRIM(CAST(dsr.lon AS text)) AS double precision)
              BETWEEN {bbox['min_lon']} AND {bbox['max_lon']}
          AND CAST(BTRIM(CAST(dsr.lat AS text)) AS double precision)
              BETWEEN {bbox['min_lat']} AND {bbox['max_lat']}
          AND lower(dsr.platform) IN ({platform_list})
    """)

    result = db.execute(insert_sql)
    db.commit()
    inserted = result.rowcount
    logger.info("Inserted %d delivery market records", inserted)

    # Per-platform counts
    platform_stats = {}
    for platform in platforms:
        row = db.execute(
            text("SELECT COUNT(*) FROM expansion_delivery_market WHERE platform = :p AND city = 'riyadh'"),
            {"p": platform},
        ).scalar()
        platform_stats[platform] = int(row or 0)
        logger.info("  %s: %d rows", platform, platform_stats[platform])

    # Fail loudly if all platforms produced zero rows
    if inserted == 0 and not allow_empty:
        logger.error(
            "All requested platforms produced zero useful rows. "
            "Set ALLOW_EMPTY_DELIVERY_INGEST=true or --allow-empty to override."
        )
        sys.exit(1)

    return {
        "inserted": inserted,
        "platforms": platforms,
        "platform_counts": platform_stats,
    }


def _run_delivery_scrape(platforms: list[str], max_pages: int) -> list[dict]:
    """Run the delivery scrape pipeline to populate delivery_source_record.

    Uses the existing app.delivery.pipeline.run_all_platforms() which handles
    per-platform session isolation, scraping, parsing, and entity resolution.
    """
    from app.delivery.pipeline import run_all_platforms

    logger.info("Running delivery scrape for platforms: %s (max_pages=%d)", platforms, max_pages)
    results = run_all_platforms(
        db=None,
        max_pages=max_pages,
        platforms=platforms,
        run_resolver=True,
    )

    total_inserted = sum(r.get("rows_inserted", 0) for r in results)
    total_matched = sum(r.get("rows_matched", 0) for r in results)
    errors = [r["platform"] for r in results if "error" in r]

    logger.info(
        "Delivery scrape complete: %d inserted, %d matched, %d errors",
        total_inserted, total_matched, len(errors),
    )
    if errors:
        logger.warning("Platforms with errors: %s", errors)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Expansion Advisor — Delivery Marketplace ingest")
    parser.add_argument("--city", default="riyadh", help="City filter (default: riyadh)")
    parser.add_argument("--platforms", default=DEFAULT_PLATFORMS,
                        help=f"Comma-separated platform list (default: {DEFAULT_PLATFORMS})")
    parser.add_argument("--allow-empty", action="store_true",
                        default=os.getenv("ALLOW_EMPTY_DELIVERY_INGEST", "").lower() in ("true", "1", "yes"),
                        help="Allow zero rows without failing")
    parser.add_argument("--skip-scrape", action="store_true",
                        help="Skip scraping; only normalize existing delivery_source_record rows")
    parser.add_argument("--max-pages", type=int, default=200,
                        help="Max pages per platform during scrape (default: 200)")
    parser.add_argument("--write-stats", type=str, default=None, help="Write JSON stats to path")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    validate_db_env()

    platforms = [p.strip().lower() for p in args.platforms.split(",") if p.strip()]

    # Step 1: Scrape fresh delivery data (unless --skip-scrape)
    scrape_results = []
    if not args.skip_scrape:
        scrape_results = _run_delivery_scrape(platforms, args.max_pages)

    # Step 2: Normalize into expansion_delivery_market
    db = get_session()
    try:
        stats = _normalize_delivery_records(db, platforms, args.allow_empty)
        stats["scrape_results"] = [
            {k: v for k, v in r.items() if k != "raw_results"}
            for r in scrape_results
        ]
        counts = log_table_counts(db, ["expansion_delivery_market"])
        stats["row_counts"] = counts

        if args.write_stats:
            write_stats(args.write_stats, stats)

        # Print structured JSON stats to stdout for workflow consumption
        print(json.dumps(stats, indent=2, default=str))
    finally:
        db.close()


if __name__ == "__main__":
    main()
