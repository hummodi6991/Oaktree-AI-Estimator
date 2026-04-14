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

from app.connectors.delivery_platforms import SCRAPER_REGISTRY
from app.ingest.expansion_advisor_common import (
    RIYADH_BBOX,
    get_session,
    log_table_counts,
    table_exists,
    validate_db_env,
    write_stats,
)
from app.services.restaurant_categories import normalize_category

logger = logging.getLogger("expansion_advisor.delivery")

# Legacy 5-platform preset kept for backwards compatibility.
CORE_PLATFORMS = ("hungerstation", "jahez", "keeta", "talabat", "mrsool")

# Named presets for --platforms argument.
PLATFORM_PRESETS: dict[str, tuple[str, ...]] = {
    "core": CORE_PLATFORMS,
    # "all" is resolved dynamically from SCRAPER_REGISTRY at call time so
    # newly-added connectors are picked up automatically.
}


def resolve_platforms(raw: str) -> list[str]:
    """Resolve a --platforms value into a sorted, validated list of names.

    Accepted forms:
      * ``"all"``   — every key in SCRAPER_REGISTRY (sorted).
      * ``"core"``  — the original 5 platforms.
      * comma-separated list — validated against SCRAPER_REGISTRY.

    Raises ``SystemExit`` with a clear message for unknown platform names.
    """
    token = raw.strip().lower()

    if token == "all":
        platforms = sorted(SCRAPER_REGISTRY.keys())
    elif token in PLATFORM_PRESETS:
        platforms = sorted(PLATFORM_PRESETS[token])
    else:
        platforms = sorted({p.strip().lower() for p in raw.split(",") if p.strip()})

    if not platforms:
        logger.error("No platforms resolved from --platforms=%r", raw)
        sys.exit(1)

    # Validate every name against the registry.
    unknown = sorted(set(platforms) - set(SCRAPER_REGISTRY.keys()))
    if unknown:
        supported = ", ".join(sorted(SCRAPER_REGISTRY.keys()))
        logger.error(
            "Unknown platform(s): %s. Supported platforms: %s",
            ", ".join(unknown),
            supported,
        )
        sys.exit(1)

    return platforms


# Default is now "all" — use every implemented connector.
DEFAULT_PLATFORMS = "all"


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

    # Post-insert: re-normalize category using the improved normalize_category()
    # so that rich Arabic cuisine_raw values resolve to specific categories
    # (burger, shawarma, dessert, etc.) instead of broad buckets.
    _renorm_rows = db.execute(
        text(
            "SELECT id, category FROM expansion_delivery_market "
            "WHERE city = 'riyadh' AND category IS NOT NULL"
        )
    ).fetchall()
    _renorm_count = 0
    for _row in _renorm_rows:
        _new_cat = normalize_category(_row[1])
        if _new_cat != _row[1]:
            db.execute(
                text("UPDATE expansion_delivery_market SET category = :cat WHERE id = :id"),
                {"cat": _new_cat, "id": _row[0]},
            )
            _renorm_count += 1
    if _renorm_count:
        db.commit()
        logger.info("Re-normalized %d/%d delivery market categories", _renorm_count, len(_renorm_rows))

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


def _snapshot_rating_counts(db, platforms: list[str]) -> dict:
    """Append a daily snapshot of rating_count per delivery_source_record.

    Feeds the ``expansion_delivery_rating_history`` table so the service
    layer can derive a realized-demand signal (Δrating_count over a trailing
    window) per category per hex.  Idempotent per UTC day via the
    ``ux_edrh_source_captured_date`` unique index.

    Safe no-op when the history table does not yet exist (migration not
    applied).  Only Riyadh-geolocated rows are captured.
    """
    if not table_exists(db, "expansion_delivery_rating_history"):
        msg = "expansion_delivery_rating_history table does not exist; skipping snapshot"
        logger.info(msg)
        return {"inserted": 0, "skipped_reason": msg}
    if not table_exists(db, "delivery_source_record"):
        return {"inserted": 0, "skipped_reason": "delivery_source_record missing"}

    bbox = RIYADH_BBOX
    platform_list = ", ".join(f"'{p}'" for p in platforms)
    insert_sql = text(f"""
        INSERT INTO expansion_delivery_rating_history (
            source_record_id, platform, brand_name,
            category_raw, cuisine_raw, rating, rating_count,
            lat, lon, geom
        )
        SELECT
            dsr.id,
            lower(dsr.platform),
            COALESCE(dsr.brand_raw, dsr.restaurant_name_normalized, dsr.restaurant_name_raw),
            dsr.category_raw,
            dsr.cuisine_raw,
            dsr.rating,
            dsr.rating_count,
            CAST(BTRIM(CAST(dsr.lat AS text)) AS double precision),
            CAST(BTRIM(CAST(dsr.lon AS text)) AS double precision),
            ST_SetSRID(ST_MakePoint(
                CAST(BTRIM(CAST(dsr.lon AS text)) AS double precision),
                CAST(BTRIM(CAST(dsr.lat AS text)) AS double precision)
            ), 4326)
        FROM delivery_source_record dsr
        WHERE dsr.rating_count IS NOT NULL
          AND dsr.lat IS NOT NULL
          AND dsr.lon IS NOT NULL
          AND BTRIM(CAST(dsr.lon AS text)) ~ '^[-+]?[0-9]*\\.?[0-9]+$'
          AND BTRIM(CAST(dsr.lat AS text)) ~ '^[-+]?[0-9]*\\.?[0-9]+$'
          AND CAST(BTRIM(CAST(dsr.lon AS text)) AS double precision)
              BETWEEN {bbox['min_lon']} AND {bbox['max_lon']}
          AND CAST(BTRIM(CAST(dsr.lat AS text)) AS double precision)
              BETWEEN {bbox['min_lat']} AND {bbox['max_lat']}
          AND lower(dsr.platform) IN ({platform_list})
        ON CONFLICT (source_record_id, captured_date) DO NOTHING
    """)

    result = db.execute(insert_sql)
    db.commit()
    inserted = result.rowcount or 0
    logger.info("Appended %d delivery rating-count history rows", inserted)
    return {"inserted": inserted}


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
                        help="Platform list: 'all', 'core', or comma-separated names "
                             f"(default: {DEFAULT_PLATFORMS})")
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

    platforms = resolve_platforms(args.platforms)
    logger.info("Resolved platforms (%d): %s", len(platforms), ", ".join(platforms))

    # Step 1: Scrape fresh delivery data (unless --skip-scrape)
    scrape_results = []
    if not args.skip_scrape:
        scrape_results = _run_delivery_scrape(platforms, args.max_pages)

    # Step 2: Normalize into expansion_delivery_market
    db = get_session()
    try:
        stats = _normalize_delivery_records(db, platforms, args.allow_empty)
        stats["resolved_platforms"] = platforms
        stats["platform_preset"] = args.platforms
        # Append a daily rating_count snapshot for the realized-demand signal.
        # Safe when the history table does not yet exist.
        try:
            stats["rating_history_snapshot"] = _snapshot_rating_counts(db, platforms)
        except Exception:
            logger.warning("rating-count history snapshot failed", exc_info=True)
        stats["scrape_results"] = [
            {k: v for k, v in r.items() if k != "raw_results"}
            for r in scrape_results
        ]

        # Attach per-platform discovery stats when available
        try:
            from app.connectors.delivery_platforms import get_discovery_stats
            discovery = get_discovery_stats()
            if discovery:
                stats["discovery_stats"] = discovery
                for plat, ds in discovery.items():
                    logger.info(
                        "Discovery stats [%s]: path=%s, sitemap_urls=%d, "
                        "candidates=%d, fetch_fail=%d, parse_fail=%d",
                        plat,
                        ds.get("discovery_success_path"),
                        ds.get("sitemap_urls_found", 0),
                        ds.get("candidate_urls_found", 0),
                        ds.get("fetch_failures", 0),
                        ds.get("parse_failures", 0),
                    )
        except ImportError:
            pass

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
