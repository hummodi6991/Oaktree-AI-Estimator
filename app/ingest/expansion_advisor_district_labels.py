"""Expansion Advisor — Pre-materialize district labels on ArcGIS parcels.

Runs a single bulk UPDATE that resolves each parcel's district from
external_feature polygons (aqar_district_hulls, rydpolygons) using the
same priority order as the expansion advisor candidate query.

This eliminates the expensive per-row correlated ST_Contains subquery at
search time.

Usage:
    python -m app.ingest.expansion_advisor_district_labels [--batch-size 5000] [--write-stats stats.json]
"""
from __future__ import annotations

import argparse
import logging
import time

from sqlalchemy import text

from app.ingest.expansion_advisor_common import (
    get_session,
    validate_db_env,
    write_stats,
)

logger = logging.getLogger("expansion_advisor.district_labels")

_DEFAULT_BATCH_SIZE = 5000


def _count_parcels_without_label(db) -> int:
    """Count parcels that need district labeling."""
    return int(
        db.execute(
            text("SELECT COUNT(*) FROM public.riyadh_parcels_arcgis_raw WHERE geom IS NOT NULL AND district_label IS NULL")
        ).scalar() or 0
    )


def _count_total_parcels(db) -> int:
    return int(
        db.execute(
            text("SELECT COUNT(*) FROM public.riyadh_parcels_arcgis_raw WHERE geom IS NOT NULL")
        ).scalar() or 0
    )


def populate_district_labels(db, batch_size: int = _DEFAULT_BATCH_SIZE) -> dict:
    """Batch-update district_label on riyadh_parcels_arcgis_raw.

    Uses LATERAL join against external_feature district polygons with the
    same layer priority as the expansion advisor candidate query:
    aqar_district_hulls (1) > rydpolygons (2).

    Only updates rows where district_label IS NULL, making this safe to
    re-run (idempotent on already-labeled parcels).
    """
    t_start = time.monotonic()
    total = _count_total_parcels(db)
    unlabeled = _count_parcels_without_label(db)
    logger.info("District label population: %d total parcels, %d unlabeled", total, unlabeled)

    if unlabeled == 0:
        logger.info("All parcels already have district labels — nothing to do")
        return {"total": total, "unlabeled_before": 0, "updated": 0, "elapsed_s": 0}

    # Batch update using ctid range for efficient pagination without an ORDER BY.
    # The LATERAL subquery resolves the district for each parcel using the
    # same logic as the expansion advisor candidate SQL.
    update_sql = text("""
        WITH batch AS (
            SELECT r.ctid AS row_ctid, r.fid, r.geom
            FROM public.riyadh_parcels_arcgis_raw r
            WHERE r.geom IS NOT NULL
              AND r.district_label IS NULL
            LIMIT :batch_size
        ),
        resolved AS (
            SELECT
                b.row_ctid,
                d.district_name
            FROM batch b
            LEFT JOIN LATERAL (
                SELECT
                    COALESCE(
                        NULLIF(ef.properties->>'district', ''),
                        NULLIF(ef.properties->>'district_raw', ''),
                        NULLIF(ef.properties->>'name', ''),
                        NULLIF(ef.properties->>'district_en', '')
                    ) AS district_name
                FROM external_feature ef
                WHERE ef.layer_name IN ('aqar_district_hulls', 'rydpolygons')
                  AND ef.geometry IS NOT NULL
                  AND ef.geometry ? 'type'
                  AND ef.geometry ? 'coordinates'
                  AND ef.geometry->>'type' IN ('Polygon', 'MultiPolygon')
                  AND ST_Contains(
                      ST_SetSRID(ST_GeomFromGeoJSON(ef.geometry::text), 4326),
                      ST_Centroid(b.geom)
                  )
                ORDER BY CASE ef.layer_name
                    WHEN 'aqar_district_hulls' THEN 1
                    WHEN 'rydpolygons' THEN 2
                    ELSE 3
                END
                LIMIT 1
            ) d ON TRUE
        )
        UPDATE public.riyadh_parcels_arcgis_raw r
        SET district_label = COALESCE(resolved.district_name, '__unresolved__')
        FROM resolved
        WHERE r.ctid = resolved.row_ctid
    """)

    total_updated = 0
    batch_num = 0
    while True:
        batch_num += 1
        result = db.execute(update_sql, {"batch_size": batch_size})
        rows_affected = result.rowcount
        db.commit()
        total_updated += rows_affected
        logger.info(
            "District label batch %d: updated %d rows (cumulative: %d / %d)",
            batch_num, rows_affected, total_updated, unlabeled,
        )
        if rows_affected < batch_size:
            break

    # Clear the sentinel value for parcels that couldn't be resolved
    unresolved_count = db.execute(
        text("SELECT COUNT(*) FROM public.riyadh_parcels_arcgis_raw WHERE district_label = '__unresolved__'")
    ).scalar() or 0
    if unresolved_count:
        db.execute(
            text("UPDATE public.riyadh_parcels_arcgis_raw SET district_label = NULL WHERE district_label = '__unresolved__'")
        )
        db.commit()
        logger.info("Cleared %d unresolved sentinel values back to NULL", unresolved_count)

    elapsed = time.monotonic() - t_start
    logger.info(
        "District label population complete: updated=%d unresolved=%d elapsed=%.1fs",
        total_updated, unresolved_count, elapsed,
    )
    return {
        "total": total,
        "unlabeled_before": unlabeled,
        "updated": total_updated,
        "unresolved": unresolved_count,
        "elapsed_s": round(elapsed, 1),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Pre-materialize district labels on ArcGIS parcels")
    parser.add_argument("--batch-size", type=int, default=_DEFAULT_BATCH_SIZE, help="Rows per UPDATE batch")
    parser.add_argument("--write-stats", type=str, default=None, help="Path to write JSON stats")
    parser.add_argument("--force", action="store_true", help="Re-label all parcels (clear existing labels first)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    validate_db_env()

    db = get_session()
    try:
        if args.force:
            logger.info("--force: clearing all existing district labels")
            db.execute(text("UPDATE public.riyadh_parcels_arcgis_raw SET district_label = NULL"))
            db.commit()

        stats = populate_district_labels(db, batch_size=args.batch_size)
        if args.write_stats:
            write_stats(args.write_stats, stats)
    finally:
        db.close()


if __name__ == "__main__":
    main()
