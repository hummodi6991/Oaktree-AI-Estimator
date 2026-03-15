"""Expansion Advisor — Parking Context Ingestion.

Sources parking amenities from OSM tables (planet_osm_polygon, planet_osm_point)
and normalizes into expansion_parking_asset for the Expansion Advisor service.

Heuristics:
- walk_access_score: derived from proximity to roads/pedestrian paths (0-100)
- dropoff_score: derived from parking type and road adjacency (0-100)
These are reasonable approximations, not survey measurements.
"""
from __future__ import annotations

import argparse
import logging
import sys

from sqlalchemy import text

from app.ingest.expansion_advisor_common import (
    RIYADH_BBOX,
    detect_srid,
    get_session,
    log_table_counts,
    riyadh_bbox_filter_sql,
    table_exists,
    validate_db_env,
    write_stats,
)

logger = logging.getLogger("expansion_advisor.parking")


def _ingest_from_polygons(db, replace: bool) -> int:
    """Extract parking from planet_osm_polygon."""
    if not table_exists(db, "planet_osm_polygon"):
        logger.warning("planet_osm_polygon not found, skipping polygon source")
        return 0

    if replace:
        db.execute(text("DELETE FROM expansion_parking_asset WHERE city = 'riyadh' AND source = 'osm_polygon'"))
        db.commit()

    # Detect source SRID: osm2pgsql with --latlong stores in 4326, without in 3857
    source_srid = detect_srid(db, "planet_osm_polygon", "way")
    logger.info("Detected SRID %d for planet_osm_polygon.way", source_srid)
    bbox_filter = riyadh_bbox_filter_sql("way", alias="op", source_srid=source_srid)

    insert_sql = text(f"""
        INSERT INTO expansion_parking_asset (
            city, source, name, amenity_type, geom, capacity, covered,
            public_access, walk_access_score, dropoff_score
        )
        SELECT
            'riyadh',
            'osm_polygon',
            COALESCE(NULLIF(op.name, ''), 'Unnamed parking'),
            CASE
                WHEN lower(COALESCE(op.parking, '')) = 'multi-storey' THEN 'multi_storey'
                WHEN lower(COALESCE(op.parking, '')) = 'underground' THEN 'underground'
                WHEN lower(COALESCE(op.parking, '')) = 'surface' THEN 'surface'
                WHEN lower(COALESCE(op.parking, '')) IN ('street_side','lane') THEN 'street_side'
                WHEN lower(COALESCE(op.amenity, '')) = 'parking' THEN 'surface'
                ELSE 'unknown'
            END,
            ST_Centroid(ST_Transform(op.way, 4326)),
            -- capacity: parse from OSM tag if present (heuristic)
            CASE
                WHEN op.capacity ~ '^[0-9]+$' THEN CAST(op.capacity AS INTEGER)
                ELSE NULL
            END,
            -- covered heuristic
            CASE
                WHEN lower(COALESCE(op.parking, '')) IN ('multi-storey','underground') THEN TRUE
                WHEN lower(COALESCE(op.covered, '')) = 'yes' THEN TRUE
                ELSE FALSE
            END,
            -- public_access heuristic
            CASE
                WHEN lower(COALESCE(op.access, '')) IN ('private','no','permit') THEN FALSE
                ELSE TRUE
            END,
            -- walk_access_score heuristic (0-100): surface parking scores higher
            CASE
                WHEN lower(COALESCE(op.parking, '')) = 'surface' THEN 75.0
                WHEN lower(COALESCE(op.parking, '')) = 'multi-storey' THEN 60.0
                WHEN lower(COALESCE(op.parking, '')) = 'underground' THEN 50.0
                WHEN lower(COALESCE(op.parking, '')) IN ('street_side','lane') THEN 80.0
                ELSE 65.0
            END,
            -- dropoff_score heuristic (0-100): street-level better for quick stops
            CASE
                WHEN lower(COALESCE(op.parking, '')) IN ('street_side','lane') THEN 85.0
                WHEN lower(COALESCE(op.parking, '')) = 'surface' THEN 70.0
                WHEN lower(COALESCE(op.parking, '')) = 'multi-storey' THEN 45.0
                WHEN lower(COALESCE(op.parking, '')) = 'underground' THEN 35.0
                ELSE 55.0
            END
        FROM planet_osm_polygon op
        WHERE op.way IS NOT NULL
          AND (
            lower(COALESCE(op.amenity, '')) = 'parking'
            OR lower(COALESCE(op.parking, '')) IN ('surface','multi-storey','underground','street_side','lane')
          )
          AND {bbox_filter}
    """)

    result = db.execute(insert_sql)
    db.commit()
    count = result.rowcount
    logger.info("Inserted %d parking assets from planet_osm_polygon", count)
    return count


def _ingest_from_points(db, replace: bool) -> int:
    """Extract parking from planet_osm_point."""
    if not table_exists(db, "planet_osm_point"):
        logger.warning("planet_osm_point not found, skipping point source")
        return 0

    if replace:
        db.execute(text("DELETE FROM expansion_parking_asset WHERE city = 'riyadh' AND source = 'osm_point'"))
        db.commit()

    # Detect source SRID for point table
    source_srid = detect_srid(db, "planet_osm_point", "way")
    logger.info("Detected SRID %d for planet_osm_point.way", source_srid)
    bbox_filter = riyadh_bbox_filter_sql("way", alias="pt", source_srid=source_srid)

    insert_sql = text(f"""
        INSERT INTO expansion_parking_asset (
            city, source, name, amenity_type, geom, capacity, covered,
            public_access, walk_access_score, dropoff_score
        )
        SELECT
            'riyadh',
            'osm_point',
            COALESCE(NULLIF(pt.name, ''), 'Unnamed parking point'),
            CASE
                WHEN lower(COALESCE(pt.parking, '')) = 'multi-storey' THEN 'multi_storey'
                WHEN lower(COALESCE(pt.parking, '')) = 'underground' THEN 'underground'
                WHEN lower(COALESCE(pt.parking, '')) IN ('street_side','lane') THEN 'street_side'
                WHEN lower(COALESCE(pt.amenity, '')) = 'parking_entrance' THEN 'entrance'
                ELSE 'surface'
            END,
            ST_Transform(pt.way, 4326),
            CASE
                WHEN pt.capacity ~ '^[0-9]+$' THEN CAST(pt.capacity AS INTEGER)
                ELSE NULL
            END,
            lower(COALESCE(pt.parking, '')) IN ('multi-storey','underground')
                OR lower(COALESCE(pt.covered, '')) = 'yes',
            CASE
                WHEN lower(COALESCE(pt.access, '')) IN ('private','no','permit') THEN FALSE
                ELSE TRUE
            END,
            65.0,  -- walk_access_score: default for point features
            55.0   -- dropoff_score: default for point features
        FROM planet_osm_point pt
        WHERE pt.way IS NOT NULL
          AND (
            lower(COALESCE(pt.amenity, '')) IN ('parking','parking_entrance','parking_space')
            OR lower(COALESCE(pt.parking, '')) IN ('surface','multi-storey','underground','street_side','lane')
          )
          AND {bbox_filter}
    """)

    result = db.execute(insert_sql)
    db.commit()
    count = result.rowcount
    logger.info("Inserted %d parking assets from planet_osm_point", count)
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Expansion Advisor — Parking Context ingest")
    parser.add_argument("--city", default="riyadh", help="City filter (default: riyadh)")
    parser.add_argument("--replace", type=lambda v: v.lower() in ("true", "1", "yes"), default=True,
                        help="Replace existing rows (default: true)")
    parser.add_argument("--write-stats", type=str, default=None, help="Write JSON stats to path")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    validate_db_env()

    db = get_session()
    try:
        polygon_count = _ingest_from_polygons(db, replace=args.replace)
        point_count = _ingest_from_points(db, replace=args.replace)

        total = polygon_count + point_count
        logger.info("Total parking assets inserted: %d", total)

        counts = log_table_counts(db, ["expansion_parking_asset"])
        stats = {
            "polygon_count": polygon_count,
            "point_count": point_count,
            "total_inserted": total,
            "row_counts": counts,
        }

        if args.write_stats:
            write_stats(args.write_stats, stats)
    finally:
        db.close()


if __name__ == "__main__":
    main()
