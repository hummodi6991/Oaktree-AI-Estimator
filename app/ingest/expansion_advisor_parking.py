"""Expansion Advisor — Parking Context Ingestion.

Sources parking amenities from OSM tables (planet_osm_polygon, planet_osm_point)
and normalizes into expansion_parking_asset for the Expansion Advisor service.

Heuristics:
- walk_access_score: derived from proximity to roads/pedestrian paths (0-100)
- dropoff_score: derived from parking type and road adjacency (0-100)
These are reasonable approximations, not survey measurements.

Schema resilience:
- Introspects actual columns before building SQL.
- When the `parking` column is absent, falls back to amenity='parking' detection
  and extracts parking subtypes from tags/other_tags (hstore) when available.
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
    get_table_columns,
    log_table_counts,
    riyadh_bbox_filter_sql,
    table_exists,
    validate_db_env,
    write_stats,
)

logger = logging.getLogger("expansion_advisor.parking")


# ---------------------------------------------------------------------------
# SQL fragment builders (schema-aware)
# ---------------------------------------------------------------------------

def _parking_expr(alias: str, cols: set[str]) -> str:
    """Return a SQL expression that resolves the parking tag value.

    Priority:
      1. Direct ``parking`` column (flattened osm2pgsql schema).
      2. hstore ``tags`` column: ``tags->'parking'``.
      3. hstore ``other_tags`` column: ``other_tags->'parking'``.
      4. NULL literal when none of the above exist.
    """
    if "parking" in cols:
        return f"{alias}.parking"
    if "tags" in cols:
        return f"{alias}.tags->'parking'"
    if "other_tags" in cols:
        return f"{alias}.other_tags->'parking'"
    return "NULL"


def _col_expr(alias: str, col: str, cols: set[str]) -> str:
    """Return ``alias.col`` if *col* exists, else ``NULL``."""
    if col in cols:
        return f"{alias}.{col}"
    return "NULL"


def _hstore_or_col(alias: str, col: str, cols: set[str]) -> str:
    """Return column ref, falling back to hstore extraction, else NULL."""
    if col in cols:
        return f"{alias}.{col}"
    if "tags" in cols:
        return f"{alias}.tags->'{col}'"
    if "other_tags" in cols:
        return f"{alias}.other_tags->'{col}'"
    return "NULL"


def _build_where_filter(alias: str, cols: set[str]) -> str:
    """Build the WHERE predicate that selects parking-related rows.

    With the ``parking`` column present the original behaviour is preserved.
    Without it we rely on ``amenity = 'parking'`` (and parking_entrance /
    parking_space for the point table, handled by callers adding extra terms).
    """
    clauses: list[str] = []
    parking = _parking_expr(alias, cols)
    if "amenity" in cols:
        clauses.append(f"lower(COALESCE({alias}.amenity, '')) = 'parking'")
    if parking != "NULL":
        clauses.append(
            f"lower(COALESCE({parking}, '')) IN "
            "('surface','multi-storey','underground','street_side','lane')"
        )
    if not clauses:
        # No useful column at all – select nothing safely.
        return "FALSE"
    return "(" + " OR ".join(clauses) + ")"


# ---------------------------------------------------------------------------
# Polygon ingestion
# ---------------------------------------------------------------------------

def _ingest_from_polygons(db, replace: bool) -> int:
    """Extract parking from planet_osm_polygon."""
    if not table_exists(db, "planet_osm_polygon"):
        logger.warning("planet_osm_polygon not found, skipping polygon source")
        return 0

    cols = get_table_columns(db, "planet_osm_polygon")
    logger.info("planet_osm_polygon columns detected: %s",
                sorted(cols & {"parking", "amenity", "capacity", "covered",
                               "access", "name", "tags", "other_tags"}))

    if replace:
        db.execute(text("DELETE FROM expansion_parking_asset WHERE city = 'riyadh' AND source = 'osm_polygon'"))
        db.commit()

    source_srid = detect_srid(db, "planet_osm_polygon", "way")
    logger.info("Detected SRID %d for planet_osm_polygon.way", source_srid)
    bbox_filter = riyadh_bbox_filter_sql("way", alias="op", source_srid=source_srid)

    pk = _parking_expr("op", cols)
    name_expr = _col_expr("op", "name", cols)
    capacity_expr = _hstore_or_col("op", "capacity", cols)
    covered_expr = _hstore_or_col("op", "covered", cols)
    access_expr = _hstore_or_col("op", "access", cols)
    amenity_expr = _col_expr("op", "amenity", cols)
    where_filter = _build_where_filter("op", cols)

    insert_sql = text(f"""
        INSERT INTO expansion_parking_asset (
            city, source, name, amenity_type, geom, capacity, covered,
            public_access, walk_access_score, dropoff_score
        )
        SELECT
            'riyadh',
            'osm_polygon',
            COALESCE(NULLIF({name_expr}, ''), 'Unnamed parking'),
            CASE
                WHEN lower(COALESCE({pk}, '')) = 'multi-storey' THEN 'multi_storey'
                WHEN lower(COALESCE({pk}, '')) = 'underground' THEN 'underground'
                WHEN lower(COALESCE({pk}, '')) = 'surface' THEN 'surface'
                WHEN lower(COALESCE({pk}, '')) IN ('street_side','lane') THEN 'street_side'
                WHEN lower(COALESCE({amenity_expr}, '')) = 'parking' THEN 'surface'
                ELSE 'unknown'
            END,
            ST_Centroid(ST_Transform(op.way, 4326)),
            CASE
                WHEN {capacity_expr} ~ '^[0-9]+$' THEN CAST({capacity_expr} AS INTEGER)
                ELSE NULL
            END,
            CASE
                WHEN lower(COALESCE({pk}, '')) IN ('multi-storey','underground') THEN TRUE
                WHEN lower(COALESCE({covered_expr}, '')) = 'yes' THEN TRUE
                ELSE FALSE
            END,
            CASE
                WHEN lower(COALESCE({access_expr}, '')) IN ('private','no','permit') THEN FALSE
                ELSE TRUE
            END,
            CASE
                WHEN lower(COALESCE({pk}, '')) = 'surface' THEN 75.0
                WHEN lower(COALESCE({pk}, '')) = 'multi-storey' THEN 60.0
                WHEN lower(COALESCE({pk}, '')) = 'underground' THEN 50.0
                WHEN lower(COALESCE({pk}, '')) IN ('street_side','lane') THEN 80.0
                ELSE 65.0
            END,
            CASE
                WHEN lower(COALESCE({pk}, '')) IN ('street_side','lane') THEN 85.0
                WHEN lower(COALESCE({pk}, '')) = 'surface' THEN 70.0
                WHEN lower(COALESCE({pk}, '')) = 'multi-storey' THEN 45.0
                WHEN lower(COALESCE({pk}, '')) = 'underground' THEN 35.0
                ELSE 55.0
            END
        FROM planet_osm_polygon op
        WHERE op.way IS NOT NULL
          AND {where_filter}
          AND {bbox_filter}
    """)

    result = db.execute(insert_sql)
    db.commit()
    count = result.rowcount
    logger.info("Inserted %d parking assets from planet_osm_polygon", count)
    return count


# ---------------------------------------------------------------------------
# Point ingestion
# ---------------------------------------------------------------------------

def _ingest_from_points(db, replace: bool) -> int:
    """Extract parking from planet_osm_point."""
    if not table_exists(db, "planet_osm_point"):
        logger.warning("planet_osm_point not found, skipping point source")
        return 0

    cols = get_table_columns(db, "planet_osm_point")
    logger.info("planet_osm_point columns detected: %s",
                sorted(cols & {"parking", "amenity", "capacity", "covered",
                               "access", "name", "tags", "other_tags"}))

    if replace:
        db.execute(text("DELETE FROM expansion_parking_asset WHERE city = 'riyadh' AND source = 'osm_point'"))
        db.commit()

    source_srid = detect_srid(db, "planet_osm_point", "way")
    logger.info("Detected SRID %d for planet_osm_point.way", source_srid)
    bbox_filter = riyadh_bbox_filter_sql("way", alias="pt", source_srid=source_srid)

    pk = _parking_expr("pt", cols)
    name_expr = _col_expr("pt", "name", cols)
    capacity_expr = _hstore_or_col("pt", "capacity", cols)
    covered_expr = _hstore_or_col("pt", "covered", cols)
    access_expr = _hstore_or_col("pt", "access", cols)
    amenity_expr = _col_expr("pt", "amenity", cols)

    # WHERE: base filter + point-specific amenity values
    where_clauses: list[str] = []
    if "amenity" in cols:
        where_clauses.append(
            f"lower(COALESCE(pt.amenity, '')) IN ('parking','parking_entrance','parking_space')"
        )
    if pk != "NULL":
        where_clauses.append(
            f"lower(COALESCE({pk}, '')) IN "
            "('surface','multi-storey','underground','street_side','lane')"
        )
    where_filter = "(" + " OR ".join(where_clauses) + ")" if where_clauses else "FALSE"

    insert_sql = text(f"""
        INSERT INTO expansion_parking_asset (
            city, source, name, amenity_type, geom, capacity, covered,
            public_access, walk_access_score, dropoff_score
        )
        SELECT
            'riyadh',
            'osm_point',
            COALESCE(NULLIF({name_expr}, ''), 'Unnamed parking point'),
            CASE
                WHEN lower(COALESCE({pk}, '')) = 'multi-storey' THEN 'multi_storey'
                WHEN lower(COALESCE({pk}, '')) = 'underground' THEN 'underground'
                WHEN lower(COALESCE({pk}, '')) IN ('street_side','lane') THEN 'street_side'
                WHEN lower(COALESCE({amenity_expr}, '')) = 'parking_entrance' THEN 'entrance'
                ELSE 'surface'
            END,
            ST_Transform(pt.way, 4326),
            CASE
                WHEN {capacity_expr} ~ '^[0-9]+$' THEN CAST({capacity_expr} AS INTEGER)
                ELSE NULL
            END,
            lower(COALESCE({pk}, '')) IN ('multi-storey','underground')
                OR lower(COALESCE({covered_expr}, '')) = 'yes',
            CASE
                WHEN lower(COALESCE({access_expr}, '')) IN ('private','no','permit') THEN FALSE
                ELSE TRUE
            END,
            65.0,
            55.0
        FROM planet_osm_point pt
        WHERE pt.way IS NOT NULL
          AND {where_filter}
          AND {bbox_filter}
    """)

    result = db.execute(insert_sql)
    db.commit()
    count = result.rowcount
    logger.info("Inserted %d parking assets from planet_osm_point", count)
    return count


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

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
