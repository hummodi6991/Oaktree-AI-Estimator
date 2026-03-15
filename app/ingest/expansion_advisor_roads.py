"""Expansion Advisor — Roads & Access Context Ingestion.

Reads Riyadh OSM road geometry from whichever source table exists
(planet_osm_line, planet_osm_roads, osm_roads) and normalizes into
expansion_road_context for the Expansion Advisor service.

Heuristics documented inline — road_class mapping, frontage estimates,
corner_lot proxy, and signalized junction distance are approximations
derived from OSM tags, not survey-grade measurements.
"""
from __future__ import annotations

import argparse
import logging
import sys

from sqlalchemy import text

from app.ingest.expansion_advisor_common import (
    RIYADH_BBOX,
    get_session,
    log_table_counts,
    riyadh_filter_sql,
    table_exists,
    validate_db_env,
    write_stats,
)

logger = logging.getLogger("expansion_advisor.roads")

# OSM highway tag → normalized road_class
_ROAD_CLASS_MAP = {
    "motorway": "motorway",
    "motorway_link": "motorway_link",
    "trunk": "trunk",
    "trunk_link": "trunk_link",
    "primary": "primary",
    "primary_link": "primary_link",
    "secondary": "secondary",
    "secondary_link": "secondary_link",
    "tertiary": "tertiary",
    "tertiary_link": "tertiary_link",
    "residential": "residential",
    "living_street": "living_street",
    "service": "service",
    "unclassified": "unclassified",
    "track": "track",
    "pedestrian": "pedestrian",
    "footway": "footway",
    "cycleway": "cycleway",
    "path": "path",
}

_MAJOR_ROAD_TAGS = {"motorway", "trunk", "primary", "secondary"}
_SERVICE_ROAD_TAGS = {"service"}


def _detect_source_table(db) -> str | None:
    """Find the best available OSM line table."""
    for candidate in ["planet_osm_line", "planet_osm_roads", "osm_roads"]:
        if table_exists(db, candidate):
            logger.info("Using OSM source table: %s", candidate)
            return candidate
    return None


def _ingest_roads(db, source_table: str, replace: bool) -> dict:
    """Normalize road segments from the OSM source table into expansion_road_context."""
    if replace:
        db.execute(text("DELETE FROM expansion_road_context WHERE city = 'riyadh'"))
        db.commit()
        logger.info("Cleared existing Riyadh rows from expansion_road_context")

    # Determine geometry column name (planet_osm uses 'way', others may use 'geom')
    geom_col = "way"
    try:
        db.execute(text(f"SELECT way FROM {source_table} LIMIT 0"))
    except Exception:
        db.rollback()
        geom_col = "geom"

    bbox = RIYADH_BBOX
    bbox_filter = (
        f"ST_Intersects({geom_col}, "
        f"ST_MakeEnvelope({bbox['min_lon']}, {bbox['min_lat']}, "
        f"{bbox['max_lon']}, {bbox['max_lat']}, 4326))"
    )

    # Insert normalized road segments
    # NOTE: frontage_length_m, corner_lot, intersection_distance_m,
    # signalized_junction_distance_m, and uturn_access_proxy are approximations.
    insert_sql = text(f"""
        INSERT INTO expansion_road_context (
            city, source, geom, road_class, is_major_road, is_service_road,
            adjacent_road_count, touches_road, frontage_length_m,
            major_road_distance_m, corner_lot,
            intersection_distance_m, signalized_junction_distance_m,
            uturn_access_proxy
        )
        SELECT
            'riyadh',
            'osm',
            ST_Transform(l.{geom_col}, 4326),
            CASE
                WHEN lower(l.highway) IN ('motorway','motorway_link') THEN 'motorway'
                WHEN lower(l.highway) IN ('trunk','trunk_link') THEN 'trunk'
                WHEN lower(l.highway) IN ('primary','primary_link') THEN 'primary'
                WHEN lower(l.highway) IN ('secondary','secondary_link') THEN 'secondary'
                WHEN lower(l.highway) IN ('tertiary','tertiary_link') THEN 'tertiary'
                WHEN lower(l.highway) = 'residential' THEN 'residential'
                WHEN lower(l.highway) = 'living_street' THEN 'living_street'
                WHEN lower(l.highway) = 'service' THEN 'service'
                WHEN lower(l.highway) = 'unclassified' THEN 'unclassified'
                ELSE COALESCE(lower(l.highway), 'unknown')
            END,
            lower(l.highway) IN ('motorway','trunk','primary','secondary'),
            lower(l.highway) = 'service',
            0,     -- adjacent_road_count: computed per-parcel at query time
            FALSE, -- touches_road: computed per-parcel at query time
            -- frontage_length_m: approximate from line length (heuristic)
            ROUND(ST_Length(ST_Transform(l.{geom_col}, 4326)::geography)::numeric, 2),
            NULL,  -- major_road_distance_m: computed per-parcel at query time
            FALSE, -- corner_lot: computed per-parcel at query time
            NULL,  -- intersection_distance_m: not reliably derivable from OSM lines alone
            -- signalized_junction_distance_m: approximate from traffic_signals nodes
            -- This is a placeholder; proper computation needs node-level queries
            NULL,
            -- uturn_access_proxy: heuristic from road class and divider tags
            CASE
                WHEN lower(l.highway) IN ('motorway','trunk') THEN 'restricted'
                WHEN lower(l.highway) IN ('primary','secondary')
                     AND COALESCE(l.oneway, '') = 'yes' THEN 'limited'
                ELSE 'available'
            END
        FROM {source_table} l
        WHERE l.{geom_col} IS NOT NULL
          AND l.highway IS NOT NULL
          AND {bbox_filter}
    """)

    result = db.execute(insert_sql)
    db.commit()
    inserted = result.rowcount
    logger.info("Inserted %d road segments into expansion_road_context", inserted)
    return {"inserted": inserted, "source_table": source_table}


def main() -> None:
    parser = argparse.ArgumentParser(description="Expansion Advisor — Roads & Access ingest")
    parser.add_argument("--city", default="riyadh", help="City filter (default: riyadh)")
    parser.add_argument("--replace", type=lambda v: v.lower() in ("true", "1", "yes"), default=True,
                        help="Replace existing rows (default: true)")
    parser.add_argument("--pbf-url", default=None, help="PBF URL (handled by osm2pgsql in workflow)")
    parser.add_argument("--bbox", default=None, help="Bounding box override (unused, Riyadh hardcoded)")
    parser.add_argument("--write-stats", type=str, default=None, help="Write JSON stats to path")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    validate_db_env()

    db = get_session()
    try:
        source_table = _detect_source_table(db)
        if not source_table:
            logger.error("No OSM road source table found (tried planet_osm_line, planet_osm_roads, osm_roads)")
            sys.exit(1)

        stats = _ingest_roads(db, source_table, replace=args.replace)
        counts = log_table_counts(db, ["expansion_road_context"])
        stats["row_counts"] = counts

        if args.write_stats:
            write_stats(args.write_stats, stats)
    finally:
        db.close()


if __name__ == "__main__":
    main()
