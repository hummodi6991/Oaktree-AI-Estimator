"""Add spatial indexes to accelerate restaurant scoring queries.

The restaurant scoring path runs ST_DWithin queries on overture_buildings
and planet_osm_polygon that require ST_Transform(..., 4326)::geography
on every row.  Functional GiST indexes on the derived geography expressions
allow PostGIS to use the index directly, avoiding per-row transforms.

Also adds a geography index on osm_roads and the base table behind the
riyadh_parcels_arcgis_proxy view.

NOTE: Indexes are created CONCURRENTLY to avoid blocking writes on large
production tables.  Alembic runs this migration outside a transaction
(see autocommit wrapping below) because CREATE INDEX CONCURRENTLY cannot
run inside a transaction block.

NOTE: Some tables (osm_roads, riyadh_parcels_arcgis_raw, overture_buildings,
planet_osm_polygon) are imported externally and may not exist in all
environments.  Index creation is guarded with to_regclass() checks so the
migration is safe on partially-provisioned databases.

Revision ID: 0016
Revises: 0015_delivery_dedup_index
"""

from alembic import op
from sqlalchemy import text


revision = "0016"
down_revision = "0015_delivery_dedup_index"
branch_labels = None
depends_on = None


def _table_exists(table: str) -> bool:
    """Return True if *table* exists in the current database."""
    conn = op.get_bind()
    result = conn.execute(
        text("SELECT to_regclass(:tbl) IS NOT NULL"),
        {"tbl": table},
    )
    return result.scalar()


def upgrade() -> None:
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction.
    # Use autocommit_block() so Alembic temporarily exits the implicit
    # transaction for these statements.
    with op.get_context().autocommit_block():
        # Overture buildings: functional GiST on geography(4326)
        # Speeds up all ST_DWithin queries in scoring (500m, 800m, 1000m, 1500m bands)
        if _table_exists("public.overture_buildings"):
            op.execute("""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_overture_buildings_geog_4326
                ON overture_buildings
                USING GIST ((ST_Transform(geom, 4326)::geography))
            """)

        # Planet OSM polygon: functional GiST on geography(4326)
        # Speeds up anchor/amenity proximity queries
        if _table_exists("public.planet_osm_polygon"):
            op.execute("""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_planet_osm_polygon_way_geog_4326
                ON planet_osm_polygon
                USING GIST ((ST_Transform(way, 4326)::geography))
            """)

        # OSM roads: functional GiST on geography
        # Speeds up road-context queries used by traffic + parking + zoning
        if _table_exists("public.osm_roads"):
            op.execute("""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_osm_roads_geog
                ON osm_roads
                USING GIST ((geom::geography))
            """)

        # ArcGIS parcels: index on the BASE TABLE (riyadh_parcels_arcgis_raw),
        # NOT the view (riyadh_parcels_arcgis_proxy).  The view's ST_DWithin
        # predicates on geom are pushed down to the base table by the planner.
        if _table_exists("public.riyadh_parcels_arcgis_raw"):
            op.execute("""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_arcgis_parcels_raw_geog
                ON riyadh_parcels_arcgis_raw
                USING GIST ((geom::geography))
            """)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        # DROP INDEX IF EXISTS is already idempotent — safe even when tables
        # (and therefore their indexes) were never created.
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_arcgis_parcels_raw_geog")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_osm_roads_geog")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_planet_osm_polygon_way_geog_4326")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_overture_buildings_geog_4326")
