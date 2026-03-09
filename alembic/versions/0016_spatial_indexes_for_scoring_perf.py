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

Revision ID: 0016
Revises: 0015_delivery_dedup_index
"""

from alembic import op


revision = "0016"
down_revision = "0015_delivery_dedup_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction.
    # Alembic normally wraps each migration in a transaction, so we
    # must commit and exit the implicit transaction first.
    op.execute("COMMIT")

    # Overture buildings: functional GiST on geography(4326)
    # Speeds up all ST_DWithin queries in scoring (500m, 800m, 1000m, 1500m bands)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_overture_buildings_geog_4326
        ON overture_buildings
        USING GIST ((ST_Transform(geom, 4326)::geography))
    """)

    # Planet OSM polygon: functional GiST on geography(4326)
    # Speeds up anchor/amenity proximity queries
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_planet_osm_polygon_way_geog_4326
        ON planet_osm_polygon
        USING GIST ((ST_Transform(way, 4326)::geography))
    """)

    # OSM roads: functional GiST on geography
    # Speeds up road-context queries used by traffic + parking + zoning
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_osm_roads_geog
        ON osm_roads
        USING GIST ((geom::geography))
    """)

    # ArcGIS parcels: index on the BASE TABLE (riyadh_parcels_arcgis_raw),
    # NOT the view (riyadh_parcels_arcgis_proxy).  The view's ST_DWithin
    # predicates on geom are pushed down to the base table by the planner.
    # A GiST index already exists on geom (riyadh_parcels_arcgis_raw_geom_gix)
    # from the create_arcgis_parcels_proxy migration — but that is a plain
    # geometry index, not a geography one.  The geography index accelerates
    # the ST_DWithin(geom::geography, ...) pattern used in scoring queries.
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_arcgis_parcels_raw_geog
        ON riyadh_parcels_arcgis_raw
        USING GIST ((geom::geography))
    """)


def downgrade() -> None:
    op.execute("COMMIT")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_overture_buildings_geog_4326")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_planet_osm_polygon_way_geog_4326")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_osm_roads_geog")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_arcgis_parcels_raw_geog")
