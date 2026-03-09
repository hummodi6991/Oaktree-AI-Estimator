"""Add spatial indexes to accelerate restaurant scoring queries.

The restaurant scoring path runs ST_DWithin queries on overture_buildings
and planet_osm_polygon that require ST_Transform(..., 4326)::geography
on every row.  Functional GiST indexes on the derived geography expressions
allow PostGIS to use the index directly, avoiding per-row transforms.

Also adds a composite index on osm_roads for the road-context query pattern.

Revision ID: 0016
"""

from alembic import op


revision = "0016"
down_revision = None  # standalone — safe to merge
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Overture buildings: functional GiST on geography(4326)
    # Speeds up all ST_DWithin queries in scoring (500m, 800m, 1000m, 1500m bands)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_overture_buildings_geog_4326
        ON overture_buildings
        USING GIST ((ST_Transform(geom, 4326)::geography))
    """)

    # Planet OSM polygon: functional GiST on geography(4326)
    # Speeds up anchor/amenity proximity queries
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_planet_osm_polygon_way_geog_4326
        ON planet_osm_polygon
        USING GIST ((ST_Transform(way, 4326)::geography))
    """)

    # OSM roads: functional GiST on geography
    # Speeds up road-context queries used by traffic + parking + zoning
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_osm_roads_geog
        ON osm_roads
        USING GIST ((geom::geography))
    """)

    # ArcGIS parcels proxy: GiST on geography for DWithin queries
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_arcgis_parcels_geog
        ON riyadh_parcels_arcgis_proxy
        USING GIST ((geom::geography))
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_overture_buildings_geog_4326")
    op.execute("DROP INDEX IF EXISTS idx_planet_osm_polygon_way_geog_4326")
    op.execute("DROP INDEX IF EXISTS idx_osm_roads_geog")
    op.execute("DROP INDEX IF EXISTS idx_arcgis_parcels_geog")
