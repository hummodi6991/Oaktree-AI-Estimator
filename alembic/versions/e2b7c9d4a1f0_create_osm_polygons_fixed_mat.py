"""Create fixed OSM polygons materialized view."""

from alembic import op


revision = "e2b7c9d4a1f0"
down_revision = "c1f4a2b3c4d5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DROP MATERIALIZED VIEW IF EXISTS public.osm_polygons_fixed_mat;
        """
    )
    op.execute(
        """
        CREATE MATERIALIZED VIEW public.osm_polygons_fixed_mat AS
        WITH base AS (
            SELECT
                osm_id,
                landuse,
                ST_SetSRID(ST_FlipCoordinates(way), 4326) AS geom_4326
            FROM public.planet_osm_polygon
            WHERE way IS NOT NULL
        )
        SELECT
            osm_id,
            landuse,
            geom_4326,
            ST_Transform(geom_4326, 3857) AS geom_3857,
            ST_Transform(geom_4326, 32638) AS geom_32638
        FROM base;
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS osm_polygons_fixed_mat_geom_3857_gix
            ON public.osm_polygons_fixed_mat USING GIST (geom_3857);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS osm_polygons_fixed_mat_geom_32638_gix
            ON public.osm_polygons_fixed_mat USING GIST (geom_32638);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS osm_polygons_fixed_mat_geom_4326_gix
            ON public.osm_polygons_fixed_mat USING GIST (geom_4326);
        """
    )
    op.execute("ANALYZE public.osm_polygons_fixed_mat;")


def downgrade() -> None:
    op.execute(
        """
        DROP MATERIALIZED VIEW IF EXISTS public.osm_polygons_fixed_mat;
        """
    )
