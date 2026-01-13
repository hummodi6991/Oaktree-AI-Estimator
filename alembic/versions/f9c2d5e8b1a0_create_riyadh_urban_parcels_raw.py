"""Create Riyadh urban parcels raw table and proxy view."""

from alembic import op


revision = "f9c2d5e8b1a0"
down_revision = "7e9c1f2a3b4c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.riyadh_urban_parcels_raw (
            id bigserial PRIMARY KEY,
            source text NOT NULL DEFAULT 'kaggle_riyadh_urban_parcels',
            geom geometry(MultiPolygon, 4326) NOT NULL,
            raw_props jsonb NULL,
            observed_at timestamptz NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS riyadh_urban_parcels_raw_geom_gix
            ON public.riyadh_urban_parcels_raw USING GIST (geom);
        """
    )
    op.execute(
        """
        DROP VIEW IF EXISTS public.riyadh_urban_parcels_proxy;
        CREATE VIEW public.riyadh_urban_parcels_proxy AS
        SELECT
            r.id::text AS id,
            NULL::text AS landuse,
            NULL::text AS classification,
            ST_Multi(ST_CollectionExtract(r.geom, 3)) AS geom,
            ST_Area(ST_Transform(ST_Multi(ST_CollectionExtract(r.geom, 3)), 32638))::bigint
                AS area_m2,
            ST_Perimeter(ST_Transform(ST_Multi(ST_CollectionExtract(r.geom, 3)), 32638))::bigint
                AS perimeter_m
        FROM public.riyadh_urban_parcels_raw r
        WHERE r.geom IS NOT NULL;
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS public.riyadh_urban_parcels_proxy;")
    op.execute("DROP INDEX IF EXISTS riyadh_urban_parcels_raw_geom_gix;")
    op.execute("DROP TABLE IF EXISTS public.riyadh_urban_parcels_raw;")
