"""Add 32638 geometry column to suhail parcels materialized view."""

from alembic import op


revision = "b7e2c8f6d1a2"
down_revision = "9d1f2a3b4c5d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DROP MATERIALIZED VIEW IF EXISTS public.suhail_parcels_mat;
        CREATE MATERIALIZED VIEW public.suhail_parcels_mat AS
        SELECT
            *,
            ST_Transform(geom, 32638) AS geom_32638
        FROM public.suhail_parcels_proxy;
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS suhail_parcels_mat_geom_gix
            ON public.suhail_parcels_mat USING GIST (geom);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS suhail_parcels_mat_geom_32638_gix
            ON public.suhail_parcels_mat USING GIST (geom_32638);
        """
    )
    op.execute(
        """
        ANALYZE public.suhail_parcels_mat;
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS suhail_parcels_mat_geom_32638_gix;")
    op.execute("DROP INDEX IF EXISTS suhail_parcels_mat_geom_gix;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS public.suhail_parcels_mat;")
    op.execute(
        """
        CREATE MATERIALIZED VIEW public.suhail_parcels_mat AS
        SELECT * FROM public.suhail_parcels_proxy;
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS suhail_parcels_mat_geom_gix
            ON public.suhail_parcels_mat USING GIST (geom);
        """
    )
    op.execute(
        """
        ANALYZE public.suhail_parcels_mat;
        """
    )
