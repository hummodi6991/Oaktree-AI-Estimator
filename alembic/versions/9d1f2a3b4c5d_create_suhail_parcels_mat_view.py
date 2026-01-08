"""Create materialized view for Suhail parcels."""

from alembic import op


revision = "9d1f2a3b4c5d"
down_revision = "8d7fb94e2f3e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.suhail_parcels_mat') IS NULL THEN
                CREATE MATERIALIZED VIEW public.suhail_parcels_mat AS
                SELECT * FROM public.suhail_parcels_proxy;
            END IF;
        END $$;
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
        DO $$
        BEGIN
            IF to_regclass('public.suhail_parcels_mat') IS NOT NULL THEN
                ANALYZE public.suhail_parcels_mat;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS suhail_parcels_mat_geom_gix;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS public.suhail_parcels_mat;")
