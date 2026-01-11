"""create derived_parcels_v1 table and view"""

from alembic import op


revision = "a3c9d8e7f6b5"
down_revision = "f2a1b3c4d5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.derived_parcels_v1') IS NOT NULL
                AND EXISTS (
                    SELECT 1
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public'
                      AND c.relname = 'derived_parcels_v1'
                      AND c.relkind = 'm'
                ) THEN
                IF to_regclass('public.derived_parcels_v1_mv_old') IS NOT NULL THEN
                    EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS public.derived_parcels_v1_mv_old';
                END IF;
                EXECUTE 'ALTER MATERIALIZED VIEW public.derived_parcels_v1 RENAME TO derived_parcels_v1_mv_old';
            END IF;
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.derived_parcels_v1_tbl (
            parcel_id bigserial PRIMARY KEY,
            geom geometry(Polygon, 4326) NOT NULL,
            site_area_m2 double precision NOT NULL,
            footprint_area_m2 double precision NOT NULL,
            building_count int NOT NULL
        );
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS derived_parcels_v1_tbl_geom_gix
            ON public.derived_parcels_v1_tbl USING GIST (geom);
        """
    )

    op.execute(
        """
        CREATE OR REPLACE VIEW public.derived_parcels_v1 AS
        SELECT
            parcel_id,
            geom,
            site_area_m2,
            footprint_area_m2,
            building_count
        FROM public.derived_parcels_v1_tbl;
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS public.derived_parcels_v1;")
    op.execute("DROP INDEX IF EXISTS derived_parcels_v1_tbl_geom_gix;")
    op.execute("DROP TABLE IF EXISTS public.derived_parcels_v1_tbl;")
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.derived_parcels_v1_mv_old') IS NOT NULL THEN
                EXECUTE 'ALTER MATERIALIZED VIEW public.derived_parcels_v1_mv_old RENAME TO derived_parcels_v1';
            END IF;
        END $$;
        """
    )
