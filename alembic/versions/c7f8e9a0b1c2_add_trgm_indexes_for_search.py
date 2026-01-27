"""Enable pg_trgm and add search indexes."""

from alembic import op
from sqlalchemy import text


revision = "c7f8e9a0b1c2"
down_revision = "b3d9c6e1a2f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    ctx = op.get_context()
    with ctx.autocommit_block():
        conn = op.get_bind()
        if conn.execute(text("SELECT to_regclass('public.planet_osm_line')")).scalar():
            op.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_planet_osm_line_name_trgm
                    ON public.planet_osm_line USING gin (lower(name) gin_trgm_ops);
                """
            )
        if conn.execute(text("SELECT to_regclass('public.planet_osm_point')")).scalar():
            op.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_planet_osm_point_name_trgm
                    ON public.planet_osm_point USING gin (lower(name) gin_trgm_ops);
                """
            )
        if conn.execute(text("SELECT to_regclass('public.planet_osm_polygon')")).scalar():
            op.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_planet_osm_polygon_name_trgm
                    ON public.planet_osm_polygon USING gin (lower(name) gin_trgm_ops);
                """
            )
        if conn.execute(text("SELECT to_regclass('public.external_feature')")).scalar():
            op.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_external_feature_district_trgm
                    ON public.external_feature USING gin (
                        (COALESCE(properties->>'district_raw', properties->>'district')) gin_trgm_ops
                    );
                """
            )
        if conn.execute(text("SELECT to_regclass('public.suhail_parcels_mat')")).scalar():
            op.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_suhail_parcels_mat_street_name_trgm
                    ON public.suhail_parcels_mat USING gin (lower(street_name) gin_trgm_ops);
                """
            )
            op.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_suhail_parcels_mat_plan_number_trgm
                    ON public.suhail_parcels_mat USING gin ((lower(plan_number::text)) gin_trgm_ops);
                """
            )
            op.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_suhail_parcels_mat_block_number_trgm
                    ON public.suhail_parcels_mat USING gin ((lower(block_number::text)) gin_trgm_ops);
                """
            )
            op.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_suhail_parcels_mat_parcel_number_trgm
                    ON public.suhail_parcels_mat USING gin ((lower(parcel_number::text)) gin_trgm_ops);
                """
            )


def downgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_suhail_parcels_mat_parcel_number_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_suhail_parcels_mat_block_number_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_suhail_parcels_mat_plan_number_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_suhail_parcels_mat_street_name_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_external_feature_district_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_planet_osm_polygon_name_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_planet_osm_point_name_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_planet_osm_line_name_trgm;")
