"""Add trigram index for parcel street names."""

from alembic import op
from sqlalchemy import text


revision = "e1f2a3b4c5d6"
down_revision = "d3e4f5a6b7c8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    ctx = op.get_context()
    with ctx.autocommit_block():
        conn = op.get_bind()
        if conn.execute(text("SELECT to_regclass('public.suhail_parcels_mat')")).scalar():
            op.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_suhail_parcels_mat_street_name_trgm
                    ON public.suhail_parcels_mat USING gin (lower(street_name) gin_trgm_ops);
                """
            )


def downgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_suhail_parcels_mat_street_name_trgm;")
