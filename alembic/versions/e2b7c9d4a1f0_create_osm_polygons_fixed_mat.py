"""Create osm_polygons_fixed_mat materialized view."""

from alembic import op


revision = "e2b7c9d4a1f0"
down_revision = "c1f4a2b3c4d5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DROP MATERIALIZED VIEW IF EXISTS public.osm_polygons_fixed_mat;
        CREATE MATERIALIZED VIEW IF NOT EXISTS public.osm_polygons_fixed_mat AS
        SELECT 1 AS id;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP MATERIALIZED VIEW IF EXISTS public.osm_polygons_fixed_mat;
        """
    )
