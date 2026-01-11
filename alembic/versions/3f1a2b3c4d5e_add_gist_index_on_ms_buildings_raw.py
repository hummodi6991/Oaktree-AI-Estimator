"""Ensure GiST index on ms_buildings_raw geom."""

from alembic import op

revision = "3f1a2b3c4d5e"
down_revision = "1c2d3e4f5a6b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ms_buildings_raw_geom_gix
            ON public.ms_buildings_raw USING GIST (geom);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ms_buildings_raw_geom_gix;")
