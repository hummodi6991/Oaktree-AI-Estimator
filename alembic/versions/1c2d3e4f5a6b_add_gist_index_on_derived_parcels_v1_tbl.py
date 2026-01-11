"""Ensure GiST index on derived_parcels_v1_tbl geom."""

from alembic import op

# revision identifiers, used by Alembic.
revision = "1c2d3e4f5a6b"
down_revision = "a3c9d8e7f6b5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS derived_parcels_v1_tbl_geom_gix
            ON public.derived_parcels_v1_tbl USING GIST (geom);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS derived_parcels_v1_tbl_geom_gix;")
