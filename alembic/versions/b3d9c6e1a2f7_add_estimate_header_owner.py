"""add owner to estimate_header"""
from alembic import op
import sqlalchemy as sa


revision = "b3d9c6e1a2f7"
down_revision = "a9b6cdbd0831"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("estimate_header", sa.Column("owner", sa.String(length=64), nullable=True))
    op.create_index(
        "ix_estimate_header_owner_created_at",
        "estimate_header",
        ["owner", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_estimate_header_owner_created_at", table_name="estimate_header")
    op.drop_column("estimate_header", "owner")
