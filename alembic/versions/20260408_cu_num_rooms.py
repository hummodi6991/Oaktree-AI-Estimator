"""Add num_rooms column to commercial_unit

Revision ID: 20260408_cu_num_rooms
Revises: 20260407_cu_apartments_count
Create Date: 2026-04-08
"""

from alembic import op
import sqlalchemy as sa

revision = "20260408_cu_num_rooms"
down_revision = "20260407_cu_apartments_count"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "commercial_unit",
        sa.Column("num_rooms", sa.Integer, nullable=True),
    )
    op.create_index(
        "ix_commercial_unit_num_rooms",
        "commercial_unit",
        ["num_rooms"],
    )


def downgrade():
    op.drop_index("ix_commercial_unit_num_rooms", table_name="commercial_unit")
    op.drop_column("commercial_unit", "num_rooms")
