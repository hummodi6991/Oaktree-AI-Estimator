"""Add apartments_count column to commercial_unit

Revision ID: 20260407_cu_apartments_count
Revises: 20260407_cu_property_type
Create Date: 2026-04-07
"""

from alembic import op
import sqlalchemy as sa

revision = "20260407_cu_apartments_count"
down_revision = "20260407_cu_property_type"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "commercial_unit",
        sa.Column("apartments_count", sa.Integer, nullable=True),
    )
    op.create_index(
        "ix_commercial_unit_apartments_count",
        "commercial_unit",
        ["apartments_count"],
    )


def downgrade():
    op.drop_index("ix_commercial_unit_apartments_count", table_name="commercial_unit")
    op.drop_column("commercial_unit", "apartments_count")
