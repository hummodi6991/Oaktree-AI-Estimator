"""Add property_type and is_furnished columns to commercial_unit

Revision ID: 20260407_cu_property_type
Revises: 20260331_profitability_columns
Create Date: 2026-04-07
"""

from alembic import op
import sqlalchemy as sa

revision = "20260407_cu_property_type"
down_revision = "20260331_profitability_columns"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "commercial_unit",
        sa.Column("property_type", sa.String(64), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("is_furnished", sa.Boolean, nullable=True),
    )
    op.create_index(
        "ix_commercial_unit_property_type",
        "commercial_unit",
        ["property_type"],
    )
    op.create_index(
        "ix_commercial_unit_is_furnished",
        "commercial_unit",
        ["is_furnished"],
    )


def downgrade():
    op.drop_index("ix_commercial_unit_is_furnished", table_name="commercial_unit")
    op.drop_index("ix_commercial_unit_property_type", table_name="commercial_unit")
    op.drop_column("commercial_unit", "is_furnished")
    op.drop_column("commercial_unit", "property_type")
