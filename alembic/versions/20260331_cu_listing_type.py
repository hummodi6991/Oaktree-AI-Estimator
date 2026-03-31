"""Add listing_type column to commercial_unit and expansion_candidate

Revision ID: 20260331_cu_listing_type
Revises: 20260330_merge_cu_exp_adv
Create Date: 2026-03-31
"""

from alembic import op
import sqlalchemy as sa

revision = "20260331_cu_listing_type"
down_revision = "20260330_merge_cu_exp_adv"
branch_labels = None
depends_on = None


def upgrade():
    # commercial_unit
    op.add_column("commercial_unit", sa.Column("listing_type", sa.String(32), nullable=True))
    op.create_index("ix_commercial_unit_listing_type", "commercial_unit", ["listing_type"])
    # Backfill existing rows: derive from listing_url
    op.execute("""
        UPDATE commercial_unit
        SET listing_type = CASE
            WHEN listing_url LIKE '%showroom-for-rent%' THEN 'showroom'
            ELSE 'store'
        END
        WHERE listing_type IS NULL
    """)

    # expansion_candidate
    op.add_column("expansion_candidate", sa.Column("unit_listing_type", sa.String(32), nullable=True))


def downgrade():
    op.drop_column("expansion_candidate", "unit_listing_type")
    op.drop_index("ix_commercial_unit_listing_type", table_name="commercial_unit")
    op.drop_column("commercial_unit", "listing_type")
