"""Add canonical brand columns to expansion_competitor_quality.

revision: 20260426_ecq_canonical_cols
"""
from alembic import op
import sqlalchemy as sa


revision = "20260426_ecq_canonical_cols"
down_revision = "20260426_brand_alias"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "expansion_competitor_quality",
        sa.Column("canonical_brand_id", sa.String(64), nullable=True),
    )
    op.add_column(
        "expansion_competitor_quality",
        sa.Column("display_name_en", sa.String(256), nullable=True),
    )
    op.add_column(
        "expansion_competitor_quality",
        sa.Column("display_name_ar", sa.String(256), nullable=True),
    )
    op.create_index(
        "ix_ecq_canonical_brand_id",
        "expansion_competitor_quality",
        ["canonical_brand_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_ecq_canonical_brand_id", table_name="expansion_competitor_quality")
    op.drop_column("expansion_competitor_quality", "display_name_ar")
    op.drop_column("expansion_competitor_quality", "display_name_en")
    op.drop_column("expansion_competitor_quality", "canonical_brand_id")
