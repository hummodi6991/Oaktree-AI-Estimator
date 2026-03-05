"""add restaurant_heatmap_cache table for city-wide opportunity heatmaps

Revision ID: 0013_restaurant_heatmap_cache
Revises: 0012_google_reviews_enrich_state
Create Date: 2026-03-05
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "0013_restaurant_heatmap_cache"
down_revision = "0012_google_reviews_enrich_state"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "restaurant_heatmap_cache",
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("radius_m", sa.Integer(), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.PrimaryKeyConstraint("category", "radius_m"),
    )


def downgrade() -> None:
    op.drop_table("restaurant_heatmap_cache")
