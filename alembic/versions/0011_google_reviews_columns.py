"""add google_place_id, google_fetched_at, google_confidence to restaurant_poi

Revision ID: 0011_google_reviews_columns
Revises: 0010_restaurant_location_tables
Create Date: 2026-03-04
"""

from alembic import op
import sqlalchemy as sa


revision = "0011_google_reviews_columns"
down_revision = "0010_restaurant_location_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "restaurant_poi",
        sa.Column("google_place_id", sa.Text, nullable=True),
    )
    op.add_column(
        "restaurant_poi",
        sa.Column(
            "google_fetched_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "restaurant_poi",
        sa.Column("google_confidence", sa.Numeric, nullable=True),
    )
    op.create_index(
        "ix_restaurant_poi_google_place_id",
        "restaurant_poi",
        ["google_place_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_restaurant_poi_google_place_id", table_name="restaurant_poi")
    op.drop_column("restaurant_poi", "google_confidence")
    op.drop_column("restaurant_poi", "google_fetched_at")
    op.drop_column("restaurant_poi", "google_place_id")
