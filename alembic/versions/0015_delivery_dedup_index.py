"""add unique partial index for delivery source deduplication

Revision ID: 0015_delivery_dedup_index
Revises: 0014_delivery_source_tables
Create Date: 2026-03-08
"""

from alembic import op


revision = "0015_delivery_dedup_index"
down_revision = "0014_delivery_source_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Prevent duplicate raw records from repeated scrapes of the same listing.
    # Only applies when source_listing_id is non-null (some scrapers may not
    # provide a stable listing ID).
    op.create_index(
        "uq_dsr_platform_listing",
        "delivery_source_record",
        ["platform", "source_listing_id"],
        unique=True,
        postgresql_where="source_listing_id IS NOT NULL",
    )


def downgrade() -> None:
    op.drop_index(
        "uq_dsr_platform_listing",
        table_name="delivery_source_record",
    )
