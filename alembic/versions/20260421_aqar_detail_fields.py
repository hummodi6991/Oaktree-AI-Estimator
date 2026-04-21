"""Aqar Phase 2 — add detail-page Info-block fields to commercial_unit.

The Aqar list-page scrape captures price/area/district/etc., but every
listing's detail page also exposes a rich "Info" block with real post
dates, update times, view counts, REGA license data, and cadastral
identifiers. This migration adds the nine columns the Phase 2 detail
scraper needs to persist those fields.

All columns are nullable — existing rows are valid with NULLs; values
populate on the first detail-page scrape after this patch lands.

Indices added to support two product queries and the backfill path:

* ``idx_commercial_unit_aqar_created_at`` — "recent listings"
* ``idx_commercial_unit_aqar_updated_at`` — "recently updated"
* ``idx_commercial_unit_detail_unscraped`` — partial, covers the backfill
  retry predicate (``WHERE aqar_detail_scraped_at IS NULL``).

Revision ID: 20260421_aqar_detail_fields
Revises: 20260418_ea_rerank_persistence
Create Date: 2026-04-21
"""
from alembic import op
import sqlalchemy as sa


revision = "20260421_aqar_detail_fields"
down_revision = "20260418_ea_rerank_persistence"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "commercial_unit",
        sa.Column("aqar_created_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("aqar_updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("aqar_views", sa.Integer(), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("aqar_advertisement_license", sa.Text(), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("aqar_license_expiry", sa.Date(), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("aqar_plan_parcel", sa.Text(), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("aqar_area_deed", sa.Numeric(10, 2), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("aqar_listing_source", sa.Text(), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column(
            "aqar_detail_scraped_at", sa.DateTime(timezone=True), nullable=True
        ),
    )

    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_commercial_unit_aqar_created_at "
        "ON commercial_unit (aqar_created_at DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_commercial_unit_aqar_updated_at "
        "ON commercial_unit (aqar_updated_at DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_commercial_unit_detail_unscraped "
        "ON commercial_unit (aqar_id) WHERE aqar_detail_scraped_at IS NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_commercial_unit_detail_unscraped")
    op.execute("DROP INDEX IF EXISTS idx_commercial_unit_aqar_updated_at")
    op.execute("DROP INDEX IF EXISTS idx_commercial_unit_aqar_created_at")

    op.drop_column("commercial_unit", "aqar_detail_scraped_at")
    op.drop_column("commercial_unit", "aqar_listing_source")
    op.drop_column("commercial_unit", "aqar_area_deed")
    op.drop_column("commercial_unit", "aqar_plan_parcel")
    op.drop_column("commercial_unit", "aqar_license_expiry")
    op.drop_column("commercial_unit", "aqar_advertisement_license")
    op.drop_column("commercial_unit", "aqar_views")
    op.drop_column("commercial_unit", "aqar_updated_at")
    op.drop_column("commercial_unit", "aqar_created_at")
