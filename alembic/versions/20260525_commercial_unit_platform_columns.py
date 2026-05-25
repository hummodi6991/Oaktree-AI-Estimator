"""Add platform and platform_listing_id columns to commercial_unit.

PR2 of the multi-portal series. Additive widen so the table can hold
rows from multiple portals (Aqar today, Bayut in PR4) while keeping
every existing reader query unchanged.

- ``platform``           VARCHAR(16)  NOT NULL — portal discriminator.
                                                 Backfilled to ``'aqar'``
                                                 for every existing row
                                                 via the temporary
                                                 server_default.
- ``platform_listing_id`` VARCHAR(128) NOT NULL — portal-native listing
                                                  id. Backfilled from
                                                  ``aqar_id`` for every
                                                  existing row, then
                                                  tightened to NOT NULL.

A unique index on ``(platform, platform_listing_id)`` enforces the
"every (portal, portal_listing_id) pair is globally unique" invariant
and will be the conflict target for Bayut rows in PR4. A separate
non-unique index on ``platform`` alone supports cheap per-platform
analytics.

The server_default on ``platform`` is dropped after the column is
populated so the application layer is authoritative going forward —
matches the post-backfill pattern used elsewhere in this table's
history.

Revision ID: 20260525_cu_platform_cols
Revises: 20260522_poi_business_status
Create Date: 2026-05-25
"""

from alembic import op
import sqlalchemy as sa


revision = "20260525_cu_platform_cols"
down_revision = "20260522_poi_business_status"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "commercial_unit",
        sa.Column(
            "platform",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'aqar'"),
        ),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("platform_listing_id", sa.String(length=128), nullable=True),
    )
    op.execute(
        "UPDATE commercial_unit "
        "SET platform_listing_id = aqar_id "
        "WHERE platform_listing_id IS NULL"
    )
    op.alter_column("commercial_unit", "platform_listing_id", nullable=False)
    op.create_index(
        "ix_commercial_unit_platform_listing_id",
        "commercial_unit",
        ["platform", "platform_listing_id"],
        unique=True,
    )
    op.create_index(
        "ix_commercial_unit_platform",
        "commercial_unit",
        ["platform"],
        unique=False,
    )
    # Application layer is the source of truth from here on.
    op.alter_column("commercial_unit", "platform", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_commercial_unit_platform", table_name="commercial_unit")
    op.drop_index(
        "ix_commercial_unit_platform_listing_id", table_name="commercial_unit"
    )
    op.drop_column("commercial_unit", "platform_listing_id")
    op.drop_column("commercial_unit", "platform")
