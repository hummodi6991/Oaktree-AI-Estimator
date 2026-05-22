"""add typed business_status column to restaurant_poi

Promotes ``business_status`` (OPERATIONAL / CLOSED_TEMPORARILY /
CLOSED_PERMANENTLY) from ``restaurant_poi.raw`` JSONB to a typed,
indexed column so competitor-count queries can exclude closed venues.

Backfills the new column from ``raw->>'business_status'`` for existing
grid-search rows. Enrich-path rows store the field under
``raw.google.business_status`` instead and are intentionally out of
scope here — they get populated going forward via the enrich path.

The number of rows backfilled is logged for validation; expect roughly
18,000-21,000 (most grid-search rows).

Revision ID: 20260522_poi_business_status
Revises: 20260519_decision_memo_lang
Create Date: 2026-05-22 00:00:00.000000

NOTE: the revision id is kept <= 32 chars to fit alembic_version.version_num.
"""

from __future__ import annotations

import logging

from alembic import op
import sqlalchemy as sa

revision = "20260522_poi_business_status"
down_revision = "20260519_decision_memo_lang"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")


def upgrade() -> None:
    op.add_column(
        "restaurant_poi",
        sa.Column("business_status", sa.String(32), nullable=True),
    )
    op.create_index(
        "ix_restaurant_poi_business_status",
        "restaurant_poi",
        ["business_status"],
    )

    bind = op.get_bind()
    result = bind.execute(
        sa.text(
            """
            UPDATE restaurant_poi
            SET business_status = raw->>'business_status'
            WHERE raw->>'business_status' IS NOT NULL
              AND business_status IS NULL
            """
        )
    )
    logger.info(
        "20260522_poi_business_status: backfilled business_status on %s row(s)",
        result.rowcount,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_restaurant_poi_business_status", table_name="restaurant_poi"
    )
    op.drop_column("restaurant_poi", "business_status")
