"""backfill_restaurant_suitable_listing_type_whitelist

Flip restaurant_suitable to false for every row whose listing_type is not in
the F&B-compatible whitelist (store, showroom).  This matches the hard gate
added to the Aqar suitability classifier in scripts/scrape_aqar.py.

Revision ID: 20260408_backfill_suit
Revises: 20260408_drop_payback
Create Date: 2026-04-08
"""

from alembic import op
from sqlalchemy import text


revision = "20260408_backfill_suit"
down_revision = "20260408_drop_payback"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Flip restaurant_suitable to false for every row whose listing_type is
    # not in the F&B-compatible whitelist.  This matches the hard gate added
    # to the Aqar suitability classifier.
    op.execute(
        text(
            """
            UPDATE commercial_unit
               SET restaurant_suitable = false
             WHERE (listing_type IS NULL
                    OR lower(trim(listing_type)) NOT IN ('store', 'showroom'))
               AND restaurant_suitable = true
            """
        )
    )


def downgrade() -> None:
    # Intentionally a no-op.  Reverting would re-contaminate the data pool
    # with warehouses and buildings.  If a true rollback is ever needed, it
    # should be performed by a deliberate manual query, not an automatic
    # downgrade path.
    pass
