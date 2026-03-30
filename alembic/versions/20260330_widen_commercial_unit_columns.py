"""widen commercial_unit varchar columns to TEXT

Revision ID: 20260330_widen_cu_cols
Revises: 20260329_fix_geocode_cols
Create Date: 2026-03-30
"""

from alembic import op
import sqlalchemy as sa

revision = "20260330_widen_cu_cols"
down_revision = "20260329_fix_geocode_cols"
branch_labels = None
depends_on = None

_TABLE = "commercial_unit"

# Columns to widen from varchar to TEXT, with their original varchar lengths
_COLUMNS = {
    "neighborhood": sa.String(256),
    "title": sa.String(512),
    "listing_url": sa.String(512),
    "image_url": sa.String(512),
    "contact_phone": sa.String(64),
}


def upgrade() -> None:
    for col, old_type in _COLUMNS.items():
        op.alter_column(
            _TABLE,
            col,
            type_=sa.Text(),
            existing_type=old_type,
            existing_nullable=True,
        )


def downgrade() -> None:
    for col, old_type in _COLUMNS.items():
        op.alter_column(
            _TABLE,
            col,
            type_=old_type,
            existing_type=sa.Text(),
            existing_nullable=True,
        )
