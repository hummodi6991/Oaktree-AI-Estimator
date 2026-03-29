"""widen geocode_cache formatted_address to TEXT, ensure raw is JSONB

Revision ID: 20260329_fix_geocode_cols
Revises: 20260329_commercial_unit
Create Date: 2026-03-29
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "20260329_fix_geocode_cols"
down_revision = "20260329_commercial_unit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Widen formatted_address from varchar(512) to TEXT
    op.alter_column(
        "geocode_cache",
        "formatted_address",
        type_=sa.Text(),
        existing_type=sa.String(512),
        existing_nullable=True,
    )

    # Ensure raw column is proper JSONB (re-cast any text-stored JSON)
    op.execute(
        "ALTER TABLE geocode_cache "
        "ALTER COLUMN raw TYPE jsonb USING raw::text::jsonb"
    )


def downgrade() -> None:
    op.alter_column(
        "geocode_cache",
        "formatted_address",
        type_=sa.String(512),
        existing_type=sa.Text(),
        existing_nullable=True,
    )
