"""create geocode_cache table

Revision ID: 20260329_geocode_cache
Revises: 20260322_ea_geog_gist
Create Date: 2026-03-29
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "20260329_geocode_cache"
down_revision = "20260322_ea_geog_gist"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "geocode_cache",
        sa.Column("query", sa.String(512), primary_key=True),
        sa.Column("lat", sa.Numeric(10, 7)),
        sa.Column("lon", sa.Numeric(10, 7)),
        sa.Column("formatted_address", sa.String(512)),
        sa.Column("raw", JSONB),
        sa.Column("created_at", sa.DateTime, server_default=sa.text("now()")),
    )


def downgrade() -> None:
    op.drop_table("geocode_cache")
