"""create commercial_unit table

Revision ID: 20260329_commercial_unit
Revises: 20260329_geocode_cache
Create Date: 2026-03-29
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "20260329_commercial_unit"
down_revision = "20260329_geocode_cache"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "commercial_unit",
        sa.Column("aqar_id", sa.String(64), primary_key=True),
        sa.Column("title", sa.String(512)),
        sa.Column("description", sa.Text),
        sa.Column("neighborhood", sa.String(256)),
        sa.Column("listing_url", sa.String(512)),
        sa.Column("image_url", sa.String(512)),
        sa.Column("price_sar_annual", sa.Numeric(14, 2)),
        sa.Column("price_per_sqm", sa.Numeric(12, 2)),
        sa.Column("area_sqm", sa.Numeric(10, 2)),
        sa.Column("street_width_m", sa.Numeric(8, 2)),
        sa.Column("num_floors", sa.Integer),
        sa.Column("has_mezzanine", sa.Boolean),
        sa.Column("has_drive_thru", sa.Boolean),
        sa.Column("facade_direction", sa.String(32)),
        sa.Column("contact_phone", sa.String(64)),
        sa.Column("lat", sa.Numeric(10, 7)),
        sa.Column("lon", sa.Numeric(10, 7)),
        sa.Column("restaurant_score", sa.Integer),
        sa.Column("restaurant_suitable", sa.Boolean),
        sa.Column("restaurant_signals", JSONB),
        sa.Column("status", sa.String(16), nullable=False, server_default=sa.text("'active'")),
        sa.Column("first_seen_at", sa.DateTime, server_default=sa.text("now()")),
        sa.Column("last_seen_at", sa.DateTime, server_default=sa.text("now()")),
    )
    op.create_index("ix_commercial_unit_neighborhood", "commercial_unit", ["neighborhood"])
    op.create_index("ix_commercial_unit_status", "commercial_unit", ["status"])
    op.create_index("ix_commercial_unit_restaurant_suitable", "commercial_unit", ["restaurant_suitable"])


def downgrade() -> None:
    op.drop_index("ix_commercial_unit_restaurant_suitable", table_name="commercial_unit")
    op.drop_index("ix_commercial_unit_status", table_name="commercial_unit")
    op.drop_index("ix_commercial_unit_neighborhood", table_name="commercial_unit")
    op.drop_table("commercial_unit")
