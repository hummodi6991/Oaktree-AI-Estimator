"""price quotes for land SAR/m2"""

from alembic import op
import sqlalchemy as sa


revision = "0007_price_quote"
down_revision = "0006_far_rules"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "price_quote",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("city", sa.String(64), nullable=False),
        sa.Column("district", sa.String(128)),
        sa.Column("parcel_id", sa.String(64)),
        sa.Column("sar_per_m2", sa.Numeric(12, 2), nullable=False),
        sa.Column("observed_at", sa.DateTime, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("method", sa.String(64)),
        sa.Column("source_url", sa.String(512)),
    )


def downgrade():
    op.drop_table("price_quote")
