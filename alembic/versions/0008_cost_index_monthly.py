"""add cost index monthly table"""

from alembic import op
import sqlalchemy as sa


revision = "0008_cost_index_monthly"
down_revision = "0007_price_quote"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "cost_index_monthly",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("month", sa.Date, nullable=False),
        sa.Column("sector", sa.String(64), nullable=False),
        sa.Column("cci_index", sa.Numeric(8, 2), nullable=False),
        sa.Column("source_url", sa.String(512)),
        sa.Column("asof_date", sa.Date),
    )
    op.create_index(
        "ix_cost_index_monthly_sector_month",
        "cost_index_monthly",
        ["sector", "month"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_cost_index_monthly_sector_month", table_name="cost_index_monthly")
    op.drop_table("cost_index_monthly")
