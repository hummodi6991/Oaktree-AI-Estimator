"""boq library"""
from alembic import op
import sqlalchemy as sa


revision = "0002_boq"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "boq_item",
        sa.Column("code", sa.String(length=32), primary_key=True),
        sa.Column("description", sa.String(length=256), nullable=False),
        sa.Column("uom", sa.String(length=16), nullable=False, server_default="m2"),
        sa.Column("quantity_per_m2", sa.Numeric(12, 4), nullable=False, server_default="1.0"),
        sa.Column("baseline_unit_cost", sa.Numeric(12, 2), nullable=False),
        sa.Column("city_factor", sa.Numeric(6, 3), nullable=False, server_default="1.000"),
        sa.Column("volatility_tag", sa.String(length=32)),
        sa.Column("source_url", sa.String(length=512)),
    )


def downgrade() -> None:
    op.drop_table("boq_item")
