"""far rules table (district-level max FAR)"""

from alembic import op
import sqlalchemy as sa


revision = "0006_far_rules"
down_revision = "0005_land_use_stat"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "far_rule",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("city", sa.String(length=64), nullable=False),
        sa.Column("district", sa.String(length=128), nullable=False),
        sa.Column("zoning", sa.String(length=64)),
        sa.Column("road_class", sa.String(length=32)),
        sa.Column("frontage_min_m", sa.Numeric(10, 2)),
        sa.Column("far_max", sa.Numeric(6, 3), nullable=False),
        sa.Column("asof_date", sa.Date),
        sa.Column("source_url", sa.String(length=512)),
    )
    op.create_index(
        "ix_far_rule_city_district",
        "far_rule",
        ["city", "district"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_far_rule_city_district", table_name="far_rule")
    op.drop_table("far_rule")
