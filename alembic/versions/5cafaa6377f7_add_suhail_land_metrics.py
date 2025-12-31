"""add suhail land metrics table"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "5cafaa6377f7"
down_revision = "0009_tax_rules"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "suhail_land_metrics",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column(
            "observed_at",
            sa.DateTime,
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("region_id", sa.Integer, nullable=False),
        sa.Column("province_id", sa.Integer, nullable=True),
        sa.Column("province_name", sa.String(length=128), nullable=True),
        sa.Column("neighborhood_id", sa.Integer, nullable=False),
        sa.Column("neighborhood_name", sa.String(length=256), nullable=False),
        sa.Column("district_norm", sa.String(length=256), nullable=True),
        sa.Column("land_use_group", sa.String(length=128), nullable=False),
        sa.Column("median_ppm2", sa.Numeric(12, 2), nullable=True),
        sa.Column("last_price_ppm2", sa.Numeric(12, 2), nullable=True),
        sa.Column("last_txn_date", sa.Date, nullable=True),
        sa.Column(
            "raw",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.create_index(
        "ux_suhail_land_metrics_as_of_region_neighborhood_land_use",
        "suhail_land_metrics",
        ["as_of_date", "region_id", "neighborhood_id", "land_use_group"],
        unique=True,
    )
    op.create_index(
        "ix_suhail_land_metrics_district_norm",
        "suhail_land_metrics",
        ["district_norm"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_suhail_land_metrics_district_norm",
        table_name="suhail_land_metrics",
    )
    op.drop_index(
        "ux_suhail_land_metrics_as_of_region_neighborhood_land_use",
        table_name="suhail_land_metrics",
    )
    op.drop_table("suhail_land_metrics")
