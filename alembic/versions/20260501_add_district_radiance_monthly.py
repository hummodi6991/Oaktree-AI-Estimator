"""Create district_radiance_monthly table for Black Marble VNP46A3 ingest.

Stores monthly per-district nighttime radiance aggregates used as the third
(growth) leg of the market-viability conjunction in the Expansion Advisor.

revision: 20260501_district_radiance_monthly
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic
revision = "20260501_district_radiance_monthly"
down_revision = "0020_candidate_location"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "district_radiance_monthly",
        sa.Column("district_key", sa.Text(), nullable=False),
        sa.Column("year_month", sa.Date(), nullable=False),
        sa.Column("radiance_mean", sa.Numeric(12, 4), nullable=True),
        sa.Column("radiance_median", sa.Numeric(12, 4), nullable=True),
        sa.Column("radiance_sum", sa.Numeric(14, 4), nullable=True),
        sa.Column("radiance_p90", sa.Numeric(12, 4), nullable=True),
        sa.Column("pixel_count_total", sa.Integer(), nullable=False),
        sa.Column("pixel_count_valid", sa.Integer(), nullable=False),
        sa.Column("quality_filter", sa.String(32), nullable=False),
        sa.Column("source", sa.String(64), nullable=False),
        sa.Column("tile", sa.String(16), nullable=False),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("district_key", "year_month", "source"),
    )
    op.create_index(
        "ix_district_radiance_monthly_year_month",
        "district_radiance_monthly",
        ["year_month"],
        postgresql_using="btree",
        postgresql_ops={"year_month": "DESC"},
    )
    op.create_index(
        "ix_district_radiance_monthly_year_month_district",
        "district_radiance_monthly",
        ["year_month", "district_key"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_district_radiance_monthly_year_month_district",
        table_name="district_radiance_monthly",
    )
    op.drop_index(
        "ix_district_radiance_monthly_year_month",
        table_name="district_radiance_monthly",
    )
    op.drop_table("district_radiance_monthly")
