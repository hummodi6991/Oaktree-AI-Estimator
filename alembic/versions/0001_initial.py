"""initial schema: parcels, comps, indices, rates, assumption ledger"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cost_index_monthly",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("month", sa.Date, nullable=False),
        sa.Column("sector", sa.String(length=64), nullable=False),
        sa.Column("cci_index", sa.Numeric(8, 2), nullable=False),
        sa.Column("source_url", sa.String(length=512)),
        sa.Column("asof_date", sa.Date),
    )
    op.create_index(
        "ix_cost_index_unique", "cost_index_monthly", ["month", "sector"], unique=True
    )

    op.create_table(
        "rates",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("tenor", sa.String(length=16), nullable=False),
        sa.Column("rate_type", sa.String(length=32), nullable=False),
        sa.Column("value", sa.Numeric(6, 3), nullable=False),
        sa.Column("source_url", sa.String(length=512)),
    )
    op.create_index(
        "ix_rates_key", "rates", ["date", "tenor", "rate_type"], unique=True
    )

    op.create_table(
        "sale_comp",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("city", sa.String(length=64), nullable=False),
        sa.Column("district", sa.String(length=128)),
        sa.Column("asset_type", sa.String(length=32), nullable=False),
        sa.Column("net_area_m2", sa.Numeric(14, 2)),
        sa.Column("price_total", sa.Numeric(14, 2)),
        sa.Column("price_per_m2", sa.Numeric(12, 2)),
        sa.Column("source", sa.String(length=64)),
        sa.Column("source_url", sa.String(length=512)),
        sa.Column("asof_date", sa.Date),
    )
    op.create_index("ix_sale_comp_date_city", "sale_comp", ["date", "city"])

    op.create_table(
        "rent_comp",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("city", sa.String(length=64), nullable=False),
        sa.Column("district", sa.String(length=128)),
        sa.Column("asset_type", sa.String(length=32), nullable=False),
        sa.Column("unit_type", sa.String(length=32)),
        sa.Column("lease_term_months", sa.Integer),
        sa.Column("rent_per_unit", sa.Numeric(12, 2)),
        sa.Column("rent_per_m2", sa.Numeric(12, 2)),
        sa.Column("source", sa.String(length=64)),
        sa.Column("source_url", sa.String(length=512)),
        sa.Column("asof_date", sa.Date),
    )
    op.create_index("ix_rent_comp_date_city", "rent_comp", ["date", "city"])

    op.create_table(
        "parcel",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("gis_polygon", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("municipality", sa.String(length=64)),
        sa.Column("district", sa.String(length=128)),
        sa.Column("zoning", sa.String(length=64)),
        sa.Column("far", sa.Numeric(6, 3)),
        sa.Column("frontage_m", sa.Numeric(10, 2)),
        sa.Column("road_class", sa.String(length=32)),
        sa.Column("setbacks", postgresql.JSONB(astext_type=sa.Text())),
        sa.Column("source_url", sa.String(length=512)),
        sa.Column("asof_date", sa.Date),
    )

    op.create_table(
        "assumption_ledger",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("estimate_id", sa.String(length=64), nullable=False),
        sa.Column("line_id", sa.String(length=64)),
        sa.Column("source_type", sa.String(length=16), nullable=False),
        sa.Column("source_ref", sa.String(length=128)),
        sa.Column("url", sa.String(length=512)),
        sa.Column("value", sa.Numeric(18, 4)),
        sa.Column("unit", sa.String(length=16)),
        sa.Column("owner", sa.String(length=64)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=False),
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )


def downgrade() -> None:
    op.drop_table("assumption_ledger")
    op.drop_table("parcel")
    op.drop_index("ix_rent_comp_date_city", table_name="rent_comp")
    op.drop_table("rent_comp")
    op.drop_index("ix_sale_comp_date_city", table_name="sale_comp")
    op.drop_table("sale_comp")
    op.drop_index("ix_rates_key", table_name="rates")
    op.drop_table("rates")
    op.drop_index("ix_cost_index_unique", table_name="cost_index_monthly")
    op.drop_table("cost_index_monthly")
