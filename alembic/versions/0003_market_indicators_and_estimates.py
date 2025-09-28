"""market indicators + estimate persistence"""
from alembic import op
import sqlalchemy as sa


revision = "0003_market_ind_est"
down_revision = "0002_boq"  # or "0001_initial" if you skipped 0002
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_indicator",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("city", sa.String(length=64), nullable=False),
        sa.Column("asset_type", sa.String(length=32), nullable=False),
        sa.Column(
            "indicator_type",
            sa.String(length=32),
            nullable=False,
        ),  # rent_per_m2 | sale_price_per_m2
        sa.Column("value", sa.Numeric(12, 2), nullable=False),
        sa.Column("unit", sa.String(length=16), nullable=False),
        sa.Column("source_url", sa.String(length=512)),
        sa.Column("asof_date", sa.Date),
    )
    op.create_index(
        "ix_indicator_key",
        "market_indicator",
        ["date", "city", "asset_type", "indicator_type"],
        unique=False,
    )

    op.create_table(
        "estimate_header",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=False),
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("strategy", sa.String(length=32), nullable=False),
        sa.Column("input_json", sa.Text, nullable=False),  # original request JSON
        sa.Column("totals_json", sa.Text, nullable=False),  # totals from solver
        sa.Column("notes_json", sa.Text),  # notes/meta
    )

    op.create_table(
        "estimate_line",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "estimate_id",
            sa.String(length=36),
            sa.ForeignKey("estimate_header.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "category",
            sa.String(length=32),
            nullable=False,
        ),  # cost | revenue | assumption
        sa.Column("key", sa.String(length=64), nullable=False),
        sa.Column("value", sa.Numeric(18, 4)),
        sa.Column("unit", sa.String(length=16)),
        sa.Column(
            "source_type",
            sa.String(length=16),
        ),  # Observed|Model|Manual
        sa.Column("url", sa.String(length=512)),
        sa.Column("model_version", sa.String(length=64)),
        sa.Column("owner", sa.String(length=64)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=False),
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )
    op.create_index("ix_estimate_line_fk", "estimate_line", ["estimate_id"])


def downgrade() -> None:
    op.drop_index("ix_estimate_line_fk", table_name="estimate_line")
    op.drop_table("estimate_line")
    op.drop_table("estimate_header")
    op.drop_index("ix_indicator_key", table_name="market_indicator")
    op.drop_table("market_indicator")
