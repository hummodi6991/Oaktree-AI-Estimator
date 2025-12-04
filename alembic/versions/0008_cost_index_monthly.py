"""add cost index monthly table"""

from alembic import op
import sqlalchemy as sa


revision = "0008_cost_index_monthly"
down_revision = "0007_price_quote"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    table_name = "cost_index_monthly"
    index_name = "ix_cost_index_monthly_sector_month"

    tables = inspector.get_table_names()

    if table_name not in tables:
        op.create_table(
            table_name,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("month", sa.Date, nullable=False),
            sa.Column("sector", sa.String(64), nullable=False),
            sa.Column("cci_index", sa.Numeric(8, 2), nullable=False),
            sa.Column("source_url", sa.String(512)),
            sa.Column("asof_date", sa.Date),
        )

    if table_name in tables:
        existing_indexes = {ix["name"] for ix in inspector.get_indexes(table_name)}
        if index_name not in existing_indexes:
            op.create_index(
                index_name,
                table_name,
                ["sector", "month"],
                unique=False,
            )


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_name = "cost_index_monthly"
    index_name = "ix_cost_index_monthly_sector_month"

    tables = inspector.get_table_names()

    if table_name in tables:
        existing_indexes = {ix["name"] for ix in inspector.get_indexes(table_name)}
        if index_name in existing_indexes:
            op.drop_index(index_name, table_name=table_name)
        op.drop_table(table_name)
