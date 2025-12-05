"""tax rules table for RETT and similar taxes"""

from alembic import op
import sqlalchemy as sa


revision = "0009_tax_rules"
down_revision = "0008_cost_index_monthly"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tax_rule",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("rule_id", sa.Integer, nullable=False),
        sa.Column("tax_type", sa.String(length=32), nullable=False),
        sa.Column("rate", sa.Numeric(6, 4), nullable=False),
        sa.Column("base_type", sa.String(length=128), nullable=True),
        sa.Column("payer_default", sa.String(length=32), nullable=True),
        sa.Column("exemptions", sa.Text, nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
    )
    op.create_index(
        "ix_tax_rule_type_rule_id",
        "tax_rule",
        ["tax_type", "rule_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_tax_rule_type_rule_id", table_name="tax_rule")
    op.drop_table("tax_rule")
