"""Drop payback columns from expansion_candidate

Revision ID: 20260408_drop_payback
Revises: 20260408_cu_num_rooms
Create Date: 2026-04-08
"""

from alembic import op
import sqlalchemy as sa


revision = "20260408_drop_payback"
down_revision = "20260408_cu_num_rooms"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column("expansion_candidate", "estimated_payback_months")
    op.drop_column("expansion_candidate", "payback_band")


def downgrade():
    op.add_column(
        "expansion_candidate",
        sa.Column("payback_band", sa.String(32), nullable=True),
    )
    op.add_column(
        "expansion_candidate",
        sa.Column("estimated_payback_months", sa.Numeric(8, 2), nullable=True),
    )
