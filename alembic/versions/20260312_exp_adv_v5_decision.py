"""expansion advisor v5 decision outputs

Revision ID: 20260312_exp_adv_v5_decision
Revises: 20260311_exp_adv_brand_v4
Create Date: 2026-03-12
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "20260312_exp_adv_v5_decision"
down_revision = "20260311_exp_adv_brand_v4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("expansion_candidate", sa.Column("gate_status_json", JSONB, nullable=True))
    op.add_column("expansion_candidate", sa.Column("confidence_grade", sa.String(length=8), nullable=True))
    op.add_column("expansion_candidate", sa.Column("demand_thesis", sa.Text(), nullable=True))
    op.add_column("expansion_candidate", sa.Column("cost_thesis", sa.Text(), nullable=True))
    op.add_column("expansion_candidate", sa.Column("comparable_competitors_json", JSONB, nullable=True))


def downgrade() -> None:
    op.drop_column("expansion_candidate", "comparable_competitors_json")
    op.drop_column("expansion_candidate", "cost_thesis")
    op.drop_column("expansion_candidate", "demand_thesis")
    op.drop_column("expansion_candidate", "confidence_grade")
    op.drop_column("expansion_candidate", "gate_status_json")
