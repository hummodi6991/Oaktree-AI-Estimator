"""expansion advisor v2 candidate economics fields

Revision ID: 20260310_exp_adv_v2
Revises: 20260310_exp_adv_v1
Create Date: 2026-03-10
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "20260310_exp_adv_v2"
down_revision = "20260310_exp_adv_v1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("expansion_candidate", sa.Column("estimated_rent_sar_m2_year", sa.Numeric(12, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("estimated_annual_rent_sar", sa.Numeric(14, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("estimated_fitout_cost_sar", sa.Numeric(14, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("estimated_revenue_index", sa.Numeric(6, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("economics_score", sa.Numeric(6, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("estimated_payback_months", sa.Numeric(8, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("payback_band", sa.String(length=32), nullable=True))
    op.add_column("expansion_candidate", sa.Column("decision_summary", sa.Text(), nullable=True))
    op.add_column("expansion_candidate", sa.Column("key_risks_json", JSONB, nullable=True))
    op.add_column("expansion_candidate", sa.Column("key_strengths_json", JSONB, nullable=True))

    op.create_index(
        "ix_expansion_candidate_search_id_economics_score",
        "expansion_candidate",
        ["search_id", "economics_score"],
    )


def downgrade() -> None:
    op.drop_index("ix_expansion_candidate_search_id_economics_score", table_name="expansion_candidate")
    op.drop_column("expansion_candidate", "key_strengths_json")
    op.drop_column("expansion_candidate", "key_risks_json")
    op.drop_column("expansion_candidate", "decision_summary")
    op.drop_column("expansion_candidate", "payback_band")
    op.drop_column("expansion_candidate", "estimated_payback_months")
    op.drop_column("expansion_candidate", "economics_score")
    op.drop_column("expansion_candidate", "estimated_revenue_index")
    op.drop_column("expansion_candidate", "estimated_fitout_cost_sar")
    op.drop_column("expansion_candidate", "estimated_annual_rent_sar")
    op.drop_column("expansion_candidate", "estimated_rent_sar_m2_year")
