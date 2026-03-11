"""expansion advisor brand profile and provider fields

Revision ID: 20260311_exp_adv_brand_v4
Revises: 20260311_exp_adv_saved_v1
Create Date: 2026-03-11
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "20260311_exp_adv_brand_v4"
down_revision = "20260311_exp_adv_saved_v1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "expansion_brand_profile",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("search_id", sa.String(length=36), sa.ForeignKey("expansion_search.id", ondelete="CASCADE"), nullable=False, unique=True),
        sa.Column("price_tier", sa.String(length=32), nullable=True),
        sa.Column("average_check_sar", sa.Numeric(10, 2), nullable=True),
        sa.Column("primary_channel", sa.String(length=32), nullable=True),
        sa.Column("parking_sensitivity", sa.String(length=16), nullable=True),
        sa.Column("frontage_sensitivity", sa.String(length=16), nullable=True),
        sa.Column("visibility_sensitivity", sa.String(length=16), nullable=True),
        sa.Column("target_customer", sa.String(length=64), nullable=True),
        sa.Column("expansion_goal", sa.String(length=32), nullable=True),
        sa.Column("cannibalization_tolerance_m", sa.Numeric(10, 2), nullable=True),
        sa.Column("preferred_districts_json", JSONB, nullable=True),
        sa.Column("excluded_districts_json", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_expansion_brand_profile_search_id", "expansion_brand_profile", ["search_id"])

    op.add_column("expansion_candidate", sa.Column("brand_fit_score", sa.Numeric(6, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("provider_density_score", sa.Numeric(6, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("provider_whitespace_score", sa.Numeric(6, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("multi_platform_presence_score", sa.Numeric(6, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("delivery_competition_score", sa.Numeric(6, 2), nullable=True))


def downgrade() -> None:
    op.drop_column("expansion_candidate", "delivery_competition_score")
    op.drop_column("expansion_candidate", "multi_platform_presence_score")
    op.drop_column("expansion_candidate", "provider_whitespace_score")
    op.drop_column("expansion_candidate", "provider_density_score")
    op.drop_column("expansion_candidate", "brand_fit_score")
    op.drop_index("ix_expansion_brand_profile_search_id", table_name="expansion_brand_profile")
    op.drop_table("expansion_brand_profile")
