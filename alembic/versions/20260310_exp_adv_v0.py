"""initial expansion advisor tables

Revision ID: 20260310_exp_adv_v0
Revises: 0016
Create Date: 2026-03-10
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "20260310_exp_adv_v0"
# Keep Alembic revision ids <= 32 chars to fit alembic_version.version_num.
down_revision = "0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "expansion_search",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("brand_name", sa.String(length=256), nullable=False),
        sa.Column("category", sa.String(length=128), nullable=False),
        sa.Column("service_model", sa.String(length=64), nullable=False),
        sa.Column("target_districts", JSONB, nullable=True),
        sa.Column("min_area_m2", sa.Numeric(12, 2), nullable=True),
        sa.Column("max_area_m2", sa.Numeric(12, 2), nullable=True),
        sa.Column("target_area_m2", sa.Numeric(12, 2), nullable=True),
        sa.Column("bbox", JSONB, nullable=True),
        sa.Column("request_json", JSONB, nullable=False),
        sa.Column("notes", JSONB, nullable=True),
    )
    op.create_index(
        "ix_expansion_search_created_at",
        "expansion_search",
        ["created_at"],
    )

    op.create_table(
        "expansion_candidate",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "search_id",
            sa.String(length=36),
            sa.ForeignKey("expansion_search.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("parcel_id", sa.String(length=128), nullable=False),
        sa.Column("lat", sa.Numeric(10, 7), nullable=False),
        sa.Column("lon", sa.Numeric(10, 7), nullable=False),
        sa.Column("area_m2", sa.Numeric(12, 2), nullable=True),
        sa.Column("landuse_label", sa.String(length=256), nullable=True),
        sa.Column("landuse_code", sa.String(length=64), nullable=True),
        sa.Column("population_reach", sa.Numeric(14, 2), nullable=True),
        sa.Column("competitor_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("delivery_listing_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("demand_score", sa.Numeric(6, 2), nullable=False),
        sa.Column("whitespace_score", sa.Numeric(6, 2), nullable=False),
        sa.Column("fit_score", sa.Numeric(6, 2), nullable=False),
        sa.Column("confidence_score", sa.Numeric(6, 2), nullable=False),
        sa.Column("final_score", sa.Numeric(6, 2), nullable=False),
        sa.Column("explanation", JSONB, nullable=True),
        sa.Column(
            "computed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "ix_expansion_candidate_search_id",
        "expansion_candidate",
        ["search_id"],
    )
    op.create_index(
        "ix_expansion_candidate_search_id_final_score",
        "expansion_candidate",
        ["search_id", "final_score"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_expansion_candidate_search_id_final_score",
        table_name="expansion_candidate",
    )
    op.drop_index(
        "ix_expansion_candidate_search_id",
        table_name="expansion_candidate",
    )
    op.drop_table("expansion_candidate")
    op.drop_index(
        "ix_expansion_search_created_at",
        table_name="expansion_search",
    )
    op.drop_table("expansion_search")
