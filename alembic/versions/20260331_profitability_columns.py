"""Add profitability model columns to candidate_location

Revision ID: 20260331_profitability_columns
Revises: 20260331_cu_listing_type
Create Date: 2026-03-31
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "20260331_profitability_columns"
down_revision = "20260331_cu_listing_type"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "candidate_location",
        sa.Column("profitability_score", sa.Numeric(5, 2)),
    )
    op.add_column(
        "candidate_location",
        sa.Column("success_proxy", sa.Numeric(5, 2)),
    )
    op.add_column(
        "candidate_location",
        sa.Column("model_features", JSONB),
    )
    op.add_column(
        "candidate_location",
        sa.Column("model_version", sa.String(32)),
    )
    op.add_column(
        "candidate_location",
        sa.Column("model_scored_at", sa.DateTime(timezone=True)),
    )
    op.create_index(
        "ix_cl_profitability",
        "candidate_location",
        ["profitability_score"],
    )


def downgrade() -> None:
    op.drop_index("ix_cl_profitability", table_name="candidate_location")
    op.drop_column("candidate_location", "model_scored_at")
    op.drop_column("candidate_location", "model_version")
    op.drop_column("candidate_location", "model_features")
    op.drop_column("candidate_location", "success_proxy")
    op.drop_column("candidate_location", "profitability_score")
