"""Expansion advisor v6 feature and gate fields

Revision ID: 20260313_exp_adv_v6_features
Revises: 20260312_exp_adv_v5_decision
Create Date: 2026-03-13 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision = "20260313_exp_adv_v6_features"
down_revision = "20260312_exp_adv_v5_decision"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("expansion_candidate", sa.Column("zoning_fit_score", sa.Numeric(6, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("frontage_score", sa.Numeric(6, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("access_score", sa.Numeric(6, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("parking_score", sa.Numeric(6, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("access_visibility_score", sa.Numeric(6, 2), nullable=True))
    op.add_column("expansion_candidate", sa.Column("gate_reasons_json", JSONB, nullable=True))
    op.add_column("expansion_candidate", sa.Column("feature_snapshot_json", JSONB, nullable=True))


def downgrade() -> None:
    op.drop_column("expansion_candidate", "feature_snapshot_json")
    op.drop_column("expansion_candidate", "gate_reasons_json")
    op.drop_column("expansion_candidate", "access_visibility_score")
    op.drop_column("expansion_candidate", "parking_score")
    op.drop_column("expansion_candidate", "access_score")
    op.drop_column("expansion_candidate", "frontage_score")
    op.drop_column("expansion_candidate", "zoning_fit_score")

