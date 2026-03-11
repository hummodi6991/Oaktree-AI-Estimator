"""Expansion advisor v6.1 completeness and output fields

Revision ID: 20260314_exp_adv_v61_outputs
Revises: 20260313_exp_adv_v6_features
Create Date: 2026-03-14 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision = "20260314_exp_adv_v61_outputs"
down_revision = "20260313_exp_adv_v6_features"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("expansion_candidate", sa.Column("score_breakdown_json", JSONB, nullable=True))
    op.add_column("expansion_candidate", sa.Column("top_positives_json", JSONB, nullable=True))
    op.add_column("expansion_candidate", sa.Column("top_risks_json", JSONB, nullable=True))
    op.add_column("expansion_candidate", sa.Column("rank_position", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("expansion_candidate", "rank_position")
    op.drop_column("expansion_candidate", "top_risks_json")
    op.drop_column("expansion_candidate", "top_positives_json")
    op.drop_column("expansion_candidate", "score_breakdown_json")
