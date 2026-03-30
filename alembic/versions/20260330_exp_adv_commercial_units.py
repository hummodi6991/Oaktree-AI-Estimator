"""Expansion advisor: add commercial unit fields to expansion_candidate

Revision ID: 20260330_exp_adv_commercial_units
Revises: merge_exp_adv_heads_20260315
Create Date: 2026-03-30 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "20260330_exp_adv_commercial_units"
down_revision = "merge_exp_adv_heads_20260315"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "expansion_candidate",
        sa.Column("source_type", sa.String(32), server_default="parcel", nullable=True),
    )
    op.add_column(
        "expansion_candidate",
        sa.Column("commercial_unit_id", sa.Text(), nullable=True),
    )
    op.add_column(
        "expansion_candidate",
        sa.Column("listing_url", sa.Text(), nullable=True),
    )
    op.add_column(
        "expansion_candidate",
        sa.Column("image_url", sa.Text(), nullable=True),
    )
    op.add_column(
        "expansion_candidate",
        sa.Column("unit_price_sar_annual", sa.Numeric(14, 2), nullable=True),
    )
    op.add_column(
        "expansion_candidate",
        sa.Column("unit_area_sqm", sa.Numeric(10, 2), nullable=True),
    )
    op.add_column(
        "expansion_candidate",
        sa.Column("unit_street_width_m", sa.Numeric(8, 2), nullable=True),
    )
    op.add_column(
        "expansion_candidate",
        sa.Column("unit_neighborhood", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("expansion_candidate", "unit_neighborhood")
    op.drop_column("expansion_candidate", "unit_street_width_m")
    op.drop_column("expansion_candidate", "unit_area_sqm")
    op.drop_column("expansion_candidate", "unit_price_sar_annual")
    op.drop_column("expansion_candidate", "image_url")
    op.drop_column("expansion_candidate", "listing_url")
    op.drop_column("expansion_candidate", "commercial_unit_id")
    op.drop_column("expansion_candidate", "source_type")
