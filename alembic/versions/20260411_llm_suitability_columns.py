"""Add LLM suitability classifier columns to commercial_unit.

Adds seven nullable columns that store the output of the GPT-4o-mini
listing classifier.  All nullable so existing rows are unaffected
until the backfill workflow runs.

Revision ID: 20260411_llm_suitability
Revises: 20260408_backfill_suit
Create Date: 2026-04-11
"""
from alembic import op
import sqlalchemy as sa


revision = "20260411_llm_suitability"
down_revision = "20260408_backfill_suit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "commercial_unit",
        sa.Column("llm_suitability_verdict", sa.String(16), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("llm_suitability_score", sa.Integer(), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("llm_listing_quality_score", sa.Integer(), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("llm_landlord_signal_score", sa.Integer(), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("llm_reasoning", sa.Text(), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("llm_classified_at", sa.TIMESTAMP(timezone=False), nullable=True),
    )
    op.add_column(
        "commercial_unit",
        sa.Column("llm_classifier_version", sa.String(32), nullable=True),
    )
    op.create_index(
        "ix_commercial_unit_llm_suitability_verdict",
        "commercial_unit",
        ["llm_suitability_verdict"],
    )
    op.create_index(
        "ix_commercial_unit_llm_classified_at",
        "commercial_unit",
        ["llm_classified_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_commercial_unit_llm_classified_at", table_name="commercial_unit"
    )
    op.drop_index(
        "ix_commercial_unit_llm_suitability_verdict", table_name="commercial_unit"
    )
    for col in (
        "llm_classifier_version",
        "llm_classified_at",
        "llm_reasoning",
        "llm_landlord_signal_score",
        "llm_listing_quality_score",
        "llm_suitability_score",
        "llm_suitability_verdict",
    ):
        op.drop_column("commercial_unit", col)
