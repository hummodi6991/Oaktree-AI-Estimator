"""Add brief-extraction columns to ``expansion_brand_profile``.

"Describe your brand" free-text brief (design
docs/llm_brief_extraction_phase_one.md §6.1): the raw operator text plus
extraction audit metadata (LLM output, model, prompt version, whether the
user accepted the proposal and which fields they edited afterwards).

All columns nullable, no backfill: rows exist only when the operator typed
a brief with EXPANSION_BRIEF_EXTRACTION_ENABLED on. Nothing here is read
by scoring — ``brief_text`` feeds memo context, the rest is audit/eval.

Revision ID: 20260612_brief_extraction
Revises: 20260611_brand_archetype
Create Date: 2026-06-12
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "20260612_brief_extraction"
down_revision = "20260611_brand_archetype"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "expansion_brand_profile",
        sa.Column("brief_text", sa.Text(), nullable=True),
    )
    op.add_column(
        "expansion_brand_profile",
        sa.Column("brief_extraction_json", JSONB(), nullable=True),
    )
    op.add_column(
        "expansion_brand_profile",
        sa.Column("brief_extraction_model", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "expansion_brand_profile",
        sa.Column(
            "brief_extraction_prompt_version", sa.String(length=32), nullable=True
        ),
    )
    op.add_column(
        "expansion_brand_profile",
        sa.Column("brief_extraction_accepted", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "expansion_brand_profile",
        sa.Column("brief_extraction_edited_fields_json", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("expansion_brand_profile", "brief_extraction_edited_fields_json")
    op.drop_column("expansion_brand_profile", "brief_extraction_accepted")
    op.drop_column("expansion_brand_profile", "brief_extraction_prompt_version")
    op.drop_column("expansion_brand_profile", "brief_extraction_model")
    op.drop_column("expansion_brand_profile", "brief_extraction_json")
    op.drop_column("expansion_brand_profile", "brief_text")
