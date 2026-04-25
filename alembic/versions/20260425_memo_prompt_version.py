"""add decision_memo_prompt_version to expansion_candidate

Cached structured memos in ``expansion_candidate.decision_memo_json`` are
keyed only on (search_id, parcel_id) today, so a meaningful change to
``STRUCTURED_MEMO_SYSTEM_PROMPT`` cannot invalidate them. This migration
adds a ``decision_memo_prompt_version`` text column that the cache helpers
compare against ``llm_decision_memo.MEMO_PROMPT_VERSION``: a mismatch (or
NULL, for rows persisted before this column existed) is treated as a
cache-miss and the memo regenerates lazily on next view. No data is
deleted; existing cached memos remain readable until they are re-served.

Additive, nullable, unindexed — display-side metadata, never queried.

Revision ID: 20260425_memo_prompt_version
Revises: 20260421_aqar_detail_fields
Create Date: 2026-04-25
"""
from alembic import op
import sqlalchemy as sa


revision = "20260425_memo_prompt_version"
down_revision = "20260421_aqar_detail_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "expansion_candidate",
        sa.Column(
            "decision_memo_prompt_version",
            sa.Text(),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("expansion_candidate", "decision_memo_prompt_version")
