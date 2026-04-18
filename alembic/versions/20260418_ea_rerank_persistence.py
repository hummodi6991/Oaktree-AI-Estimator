"""Expansion Advisor — persist Phase 2 rerank metadata on candidates.

Adds six additive columns to ``expansion_candidate`` so the rerank
metadata produced by ``_apply_rerank_to_candidates`` survives a page
reload (today the values live only on the in-memory candidate dict
returned from POST /searches and are dropped on the next GET):

* ``deterministic_rank`` (INTEGER, NULL) — 1-based rank before any LLM
  shortlist reranking.
* ``final_rank`` (INTEGER, NULL) — 1-based rank after reranking. With
  ``EXPANSION_LLM_RERANK_ENABLED=False`` (the default) this equals
  ``deterministic_rank``.
* ``rerank_applied`` (BOOLEAN, NOT NULL DEFAULT FALSE) — True iff the
  LLM moved this candidate.
* ``rerank_reason`` (JSONB, NULL) — structured explanation when applied.
* ``rerank_delta`` (INTEGER, NOT NULL DEFAULT 0) — final_rank minus
  deterministic_rank.
* ``rerank_status`` (VARCHAR(32), NULL) — one of the canonical
  ``_RERANK_STATUS_*`` strings (``flag_off``, ``shortlist_below_minimum``,
  ``llm_failed``, ``outside_rerank_cap``, ``unchanged``, ``applied``).

Server defaults are dropped after the column is created so future inserts
must come from the application layer (matches the ``20260408_cu_num_rooms``
pattern). All columns are display/explanation fields, not query/filter
fields, so no indexes are added.

Revision ID: 20260418_ea_rerank_persistence
Revises: 20260414_memo_json
Create Date: 2026-04-18
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260418_ea_rerank_persistence"
down_revision = "20260414_memo_json"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "expansion_candidate",
        sa.Column("deterministic_rank", sa.Integer(), nullable=True),
    )
    op.add_column(
        "expansion_candidate",
        sa.Column("final_rank", sa.Integer(), nullable=True),
    )
    op.add_column(
        "expansion_candidate",
        sa.Column(
            "rerank_applied",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "expansion_candidate",
        sa.Column("rerank_reason", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "expansion_candidate",
        sa.Column(
            "rerank_delta",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.add_column(
        "expansion_candidate",
        sa.Column("rerank_status", sa.String(length=32), nullable=True),
    )

    op.alter_column("expansion_candidate", "rerank_applied", server_default=None)
    op.alter_column("expansion_candidate", "rerank_delta", server_default=None)


def downgrade() -> None:
    op.drop_column("expansion_candidate", "rerank_status")
    op.drop_column("expansion_candidate", "rerank_delta")
    op.drop_column("expansion_candidate", "rerank_reason")
    op.drop_column("expansion_candidate", "rerank_applied")
    op.drop_column("expansion_candidate", "final_rank")
    op.drop_column("expansion_candidate", "deterministic_rank")
