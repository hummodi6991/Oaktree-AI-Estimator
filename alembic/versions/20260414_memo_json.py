"""Expansion Advisor — structured decision memo columns.

Adds two nullable display columns to ``expansion_candidate``:

* ``decision_memo`` (TEXT) — rendered, human-readable memo. On the structured
  path this is the text rendered from the JSON; on the legacy fallback path
  this is the generic memo returned to the caller. Populated whenever a memo
  is generated; never queried for filtering/ranking.
* ``decision_memo_json`` (JSONB) — the structured memo object (headline,
  ranking_explanation, key_evidence, risks, comparison, bottom_line) when
  the structured LLM path succeeds; NULL on fallback.

Both columns are nullable, have no default, and are intentionally unindexed —
they are display fields, not query fields. Raw SQL is used (instead of
``op.add_column``) to match the style of ``20260413_ea_rating_hist`` and to
keep the upgrade idempotent via ``IF NOT EXISTS`` / ``IF EXISTS`` guards.

Revision ID: 20260414_memo_json
Revises: 20260413_ea_rating_hist
Create Date: 2026-04-14
"""
from alembic import op


revision = "20260414_memo_json"
down_revision = "20260413_ea_rating_hist"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE expansion_candidate "
        "ADD COLUMN IF NOT EXISTS decision_memo TEXT"
    )
    op.execute(
        "ALTER TABLE expansion_candidate "
        "ADD COLUMN IF NOT EXISTS decision_memo_json JSONB"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE expansion_candidate "
        "DROP COLUMN IF EXISTS decision_memo_json"
    )
    op.execute(
        "ALTER TABLE expansion_candidate "
        "DROP COLUMN IF EXISTS decision_memo"
    )
