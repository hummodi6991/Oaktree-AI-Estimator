"""Expansion advisor decision_memo_lang column (PR #4a)

Adds a single nullable column on expansion_candidate recording the locale
the persisted decision memo was generated in. GET /candidates/{id}/memo
uses it to regenerate a memo when the requested ``lang`` does not match
the persisted memo's locale.

Purely additive — no existing column is modified, renamed, or dropped,
and no rows are backfilled. Pre-PR-4a rows keep decision_memo_lang NULL;
the read path treats NULL as "unknown locale" and regenerates only when
the requested lang is not English.

Revision ID: 20260519_decision_memo_lang
Revises: 20260518_ea_strengths_risks
Create Date: 2026-05-19 00:00:00.000000

NOTE: the revision id is kept <= 32 chars to fit alembic_version.version_num.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260519_decision_memo_lang"
down_revision = "20260518_ea_strengths_risks"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "expansion_candidate",
        sa.Column("decision_memo_lang", sa.String(8), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("expansion_candidate", "decision_memo_lang")
