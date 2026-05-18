"""Expansion advisor structured strengths/risks columns (PR #3)

Adds two nullable JSONB columns on expansion_candidate so the
_build_strengths_and_risks producer can persist locale-invariant
structured records alongside its existing English string lists. The
Arabic read path renders them in _normalize_candidate_payload. Purely
additive — no existing column is modified, renamed, or dropped, and no
rows are backfilled (pre-PR-3 rows keep these columns NULL and the
Arabic read path falls back to the persisted English text).

Revision ID: 20260518_ea_strengths_risks
Revises: 20260516_ea_structured_inputs
Create Date: 2026-05-18 00:00:00.000000

NOTE: the revision id is kept <= 32 chars to fit alembic_version.version_num.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "20260518_ea_strengths_risks"
down_revision = "20260516_ea_structured_inputs"
branch_labels = None
depends_on = None


_COLS = (
    "key_strengths_structured_json",
    "key_risks_structured_json",
)


def upgrade() -> None:
    for col in _COLS:
        op.add_column("expansion_candidate", sa.Column(col, JSONB, nullable=True))


def downgrade() -> None:
    for col in reversed(_COLS):
        op.drop_column("expansion_candidate", col)
