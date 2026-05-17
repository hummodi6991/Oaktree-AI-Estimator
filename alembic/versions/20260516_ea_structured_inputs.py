"""Expansion advisor structured-inputs columns (PR #2a)

Adds five nullable JSONB columns on expansion_candidate so the heuristic
producers can persist a locale-invariant structured record alongside
their existing English string. Nothing reads these columns yet; the
Arabic read path lands in PR #2b. Purely additive — no existing column
is modified, renamed, or dropped, and no rows are backfilled.

Revision ID: 20260516_ea_structured_inputs
Revises: 20260501b_drop_osm_districts
Create Date: 2026-05-16 00:00:00.000000

NOTE: the revision id is kept <= 32 chars to fit alembic_version.version_num.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision = "20260516_ea_structured_inputs"
down_revision = "20260501b_drop_osm_districts"
branch_labels = None
depends_on = None


_COLS = (
    "top_positives_structured_json",
    "top_risks_structured_json",
    "decision_summary_structured_json",
    "demand_thesis_structured_json",
    "cost_thesis_structured_json",
)


def upgrade() -> None:
    for col in _COLS:
        op.add_column("expansion_candidate", sa.Column(col, JSONB, nullable=True))


def downgrade() -> None:
    for col in reversed(_COLS):
        op.drop_column("expansion_candidate", col)
