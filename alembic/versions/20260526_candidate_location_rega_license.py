"""Add ``rega_advertisement_license`` to ``candidate_location``.

PR3 of the multi-portal series. The dedup pass in
``app/ingest/candidate_locations.py::_run_deduplication`` needs an
on-row REGA-license signal so it can collapse two Tier 1 rows that
describe the same physical unit on two different portals (Aqar today,
Bayut in PR4). The license already lives on ``commercial_unit`` as
``aqar_advertisement_license``; this migration denormalizes it onto
``candidate_location`` so the dedup query is single-table.

- ``rega_advertisement_license`` VARCHAR(64) NULL — sparse (~95.5%
  coverage on active Tier 1 rows).
- Backfill from ``commercial_unit.aqar_advertisement_license`` via the
  existing ``(source_tier=1, source_id=aqar_id)`` mapping.
- Partial index on the non-NULL subset because the column is sparse.

No NOT NULL constraint — the column is genuinely optional (~4.5% of
Aqar rows lack a license, and Bayut coverage will be measured in PR4).

Revision ID: 20260526_cl_rega_license
Revises: 20260525_cu_platform_cols
Create Date: 2026-05-25
"""

from alembic import op
import sqlalchemy as sa


revision = "20260526_cl_rega_license"
down_revision = "20260525_cu_platform_cols"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "candidate_location",
        sa.Column("rega_advertisement_license", sa.String(length=64), nullable=True),
    )
    op.execute(
        """
        UPDATE candidate_location
           SET rega_advertisement_license = cu.aqar_advertisement_license
          FROM commercial_unit cu
         WHERE candidate_location.source_tier = 1
           AND candidate_location.source_id = cu.aqar_id
           AND candidate_location.rega_advertisement_license IS NULL
        """
    )
    op.create_index(
        "ix_candidate_location_rega_license",
        "candidate_location",
        ["rega_advertisement_license"],
        unique=False,
        postgresql_where=sa.text("rega_advertisement_license IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_candidate_location_rega_license", table_name="candidate_location"
    )
    op.drop_column("candidate_location", "rega_advertisement_license")
