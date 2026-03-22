"""add geography-cast GiST indexes on EA tables and parcels

These indexes allow ST_DWithin(geom::geography, ...) queries to use index
scans instead of sequential scans on the three EA enrichment tables and
the raw parcels table.

NOTE: These indexes are created with plain CREATE INDEX (not CONCURRENTLY)
because Alembic migrations run inside a transaction.  For very large tables
in production you may prefer to run the CONCURRENTLY variant outside of
Alembic to avoid holding an ACCESS EXCLUSIVE lock.

Revision ID: 20260322_ea_geog_gist
Revises: merge_exp_adv_heads_20260315
Create Date: 2026-03-22
"""

from alembic import op

revision = "20260322_ea_geog_gist"
down_revision = "merge_exp_adv_heads_20260315"
branch_labels = None
depends_on = None

_INDEXES = [
    ("ix_erc_geom_geog", "expansion_road_context", "(geom::geography)"),
    ("ix_epa_geom_geog", "expansion_parking_asset", "(geom::geography)"),
    ("ix_ecq_geom_geog", "expansion_competitor_quality", "(geom::geography)"),
    ("ix_parcels_geom_geog", "riyadh_parcels_arcgis_raw", "(geom::geography)"),
]


def upgrade() -> None:
    for idx_name, table, expr in _INDEXES:
        op.execute(
            f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} USING GIST ({expr})"
        )


def downgrade() -> None:
    for idx_name, _table, _expr in reversed(_INDEXES):
        op.execute(f"DROP INDEX IF EXISTS {idx_name}")
