"""Expansion Advisor — delivery rating-count history for realized-demand signal.

Creates ``expansion_delivery_rating_history``: a per-snapshot record of the
rating_count observed on each delivery_source_record (HungerStation, Jahez,
Keeta, Talabat, Mrsool, …).  Snapshotting rating_count nightly lets the
service layer compute a *realized* demand signal (Δrating_count per hex per
category over a trailing window) instead of the current *supply* proxy
(listing count).

The table is additive and not yet wired into scoring by default — the
service layer only consults it when ``EXPANSION_REALIZED_DEMAND_ENABLED``
is true, so creating this table is safe with no immediate behavior change.

Revision ID: 20260413_ea_rating_hist
Revises: 20260411_llm_suitability
Create Date: 2026-04-13
"""
from alembic import op


revision = "20260413_ea_rating_hist"
down_revision = "20260411_llm_suitability"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS expansion_delivery_rating_history (
            id                      BIGSERIAL PRIMARY KEY,
            source_record_id        INTEGER NOT NULL,
            platform                VARCHAR(32) NOT NULL,
            brand_name              VARCHAR(256),
            category_raw            VARCHAR(128),
            cuisine_raw             VARCHAR(128),
            rating                  NUMERIC(3,2),
            rating_count            INTEGER,
            lat                     DOUBLE PRECISION,
            lon                     DOUBLE PRECISION,
            geom                    geometry(Point, 4326),
            captured_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
            captured_date           DATE GENERATED ALWAYS AS
                                    (((captured_at AT TIME ZONE 'UTC')::date)) STORED
        )
    """)
    # Idempotent daily snapshots: a given source_record gets at most one row per UTC day.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            ux_edrh_source_captured_date
        ON expansion_delivery_rating_history (source_record_id, captured_date)
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_edrh_captured_at "
        "ON expansion_delivery_rating_history USING btree (captured_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_edrh_geom "
        "ON expansion_delivery_rating_history USING gist (geom)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_edrh_source_captured_desc "
        "ON expansion_delivery_rating_history (source_record_id, captured_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_edrh_source_captured_desc")
    op.execute("DROP INDEX IF EXISTS ix_edrh_geom")
    op.execute("DROP INDEX IF EXISTS ix_edrh_captured_at")
    op.execute("DROP INDEX IF EXISTS ux_edrh_source_captured_date")
    op.execute("DROP TABLE IF EXISTS expansion_delivery_rating_history")
