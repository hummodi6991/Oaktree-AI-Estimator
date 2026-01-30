"""Add trigram indexes for expanded parcel search fields

Revision ID: 20260201_search_trgm_indexes
Revises: e1f2a3b4c5d6
Create Date: 2026-02-01
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260201_search_trgm_indexes"
down_revision = "e1f2a3b4c5d6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Ensure pg_trgm is available
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

    # These indexes accelerate ILIKE and % (trigram) matching on lower(...) expressions.
    # Use IF NOT EXISTS so repeated runs are safe.
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_suhail_parcels_neighborhood_trgm "
            "ON suhail_parcels_mat USING gin (lower(neighborhood_name) gin_trgm_ops);"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_suhail_parcels_municipality_trgm "
            "ON suhail_parcels_mat USING gin (lower(municipality_name) gin_trgm_ops);"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_suhail_parcels_zoning_id_trgm "
            "ON suhail_parcels_mat USING gin (lower(zoning_id) gin_trgm_ops);"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_suhail_parcels_zoning_category_trgm "
            "ON suhail_parcels_mat USING gin (lower(zoning_category) gin_trgm_ops);"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_suhail_parcels_zoning_subcategory_trgm "
            "ON suhail_parcels_mat USING gin (lower(zoning_subcategory) gin_trgm_ops);"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_suhail_parcels_landuse_trgm "
            "ON suhail_parcels_mat USING gin (lower(landuse) gin_trgm_ops);"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_suhail_parcels_classification_trgm "
            "ON suhail_parcels_mat USING gin (lower(classification) gin_trgm_ops);"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_suhail_parcels_classification_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_suhail_parcels_landuse_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_suhail_parcels_zoning_subcategory_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_suhail_parcels_zoning_category_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_suhail_parcels_zoning_id_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_suhail_parcels_municipality_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_suhail_parcels_neighborhood_trgm;")
