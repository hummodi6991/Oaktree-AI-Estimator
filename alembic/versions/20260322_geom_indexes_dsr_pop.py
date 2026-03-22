"""Add pre-computed geom column + GiST index to delivery_source_record
and population_density for indexed spatial lookups.

NOTE: CREATE INDEX runs inside the Alembic transaction (not CONCURRENTLY).
For tables with millions of rows this will hold a table lock for several
seconds.  If that is a concern, run the CREATE INDEX statements outside
Alembic with CONCURRENTLY instead.

Revision ID: 20260322_geom_indexes_dsr_pop
Revises: 20260321_parcels_district_label
Create Date: 2026-03-22
"""
from alembic import op

revision = "20260322_geom_indexes_dsr_pop"
down_revision = "20260321_parcels_district_label"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── delivery_source_record ──

    # 1. Add geometry column
    op.execute("""
        ALTER TABLE delivery_source_record
        ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);
    """)

    # 2. Populate from existing lat/lon (NULLs stay NULL)
    op.execute("""
        UPDATE delivery_source_record
        SET geom = ST_SetSRID(ST_MakePoint(
            lon::double precision,
            lat::double precision
        ), 4326)
        WHERE lat IS NOT NULL
          AND lon IS NOT NULL
          AND geom IS NULL;
    """)

    # 3. Spatial index (CONCURRENTLY requires no transaction wrapper)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_dsr_geom_gist
        ON delivery_source_record USING GIST (geom);
    """)

    # 4. Functional indexes on category columns for regex filter pushdown
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_dsr_category_lower
        ON delivery_source_record (lower(category_raw));
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_dsr_cuisine_lower
        ON delivery_source_record (lower(cuisine_raw));
    """)

    # 5. Trigger to keep geom in sync on INSERT/UPDATE
    op.execute("""
        CREATE OR REPLACE FUNCTION trg_dsr_update_geom()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.lat IS NOT NULL AND NEW.lon IS NOT NULL THEN
                NEW.geom := ST_SetSRID(ST_MakePoint(
                    NEW.lon::double precision,
                    NEW.lat::double precision
                ), 4326);
            ELSE
                NEW.geom := NULL;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("DROP TRIGGER IF EXISTS trg_dsr_geom_sync ON delivery_source_record;")
    op.execute("""
        CREATE TRIGGER trg_dsr_geom_sync
        BEFORE INSERT OR UPDATE OF lat, lon ON delivery_source_record
        FOR EACH ROW EXECUTE FUNCTION trg_dsr_update_geom();
    """)

    # ── population_density ──

    # 1. Add geometry column
    op.execute("""
        ALTER TABLE population_density
        ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);
    """)

    # 2. Populate from existing lat/lon
    op.execute("""
        UPDATE population_density
        SET geom = ST_SetSRID(ST_MakePoint(
            lon::double precision,
            lat::double precision
        ), 4326)
        WHERE lat IS NOT NULL
          AND lon IS NOT NULL
          AND geom IS NULL;
    """)

    # 3. Spatial index
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_pop_density_geom_gist
        ON population_density USING GIST (geom);
    """)

    # 4. Trigger to keep geom in sync
    op.execute("""
        CREATE OR REPLACE FUNCTION trg_pop_density_update_geom()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.lat IS NOT NULL AND NEW.lon IS NOT NULL THEN
                NEW.geom := ST_SetSRID(ST_MakePoint(
                    NEW.lon::double precision,
                    NEW.lat::double precision
                ), 4326);
            ELSE
                NEW.geom := NULL;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("DROP TRIGGER IF EXISTS trg_pop_density_geom_sync ON population_density;")
    op.execute("""
        CREATE TRIGGER trg_pop_density_geom_sync
        BEFORE INSERT OR UPDATE OF lat, lon ON population_density
        FOR EACH ROW EXECUTE FUNCTION trg_pop_density_update_geom();
    """)


def downgrade() -> None:
    # ── population_density ──
    op.execute("DROP TRIGGER IF EXISTS trg_pop_density_geom_sync ON population_density;")
    op.execute("DROP FUNCTION IF EXISTS trg_pop_density_update_geom();")
    op.execute("DROP INDEX IF EXISTS idx_pop_density_geom_gist;")
    op.execute("ALTER TABLE population_density DROP COLUMN IF EXISTS geom;")

    # ── delivery_source_record ──
    op.execute("DROP TRIGGER IF EXISTS trg_dsr_geom_sync ON delivery_source_record;")
    op.execute("DROP FUNCTION IF EXISTS trg_dsr_update_geom();")
    op.execute("DROP INDEX IF EXISTS idx_dsr_cuisine_lower;")
    op.execute("DROP INDEX IF EXISTS idx_dsr_category_lower;")
    op.execute("DROP INDEX IF EXISTS idx_dsr_geom_gist;")
    op.execute("ALTER TABLE delivery_source_record DROP COLUMN IF EXISTS geom;")
