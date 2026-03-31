"""candidate_location unified table for expansion advisor

Merges Aqar commercial units, delivery restaurant locations, and
ArcGIS commercial parcels into a single scored candidate pool.

Revision ID: 0020_candidate_location
Revises: merge_exp_adv_heads_20260315
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB

revision = "0020_candidate_location"
down_revision = "merge_exp_adv_heads_20260315"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "candidate_location",
        # Primary key
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),

        # ── Source tracking ──
        sa.Column("source_tier", sa.SmallInteger, nullable=False),  # 1=aqar, 2=delivery/poi, 3=arcgis
        sa.Column("source_type", sa.String(32), nullable=False),    # 'aqar', 'hungerstation', 'restaurant_poi', 'arcgis_parcel'
        sa.Column("source_id", sa.String(256)),                     # original PK from source table

        # ── Location ──
        sa.Column("lat", sa.Numeric(10, 7), nullable=False),
        sa.Column("lon", sa.Numeric(10, 7), nullable=False),
        # geom added via raw SQL below for PostGIS

        # ── District ──
        sa.Column("district_ar", sa.String(256)),   # Arabic name (matched from external_feature)
        sa.Column("district_en", sa.String(256)),   # English name
        sa.Column("neighborhood_raw", sa.String(256)),  # original value from source

        # ── Unit attributes (known or inferred) ──
        sa.Column("area_sqm", sa.Numeric(10, 2)),
        sa.Column("rent_sar_annual", sa.Numeric(14, 2)),
        sa.Column("rent_sar_m2_month", sa.Numeric(12, 2)),
        sa.Column("rent_confidence", sa.String(24)),  # 'actual', 'comp_interpolated', 'district_median', 'city_default'
        sa.Column("area_confidence", sa.String(24)),  # 'actual', 'category_inferred', 'default'

        # ── Listing info (Tier 1 primarily) ──
        sa.Column("listing_url", sa.Text),
        sa.Column("listing_type", sa.String(32)),     # 'store', 'showroom'
        sa.Column("image_url", sa.Text),

        # ── Occupancy ──
        sa.Column("is_vacant", sa.Boolean),           # TRUE for Aqar, FALSE for delivery, NULL for arcgis
        sa.Column("current_tenant", sa.String(512)),   # brand name if occupied (Tier 2)
        sa.Column("current_category", sa.String(64)),  # cuisine category if occupied

        # ── Quality signals ──
        sa.Column("street_width_m", sa.Numeric(8, 2)),
        sa.Column("has_drive_thru", sa.Boolean),
        sa.Column("road_class", sa.String(32)),
        sa.Column("landuse_code", sa.Integer),
        sa.Column("landuse_label", sa.String(64)),

        # ── Delivery context (Tier 2 enrichment) ──
        sa.Column("platform_count", sa.SmallInteger),      # how many delivery platforms list this location
        sa.Column("avg_rating", sa.Numeric(3, 2)),
        sa.Column("total_rating_count", sa.Integer),
        sa.Column("supports_late_night", sa.Boolean),

        # ── Dedup / clustering ──
        sa.Column("cluster_id", sa.Integer),
        sa.Column("is_cluster_primary", sa.Boolean, server_default=sa.text("TRUE")),

        # ── Metadata ──
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("population_run_id", sa.String(64)),  # tracks which ingest run created this row
    )

    # PostGIS geometry column
    op.execute("""
        ALTER TABLE candidate_location
        ADD COLUMN geom geometry(Point, 4326);
    """)

    # Auto-populate geom from lat/lon
    op.execute("""
        CREATE OR REPLACE FUNCTION trg_candidate_location_geom()
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
    op.execute("""
        CREATE TRIGGER trg_cl_geom_sync
        BEFORE INSERT OR UPDATE OF lat, lon ON candidate_location
        FOR EACH ROW EXECUTE FUNCTION trg_candidate_location_geom();
    """)

    # Indexes
    op.execute("CREATE INDEX ix_cl_geom_gist ON candidate_location USING GIST (geom);")
    op.create_index("ix_cl_source_tier", "candidate_location", ["source_tier"])
    op.create_index("ix_cl_source_type_id", "candidate_location", ["source_type", "source_id"])
    op.create_index("ix_cl_district_ar", "candidate_location", ["district_ar"])
    op.create_index("ix_cl_is_vacant", "candidate_location", ["is_vacant"])
    op.create_index("ix_cl_cluster_primary", "candidate_location", ["is_cluster_primary"])
    op.create_index("ix_cl_current_category", "candidate_location", ["current_category"])
    op.create_index("ix_cl_rent_confidence", "candidate_location", ["rent_confidence"])


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_cl_geom_sync ON candidate_location;")
    op.execute("DROP FUNCTION IF EXISTS trg_candidate_location_geom();")
    op.drop_index("ix_cl_rent_confidence", table_name="candidate_location")
    op.drop_index("ix_cl_current_category", table_name="candidate_location")
    op.drop_index("ix_cl_cluster_primary", table_name="candidate_location")
    op.drop_index("ix_cl_is_vacant", table_name="candidate_location")
    op.drop_index("ix_cl_district_ar", table_name="candidate_location")
    op.drop_index("ix_cl_source_type_id", table_name="candidate_location")
    op.drop_index("ix_cl_source_tier", table_name="candidate_location")
    op.execute("DROP INDEX IF EXISTS ix_cl_geom_gist;")
    op.drop_table("candidate_location")
