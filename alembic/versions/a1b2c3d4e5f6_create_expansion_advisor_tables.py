"""create expansion advisor normalized tables

Revision ID: a1b2c3d4e5f6
Revises: f9c2d5e8b1a0
Create Date: 2026-03-15 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "f9c2d5e8b1a0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── 1. expansion_road_context ──
    op.execute("""
        CREATE TABLE IF NOT EXISTS expansion_road_context (
            id              SERIAL PRIMARY KEY,
            city            VARCHAR(64) NOT NULL DEFAULT 'riyadh',
            source          VARCHAR(64) NOT NULL DEFAULT 'osm',
            parcel_id       VARCHAR(64),
            geom            geometry(Geometry, 4326),
            road_class      VARCHAR(32),
            is_major_road   BOOLEAN DEFAULT FALSE,
            is_service_road BOOLEAN DEFAULT FALSE,
            intersection_distance_m     DOUBLE PRECISION,
            major_road_distance_m       DOUBLE PRECISION,
            adjacent_road_count         INTEGER DEFAULT 0,
            touches_road                BOOLEAN DEFAULT FALSE,
            corner_lot                  BOOLEAN DEFAULT FALSE,
            frontage_length_m           DOUBLE PRECISION,
            uturn_access_proxy          VARCHAR(32),
            signalized_junction_distance_m  DOUBLE PRECISION,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_erc_geom ON expansion_road_context USING gist (geom)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_erc_city ON expansion_road_context USING btree (city)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_erc_parcel_id ON expansion_road_context USING btree (parcel_id)")

    # ── 2. expansion_parking_asset ──
    op.execute("""
        CREATE TABLE IF NOT EXISTS expansion_parking_asset (
            id              SERIAL PRIMARY KEY,
            city            VARCHAR(64) NOT NULL DEFAULT 'riyadh',
            source          VARCHAR(64) NOT NULL DEFAULT 'osm',
            name            VARCHAR(256),
            amenity_type    VARCHAR(64),
            geom            geometry(Geometry, 4326),
            capacity        INTEGER,
            covered         BOOLEAN,
            public_access   BOOLEAN,
            walk_access_score   DOUBLE PRECISION,
            dropoff_score       DOUBLE PRECISION,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_epa_geom ON expansion_parking_asset USING gist (geom)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_epa_city ON expansion_parking_asset USING btree (city)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_epa_amenity_type ON expansion_parking_asset USING btree (amenity_type)")

    # ── 3. expansion_delivery_market ──
    op.execute("""
        CREATE TABLE IF NOT EXISTS expansion_delivery_market (
            id                      SERIAL PRIMARY KEY,
            city                    VARCHAR(64) NOT NULL DEFAULT 'riyadh',
            platform                VARCHAR(32) NOT NULL,
            branch_name             VARCHAR(256),
            brand_name              VARCHAR(256),
            category                VARCHAR(64),
            geom                    geometry(Point, 4326),
            district                VARCHAR(128),
            rating                  NUMERIC(3,2),
            rating_count            INTEGER,
            min_order_sar           NUMERIC(8,2),
            delivery_fee_sar        NUMERIC(8,2),
            eta_minutes             INTEGER,
            is_open_now             BOOLEAN,
            supports_late_night     BOOLEAN,
            source_record_id        INTEGER,
            resolved_restaurant_poi_id  VARCHAR(128),
            scraped_at              TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_edm_geom ON expansion_delivery_market USING gist (geom)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_edm_city ON expansion_delivery_market USING btree (city)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_edm_platform ON expansion_delivery_market USING btree (platform)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_edm_resolved_poi ON expansion_delivery_market USING btree (resolved_restaurant_poi_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_edm_district ON expansion_delivery_market USING btree (district)")

    # ── 4. expansion_rent_comp ──
    op.execute("""
        CREATE TABLE IF NOT EXISTS expansion_rent_comp (
            id                  SERIAL PRIMARY KEY,
            city                VARCHAR(64) NOT NULL DEFAULT 'riyadh',
            district            VARCHAR(128),
            source              VARCHAR(64) NOT NULL DEFAULT 'aqar',
            listing_id          VARCHAR(128),
            asset_type          VARCHAR(32) NOT NULL DEFAULT 'commercial',
            unit_type           VARCHAR(32),
            geom                geometry(Point, 4326),
            area_m2             DOUBLE PRECISION,
            annual_rent_sar     DOUBLE PRECISION,
            monthly_rent_sar    DOUBLE PRECISION,
            rent_sar_m2_year    DOUBLE PRECISION,
            frontage_class      VARCHAR(32),
            road_class          VARCHAR(32),
            floor_level         VARCHAR(32),
            shell_condition     VARCHAR(32),
            vacancy_days        INTEGER,
            listed_at           DATE,
            ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_erentc_city ON expansion_rent_comp USING btree (city)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_erentc_district ON expansion_rent_comp USING btree (district)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_erentc_asset_type ON expansion_rent_comp USING btree (asset_type)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_erentc_unit_type ON expansion_rent_comp USING btree (unit_type)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_erentc_geom ON expansion_rent_comp USING gist (geom)")

    # ── 5. expansion_competitor_quality ──
    op.execute("""
        CREATE TABLE IF NOT EXISTS expansion_competitor_quality (
            id                      SERIAL PRIMARY KEY,
            city                    VARCHAR(64) NOT NULL DEFAULT 'riyadh',
            restaurant_poi_id       VARCHAR(128),
            brand_name              VARCHAR(256),
            category                VARCHAR(64),
            district                VARCHAR(128),
            geom                    geometry(Point, 4326),
            chain_strength_score    DOUBLE PRECISION,
            review_score            DOUBLE PRECISION,
            review_count            INTEGER,
            delivery_presence_score DOUBLE PRECISION,
            multi_platform_score    DOUBLE PRECISION,
            late_night_score        DOUBLE PRECISION,
            price_tier              VARCHAR(16),
            overall_quality_score   DOUBLE PRECISION,
            refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_ecq_geom ON expansion_competitor_quality USING gist (geom)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_ecq_city ON expansion_competitor_quality USING btree (city)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_ecq_restaurant_poi_id ON expansion_competitor_quality USING btree (restaurant_poi_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_ecq_category ON expansion_competitor_quality USING btree (category)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_ecq_district ON expansion_competitor_quality USING btree (district)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS expansion_competitor_quality CASCADE")
    op.execute("DROP TABLE IF EXISTS expansion_rent_comp CASCADE")
    op.execute("DROP TABLE IF EXISTS expansion_delivery_market CASCADE")
    op.execute("DROP TABLE IF EXISTS expansion_parking_asset CASCADE")
    op.execute("DROP TABLE IF EXISTS expansion_road_context CASCADE")
