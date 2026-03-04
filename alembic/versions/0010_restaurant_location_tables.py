"""restaurant location finder tables: restaurant_poi, population_density, location_score

Revision ID: 0010_restaurant_location_tables
Revises: 20260202_search_index_mat
Create Date: 2026-03-04
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB


revision = "0010_restaurant_location_tables"
down_revision = "20260202_search_index_mat"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "restaurant_poi",
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column("name", sa.String(256), nullable=False),
        sa.Column("name_ar", sa.String(256)),
        sa.Column("category", sa.String(64), nullable=False),
        sa.Column("subcategory", sa.String(64)),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("lat", sa.Numeric(10, 7), nullable=False),
        sa.Column("lon", sa.Numeric(10, 7), nullable=False),
        sa.Column("rating", sa.Numeric(3, 2)),
        sa.Column("review_count", sa.Integer),
        sa.Column("price_level", sa.Integer),
        sa.Column("chain_name", sa.String(128)),
        sa.Column("district", sa.String(128)),
        sa.Column("raw", JSONB),
        sa.Column("observed_at", sa.DateTime),
    )

    # Add PostGIS geometry column (Point, SRID 4326)
    op.execute(
        "SELECT AddGeometryColumn('public', 'restaurant_poi', 'geom', 4326, 'POINT', 2);"
    )

    # GiST spatial index for efficient distance / within queries
    op.execute(
        "CREATE INDEX ix_restaurant_poi_geom_gist "
        "ON restaurant_poi USING GIST (geom);"
    )

    # Auto-populate geom from lat/lon on INSERT/UPDATE
    op.execute("""
        CREATE OR REPLACE FUNCTION restaurant_poi_set_geom()
        RETURNS trigger AS $$
        BEGIN
            NEW.geom := ST_SetSRID(ST_MakePoint(NEW.lon::float, NEW.lat::float), 4326);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER trg_restaurant_poi_geom
        BEFORE INSERT OR UPDATE OF lat, lon ON restaurant_poi
        FOR EACH ROW EXECUTE FUNCTION restaurant_poi_set_geom();
    """)

    op.create_index("ix_restaurant_poi_category", "restaurant_poi", ["category"])
    op.create_index("ix_restaurant_poi_source", "restaurant_poi", ["source"])
    op.create_index("ix_restaurant_poi_district", "restaurant_poi", ["district"])

    op.create_table(
        "population_density",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("h3_index", sa.String(16), unique=True, nullable=False),
        sa.Column("lat", sa.Numeric(10, 7)),
        sa.Column("lon", sa.Numeric(10, 7)),
        sa.Column("population", sa.Numeric(10, 1)),
        sa.Column("source", sa.String(32)),
        sa.Column("observed_at", sa.DateTime),
    )

    op.create_table(
        "location_score",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("parcel_id", sa.String(64)),
        sa.Column("h3_index", sa.String(16)),
        sa.Column("category", sa.String(64), nullable=False),
        sa.Column("overall_score", sa.Numeric(5, 2)),
        sa.Column("factors", JSONB),
        sa.Column("model_version", sa.String(32)),
        sa.Column("computed_at", sa.DateTime),
    )
    op.create_index(
        "ix_location_score_category_h3",
        "location_score",
        ["category", "h3_index"],
    )


def downgrade() -> None:
    op.drop_index("ix_location_score_category_h3", table_name="location_score")
    op.drop_table("location_score")
    op.drop_table("population_density")
    op.execute("DROP TRIGGER IF EXISTS trg_restaurant_poi_geom ON restaurant_poi;")
    op.execute("DROP FUNCTION IF EXISTS restaurant_poi_set_geom();")
    op.execute("DROP INDEX IF EXISTS ix_restaurant_poi_geom_gist;")
    op.drop_index("ix_restaurant_poi_district", table_name="restaurant_poi")
    op.drop_index("ix_restaurant_poi_source", table_name="restaurant_poi")
    op.drop_index("ix_restaurant_poi_category", table_name="restaurant_poi")
    op.drop_table("restaurant_poi")
