"""suhail parcel tile ingestion tables and view"""

from alembic import op
import sqlalchemy as sa


revision = "6a8c5b0e8c3c"
down_revision = "5cafaa6377f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

    op.create_table(
        "suhail_tile_ingest_state",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("map_name", sa.Text(), server_default=sa.text("'riyadh'")),
        sa.Column("layer_name", sa.Text(), server_default=sa.text("'parcels-base'")),
        sa.Column("zoom", sa.Integer(), server_default=sa.text("15")),
        sa.Column("x_min", sa.Integer()),
        sa.Column("x_max", sa.Integer()),
        sa.Column("y_min", sa.Integer()),
        sa.Column("y_max", sa.Integer()),
        sa.Column("last_index", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("status", sa.Text()),
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS suhail_parcel_raw (
            id text PRIMARY KEY,
            geom geometry(MultiPolygon,4326) NOT NULL,
            props jsonb,
            source_layer text,
            z int,
            x int,
            y int,
            observed_at timestamptz DEFAULT now()
        );
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS suhail_parcel_raw_geom_gix
            ON suhail_parcel_raw USING GIST (geom);
        """
    )

    op.execute(
        """
        CREATE OR REPLACE VIEW suhail_parcels_proxy AS
        SELECT
            r.id,
            'suhail' AS source,
            COALESCE(
                r.props->>'landuse',
                r.props->>'land_use',
                r.props->>'use',
                r.props->>'usage',
                r.props->>'zoning'
            ) AS landuse,
            COALESCE(
                r.props->>'classification',
                r.props->>'class',
                r.props->>'type',
                'parcel'
            ) AS classification,
            ST_Area(ST_Transform(r.geom, 32638)) AS area_m2,
            ST_Transform(r.geom, 32638) AS geom
        FROM suhail_parcel_raw r;
        """
    )

    op.execute(
        """
        INSERT INTO suhail_tile_ingest_state (id, map_name, layer_name, zoom, x_min, x_max, y_min, y_max, last_index)
        VALUES (1, 'riyadh', 'parcels-base', 15, NULL, NULL, NULL, NULL, 0)
        ON CONFLICT (id) DO NOTHING;
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS suhail_parcels_proxy;")
    op.execute("DROP INDEX IF EXISTS suhail_parcel_raw_geom_gix;")
    op.execute("DROP TABLE IF EXISTS suhail_parcel_raw;")
    op.drop_table("suhail_tile_ingest_state")
