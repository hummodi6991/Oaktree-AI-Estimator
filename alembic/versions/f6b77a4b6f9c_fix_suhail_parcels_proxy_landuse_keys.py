"""Adjust landuse keys in suhail parcels proxy view"""

from alembic import op


revision = "f6b77a4b6f9c"
down_revision = "0a947218f1f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DROP VIEW IF EXISTS suhail_parcels_proxy;
        CREATE VIEW suhail_parcels_proxy AS
        SELECT
            r.id,
            'suhail' AS source,
            COALESCE(
                r.props->>'landuseagroup',
                r.props->>'landuseadetailed',
                r.props->>'landusegroup',
                r.props->>'landusedetailed',
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
            r.props->>'zoning_id' AS zoning_id,
            COALESCE(
                r.props->>'municipality_aname',
                r.props->>'municipality_name',
                r.props->>'municipality'
            ) AS municipality_name,
            ST_Area(ST_Transform(r.geom, 32638)) AS area_m2,
            ST_Transform(r.geom, 32638) AS geom
        FROM suhail_parcel_raw r;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP VIEW IF EXISTS suhail_parcels_proxy;
        CREATE VIEW suhail_parcels_proxy AS
        SELECT
            r.id,
            'suhail' AS source,
            COALESCE(
                r.props->>'landusegroup',
                r.props->>'landusedetailed',
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
            r.props->>'zoning_id' AS zoning_id,
            COALESCE(
                r.props->>'municipality_name',
                r.props->>'municipality'
            ) AS municipality_name,
            ST_Area(ST_Transform(r.geom, 32638)) AS area_m2,
            ST_Transform(r.geom, 32638) AS geom
        FROM suhail_parcel_raw r;
        """
    )
