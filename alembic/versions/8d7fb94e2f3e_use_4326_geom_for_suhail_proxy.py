"""Expose 4326 geom in suhail parcels proxy view and compute metrics in 32638."""

from alembic import op


revision = "8d7fb94e2f3e"
down_revision = "f6b77a4b6f9c"
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
            r.props->>'neighborhood_name' AS neighborhood_name,
            r.props->>'zoning_category' AS zoning_category,
            r.props->>'zoning_subcategory' AS zoning_subcategory,
            r.props->>'plan_number' AS plan_number,
            r.props->>'block_number' AS block_number,
            r.props->>'parcel_number' AS parcel_number,
            r.props->>'street_name' AS street_name,
            r.props AS raw_props,
            r.geom AS geom,
            ST_Area(ST_Transform(r.geom, 32638))::bigint AS area_m2,
            ST_Perimeter(ST_Transform(r.geom, 32638))::bigint AS perimeter_m
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
