"""Standardize Suhail parcel proxy/materialized views."""

from alembic import op


revision = "c1f4a2b3c4d5"
down_revision = "b7e2c8f6d1a2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # IMPORTANT: suhail_parcels_mat depends on suhail_parcels_proxy.
    # Drop the materialized view first, then the proxy view, then recreate both.
    op.execute(
        """
        DROP MATERIALIZED VIEW IF EXISTS public.suhail_parcels_mat;
        """
    )
    op.execute(
        """
        DROP VIEW IF EXISTS public.suhail_parcels_proxy;
        CREATE VIEW public.suhail_parcels_proxy AS
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
            ST_SetSRID(r.geom, 4326) AS geom,
            ST_Area(ST_Transform(ST_SetSRID(r.geom, 4326), 32638))::bigint AS area_m2,
            ST_Perimeter(ST_Transform(ST_SetSRID(r.geom, 4326), 32638))::bigint AS perimeter_m
        FROM public.suhail_parcel_raw r;
        """
    )
    op.execute(
        """
        CREATE MATERIALIZED VIEW public.suhail_parcels_mat AS
        WITH base AS (
            SELECT
                p.id,
                p.source,
                p.landuse,
                p.classification,
                p.zoning_id,
                p.municipality_name,
                p.neighborhood_name,
                p.zoning_category,
                p.zoning_subcategory,
                p.plan_number,
                p.block_number,
                p.parcel_number,
                p.street_name,
                p.raw_props,
                p.geom,
                ST_Transform(p.geom, 32638) AS geom_32638
            FROM public.suhail_parcels_proxy p
        )
        SELECT
            id,
            source,
            landuse,
            classification,
            zoning_id,
            municipality_name,
            neighborhood_name,
            zoning_category,
            zoning_subcategory,
            plan_number,
            block_number,
            parcel_number,
            street_name,
            raw_props,
            geom,
            geom_32638,
            ST_Area(geom_32638)::bigint AS area_m2,
            ST_Perimeter(geom_32638)::bigint AS perimeter_m
        FROM base;
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS suhail_parcels_mat_geom_gix
            ON public.suhail_parcels_mat USING GIST (geom);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS suhail_parcels_mat_geom_32638_gix
            ON public.suhail_parcels_mat USING GIST (geom_32638);
        """
    )
    op.execute("ANALYZE public.suhail_parcels_mat;")


def downgrade() -> None:
    op.execute(
        """
        DROP MATERIALIZED VIEW IF EXISTS public.suhail_parcels_mat;
        CREATE MATERIALIZED VIEW public.suhail_parcels_mat AS
        SELECT
            *,
            ST_Transform(geom, 32638) AS geom_32638
        FROM public.suhail_parcels_proxy;
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS suhail_parcels_mat_geom_gix
            ON public.suhail_parcels_mat USING GIST (geom);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS suhail_parcels_mat_geom_32638_gix
            ON public.suhail_parcels_mat USING GIST (geom_32638);
        """
    )
    op.execute("ANALYZE public.suhail_parcels_mat;")
    op.execute(
        """
        DROP VIEW IF EXISTS public.suhail_parcels_proxy;
        CREATE VIEW public.suhail_parcels_proxy AS
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
        FROM public.suhail_parcel_raw r;
        """
    )
