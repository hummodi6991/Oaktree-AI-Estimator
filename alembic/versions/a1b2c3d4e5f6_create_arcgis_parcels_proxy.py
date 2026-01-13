"""Create ArcGIS parcels proxy view."""

from alembic import op


revision = "a1b2c3d4e5f6"
down_revision = "f9c2d5e8b1a0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        -- ArcGIS parcels are stored in EPSG:4326; compute area/perimeter in UTM 38N (EPSG:32638).
        CREATE INDEX IF NOT EXISTS riyadh_parcels_arcgis_raw_geom_gix
            ON public.riyadh_parcels_arcgis_raw USING GIST (geom);

        CREATE OR REPLACE VIEW public.riyadh_parcels_arcgis_proxy AS
        SELECT
            r.fid::text AS id,
            r.parcelsub AS landuse_label,
            r.parcelsubt AS landuse_code,
            r.geom AS geom,
            ST_Area(ST_Transform(r.geom, 32638))::bigint AS area_m2,
            ST_Perimeter(ST_Transform(r.geom, 32638))::bigint AS perimeter_m
        FROM public.riyadh_parcels_arcgis_raw r
        WHERE r.geom IS NOT NULL;
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS public.riyadh_parcels_arcgis_proxy;")
    op.execute("DROP INDEX IF EXISTS riyadh_parcels_arcgis_raw_geom_gix;")
