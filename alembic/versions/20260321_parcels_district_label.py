"""Add pre-materialized district_label to riyadh_parcels_arcgis_raw

Revision ID: 20260321_parcels_district_label
Revises: merge_exp_adv_heads_20260315
Create Date: 2026-03-21
"""
from alembic import op

revision = "20260321_parcels_district_label"
down_revision = "merge_exp_adv_heads_20260315"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Add nullable district_label column to the base table
    op.execute("""
        ALTER TABLE public.riyadh_parcels_arcgis_raw
        ADD COLUMN IF NOT EXISTS district_label VARCHAR(256);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_parcels_arcgis_district_label
        ON public.riyadh_parcels_arcgis_raw (district_label);
    """)

    # 2. Recreate the proxy view to expose the new column
    op.execute("DROP VIEW IF EXISTS public.riyadh_parcels_arcgis_proxy;")
    op.execute("""
        CREATE OR REPLACE VIEW public.riyadh_parcels_arcgis_proxy AS
        SELECT
            r.fid::text AS id,
            r.parcelsub AS landuse_label,
            r.parcelsubt AS landuse_code,
            r.geom AS geom,
            ST_Area(ST_Transform(r.geom, 32638))::bigint AS area_m2,
            ST_Perimeter(ST_Transform(r.geom, 32638))::bigint AS perimeter_m,
            r.district_label
        FROM public.riyadh_parcels_arcgis_raw r
        WHERE r.geom IS NOT NULL;
    """)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS public.riyadh_parcels_arcgis_proxy;")
    op.execute("""
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
    """)
    op.execute("ALTER TABLE public.riyadh_parcels_arcgis_raw DROP COLUMN IF EXISTS district_label;")
    op.execute("DROP INDEX IF EXISTS ix_parcels_arcgis_district_label;")
