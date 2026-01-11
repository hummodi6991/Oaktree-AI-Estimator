"""create inferred_parcels_v1 table."""

from alembic import op

revision = "7e9c1f2a3b4c"
down_revision = "3f1a2b3c4d5e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.inferred_parcels_v1 (
            parcel_id text PRIMARY KEY,
            building_id bigint NOT NULL,
            part_index int NOT NULL,
            geom geometry(Polygon,4326) NOT NULL,
            area_m2 double precision NOT NULL,
            perimeter_m double precision NOT NULL,
            footprint_area_m2 double precision NOT NULL,
            method text NOT NULL DEFAULT 'road_block_voronoi_v1',
            block_id text NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS inferred_parcels_v1_geom_gix
            ON public.inferred_parcels_v1 USING GIST (geom);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS inferred_parcels_v1_building_idx
            ON public.inferred_parcels_v1 (building_id, part_index);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS inferred_parcels_v1_building_idx;")
    op.execute("DROP INDEX IF EXISTS inferred_parcels_v1_geom_gix;")
    op.execute("DROP TABLE IF EXISTS public.inferred_parcels_v1;")
