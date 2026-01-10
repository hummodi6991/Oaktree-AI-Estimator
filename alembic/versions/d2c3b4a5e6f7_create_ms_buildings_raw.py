"""create ms_buildings_raw table"""

from alembic import op


revision = "d2c3b4a5e6f7"
down_revision = "f6b77a4b6f9c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.ms_buildings_raw (
            id bigserial PRIMARY KEY,
            source text NOT NULL DEFAULT 'microsoft_globalml',
            country text NULL,
            quadkey text NULL,
            source_id text NOT NULL,
            geom geometry(MultiPolygon,4326) NOT NULL,
            area_m2 double precision NOT NULL DEFAULT 0,
            observed_at timestamptz NOT NULL DEFAULT now()
        );
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ms_buildings_raw_geom_gix
            ON public.ms_buildings_raw USING GIST (geom);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ms_buildings_raw_source_country_idx
            ON public.ms_buildings_raw (source, country);
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ms_buildings_raw_source_id_ux
            ON public.ms_buildings_raw (source, source_id);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ms_buildings_raw_source_id_ux;")
    op.execute("DROP INDEX IF EXISTS ms_buildings_raw_source_country_idx;")
    op.execute("DROP INDEX IF EXISTS ms_buildings_raw_geom_gix;")
    op.execute("DROP TABLE IF EXISTS public.ms_buildings_raw;")
