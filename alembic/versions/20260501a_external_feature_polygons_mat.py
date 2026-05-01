"""Create external_feature_polygons_mat materialized view.

Pre-parses district polygons (osm_districts + aqar_district_hulls) from
external_feature.geometry JSONB into a GIST-indexed PostGIS geometry, so
spatial joins don't have to re-parse on every read.

Polygons effectively never change; refresh is on-demand via the
"Refresh external_feature_polygons_mat" GitHub Actions workflow.

revision: 20260501a_ext_feat_polygons_mat
"""
from typing import Sequence, Union

from alembic import op


revision: str = "20260501a_ext_feat_polygons_mat"
down_revision: Union[str, Sequence[str], None] = ("20260501_district_radiance_monthly", "20260426_ecq_canonical_cols")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE MATERIALIZED VIEW external_feature_polygons_mat AS
        SELECT
            ef.id                                                         AS feature_id,
            ef.layer_name,
            TRIM(COALESCE(ef.properties->>'district_raw',
                          ef.properties->>'district'))                    AS district_label,
            ST_SetSRID(ST_GeomFromGeoJSON(ef.geometry::text), 4326)       AS geom
        FROM external_feature ef
        WHERE ef.layer_name IN ('osm_districts', 'aqar_district_hulls')
          AND ef.geometry IS NOT NULL
          AND jsonb_typeof(ef.geometry) = 'object'
          AND ef.geometry ? 'type'
          AND ef.geometry ? 'coordinates'
          AND ef.geometry->>'type' IN ('Polygon', 'MultiPolygon')
          AND COALESCE(ef.properties->>'district_raw',
                       ef.properties->>'district') IS NOT NULL
          AND TRIM(COALESCE(ef.properties->>'district_raw',
                            ef.properties->>'district')) <> ''
        WITH DATA;
        """
    )

    # UNIQUE on feature_id is required for REFRESH MATERIALIZED VIEW CONCURRENTLY.
    op.execute(
        """
        CREATE UNIQUE INDEX ux_external_feature_polygons_mat_feature_id
            ON external_feature_polygons_mat (feature_id);
        """
    )

    # GIST on the parsed geometry — used by ST_Contains in
    # _district_momentum_score and any future district-keyed spatial join.
    op.execute(
        """
        CREATE INDEX ix_external_feature_polygons_mat_geom
            ON external_feature_polygons_mat USING GIST (geom);
        """
    )

    # btree on layer_name for the OSM-first DISTINCT ON pattern in consumers.
    op.execute(
        """
        CREATE INDEX ix_external_feature_polygons_mat_layer_name
            ON external_feature_polygons_mat (layer_name);
        """
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS external_feature_polygons_mat;")
