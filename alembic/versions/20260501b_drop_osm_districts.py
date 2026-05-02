"""Drop osm_districts layer; Aqar becomes single source for district polygons.

The osm_districts layer was contaminated with non-Riyadh data via the
ingest at app/ingest/osm_district_polygons.py — most rows resolved to
Romanian neighborhood polygons (Reghin, Transylvania) that envelope-
intersected the Riyadh bbox. Code-side, the OSM-first preference was
already inconsistent across the codebase (4 sites OSM-first, 3 Aqar-first,
1 no priority), with one site carrying an explicit "OSM leakage" workaround
comment.

Decision: drop OSM entirely. Aqar district hulls (146 polygons, scoped to
Riyadh by construction via aqar.listings filtering) become the single
source of truth for district polygons.

This migration:
1. Deletes all rows from external_feature where layer_name='osm_districts'.
2. Rebuilds external_feature_polygons_mat without OSM in the WHERE clause
   (the matview becomes Aqar-only by definition).

Rollback: re-ingest from OSM is NOT a clean recovery path — the upstream
leakage in app/ingest/osm_district_polygons.py is unaddressed at the time
of this migration. Recovery requires (a) fixing the upstream OSM ingest's
filter to actually exclude non-Riyadh polygons, then (b) re-running it.
The downgrade() here only restores the matview's dual-layer definition;
it does not restore OSM rows.

revision: 20260501b_drop_osm_districts
"""
from typing import Sequence, Union

from alembic import op


revision: str = "20260501b_drop_osm_districts"
down_revision: Union[str, Sequence[str], None] = "20260501a_ext_feat_polygons_mat"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Delete OSM rows from external_feature.
    op.execute("DELETE FROM external_feature WHERE layer_name = 'osm_districts'")

    # 2. Rebuild the matview without OSM in the WHERE clause.
    op.execute("DROP MATERIALIZED VIEW IF EXISTS external_feature_polygons_mat")

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
        WHERE ef.layer_name = 'aqar_district_hulls'
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

    op.execute(
        """
        CREATE UNIQUE INDEX ux_external_feature_polygons_mat_feature_id
            ON external_feature_polygons_mat (feature_id);
        """
    )

    op.execute(
        """
        CREATE INDEX ix_external_feature_polygons_mat_geom
            ON external_feature_polygons_mat USING GIST (geom);
        """
    )

    op.execute(
        """
        CREATE INDEX ix_external_feature_polygons_mat_layer_name
            ON external_feature_polygons_mat (layer_name);
        """
    )


def downgrade() -> None:
    # Restore the matview's dual-layer definition. Does NOT restore OSM
    # rows in external_feature; that requires re-running OSM ingest with
    # a fixed upstream filter (see migration docstring).
    op.execute("DROP MATERIALIZED VIEW IF EXISTS external_feature_polygons_mat")

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

    op.execute(
        """
        CREATE UNIQUE INDEX ux_external_feature_polygons_mat_feature_id
            ON external_feature_polygons_mat (feature_id);
        """
    )

    op.execute(
        """
        CREATE INDEX ix_external_feature_polygons_mat_geom
            ON external_feature_polygons_mat USING GIST (geom);
        """
    )

    op.execute(
        """
        CREATE INDEX ix_external_feature_polygons_mat_layer_name
            ON external_feature_polygons_mat (layer_name);
        """
    )
