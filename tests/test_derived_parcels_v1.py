import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


DERIVED_PARCELS_SQL_NO_ROADS = """
WITH params AS (
    SELECT
        ST_MakeEnvelope(46.20, 24.20, 47.30, 25.10, 4326) AS bbox_4326,
        500.0::double precision AS cell_size_m,
        12.0::double precision AS buffer_m
),
bldgs AS (
    SELECT
        id,
        geom,
        ST_Transform(geom, 32638) AS g32638,
        ST_Centroid(ST_Transform(geom, 32638)) AS centroid_32638
    FROM public.ms_buildings_raw
    WHERE geom && (SELECT bbox_4326 FROM params)
      AND ST_Intersects(geom, (SELECT bbox_4326 FROM params))
),
cells AS (
    SELECT
        id,
        g32638,
        centroid_32638,
        floor(ST_X(centroid_32638) / (SELECT cell_size_m FROM params))::bigint AS gx,
        floor(ST_Y(centroid_32638) / (SELECT cell_size_m FROM params))::bigint AS gy
    FROM bldgs
),
cell_unions AS (
    SELECT
        gx,
        gy,
        ST_UnaryUnion(
            ST_Collect(
                ST_Buffer(g32638, (SELECT buffer_m FROM params))
            )
        ) AS buffered_union
    FROM cells
    GROUP BY gx, gy
),
cell_sites AS (
    SELECT
        (ST_Dump(
            ST_Multi(
                ST_Buffer(buffered_union, -(SELECT buffer_m FROM params))
            )
        )).geom AS site_32638
    FROM cell_unions
),
site_candidates AS (
    SELECT site_32638
    FROM cell_sites
    WHERE ST_Area(site_32638) > 200
),
sites AS (
    SELECT site_32638
    FROM site_candidates
    WHERE ST_Area(site_32638) > 200
),
metrics AS (
    SELECT
        s.site_32638,
        COALESCE(SUM(ST_Area(b.g32638)), 0) AS footprint_area_m2,
        COALESCE(COUNT(b.id), 0)::int AS building_count
    FROM sites s
    LEFT JOIN bldgs b
        ON ST_Intersects(s.site_32638, b.centroid_32638)
    GROUP BY s.site_32638
)
SELECT
    ROW_NUMBER() OVER (ORDER BY ST_XMin(site_32638), ST_YMin(site_32638))::bigint AS parcel_id,
    ST_Transform(site_32638, 4326)::geometry(Polygon, 4326) AS geom,
    COALESCE(ST_Area(site_32638), 0) AS site_area_m2,
    footprint_area_m2,
    building_count
FROM metrics;
"""


def _database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set for derived parcels test")
    if "postgres" not in url:
        pytest.skip("DATABASE_URL is not a Postgres connection")
    return url


def test_derived_parcels_v1_refresh_or_query():
    url = _database_url()

    try:
        engine = create_engine(url)
    except SQLAlchemyError as exc:
        pytest.skip(f"Database unavailable: {exc}")

    try:
        with engine.begin() as conn:
            try:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            except SQLAlchemyError as exc:
                pytest.skip(f"PostGIS unavailable: {exc}")

            conn.execute(
                text(
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
            )

            conn.execute(
                text("DELETE FROM public.ms_buildings_raw WHERE source = 'test';")
            )
            conn.execute(
                text(
                    """
                    INSERT INTO public.ms_buildings_raw (source, source_id, geom, area_m2)
                    VALUES
                        (
                            'test',
                            'b1',
                            ST_Multi(ST_GeomFromText(
                                'POLYGON((46.70 24.70, 46.701 24.70, 46.701 24.701, 46.70 24.701, 46.70 24.70))',
                                4326
                            )),
                            0
                        ),
                        (
                            'test',
                            'b2',
                            ST_Multi(ST_GeomFromText(
                                'POLYGON((46.702 24.7005, 46.703 24.7005, 46.703 24.7015, 46.702 24.7015, 46.702 24.7005))',
                                4326
                            )),
                            0
                        ),
                        (
                            'test',
                            'b3',
                            ST_Multi(ST_GeomFromText(
                                'POLYGON((46.705 24.704, 46.706 24.704, 46.706 24.705, 46.705 24.705, 46.705 24.704))',
                                4326
                            )),
                            0
                        );
                    """
                )
            )

            has_view = conn.execute(
                text("SELECT to_regclass('public.derived_parcels_v1');")
            ).scalar()

            if has_view:
                conn.execute(
                    text("REFRESH MATERIALIZED VIEW public.derived_parcels_v1;")
                )
                rows = conn.execute(
                    text(
                        """
                        SELECT COUNT(*) AS parcel_count
                        FROM public.derived_parcels_v1
                        WHERE site_area_m2 > 0
                        """
                    )
                ).first()
                assert rows is not None
                assert rows.parcel_count >= 1
            else:
                rows = conn.execute(text(DERIVED_PARCELS_SQL_NO_ROADS)).fetchall()
                assert rows
                assert any(row.site_area_m2 > 0 for row in rows)
    finally:
        engine.dispose()
