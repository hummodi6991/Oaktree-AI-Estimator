"""recreate derived_parcels_v1 mat view with no data"""

from alembic import op


revision = "f2a1b3c4d5e6"
down_revision = "e3f4a5b6c7d8"
branch_labels = None
depends_on = None


def _derived_parcels_sql(include_roads: bool) -> str:
    do_road_cut = "true" if include_roads else "false"
    base_ctes = f"""
    WITH params AS (
        SELECT
            {do_road_cut}::boolean AS do_road_cut,
            ST_MakeEnvelope(46.20, 24.20, 47.30, 25.10, 4326) AS bbox_4326,
            ST_Transform(ST_MakeEnvelope(46.20, 24.20, 47.30, 25.10, 4326), 32638) AS bbox_32638,
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
    )
    """

    if include_roads:
        roads_ctes = """,
    roads_raw AS (
        SELECT ST_Transform(way, 32638) AS way_32638
        FROM public.planet_osm_line
        WHERE way && (SELECT bbox_4326 FROM params)
    ),
    roads_32638 AS (
        SELECT COALESCE(
            ST_UnaryUnion(
                ST_Collect(
                    ST_Buffer(way_32638, 10)
                )
            ),
            ST_GeomFromText('POLYGON EMPTY', 32638)
        ) AS geom
        FROM roads_raw
    ),
    cut_sites AS (
        SELECT CASE
            WHEN (SELECT do_road_cut FROM params)
                THEN ST_Difference(site_32638, (SELECT geom FROM roads_32638))
            ELSE site_32638
        END AS site_32638
        FROM site_candidates
    ),
    cut_polys AS (
        SELECT (ST_Dump(ST_Multi(site_32638))).geom AS site_32638
        FROM cut_sites
    ),
    sites AS (
        SELECT site_32638
        FROM cut_polys
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
    FROM metrics
    """
    else:
        roads_ctes = """,
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
    FROM metrics
    """

    return (
        """
    CREATE MATERIALIZED VIEW public.derived_parcels_v1 (
        parcel_id,
        geom,
        site_area_m2,
        footprint_area_m2,
        building_count
    ) AS
    """
        + base_ctes
        + roads_ctes
        + " WITH NO DATA;"
    )


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

    sql_with_roads = _derived_parcels_sql(include_roads=True)
    sql_no_roads = _derived_parcels_sql(include_roads=False)

    op.execute("DROP MATERIALIZED VIEW IF EXISTS public.derived_parcels_v1;")

    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.planet_osm_line') IS NOT NULL THEN
                EXECUTE $mv$%s$mv$;
            ELSE
                EXECUTE $mv$%s$mv$;
            END IF;
        END $$;
        """
        % (sql_with_roads, sql_no_roads)
    )

    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS derived_parcels_v1_parcel_id_uq
            ON public.derived_parcels_v1 (parcel_id);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS derived_parcels_v1_geom_gix
            ON public.derived_parcels_v1 USING GIST (geom);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS derived_parcels_v1_geom_gix;")
    op.execute("DROP INDEX IF EXISTS derived_parcels_v1_parcel_id_uq;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS public.derived_parcels_v1;")
