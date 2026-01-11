DO $$
DECLARE
    bbox_4326 geometry := ST_MakeEnvelope(46.20, 24.20, 47.30, 25.10, 4326);
    bbox_32638 geometry := ST_Transform(bbox_4326, 32638);
    cell_size_m double precision := 500.0;
    buffer_m double precision := 12.0;
    do_road_cut boolean := to_regclass('public.planet_osm_line') IS NOT NULL;
    cell_32638 geometry;
    cell_4326 geometry;
    x0 double precision;
    y0 double precision;
    cell_count int := 0;
    log_every int := 100;
    r record;
BEGIN
    TRUNCATE public.derived_parcels_v1_tbl;

    DROP TABLE IF EXISTS tmp_bldgs;
    DROP TABLE IF EXISTS tmp_cells;

    CREATE TEMP TABLE tmp_bldgs AS
    SELECT
        id,
        g32638,
        ST_PointOnSurface(g32638) AS centroid_32638
    FROM (
        SELECT
            id,
            ST_Transform(geom, 32638) AS g32638
        FROM public.ms_buildings_raw
        WHERE geom && bbox_4326
          AND ST_Intersects(geom, bbox_4326)
    ) b;

    CREATE INDEX tmp_bldgs_centroid_gix ON tmp_bldgs USING GIST (centroid_32638);

    CREATE TEMP TABLE tmp_cells AS
    SELECT DISTINCT
        floor(ST_X(centroid_32638) / cell_size_m)::bigint AS gx,
        floor(ST_Y(centroid_32638) / cell_size_m)::bigint AS gy
    FROM tmp_bldgs;

    CREATE INDEX tmp_cells_idx ON tmp_cells (gx, gy);

    FOR r IN SELECT gx, gy FROM tmp_cells ORDER BY gx, gy LOOP
        x0 := r.gx * cell_size_m;
        y0 := r.gy * cell_size_m;
        cell_32638 := ST_MakeEnvelope(x0, y0, x0 + cell_size_m, y0 + cell_size_m, 32638);
        cell_4326 := ST_Transform(cell_32638, 4326);

        IF do_road_cut THEN
            WITH bldgs AS (
                SELECT
                    id,
                    g32638,
                    centroid_32638
                FROM tmp_bldgs
                WHERE ST_Intersects(centroid_32638, cell_32638)
            ),
            cell_unions AS (
                SELECT ST_UnaryUnion(ST_Collect(ST_Buffer(g32638, buffer_m))) AS buffered_union
                FROM bldgs
            ),
            cell_sites AS (
                SELECT (ST_Dump(ST_Multi(ST_Buffer(buffered_union, -buffer_m)))).geom AS site_32638
                FROM cell_unions
                WHERE buffered_union IS NOT NULL
            ),
            site_candidates AS (
                SELECT site_32638
                FROM cell_sites
                WHERE ST_Area(site_32638) > 200
            ),
            roads_raw AS (
                SELECT ST_Transform(way, 32638) AS way_32638
                FROM public.planet_osm_line
                WHERE way && cell_4326
                  AND ST_Intersects(way, cell_4326)
            ),
            roads_32638 AS (
                SELECT COALESCE(
                    ST_UnaryUnion(ST_Collect(ST_Buffer(way_32638, 10))),
                    ST_GeomFromText('POLYGON EMPTY', 32638)
                ) AS geom
                FROM roads_raw
            ),
            cut_sites AS (
                SELECT ST_Difference(site_32638, (SELECT geom FROM roads_32638)) AS site_32638
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
            INSERT INTO public.derived_parcels_v1_tbl (
                geom,
                site_area_m2,
                footprint_area_m2,
                building_count
            )
            SELECT
                ST_Transform(site_32638, 4326)::geometry(Polygon, 4326) AS geom,
                COALESCE(ST_Area(site_32638), 0) AS site_area_m2,
                footprint_area_m2,
                building_count
            FROM metrics
            ORDER BY ST_XMin(site_32638), ST_YMin(site_32638);
        ELSE
            WITH bldgs AS (
                SELECT
                    id,
                    g32638,
                    centroid_32638
                FROM tmp_bldgs
                WHERE ST_Intersects(centroid_32638, cell_32638)
            ),
            cell_unions AS (
                SELECT ST_UnaryUnion(ST_Collect(ST_Buffer(g32638, buffer_m))) AS buffered_union
                FROM bldgs
            ),
            cell_sites AS (
                SELECT (ST_Dump(ST_Multi(ST_Buffer(buffered_union, -buffer_m)))).geom AS site_32638
                FROM cell_unions
                WHERE buffered_union IS NOT NULL
            ),
            sites AS (
                SELECT site_32638
                FROM cell_sites
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
            INSERT INTO public.derived_parcels_v1_tbl (
                geom,
                site_area_m2,
                footprint_area_m2,
                building_count
            )
            SELECT
                ST_Transform(site_32638, 4326)::geometry(Polygon, 4326) AS geom,
                COALESCE(ST_Area(site_32638), 0) AS site_area_m2,
                footprint_area_m2,
                building_count
            FROM metrics
            ORDER BY ST_XMin(site_32638), ST_YMin(site_32638);
        END IF;

        cell_count := cell_count + 1;
        IF (cell_count % log_every) = 0 THEN
            RAISE NOTICE 'processed cell gx %, gy %', r.gx, r.gy;
        END IF;
    END LOOP;

    ANALYZE public.derived_parcels_v1_tbl;
END $$;
