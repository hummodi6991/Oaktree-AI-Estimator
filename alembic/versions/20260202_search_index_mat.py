"""Create unified materialized search index (search_index_mat)

Revision ID: 20260202_search_index_mat
Revises: 20260201_search_trgm_indexes
Create Date: 2026-02-02
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "20260202_search_index_mat"
down_revision = "20260201_search_trgm_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Extensions used by indexes + token ranking
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

    # Build a unified materialized view to avoid juggling many SQL templates.
    # NOTE: all geometries are stored in EPSG:4326.
    op.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS public.search_index_mat AS
        WITH
        -- ---- OSM POIs (points) ----
        poi_point AS (
          SELECT
            'poi'::text AS type,
            'osm_point'::text AS source,
            ('osm_point:' || osm_id)::text AS id,
            COALESCE(
              NULLIF(name,''),
              NULLIF(amenity,''),
              NULLIF(shop,''),
              NULLIF(tourism,''),
              NULLIF(leisure,''),
              NULLIF(office,''),
              NULLIF(building,''),
              NULLIF(landuse,''),
              NULLIF(man_made,''),
              NULLIF(sport,''),
              NULLIF(historic,''),
              'POI'
            )::text AS label,
            lower(COALESCE(name,''))::text AS label_norm,
            ARRAY_REMOVE(ARRAY[
              NULLIF(lower(COALESCE(amenity,'')), ''),
              NULLIF(lower(COALESCE(shop,'')), ''),
              NULLIF(lower(COALESCE(tourism,'')), ''),
              NULLIF(lower(COALESCE(leisure,'')), ''),
              NULLIF(lower(COALESCE(office,'')), ''),
              NULLIF(lower(COALESCE(building,'')), ''),
              NULLIF(lower(COALESCE(landuse,'')), ''),
              NULLIF(lower(COALESCE(man_made,'')), ''),
              NULLIF(lower(COALESCE(sport,'')), ''),
              NULLIF(lower(COALESCE(historic,'')), '')
            ], NULL)::text[] AS alt_labels,
            COALESCE(amenity, shop, tourism, leisure, office, building, landuse, man_made, sport, historic)::text AS subtitle,
            ST_Transform(way, 4326) AS geom,
            ST_PointOnSurface(ST_Transform(way, 4326)) AS center,
            ST_Envelope(ST_Transform(way, 4326)) AS bbox,
            0.0::double precision AS popularity
          FROM public.planet_osm_point
          WHERE way IS NOT NULL
            AND (
              name IS NOT NULL OR amenity IS NOT NULL OR shop IS NOT NULL OR tourism IS NOT NULL
              OR leisure IS NOT NULL OR office IS NOT NULL OR building IS NOT NULL OR landuse IS NOT NULL
              OR man_made IS NOT NULL OR sport IS NOT NULL OR historic IS NOT NULL
            )
        ),
        -- ---- OSM POIs (polygons) ----
        poi_polygon AS (
          SELECT
            'poi'::text AS type,
            'osm_polygon'::text AS source,
            ('osm_polygon:' || osm_id)::text AS id,
            COALESCE(
              NULLIF(name,''),
              NULLIF(amenity,''),
              NULLIF(shop,''),
              NULLIF(tourism,''),
              NULLIF(leisure,''),
              NULLIF(office,''),
              NULLIF(building,''),
              NULLIF(landuse,''),
              NULLIF(man_made,''),
              NULLIF(sport,''),
              NULLIF(historic,''),
              'POI'
            )::text AS label,
            lower(COALESCE(name,''))::text AS label_norm,
            ARRAY_REMOVE(ARRAY[
              NULLIF(lower(COALESCE(amenity,'')), ''),
              NULLIF(lower(COALESCE(shop,'')), ''),
              NULLIF(lower(COALESCE(tourism,'')), ''),
              NULLIF(lower(COALESCE(leisure,'')), ''),
              NULLIF(lower(COALESCE(office,'')), ''),
              NULLIF(lower(COALESCE(building,'')), ''),
              NULLIF(lower(COALESCE(landuse,'')), ''),
              NULLIF(lower(COALESCE(man_made,'')), ''),
              NULLIF(lower(COALESCE(sport,'')), ''),
              NULLIF(lower(COALESCE(historic,'')), '')
            ], NULL)::text[] AS alt_labels,
            COALESCE(amenity, shop, tourism, leisure, office, building, landuse, man_made, sport, historic)::text AS subtitle,
            ST_Transform(way, 4326) AS geom,
            ST_PointOnSurface(ST_Transform(way, 4326)) AS center,
            ST_Envelope(ST_Transform(way, 4326)) AS bbox,
            0.0::double precision AS popularity
          FROM public.planet_osm_polygon
          WHERE way IS NOT NULL
            AND (
              name IS NOT NULL OR amenity IS NOT NULL OR shop IS NOT NULL OR tourism IS NOT NULL
              OR leisure IS NOT NULL OR office IS NOT NULL OR building IS NOT NULL OR landuse IS NOT NULL
              OR man_made IS NOT NULL OR sport IS NOT NULL OR historic IS NOT NULL
            )
        ),
        -- ---- OSM Roads (lines) ----
        road_line AS (
          SELECT
            'road'::text AS type,
            'osm_line'::text AS source,
            ('osm_line:' || osm_id)::text AS id,
            COALESCE(NULLIF(name,''), NULLIF(ref,''), 'Road')::text AS label,
            lower(COALESCE(name, ref, ''))::text AS label_norm,
            ARRAY_REMOVE(ARRAY[
              NULLIF(lower(COALESCE(ref,'')), ''),
              NULLIF(lower(COALESCE(tags->'name:ar','')), ''),
              NULLIF(lower(COALESCE(tags->'name:en','')), ''),
              NULLIF(lower(COALESCE(tags->'alt_name','')), '')
            ], NULL)::text[] AS alt_labels,
            COALESCE(highway, 'road')::text AS subtitle,
            ST_Transform(way, 4326) AS geom,
            ST_PointOnSurface(ST_Transform(way, 4326)) AS center,
            ST_Envelope(ST_Transform(way, 4326)) AS bbox,
            0.0::double precision AS popularity
          FROM public.planet_osm_line
          WHERE way IS NOT NULL AND highway IS NOT NULL
        ),
        -- ---- Districts (external_feature; both OSM + Aqar hulls) ----
        district_ext AS (
          SELECT
            'district'::text AS type,
            COALESCE(layer_name, 'external')::text AS source,
            ('district:' || COALESCE(layer_name, 'external') || ':' || id)::text AS id,
            COALESCE(properties->>'district_raw', properties->>'name', properties->>'district', 'District')::text AS label,
            lower(COALESCE(properties->>'district_raw', properties->>'name', properties->>'district', ''))::text AS label_norm,
            ARRAY_REMOVE(ARRAY[
              NULLIF(lower(COALESCE(properties->>'district_en','')), ''),
              NULLIF(lower(COALESCE(properties->>'name_en','')), ''),
              NULLIF(lower(COALESCE(properties->>'district_ar','')), ''),
              NULLIF(lower(COALESCE(properties->>'name_ar','')), '')
            ], NULL)::text[] AS alt_labels,
            ('District • ' || COALESCE(layer_name, 'external'))::text AS subtitle,
            ST_SetSRID(ST_GeomFromGeoJSON(geometry::text), 4326) AS geom,
            ST_PointOnSurface(ST_SetSRID(ST_GeomFromGeoJSON(geometry::text), 4326)) AS center,
            ST_Envelope(ST_SetSRID(ST_GeomFromGeoJSON(geometry::text), 4326)) AS bbox,
            COALESCE(
              NULLIF(properties->>'point_count','')::double precision,
              NULLIF(properties->>'points_count','')::double precision,
              NULLIF(properties->>'n_points','')::double precision,
              NULLIF(properties->>'count','')::double precision,
              0.0
            ) AS popularity
          FROM public.external_feature
          WHERE layer_name IN ('osm_districts', 'aqar_district_hulls')
            AND geometry IS NOT NULL
        ),
        -- ---- Parcels (Suhail mat; can be extended later) ----
        parcel_suhail AS (
          SELECT
            'parcel'::text AS type,
            'suhail'::text AS source,
            ('suhail:' || p.id::text)::text AS id,
            COALESCE(NULLIF(street_name,''), NULLIF(neighborhood_name,''), NULLIF(municipality_name,''), 'Parcel')::text AS label,
            lower(COALESCE(street_name, neighborhood_name, municipality_name, ''))::text AS label_norm,
            ARRAY_REMOVE(ARRAY[
              NULLIF(lower(COALESCE(neighborhood_name,'')), ''),
              NULLIF(lower(COALESCE(municipality_name,'')), ''),
              NULLIF(lower(COALESCE(plan_number::text,'')), ''),
              NULLIF(lower(COALESCE(block_number::text,'')), ''),
              NULLIF(lower(COALESCE(parcel_number::text,'')), ''),
              NULLIF(lower(COALESCE(zoning_id,'')), ''),
              NULLIF(lower(COALESCE(zoning_category,'')), ''),
              NULLIF(lower(COALESCE(zoning_subcategory,'')), ''),
              NULLIF(lower(COALESCE(landuse,'')), ''),
              NULLIF(lower(COALESCE(classification,'')), '')
            ], NULL)::text[] AS alt_labels,
            concat_ws(' • ',
              NULLIF(neighborhood_name,''),
              NULLIF(municipality_name,''),
              CASE WHEN plan_number IS NOT NULL THEN ('Plan ' || plan_number::text) END,
              CASE WHEN block_number IS NOT NULL THEN ('Block ' || block_number::text) END,
              CASE WHEN parcel_number IS NOT NULL THEN ('Parcel ' || parcel_number::text) END,
              'Source: Suhail'
            )::text AS subtitle,
            p.geom_4326 AS geom,
            ST_PointOnSurface(p.geom_4326) AS center,
            ST_Envelope(p.geom_4326) AS bbox,
            0.0::double precision AS popularity
          FROM public.suhail_parcels_mat p
          WHERE p.geom_4326 IS NOT NULL
        )
        SELECT
          u.*,
          COALESCE(array_to_string(u.alt_labels, ' '), '')::text AS alt_text,
          to_tsvector('simple', COALESCE(u.label_norm,'') || ' ' || COALESCE(array_to_string(u.alt_labels, ' '), '')) AS tsv
        FROM (
          SELECT * FROM poi_point
          UNION ALL SELECT * FROM poi_polygon
          UNION ALL SELECT * FROM road_line
          UNION ALL SELECT * FROM district_ext
          UNION ALL SELECT * FROM parcel_suhail
        ) u
        ;
        """
    )

    # Ensure it is populated/up-to-date on first install/upgrade.
    op.execute("REFRESH MATERIALIZED VIEW public.search_index_mat;")

    # Indexes (CONCURRENTLY requires autocommit)
    with op.get_context().autocommit_block():
        bind = op.get_bind()

        def ensure_index(name: str, ddl: str) -> None:
            exists = bind.execute(text("SELECT to_regclass(:n)"), {"n": name}).scalar()
            if exists is None:
                op.execute(ddl)

        # Required for REFRESH MATERIALIZED VIEW CONCURRENTLY
        ensure_index(
            "public.ux_search_index_mat_id",
            "CREATE UNIQUE INDEX CONCURRENTLY ux_search_index_mat_id ON public.search_index_mat (id);",
        )
        # Spatial filter
        ensure_index(
            "public.ix_search_index_mat_geom_gist",
            "CREATE INDEX CONCURRENTLY ix_search_index_mat_geom_gist ON public.search_index_mat USING gist (geom);",
        )
        # Trigram match on label + alt_text
        ensure_index(
            "public.ix_search_index_mat_label_trgm",
            "CREATE INDEX CONCURRENTLY ix_search_index_mat_label_trgm ON public.search_index_mat USING gin (label_norm gin_trgm_ops);",
        )
        ensure_index(
            "public.ix_search_index_mat_alt_trgm",
            "CREATE INDEX CONCURRENTLY ix_search_index_mat_alt_trgm ON public.search_index_mat USING gin (alt_text gin_trgm_ops);",
        )
        # Token ranking
        ensure_index(
            "public.ix_search_index_mat_tsv_gin",
            "CREATE INDEX CONCURRENTLY ix_search_index_mat_tsv_gin ON public.search_index_mat USING gin (tsv);",
        )

    # Convenience refresh function
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.refresh_search_index_mat() RETURNS void AS $$
        BEGIN
          REFRESH MATERIALIZED VIEW CONCURRENTLY public.search_index_mat;
        END;
        $$ LANGUAGE plpgsql;
        """
    )


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS public.refresh_search_index_mat();")
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ux_search_index_mat_id;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_search_index_mat_tsv_gin;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_search_index_mat_alt_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_search_index_mat_label_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_search_index_mat_geom_gist;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS public.search_index_mat;")
