-- Add geography GiST index to accelerate identify lookups.
CREATE INDEX CONCURRENTLY IF NOT EXISTS riyadh_parcels_arcgis_raw_geom_geog_gix
    ON public.riyadh_parcels_arcgis_raw
    USING GIST ((geom::geography));

ANALYZE public.riyadh_parcels_arcgis_raw;

-- Optional: add similar indexes on derived/inferred parcel tables used by identify.
-- Example:
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS inferred_parcels_v1_geom_geog_gix
--     ON public.inferred_parcels_v1
--     USING GIST ((geom::geography));
-- ANALYZE public.inferred_parcels_v1;
