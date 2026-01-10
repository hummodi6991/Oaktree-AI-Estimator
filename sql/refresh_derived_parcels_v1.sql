\set ON_ERROR_STOP off

REFRESH MATERIALIZED VIEW CONCURRENTLY public.derived_parcels_v1;
REFRESH MATERIALIZED VIEW public.derived_parcels_v1;
