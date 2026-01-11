import pathlib


def test_refresh_derived_parcels_sql_uses_fallback_block():
    sql = pathlib.Path("sql/refresh_derived_parcels_v1.sql").read_text(encoding="utf-8")

    assert "DO $$" in sql
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY public.derived_parcels_v1" in sql
    assert "EXCEPTION WHEN OTHERS" in sql
    assert "REFRESH MATERIALIZED VIEW public.derived_parcels_v1" in sql
