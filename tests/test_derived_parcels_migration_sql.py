from alembic import op  # noqa: F401


def test_derived_parcels_mv_guarded_by_to_regclass():
    import pathlib

    path = pathlib.Path("alembic/versions/e3f4a5b6c7d8_create_derived_parcels_v1.py")
    text = path.read_text(encoding="utf-8")

    assert "to_regclass('public.derived_parcels_v1')" in text
    assert "IF to_regclass('public.derived_parcels_v1') IS NULL" in text
    assert "CREATE MATERIALIZED VIEW public.derived_parcels_v1" in text
