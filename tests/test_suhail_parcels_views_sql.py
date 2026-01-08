from alembic import op  # noqa: F401


def test_suhail_views_use_setsrid_and_geom32638_reuse():
    import pathlib

    path = pathlib.Path("alembic/versions/c1f4a2b3c4d5_standardize_suhail_parcels_views.py")
    text = path.read_text(encoding="utf-8")

    assert "ST_SetSRID" in text
    assert "geom_32638" in text
    assert "ST_Area(geom_32638)" in text or "ST_Area(geom_32638)::bigint" in text
    assert "ST_Perimeter(geom_32638)" in text or "ST_Perimeter(geom_32638)::bigint" in text
