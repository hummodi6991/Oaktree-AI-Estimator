from app.api.tiles import SUHAIL_PARCEL_TABLE, _SUHAIL_PARCEL_TILE_SQL


def test_suhail_parcel_tile_sql_contains_expected_fields():
    sql = str(_SUHAIL_PARCEL_TILE_SQL)

    assert SUHAIL_PARCEL_TABLE in sql
    assert "ST_AsMVTGeom" in sql
    assert "landuse" in sql

    # Tiles must be generated in WebMercator (EPSG:3857)
    # Geometry should be explicitly transformed before ST_AsMVTGeom
    assert (
        "ST_Transform(p.geom, 3857)" in sql
        or "ST_Transform(p.geom,3857)" in sql
    )
    assert "ST_SetSRID(ST_TileEnvelope(" in sql
    assert "ST_TileEnvelope(:z,:x,:y, 3857)" not in sql
    # Filtering must happen in the same CRS as the tile envelope to avoid
    # tile-boundary "patchwork" artifacts (filter + clip both in 3857).
    assert (
        "ST_Transform(p.geom, 3857) && t.geom3857" in sql
        or "ST_Transform(p.geom,3857) && t.geom3857" in sql
    )
