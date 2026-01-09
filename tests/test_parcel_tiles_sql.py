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
    assert (
        "ST_Transform(t.geom3857, 4326)" in sql
        or "ST_Transform(t.geom3857,4326)" in sql
    )
