from app.api.tiles import SUHAIL_PARCEL_TABLE, _SUHAIL_PARCEL_TILE_SQL


def test_suhail_parcel_tile_sql_contains_expected_fields():
    sql = str(_SUHAIL_PARCEL_TILE_SQL)

    assert SUHAIL_PARCEL_TABLE in sql
    assert "ST_AsMVTGeom" in sql
    assert "landuse" in sql
    assert "geom_32638" in sql
    assert "ST_TileEnvelope" in sql
    assert "ST_Transform(t.geom3857, 32638)" in sql
