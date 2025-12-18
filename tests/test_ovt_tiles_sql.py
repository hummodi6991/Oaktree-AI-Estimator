from app.api.tiles import _OVT_TILE_SQL


def test_ovt_tile_sql_uses_explicit_aliases():
    sql = str(_OVT_TILE_SQL)

    assert "overture_buildings AS ovt" in sql
    assert "CROSS JOIN bounds AS b" in sql
    assert "ovt.geom" in sql
    assert "b.geom" in sql
    assert "ST_AsMVTGeom(geom" not in sql
    assert "WHERE geom &&" not in sql
