from app.api.tiles import OSM_POLYGON_TABLE, SUHAIL_PARCEL_TABLE, _OSM_PARCEL_TILE_SQL, _SUHAIL_PARCEL_TILE_SQL


def test_osm_parcel_tile_sql_uses_fixed_mat_view():
    sql = str(_OSM_PARCEL_TILE_SQL)

    assert OSM_POLYGON_TABLE in sql
    assert "geom_3857" in sql
    assert "ST_TileEnvelope(:z,:x,:y, 3857)" in sql
    assert "ST_AsMVTGeom" in sql
    assert "ST_Area(p.geom_32638)" in sql
    assert "max_area_m2" in sql


def test_suhail_parcel_tile_sql_contains_expected_fields():
    sql = str(_SUHAIL_PARCEL_TILE_SQL)

    assert SUHAIL_PARCEL_TABLE in sql
    assert "ST_AsMVTGeom" in sql
    assert "landuse" in sql
    assert "geom_32638" in sql
    assert "ST_TileEnvelope" in sql
    assert "ST_Transform(t.geom3857, 32638)" in sql
