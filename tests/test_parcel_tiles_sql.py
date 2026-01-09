from app.api.tiles import SUHAIL_PARCEL_TABLE, _SUHAIL_PARCEL_TILE_SQL


def test_suhail_parcel_tile_sql_contains_expected_fields():
    sql = str(_SUHAIL_PARCEL_TILE_SQL)

    assert SUHAIL_PARCEL_TABLE in sql
    assert "ST_AsMVTGeom" in sql
    assert "landuse" in sql
    assert "geom_32638" in sql
    assert "ST_SetSRID(ST_TileEnvelope(" in sql
    assert "ST_TileEnvelope(:z,:x,:y, 3857)" not in sql
    assert "tile32638" in sql
    assert "ST_SimplifyPreserveTopology" in sql
    assert ":min_area_z15" in sql
    assert ":min_area_z16" in sql
    assert ":simp_z15" in sql
    assert ":simp_z16" in sql
    assert ":z <= 15" in sql
    assert ":z = 16" in sql
