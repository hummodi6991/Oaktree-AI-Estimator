from app.api.tiles import PARCEL_TILE_GEOM_COLUMN, _PARCEL_TILE_SQL


def test_parcel_tile_sql_filters_with_4326_and_renders_3857():
    sql = str(_PARCEL_TILE_SQL)

    assert f"p.{PARCEL_TILE_GEOM_COLUMN} && ST_Transform(t.geom3857, 4326)" in sql
    assert f"ST_Intersects(p.{PARCEL_TILE_GEOM_COLUMN}, ST_Transform(t.geom3857, 4326))" in sql
    assert f"ST_Transform(p.{PARCEL_TILE_GEOM_COLUMN}, 3857)" in sql
