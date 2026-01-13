from app.api.geo_portal import _IDENTIFY_SQL


def test_identify_sql_prefers_contains_and_uses_dwithin_filter():
    sql = str(_IDENTIFY_SQL)
    assert "ST_Contains" in sql
    assert "WHERE ST_DWithin" in sql
    assert "landuse_label" in sql
    assert "contains DESC" in sql
    assert "is_non_ovt DESC" in sql
    assert "distance_m ASC" in sql
    assert "area_m2 DESC" in sql
    assert sql.index("is_non_ovt DESC") < sql.index("is_ovt DESC")
