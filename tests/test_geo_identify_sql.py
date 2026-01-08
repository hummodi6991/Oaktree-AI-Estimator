from app.api.geo_portal import _IDENTIFY_SQL


def test_identify_sql_prefers_contains_and_uses_dwithin_filter():
    sql = str(_IDENTIFY_SQL)
    assert "ST_Contains" in sql
    assert "WHERE ST_DWithin" in sql
    assert "contains DESC" in sql
    assert "distance_m ASC" in sql
    assert "area_m2 DESC" in sql
