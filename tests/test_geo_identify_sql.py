from app.api.geo_portal import _IDENTIFY_SQL


def test_identify_sql_prefers_contains_and_large_area():
    sql = str(_IDENTIFY_SQL)
    assert "ST_Contains" in sql
    assert "area_m2 DESC" in sql
