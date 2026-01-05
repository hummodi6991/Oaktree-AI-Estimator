import importlib

from app.api import tiles


def _reload_tiles_with_filter(monkeypatch, value: str | None):
    if value is None:
        monkeypatch.delenv("PARCEL_TILE_SOURCE_FILTER", raising=False)
    else:
        monkeypatch.setenv("PARCEL_TILE_SOURCE_FILTER", value)
    return importlib.reload(tiles)


def test_parcel_tile_sql_includes_source_filter_when_set(monkeypatch):
    module = _reload_tiles_with_filter(monkeypatch, "suhail")
    sql = str(module._PARCEL_TILE_SQL)
    assert "AND p.source = :source_filter" in sql


def test_parcel_tile_sql_omits_source_filter_when_unset(monkeypatch):
    module = _reload_tiles_with_filter(monkeypatch, None)
    sql = str(module._PARCEL_TILE_SQL)
    assert "AND p.source = :source_filter" not in sql
