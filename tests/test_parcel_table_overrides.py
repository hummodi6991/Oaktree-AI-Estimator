import importlib


def _reload_tiles():
    import app.core.config as config
    import app.api.tiles as tiles

    importlib.reload(config)
    return importlib.reload(tiles)


def _reload_geo_portal():
    import app.core.config as config
    import app.api.geo_portal as geo_portal

    importlib.reload(config)
    return importlib.reload(geo_portal)


def test_parcel_tile_default_uses_suhail_when_env_missing(monkeypatch):
    monkeypatch.delenv("PARCEL_TILE_TABLE", raising=False)

    tiles = _reload_tiles()
    sql = str(tiles._generic_parcel_tile_sql(tiles.PARCEL_TILE_TABLE, simplify=False))

    assert tiles.PARCEL_TILE_TABLE == "public.suhail_parcels_mat"
    assert "public.suhail_parcels_mat" in sql


def test_parcel_tile_inferred_env_is_overridden_to_suhail(monkeypatch):
    monkeypatch.setenv("PARCEL_TILE_TABLE", "public.inferred_parcels_v1")

    tiles = _reload_tiles()

    assert tiles.PARCEL_TILE_TABLE == "public.suhail_parcels_mat"


def test_parcel_identify_inferred_env_is_overridden_to_suhail(monkeypatch):
    monkeypatch.setenv("PARCEL_IDENTIFY_TABLE", "inferred_parcels_v1")

    geo_portal = _reload_geo_portal()

    assert geo_portal._PARCEL_TABLE == "public.suhail_parcels_mat"
