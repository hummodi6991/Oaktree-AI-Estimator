import importlib


def test_parcel_target_srid_default(monkeypatch):
    monkeypatch.delenv("PARCEL_TARGET_SRID", raising=False)

    import app.core.config as config

    config = importlib.reload(config)

    assert config.Settings().PARCEL_TARGET_SRID == 4326


def test_parcel_identify_geom_column_override(monkeypatch):
    monkeypatch.setenv("PARCEL_IDENTIFY_GEOM_COLUMN", "geom_32638")

    import app.core.config as config

    config = importlib.reload(config)

    assert config.Settings().PARCEL_IDENTIFY_GEOM_COLUMN == "geom_32638"


def test_parcel_tile_table_default(monkeypatch):
    monkeypatch.delenv("PARCEL_TILE_TABLE", raising=False)
    monkeypatch.delenv("PARCEL_IDENTIFY_TABLE", raising=False)

    import app.core.config as config

    config = importlib.reload(config)

    settings = config.Settings()
    assert settings.PARCEL_TILE_TABLE == "public.riyadh_parcels_arcgis_proxy"
    assert settings.PARCEL_IDENTIFY_TABLE == "public.riyadh_parcels_arcgis_proxy"
