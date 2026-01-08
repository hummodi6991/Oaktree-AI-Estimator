import importlib


def test_parcel_target_srid_default(monkeypatch):
    monkeypatch.delenv("PARCEL_TARGET_SRID", raising=False)

    import app.core.config as config

    config = importlib.reload(config)

    assert config.Settings().PARCEL_TARGET_SRID == 32638
