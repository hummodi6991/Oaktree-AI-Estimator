import os

import pytest


def pytest_configure() -> None:
    os.environ.pop("PARCEL_TILE_TABLE", None)
    os.environ["PARCEL_TILE_TABLE"] = "public.riyadh_parcels_arcgis_proxy"
    try:
        import app.core.config as config

        config.settings.PARCEL_TILE_TABLE = "public.riyadh_parcels_arcgis_proxy"
    except Exception:
        pass


@pytest.fixture(autouse=True)
def _reset_parcel_tile_table() -> None:
    try:
        import app.api.tiles as tiles

        tiles.PARCEL_TILE_TABLE = "public.riyadh_parcels_arcgis_proxy"
    except Exception:
        pass
