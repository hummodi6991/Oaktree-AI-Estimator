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


@pytest.fixture(autouse=True)
def _disable_market_viability_hard_floors():
    """Disable the production market-viability hard floors during tests.

    Production sets EXPANSION_VIABILITY_POPULATION_HARD_FLOOR=20000 and
    EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR=1, which would drop the
    bulk of regression-test cohorts (which use population_reach values
    well below 20k for compactness). Tests that explicitly cover the
    hard floors should set these values via monkeypatch within the test.
    """
    try:
        import app.core.config as config

        prev_pop = getattr(config.settings, "EXPANSION_VIABILITY_POPULATION_HARD_FLOOR", None)
        prev_bp = getattr(config.settings, "EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR", None)
        config.settings.EXPANSION_VIABILITY_POPULATION_HARD_FLOOR = 0
        config.settings.EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR = 0
        try:
            yield
        finally:
            if prev_pop is not None:
                config.settings.EXPANSION_VIABILITY_POPULATION_HARD_FLOOR = prev_pop
            if prev_bp is not None:
                config.settings.EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR = prev_bp
    except Exception:
        yield
