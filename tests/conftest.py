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


@pytest.fixture
def disable_market_viability_floors(monkeypatch):
    """Opt-in fixture to disable the production market-viability hard floors.

    Production sets EXPANSION_VIABILITY_POPULATION_HARD_FLOOR=20000,
    EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR=1, and
    EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M=75.0, which would drop
    regression-test cohorts that use population_reach values well below
    20k for compactness or that happen to share coordinates with OSM
    construction polygons. Tests that pre-date the new gates and don't
    care about them should request this fixture; tests that explicitly
    cover the hard floors must NOT use this fixture.

    A handful of tests (e.g. test_parcel_table_overrides) call
    importlib.reload(app.core.config), which replaces the ``settings``
    singleton. Modules that imported ``settings`` before the reload
    (notably ``app.services.expansion_advisor``) still hold the
    pre-reload instance, so we patch every live reference we can find
    rather than just the canonical one.
    """
    import sys

    import app.core.config as config

    seen_ids: set[int] = set()
    for module in list(sys.modules.values()):
        if module is None:
            continue
        candidate = getattr(module, "settings", None)
        if candidate is None or id(candidate) in seen_ids:
            continue
        if not hasattr(candidate, "EXPANSION_VIABILITY_POPULATION_HARD_FLOOR"):
            continue
        seen_ids.add(id(candidate))
        monkeypatch.setattr(candidate, "EXPANSION_VIABILITY_POPULATION_HARD_FLOOR", 0)
        monkeypatch.setattr(candidate, "EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR", 0)
        if hasattr(candidate, "EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M"):
            monkeypatch.setattr(candidate, "EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M", 0)
    if id(config.settings) not in seen_ids:
        monkeypatch.setattr(config.settings, "EXPANSION_VIABILITY_POPULATION_HARD_FLOOR", 0)
        monkeypatch.setattr(config.settings, "EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR", 0)
        if hasattr(config.settings, "EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M"):
            monkeypatch.setattr(config.settings, "EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M", 0)
