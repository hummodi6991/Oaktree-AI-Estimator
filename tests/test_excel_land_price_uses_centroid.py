import pytest

from app.db.deps import get_db
from app.main import app


class DummyQuery:
    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def first(self):
        return None

    def all(self):
        return []

    def limit(self, *args, **kwargs):
        return self


class DummySession:
    def query(self, *args, **kwargs):
        return DummyQuery()

    def add_all(self, entries):
        pass

    def add(self, entry):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def override_get_db():
    session = DummySession()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def client():
    from fastapi.testclient import TestClient

    app.dependency_overrides[get_db] = override_get_db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_excel_land_price_calls_hedonic_with_lon_lat(monkeypatch, client):
    """
    Regression test:
    Excel-mode must pass centroid lon/lat into price_from_kaggle_hedonic,
    otherwise land ppm2 can collapse to a constant citywide value.
    """

    seen = {"lon": None, "lat": None, "district": "sentinel"}

    def fake_price_from_kaggle_hedonic(db, *, city, lon=None, lat=None, district=None):
        seen["lon"] = lon
        seen["lat"] = lat
        seen["district"] = district
        # Return any numeric value to keep the flow moving
        return 4000.0, "kaggle_hedonic_v0", {"source": "kaggle_hedonic_v0", "district": district}

    # Patch the symbol used by estimates.py
    import app.api.estimates as estimates_mod

    monkeypatch.setattr(estimates_mod, "price_from_kaggle_hedonic", fake_price_from_kaggle_hedonic)

    payload = {
        "city": "Riyadh",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[46.675, 24.713], [46.676, 24.713], [46.676, 24.714], [46.675, 24.714], [46.675, 24.713]]],
        },
        "excel_inputs": {
            "area_ratio": {"residential": 1.6, "basement": 1},
            "unit_cost": {"residential": 2200, "basement": 1200},
            "efficiency": {"residential": 0.82},
            "cp_sqm_per_space": {"basement": 30},
            "rent_sar_m2_yr": {"residential": 2400},
            "fitout_rate": 400,
            "contingency_pct": 0.10,
            "consultants_pct": 0.06,
            "feasibility_fee": 1500000,
            "transaction_pct": 0.03,
            "land_price_sar_m2": 0,
        },
    }

    r = client.post("/v1/estimates", json=payload)
    assert r.status_code == 200, r.text

    assert seen["lon"] is not None
    assert seen["lat"] is not None
    # district can be None (then hedonic infers from lon/lat); that's fine
