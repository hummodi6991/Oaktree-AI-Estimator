import json
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

import app.api.estimates as estimates_api
from app.db.deps import get_db
from app.main import app
from app.models.tables import PriceQuote
from app.services import land_price_engine
from app.services.district_resolver import DistrictResolution, resolution_meta
from tests.excel_inputs import sample_excel_inputs


RIYADH_POLYGON = {
    "type": "Polygon",
    "coordinates": [
        [
            [46.675, 24.713],
            [46.676, 24.713],
            [46.676, 24.714],
            [46.675, 24.714],
            [46.675, 24.713],
        ]
    ],
}
RIYADH_CENTROID = {"lng": 46.6755, "lat": 24.7135}


class DummyResult:
    def scalar(self):
        return None

    def mappings(self):
        return self

    def first(self):
        return None


class DummyQuery:
    def __init__(self, items=None):
        self.items = items or []

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def first(self):
        return self.items[0] if self.items else None

    def all(self):
        return list(self.items)

    def limit(self, *args, **kwargs):
        return self


class DummySession:
    def __init__(self, storage: list):
        self.storage = storage

    def query(self, *args, **kwargs):
        return DummyQuery()

    def add(self, obj):
        self.storage.append(obj)

    def add_all(self, entries):
        self.storage.extend(entries)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass

    def execute(self, *args, **kwargs):
        return DummyResult()

    def get(self, *args, **kwargs):
        return None


@pytest.fixture
def quote_store():
    return []


@pytest.fixture(autouse=True)
def override_db_dependency(quote_store):
    def override_get_db():
        session = DummySession(quote_store)
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_get_db
    yield
    app.dependency_overrides.pop(get_db, None)


@pytest.fixture(autouse=True)
def stub_pricing(monkeypatch):
    resolution = DistrictResolution(
        city_norm="riyadh",
        district_raw="Al Olaya",
        district_norm="al_olaya",
        method="stubbed_polygon",
        confidence=0.99,
        evidence_count=50,
    )

    def _resolution_stub(*args, **kwargs):
        return resolution

    monkeypatch.setattr(land_price_engine, "resolve_district", _resolution_stub)
    monkeypatch.setattr(estimates_api, "resolve_district", _resolution_stub)

    suhail_meta = {
        "source": "suhail_land_metrics",
        "as_of_date": "2024-02-01",
        "land_use_group": "الكل",
        "last_price_ppm2": 975.0,
        "last_txn_date": "2024-01-15",
        "level": "district",
    }
    aqar_meta = {"source": "aqar.mv_city_price_per_sqm", "n": 80, "level": "district"}

    monkeypatch.setattr(land_price_engine, "_suhail_land_signal", lambda *args, **kwargs: (1000.0, suhail_meta))
    monkeypatch.setattr(land_price_engine, "_aqar_land_signal", lambda *args, **kwargs: (900.0, aqar_meta))

    monkeypatch.setattr(estimates_api, "price_from_kaggle_hedonic", lambda *args, **kwargs: (None, None, {}))
    monkeypatch.setattr(estimates_api, "price_from_aqar", lambda *args, **kwargs: None)

    return resolution


@pytest.fixture
def client(override_db_dependency, stub_pricing):
    return TestClient(app)


def _pricing_params():
    return {"city": "Riyadh", **RIYADH_CENTROID}


def _extract_pricing_value(payload: dict) -> float:
    return payload.get("value_sar_m2") or payload.get("value")


def test_pricing_defaults_city_and_returns_legacy_keys(monkeypatch, client):
    def mock_quote_land_price_blended_v1(db, city, district, lon, lat, geom_geojson=None):
        return {
            "provider": "blended_v1",
            "value": 1234.5,
            "district_norm": "test_district",
            "district_raw": "Test District",
            "district_resolution": {"method": "stub"},
            "meta": {},
        }

    monkeypatch.setattr("app.api.pricing.quote_land_price_blended_v1", mock_quote_land_price_blended_v1)

    response = client.get("/v1/pricing/land", params={"lng": 1.0, "lat": 2.0})
    assert response.status_code == 200

    payload = response.json()
    assert payload.get("value_sar_m2") is not None
    assert payload.get("sar_per_m2") is not None
    assert payload["value_sar_m2"] == payload["sar_per_m2"]
    assert payload["city"] == "Riyadh"

def test_pricing_deterministic(client):
    resp1 = client.get("/v1/pricing/land", params=_pricing_params())
    resp2 = client.get("/v1/pricing/land", params=_pricing_params())

    assert resp1.status_code == 200
    assert resp2.status_code == 200

    data1 = resp1.json()
    data2 = resp2.json()

    assert data1["provider"] == "blended_v1" == data2["provider"]

    assert data1.get("district_norm")
    assert data1["district_norm"] == data2["district_norm"]

    assert data1.get("district_resolution")
    assert data1["district_resolution"].get("method")
    assert data1["district_resolution"]["method"] == data2["district_resolution"]["method"]


def test_pricing_matches_feasibility(client):
    price_resp = client.get("/v1/pricing/land", params=_pricing_params())
    assert price_resp.status_code == 200
    price_data = price_resp.json()
    price_value = _extract_pricing_value(price_data)
    assert price_value is not None

    estimate_payload = {
        "geometry": RIYADH_POLYGON,
        "strategy": "build_to_sell",
        "city": "Riyadh",
        "excel_inputs": {**sample_excel_inputs(), "land_price_sar_m2": 0},
    }

    estimate_resp = client.post("/v1/estimates", json=estimate_payload)
    assert estimate_resp.status_code == 200

    estimate_data = estimate_resp.json()
    excel_land = estimate_data["notes"]["excel_land_price"]
    feasibility_price = excel_land.get("ppm2") or excel_land.get("land_price_sar_m2")
    assert feasibility_price is not None
    assert feasibility_price == pytest.approx(float(price_value), abs=1e-6)


def test_pricing_audit_persistence(client, quote_store):
    has_meta = hasattr(PriceQuote, "meta") or ("meta" in getattr(getattr(PriceQuote, "__table__", None), "columns", {}))
    if not has_meta:
        pytest.skip("price_quote.meta not yet available; enable after audit PR")

    response = client.get("/v1/pricing/land", params=_pricing_params())
    assert response.status_code == 200

    quotes = [q for q in quote_store if isinstance(q, PriceQuote)]
    assert quotes, "pricing call should persist a PriceQuote row"

    latest = sorted(quotes, key=lambda q: getattr(q, "observed_at", datetime.min))[-1]
    meta_raw = getattr(latest, "meta", None)
    assert meta_raw is not None

    meta = json.loads(meta_raw) if isinstance(meta_raw, str) else meta_raw
    assert meta, "meta should not be empty when available"

    def _from_meta(container: dict, key: str):
        if not isinstance(container, dict):
            return None
        if key in container:
            return container.get(key)
        nested = container.get("meta")
        if isinstance(nested, dict) and key in nested:
            return nested.get(key)
        return None

    weights = _from_meta(meta, "weights")
    assert weights, "weights should be recorded in meta"

    district_resolution = _from_meta(meta, "district_resolution")
    assert district_resolution, "district_resolution should be recorded in meta"

    components = _from_meta(meta, "components")
    assert components and isinstance(components, dict), "components should be present in meta"

    suhail_component = components.get("suhail") if isinstance(components, dict) else None
    assert suhail_component and isinstance(suhail_component, dict), "suhail component should be present"
    snapshot_date = suhail_component.get("as_of_date") or suhail_component.get("as_of")
    assert snapshot_date, "suhail snapshot date should be present in meta"
