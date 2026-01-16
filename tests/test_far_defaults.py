from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

import app.api.estimates as estimates_api
from app.db.deps import get_db
from app.main import app
from tests.excel_inputs import sample_excel_inputs


class DummySession:
    def query(self, *args, **kwargs):
        class _Q:
            def filter(self, *args, **kwargs):
                return self

            def all(self):
                return []

            def first(self):
                return None

        return _Q()

    def close(self):
        pass


@contextmanager
def dummy_session_scope():
    session = DummySession()
    try:
        yield session
    finally:
        session.close()


def override_get_db():
    with dummy_session_scope() as session:
        yield session


@pytest.fixture(autouse=True)
def override_db_dependency():
    app.dependency_overrides[get_db] = override_get_db
    yield
    app.dependency_overrides.pop(get_db, None)


@pytest.fixture(autouse=True)
def stub_indicators(monkeypatch):
    monkeypatch.setattr(estimates_api, "latest_re_price_index_scalar", lambda *args, **kwargs: 1.0)
    monkeypatch.setattr(estimates_api, "latest_rega_residential_rent_per_m2", lambda *args, **kwargs: None)
    monkeypatch.setattr(estimates_api, "price_from_kaggle_hedonic", lambda *args, **kwargs: (None, None, {}))
    monkeypatch.setattr(estimates_api, "price_from_aqar", lambda *args, **kwargs: None)
    monkeypatch.setattr(estimates_api, "latest_tax_rate", lambda *args, **kwargs: {"rate": 0.0, "base_type": None})


def _simple_polygon():
    return {
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


def test_far_autofill_when_not_explicit(monkeypatch):
    monkeypatch.setattr(
        estimates_api,
        "compute_building_metrics",
        lambda _db, _geom, buffer_m=None: {
            "site_area_m2": 1000.0,
            "far_proxy_existing": 1.5 if buffer_m else None,
            "footprint_area_m2": 200.0,
            "existing_bua_m2": 600.0,
            "buffer_m": buffer_m,
        },
    )
    # lookup_far is no longer used (district FAR intentionally ignored)

    client = TestClient(app)
    payload = {
        "geometry": _simple_polygon(),
        "strategy": "build_to_sell",
        "city": "Riyadh",
        # Make land-use deterministic (otherwise code may default to "m")
        "excel_inputs": {
            **sample_excel_inputs(),
            "land_use_code": "s",
            "area_ratio": {},
        },
    }
    resp = client.post("/v1/estimates", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    notes = data["notes"]
    far_info = notes.get("far_inference") or {}
    # New contract: FAR is fully automatic. Residential clamp band is [1.5, 3.0].
    far_val = float(far_info.get("suggested_far") or 0.0)
    assert 1.5 <= far_val <= 3.0
    # Auto engine sets far_used == suggested_far (no user override, no district FAR).
    far_used = float(far_info.get("far_used") or 0.0)
    assert 1.5 <= far_used <= 3.0
    assert far_used == pytest.approx(far_val, rel=1e-9)
    built_area = notes.get("excel_breakdown", {}).get("built_area", {})
    # FAR proxy 1.5 → residential area ratio should scale to ~1.5 on 1000 m² site
    assert pytest.approx(built_area.get("residential") or 0.0, rel=1e-6) == 1500.0


def test_far_preserved_when_explicit(monkeypatch):
    monkeypatch.setattr(
        estimates_api,
        "compute_building_metrics",
        lambda _db, _geom, buffer_m=None: {
            "site_area_m2": 800.0,
            "far_proxy_existing": 1.0 if buffer_m else None,
            "footprint_area_m2": 150.0,
            "existing_bua_m2": 300.0,
            "buffer_m": buffer_m,
        },
    )
    # lookup_far is no longer used (district FAR intentionally ignored)

    client = TestClient(app)
    payload = {
        "geometry": _simple_polygon(),
        "strategy": "build_to_sell",
        "city": "Riyadh",
        # Request FAR is deprecated/ignored by design.
        "far": 3.3,
        "excel_inputs": {
            **sample_excel_inputs(),
            "land_use_code": "s",
            "area_ratio": {"residential": 1.0},
        },
    }
    resp = client.post("/v1/estimates", json=payload)
    assert resp.status_code == 200
    notes = resp.json()["notes"]
    far_info = notes.get("far_inference") or {}
    # New contract: FAR is auto and must remain in the residential clamp band.
    far_used = float(far_info.get("far_used") or 0.0)
    assert 1.5 <= far_used <= 3.0
    assert far_used != pytest.approx(3.3, rel=1e-6)
    built_area = notes.get("excel_breakdown", {}).get("built_area", {})
    assert pytest.approx(built_area.get("residential") or 0.0, rel=1e-6) == 800.0
