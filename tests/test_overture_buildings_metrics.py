from contextlib import contextmanager

from fastapi.testclient import TestClient

from app.api.geo_portal import BuildingMetricsRequest
from app.db.deps import get_db
from app.main import app
from app.services.overture_buildings_metrics import floors_proxy


def test_floors_proxy_prefers_num_floors_and_clamps_height():
    assert floors_proxy(5, 12.0) == 5
    assert floors_proxy(None, 9.5) == 3  # 9.5 / 3.2 ≈ 2.97 → 3
    assert floors_proxy(None, 1.0) == 1  # clamp minimum
    assert floors_proxy(None, 500.0) == 60  # clamp maximum
    assert floors_proxy(None, None) is None


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class _FakeSession:
    def execute(self, *_args, **_kwargs):
        return _FakeResult(
            {
                "site_area_m2": 1200.0,
                "footprint_area_m2": 300.0,
                "coverage_ratio": 0.25,
                "floors_mean": 4.0,
                "floors_median": 4.0,
                "existing_bua_m2": 1200.0,
                "far_proxy_existing": 1.0,
                "built_density_m2_per_ha": 10000.0,
                "building_count": 12,
                "pct_buildings_with_floors_data": 0.5,
                "buffer_m": None,
            }
        )

    def close(self):
        pass


@contextmanager
def _fake_db():
    session = _FakeSession()
    try:
        yield session
    finally:
        session.close()


def _override_get_db():
    with _fake_db() as db:
        yield db


def test_building_metrics_endpoint_returns_metrics(monkeypatch):
    app.dependency_overrides[get_db] = _override_get_db
    client = TestClient(app)
    payload = BuildingMetricsRequest(
        geojson={
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
        },
        buffer_m=25.0,
    )
    resp = client.post("/v1/geo/building-metrics", json=payload.model_dump())
    assert resp.status_code == 200
    data = resp.json()
    assert data["site_area_m2"] == 1200.0
    assert data["footprint_area_m2"] == 300.0
    assert data["building_count"] == 12
    assert data["floors_mean"] == 4.0
    app.dependency_overrides.pop(get_db, None)
