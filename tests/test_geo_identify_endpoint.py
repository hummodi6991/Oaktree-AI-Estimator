from fastapi.testclient import TestClient

from app.db.deps import get_db
from app.main import app


class _DummyResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class _DummyDB:
    def __init__(self, row):
        self.row = row
        self.calls = []

    def execute(self, statement, params=None):
        self.calls.append((str(statement), params))
        return _DummyResult(self.row)


def test_identify_get_returns_parcel_id_when_available():
    dummy = _DummyDB(
        {
            "id": "parcel-123",
            "landuse": None,
            "classification": None,
            "area_m2": 1200,
            "perimeter_m": 140,
            "geom": None,
            "distance_m": 0,
            "site_area_m2": None,
            "footprint_area_m2": None,
            "building_count": None,
        }
    )

    def override_get_db():
        yield dummy

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = TestClient(app)
        resp = client.get("/v1/geo/identify?lng=46.675&lat=24.713")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["found"] is True
        assert payload["parcel"]["parcel_id"] == "parcel-123"
    finally:
        app.dependency_overrides.pop(get_db, None)
