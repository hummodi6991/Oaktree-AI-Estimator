from fastapi.testclient import TestClient

from app.db.deps import get_db
from app.main import app


class DummyResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row

    def scalar(self):
        return self._row


class DummySession:
    def __init__(self, rows):
        self.rows = list(rows)
        self.last_params = []

    def execute(self, _statement, params=None):
        self.last_params.append(params)
        row = self.rows.pop(0) if self.rows else None
        return DummyResult(row)


def test_infer_parcel_clamps_params() -> None:
    dummy = DummySession(
        [
            "public.planet_osm_line",
            {
                "geom": '{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}',
                "area_m2": 123.0,
                "perimeter_m": 44.0,
                "block_area_m2": 999.0,
                "neighbor_count": 12,
            },
        ]
    )

    def override_get_db():
        yield dummy

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = TestClient(app)
        resp = client.get(
            "/v1/geo/infer-parcel",
            params={
                "lng": 1.0,
                "lat": 2.0,
                "building_id": 123,
                "part_index": 1,
                "radius_m": 999,
                "road_buf_m": 1,
                "k": 999,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["found"] is True
        assert data["parcel_id"] == "ms:123:1"
        params = dummy.last_params[1]
        assert params["radius_m"] == 500.0
        assert params["road_buf_m"] == 4.0
        assert params["k"] == 80
    finally:
        app.dependency_overrides.pop(get_db, None)
