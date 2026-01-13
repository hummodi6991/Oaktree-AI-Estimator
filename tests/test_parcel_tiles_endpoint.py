import math

from fastapi.testclient import TestClient

from app.db.deps import get_db
from app.main import app


class DummyResult:
    def __init__(self, payload: bytes | None):
        self._payload = payload

    def scalar(self):
        return self._payload


class DummySession:
    def __init__(self, payload: bytes | None = None, allow_execute: bool = True) -> None:
        self.payload = payload
        self.allow_execute = allow_execute
        self.executed = False
        self.last_params = None
        self.last_sql = None

    def execute(self, *args, **kwargs):
        if not self.allow_execute:
            raise AssertionError("execute should not be called")
        self.executed = True
        self.last_sql = args[0] if args else kwargs.get("statement")
        self.last_params = args[1] if len(args) > 1 else kwargs.get("params")
        return DummyResult(self.payload)


def _riyadh_tile(z: int = 16, lng: float = 46.675, lat: float = 24.713) -> tuple[int, int]:
    lat_rad = math.radians(lat)
    n = 2.0 ** z
    x = int((lng + 180.0) / 360.0 * n)
    y = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return x, y


def test_parcel_tile_low_zoom_returns_204_without_db() -> None:
    dummy = DummySession(allow_execute=False)

    def override_get_db():
        yield dummy

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = TestClient(app)
        resp = client.get("/v1/tiles/parcels/15/0/0.pbf")
        assert resp.status_code == 204
        assert not dummy.executed
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_parcel_tile_high_zoom_returns_bytes() -> None:
    dummy = DummySession(payload=b"parcels")

    def override_get_db():
        yield dummy

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = TestClient(app)
        resp = client.get("/v1/tiles/parcels/16/0/0.pbf")
        assert resp.status_code == 200
        assert resp.content == b"parcels"
        assert dummy.executed
        assert dummy.last_params is not None
        assert "simplify_tol" in dummy.last_params
        assert "suhail_parcels_mat" in str(dummy.last_sql)
        assert "c.geom3857" in str(dummy.last_sql)
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_parcel_tile_riyadh_tile_empty_returns_204() -> None:
    dummy = DummySession(payload=None)

    def override_get_db():
        yield dummy

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = TestClient(app)
        x, y = _riyadh_tile()
        resp = client.get(f"/v1/tiles/parcels/16/{x}/{y}.pbf")
        assert resp.status_code == 204
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_parcel_tile_riyadh_tile_returns_bytes_when_available() -> None:
    dummy = DummySession(payload=b"parcels")

    def override_get_db():
        yield dummy

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = TestClient(app)
        x, y = _riyadh_tile()
        resp = client.get(f"/v1/tiles/parcels/16/{x}/{y}.pbf")
        assert resp.status_code == 200
        assert resp.content == b"parcels"
        assert dummy.executed
    finally:
        app.dependency_overrides.pop(get_db, None)
