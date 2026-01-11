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
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_parcel_tile_inferred_table_selects_inferred_sql(monkeypatch) -> None:
    from app.api import tiles

    dummy = DummySession(payload=b"parcels")

    def override_get_db():
        yield dummy

    monkeypatch.setattr(tiles, "PARCEL_TILE_TABLE", "public.inferred_parcels_v1")
    monkeypatch.setattr(tiles, "_HAS_INFERRED_PARCELS", True)
    app.dependency_overrides[get_db] = override_get_db
    try:
        client = TestClient(app)
        resp = client.get("/v1/tiles/parcels/16/0/0.pbf")
        assert resp.status_code == 200
        assert resp.content == b"parcels"
        assert dummy.executed
        assert dummy.last_sql is not None
        assert "inferred_parcels_v1" in str(dummy.last_sql)
    finally:
        app.dependency_overrides.pop(get_db, None)
