from contextlib import contextmanager

from fastapi.testclient import TestClient

from app.db.deps import get_db
from app.main import app


class _DummyResult:
    def __init__(self, payload: bytes):
        self._payload = payload

    def scalar(self):
        return self._payload


class _DummySession:
    def execute(self, *args, **kwargs):
        return _DummyResult(b"tile-bytes")

    def close(self):
        pass


@contextmanager
def _dummy_session_scope():
    session = _DummySession()
    try:
        yield session
    finally:
        session.close()


def _override_get_db():
    with _dummy_session_scope() as session:
        yield session


def test_suhail_tile_endpoint_returns_protobuf():
    app.dependency_overrides[get_db] = _override_get_db
    try:
        client = TestClient(app)
        resp = client.get("/v1/tiles/suhail/15/20634/14062.pbf")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert resp.status_code == 200
    assert resp.content == b"tile-bytes"
    assert resp.headers["content-type"].startswith("application/x-protobuf")
