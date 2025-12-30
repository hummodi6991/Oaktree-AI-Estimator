from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

from app.api import estimates as estimates_api
from app.db.deps import get_db
from app.main import app
from tests.excel_inputs import sample_excel_inputs


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
    def __init__(self):
        self.added = []
        self.committed = False
        self.rolled_back = False

    def query(self, *args, **kwargs):
        return DummyQuery()

    def add_all(self, entries):
        self.added.extend(entries)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

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


client = TestClient(app)


def test_create_estimate_basic():
    poly = {
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
    payload = {
        "geometry": poly,
        "asset_program": "residential_midrise",
        "unit_mix": [{"type": "1BR", "count": 10}],
        "finish_level": "mid",
        "timeline": {"start": "2025-10-01", "months": 18},
        "financing_params": {"margin_bps": 250, "ltv": 0.6},
        "strategy": "build_to_sell",
        "excel_inputs": sample_excel_inputs(),
    }
    response = client.post("/v1/estimates", json=payload)
    assert response.status_code == 200
    data = response.json()
    for key in ["totals", "confidence_bands", "assumptions"]:
        assert key in data
    assert data["totals"]["land_value"] > 0


def test_estimate_falls_back_when_db_commit_fails():
    class FailingSession:
        def __init__(self):
            self.added = []
            self.committed = False
            self.rolled_back = False

        def query(self, *args, **kwargs):
            return DummyQuery()

        def add(self, entry):
            self.added.append(entry)

        def add_all(self, entries):
            self.added.extend(entries)

        def get(self, model, ident):
            return None

        def commit(self):
            self.committed = True
            raise RuntimeError("db unavailable")

        def rollback(self):
            self.rolled_back = True

        def close(self):
            pass

    sessions = []

    def override_db():
        session = FailingSession()
        sessions.append(session)
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_db

    poly = {
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
    payload = {
        "geometry": poly,
        "asset_program": "residential_midrise",
        "unit_mix": [{"type": "1BR", "count": 2}],
        "finish_level": "mid",
        "timeline": {"start": "2025-10-01", "months": 18},
        "financing_params": {"margin_bps": 250, "ltv": 0.6},
        "strategy": "build_to_sell",
        "excel_inputs": sample_excel_inputs(),
    }

    estimate_id = None
    try:
        response = client.post("/v1/estimates", json=payload)
        assert response.status_code == 200
        assert sessions and sessions[0].rolled_back

        estimate_id = response.json()["id"]

        get_response = client.get(f"/v1/estimates/{estimate_id}")
        assert get_response.status_code == 200
        assert estimate_id in estimates_api._INMEM_HEADERS
    finally:
        if estimate_id:
            estimates_api._INMEM_HEADERS.pop(estimate_id, None)
            estimates_api._INMEM_LINES.pop(estimate_id, None)
