from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

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


def test_excel_scenario_price_uplift_updates_cost_breakdown():
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
    create_response = client.post("/v1/estimates", json=payload)
    assert create_response.status_code == 200
    base_body = create_response.json()
    estimate_id = base_body["id"]
    base_totals = base_body["totals"]
    base_breakdown = base_body["notes"]["cost_breakdown"]

    scenario_response = client.post(
        f"/v1/estimates/{estimate_id}/scenario",
        json={"price_uplift_pct": 10},
    )
    assert scenario_response.status_code == 200
    scenario_body = scenario_response.json()
    assert "totals" in scenario_body
    assert scenario_body["totals"]["p50_profit"] != base_totals["p50_profit"]
    updated_notes = scenario_body["notes"]
    updated_breakdown = updated_notes["cost_breakdown"]

    assert updated_breakdown["y1_income"] > base_breakdown["y1_income"]
    assert updated_breakdown["y1_noi"] > base_breakdown["y1_noi"]
    assert updated_breakdown["roi"] > base_breakdown["roi"]
    assert "excel_breakdown" in updated_notes
