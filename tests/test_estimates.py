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


def test_land_use_override_does_not_persist_between_requests():
    poly_mixed_use = {
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
    poly_auto = {
        "type": "Polygon",
        "coordinates": [
            [
                [46.677, 24.715],
                [46.678, 24.715],
                [46.678, 24.716],
                [46.677, 24.716],
                [46.677, 24.715],
            ]
        ],
    }
    base_payload = {
        "asset_program": "residential_midrise",
        "unit_mix": [{"type": "1BR", "count": 10}],
        "finish_level": "mid",
        "timeline": {"start": "2025-10-01", "months": 18},
        "financing_params": {"margin_bps": 250, "ltv": 0.6},
        "strategy": "build_to_sell",
    }
    mixed_inputs = sample_excel_inputs()
    mixed_inputs["land_use_code"] = "m"
    mixed_inputs["area_ratio"] = {"residential": 1.2, "retail": 0.5, "basement": 1.0}

    response_mixed = client.post(
        "/v1/estimates",
        json={**base_payload, "geometry": poly_mixed_use, "excel_inputs": mixed_inputs},
    )
    assert response_mixed.status_code == 200
    notes_mixed = response_mixed.json().get("notes") or {}
    assert notes_mixed.get("landuse_for_far_cap") == "m"

    auto_inputs = sample_excel_inputs()
    response_auto = client.post(
        "/v1/estimates",
        json={**base_payload, "geometry": poly_auto, "excel_inputs": auto_inputs},
    )
    assert response_auto.status_code == 200
    notes_auto = response_auto.json().get("notes") or {}
    assert notes_auto.get("landuse_for_far_cap") == "s"


def test_mixed_use_desired_floors_applied():
    poly_mixed_use = {
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
        "geometry": poly_mixed_use,
        "asset_program": "residential_midrise",
        "unit_mix": [{"type": "1BR", "count": 10}],
        "finish_level": "mid",
        "timeline": {"start": "2025-10-01", "months": 18},
        "financing_params": {"margin_bps": 250, "ltv": 0.6},
        "strategy": "build_to_sell",
    }
    mixed_inputs = sample_excel_inputs()
    mixed_inputs["land_use_code"] = "m"
    mixed_inputs["area_ratio"] = {"residential": 2.0, "retail": 1.0, "basement": 1.0}
    mixed_inputs["desired_floors_above_ground"] = 5

    response = client.post(
        "/v1/estimates",
        json={**payload, "excel_inputs": mixed_inputs},
    )
    assert response.status_code == 200
    notes = response.json().get("notes") or {}
    floors_adjustment = notes.get("floors_adjustment") or {}
    assert floors_adjustment.get("desired_floors_above_ground") == pytest.approx(5.0)


def test_disable_floors_scaling_skips_mixed_use_adjustment():
    poly_mixed_use = {
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
        "geometry": poly_mixed_use,
        "asset_program": "residential_midrise",
        "unit_mix": [{"type": "1BR", "count": 10}],
        "finish_level": "mid",
        "timeline": {"start": "2025-10-01", "months": 18},
        "financing_params": {"margin_bps": 250, "ltv": 0.6},
        "strategy": "build_to_sell",
    }
    mixed_inputs = sample_excel_inputs()
    mixed_inputs["land_use_code"] = "m"
    mixed_inputs["area_ratio"] = {"residential": 2.0, "retail": 1.0, "basement": 1.0}
    mixed_inputs["disable_floors_scaling"] = True

    response = client.post(
        "/v1/estimates",
        json={**payload, "excel_inputs": mixed_inputs},
    )
    assert response.status_code == 200
    notes = response.json().get("notes") or {}
    breakdown = notes.get("excel_breakdown") or {}
    assert breakdown.get("far_above_ground") == pytest.approx(3.0, rel=1e-6)
    floors_adjustment = notes.get("floors_adjustment") or {}
    assert floors_adjustment.get("floors_scaling_skipped") is True
