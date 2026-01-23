from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

from app.db.deps import get_db
from app.main import app
from app.api import estimates as estimates_api
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


def test_excel_scenario_price_uplift_increases_net_revenue_opex_noi():
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
    base_breakdown = base_body["notes"]["cost_breakdown"]

    scenario_response = client.post(
        f"/v1/estimates/{estimate_id}/scenario",
        json={"price_uplift_pct": 12},
    )
    assert scenario_response.status_code == 200
    scenario_body = scenario_response.json()
    updated_breakdown = scenario_body["notes"]["cost_breakdown"]

    assert updated_breakdown["y1_income_effective"] > base_breakdown["y1_income_effective"]
    assert updated_breakdown["opex_cost"] > base_breakdown["opex_cost"]
    assert updated_breakdown["y1_noi"] > base_breakdown["y1_noi"]


def test_excel_scenario_uses_nested_site_area_and_land_price_overrides():
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
    base_notes = base_body["notes"]

    site_area_m2 = base_notes.get("site_area_m2") or base_notes.get("notes", {}).get("site_area_m2")
    assert site_area_m2 and site_area_m2 > 0

    updated_notes = dict(base_notes)
    updated_notes.pop("site_area_m2", None)
    updated_notes.pop("nfa_m2", None)
    nested_notes = dict(updated_notes.get("notes") or {})
    nested_notes["site_area_m2"] = site_area_m2
    updated_notes["notes"] = nested_notes

    cost_breakdown = dict(updated_notes.get("cost_breakdown") or {})
    cost_breakdown.pop("site_area_m2", None)
    land_cost = cost_breakdown.get("land_cost") or base_totals.get("land_value")
    land_price_final = cost_breakdown.get("land_price_final")
    if not land_price_final and site_area_m2 > 0:
        land_price_final = land_cost / site_area_m2
    cost_breakdown["land_cost"] = land_cost
    cost_breakdown["land_price_final"] = land_price_final
    updated_notes["cost_breakdown"] = cost_breakdown

    estimates_api._INMEM_HEADERS[estimate_id]["notes"] = updated_notes

    new_land_price = land_price_final * 1.1
    scenario_response = client.post(
        f"/v1/estimates/{estimate_id}/scenario",
        json={"far": 3.1, "land_price_sar_m2": new_land_price},
    )
    assert scenario_response.status_code == 200
    scenario_body = scenario_response.json()
    updated_breakdown = scenario_body["notes"]["cost_breakdown"]

    assert scenario_body["totals"]["p50_profit"] != base_totals["p50_profit"]
    assert updated_breakdown["land_cost"] == pytest.approx(new_land_price * site_area_m2, rel=1e-6)
    assert updated_breakdown["land_price_final"] == pytest.approx(new_land_price, rel=1e-6)


def test_excel_scenario_includes_scenario_overrides_in_notes():
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
    base_notes = base_body["notes"]
    base_notes["notes"] = dict(base_notes.get("notes") or {"nested": True})
    estimates_api._INMEM_HEADERS[estimate_id]["notes"] = base_notes

    scenario_response = client.post(
        f"/v1/estimates/{estimate_id}/scenario",
        json={"far": 3.5, "land_price_sar_m2": 4250},
    )
    assert scenario_response.status_code == 200
    scenario_body = scenario_response.json()
    overrides = scenario_body["notes"]["scenario_overrides"]
    nested_overrides = scenario_body["notes"]["notes"]["scenario_overrides"]

    assert overrides["far"] == pytest.approx(3.5, rel=1e-6)
    assert overrides["land_price_sar_m2"] == pytest.approx(4250, rel=1e-6)
    assert overrides["area_ratio"] > 0
    assert nested_overrides == overrides
