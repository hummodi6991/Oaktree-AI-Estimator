import pytest
from fastapi.testclient import TestClient

from app.db.deps import get_db
from app.main import app
from app.services.excel_method import allocate_mixed_use_area_ratio
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


@pytest.fixture(autouse=True)
def override_db_dependency():
    def _override():
        session = DummySession()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = _override
    yield
    app.dependency_overrides.pop(get_db, None)


client = TestClient(app)


def _mixed_template_inputs():
    return {
        "land_use_code": "m",
        "area_ratio": {"residential": 1.2, "retail": 0.6, "office": 0.4, "basement": 1.0},
        "unit_cost": {"residential": 2200, "retail": 2600, "office": 2400, "basement": 1200},
        "efficiency": {"residential": 0.82, "retail": 0.88, "office": 0.72},
        "cp_sqm_per_space": {"residential": 35, "retail": 30, "office": 28, "basement": 30},
        "rent_sar_m2_yr": {"residential": 2400, "retail": 3500, "office": 3000},
        "fitout_rate": 400,
        "contingency_pct": 0.1,
        "consultants_pct": 0.06,
        "feasibility_fee": 1500000,
        "transaction_pct": 0.05,
        "land_price_sar_m2": 2800,
    }


def _polygon():
    return {
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


def test_allocate_mixed_use_area_ratio_uses_weights_and_caps():
    inputs = _mixed_template_inputs()
    inputs["rent_sar_m2_yr"].update({"retail": 6000, "office": 1500})
    inputs["efficiency"].update({"retail": 0.9, "office": 0.7})

    updated, meta = allocate_mixed_use_area_ratio(3.0, inputs)
    allocated = meta["allocated_far"]

    assert allocated["total"] == pytest.approx(3.0)
    assert allocated["retail"] <= 0.8  # retail cap
    assert allocated["office"] <= 0.6  # office cap
    assert allocated["retail"] > allocated["office"]
    assert updated["area_ratio"]["basement"] == 1.0


def test_allocate_mixed_use_area_ratio_shifts_mix_when_office_is_weaker():
    strong_office_inputs = _mixed_template_inputs()
    weak_office_inputs = _mixed_template_inputs()
    strong_office_inputs["rent_sar_m2_yr"].update({"office": 4500})
    weak_office_inputs["rent_sar_m2_yr"].update({"office": 800})

    _, strong_meta = allocate_mixed_use_area_ratio(2.5, strong_office_inputs)
    _, weak_meta = allocate_mixed_use_area_ratio(2.5, weak_office_inputs)

    assert weak_meta["allocated_far"]["office"] < strong_meta["allocated_far"]["office"]
    assert weak_meta["allocated_far"]["total"] == pytest.approx(2.5)
    assert strong_meta["allocated_far"]["total"] == pytest.approx(2.5)


def test_program_allocator_applies_for_mixed_use_templates():
    payload = {
        "geometry": _polygon(),
        "asset_program": "residential_midrise",
        "unit_mix": [{"type": "1BR", "count": 5}],
        "finish_level": "mid",
        "timeline": {"start": "2025-10-01", "months": 18},
        "financing_params": {"margin_bps": 250, "ltv": 0.6},
        "strategy": "build_to_sell",
        "far": 3.2,
        "excel_inputs": _mixed_template_inputs(),
    }
    payload["excel_inputs"]["rent_sar_m2_yr"].update({"retail": 7000, "office": 1200})

    response = client.post("/v1/estimates", json=payload)
    assert response.status_code == 200
    notes = response.json()["notes"]

    allocator_meta = notes["program_allocator"]
    assert allocator_meta is not None
    allocated = allocator_meta["allocated_far"]
    assert allocated["total"] == pytest.approx(payload["far"], rel=1e-4)
    assert allocated["retail"] > allocated["office"]

    explanations = notes["excel_breakdown"]["explanations"]
    assert "Mixed-use program mix auto-allocated" in explanations["area_ratio_override"]


def test_program_allocator_skips_when_area_ratio_is_manual_or_residential():
    # Manual note should bypass allocator
    mixed_inputs = _mixed_template_inputs()
    mixed_inputs["area_ratio_note"] = "user override"
    payload_manual = {
        "geometry": _polygon(),
        "asset_program": "residential_midrise",
        "unit_mix": [],
        "finish_level": "mid",
        "timeline": {"start": "2025-10-01", "months": 18},
        "financing_params": {"margin_bps": 250, "ltv": 0.6},
        "strategy": "build_to_sell",
        "far": 2.8,
        "excel_inputs": mixed_inputs,
    }

    res_payload = {
        "geometry": _polygon(),
        "asset_program": "residential_midrise",
        "unit_mix": [],
        "finish_level": "mid",
        "timeline": {"start": "2025-10-01", "months": 18},
        "financing_params": {"margin_bps": 250, "ltv": 0.6},
        "strategy": "build_to_sell",
        "far": 2.0,
        "excel_inputs": sample_excel_inputs(),
    }

    resp_manual = client.post("/v1/estimates", json=payload_manual)
    resp_res = client.post("/v1/estimates", json=res_payload)

    assert resp_manual.status_code == 200
    assert resp_res.status_code == 200
    assert resp_manual.json()["notes"]["program_allocator"] is None
    assert resp_res.json()["notes"]["program_allocator"] is None
