from contextlib import contextmanager
from datetime import date

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

    def add(self, entry):
        self.added.append(entry)

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


def test_excel_rent_prefers_aqar_adjustment(monkeypatch):
    client = TestClient(app)
    monkeypatch.setattr(
        estimates_api,
        "latest_rega_residential_rent_per_m2",
        lambda *args, **kwargs: (100.0, "SAR/m²/month", date(2024, 1, 1), "https://rega.example"),
    )
    monkeypatch.setattr(
        estimates_api,
        "aqar_rent_median",
        lambda *args, **kwargs: (120.0, 80.0, 7, 21),
    )

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
    body = response.json()
    rent_meta = body["notes"]["excel_rent"]["rent_source_metadata"]
    rent_rates = body["notes"]["excel_rent"]["rent_sar_m2_yr"]

    expected_monthly = 100.0 * (120.0 / 80.0)
    assert rent_rates["residential"] == pytest.approx(expected_monthly * 12.0)
    assert rent_meta["method"] == "rega_city_scaled_by_aqar_ratio"
    assert rent_meta["aqar_sample_sizes"]["district"] == 7
    assert rent_meta["aqar_medians"]["city"] == 80.0
