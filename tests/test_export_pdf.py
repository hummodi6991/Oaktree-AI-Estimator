from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

from app.db.deps import get_db
from app.main import app
from tests.excel_inputs import sample_excel_inputs

pytest.importorskip("fpdf")


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


def _make_estimate() -> str:
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
        "unit_mix": [{"type": "1BR", "count": 10, "avg_m2": 60}],
        "finish_level": "mid",
        "timeline": {"start": "2025-10-01", "months": 18},
        "financing_params": {"margin_bps": 250, "ltv": 0.6},
        "strategy": "build_to_sell",
        "city": "Riyadh",
        "excel_inputs": sample_excel_inputs(),
    }
    response = client.post("/v1/estimates", json=payload)
    assert response.status_code == 200
    return response.json()["id"]


def test_pdf_export_roundtrip():
    estimate_id = _make_estimate()
    response = client.get(f"/v1/estimates/{estimate_id}/memo.pdf")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/pdf")
    assert len(response.content) > 500


def test_export_pdf_includes_excel_breakdown_from_wrapped_notes(monkeypatch):
    estimate_id = _make_estimate()
    captured = {}

    def fake_build_memo_pdf(
        title,
        totals,
        assumptions,
        top_comps,
        excel_breakdown=None,
    ):
        captured["excel_breakdown"] = excel_breakdown
        return b"%PDF-1.4\n%%EOF"

    monkeypatch.setattr("app.api.estimates.build_memo_pdf", fake_build_memo_pdf)
    response = client.get(f"/v1/estimates/{estimate_id}/memo.pdf")
    assert response.status_code == 200
    assert captured.get("excel_breakdown") is not None
