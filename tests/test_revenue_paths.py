from datetime import date

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles

from app.db.deps import get_db
from app.main import app
from app.models.base import Base
from app.models.tables import (
    CostIndexMonthly,
    EstimateHeader,
    EstimateLine,
    ExternalFeature,
    LandUseStat,
    MarketIndicator,
)
import app.api.estimates as estimates_api
from app.services import indicators as indicators_svc
from app.services.excel_method import compute_excel_estimate
from tests.excel_inputs import sample_excel_inputs


@compiles(JSONB, "sqlite")
def _compile_jsonb(element, compiler, **kw):
    return "JSON"


@pytest.fixture
def session_factory():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # Ensure at least one CCI row exists for tests that depend on cost indices.
    with TestingSessionLocal() as session:
        if not session.query(CostIndexMonthly).first():
            session.add(
                CostIndexMonthly(
                    month=date(2024, 1, 1),
                    sector="construction",
                    cci_index=100.0,
                    source_url="internal:test_fixture",
                    asof_date=date(2024, 1, 1),
                )
            )
            session.commit()

    def _session_maker():
        return TestingSessionLocal()

    yield _session_maker
    engine.dispose()


@pytest.fixture
def client(session_factory):
    def _override_get_db():
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = _override_get_db
    client = TestClient(app)
    yield client
    app.dependency_overrides.pop(get_db, None)
@pytest.fixture(autouse=True)
def stub_costs_and_land(monkeypatch):
    monkeypatch.setattr(estimates_api, "top_sale_comps", lambda *args, **kwargs: [])


def _simple_polygon():
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


def _sale_value(area: float, far: float, eff: float, price: float) -> float:
    return area * far * eff * price


def test_sale_revenue_formula_mvp(monkeypatch, client):
    monkeypatch.setattr(estimates_api.geo_svc, "area_m2", lambda geom: 2362.0)
    monkeypatch.setattr(estimates_api.geo_svc, "infer_far_from_features", lambda *args, **kwargs: None)
    monkeypatch.setattr(indicators_svc, "latest_sale_price_per_m2", lambda *args, **kwargs: 6500.0)
    monkeypatch.setattr(indicators_svc, "latest_rent_per_m2", lambda *args, **kwargs: 200.0)

    excel_inputs = sample_excel_inputs()
    payload = {
        "geometry": _simple_polygon(),
        "strategy": "build_to_sell",
        "far": 2.0,
        "efficiency": 1.0,
        "city": "Riyadh",
        "excel_inputs": excel_inputs,
    }
    response = client.post("/v1/estimates", json=payload)
    assert response.status_code == 200
    data = response.json()

    excel = compute_excel_estimate(2362.0, excel_inputs)
    assert data["totals"]["land_value"] == pytest.approx(excel["land_cost"], rel=1e-6)
    assert data["totals"]["hard_costs"] == pytest.approx(excel["sub_total"], rel=1e-6)
    assert data["totals"]["revenues"] == pytest.approx(excel["y1_income"], rel=1e-6)
    assert data["totals"]["p50_profit"] == pytest.approx(
        excel["y1_income"] - excel["grand_total_capex"], rel=1e-6
    )
    assert data["notes"]["excel_land_price"]["ppm2"] == excel_inputs["land_price_sar_m2"]


def test_btr_value_mvp(monkeypatch, client):
    monkeypatch.setattr(estimates_api.geo_svc, "area_m2", lambda geom: 5731.0)
    monkeypatch.setattr(estimates_api.geo_svc, "infer_far_from_features", lambda *args, **kwargs: None)
    monkeypatch.setattr(indicators_svc, "latest_sale_price_per_m2", lambda *args, **kwargs: 6500.0)
    monkeypatch.setattr(indicators_svc, "latest_rent_per_m2", lambda *args, **kwargs: 200.0)

    excel_inputs = sample_excel_inputs()
    payload = {
        "geometry": _simple_polygon(),
        "strategy": "build_to_rent",
        "far": 2.0,
        "efficiency": 1.0,
        "city": "Riyadh",
        "excel_inputs": excel_inputs,
    }
    response = client.post("/v1/estimates", json=payload)
    assert response.status_code == 200
    data = response.json()

    excel = compute_excel_estimate(5731.0, excel_inputs)
    assert data["totals"]["revenues"] == pytest.approx(excel["y1_income"], rel=1e-6)
    assert data["totals"]["excel_roi"] == pytest.approx(excel["roi"], rel=1e-6)


def test_auto_far_from_zoning(session_factory, monkeypatch, client):
    polygon = _simple_polygon()
    with session_factory() as session:
        session.add(
            ExternalFeature(
                layer_name="rydpolygons",
                feature_type="Polygon",
                geometry=polygon,
                properties={"FAR": "3.5"},
            )
        )
        session.commit()

    monkeypatch.setattr(estimates_api.geo_svc, "area_m2", lambda geom: 1000.0)
    monkeypatch.setattr(indicators_svc, "latest_sale_price_per_m2", lambda *args, **kwargs: 6000.0)
    monkeypatch.setattr(indicators_svc, "latest_rent_per_m2", lambda *args, **kwargs: 200.0)

    excel_inputs = sample_excel_inputs()
    payload = {
        "geometry": polygon,
        "strategy": "build_to_sell",
        "far": 2.0,
        "efficiency": 1.0,
        "city": "Riyadh",
        "excel_inputs": excel_inputs,
    }
    response = client.post("/v1/estimates", json=payload)
    assert response.status_code == 200
    data = response.json()

    excel = compute_excel_estimate(1000.0, excel_inputs)
    assert data["totals"]["land_value"] == pytest.approx(excel["land_cost"], rel=1e-6)
    assert data["notes"]["excel_land_price"]["ppm2"] == excel_inputs["land_price_sar_m2"]
