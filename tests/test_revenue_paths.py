import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles

from app.db.deps import get_db
from app.main import app
from app.models.tables import (
    EstimateHeader,
    EstimateLine,
    ExternalFeature,
    LandUseStat,
    MarketIndicator,
)
import app.api.estimates as estimates_api
from app.services import indicators as indicators_svc


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
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    for table in (
        ExternalFeature.__table__,
        MarketIndicator.__table__,
        EstimateHeader.__table__,
        EstimateLine.__table__,
        LandUseStat.__table__,
    ):
        table.create(bind=engine)

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
    monkeypatch.setattr(
        estimates_api,
        "land_price_per_m2",
        lambda *args, **kwargs: (2800.0, {"n_comps": 0, "model": {}}),
    )
    monkeypatch.setattr(
        estimates_api,
        "compute_hard_costs",
        lambda *args, **kwargs: {"total": 1000.0, "cci_scalar": 1.0, "lines": []},
    )
    monkeypatch.setattr(
        estimates_api,
        "compute_financing",
        lambda *args, **kwargs: {
            "interest": 100.0,
            "apr": 0.08,
            "ltv": 0.6,
            "months": 18,
            "principal": 600.0,
        },
    )
    monkeypatch.setattr(estimates_api, "top_sale_comps", lambda *args, **kwargs: [])
    monkeypatch.setattr(estimates_api, "heuristic_drivers", lambda *args, **kwargs: [])


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

    payload = {
        "geometry": _simple_polygon(),
        "strategy": "build_to_sell",
        "far": 2.0,
        "efficiency": 1.0,
        "city": "Riyadh",
    }
    response = client.post("/v1/estimates", json=payload)
    assert response.status_code == 200
    data = response.json()

    expected_revenue = _sale_value(2362.0, 2.0, 1.0, 6500.0)
    assert data["totals"]["revenues"] == pytest.approx(expected_revenue, rel=1e-6)

    soft_costs = data["totals"]["soft_costs"]
    assert soft_costs == pytest.approx(data["totals"]["hard_costs"] * 0.12)
    assert data["notes"]["soft_cost_pct_defaulted"] is True

    sale_line = next(a for a in data["assumptions"] if a["key"] == "sale_price_per_m2")
    assert sale_line["value"] == pytest.approx(6500.0)
    assert sale_line["source_type"] == "Observed"


def test_btr_value_mvp(monkeypatch, client):
    monkeypatch.setattr(estimates_api.geo_svc, "area_m2", lambda geom: 5731.0)
    monkeypatch.setattr(estimates_api.geo_svc, "infer_far_from_features", lambda *args, **kwargs: None)
    monkeypatch.setattr(indicators_svc, "latest_sale_price_per_m2", lambda *args, **kwargs: 6500.0)
    monkeypatch.setattr(indicators_svc, "latest_rent_per_m2", lambda *args, **kwargs: 200.0)

    payload = {
        "geometry": _simple_polygon(),
        "strategy": "build_to_rent",
        "far": 2.0,
        "efficiency": 1.0,
        "city": "Riyadh",
    }
    response = client.post("/v1/estimates", json=payload)
    assert response.status_code == 200
    data = response.json()

    nfa = 5731.0 * 2.0 * 1.0
    egi = 200.0 * nfa * 0.92 * 12.0
    noi = egi * (1.0 - 0.30)
    expected_value = noi / 0.07
    assert data["totals"]["revenues"] == pytest.approx(expected_value, rel=1e-6)

    btr_notes = data["notes"].get("btr")
    assert btr_notes is not None
    assert btr_notes["nla_equals_nfa"] is True

    rent_line = next(a for a in data["assumptions"] if a["key"] == "rent_per_m2_month")
    assert rent_line["value"] == pytest.approx(200.0)
    assert rent_line["source_type"] == "Observed"


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

    payload = {
        "geometry": polygon,
        "strategy": "build_to_sell",
        "far": 2.0,
        "efficiency": 1.0,
        "city": "Riyadh",
    }
    response = client.post("/v1/estimates", json=payload)
    assert response.status_code == 200
    data = response.json()

    far_line = next(a for a in data["assumptions"] if a["key"] == "far")
    assert far_line["value"] == pytest.approx(3.5)
    assert far_line["source_type"] == "Observed"
    assert data["notes"]["far_source"] == "external_feature/rydpolygons"

    expected_nfa = 1000.0 * 3.5 * 1.0
    assert data["notes"]["nfa_m2"] == pytest.approx(expected_nfa)
