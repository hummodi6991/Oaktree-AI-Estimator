from datetime import date

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.api import estimates as estimates_api
from app.db.deps import get_db
from app.main import app
from app.models.tables import RentComp
from tests.excel_inputs import sample_excel_inputs


@pytest.fixture
def SessionLocal():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    RentComp.__table__.create(bind=engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture
def db_session(SessionLocal):
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture(autouse=True)
def override_db_dependency(db_session):
    def _override_get_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db] = _override_get_db
    yield
    app.dependency_overrides.pop(get_db, None)


@pytest.fixture(autouse=True)
def force_inmemory_persistence(monkeypatch):
    monkeypatch.setattr(estimates_api, "_supports_sqlalchemy", lambda db: False)


@pytest.fixture(autouse=True)
def patch_tax_rate(monkeypatch):
    monkeypatch.setattr(estimates_api, "latest_tax_rate", lambda *args, **kwargs: None)


@pytest.fixture
def client():
    return TestClient(app)


def _payload():
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
    return {
        "geometry": poly,
        "asset_program": "residential_midrise",
        "unit_mix": [{"type": "1BR", "count": 10}],
        "finish_level": "mid",
        "timeline": {"start": "2025-10-01", "months": 18},
        "financing_params": {"margin_bps": 250, "ltv": 0.6},
        "strategy": "build_to_sell",
        "city": "Riyadh",
        "excel_inputs": sample_excel_inputs(),
    }


def test_revenue_rent_matches_noi(monkeypatch, client):
    monkeypatch.setattr(
        estimates_api,
        "latest_rega_residential_rent_per_m2",
        lambda *args, **kwargs: (100.0, "SAR/m²/month", date(2024, 1, 1), "https://rega.example"),
    )
    monkeypatch.setattr(estimates_api, "aqar_rent_median", lambda *args, **kwargs: (120.0, 80.0, 7, 21))
    monkeypatch.setattr(estimates_api, "latest_re_price_index_scalar", lambda *args, **kwargs: 1.0)
    monkeypatch.setattr(estimates_api.geo_svc, "infer_district_from_features", lambda *args, **kwargs: "alpha")

    response = client.post("/v1/estimates", json=_payload())
    assert response.status_code == 200
    body = response.json()

    rent_rates = body["notes"]["excel_rent"]["rent_sar_m2_yr"]
    excel_breakdown = body["notes"]["excel_breakdown"]
    rent_used = rent_rates["residential"]
    income_component = excel_breakdown["y1_income_components"]["residential"]
    nla = excel_breakdown["nla"]["residential"]
    explanation = excel_breakdown["explanations"]["y1_income"]

    assert nla > 0
    assert income_component / nla == pytest.approx(rent_used)
    assert body["notes"]["rent_debug_metadata"]["rent_strategy"] == "rega_x_aqar_ratio"
    assert "1,800 SAR/m²/year" in explanation


def test_rent_differs_by_district(monkeypatch, client, db_session):
    today = date.today()
    rents = []
    for i in range(5):
        rents.append(("Riyadh", "Alpha", 100.0))
        rents.append(("Riyadh", "Beta", 200.0))
    for i in range(5):
        rents.append(("Riyadh", "Gamma", 150.0))

    for idx, (city, district, rent) in enumerate(rents, start=1):
        db_session.add(
            RentComp(
                id=f"r{idx}",
                date=today,
                asof_date=today,
                city=city,
                district=district,
                asset_type="residential",
                unit_type="apartment",
                lease_term_months=12,
                rent_per_unit=None,
                rent_per_m2=rent,
                source="test",
                source_url=None,
            )
        )
    db_session.commit()

    monkeypatch.setattr(
        estimates_api,
        "latest_rega_residential_rent_per_m2",
        lambda *args, **kwargs: (150.0, "SAR/m²/month", date(2024, 1, 1), "https://rega.example/rent"),
    )
    monkeypatch.setattr(estimates_api, "latest_re_price_index_scalar", lambda *args, **kwargs: 1.0)

    current_district = {"value": "Alpha"}

    def _infer_district(db, geom, layer=None):
        return current_district["value"]

    monkeypatch.setattr(estimates_api.geo_svc, "infer_district_from_features", _infer_district)

    def _run():
        response = client.post("/v1/estimates", json=_payload())
        assert response.status_code == 200
        data = response.json()
        rent_val = data["notes"]["excel_rent"]["rent_sar_m2_yr"]["residential"]
        rent_meta = data["notes"]["rent_debug_metadata"]
        return rent_val, rent_meta

    rent_alpha, meta_alpha = _run()
    current_district["value"] = "Beta"
    rent_beta, meta_beta = _run()

    assert rent_alpha == pytest.approx(1200.0)
    assert rent_beta == pytest.approx(2400.0)
    assert rent_alpha != rent_beta
    assert meta_alpha["rent_strategy"] == "rega_x_aqar_ratio"
    assert meta_beta["rent_strategy"] == "rega_x_aqar_ratio"
    assert meta_alpha["district_used_for_aqar_query"] == "alpha"
    assert meta_beta["district_used_for_aqar_query"] == "beta"


def test_kaggle_district_fallback(monkeypatch, client):
    monkeypatch.setattr(estimates_api.geo_svc, "infer_district_from_features", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        estimates_api,
        "infer_district_from_kaggle",
        lambda db, lon, lat, city=None: ("gamma", 123.4),
    )
    monkeypatch.setattr(
        estimates_api,
        "latest_rega_residential_rent_per_m2",
        lambda *args, **kwargs: (90.0, "SAR/m²/month", date(2024, 1, 1), "https://rega.example"),
    )
    monkeypatch.setattr(estimates_api, "aqar_rent_median", lambda *args, **kwargs: (100.0, 80.0, 5, 10))
    monkeypatch.setattr(estimates_api, "latest_re_price_index_scalar", lambda *args, **kwargs: 1.0)

    response = client.post("/v1/estimates", json=_payload())
    assert response.status_code == 200
    body = response.json()

    rent_meta = body["notes"]["rent_debug_metadata"]
    assert rent_meta["district"] == "gamma"
    assert rent_meta["district_inference"] == {"method": "kaggle_nearest_listing", "distance_m": 123.4}
