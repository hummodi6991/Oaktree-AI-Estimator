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


def test_kaggle_district_inference_differs(monkeypatch, client):
    # Simulate two Kaggle listings in different districts and ensure inference follows geometry
    listings = [
        {"lon": 46.675, "lat": 24.713, "district": "Alpha"},
        {"lon": 46.685, "lat": 24.723, "district": "Beta"},
    ]

    def _infer(db, city, lon, lat, max_radius_m=2000.0, geom_geojson=None):
        # Simple nearest-neighbour based on squared distance
        best = min(listings, key=lambda r: (r["lon"] - lon) ** 2 + (r["lat"] - lat) ** 2)
        return {
            "district_raw": best["district"],
            "district_normalized": best["district"].lower(),
            "method": "kaggle_nearest_listing",
            "distance_m": 10.0,
            "evidence_count": len(listings),
            "confidence": 0.9,
        }

    monkeypatch.setattr(estimates_api, "infer_district_from_kaggle", _infer)
    monkeypatch.setattr(estimates_api, "latest_re_price_index_scalar", lambda *args, **kwargs: 1.0)
    monkeypatch.setattr(estimates_api, "aqar_rent_median", lambda *args, **kwargs: (100.0, 90.0, 5, 12))
    monkeypatch.setattr(estimates_api, "latest_rega_residential_rent_per_m2", lambda *args, **kwargs: None)

    base_payload = _payload()
    payload_alpha = {**base_payload, "geometry": {"type": "Polygon", "coordinates": [[[46.675, 24.713], [46.676, 24.713], [46.676, 24.714], [46.675, 24.714], [46.675, 24.713]]]}}
    payload_beta = {**base_payload, "geometry": {"type": "Polygon", "coordinates": [[[46.685, 24.723], [46.686, 24.723], [46.686, 24.724], [46.685, 24.724], [46.685, 24.723]]]}}

    resp_alpha = client.post("/v1/estimates", json=payload_alpha)
    resp_beta = client.post("/v1/estimates", json=payload_beta)
    assert resp_alpha.status_code == 200
    assert resp_beta.status_code == 200

    body_alpha = resp_alpha.json()
    body_beta = resp_beta.json()

    assert body_alpha["notes"]["district"] == "alpha"
    assert body_beta["notes"]["district"] == "beta"
    assert body_alpha["notes"]["district_inference"]["district_raw"] == "Alpha"
    assert body_beta["notes"]["district_inference"]["district_raw"] == "Beta"
    assert body_alpha["notes"]["rent_debug_metadata"]["district_normalized_used"] == "alpha"
    assert body_beta["notes"]["rent_debug_metadata"]["district_normalized_used"] == "beta"
    assert body_alpha["notes"]["rent_debug_metadata"]["district_inference_method"] == "kaggle_nearest_listing"
    assert body_beta["notes"]["rent_debug_metadata"]["district_inference_method"] == "kaggle_nearest_listing"


def test_rent_differs_by_district(monkeypatch, client, db_session):
    today = date.today()
    rents = []
    for i in range(12):
        rents.append(("Riyadh", "Alpha", 100.0))
        rents.append(("Riyadh", "Beta", 200.0))

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

    monkeypatch.setattr(estimates_api, "latest_rega_residential_rent_per_m2", lambda *args, **kwargs: None)
    monkeypatch.setattr(estimates_api, "latest_re_price_index_scalar", lambda *args, **kwargs: 1.0)

    district_points = {
        "Alpha": (46.6755, 24.7135),
        "Beta": (46.6855, 24.7235),
    }

    def _infer(db, city, lon, lat, max_radius_m=2000.0, geom_geojson=None):
        nearest = min(
            district_points.items(),
            key=lambda item: (item[1][0] - lon) ** 2 + (item[1][1] - lat) ** 2,
        )
        district_name = nearest[0]
        return {
            "district_raw": district_name,
            "district_normalized": district_name.lower(),
            "method": "kaggle_nearest_listing",
            "distance_m": 5.0,
            "evidence_count": 2,
            "confidence": 0.9,
        }

    monkeypatch.setattr(estimates_api, "infer_district_from_kaggle", _infer)

    payload_alpha = {
        **_payload(),
        "geometry": {
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
        },
    }
    payload_beta = {
        **_payload(),
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [46.685, 24.723],
                    [46.686, 24.723],
                    [46.686, 24.724],
                    [46.685, 24.724],
                    [46.685, 24.723],
                ]
            ],
        },
    }

    def _run(payload):
        response = client.post("/v1/estimates", json=payload)
        assert response.status_code == 200
        data = response.json()
        rent_val = data["notes"]["excel_rent"]["rent_sar_m2_yr"]["residential"]
        rent_meta = data["notes"]["rent_debug_metadata"]
        return rent_val, rent_meta

    rent_alpha, meta_alpha = _run(payload_alpha)
    rent_beta, meta_beta = _run(payload_beta)

    assert rent_alpha == pytest.approx(1200.0)
    assert rent_beta == pytest.approx(2400.0)
    assert rent_alpha != rent_beta
    assert meta_alpha["rent_strategy"] == "aqar_district_median"
    assert meta_beta["rent_strategy"] == "aqar_district_median"
    assert meta_alpha["district_normalized_used"] == "alpha"
    assert meta_beta["district_normalized_used"] == "beta"
    assert meta_alpha["aqar_district_samples"] > 0
    assert meta_beta["aqar_district_samples"] > 0


def test_rent_scalar_and_applied_rent_logging(monkeypatch, client):
    monkeypatch.setattr(
        estimates_api,
        "latest_rega_residential_rent_per_m2",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(estimates_api, "latest_re_price_index_scalar", lambda *args, **kwargs: 1.1)
    monkeypatch.setattr(
        estimates_api,
        "aqar_rent_median",
        lambda *args, **kwargs: (110.0, 90.0, 12, 12),
    )
    monkeypatch.setattr(
        estimates_api,
        "infer_district_from_kaggle",
        lambda *args, **kwargs: {
            "district_raw": "Alpha",
            "district_normalized": "alpha",
            "method": "kaggle_nearest_listing",
            "distance_m": 12.0,
            "evidence_count": 10,
            "confidence": 0.8,
        },
    )

    response = client.post("/v1/estimates", json=_payload())
    assert response.status_code == 200
    body = response.json()

    rent_rates = body["notes"]["excel_rent"]["rent_sar_m2_yr"]
    breakdown = body["notes"]["excel_breakdown"]
    rent_applied = rent_rates["residential"]
    income_component = breakdown["y1_income_components"]["residential"]
    nla = breakdown["nla"]["residential"]

    rent_debug = body["notes"]["rent_debug_metadata"]

    assert rent_applied == pytest.approx(1452.0)  # 110 * 12 * 1.1
    assert income_component / nla == pytest.approx(rent_applied)
    assert rent_debug["district_normalized_used"] == "alpha"
    assert rent_debug["district_raw_inferred"] == "Alpha"
    assert rent_debug["rent_applied_sar_m2_yr"]["residential"] == pytest.approx(rent_applied)
    assert "Alpha" in rent_debug["district_raw_inferred"]
