from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.services.rent import aqar_rent_median
from app.models.tables import RentComp


@pytest.fixture
def db_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    RentComp.__table__.create(bind=engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def test_aqar_rent_median_returns_city_when_no_district(db_session):
    today = date.today()
    db_session.add(RentComp(
        id="t1",
        date=today,
        city="riyadh",
        district="العليا",
        asset_type="residential",
        lease_term_months=12,
        rent_per_unit=10000,
        rent_per_m2=200,
        source="kaggle_aqar",
        source_url=None,
        asof_date=today,
    ))
    db_session.commit()

    d_med, c_med, n_d, n_c = aqar_rent_median(db_session, city="Riyadh", district=None)
    assert d_med is None
    assert c_med is not None
    assert n_c > 0


def test_aqar_rent_median_hits_district_when_district_present(db_session):
    today = date.today()
    db_session.add(RentComp(
        id="t2",
        date=today,
        city="riyadh",
        district="العليا",
        asset_type="residential",
        lease_term_months=12,
        rent_per_unit=10000,
        rent_per_m2=300,
        source="kaggle_aqar",
        source_url=None,
        asof_date=today,
    ))
    db_session.commit()

    d_med, c_med, n_d, n_c = aqar_rent_median(db_session, city="Riyadh", district="العليا")
    assert d_med is not None
    assert n_d > 0
