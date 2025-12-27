from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.models.tables import RentComp
from app.services.rent import aqar_rent_median


def _session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    RentComp.__table__.create(bind=engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)


def test_aqar_rent_median_clips_outliers():
    Session = _session()
    today = date.today()
    rents = [
        ("riyadh", "alpha", 10),
        ("riyadh", "alpha", 11),
        ("riyadh", "alpha", 12),
        ("riyadh", "alpha", 13),
        ("riyadh", "alpha", 14),
        ("riyadh", "alpha", 15),
        ("riyadh", "alpha", 200),  # outlier
        ("riyadh", "beta", 11),
        ("riyadh", "beta", 12),
        ("riyadh", "beta", 13),
        ("riyadh", "beta", 14),
        ("riyadh", "beta", 15),
        ("riyadh", "beta", 16),
        ("riyadh", "beta", 17),
        ("riyadh", "beta", 18),
        ("riyadh", "beta", 19),
        ("riyadh", "beta", 20),
        ("riyadh", "beta", 11),
        ("riyadh", "beta", 12),
        ("riyadh", "beta", 13),
        ("riyadh", "beta", 14),
    ]

    with Session() as session:
        for idx, (city, district, rent) in enumerate(rents, start=1):
            session.add(
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
        session.commit()

        district_median, city_median, n_district, n_city = aqar_rent_median(
            session, city="Riyadh", district="Alpha", asset_type="residential", unit_type="apartment"
        )

        # Outlier (200) should be clipped by p95, and 10 should be clipped by p05
        assert n_city == 19
        assert n_district == 5
        assert district_median == pytest.approx(13)
        assert city_median == pytest.approx(14)
