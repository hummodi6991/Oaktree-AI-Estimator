from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.tables import AssumptionLedger, EstimateLine


def test_assumption_source_type_allows_riyadh_municipality() -> None:
    assert AssumptionLedger.__table__.c.source_type.type.length == 64
    assert EstimateLine.__table__.c.source_type.type.length == 64

    engine = create_engine("sqlite:///:memory:")
    AssumptionLedger.__table__.create(engine)
    Session = sessionmaker(bind=engine)

    session = Session()
    session.add(
        AssumptionLedger(
            estimate_id="estimate-1",
            line_id="line-1",
            source_type="Riyadh Municipality",
        )
    )
    session.commit()

    persisted = session.query(AssumptionLedger).first()
    assert persisted is not None
    assert persisted.source_type == "Riyadh Municipality"
