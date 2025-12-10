from datetime import date

from sqlalchemy.orm import Session

from app.models.tables import Rate, MarketIndicator
from app.connectors.sama import fetch_rates
from app.connectors.rega import fetch_market_indicators


def upsert_rates(db: Session) -> int:
    n = 0
    for r in fetch_rates():
        d = date.fromisoformat(str(r["date"])[:10])
        t = str(r["tenor"])
        rt = str(r["rate_type"])
        row = db.query(Rate).filter_by(date=d, tenor=t, rate_type=rt).first()
        if row:
            row.value = float(r["value"])
            row.source_url = r.get("source_url")
        else:
            db.add(
                Rate(
                    date=d,
                    tenor=t,
                    rate_type=rt,
                    value=float(r["value"]),
                    source_url=r.get("source_url"),
                )
            )
        n += 1
    db.commit()
    return n


def upsert_indicators(db: Session) -> int:
    n = 0
    for r in fetch_market_indicators():
        d = date.fromisoformat(str(r["date"])[:10])
        row = (
            db.query(MarketIndicator)
            .filter_by(
                date=d,
                city=r["city"],
                asset_type=r["asset_type"],
                indicator_type=r["indicator_type"],
            )
            .first()
        )
        if row:
            row.value = float(r["value"])
            row.unit = r["unit"]
            row.source_url = r.get("source_url")
        else:
            db.add(
                MarketIndicator(
                    date=d,
                    city=r["city"],
                    asset_type=r["asset_type"],
                    indicator_type=r["indicator_type"],
                    value=float(r["value"]),
                    unit=r["unit"],
                    source_url=r.get("source_url"),
                )
            )
        n += 1
    db.commit()
    return n


    if __name__ == "__main__":
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        n1 = upsert_rates(db)
        n2 = upsert_indicators(db)
        print(f"Harvest complete: rates={n1}, indicators={n2}")
    finally:
        db.close()
