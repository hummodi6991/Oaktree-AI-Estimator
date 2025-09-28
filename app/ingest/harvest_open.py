from datetime import date

from sqlalchemy.orm import Session

from app.models.tables import CostIndexMonthly, Rate, MarketIndicator
from app.connectors.gastat import fetch_cci_rows
from app.connectors.sama import fetch_rates
from app.connectors.rega import fetch_market_indicators


def upsert_cci(db: Session) -> int:
    n = 0
    for r in fetch_cci_rows():
        d = date.fromisoformat(str(r["month"])[:7] + "-01")
        row = db.query(CostIndexMonthly).filter_by(month=d, sector="construction").first()
        if row:
            row.cci_index = float(r["cci_index"])
            row.source_url = r.get("source_url")
        else:
            db.add(
                CostIndexMonthly(
                    month=d,
                    sector="construction",
                    cci_index=float(r["cci_index"]),
                    source_url=r.get("source_url"),
                )
            )
        n += 1
    db.commit()
    return n


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
