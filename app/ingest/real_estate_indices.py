from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.tables import MarketIndicator


DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "real_estate_indices.csv"


def _quarter_to_month(q: str) -> int:
    return {"Q1": 3, "Q2": 6, "Q3": 9, "Q4": 12}.get(q, 12)  # annual -> Dec


def _asof_date(row) -> date:
    month = _quarter_to_month(row.quarter) if row.periodicity == "Quarterly" else 12
    return date(int(row.year), month, 1)


def build_rows(df: pd.DataFrame) -> list[MarketIndicator]:
    rows: list[MarketIndicator] = []
    for rec in df.itertuples(index=False):
        asof = _asof_date(rec)
        rows.append(
            MarketIndicator(
                date=asof,
                asof_date=asof,
                city="Saudi Arabia",  # national index
                asset_type=str(rec.indicator),  # e.g. "Residential Plot"
                indicator_type="real_estate_price_index",
                value=float(rec.value),
                unit="index_2014_100",
            )
        )
    return rows


def ingest_real_estate_indices(db: Session) -> int:
    df = pd.read_csv(DATA_PATH)

    # Clear old REPI rows to avoid duplicates
    db.query(MarketIndicator).filter(
        MarketIndicator.indicator_type == "real_estate_price_index",
        MarketIndicator.city == "Saudi Arabia",
    ).delete()

    rows = build_rows(df)
    db.add_all(rows)
    db.commit()
    return len(rows)


if __name__ == "__main__":
    with SessionLocal() as db:
        n = ingest_real_estate_indices(db)
        print(f"Ingested {n} real estate index rows")
