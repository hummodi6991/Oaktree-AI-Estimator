from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.tables import MarketIndicator


DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "rega_indicators_combined.csv"

REQUIRED_COLUMNS = {"date", "city", "asset_type", "indicator_type", "value", "unit"}


def _coerce_date(value) -> date:
    text = str(value).strip()
    # Handles both YYYY-MM-DD and YYYY-MM formats safely
    return date.fromisoformat(text[:10])


def ingest_rega_indicators(db: Session, csv_path: Path | None = None) -> int:
    """
    Ingest REGA indicators from a CSV into the market_indicator table.

    Expected columns:
      - date (YYYY-MM or YYYY-MM-DD)
      - city
      - asset_type
      - indicator_type (e.g. rent_per_m2, sale_price_per_m2, rent_avg_unit)
      - value
      - unit
      - source_url (optional)
    """
    path = csv_path or DATA_PATH
    if not path.exists():
        raise SystemExit(f"CSV not found at {path}")

    df = pd.read_csv(path)
    df.columns = [c.lower().strip() for c in df.columns]

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise SystemExit(f"Missing columns in {path.name}: {sorted(missing)}")

    upserted = 0
    for _, r in df.iterrows():
        d = _coerce_date(r["date"])
        city = str(r["city"])
        asset_type = str(r["asset_type"])
        indicator_type = str(r["indicator_type"])

        row = (
            db.query(MarketIndicator)
            .filter_by(
                date=d,
                city=city,
                asset_type=asset_type,
                indicator_type=indicator_type,
            )
            .first()
        )

        value = float(r["value"])
        unit = str(r["unit"])
        source_url = str(r.get("source_url") or "") or None

        if row:
            row.value = value
            row.unit = unit
            row.source_url = source_url
        else:
            db.add(
                MarketIndicator(
                    date=d,
                    city=city,
                    asset_type=asset_type,
                    indicator_type=indicator_type,
                    value=value,
                    unit=unit,
                    source_url=source_url,
                )
            )
        upserted += 1

    db.commit()
    return upserted


if __name__ == "__main__":
    with SessionLocal() as db:
        n = ingest_rega_indicators(db)
        print(f"Ingested/updated {n} REGA indicator rows from {DATA_PATH.name}")
