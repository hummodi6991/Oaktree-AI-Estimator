from datetime import date

from app.db.session import SessionLocal
from app.models.tables import MarketIndicator

SEED = [
    # Citywide baselines (adjust later with real feeds)
    {"date": date(2025, 6, 1), "city": "Riyadh", "asset_type": "residential", "indicator_type": "sale_price_per_m2", "value": 6500.0, "unit": "SAR/m2"},
    {"date": date(2025, 6, 1), "city": "Riyadh", "asset_type": "residential", "indicator_type": "rent_per_m2", "value": 230.0, "unit": "SAR/m2/mo"},
]


def main():
    db = SessionLocal()
    try:
        for record in SEED:
            exists = (
                db.query(MarketIndicator)
                .filter_by(
                    date=record["date"],
                    city=record["city"],
                    asset_type=record["asset_type"],
                    indicator_type=record["indicator_type"],
                )
                .first()
            )
            if not exists:
                db.add(MarketIndicator(**record))
        db.commit()
        print("Seeded market indicators.")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
