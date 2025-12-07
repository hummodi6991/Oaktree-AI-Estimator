from datetime import date

from app.db.session import SessionLocal
from app.models.tables import MarketIndicator


def main():
    db = SessionLocal()
    try:
        q = db.query(MarketIndicator).filter(
            MarketIndicator.city == "Riyadh",
            MarketIndicator.asset_type == "residential",
            MarketIndicator.indicator_type == "rent_per_m2",
            MarketIndicator.date == date(2025, 6, 1),
        )
        deleted = q.delete()
        db.commit()
        print(f"Deleted {deleted} seeded rent rows.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
