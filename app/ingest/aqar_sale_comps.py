from datetime import date
from sqlalchemy import text

from app.db.session import SessionLocal
from app.models.tables import SaleComp

SOURCE = "kaggle_aqar"

SQL = """
    SELECT
        city,
        district,
        area_sqm,
        price_sar,
        price_per_sqm
    FROM aqar.listings
    WHERE price_per_sqm IS NOT NULL
      AND area_sqm > 50
      AND price_sar > 50000
      -- keep only land listings (update this predicate if your flag differs)
      AND (
          lower(property_type) ~ '\\m(أرض|ارض|land|plot)\\M'
          OR lower(coalesce(title,'')) ~ '\\m(أرض|ارض|land|plot)\\M'
          OR lower(coalesce(description,'')) ~ '\\m(أرض|ارض|land|plot)\\M'
      )
"""


def main() -> None:
    db = SessionLocal()
    try:
        # clear any previous Kaggle-derived comps so the script is idempotent
        db.execute(text("DELETE FROM sale_comp WHERE source = :src"), {"src": SOURCE})
        db.commit()

        rows = db.execute(text(SQL)).mappings()

        today = date.today()
        inserted = 0
        for idx, r in enumerate(rows, start=1):
            comp = SaleComp(
                id=f"{SOURCE}_{idx}",
                date=today,
                asof_date=today,
                city=(r["city"] or "").strip() or "Riyadh",
                district=(r["district"] or "").strip() or None,
                asset_type="land",
                net_area_m2=float(r["area_sqm"]),
                price_total=float(r["price_sar"]),
                price_per_m2=float(r["price_per_sqm"]),
                source=SOURCE,
                source_url=None,
            )
            db.add(comp)
            inserted += 1

        db.commit()
        print(f"Inserted {inserted} Kaggle comps into sale_comp (source='{SOURCE}')")
    finally:
        db.close()


if __name__ == "__main__":
    main()
