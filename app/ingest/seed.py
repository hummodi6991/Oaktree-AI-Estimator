from datetime import date
from app.db.session import SessionLocal
from app.models.tables import Rate, SaleComp

def upsert_rate(db, date_, tenor, rate_type, value, url=None):
    obj = db.query(Rate).filter_by(date=date_, tenor=tenor, rate_type=rate_type).first()
    if obj:
        obj.value = value
        obj.source_url = url
    else:
        db.add(Rate(date=date_, tenor=tenor, rate_type=rate_type, value=value, source_url=url))

def upsert_sale_comp(db, data):
    obj = db.query(SaleComp).get(data["id"])
    if obj:
        for k, v in data.items():
            setattr(obj, k, v)
    else:
        db.add(SaleComp(**data))

def main():
    db = SessionLocal()
    try:
        upsert_rate(db, date.fromisoformat("2025-06-01"), "overnight", "SAMA_base", 6.00, "https://example-sama")
        upsert_rate(db, date.fromisoformat("2025-06-01"), "1M", "SAIBOR", 6.10, "https://example-sama")
        upsert_sale_comp(db, {
            "id": "C-001",
            "date": date.fromisoformat("2025-06-15"),
            "city": "Riyadh",
            "district": "Al Olaya",
            "asset_type": "land",
            "net_area_m2": 1500,
            "price_total": 4200000,
            "price_per_m2": 2800,
            "source": "rega_indicator",
            "source_url": "https://example-rega",
            "asof_date": date.fromisoformat("2025-06-30"),
        })
        db.commit()
        print("Seed complete.")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main()
