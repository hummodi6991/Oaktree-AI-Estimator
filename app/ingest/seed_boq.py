from app.db.session import SessionLocal
from app.models.tables import BoqItem

SEED = [
    {"code": "STRUC", "description": "Structure & frame", "uom": "m2", "quantity_per_m2": 1.0, "baseline_unit_cost": 850.0, "city_factor": 1.00},
    {"code": "ENVEL", "description": "Envelope & finishes", "uom": "m2", "quantity_per_m2": 1.0, "baseline_unit_cost": 1200.0, "city_factor": 1.00},
    {"code": "MEP",   "description": "MEP core",           "uom": "m2", "quantity_per_m2": 1.0, "baseline_unit_cost": 700.0, "city_factor": 1.00},
    {"code": "SITE",  "description": "Site works",         "uom": "m2", "quantity_per_m2": 0.15,"baseline_unit_cost": 500.0, "city_factor": 1.00},
]


def main():
    db = SessionLocal()
    try:
        for r in SEED:
            if not db.get(BoqItem, r["code"]):
                db.add(BoqItem(**r))
        db.commit()
        print("Seeded BoQ items.")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
