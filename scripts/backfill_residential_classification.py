#!/usr/bin/env python3
"""Backfill restaurant_suitable on existing commercial_unit rows
using the new residential keyword detection logic.

Run once after deploying the residential filter patch.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.db.session import SessionLocal


# Same keywords as classify_restaurant_suitability — keep in sync
_RESIDENTIAL_AR_KEYWORDS = [
    "شقة", "شقق", "سكني", "سكنية", "للسكن",
    "دور سكني", "دور للسكن", "غرفة نوم", "غرف نوم",
    "مفروشة", "مفروش", "عوائل", "عزاب",
    "شقق مفروشة", "سكن طالبات", "سكن موظفات", "سكن عمال",
    "استوديو", "ستوديو",
    "عمارة سكنية", "مبنى سكني", "بناء سكني",
    "فلة", "فيلا", "دوبلكس",
]

_RESIDENTIAL_EN_KEYWORDS = [
    "apartment", "apartments", "residential",
    "accommodation", "accomodation",
    "housing", "furnished",
    "studio", "bedroom", "bedrooms",
    "women's accommodation", "womens accommodation",
    "employees accommodation", "students accommodation",
    "workers housing", "staff housing",
    "villa", "duplex", "townhouse",
    "single family", "single-family",
    "for living", "for residence",
]


def main():
    db = SessionLocal()
    try:
        # Fetch all active listings with title/description
        rows = db.execute(text("""
            SELECT aqar_id, title, description
            FROM commercial_unit
            WHERE status = 'active'
        """)).mappings().all()

        print(f"Scanning {len(rows)} active listings...")

        rejected_ids = []
        for r in rows:
            title_desc_raw = f"{r['title'] or ''} {r['description'] or ''}"
            combined_lower = title_desc_raw.lower()

            is_residential = False
            for kw in _RESIDENTIAL_AR_KEYWORDS:
                if kw in title_desc_raw:
                    is_residential = True
                    break
            if not is_residential:
                for kw in _RESIDENTIAL_EN_KEYWORDS:
                    if kw in combined_lower:
                        is_residential = True
                        break

            if is_residential:
                rejected_ids.append(r["aqar_id"])

        print(f"Found {len(rejected_ids)} residential listings to mark unsuitable")

        if rejected_ids:
            # Update in batches
            batch_size = 500
            for i in range(0, len(rejected_ids), batch_size):
                batch = rejected_ids[i:i+batch_size]
                db.execute(text("""
                    UPDATE commercial_unit
                    SET restaurant_suitable = FALSE,
                        restaurant_score = 0
                    WHERE aqar_id = ANY(:ids)
                """), {"ids": batch})
                db.commit()
                print(f"  Updated batch {i//batch_size + 1}/{(len(rejected_ids) + batch_size - 1)//batch_size}")

        # Mark everything else as suitable (in case some were False from before)
        result = db.execute(text("""
            UPDATE commercial_unit
            SET restaurant_suitable = TRUE
            WHERE status = 'active'
              AND aqar_id != ALL(:rejected_ids)
              AND (restaurant_suitable IS NULL OR restaurant_suitable = FALSE)
              AND restaurant_score IS DISTINCT FROM 0
        """), {"rejected_ids": rejected_ids or [""]})
        db.commit()
        print(f"Marked {result.rowcount} listings as suitable")

        # Final summary
        summary = db.execute(text("""
            SELECT restaurant_suitable, COUNT(*)
            FROM commercial_unit
            WHERE status = 'active'
            GROUP BY restaurant_suitable
            ORDER BY restaurant_suitable
        """)).all()
        print("\nFinal counts:")
        for row in summary:
            print(f"  restaurant_suitable={row[0]}: {row[1]}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
