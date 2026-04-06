#!/usr/bin/env python3
"""Backfill property_type and is_furnished on existing commercial_unit rows
by re-fetching each listing's detail page from Aqar.

This is a one-time script to populate the new columns for listings scraped
before property_type and is_furnished extraction was added.

Resumable: skips listings that already have property_type set.
Rate-limited: 2-3 second delay between requests.
"""

import os
import sys
import time
import random

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from bs4 import BeautifulSoup
from sqlalchemy import text
from app.db.session import SessionLocal

# Import the extraction functions from scrape_aqar
from scripts.scrape_aqar import (
    _extract_property_type,
    _extract_is_furnished,
    _extract_description,
)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def fetch_listing_metadata(url: str) -> tuple[str | None, bool, str | None]:
    """Fetch a listing detail page and extract property_type, is_furnished, description."""
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        return (
            _extract_property_type(soup),
            _extract_is_furnished(soup),
            _extract_description(soup),
        )
    except Exception as e:
        print(f"  ERROR fetching {url}: {e}")
        return None, False, None


def main():
    db = SessionLocal()
    try:
        # Fetch listings that need backfill (resumable: skip ones already done)
        rows = db.execute(text("""
            SELECT aqar_id, listing_url
            FROM commercial_unit
            WHERE status = 'active'
              AND property_type IS NULL
              AND is_furnished IS NULL
              AND listing_url IS NOT NULL
            ORDER BY aqar_id
        """)).mappings().all()

        print(f"Backfilling {len(rows)} listings...")

        success_count = 0
        residential_count = 0
        furnished_count = 0
        error_count = 0

        for i, row in enumerate(rows):
            aqar_id = row["aqar_id"]
            url = row["listing_url"]

            property_type, is_furnished, description = fetch_listing_metadata(url)

            # Update even if property_type is None — we still know it's not Residential
            db.execute(text("""
                UPDATE commercial_unit
                SET property_type = :pt,
                    is_furnished = :furn,
                    description = COALESCE(:desc, description)
                WHERE aqar_id = :id
            """), {
                "pt": property_type,
                "furn": is_furnished,
                "desc": description,
                "id": aqar_id,
            })
            db.commit()
            success_count += 1

            if property_type and property_type.lower() in ("residential", "سكني"):
                residential_count += 1
            if is_furnished:
                furnished_count += 1

            if (i + 1) % 50 == 0:
                print(
                    f"  [{i+1}/{len(rows)}] success={success_count}, "
                    f"residential={residential_count}, furnished={furnished_count}, "
                    f"errors={error_count}"
                )

            # Rate limit
            time.sleep(random.uniform(2, 3))

        print(f"\n=== Done ===")
        print(f"  Total processed: {len(rows)}")
        print(f"  Successful: {success_count}")
        print(f"  Residential found: {residential_count}")
        print(f"  Furnished found: {furnished_count}")
        print(f"  Errors: {error_count}")

        # Final summary
        print("\nProperty type distribution:")
        for row in db.execute(text("""
            SELECT property_type, COUNT(*)
            FROM commercial_unit
            WHERE status = 'active'
            GROUP BY property_type
            ORDER BY COUNT(*) DESC
        """)).all():
            print(f"  {row[0] or '(NULL)'}: {row[1]}")

        print("\nFurnished distribution by listing_type:")
        for row in db.execute(text("""
            SELECT listing_type, is_furnished, COUNT(*)
            FROM commercial_unit
            WHERE status = 'active'
            GROUP BY listing_type, is_furnished
            ORDER BY listing_type, is_furnished
        """)).all():
            print(f"  {row[0]} / furnished={row[1]}: {row[2]}")

        # Show what would be filtered out
        print("\nListings that would be filtered out by new rules:")
        filtered = db.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE property_type IN ('Residential', 'سكني')) AS residential,
                COUNT(*) FILTER (WHERE is_furnished = TRUE AND listing_type = 'building') AS furnished_building,
                COUNT(*) FILTER (
                    WHERE property_type IN ('Residential', 'سكني')
                       OR (is_furnished = TRUE AND listing_type = 'building')
                ) AS total_excluded,
                COUNT(*) AS total_active
            FROM commercial_unit
            WHERE status = 'active'
        """)).first()
        print(f"  Residential: {filtered[0]}")
        print(f"  Furnished buildings (offices): {filtered[1]}")
        print(f"  Total excluded: {filtered[2]} / {filtered[3]}")
        print(f"  Remaining as candidates: {filtered[3] - filtered[2]}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
