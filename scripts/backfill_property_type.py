#!/usr/bin/env python3
"""Backfill property_type, is_furnished, apartments_count, and description on
existing commercial_unit rows by re-fetching each listing's detail page from Aqar.

This script populates the new columns for listings scraped before these
extraction features were added. It also overwrites the metadata-garbage
descriptions with real ad body text.

Resumable: skips listings that already have apartments_count set.
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
    _extract_apartments_count,
    _extract_num_rooms,
    _extract_description,
)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def fetch_listing_metadata(url: str) -> tuple[str | None, bool, int | None, int | None, str | None]:
    """Fetch a listing detail page and extract property_type, is_furnished, apartments_count, num_rooms, description."""
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        return (
            _extract_property_type(soup),
            _extract_is_furnished(soup),
            _extract_apartments_count(soup),
            _extract_num_rooms(soup),
            _extract_description(soup),
        )
    except Exception as e:
        print(f"  ERROR fetching {url}: {e}")
        return None, False, None, None, None


def main():
    db = SessionLocal()
    try:
        # Fetch listings that need backfill (resumable: skip ones already done)
        rows = db.execute(text("""
            SELECT aqar_id, listing_url
            FROM commercial_unit
            WHERE status = 'active'
              AND num_rooms IS NULL
              AND listing_url IS NOT NULL
            ORDER BY aqar_id
        """)).mappings().all()

        print(f"Backfilling {len(rows)} listings...")

        success_count = 0
        residential_count = 0
        furnished_count = 0
        multi_apt_count = 0
        high_rooms_count = 0
        error_count = 0

        for i, row in enumerate(rows):
            aqar_id = row["aqar_id"]
            url = row["listing_url"]

            property_type, is_furnished, apartments_count, num_rooms, description = fetch_listing_metadata(url)

            # Always overwrite description — the existing rows have metadata garbage
            db.execute(text("""
                UPDATE commercial_unit
                SET property_type = COALESCE(:pt, property_type),
                    is_furnished = COALESCE(:furn, is_furnished),
                    apartments_count = :apt,
                    num_rooms = :rooms,
                    description = COALESCE(:desc, description)
                WHERE aqar_id = :id
            """), {
                "pt": property_type,
                "furn": is_furnished,
                "apt": apartments_count,
                "rooms": num_rooms,
                "desc": description,
                "id": aqar_id,
            })
            db.commit()
            success_count += 1

            if property_type and property_type.lower() in ("residential", "سكني"):
                residential_count += 1
            if is_furnished:
                furnished_count += 1
            if apartments_count and apartments_count >= 2:
                multi_apt_count += 1
            if num_rooms and num_rooms >= 6:
                high_rooms_count += 1

            if (i + 1) % 50 == 0:
                print(
                    f"  [{i+1}/{len(rows)}] success={success_count}, "
                    f"residential={residential_count}, furnished={furnished_count}, "
                    f"multi_apt={multi_apt_count}, high_rooms={high_rooms_count}, "
                    f"errors={error_count}"
                )

            # Rate limit
            time.sleep(random.uniform(2, 3))

        print(f"\n=== Done ===")
        print(f"  Total processed: {len(rows)}")
        print(f"  Successful: {success_count}")
        print(f"  Residential found: {residential_count}")
        print(f"  Furnished found: {furnished_count}")
        print(f"  Multi-apartment buildings (>=2): {multi_apt_count}")
        print(f"  High-rooms buildings (>=6): {high_rooms_count}")
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

        print("\nApartments count distribution:")
        for row in db.execute(text("""
            SELECT
                CASE
                    WHEN apartments_count IS NULL THEN '(NULL)'
                    WHEN apartments_count = 0 THEN '0'
                    WHEN apartments_count = 1 THEN '1'
                    WHEN apartments_count BETWEEN 2 AND 5 THEN '2-5'
                    WHEN apartments_count BETWEEN 6 AND 10 THEN '6-10'
                    ELSE '11+'
                END AS bucket,
                COUNT(*)
            FROM commercial_unit
            WHERE status = 'active'
            GROUP BY bucket
            ORDER BY bucket
        """)).all():
            print(f"  {row[0]}: {row[1]}")

        print("\nRooms count distribution:")
        for row in db.execute(text("""
            SELECT
                CASE
                    WHEN num_rooms IS NULL THEN '(NULL)'
                    WHEN num_rooms = 0 THEN '0'
                    WHEN num_rooms BETWEEN 1 AND 5 THEN '1-5'
                    WHEN num_rooms BETWEEN 6 AND 10 THEN '6-10'
                    WHEN num_rooms BETWEEN 11 AND 20 THEN '11-20'
                    ELSE '21+'
                END AS bucket,
                COUNT(*)
            FROM commercial_unit
            WHERE status = 'active'
            GROUP BY bucket
            ORDER BY bucket
        """)).all():
            print(f"  {row[0]}: {row[1]}")

        # Show what would be filtered out by ALL five rules
        print("\nListings that would be filtered out by new rules:")
        filtered = db.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE property_type IN ('Residential', 'سكني')) AS residential,
                COUNT(*) FILTER (WHERE is_furnished = TRUE AND listing_type = 'building') AS furnished_building,
                COUNT(*) FILTER (WHERE COALESCE(apartments_count, 0) >= 2 AND listing_type = 'building') AS multi_apt_building,
                COUNT(*) FILTER (WHERE COALESCE(num_rooms, 0) >= 6 AND listing_type = 'building') AS high_rooms_building,
                COUNT(*) FILTER (WHERE listing_type = 'warehouse') AS warehouses,
                COUNT(*) FILTER (
                    WHERE property_type IN ('Residential', 'سكني')
                       OR (is_furnished = TRUE AND listing_type = 'building')
                       OR (COALESCE(apartments_count, 0) >= 2 AND listing_type = 'building')
                       OR (COALESCE(num_rooms, 0) >= 6 AND listing_type = 'building')
                       OR listing_type = 'warehouse'
                ) AS total_excluded,
                COUNT(*) AS total_active
            FROM commercial_unit
            WHERE status = 'active'
        """)).first()
        print(f"  Residential (Aqar tag): {filtered[0]}")
        print(f"  Furnished buildings (offices): {filtered[1]}")
        print(f"  Multi-apartment buildings (residential): {filtered[2]}")
        print(f"  High-rooms buildings (>=6 rooms): {filtered[3]}")
        print(f"  Warehouses: {filtered[4]}")
        print(f"  Total excluded: {filtered[5]} / {filtered[6]}")
        print(f"  Remaining as candidates: {filtered[6] - filtered[5]}")

        # Verify description extraction worked
        print("\nDescription quality check:")
        desc_check = db.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE description LIKE '%Advertisement License%') AS still_metadata,
                COUNT(*) FILTER (WHERE description LIKE '%apartment%' OR description LIKE '%شقة%' OR description LIKE '%سكني%') AS has_residential_kw,
                COUNT(*) FILTER (WHERE description LIKE '%shop%' OR description LIKE '%commercial%' OR description LIKE '%محل%' OR description LIKE '%تجاري%') AS has_commercial_kw,
                COUNT(*) AS total
            FROM commercial_unit
            WHERE status = 'active'
        """)).first()
        print(f"  Still has metadata garbage: {desc_check[0]}")
        print(f"  Contains residential keywords: {desc_check[1]}")
        print(f"  Contains commercial keywords: {desc_check[2]}")
        print(f"  Total active: {desc_check[3]}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
