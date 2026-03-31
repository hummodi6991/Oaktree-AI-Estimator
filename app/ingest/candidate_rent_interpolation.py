"""
Interpolate rent and area for candidate_location rows missing actual values.

Fallback chain for rent:
  1. IDW from nearest commercial_unit comps within 1km
  2. District median from expansion_rent_comp
  3. City default (900 SAR/m²/year)

Area inference for Tier 2 (delivery/POI):
  - Uses category-based defaults when area_sqm is NULL

Usage:
    python -m app.ingest.candidate_rent_interpolation [--run-id RUN_ID]
"""

import argparse
import logging
import time
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Category-based area defaults (m²) for Tier 2 candidates
CATEGORY_AREA_DEFAULTS = {
    "burger": 120, "pizza": 130, "fried_chicken": 110,
    "shawarma": 80, "falafel": 60, "grills": 150,
    "indian": 160, "asian": 140, "italian": 150,
    "coffee": 100, "bakery": 90, "dessert": 80,
    "juice": 50, "seafood": 160, "turkish": 150,
    "lebanese": 150, "yemeni": 120, "bukhari": 140,
}
DEFAULT_AREA_SQM = 120.0

# City-wide fallback rent
CITY_DEFAULT_RENT_SAR_M2_YEAR = 900.0


def _step1_infer_area(db: Session) -> int:
    """Infer area_sqm for Tier 2 candidates using category-based defaults.

    Only updates rows where area_sqm IS NULL and area_confidence != 'actual'.
    Uses current_category to look up the default.
    """
    updated = 0

    # First: update by known categories
    for category, area in CATEGORY_AREA_DEFAULTS.items():
        result = db.execute(text("""
            UPDATE candidate_location
            SET area_sqm = :area,
                area_confidence = 'category_inferred'
            WHERE area_sqm IS NULL
              AND source_tier = 2
              AND current_category IS NOT NULL
              AND lower(current_category) LIKE :pattern
        """), {"area": area, "pattern": f"%{category}%"})
        updated += result.rowcount

    # Then: set default for any remaining NULL area Tier 2 candidates
    result = db.execute(text("""
        UPDATE candidate_location
        SET area_sqm = :default_area,
            area_confidence = 'default'
        WHERE area_sqm IS NULL
          AND source_tier = 2
    """), {"default_area": DEFAULT_AREA_SQM})
    updated += result.rowcount

    db.commit()
    logger.info("Area inference: updated %d Tier 2 candidates", updated)
    return updated


def _step2_rent_from_comps(db: Session) -> int:
    """IDW rent interpolation from nearest commercial_unit records within 1km.

    For each candidate missing actual rent, find all commercial_unit rows
    with known rent and area within 1km. Weight each comp by 1/distance
    (minimum distance clamped to 10m to avoid division issues).

    Updates rent_sar_m2_month, rent_sar_annual, and rent_confidence.
    """
    sql = text("""
        UPDATE candidate_location cl
        SET rent_sar_m2_month = sub.rent_m2_month,
            rent_sar_annual = CASE
                WHEN cl.area_sqm IS NOT NULL AND cl.area_sqm > 0
                THEN ROUND(sub.rent_m2_month * cl.area_sqm * 12, 2)
                ELSE NULL
            END,
            rent_confidence = 'comp_interpolated'
        FROM (
            SELECT
                cl2.id,
                ROUND((
                    SUM(
                        (cu.price_sar_annual / cu.area_sqm / 12.0)
                        * (1.0 / GREATEST(
                            ST_Distance(
                                cl2.geom::geography,
                                ST_SetSRID(ST_MakePoint(cu.lon::float, cu.lat::float), 4326)::geography
                            ),
                            10.0
                        ))
                    )
                    /
                    SUM(
                        1.0 / GREATEST(
                            ST_Distance(
                                cl2.geom::geography,
                                ST_SetSRID(ST_MakePoint(cu.lon::float, cu.lat::float), 4326)::geography
                            ),
                            10.0
                        )
                    )
                )::numeric, 2) AS rent_m2_month
            FROM candidate_location cl2
            JOIN commercial_unit cu
                ON ST_DWithin(
                    cl2.geom::geography,
                    ST_SetSRID(ST_MakePoint(cu.lon::float, cu.lat::float), 4326)::geography,
                    1000  -- 1km radius
                )
            WHERE (cl2.rent_confidence IS NULL OR cl2.rent_confidence NOT IN ('actual', 'comp_interpolated'))
              AND cl2.is_cluster_primary = TRUE
              AND cu.price_sar_annual IS NOT NULL
              AND cu.price_sar_annual > 0
              AND cu.area_sqm IS NOT NULL
              AND cu.area_sqm > 0
              AND cu.lat IS NOT NULL
              AND cu.lon IS NOT NULL
            GROUP BY cl2.id
            HAVING COUNT(*) >= 1
        ) sub
        WHERE cl.id = sub.id
    """)
    result = db.execute(sql)
    count = result.rowcount
    db.commit()
    logger.info("Rent from comps (IDW 1km): updated %d candidates", count)
    return count


def _step3_rent_from_district_median(db: Session) -> int:
    """Fill remaining candidates using district median from expansion_rent_comp.

    Matches on district_ar (Arabic name) against expansion_rent_comp.district.
    Uses the median rent_sar_m2_year from commercial/retail comps.
    """
    sql = text("""
        UPDATE candidate_location cl
        SET rent_sar_m2_month = sub.median_rent_month,
            rent_sar_annual = CASE
                WHEN cl.area_sqm IS NOT NULL AND cl.area_sqm > 0
                THEN ROUND(sub.median_rent_month * cl.area_sqm * 12, 2)
                ELSE NULL
            END,
            rent_confidence = 'district_median'
        FROM (
            SELECT
                erc.district,
                ROUND((
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY erc.rent_sar_m2_year) / 12.0)::numeric, 2) AS median_rent_month
            FROM expansion_rent_comp erc
            WHERE erc.city = 'riyadh'
              AND erc.rent_sar_m2_year IS NOT NULL
              AND erc.rent_sar_m2_year > 0
            GROUP BY erc.district
            HAVING COUNT(*) >= 1
        ) sub
        WHERE cl.district_ar IS NOT NULL
          AND lower(cl.district_ar) = lower(sub.district)
          AND (cl.rent_confidence IS NULL OR cl.rent_confidence NOT IN ('actual', 'comp_interpolated'))
          AND cl.is_cluster_primary = TRUE
    """)
    result = db.execute(sql)
    count = result.rowcount
    db.commit()
    logger.info("Rent from district median: updated %d candidates", count)
    return count


def _step4_rent_city_default(db: Session) -> int:
    """Fill any remaining candidates with city-wide default rent."""
    rent_m2_month = round(CITY_DEFAULT_RENT_SAR_M2_YEAR / 12.0, 2)
    sql = text("""
        UPDATE candidate_location
        SET rent_sar_m2_month = :rent_month,
            rent_sar_annual = CASE
                WHEN area_sqm IS NOT NULL AND area_sqm > 0
                THEN ROUND((:rent_month * area_sqm * 12)::numeric, 2)
                ELSE NULL
            END,
            rent_confidence = 'city_default'
        WHERE (rent_confidence IS NULL OR rent_confidence NOT IN ('actual', 'comp_interpolated', 'district_median'))
          AND is_cluster_primary = TRUE
    """)
    result = db.execute(sql, {"rent_month": rent_m2_month})
    count = result.rowcount
    db.commit()
    logger.info("Rent city default: updated %d candidates", count)
    return count


def interpolate_rents(db: Session) -> dict[str, Any]:
    """Main entry point: run all interpolation steps in sequence."""
    t_start = time.time()
    stats: dict[str, Any] = {}

    stats["area_inferred"] = _step1_infer_area(db)
    stats["rent_from_comps"] = _step2_rent_from_comps(db)
    stats["rent_from_district"] = _step3_rent_from_district_median(db)
    stats["rent_city_default"] = _step4_rent_city_default(db)

    # Summary stats
    summary = db.execute(text("""
        SELECT rent_confidence, COUNT(*) AS cnt
        FROM candidate_location
        WHERE is_cluster_primary = TRUE
        GROUP BY rent_confidence
        ORDER BY cnt DESC
    """)).fetchall()
    stats["rent_confidence_breakdown"] = {r[0] or "null": r[1] for r in summary}

    area_summary = db.execute(text("""
        SELECT area_confidence, COUNT(*) AS cnt
        FROM candidate_location
        WHERE is_cluster_primary = TRUE
        GROUP BY area_confidence
        ORDER BY cnt DESC
    """)).fetchall()
    stats["area_confidence_breakdown"] = {r[0] or "null": r[1] for r in area_summary}

    stats["elapsed_s"] = round(time.time() - t_start, 1)
    logger.info("Rent interpolation complete in %.1fs: %s", stats["elapsed_s"], stats)
    return stats


def main():
    parser = argparse.ArgumentParser(description="Interpolate rent/area for candidate_location")
    parser.parse_args()  # no args needed currently

    db = SessionLocal()
    try:
        stats = interpolate_rents(db)
        print("\n=== Rent Interpolation Results ===")
        for k, v in stats.items():
            print(f"  {k}: {v}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
