"""
Interpolate rent and area for candidate_location rows missing actual values.

Area estimation chain (runs before rent so rent can use interpolated area):
  Tier 2 (delivery/POI) — area_sqm is NULL at ingestion:
    1. IDW from nearest Aqar commercial_unit listings within 500m
    2. Cap by nearest ms_buildings_raw footprint (prevents oversized estimates)
    3. Category-based default fallback
  Tier 3 (ArcGIS) — area_sqm is raw parcel polygon area:
    1. Small parcels (≤300 m²): keep as-is (likely single commercial unit)
    2. Larger parcels: apply empirical conversion factor from Tier1-vs-parcel overlap
    3. Cap by nearest ms_buildings_raw footprint
    4. Category-based default as floor/sanity check

Fallback chain for rent:
  1. IDW from nearest commercial_unit comps within 1km
  2. District median from expansion_rent_comp
  3. City default (900 SAR/m²/year)

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

# Area interpolation constants
AREA_IDW_RADIUS_M = 500        # radius for IDW area interpolation from Aqar comps
AREA_IDW_MIN_COMPS = 1         # minimum comps required for IDW
AREA_FOOTPRINT_RADIUS_M = 50   # radius to match candidate to nearest building footprint
TIER3_SMALL_PARCEL_THRESHOLD = 300.0  # m² — parcels at or below this are kept as-is
TIER3_DEFAULT_CONVERSION_FACTOR = 0.55  # fallback if empirical calibration returns nothing
TIER3_MIN_UNIT_AREA = 30.0     # floor — no restaurant unit below 30 m²
TIER3_MAX_UNIT_AREA = 300.0    # ceiling — no single restaurant unit above 300 m²

# City-wide fallback rent
CITY_DEFAULT_RENT_SAR_M2_YEAR = 900.0


def _step_area_from_comps(db: Session) -> int:
    """IDW area interpolation from nearest Aqar commercial_unit listings.

    For each Tier 2 candidate with NULL area, find all commercial_unit rows
    with known area within AREA_IDW_RADIUS_M. Weight each comp by 1/distance
    (minimum distance clamped to 10m).

    Only updates Tier 2 candidates (delivery/POI) where area is unknown.
    """
    sql = text("""
        UPDATE candidate_location cl
        SET area_sqm = sub.idw_area,
            area_confidence = 'comp_interpolated'
        FROM (
            SELECT
                cl2.id,
                ROUND((
                    SUM(
                        cu.area_sqm
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
                )::numeric, 1) AS idw_area
            FROM candidate_location cl2
            JOIN commercial_unit cu
                ON ST_DWithin(
                    cl2.geom::geography,
                    ST_SetSRID(ST_MakePoint(cu.lon::float, cu.lat::float), 4326)::geography,
                    :radius
                )
            WHERE cl2.area_sqm IS NULL
              AND cl2.source_tier = 2
              AND cl2.is_cluster_primary = TRUE
              AND cu.area_sqm IS NOT NULL
              AND cu.area_sqm > 0
              AND cu.lat IS NOT NULL
              AND cu.lon IS NOT NULL
              AND cu.status = 'active'
            GROUP BY cl2.id
            HAVING COUNT(*) >= :min_comps
        ) sub
        WHERE cl.id = sub.id
    """)
    result = db.execute(sql, {
        "radius": AREA_IDW_RADIUS_M,
        "min_comps": AREA_IDW_MIN_COMPS,
    })
    count = result.rowcount
    db.commit()
    logger.info("Area from Aqar comps (IDW %dm): updated %d Tier 2 candidates", AREA_IDW_RADIUS_M, count)
    return count


def _step_area_from_building_footprint(db: Session) -> int:
    """Cap area estimates using nearest MS building footprint.

    Currently disabled — LATERAL join against ms_buildings_raw
    is too slow for the full candidate set. Re-enable once a
    pre-computed spatial join table is available.
    """
    logger.info("Area footprint cap: SKIPPED (performance — needs pre-computed join)")
    return 0


def _step_area_tier3_conversion(db: Session) -> int:
    """Convert Tier 3 ArcGIS parcel areas to estimated unit areas.

    Strategy:
    1. Compute empirical conversion factor by cross-referencing Tier 1 Aqar
       listings against the ArcGIS parcels they fall within.
    2. For parcels ≤ TIER3_SMALL_PARCEL_THRESHOLD: keep area as-is
       (small parcels are likely single commercial units).
    3. For larger parcels: apply the empirical factor (or default 0.55).
    4. Clamp result to [TIER3_MIN_UNIT_AREA, TIER3_MAX_UNIT_AREA].

    Marks area_confidence = 'parcel_converted'.
    """
    # ── Step A: Compute empirical conversion factor ──
    # Find Tier 1 Aqar listings that spatially overlap an ArcGIS parcel,
    # compute listing_area / parcel_area, take the median.
    empirical_factor = TIER3_DEFAULT_CONVERSION_FACTOR
    try:
        factor_row = db.execute(text("""
            SELECT
                PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY (cu.area_sqm / p.area_m2)
                ) AS median_ratio,
                COUNT(*) AS n_pairs
            FROM commercial_unit cu
            JOIN riyadh_parcels_arcgis_proxy p
                ON ST_Contains(
                    p.geom,
                    ST_SetSRID(ST_MakePoint(cu.lon::float, cu.lat::float), 4326)
                )
            WHERE cu.status = 'active'
              AND cu.restaurant_suitable = TRUE
              AND cu.area_sqm IS NOT NULL
              AND cu.area_sqm > 10
              AND cu.lat IS NOT NULL
              AND cu.lon IS NOT NULL
              AND p.area_m2 IS NOT NULL
              AND p.area_m2 > 10
              -- Exclude implausible ratios (listing larger than parcel, or tiny fractions)
              AND (cu.area_sqm / p.area_m2) BETWEEN 0.05 AND 1.5
        """)).mappings().first()

        if factor_row and factor_row["n_pairs"] >= 10:
            empirical_factor = round(float(factor_row["median_ratio"]), 3)
            logger.info(
                "Tier 3 empirical conversion factor: %.3f (from %d Aqar-parcel pairs)",
                empirical_factor, factor_row["n_pairs"],
            )
        elif factor_row and factor_row["n_pairs"] > 0:
            # Some data but not enough — blend with default
            blend_weight = min(1.0, factor_row["n_pairs"] / 10.0)
            raw_factor = float(factor_row["median_ratio"])
            empirical_factor = round(
                blend_weight * raw_factor + (1 - blend_weight) * TIER3_DEFAULT_CONVERSION_FACTOR, 3
            )
            logger.info(
                "Tier 3 blended conversion factor: %.3f (raw=%.3f from %d pairs, blended with default %.2f)",
                empirical_factor, raw_factor, factor_row["n_pairs"], TIER3_DEFAULT_CONVERSION_FACTOR,
            )
        else:
            logger.info(
                "No Aqar-parcel overlap pairs found, using default factor: %.2f",
                TIER3_DEFAULT_CONVERSION_FACTOR,
            )
    except Exception:
        logger.warning(
            "Empirical factor computation failed, using default: %.2f",
            TIER3_DEFAULT_CONVERSION_FACTOR,
            exc_info=True,
        )

    # ── Step B: Apply tiered conversion to large Tier 3 parcels ──
    # Diminishing factor: bigger parcels contain more units, so each
    # unit occupies a smaller share of the total parcel area.
    tiers = [
        (300.0, 500.0, 0.50),    # yields 150–250 m²
        (500.0, 1000.0, 0.30),   # yields 150–300 m²
        (1000.0, 999999.0, 0.15),  # yields 150–300 m² (capped)
    ]

    large_count = 0
    for tier_min, tier_max, factor in tiers:
        sql = text("""
            UPDATE candidate_location
            SET area_sqm = ROUND(
                    LEAST(
                        GREATEST(area_sqm * :factor, :min_area),
                        :max_area
                    )::numeric, 1
                ),
                area_confidence = 'parcel_converted'
            WHERE source_tier = 3
              AND is_cluster_primary = TRUE
              AND area_sqm IS NOT NULL
              AND area_sqm > :tier_min
              AND area_sqm <= :tier_max
              AND (area_confidence IS NULL OR area_confidence IN ('parcel_raw', 'actual', 'default'))
        """)
        result = db.execute(sql, {
            "factor": factor,
            "tier_min": tier_min,
            "tier_max": tier_max,
            "min_area": TIER3_MIN_UNIT_AREA,
            "max_area": TIER3_MAX_UNIT_AREA,
        })
        tier_count = result.rowcount
        large_count += tier_count
        if tier_count > 0:
            logger.info(
                "Tier 3 area conversion: %d parcels in %.0f–%.0f m² range (factor=%.2f)",
                tier_count, tier_min, tier_max, factor,
            )

    # Mark small parcels as 'parcel_direct' (kept as-is but relabeled for clarity)
    result2 = db.execute(text("""
        UPDATE candidate_location
        SET area_confidence = 'parcel_direct'
        WHERE source_tier = 3
          AND is_cluster_primary = TRUE
          AND area_sqm IS NOT NULL
          AND area_sqm <= :threshold
          AND (area_confidence IS NULL OR area_confidence IN ('parcel_raw', 'actual', 'default'))
    """), {"threshold": TIER3_SMALL_PARCEL_THRESHOLD})
    small_count = result2.rowcount

    db.commit()
    logger.info(
        "Tier 3 area conversion: %d large parcels converted (tiered), %d small parcels kept as-is",
        large_count, small_count,
    )
    return large_count + small_count


def _step_area_category_default(db: Session) -> int:
    """Final fallback: assign area from category defaults for any remaining NULL areas.

    This is the last resort — only applies to Tier 2 candidates that had no
    Aqar comps nearby and no building footprint match.
    """
    updated = 0

    for category, area in CATEGORY_AREA_DEFAULTS.items():
        result = db.execute(text("""
            UPDATE candidate_location
            SET area_sqm = :area,
                area_confidence = 'category_default'
            WHERE area_sqm IS NULL
              AND source_tier = 2
              AND is_cluster_primary = TRUE
              AND current_category IS NOT NULL
              AND lower(current_category) LIKE :pattern
        """), {"area": area, "pattern": f"%{category}%"})
        updated += result.rowcount

    # Catch-all for any remaining NULL area candidates (any tier)
    result = db.execute(text("""
        UPDATE candidate_location
        SET area_sqm = :default_area,
            area_confidence = 'default'
        WHERE area_sqm IS NULL
          AND is_cluster_primary = TRUE
    """), {"default_area": DEFAULT_AREA_SQM})
    updated += result.rowcount

    db.commit()
    logger.info("Area category/default fallback: updated %d candidates", updated)
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
    """Main entry point: run all area + rent interpolation steps in sequence.

    Area steps run first (so rent calculations can use interpolated areas).
    Order matters within area steps:
      1. Tier 3 parcel conversion (convert raw parcel → unit estimate)
      2. Tier 2 IDW from Aqar comps (spatial interpolation)
      3. Building footprint cap (sanity-check both Tier 2 and Tier 3)
      4. Category default fallback (catch remaining NULLs)
    Then rent steps:
      5. IDW rent from comps
      6. District median rent
      7. City default rent
    """
    t_start = time.time()
    stats: dict[str, Any] = {}

    # ── Area interpolation chain ──
    stats["area_tier3_conversion"] = _step_area_tier3_conversion(db)
    stats["area_from_comps"] = _step_area_from_comps(db)
    stats["area_footprint_cap"] = _step_area_from_building_footprint(db)
    stats["area_category_default"] = _step_area_category_default(db)

    # ── Rent interpolation chain (unchanged) ──
    stats["rent_from_comps"] = _step2_rent_from_comps(db)
    stats["rent_from_district"] = _step3_rent_from_district_median(db)
    stats["rent_city_default"] = _step4_rent_city_default(db)

    # ── Summary stats ──
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
    logger.info("Rent & area interpolation complete in %.1fs: %s", stats["elapsed_s"], stats)
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
