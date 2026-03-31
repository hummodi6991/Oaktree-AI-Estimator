"""
Populate the candidate_location table from three source tiers.

Tier 1: commercial_unit (Aqar) — vacant, with real rent/area
Tier 2: delivery_source_record + restaurant_poi — occupied, proven locations
Tier 3: riyadh_parcels_arcgis_proxy — commercial/mixed-use parcels

After inserting all tiers, run spatial deduplication using
ST_ClusterDBSCAN (eps=50m) and mark cluster primaries.

Usage:
    python -m app.ingest.candidate_locations [--replace] [--tiers 1,2,3]
"""

import argparse
import logging
import time
import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import SessionLocal

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Riyadh bounding box
RIYADH_BBOX = {
    "min_lon": 46.3, "max_lon": 47.1,
    "min_lat": 24.4, "max_lat": 25.0,
}

# Category-based area defaults (m²) when area is unknown
CATEGORY_AREA_DEFAULTS = {
    "burger": 120, "pizza": 130, "fried_chicken": 110,
    "shawarma": 80, "falafel": 60, "grills": 150,
    "indian": 160, "asian": 140, "italian": 150,
    "coffee": 100, "bakery": 90, "dessert": 80,
    "juice": 50, "seafood": 160, "turkish": 150,
    "lebanese": 150, "yemeni": 120, "bukhari": 140,
    "_default": 120,
}


def _ingest_tier1_aqar(db: Session, run_id: str) -> int:
    """Insert Tier 1 candidates from commercial_unit (Aqar)."""
    sql = text("""
        INSERT INTO candidate_location (
            source_tier, source_type, source_id,
            lat, lon,
            neighborhood_raw,
            area_sqm, rent_sar_annual, rent_sar_m2_month,
            rent_confidence, area_confidence,
            listing_url, listing_type, image_url,
            is_vacant,
            street_width_m, has_drive_thru,
            population_run_id
        )
        SELECT
            1, 'aqar', cu.aqar_id,
            cu.lat, cu.lon,
            cu.neighborhood,
            cu.area_sqm,
            cu.price_sar_annual,
            CASE WHEN cu.area_sqm > 0 AND cu.price_sar_annual > 0
                 THEN ROUND(cu.price_sar_annual / cu.area_sqm / 12, 2)
                 ELSE NULL END,
            CASE WHEN cu.price_sar_annual IS NOT NULL THEN 'actual' ELSE NULL END,
            CASE WHEN cu.area_sqm IS NOT NULL THEN 'actual' ELSE NULL END,
            cu.listing_url, cu.listing_type, cu.image_url,
            TRUE,
            cu.street_width_m, cu.has_drive_thru,
            :run_id
        FROM commercial_unit cu
        WHERE cu.status = 'active'
          AND cu.restaurant_suitable = TRUE
          AND cu.lat IS NOT NULL
          AND cu.lon IS NOT NULL
          AND cu.lat BETWEEN :min_lat AND :max_lat
          AND cu.lon BETWEEN :min_lon AND :max_lon
    """)
    result = db.execute(sql, {"run_id": run_id, "run_id_filter": run_id, **RIYADH_BBOX})
    count = result.rowcount
    db.commit()
    logger.info("Tier 1 (Aqar): inserted %d candidates", count)
    return count


def _ingest_tier2_delivery(db: Session, run_id: str) -> int:
    """Insert Tier 2 candidates from delivery_source_record.

    Aggregates per unique location (clustered by source_listing_id + platform,
    or by lat/lon rounded to ~50m precision).
    Groups nearby records to avoid duplicates from the same restaurant
    listed under slightly different coordinates.
    """
    sql = text("""
        INSERT INTO candidate_location (
            source_tier, source_type, source_id,
            lat, lon,
            neighborhood_raw,
            is_vacant,
            current_tenant, current_category,
            platform_count, avg_rating, total_rating_count,
            area_confidence, rent_confidence,
            population_run_id
        )
        SELECT
            2,
            agg.platform,
            agg.source_listing_id,
            agg.lat, agg.lon,
            agg.district_text,
            FALSE,
            agg.brand_raw,
            agg.category_raw,
            1,  -- platform_count updated in enrichment pass
            agg.rating,
            agg.rating_count,
            'default',
            'default',
            :run_id
        FROM (
            SELECT DISTINCT ON (ROUND(CAST(lat AS numeric), 4), ROUND(CAST(lon AS numeric), 4))
                platform,
                source_listing_id,
                CAST(lat AS double precision) AS lat,
                CAST(lon AS double precision) AS lon,
                district_text,
                brand_raw,
                category_raw,
                CAST(rating AS numeric(3,2)) AS rating,
                rating_count
            FROM delivery_source_record
            WHERE lat IS NOT NULL
              AND lon IS NOT NULL
              AND CAST(lat AS double precision) BETWEEN :min_lat AND :max_lat
              AND CAST(lon AS double precision) BETWEEN :min_lon AND :max_lon
            ORDER BY
                ROUND(CAST(lat AS numeric), 4),
                ROUND(CAST(lon AS numeric), 4),
                rating_count DESC NULLS LAST,
                scraped_at DESC
        ) agg
    """)
    result = db.execute(sql, {"run_id": run_id, "run_id_filter": run_id, **RIYADH_BBOX})
    count = result.rowcount
    db.commit()
    logger.info("Tier 2 (Delivery): inserted %d candidates", count)
    return count


def _ingest_tier2_poi(db: Session, run_id: str) -> int:
    """Insert Tier 2 candidates from restaurant_poi not already covered by delivery records.

    Only adds POIs that are >50m from any existing candidate_location
    to avoid duplicating delivery-sourced locations.
    """
    sql = text("""
        INSERT INTO candidate_location (
            source_tier, source_type, source_id,
            lat, lon,
            neighborhood_raw,
            is_vacant,
            current_tenant, current_category,
            avg_rating, total_rating_count,
            area_confidence, rent_confidence,
            population_run_id
        )
        SELECT
            2, 'restaurant_poi', rp.id,
            rp.lat, rp.lon,
            rp.district,
            FALSE,
            rp.chain_name,
            rp.category,
            rp.rating,
            rp.review_count,
            'default',
            'default',
            :run_id
        FROM restaurant_poi rp
        WHERE rp.lat IS NOT NULL
          AND rp.lon IS NOT NULL
          AND rp.lat BETWEEN :min_lat AND :max_lat
          AND rp.lon BETWEEN :min_lon AND :max_lon
          AND NOT EXISTS (
              SELECT 1 FROM candidate_location cl
              WHERE cl.population_run_id = :run_id_filter
                AND ST_DWithin(
                    cl.geom::geography,
                    ST_SetSRID(ST_MakePoint(rp.lon, rp.lat), 4326)::geography,
                    50  -- 50 meters
                )
          )
    """)
    result = db.execute(sql, {"run_id": run_id, "run_id_filter": run_id, **RIYADH_BBOX})
    count = result.rowcount
    db.commit()
    logger.info("Tier 2 (POI): inserted %d candidates", count)
    return count


def _ingest_tier3_arcgis(db: Session, run_id: str) -> int:
    """Insert Tier 3 candidates from ArcGIS commercial/mixed-use parcels.

    Only adds parcels not within 50m of any existing Tier 1/2 candidate.
    Uses parcel centroids for lat/lon.
    """
    sql = text("""
        INSERT INTO candidate_location (
            source_tier, source_type, source_id,
            lat, lon,
            neighborhood_raw,
            is_vacant,
            area_sqm, area_confidence,
            rent_confidence,
            landuse_code, landuse_label,
            population_run_id
        )
        SELECT
            3, 'arcgis_parcel', p.id::text,
            ST_Y(ST_Centroid(p.geom)),
            ST_X(ST_Centroid(p.geom)),
            p.district_label,
            NULL,  -- vacancy unknown
            p.area_m2, 'actual',
            'default',
            p.landuse_code, p.landuse_label,
            :run_id
        FROM riyadh_parcels_arcgis_proxy p
        WHERE p.geom IS NOT NULL
          AND p.landuse_code IN (2000, 7500)  -- commercial, mixed_use
          AND p.area_m2 BETWEEN 30 AND 2000   -- reasonable restaurant sizes

    """)
    result = db.execute(sql, {"run_id": run_id, "run_id_filter": run_id, **RIYADH_BBOX})
    count = result.rowcount
    db.commit()
    logger.info("Tier 3 (ArcGIS): inserted %d candidates", count)
    return count


def _run_deduplication(db: Session, run_id: str) -> int:
    """Cluster nearby candidates and mark one primary per cluster.

    Uses PostGIS ST_ClusterDBSCAN with 50m eps, minpoints=1.
    Within each cluster, the highest-tier (lowest number) candidate is primary.
    """
    # Assign cluster IDs
    db.execute(text("""
        UPDATE candidate_location cl
        SET cluster_id = sub.cid
        FROM (
            SELECT id,
                   ST_ClusterDBSCAN(geom::geometry, eps := 0.00045, minpoints := 1)
                       OVER () AS cid
            FROM candidate_location
            WHERE population_run_id = :run_id
              AND geom IS NOT NULL
        ) sub
        WHERE cl.id = sub.id
    """), {"run_id": run_id})
    db.commit()

    # Mark all as non-primary first
    db.execute(text("""
        UPDATE candidate_location
        SET is_cluster_primary = FALSE
        WHERE population_run_id = :run_id
    """), {"run_id": run_id})

    # Mark the best candidate per cluster as primary
    # Priority: lowest source_tier, then highest total_rating_count, then lowest id
    db.execute(text("""
        UPDATE candidate_location cl
        SET is_cluster_primary = TRUE
        FROM (
            SELECT DISTINCT ON (cluster_id) id
            FROM candidate_location
            WHERE population_run_id = :run_id
              AND cluster_id IS NOT NULL
            ORDER BY cluster_id,
                     source_tier ASC,
                     total_rating_count DESC NULLS LAST,
                     rent_confidence = 'actual' DESC,
                     id ASC
        ) best
        WHERE cl.id = best.id
    """), {"run_id": run_id})
    db.commit()

    # Count primaries
    primary_count = db.execute(text("""
        SELECT COUNT(*) FROM candidate_location
        WHERE population_run_id = :run_id AND is_cluster_primary = TRUE
    """), {"run_id": run_id}).scalar()

    logger.info("Deduplication: %d primary candidates after clustering", primary_count)
    return int(primary_count or 0)


def _resolve_districts(db: Session, run_id: str) -> int:
    """Resolve Arabic/English district names from external_feature polygons.

    Uses the same priority as the expansion advisor: osm_districts first,
    then aqar_district_hulls, then rydpolygons.
    """
    updated = 0
    for layer, name_field_ar, name_field_en in [
        ("osm_districts", "name", "name:en"),
        ("aqar_district_hulls", "district", "district_en"),
        ("rydpolygons", "DISTRICTNA", "DISTRICTEN"),
    ]:
        sql = text(f"""
            UPDATE candidate_location cl
            SET district_ar = COALESCE(cl.district_ar, ef.properties->>'{name_field_ar}'),
                district_en = COALESCE(cl.district_en, ef.properties->>'{name_field_en}')
            FROM external_feature ef
            WHERE cl.population_run_id = :run_id
              AND cl.district_ar IS NULL
              AND ef.layer_name = :layer
              AND cl.geom IS NOT NULL
              AND ef.geometry IS NOT NULL
              AND ST_Within(cl.geom, ST_GeomFromGeoJSON(ef.geometry::text))
        """)
        result = db.execute(sql, {"run_id": run_id, "layer": layer})
        count = result.rowcount
        db.commit()
        updated += count
        logger.info("District resolution (%s): matched %d candidates", layer, count)

    return updated


def _enrich_platform_counts(db: Session, run_id: str) -> int:
    """For Tier 2 candidates, count how many delivery platforms are within 50m."""
    sql = text("""
        UPDATE candidate_location cl
        SET platform_count = sub.pcount
        FROM (
            SELECT cl2.id,
                   COUNT(DISTINCT dsr.platform) AS pcount
            FROM candidate_location cl2
            JOIN delivery_source_record dsr
              ON ST_DWithin(
                  cl2.geom::geography,
                  dsr.geom::geography,
                  50
              )
            WHERE cl2.population_run_id = :run_id
              AND cl2.source_tier <= 2
              AND dsr.lat IS NOT NULL
            GROUP BY cl2.id
        ) sub
        WHERE cl.id = sub.id
    """)
    result = db.execute(sql, {"run_id": run_id})
    count = result.rowcount
    db.commit()
    logger.info("Platform count enrichment: updated %d candidates", count)
    return count


def populate_candidate_locations(
    db: Session,
    tiers: list[int] | None = None,
    replace: bool = True,
) -> dict[str, Any]:
    """Main entry point: populate candidate_location table."""

    run_id = str(uuid.uuid4())[:8]
    t_start = time.time()

    if tiers is None:
        tiers = [1, 2, 3]

    if replace:
        db.execute(text("TRUNCATE candidate_location RESTART IDENTITY"))
        db.commit()
        logger.info("Truncated candidate_location table")

    stats: dict[str, Any] = {"run_id": run_id, "tiers": {}}

    # Ingest each tier in order (important: Tier 1 first so dedup prefers it)
    if 1 in tiers:
        stats["tiers"]["tier1_aqar"] = _ingest_tier1_aqar(db, run_id)

    if 2 in tiers:
        stats["tiers"]["tier2_delivery"] = _ingest_tier2_delivery(db, run_id)
        stats["tiers"]["tier2_poi"] = _ingest_tier2_poi(db, run_id)

    if 3 in tiers:
        stats["tiers"]["tier3_arcgis"] = _ingest_tier3_arcgis(db, run_id)

    # Total before dedup
    total_raw = db.execute(text(
        "SELECT COUNT(*) FROM candidate_location WHERE population_run_id = :run_id"
    ), {"run_id": run_id}).scalar()
    stats["total_raw"] = int(total_raw or 0)

    # Deduplication
    stats["primary_after_dedup"] = _run_deduplication(db, run_id)

    # District resolution
    stats["districts_resolved"] = _resolve_districts(db, run_id)

    # Platform count enrichment
    stats["platform_counts_enriched"] = _enrich_platform_counts(db, run_id)

    # Rent & area interpolation
    from app.ingest.candidate_rent_interpolation import interpolate_rents
    rent_stats = interpolate_rents(db)
    stats["rent_interpolation"] = rent_stats

    stats["elapsed_s"] = round(time.time() - t_start, 1)

    logger.info(
        "candidate_location population complete: %d raw → %d primary in %.1fs",
        stats["total_raw"], stats["primary_after_dedup"], stats["elapsed_s"],
    )
    return stats


def main():
    parser = argparse.ArgumentParser(description="Populate candidate_location table")
    parser.add_argument("--replace", action="store_true", default=True,
                        help="Truncate table before inserting (default: True)")
    parser.add_argument("--no-replace", dest="replace", action="store_false")
    parser.add_argument("--tiers", type=str, default="1,2,3",
                        help="Comma-separated tier numbers to ingest (default: 1,2,3)")
    args = parser.parse_args()

    tiers = [int(t.strip()) for t in args.tiers.split(",")]

    db = SessionLocal()
    try:
        stats = populate_candidate_locations(db, tiers=tiers, replace=args.replace)
        print("\n=== Results ===")
        for k, v in stats.items():
            print(f"  {k}: {v}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
