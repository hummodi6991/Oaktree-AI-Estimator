"""
Delivery-derived scoring features.

Exposes aggregate signals computed from delivery_source_record that
are useful for the restaurant scoring engine, heatmaps, and analytics.

CRITICAL RULE:
- Exact parcel-nearby competition uses only records with location_confidence >= 0.7
- District/category heatmap features may use weaker area-level records (>= 0.2)
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Minimum confidence thresholds
PARCEL_MIN_CONFIDENCE = 0.7  # for exact spatial queries
DISTRICT_MIN_CONFIDENCE = 0.2  # for area aggregations


def platform_presence_count(
    db: Session,
    *,
    district: str | None = None,
    category: str | None = None,
) -> dict[str, Any]:
    """
    Count distinct platforms present in a district/category.
    Useful for measuring delivery market depth.
    """
    conditions = ["location_confidence >= :min_conf"]
    params: dict[str, Any] = {"min_conf": DISTRICT_MIN_CONFIDENCE}

    if district:
        conditions.append("LOWER(district_text) = LOWER(:district)")
        params["district"] = district
    if category:
        conditions.append("LOWER(category_raw) = LOWER(:category)")
        params["category"] = category

    where = " AND ".join(conditions)

    try:
        rows = db.execute(
            text(f"""
                SELECT platform, COUNT(*) as cnt
                FROM delivery_source_record
                WHERE {where}
                GROUP BY platform
                ORDER BY cnt DESC
            """),
            params,
        ).fetchall()

        return {
            "platforms": {r[0]: r[1] for r in rows},
            "platform_count": len(rows),
            "total_listings": sum(r[1] for r in rows),
        }
    except Exception as exc:
        logger.debug("platform_presence_count failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {"platforms": {}, "platform_count": 0, "total_listings": 0}


def avg_delivery_metrics(
    db: Session,
    *,
    district: str | None = None,
    category: str | None = None,
) -> dict[str, Any]:
    """
    Compute average delivery time, fee, minimum order for an area/category.
    """
    conditions = ["location_confidence >= :min_conf"]
    params: dict[str, Any] = {"min_conf": DISTRICT_MIN_CONFIDENCE}

    if district:
        conditions.append("LOWER(district_text) = LOWER(:district)")
        params["district"] = district
    if category:
        conditions.append("LOWER(category_raw) = LOWER(:category)")
        params["category"] = category

    where = " AND ".join(conditions)

    try:
        row = db.execute(
            text(f"""
                SELECT
                    AVG(delivery_time_min) as avg_time,
                    AVG(delivery_fee) as avg_fee,
                    AVG(minimum_order) as avg_min_order,
                    COUNT(*) FILTER (WHERE delivery_time_min IS NOT NULL) as time_count,
                    COUNT(*) FILTER (WHERE delivery_fee IS NOT NULL) as fee_count
                FROM delivery_source_record
                WHERE {where}
            """),
            params,
        ).first()

        return {
            "avg_delivery_time_min": round(float(row[0]), 1) if row[0] else None,
            "avg_delivery_fee": round(float(row[1]), 2) if row[1] else None,
            "avg_minimum_order": round(float(row[2]), 2) if row[2] else None,
            "records_with_time": row[3] or 0,
            "records_with_fee": row[4] or 0,
        }
    except Exception as exc:
        logger.debug("avg_delivery_metrics failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {
            "avg_delivery_time_min": None,
            "avg_delivery_fee": None,
            "avg_minimum_order": None,
            "records_with_time": 0,
            "records_with_fee": 0,
        }


def category_saturation(
    db: Session,
    district: str,
) -> dict[str, Any]:
    """
    Category saturation from delivery platforms for a district.
    Returns {category: count} showing which categories are most served.
    """
    try:
        rows = db.execute(
            text("""
                SELECT category_raw, COUNT(*) as cnt
                FROM delivery_source_record
                WHERE LOWER(district_text) = LOWER(:district)
                  AND category_raw IS NOT NULL
                  AND location_confidence >= :min_conf
                GROUP BY category_raw
                ORDER BY cnt DESC
            """),
            {"district": district, "min_conf": DISTRICT_MIN_CONFIDENCE},
        ).fetchall()

        return {
            "district": district,
            "categories": {r[0]: r[1] for r in rows},
            "total_listings": sum(r[1] for r in rows),
        }
    except Exception as exc:
        logger.debug("category_saturation failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {"district": district, "categories": {}, "total_listings": 0}


def multi_platform_presence_score(
    db: Session,
    restaurant_name: str,
) -> dict[str, Any]:
    """
    Check how many platforms a restaurant is present on.
    Multi-platform presence is a signal of establishment viability.
    """
    try:
        rows = db.execute(
            text("""
                SELECT DISTINCT platform
                FROM delivery_source_record
                WHERE LOWER(restaurant_name_normalized) = LOWER(:name)
            """),
            {"name": restaurant_name.strip()},
        ).fetchall()

        platforms = [r[0] for r in rows]
        return {
            "name": restaurant_name,
            "platforms": platforms,
            "platform_count": len(platforms),
            "score": min(100.0, len(platforms) * 20.0),
        }
    except Exception as exc:
        logger.debug("multi_platform_presence failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {
            "name": restaurant_name,
            "platforms": [],
            "platform_count": 0,
            "score": 0.0,
        }


def delivery_rating_aggregates(
    db: Session,
    *,
    district: str | None = None,
    category: str | None = None,
) -> dict[str, Any]:
    """Aggregate rating data from delivery platforms."""
    conditions = [
        "rating IS NOT NULL",
        "location_confidence >= :min_conf",
    ]
    params: dict[str, Any] = {"min_conf": DISTRICT_MIN_CONFIDENCE}

    if district:
        conditions.append("LOWER(district_text) = LOWER(:district)")
        params["district"] = district
    if category:
        conditions.append("LOWER(category_raw) = LOWER(:category)")
        params["category"] = category

    where = " AND ".join(conditions)

    try:
        row = db.execute(
            text(f"""
                SELECT
                    AVG(rating) as avg_rating,
                    MIN(rating) as min_rating,
                    MAX(rating) as max_rating,
                    SUM(rating_count) as total_reviews,
                    COUNT(*) as rated_count
                FROM delivery_source_record
                WHERE {where}
            """),
            params,
        ).first()

        return {
            "avg_rating": round(float(row[0]), 2) if row[0] else None,
            "min_rating": round(float(row[1]), 2) if row[1] else None,
            "max_rating": round(float(row[2]), 2) if row[2] else None,
            "total_reviews": int(row[3]) if row[3] else 0,
            "rated_count": row[4] or 0,
        }
    except Exception as exc:
        logger.debug("delivery_rating_aggregates failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {
            "avg_rating": None,
            "min_rating": None,
            "max_rating": None,
            "total_reviews": 0,
            "rated_count": 0,
        }


def nearby_delivery_competition(
    db: Session,
    lat: float,
    lon: float,
    radius_m: float = 1000,
    category: str | None = None,
) -> dict[str, Any]:
    """
    Count delivery-platform restaurants near a point.
    ONLY uses records with high location confidence (>= 0.7) and
    first-party coordinate methods (platform_payload, json_ld, address_geocode).
    Excludes poi_match and district_centroid which are approximate.
    """
    conditions = [
        "lat IS NOT NULL",
        "lon IS NOT NULL",
        "location_confidence >= :min_conf",
        "geocode_method IN ('platform_payload', 'json_ld', 'address_geocode')",
    ]
    params: dict[str, Any] = {
        "lat": lat,
        "lon": lon,
        "radius_m": radius_m,
        "min_conf": PARCEL_MIN_CONFIDENCE,
    }

    if category:
        conditions.append("LOWER(category_raw) = LOWER(:category)")
        params["category"] = category

    where = " AND ".join(conditions)

    try:
        rows = db.execute(
            text(f"""
                SELECT platform, COUNT(*) as cnt
                FROM delivery_source_record
                WHERE {where}
                  AND (
                    6371000 * acos(
                      LEAST(1.0,
                        cos(radians(:lat)) * cos(radians(lat))
                        * cos(radians(lon) - radians(:lon))
                        + sin(radians(:lat)) * sin(radians(lat))
                      )
                    )
                  ) <= :radius_m
                GROUP BY platform
            """),
            params,
        ).fetchall()

        return {
            "platforms": {r[0]: r[1] for r in rows},
            "total": sum(r[1] for r in rows),
        }
    except Exception as exc:
        logger.debug("nearby_delivery_competition failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {"platforms": {}, "total": 0}


def unmatched_demand_by_district(
    db: Session,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Find districts with high unmatched delivery record counts.
    These represent potential demand signals not yet captured in restaurant_poi.
    """
    try:
        rows = db.execute(
            text("""
                SELECT
                    district_text,
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE entity_resolution_status = 'unmatched') as unmatched,
                    COUNT(DISTINCT platform) as platform_count,
                    COUNT(DISTINCT restaurant_name_normalized) as unique_names
                FROM delivery_source_record
                WHERE district_text IS NOT NULL
                  AND location_confidence >= :min_conf
                GROUP BY district_text
                ORDER BY unmatched DESC
                LIMIT :limit
            """),
            {"min_conf": DISTRICT_MIN_CONFIDENCE, "limit": limit},
        ).fetchall()

        return [
            {
                "district": r[0],
                "total_records": r[1],
                "unmatched": r[2],
                "platform_count": r[3],
                "unique_names": r[4],
                "unmatched_ratio": round(r[2] / max(r[1], 1), 3),
            }
            for r in rows
        ]
    except Exception as exc:
        logger.debug("unmatched_demand_by_district failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return []
