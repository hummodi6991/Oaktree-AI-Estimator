"""
Traffic density proxy using OSM road classification.

Maps road classes from the existing OSM roads data to estimated
traffic density scores (0-100). This avoids the need for an external
traffic API in the MVP.
"""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Road class → estimated relative traffic score (0-100)
ROAD_CLASS_SCORES: dict[str, float] = {
    "motorway": 95,
    "motorway_link": 85,
    "trunk": 90,
    "trunk_link": 80,
    "primary": 80,
    "primary_link": 70,
    "secondary": 65,
    "secondary_link": 55,
    "tertiary": 50,
    "tertiary_link": 40,
    "residential": 30,
    "living_street": 20,
    "unclassified": 25,
    "service": 15,
    "pedestrian": 10,
    "footway": 5,
}


def road_class_score(road_class: str | None) -> float:
    """Convert a road class string to a traffic score (0-100)."""
    if not road_class:
        return 25.0  # default: unclassified
    return ROAD_CLASS_SCORES.get(road_class.lower(), 25.0)


_NEAREST_ROAD_SQL = text("""
    SELECT highway, name,
           ST_Distance(
               ST_Transform(geom, 32638),
               ST_Transform(ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), 32638)
           ) AS distance_m
    FROM osm_roads
    WHERE ST_DWithin(
        geom::geography,
        ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
        :radius_m
    )
    ORDER BY distance_m
    LIMIT :limit
""")


def traffic_score_at(
    db: Session,
    lat: float,
    lon: float,
    radius_m: float = 200,
) -> dict:
    """
    Compute a traffic proxy score at a given point based on the nearest
    OSM road segments.

    Returns dict with score (0-100), nearest road details, and road count.
    """
    try:
        rows = db.execute(
            _NEAREST_ROAD_SQL,
            {"lat": lat, "lon": lon, "radius_m": radius_m, "limit": 5},
        ).mappings().all()
    except Exception as exc:
        logger.warning("traffic_score_at query failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {"score": 25.0, "road_count": 0, "nearest_road": None}

    if not rows:
        return {"score": 10.0, "road_count": 0, "nearest_road": None}

    # Weight by inverse distance: closer roads matter more
    total_weight = 0.0
    weighted_score = 0.0
    for r in rows:
        dist = float(r.get("distance_m", 1.0)) or 1.0
        weight = 1.0 / dist
        total_weight += weight
        weighted_score += weight * road_class_score(r.get("highway"))

    avg_score = weighted_score / total_weight if total_weight > 0 else 25.0
    nearest = rows[0] if rows else None

    return {
        "score": round(avg_score, 1),
        "road_count": len(rows),
        "nearest_road": {
            "class": nearest.get("highway") if nearest else None,
            "name": nearest.get("name") if nearest else None,
            "distance_m": round(float(nearest.get("distance_m", 0)), 1) if nearest else None,
        },
    }
