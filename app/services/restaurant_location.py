"""
Restaurant Location Scoring Engine.

Computes a 0-100 demand-potential score for a given restaurant category
at a specific location, factoring in competition, population, traffic,
commercial density, delivery demand, competitor ratings, and rent.

NOTE: This is a demand-potential proxy, not a profitability predictor.
True profitability requires merchant outcome data (sales, order volumes).
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any, Optional

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.models.tables import PopulationDensity, RestaurantPOI
from app.services.restaurant_categories import CATEGORIES
from app.services.traffic_proxy import traffic_score_at

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Factor weights (MVP defaults — ML model replaces these in Phase 2)
# ---------------------------------------------------------------------------

DEFAULT_WEIGHTS: dict[str, float] = {
    "competition": 0.20,
    "complementary": 0.05,
    "population": 0.15,
    "traffic": 0.15,
    "road_frontage": 0.05,
    "commercial_density": 0.10,
    "delivery_demand": 0.10,
    "competitor_rating": 0.05,
    "rent": 0.10,
    "parking": 0.05,
}


@dataclass
class LocationScoreResult:
    overall_score: float
    factors: dict[str, float]
    confidence: float
    nearby_competitors: list[dict[str, Any]] = field(default_factory=list)
    model_version: str = "weighted_v1"
    debug: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Individual factor scoring functions
# ---------------------------------------------------------------------------

_COUNT_NEARBY_SQL = text("""
    SELECT id, name, category, rating, source, lat, lon,
           ST_Distance(
               geom::geography,
               ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography
           ) AS distance_m
    FROM restaurant_poi
    WHERE geom IS NOT NULL
      AND ST_DWithin(
        geom::geography,
        ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
        :radius_m
    )
""")


def _nearby_restaurants(
    db: Session,
    lat: float,
    lon: float,
    radius_m: float,
    category: str | None = None,
) -> list[dict]:
    """Query nearby restaurant POIs within radius."""
    try:
        rows = db.execute(
            _COUNT_NEARBY_SQL,
            {"lat": lat, "lon": lon, "radius_m": radius_m},
        ).mappings().all()
    except Exception as exc:
        logger.warning("Nearby restaurants query failed, falling back to ORM: %s", exc)
        # Fallback: simple lat/lon bounding box (less accurate but works without PostGIS)
        deg_offset = radius_m / 111_000
        rows_orm = (
            db.query(RestaurantPOI)
            .filter(
                RestaurantPOI.lat.between(lat - deg_offset, lat + deg_offset),
                RestaurantPOI.lon.between(lon - deg_offset, lon + deg_offset),
            )
            .all()
        )
        rows = [
            {
                "id": r.id,
                "name": r.name,
                "category": r.category,
                "rating": float(r.rating) if r.rating else None,
                "source": r.source,
                "lat": float(r.lat),
                "lon": float(r.lon),
                "distance_m": _haversine(lat, lon, float(r.lat), float(r.lon)),
            }
            for r in rows_orm
            if _haversine(lat, lon, float(r.lat), float(r.lon)) <= radius_m
        ]

    if category:
        same = [dict(r) for r in rows if r.get("category") == category]
        diff = [dict(r) for r in rows if r.get("category") != category]
    else:
        same = [dict(r) for r in rows]
        diff = []

    return same, diff


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in meters."""
    R = 6_371_000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(
        math.radians(lat2)
    ) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def competition_score(same_category_count: int) -> float:
    """
    Score 0-100 based on same-category competition density.
    0 competitors → 100 (blue ocean), 20+ → low score.
    """
    if same_category_count == 0:
        return 100.0
    # Exponential decay: score = 100 * exp(-0.15 * count)
    return max(5.0, 100.0 * math.exp(-0.15 * same_category_count))


def complementary_score(diff_category_count: int) -> float:
    """
    Score 0-100 based on nearby restaurants of *different* categories.
    A cluster of restaurants attracts foot traffic (dining destination effect).
    Sweet spot: 5-15 nearby restaurants. Too many → diminishing returns.
    """
    if diff_category_count == 0:
        return 20.0
    if diff_category_count <= 15:
        return min(100.0, 20.0 + diff_category_count * 5.0)
    return max(60.0, 100.0 - (diff_category_count - 15) * 2.0)


def population_score(db: Session, lat: float, lon: float, radius_m: float = 2000) -> float:
    """
    Score 0-100 based on population density around the location.
    Uses H3-indexed population data.
    """
    try:
        import h3

        center_h3 = h3.latlng_to_cell(lat, lon, 8)
        ring = h3.grid_disk(center_h3, int(radius_m / 460))  # ~460m per H3 res-8 cell
        h3_indices = list(ring)

        pop_rows = (
            db.query(func.sum(PopulationDensity.population))
            .filter(PopulationDensity.h3_index.in_(h3_indices))
            .scalar()
        )
        total_pop = float(pop_rows or 0)
    except ImportError:
        logger.debug("h3 not installed, skipping population score")
        return 50.0
    except Exception as exc:
        logger.warning("Population query failed: %s", exc)
        return 50.0

    # Scale: 0 pop → 10, 5000 → 50, 20000+ → 95
    if total_pop <= 0:
        return 10.0
    return min(95.0, 10.0 + 85.0 * (1 - math.exp(-total_pop / 10000)))


def commercial_density_score(db: Session, lat: float, lon: float, radius_m: float = 500) -> float:
    """
    Score 0-100 based on density of commercial/office buildings nearby.
    Uses the existing Overture buildings table.
    """
    try:
        result = db.execute(
            text("""
                SELECT COUNT(*) AS cnt
                FROM overture_buildings
                WHERE ST_DWithin(
                    geom::geography,
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                    :radius_m
                )
            """),
            {"lat": lat, "lon": lon, "radius_m": radius_m},
        ).scalar()
        count = int(result or 0)
    except Exception as exc:
        logger.debug("Commercial density query failed: %s", exc)
        return 50.0

    # Scale: 0 buildings → 10, 50 → 60, 200+ → 95
    if count == 0:
        return 10.0
    return min(95.0, 10.0 + 85.0 * (1 - math.exp(-count / 80)))


def delivery_demand_score(same_category_on_platforms: int, total_on_platforms: int) -> float:
    """
    Score 0-100 based on delivery platform presence in the area.
    More delivery listings = proven delivery demand for the area.
    """
    if total_on_platforms == 0:
        return 30.0  # no data — neutral
    # Ratio of same-category to total
    ratio = same_category_on_platforms / max(1, total_on_platforms)
    # Sweet spot: some demand but not oversaturated
    if ratio < 0.05:
        return 70.0  # underserved category
    if ratio < 0.15:
        return 85.0  # healthy demand
    if ratio < 0.30:
        return 60.0  # moderate saturation
    return 40.0  # oversaturated


def competitor_rating_score(avg_rating: float | None, count: int) -> float:
    """
    Score 0-100 based on average rating of nearby competitors.
    Low avg rating = opportunity (customers underserved).
    High avg rating = stiff competition.
    """
    if avg_rating is None or count == 0:
        return 50.0  # no data
    # Low ratings (< 3.5) = opportunity, high ratings (> 4.5) = tough competition
    if avg_rating < 3.0:
        return 90.0
    if avg_rating < 3.5:
        return 75.0
    if avg_rating < 4.0:
        return 60.0
    if avg_rating < 4.5:
        return 45.0
    return 30.0


def rent_score_value(rent_per_m2: float | None) -> float:
    """
    Score 0-100 based on commercial rent levels.
    Lower rent → higher score (better margins).
    """
    if rent_per_m2 is None:
        return 50.0
    # Riyadh commercial rent ranges roughly 200-2000 SAR/m2/year
    if rent_per_m2 < 300:
        return 90.0
    if rent_per_m2 < 600:
        return 75.0
    if rent_per_m2 < 1000:
        return 55.0
    if rent_per_m2 < 1500:
        return 35.0
    return 20.0


# ---------------------------------------------------------------------------
# Main scoring function
# ---------------------------------------------------------------------------


def score_location(
    db: Session,
    lat: float,
    lon: float,
    category: str,
    radius_m: float = 1000,
    weights: dict[str, float] | None = None,
) -> LocationScoreResult:
    """
    Compute a 0-100 demand-potential score for a restaurant category at a given location.
    """
    w = weights or DEFAULT_WEIGHTS

    # 1. Nearby restaurants
    same_cat, diff_cat = _nearby_restaurants(db, lat, lon, radius_m, category)
    platform_sources = {"hungerstation", "talabat", "mrsool"}
    same_on_platforms = [r for r in same_cat if r.get("source") in platform_sources]
    all_on_platforms_rows = db.query(RestaurantPOI).filter(
        RestaurantPOI.source.in_(list(platform_sources))
    ).count() if platform_sources else 0

    # Avg rating of same-category competitors
    rated = [r for r in same_cat if r.get("rating") is not None]
    avg_rating = (
        sum(float(r["rating"]) for r in rated) / len(rated) if rated else None
    )

    # 2. Compute each factor score
    factors = {
        "competition": competition_score(len(same_cat)),
        "complementary": complementary_score(len(diff_cat)),
        "population": population_score(db, lat, lon),
        "traffic": traffic_score_at(db, lat, lon).get("score", 25.0),
        "road_frontage": traffic_score_at(db, lat, lon, radius_m=100).get("score", 25.0),
        "commercial_density": commercial_density_score(db, lat, lon),
        "delivery_demand": delivery_demand_score(len(same_on_platforms), all_on_platforms_rows),
        "competitor_rating": competitor_rating_score(avg_rating, len(rated)),
        "rent": 50.0,  # TODO: integrate with existing rent service when commercial rent data available
        "parking": 50.0,  # TODO: integrate with parking service
    }

    # 3. Weighted aggregation
    total_weight = sum(w.get(k, 0) for k in factors)
    if total_weight <= 0:
        total_weight = 1.0

    overall = sum(w.get(k, 0) * v for k, v in factors.items()) / total_weight

    # 4. Confidence based on data availability
    data_count = len(same_cat) + len(diff_cat)
    confidence = min(1.0, data_count / 20.0)  # need ~20 data points for high confidence

    # 5. Format nearby competitors for response
    competitors_out = sorted(
        [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "category": r.get("category"),
                "rating": r.get("rating"),
                "source": r.get("source"),
                "distance_m": round(float(r.get("distance_m", 0)), 0),
            }
            for r in same_cat
        ],
        key=lambda x: x.get("distance_m", 0),
    )[:20]

    return LocationScoreResult(
        overall_score=round(overall, 1),
        factors={k: round(v, 1) for k, v in factors.items()},
        confidence=round(confidence, 2),
        nearby_competitors=competitors_out,
        debug={
            "same_category_count": len(same_cat),
            "diff_category_count": len(diff_cat),
            "avg_competitor_rating": round(avg_rating, 2) if avg_rating else None,
            "radius_m": radius_m,
        },
    )
