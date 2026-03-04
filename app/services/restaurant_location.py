"""
Restaurant Location Scoring Engine.

Computes a composite *opportunity score* (0-100) for a given restaurant
category at a specific location, broken into two sub-scores:

- **demand_score** — aggregates demand-side signals: competition density,
  population, traffic, commercial density, delivery demand, dining cluster
  effect, and competitor quality.
- **cost_penalty** — aggregates cost-side signals: rent, parking.
  Higher = cheaper = better opportunity.

``opportunity_score = 0.80 * demand_score + 0.20 * cost_penalty``

NOTE: This is a demand-potential proxy, not a profitability predictor.
True profitability requires merchant outcome data (sales, order volumes).
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.models.tables import PopulationDensity, RestaurantPOI
from app.services.restaurant_categories import CATEGORIES
from app.services.traffic_proxy import traffic_score_at

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Factor weights grouped by sub-score
# ---------------------------------------------------------------------------

DEMAND_WEIGHTS: dict[str, float] = {
    "competition": 0.25,
    "complementary": 0.05,
    "population": 0.20,
    "traffic": 0.15,
    "road_frontage": 0.05,
    "commercial_density": 0.10,
    "delivery_demand": 0.10,
    "competitor_rating": 0.10,
}

COST_WEIGHTS: dict[str, float] = {
    "rent": 0.70,
    "parking": 0.30,
}


@dataclass
class LocationScoreResult:
    opportunity_score: float
    demand_score: float
    cost_penalty: float
    factors: dict[str, float]
    contributions: list[dict[str, Any]]  # top-N feature contributions
    confidence: float
    nearby_competitors: list[dict[str, Any]] = field(default_factory=list)
    model_version: str = "weighted_v2"
    debug: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Nearby restaurants — single-query, uses GiST on geom
# ---------------------------------------------------------------------------

_NEARBY_SQL = text("""
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
) -> tuple[list[dict], list[dict]]:
    """Query nearby restaurant POIs within radius.  Returns (same_cat, diff_cat)."""
    try:
        rows = db.execute(
            _NEARBY_SQL,
            {"lat": lat, "lon": lon, "radius_m": radius_m},
        ).mappings().all()
    except Exception as exc:
        logger.warning("Nearby restaurants query failed, falling back to ORM: %s", exc)
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


# ---------------------------------------------------------------------------
# Individual factor scoring functions
# ---------------------------------------------------------------------------


def competition_score(same_category_count: int) -> float:
    """0 competitors -> 100 (blue ocean), 20+ -> low score."""
    if same_category_count == 0:
        return 100.0
    return max(5.0, 100.0 * math.exp(-0.15 * same_category_count))


def complementary_score(diff_category_count: int) -> float:
    """Dining-cluster effect: sweet spot is 5-15 nearby restaurants."""
    if diff_category_count == 0:
        return 20.0
    if diff_category_count <= 15:
        return min(100.0, 20.0 + diff_category_count * 5.0)
    return max(60.0, 100.0 - (diff_category_count - 15) * 2.0)


def population_score(db: Session, lat: float, lon: float, radius_m: float = 2000) -> float:
    """Score based on H3-indexed population density."""
    try:
        import h3
        center_h3 = h3.latlng_to_cell(lat, lon, 8)
        ring = h3.grid_disk(center_h3, int(radius_m / 460))
        pop_rows = (
            db.query(func.sum(PopulationDensity.population))
            .filter(PopulationDensity.h3_index.in_(list(ring)))
            .scalar()
        )
        total_pop = float(pop_rows or 0)
    except ImportError:
        logger.debug("h3 not installed, skipping population score")
        return 50.0
    except Exception as exc:
        logger.warning("Population query failed: %s", exc)
        return 50.0

    if total_pop <= 0:
        return 10.0
    return min(95.0, 10.0 + 85.0 * (1 - math.exp(-total_pop / 10000)))


def commercial_density_score(db: Session, lat: float, lon: float, radius_m: float = 500) -> float:
    """Score based on density of commercial/office buildings (Overture)."""
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

    if count == 0:
        return 10.0
    return min(95.0, 10.0 + 85.0 * (1 - math.exp(-count / 80)))


def delivery_demand_score(same_category_on_platforms: int, total_on_platforms: int) -> float:
    """Score based on delivery-platform presence in the area."""
    if total_on_platforms == 0:
        return 30.0
    ratio = same_category_on_platforms / max(1, total_on_platforms)
    if ratio < 0.05:
        return 70.0
    if ratio < 0.15:
        return 85.0
    if ratio < 0.30:
        return 60.0
    return 40.0


def competitor_rating_score(avg_rating: float | None, count: int) -> float:
    """Low avg rating = opportunity, high = tough competition."""
    if avg_rating is None or count == 0:
        return 50.0
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
    """Lower rent -> higher score (better margins)."""
    if rent_per_m2 is None:
        return 50.0
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
# Helpers
# ---------------------------------------------------------------------------

def _weighted_avg(scores: dict[str, float], weights: dict[str, float]) -> float:
    total_w = sum(weights.get(k, 0) for k in scores)
    if total_w <= 0:
        return 50.0
    return sum(weights.get(k, 0) * v for k, v in scores.items()) / total_w


def _build_contributions(
    factors: dict[str, float],
    all_weights: dict[str, float],
) -> list[dict[str, Any]]:
    """Top-N feature contributions, sorted by weighted impact."""
    items = []
    total_w = sum(all_weights.values())
    for k, v in factors.items():
        w = all_weights.get(k, 0)
        items.append({
            "factor": k,
            "score": round(v, 1),
            "weight": round(w, 3),
            "weighted_contribution": round(w * v / total_w, 1) if total_w else 0,
        })
    items.sort(key=lambda x: abs(x["weighted_contribution"]), reverse=True)
    return items


# ---------------------------------------------------------------------------
# Main scoring function
# ---------------------------------------------------------------------------


def score_location(
    db: Session,
    lat: float,
    lon: float,
    category: str,
    radius_m: float = 1000,
) -> LocationScoreResult:
    """
    Compute an opportunity score (0-100) for a restaurant category at a
    given location, split into demand_score and cost_penalty sub-scores.
    """
    # 1. Nearby restaurants (single GiST query)
    same_cat, diff_cat = _nearby_restaurants(db, lat, lon, radius_m, category)

    # Count platform listings *within the same radius* (no N+1 global count)
    platform_sources = {"hungerstation", "talabat", "mrsool"}
    same_on_platforms = [r for r in same_cat if r.get("source") in platform_sources]
    all_on_platforms = [r for r in (same_cat + diff_cat) if r.get("source") in platform_sources]

    # Avg rating of same-category competitors
    rated = [r for r in same_cat if r.get("rating") is not None]
    avg_rating = (
        sum(float(r["rating"]) for r in rated) / len(rated) if rated else None
    )

    # 2. Compute demand factors
    traffic_info = traffic_score_at(db, lat, lon)
    traffic_info_close = traffic_score_at(db, lat, lon, radius_m=100)

    demand_factors = {
        "competition": competition_score(len(same_cat)),
        "complementary": complementary_score(len(diff_cat)),
        "population": population_score(db, lat, lon),
        "traffic": traffic_info.get("score", 25.0),
        "road_frontage": traffic_info_close.get("score", 25.0),
        "commercial_density": commercial_density_score(db, lat, lon),
        "delivery_demand": delivery_demand_score(len(same_on_platforms), len(all_on_platforms)),
        "competitor_rating": competitor_rating_score(avg_rating, len(rated)),
    }
    demand = _weighted_avg(demand_factors, DEMAND_WEIGHTS)

    # 3. Compute cost factors
    cost_factors = {
        "rent": 50.0,      # TODO: integrate with existing rent service
        "parking": 50.0,   # TODO: integrate with parking service
    }
    cost = _weighted_avg(cost_factors, COST_WEIGHTS)

    # 4. Composite opportunity score
    opportunity = 0.80 * demand + 0.20 * cost

    # 5. Merge all factors for full breakdown
    all_factors = {**demand_factors, **cost_factors}
    all_weights = {**DEMAND_WEIGHTS, **COST_WEIGHTS}
    contributions = _build_contributions(all_factors, all_weights)

    # 6. Confidence
    data_count = len(same_cat) + len(diff_cat)
    confidence = min(1.0, data_count / 20.0)

    # 7. Format nearby competitors
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
        opportunity_score=round(opportunity, 1),
        demand_score=round(demand, 1),
        cost_penalty=round(cost, 1),
        factors={k: round(v, 1) for k, v in all_factors.items()},
        contributions=contributions,
        confidence=round(confidence, 2),
        nearby_competitors=competitors_out,
        debug={
            "same_category_count": len(same_cat),
            "diff_category_count": len(diff_cat),
            "platform_count_nearby": len(all_on_platforms),
            "avg_competitor_rating": round(avg_rating, 2) if avg_rating else None,
            "radius_m": radius_m,
            "nearest_road": traffic_info.get("nearest_road"),
        },
    )
