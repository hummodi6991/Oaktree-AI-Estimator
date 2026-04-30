"""
Restaurant Location Scoring Engine.

Computes a composite *opportunity score* (0-100) for a given restaurant
category at a specific location, broken into two sub-scores:

- **demand_score** — aggregates demand-side signals: competition density,
  population, traffic, commercial density, delivery demand, dining cluster
  effect, competitor quality, anchor proximity, foot-traffic proxy,
  chain gap, income proxy, and visibility.
- **cost_penalty** — aggregates cost-side signals: rent, parking, zoning.
  Higher = cheaper / better opportunity.

``opportunity_score = 0.80 * demand_score + 0.20 * cost_penalty``

When a trained ML model is available, factor weights are predicted by
the model's feature importances rather than using static defaults.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.connectors.population import H3_RESOLUTION
from app.models.tables import PopulationDensity, RestaurantPOI
from app.services.restaurant_categories import CATEGORIES
from app.services.rent import RentMedianResult, aqar_rent_median
from app.services.traffic_proxy import traffic_score_at, road_class_score
from app.services.restaurant_scoring_factors import (
    ScoredFactor,
    zoning_fit_score as _zoning_fit_v2,
    parking_availability_score as _parking_v2,
    commercial_density_score as _commercial_density_v2,
    compute_factors_batch,
    _commercial_density_from_context,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# All delivery platform sources (used to identify platform listings)
# ---------------------------------------------------------------------------

PLATFORM_SOURCES = frozenset({
    "hungerstation", "talabat", "mrsool", "jahez", "toyou", "keeta",
    "thechefz", "lugmety", "shgardi", "ninja", "nana", "dailymealz",
    "careemfood", "deliveroo",
})

# ---------------------------------------------------------------------------
# Factor weights grouped by sub-score (static defaults)
# ---------------------------------------------------------------------------

DEMAND_WEIGHTS: dict[str, float] = {
    "competition": 0.18,
    "complementary": 0.04,
    "population": 0.14,
    "traffic": 0.10,
    "road_frontage": 0.04,
    "commercial_density": 0.08,
    "delivery_demand": 0.10,
    "competitor_rating": 0.08,
    "anchor_proximity": 0.08,
    "foot_traffic": 0.06,
    "chain_gap": 0.05,
    "income_proxy": 0.05,
}

COST_WEIGHTS: dict[str, float] = {
    "rent": 0.55,
    "parking": 0.20,
    "zoning_fit": 0.25,
}


@dataclass
class LocationScoreResult:
    opportunity_score: float
    demand_score: float
    cost_penalty: float
    factors: dict[str, float]
    contributions: list[dict[str, Any]]  # top-N feature contributions
    confidence: float
    confidence_score: float = 0.0  # data-reliability score (0-100)
    final_score: float = 0.0  # ranking score = opportunity * confidence blend
    contributions_confidence: list[dict[str, Any]] = field(default_factory=list)
    nearby_competitors: list[dict[str, Any]] = field(default_factory=list)
    model_version: str = "weighted_v3"
    ai_weights_used: bool = False
    elapsed_ms: float = 0.0  # total scoring time in ms
    factor_timing: dict[str, Any] = field(default_factory=dict)  # per-factor timing
    debug: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Nearby restaurants — single-query, uses GiST on geom
# ---------------------------------------------------------------------------

_NEARBY_SQL = text("""
    SELECT id, name, category, rating, source, lat, lon, chain_name,
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
        try:
            db.rollback()
        except Exception:
            pass
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
                "chain_name": r.chain_name,
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
        center_h3 = h3.latlng_to_cell(lat, lon, H3_RESOLUTION)
        edge_m = h3.average_hexagon_edge_length(H3_RESOLUTION, unit="m")
        k = max(1, int(round(radius_m / edge_m)))
        ring = h3.grid_disk(center_h3, k)
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
        try:
            db.rollback()
        except Exception:
            pass
        return 50.0

    if total_pop <= 0:
        return 10.0
    return min(95.0, 10.0 + 85.0 * (1 - math.exp(-total_pop / 10000)))


def commercial_density_score(db: Session, lat: float, lon: float, radius_m: float = 500) -> float:
    """Score based on density of commercial/office buildings (Overture).

    Delegates to the upgraded composite scorer and returns just the numeric
    score for backward compatibility.  The full ScoredFactor (with confidence
    and rationale) is available via ``commercial_density_score_v2()``.

    On DB error / poisoned transaction, returns the legacy neutral 50.0.
    """
    try:
        db.execute(text("SELECT 1"))
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return 50.0
    result = commercial_density_score_v2(db, lat, lon)
    return result.score


def commercial_density_score_v2(
    db: Session, lat: float, lon: float, radius_m: float = 500,
) -> ScoredFactor:
    """Upgraded commercial density — returns ScoredFactor with confidence."""
    return _commercial_density_v2(db, lat, lon, radius_m)


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
    """Lower rent -> higher score (better margins).

    Thresholds are calibrated for **monthly** Aqar commercial/retail rent in
    Riyadh (SAR/m²/month).  The ingestion pipeline normalises annual Aqar
    listings to monthly; values above 800 SAR/m²/month are clipped during
    normalisation, so the realistic input range is roughly 30-800.

    A piecewise-linear interpolation between breakpoints gives a smooth,
    discriminative mapping instead of the coarse step-function that collapsed
    almost every Riyadh parcel to 90.

    Breakpoints (rent → score):
        ≤ 50   →  95   very cheap (industrial / peripheral)
         100   →  80   below-average rent
         150   →  65   moderate
         250   →  45   above-average / desirable corridor
         400   →  25   prime retail
        ≥ 700  →  10   ultra-premium (near normalisation ceiling)
    """
    if rent_per_m2 is None:
        return 50.0
    if rent_per_m2 <= 0:
        return 50.0  # invalid / missing, neutral

    _BREAKPOINTS: list[tuple[float, float]] = [
        (50,  95.0),
        (100, 80.0),
        (150, 65.0),
        (250, 45.0),
        (400, 25.0),
        (700, 10.0),
    ]

    if rent_per_m2 <= _BREAKPOINTS[0][0]:
        return _BREAKPOINTS[0][1]
    if rent_per_m2 >= _BREAKPOINTS[-1][0]:
        return _BREAKPOINTS[-1][1]

    for i in range(len(_BREAKPOINTS) - 1):
        r0, s0 = _BREAKPOINTS[i]
        r1, s1 = _BREAKPOINTS[i + 1]
        if rent_per_m2 <= r1:
            t = (rent_per_m2 - r0) / (r1 - r0)
            return round(s0 + t * (s1 - s0), 1)

    return _BREAKPOINTS[-1][1]


# ---------------------------------------------------------------------------
# NEW scoring factors
# ---------------------------------------------------------------------------

def anchor_proximity_score(db: Session, lat: float, lon: float) -> float:
    """
    Score proximity to anchor destinations — malls, universities,
    government buildings, and hospitals. These generate high foot-traffic
    and are strong demand drivers for restaurants.
    """
    anchor_queries = [
        # Overture buildings: malls, shopping, retail classes
        ("""
            SELECT COUNT(*) FROM overture_buildings
            WHERE class IS NOT NULL
              AND (class ILIKE '%%mall%%'
                   OR class ILIKE '%%shopping%%'
                   OR class ILIKE '%%retail%%'
                   OR class ILIKE '%%commercial%%')
              AND ST_DWithin(
                  ST_Transform(geom, 4326)::geography,
                  ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                  :radius_m)
        """, 1500, 30),
        # OSM amenities: universities, schools, hospitals from polygon layer
        ("""
            SELECT COUNT(*) FROM planet_osm_polygon
            WHERE (amenity IS NOT NULL OR shop IS NOT NULL)
              AND ST_DWithin(
                  ST_Transform(way, 4326)::geography,
                  ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                  :radius_m)
        """, 1000, 20),
    ]

    total = 0.0
    for sql_str, radius, weight in anchor_queries:
        try:
            count = db.execute(
                text(sql_str), {"lat": lat, "lon": lon, "radius_m": radius}
            ).scalar() or 0
            total += min(weight, count * (weight / 3.0))
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
            total += weight * 0.5  # neutral fallback

    return min(95.0, max(10.0, 10.0 + total))


def foot_traffic_score(
    all_restaurants: list[dict], commercial_density: float, population: float
) -> float:
    """
    Proxy for foot-traffic based on combined restaurant density,
    commercial density score, and population score. True foot-traffic
    data would require mobility datasets; this is a reasonable proxy.
    """
    restaurant_count = len(all_restaurants)
    density_factor = min(1.0, restaurant_count / 30.0) * 40
    commercial_factor = commercial_density * 0.3
    pop_factor = population * 0.3
    return min(95.0, max(10.0, density_factor + commercial_factor + pop_factor))


def chain_gap_score(
    same_cat: list[dict], category: str, chain_name: str | None = None
) -> float:
    """
    Score based on whether the specific chain (or category in general)
    is underrepresented in the area. If a chain_name is provided,
    checks whether that chain already has nearby locations.
    """
    if not same_cat:
        return 90.0  # no competitors at all — huge gap

    if chain_name:
        chain_lower = chain_name.lower()
        chain_nearby = [
            r for r in same_cat
            if r.get("chain_name") and chain_lower in r["chain_name"].lower()
        ]
        if chain_nearby:
            # Chain already present — penalize
            return max(10.0, 50.0 - len(chain_nearby) * 15)
        return 85.0  # chain not present — good gap

    # No specific chain — score based on overall saturation
    if len(same_cat) <= 3:
        return 80.0
    if len(same_cat) <= 8:
        return 60.0
    if len(same_cat) <= 15:
        return 40.0
    return 20.0


def income_proxy_score(db: Session, lat: float, lon: float) -> float:
    """
    Estimate area income level from commercial rent rates as a proxy.
    Higher rent areas correlate with higher income / spending power,
    which benefits restaurants. Uses market indicator data when available.
    """
    try:
        from app.services.district_resolver import resolve_district
        resolution = resolve_district(db, city="riyadh", lat=lat, lon=lon)
        district = resolution.district_norm or resolution.district_raw
        if district:
            from app.services.indicators import latest_rent_per_m2
            rent = latest_rent_per_m2(db, "riyadh", district)
            if rent is not None:
                # Higher rent = higher income area = higher spending
                if rent > 1200:
                    return 90.0
                if rent > 800:
                    return 75.0
                if rent > 400:
                    return 60.0
                return 40.0
    except Exception as exc:
        logger.debug("Income proxy failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass

    return 50.0  # neutral default


def zoning_fit_score(db: Session, lat: float, lon: float) -> float:
    """
    Score how well the zoning/land-use at this location supports restaurants.

    Delegates to the upgraded ArcGIS-based scorer and returns just the
    numeric score for backward compatibility.  The full ScoredFactor (with
    confidence and rationale) is available via ``zoning_fit_score_v2()``.

    On DB error / poisoned transaction, returns the legacy neutral 50.0.
    """
    try:
        db.execute(text("SELECT 1"))
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return 50.0
    result = zoning_fit_score_v2(db, lat, lon)
    return result.score


def zoning_fit_score_v2(db: Session, lat: float, lon: float) -> ScoredFactor:
    """Upgraded zoning fit — returns ScoredFactor with confidence."""
    return _zoning_fit_v2(db, lat, lon)


def parking_availability_score(db: Session, lat: float, lon: float) -> float:
    """
    Score based on nearby parking availability.

    Delegates to the upgraded composite scorer and returns just the numeric
    score for backward compatibility.  The full ScoredFactor (with confidence
    and rationale) is available via ``parking_availability_score_v2()``.

    On DB error / poisoned transaction, returns the legacy neutral 50.0.
    """
    try:
        db.execute(text("SELECT 1"))
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return 50.0
    result = parking_availability_score_v2(db, lat, lon)
    return result.score


def parking_availability_score_v2(db: Session, lat: float, lon: float) -> ScoredFactor:
    """Upgraded parking — returns ScoredFactor with confidence."""
    return _parking_v2(db, lat, lon)


# ---------------------------------------------------------------------------
# AI-driven weight prediction
# ---------------------------------------------------------------------------

_MODEL_DIR = os.environ.get("MODEL_DIR", "models")
_MODEL_PKL_PATH = os.path.join(_MODEL_DIR, "restaurant_score_v0.pkl")
_MODEL_META_PATH = os.path.join(_MODEL_DIR, "restaurant_score_v0.meta.json")
_cached_ai_weights: dict[str, float] | None = None
_parcel_load_error: str | None = None
_parcel_meta: dict[str, Any] = {}

# Startup logging — runs once at import time
logger.info(
    "Parcel AI loader: MODEL_DIR=%s, pkl=%s (exists=%s), meta=%s (exists=%s)",
    _MODEL_DIR,
    _MODEL_PKL_PATH,
    os.path.exists(_MODEL_PKL_PATH),
    _MODEL_META_PATH,
    os.path.exists(_MODEL_META_PATH),
)


def get_ai_weights() -> dict[str, float] | None:
    """
    Load AI-predicted factor weights from the trained model's feature
    importances. Returns None if no model is available.
    """
    global _cached_ai_weights, _parcel_load_error, _parcel_meta
    if _cached_ai_weights is not None:
        return _cached_ai_weights

    try:
        with open(_MODEL_META_PATH) as f:
            meta = json.load(f)

        _parcel_meta = meta
        importances = meta.get("feature_importances", {})
        if not importances:
            _parcel_load_error = "meta.json present but feature_importances is empty"
            logger.warning("Parcel AI: %s", _parcel_load_error)
            return None

        # Map model feature names to scoring factor names.
        # IMPORTANT: google_confidence and has_google are excluded from
        # opportunity weights — they feed into confidence_score instead.
        feature_to_factor = {
            "restaurant_count": "competition",
            "avg_rating": "competitor_rating",
            "platform_count": "delivery_demand",
            "neighbor_competition": "complementary",
            "population": "population",
            "google_rating": "competitor_rating",
            "google_review_count": "delivery_demand",
            "log_review_count": "delivery_demand",
            "google_price_level": "income_proxy",
            # "google_confidence" and "has_google" deliberately excluded
        }

        weights: dict[str, float] = {}
        total_imp = sum(importances.values()) or 1.0
        for feat_name, importance in importances.items():
            factor = feature_to_factor.get(feat_name)
            if factor:
                weights[factor] = weights.get(factor, 0.0) + importance / total_imp

        # Distribute remaining weight to factors not in the model
        covered = sum(weights.values())
        remaining_factors = [
            k for k in DEMAND_WEIGHTS if k not in weights
        ]
        if remaining_factors and covered < 1.0:
            per_factor = (1.0 - covered) / len(remaining_factors)
            for f in remaining_factors:
                weights[f] = per_factor

        _cached_ai_weights = weights
        _parcel_load_error = None
        logger.info("Parcel AI: loaded AI weights successfully — version=%s",
                     meta.get("model_version", "unknown"))
        return weights

    except FileNotFoundError:
        _parcel_load_error = f"Model meta not found at {_MODEL_META_PATH}"
        logger.info("Parcel AI: %s — will use static fallback weights", _parcel_load_error)
        return None
    except Exception as exc:
        _parcel_load_error = f"Failed to load: {exc}"
        logger.warning("Parcel AI: %s", _parcel_load_error)
        return None


def get_parcel_ai_status() -> dict[str, Any]:
    """
    Return introspection info for the parcel AI (scoring weight) model.
    Used by the ``/v1/restaurant/parcel-ai-status`` endpoint.
    """
    # Ensure we've attempted loading
    ai_w = get_ai_weights()
    available = ai_w is not None
    return {
        "available": available,
        "artifact_present": os.path.exists(_MODEL_PKL_PATH),
        "model_path": _MODEL_PKL_PATH,
        "meta_path": _MODEL_META_PATH,
        "meta_present": os.path.exists(_MODEL_META_PATH),
        "load_error": _parcel_load_error,
        "model_version": _parcel_meta.get("model_version") if available else None,
        "trained_at": _parcel_meta.get("trained_at") if available else None,
        "mae": _parcel_meta.get("mae") if available else None,
        "r2": _parcel_meta.get("r2") if available else None,
        "n_rows": _parcel_meta.get("n_rows") if available else None,
        "fallback_mode": not available,
    }


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
# Fast-path helpers using pre-fetched context (Phase 7 perf optimization)
# ---------------------------------------------------------------------------

# Restaurant categories for POI ecosystem filtering (same list as in scoring_factors)
_RESTAURANT_CATEGORIES = frozenset({
    "burger", "pizza", "coffee", "cafe", "bakery",
    "shawarma", "chicken", "seafood", "asian",
    "indian", "italian", "steak", "sushi",
    "healthy", "dessert", "juice", "ice_cream",
    "middle_eastern", "fast_food", "arabic",
    "breakfast", "sandwich", "turkish",
})


def _traffic_from_roads(
    roads: list[dict], max_dist: float = 200,
) -> dict:
    """Compute traffic score from pre-fetched road rows, filtering by distance."""
    filtered = [r for r in roads if float(r.get("distance_m", 9999)) <= max_dist]
    if not filtered:
        return {"score": 10.0, "road_count": 0, "nearest_road": None}

    total_weight = 0.0
    weighted_score = 0.0
    for r in filtered:
        dist = float(r.get("distance_m", 1.0)) or 1.0
        weight = 1.0 / dist
        total_weight += weight
        weighted_score += weight * road_class_score(r.get("highway"))

    avg_score = weighted_score / total_weight if total_weight > 0 else 25.0
    nearest = filtered[0]
    return {
        "score": round(avg_score, 1),
        "road_count": len(filtered),
        "nearest_road": {
            "class": nearest.get("highway"),
            "name": nearest.get("name"),
            "distance_m": round(float(nearest.get("distance_m", 0)), 1),
        },
    }


def _anchor_proximity_from_context(
    overture_commercial_count: int,
    osm_amenity_count: int,
) -> float:
    """Compute anchor_proximity score from pre-fetched counts.
    Same logic as anchor_proximity_score but using pre-computed counts."""
    total = 0.0
    # Overture commercial buildings within 1500m (weight=30)
    total += min(30, overture_commercial_count * (30.0 / 3.0))
    # OSM amenities within 1000m (weight=20)
    total += min(20, osm_amenity_count * (20.0 / 3.0))
    return min(95.0, max(10.0, 10.0 + total))


def _poi_ecosystem_from_nearby(all_nearby: list[dict]) -> float:
    """Compute POI ecosystem sub-score from already-fetched nearby restaurants.
    Avoids a separate restaurant_poi query."""
    within_800 = [r for r in all_nearby if float(r.get("distance_m", 9999)) <= 800]
    total = len(within_800)
    if total == 0:
        return 15.0

    # Count non-restaurant POIs and source diversity (same logic as _nearby_poi_ecosystem)
    diversity = len({
        str(r.get("source") or "").strip().lower()
        for r in within_800 if r.get("source")
    })

    score = min(90.0, 15.0 + 75.0 * (1 - math.exp(-total / 40.0)))
    if diversity >= 3:
        score += 5.0
    return max(10.0, min(95.0, score))


# ---------------------------------------------------------------------------
# Main scoring function
# ---------------------------------------------------------------------------


def score_location(
    db: Session,
    lat: float,
    lon: float,
    category: str,
    radius_m: float = 1000,
    chain_name: str | None = None,
    use_ai_weights: bool = True,
) -> LocationScoreResult:
    """
    Compute an opportunity score (0-100) for a restaurant category at a
    given location, split into demand_score and cost_penalty sub-scores.

    When ``use_ai_weights=True`` (default), the trained ML model's feature
    importances are used to dynamically weight demand factors instead of
    static defaults.

    Performance (Phase 7): uses ``compute_factors_batch()`` to consolidate
    ~12 spatial queries into 5 and reuses pre-fetched road/building context
    for traffic and anchor_proximity factors, eliminating 4+ more queries.
    Total DB round-trips reduced from ~23 to ~10.
    """
    t_total = time.perf_counter()
    factor_timing: dict[str, float] = {}

    # 1. Nearby restaurants (single GiST query)
    t = time.perf_counter()
    same_cat, diff_cat = _nearby_restaurants(db, lat, lon, radius_m, category)
    factor_timing["nearby_restaurants_ms"] = round((time.perf_counter() - t) * 1000, 1)

    # Count platform listings *within the same radius*
    same_on_platforms = [r for r in same_cat if r.get("source") in PLATFORM_SOURCES]
    all_on_platforms = [r for r in (same_cat + diff_cat) if r.get("source") in PLATFORM_SOURCES]

    # Avg rating of same-category competitors
    rated = [r for r in same_cat if r.get("rating") is not None]
    avg_rating = (
        sum(float(r["rating"]) for r in rated) / len(rated) if rated else None
    )

    # Confidence-gating constants (Phase 6)
    _CONFIDENCE_GATE_FLOOR = 0.3
    _NEUTRAL_FALLBACK = 45.0

    # 2. Batch-compute zoning, parking, commercial_density + shared context
    t = time.perf_counter()
    batch = compute_factors_batch(db, lat, lon)
    factor_timing["batch_factors_ms"] = round((time.perf_counter() - t) * 1000, 1)
    factor_timing["batch_detail"] = batch.timing

    # Compute POI ecosystem from already-fetched nearby restaurants (no extra query)
    all_nearby = same_cat + diff_cat
    poi_sub_score = _poi_ecosystem_from_nearby(all_nearby)

    # Update commercial density with real POI sub-score
    comm_density_v2 = batch.commercial_density
    if abs(poi_sub_score - 50.0) > 1.0:
        # Re-compute with actual POI sub-score from nearby restaurants
        from app.services.restaurant_scoring_factors import (
            _compute_osm_anchor_weighted,
            _batch_fetch_buildings,
        )
        comm_density_v2 = _commercial_density_from_context(
            batch.timing.get("_bld_ctx", {}),  # not available, use score as-is
            [], batch.commercial_density.meta.get("parcels", {}),
            {"weighted_total": batch.osm_anchor_weighted_800},
            poi_sub_score=poi_sub_score,
        )
        # Simpler: just adjust the composite score proportionally
        # Original used poi=50.0 (default), real value may differ
        _W_POI = 0.20
        old_poi_contribution = _W_POI * 50.0
        new_poi_contribution = _W_POI * poi_sub_score
        adjusted_score = batch.commercial_density.score - old_poi_contribution + new_poi_contribution
        adjusted_score = max(10.0, min(95.0, adjusted_score))
        comm_density_v2 = ScoredFactor(
            score=round(adjusted_score, 1),
            confidence=batch.commercial_density.confidence,
            rationale=batch.commercial_density.rationale,
            meta={**batch.commercial_density.meta,
                  "poi_ecosystem": {"score": round(poi_sub_score, 1),
                                    "source": "poi_from_nearby_restaurants"}},
        )

    comm_density = comm_density_v2.score

    # 2b. Traffic scores from pre-fetched roads (no extra DB queries)
    t = time.perf_counter()
    traffic_info = _traffic_from_roads(batch.roads_300m, max_dist=200)
    traffic_info_close = _traffic_from_roads(batch.roads_300m, max_dist=100)
    factor_timing["traffic_ms"] = round((time.perf_counter() - t) * 1000, 1)

    # 2c. Population score
    t = time.perf_counter()
    pop_score = population_score(db, lat, lon)
    factor_timing["population_ms"] = round((time.perf_counter() - t) * 1000, 1)

    # Confidence-gate commercial density on demand side too
    if comm_density_v2.confidence < _CONFIDENCE_GATE_FLOOR:
        _blend = comm_density_v2.confidence / _CONFIDENCE_GATE_FLOOR
        comm_density = _blend * comm_density + (1 - _blend) * _NEUTRAL_FALLBACK

    # 2d. Anchor proximity from pre-fetched context (no extra DB queries)
    anchor_prox = _anchor_proximity_from_context(
        batch.overture_commercial_1500,
        batch.osm_amenity_count_1000,
    )

    demand_factors = {
        "competition": competition_score(len(same_cat)),
        "complementary": complementary_score(len(diff_cat)),
        "population": pop_score,
        "traffic": traffic_info.get("score", 25.0),
        "road_frontage": traffic_info_close.get("score", 25.0),
        "commercial_density": comm_density,
        "delivery_demand": delivery_demand_score(len(same_on_platforms), len(all_on_platforms)),
        "competitor_rating": competitor_rating_score(avg_rating, len(rated)),
        "anchor_proximity": anchor_prox,
        "foot_traffic": foot_traffic_score(same_cat + diff_cat, comm_density, pop_score),
        "chain_gap": chain_gap_score(same_cat, category, chain_name),
        "income_proxy": income_proxy_score(db, lat, lon),
    }

    # Try AI-predicted weights
    ai_used = False
    demand_w = DEMAND_WEIGHTS
    if use_ai_weights:
        ai_w = get_ai_weights()
        if ai_w:
            demand_w = ai_w
            ai_used = True

    demand = _weighted_avg(demand_factors, demand_w)

    # 3. Compute cost factors — wired up to real Aqar rent data
    t = time.perf_counter()
    rent_resolution = _resolve_rent_aqar(db, lat, lon)
    factor_timing["rent_ms"] = round((time.perf_counter() - t) * 1000, 1)
    rent_val = rent_resolution.rent_per_m2

    # Upgraded v2 factors from batch (already computed, no extra queries)
    parking_v2 = batch.parking
    zoning_v2 = batch.zoning

    cost_factors = {
        "rent": rent_score_value(rent_val),
        "parking": parking_v2.score,
        "zoning_fit": zoning_v2.score,
    }

    # Confidence-gated cost factors (Phase 6)
    cost_factor_confidence: dict[str, float] = {
        "rent": 1.0 if rent_val and rent_val > 0 else 0.2,
        "parking": parking_v2.confidence,
        "zoning_fit": zoning_v2.confidence,
    }
    for k in ("parking", "zoning_fit"):
        conf = cost_factor_confidence[k]
        if conf < _CONFIDENCE_GATE_FLOOR:
            blend = conf / _CONFIDENCE_GATE_FLOOR
            cost_factors[k] = blend * cost_factors[k] + (1 - blend) * _NEUTRAL_FALLBACK

    cost = _weighted_avg(cost_factors, COST_WEIGHTS)

    # 4. Composite opportunity score (market-only)
    opportunity = 0.80 * demand + 0.20 * cost

    # 5. Merge all factors for full breakdown (opportunity contributions)
    all_factors = {**demand_factors, **cost_factors}
    all_weights = {**demand_w, **COST_WEIGHTS}
    contributions = _build_contributions(all_factors, all_weights)

    # 6. Legacy confidence
    data_count = len(same_cat) + len(diff_cat)
    platform_coverage = len(all_on_platforms)
    confidence = min(1.0, (data_count / 20.0) * 0.6 + (platform_coverage / 10.0) * 0.4)

    # 7. Confidence score (consolidated: 3 queries → 1)
    t = time.perf_counter()
    confidence_features = _compute_confidence_features(db, lat, lon, same_cat, diff_cat)
    confidence_features["rent_data_quality"] = _rent_data_quality(rent_resolution)
    factor_timing["confidence_ms"] = round((time.perf_counter() - t) * 1000, 1)

    confidence_score_01 = _aggregate_confidence(confidence_features)
    confidence_score_01 = max(0.0, min(1.0, confidence_score_01))
    confidence_score_100 = round(confidence_score_01 * 100, 1)

    # 8. Final ranking score: opportunity dampened by confidence
    _CONFIDENCE_BASE = 0.60
    final_score = round(
        opportunity * (_CONFIDENCE_BASE + (1 - _CONFIDENCE_BASE) * confidence_score_01),
        1,
    )

    # 9. Confidence contributions
    contributions_confidence = _build_confidence_contributions(confidence_features)

    # 10. Format nearby competitors
    competitors_out = sorted(
        [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "category": r.get("category"),
                "rating": r.get("rating"),
                "source": r.get("source"),
                "chain_name": r.get("chain_name"),
                "distance_m": round(float(r.get("distance_m", 0)), 0),
            }
            for r in same_cat
        ],
        key=lambda x: x.get("distance_m", 0),
    )[:20]

    elapsed_total_ms = round((time.perf_counter() - t_total) * 1000, 1)
    factor_timing["total_ms"] = elapsed_total_ms
    logger.info(
        "score_location: %.1f ms total (batch=%.1f nearby=%.1f rent=%.1f pop=%.1f conf=%.1f)",
        elapsed_total_ms,
        factor_timing.get("batch_factors_ms", 0),
        factor_timing.get("nearby_restaurants_ms", 0),
        factor_timing.get("rent_ms", 0),
        factor_timing.get("population_ms", 0),
        factor_timing.get("confidence_ms", 0),
    )

    return LocationScoreResult(
        opportunity_score=round(opportunity, 1),
        demand_score=round(demand, 1),
        cost_penalty=round(cost, 1),
        factors={k: round(v, 1) for k, v in all_factors.items()},
        contributions=contributions,
        confidence=round(confidence, 2),
        confidence_score=confidence_score_100,
        final_score=final_score,
        contributions_confidence=contributions_confidence,
        nearby_competitors=competitors_out,
        model_version="ai_weighted_v3" if ai_used else "weighted_v3",
        ai_weights_used=ai_used,
        elapsed_ms=elapsed_total_ms,
        factor_timing=factor_timing,
        debug={
            "same_category_count": len(same_cat),
            "diff_category_count": len(diff_cat),
            "platform_count_nearby": len(all_on_platforms),
            "platform_sources_detected": list({r.get("source") for r in all_on_platforms}),
            "avg_competitor_rating": round(avg_rating, 2) if avg_rating else None,
            "radius_m": radius_m,
            "nearest_road": traffic_info.get("nearest_road"),
            "rent_per_m2": rent_val,
            "ai_weights_used": ai_used,
            "confidence_features": confidence_features,
            "rent_meta": {
                "aqar_used": rent_resolution.scope not in ("indicator_fallback", "none"),
                "scope": rent_resolution.scope,
                "method": rent_resolution.method,
                "sample_count": rent_resolution.sample_count,
                "median_rent_per_m2": rent_resolution.median_used,
                "asset_type": _RESTAURANT_AQAR_ASSET,
                "unit_type": _RESTAURANT_AQAR_UNIT,
            },
            "factor_confidence": {
                "zoning_fit": {
                    "confidence": zoning_v2.confidence,
                    "rationale": zoning_v2.rationale,
                    "meta": zoning_v2.meta,
                },
                "parking": {
                    "confidence": parking_v2.confidence,
                    "rationale": parking_v2.rationale,
                    "meta": parking_v2.meta,
                },
                "commercial_density": {
                    "confidence": comm_density_v2.confidence,
                    "rationale": comm_density_v2.rationale,
                    "meta": comm_density_v2.meta,
                },
            },
        },
    )


# ---------------------------------------------------------------------------
# Confidence score helpers
# ---------------------------------------------------------------------------

# Weights for confidence sub-factors (must sum to 1.0)
#
# The original version relied only on Google enrichment signals. That makes the
# confidence score collapse to zero in perfectly valid areas whenever nearby POIs
# have not been enriched yet. Keep Google as the strongest signal, but add
# fallback evidence signals so confidence reflects actual nearby market evidence.
_CONF_WEIGHTS = {
    "has_google": 0.15,
    "google_confidence": 0.15,
    "review_sufficiency": 0.15,
    "nearby_evidence": 0.15,
    "source_diversity": 0.10,
    "rating_coverage": 0.10,
    "rent_data_quality": 0.20,
}


def _compute_confidence_features(
    db: Session,
    lat: float,
    lon: float,
    same_cat: list[dict],
    diff_cat: list[dict],
) -> dict[str, float]:
    """
    Compute confidence/reliability features for the scored location.
    Returns dict of feature name -> value in [0, 1].

    Phase 7: consolidates 3 separate DB queries into 1.
    """
    all_nearby = same_cat + diff_cat

    # Consolidated query: google_place_id count, avg confidence, total reviews
    google_count = 0
    total_with_google_check = 0
    avg_google_conf = 0.0
    total_reviews = 0

    if all_nearby:
        ids = [r.get("id") for r in all_nearby if r.get("id")]
        if ids:
            try:
                from sqlalchemy import text as sa_text
                placeholders = ", ".join([f":id_{i}" for i in range(len(ids))])
                params = {f"id_{i}": id_val for i, id_val in enumerate(ids)}
                row = db.execute(
                    sa_text(
                        f"SELECT"
                        f"  COUNT(*) FILTER (WHERE google_place_id IS NOT NULL) AS google_count,"
                        f"  COUNT(*) AS total,"
                        f"  AVG(google_confidence) FILTER (WHERE google_confidence IS NOT NULL) AS avg_conf,"
                        f"  COALESCE(SUM(review_count), 0) AS total_reviews"
                        f" FROM restaurant_poi WHERE id IN ({placeholders})"
                    ),
                    params,
                ).fetchone()
                if row:
                    google_count = row[0] or 0
                    total_with_google_check = row[1] or 0
                    avg_google_conf = float(row[2]) if row[2] else 0.0
                    total_reviews = int(row[3]) if row[3] else 0
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass

    has_google = (
        google_count / total_with_google_check
        if total_with_google_check > 0
        else 0.0
    )

    google_conf_score = max(0.0, min(1.0, avg_google_conf))
    review_sufficiency = min(1.0, math.log1p(total_reviews) / math.log1p(200))
    nearby_evidence = min(1.0, len(all_nearby) / 8.0) if all_nearby else 0.0

    distinct_sources = len(
        {
            str(r.get("source") or "").strip().lower()
            for r in all_nearby
            if r.get("source")
        }
    )
    source_diversity = min(1.0, distinct_sources / 4.0)

    rated_count = sum(1 for r in all_nearby if r.get("rating") is not None)
    rating_coverage = (rated_count / len(all_nearby)) if all_nearby else 0.0

    return {
        "has_google": round(has_google, 4),
        "google_confidence": round(google_conf_score, 4),
        "review_sufficiency": round(review_sufficiency, 4),
        "nearby_evidence": round(nearby_evidence, 4),
        "source_diversity": round(source_diversity, 4),
        "rating_coverage": round(rating_coverage, 4),
    }


def _aggregate_confidence(features: dict[str, float]) -> float:
    """Weighted average of confidence features -> score in [0, 1].

    Normalize by the weights of the features actually present so legacy callers
    or tests that only provide a subset of factors still behave correctly.
    """
    weighted_sum = sum(_CONF_WEIGHTS.get(k, 0.0) * float(v) for k, v in features.items())
    weight_total = sum(_CONF_WEIGHTS.get(k, 0.0) for k in features.keys())
    if weight_total <= 0:
        return 0.0
    return max(0.0, min(1.0, weighted_sum / weight_total))


def _build_confidence_contributions(
    features: dict[str, float],
) -> list[dict[str, Any]]:
    """Explainable confidence contributions, sorted by weighted impact."""
    items = []
    for k, v in features.items():
        w = _CONF_WEIGHTS.get(k, 0)
        items.append({
            "factor": k,
            "score": round(v, 4),
            "weight": round(w, 3),
            "weighted_contribution": round(w * v, 4),
        })
    items.sort(key=lambda x: abs(x["weighted_contribution"]), reverse=True)
    return items


@dataclass
class _RentResolution:
    """Internal result from Aqar-aware rent lookup for Restaurant Finder."""
    rent_per_m2: float | None
    scope: str  # "district" | "district_shrinkage" | "city" | "city_asset" | "indicator_fallback" | "none"
    sample_count: int
    median_used: float | None
    method: str  # human-readable label


def _rent_data_quality(rent: _RentResolution) -> float:
    """
    Map rent resolution scope to a 0-1 confidence contribution.

    - district with strong sample: 1.0
    - district_shrinkage: 0.7
    - city / city_asset: 0.5
    - indicator_fallback: 0.2
    - none: 0.0
    """
    _SCOPE_QUALITY = {
        "district": 1.0,
        "district_shrinkage": 0.7,
        "city": 0.5,
        "city_asset": 0.5,
        "indicator_fallback": 0.2,
        "none": 0.0,
    }
    return _SCOPE_QUALITY.get(rent.scope, 0.0)


# Restaurant rent config — mirrors COMPONENT_AQAR_CONFIG in estimates.py
_RESTAURANT_AQAR_ASSET = "commercial"
_RESTAURANT_AQAR_UNIT = "retail"  # best proxy for restaurant space
_RESTAURANT_AQAR_MIN_SAMPLES = 5
_RESTAURANT_AQAR_SINCE_DAYS = 730


def _resolve_rent_aqar(db: Session, lat: float, lon: float) -> _RentResolution:
    """
    Resolve rent for Restaurant Finder using Aqar rent comps.

    Hierarchy (same as Development Feasibility / estimates):
      1. District Aqar median (if >= min_samples)
      2. District shrinkage toward city median (if district sparse but >0)
      3. City Aqar median (unit_type match)
      4. City Aqar median (asset_type only)
      5. Indicator-based fallback (legacy _resolve_rent logic)
      6. None
    """
    try:
        from app.services.district_resolver import resolve_district

        resolution = resolve_district(db, city="riyadh", lat=lat, lon=lon)
        city_norm = resolution.city_norm or "riyadh"
        district_norm = resolution.district_norm or None

        result = aqar_rent_median(
            db,
            city=city_norm,
            district=district_norm,
            asset_type=_RESTAURANT_AQAR_ASSET,
            unit_type=_RESTAURANT_AQAR_UNIT,
            since_days=_RESTAURANT_AQAR_SINCE_DAYS,
        )

        # Determine best city-level baseline for shrinkage
        base_city_median: float | None = None
        if result.city_median is not None:
            base_city_median = float(result.city_median)
        elif result.city_asset_median is not None:
            base_city_median = float(result.city_asset_median)

        min_samples = _RESTAURANT_AQAR_MIN_SAMPLES

        # 1. District median with enough samples
        if result.district_median is not None and result.n_district >= min_samples:
            return _RentResolution(
                rent_per_m2=float(result.district_median),
                scope="district",
                sample_count=result.n_district,
                median_used=float(result.district_median),
                method="aqar_district_median",
            )

        # 2. District shrinkage (sparse district blended toward city)
        if (
            result.district_median is not None
            and result.n_district > 0
            and base_city_median is not None
        ):
            weight = min(1.0, result.n_district / float(min_samples))
            blended = float(result.district_median) * weight + base_city_median * (1.0 - weight)
            return _RentResolution(
                rent_per_m2=blended,
                scope="district_shrinkage",
                sample_count=result.n_district,
                median_used=blended,
                method="aqar_district_shrinkage",
            )

        # 3. City median (unit_type match)
        if result.city_median is not None and result.n_city >= min_samples:
            return _RentResolution(
                rent_per_m2=float(result.city_median),
                scope="city",
                sample_count=result.n_city,
                median_used=float(result.city_median),
                method="aqar_city_median",
            )

        # 4. City median (asset_type only)
        if result.city_asset_median is not None and result.n_city_asset >= min_samples:
            return _RentResolution(
                rent_per_m2=float(result.city_asset_median),
                scope="city_asset",
                sample_count=result.n_city_asset,
                median_used=float(result.city_asset_median),
                method="aqar_city_asset_median",
            )

    except Exception as exc:
        logger.debug("Aqar rent resolution failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass

    # 5. Indicator-based fallback (legacy)
    return _resolve_rent_indicator_fallback(db, lat, lon)


def _resolve_rent_indicator_fallback(db: Session, lat: float, lon: float) -> _RentResolution:
    """Legacy rent resolution via market indicators — used when Aqar data is unavailable."""
    try:
        from app.services.district_resolver import resolve_district
        resolution = resolve_district(db, city="riyadh", lat=lat, lon=lon)
        district = resolution.district_norm or resolution.district_raw
        if district:
            from app.services.indicators import latest_rent_per_m2
            rent = latest_rent_per_m2(db, "riyadh", district)
            if rent is not None:
                return _RentResolution(
                    rent_per_m2=rent,
                    scope="indicator_fallback",
                    sample_count=0,
                    median_used=rent,
                    method="indicator_district_rent",
                )
    except Exception as exc:
        logger.debug("Indicator rent fallback failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass

    return _RentResolution(
        rent_per_m2=None,
        scope="none",
        sample_count=0,
        median_used=None,
        method="none",
    )
