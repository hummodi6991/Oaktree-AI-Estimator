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
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.models.tables import PopulationDensity, RestaurantPOI
from app.services.restaurant_categories import CATEGORIES
from app.services.rent import RentMedianResult, aqar_rent_median
from app.services.traffic_proxy import traffic_score_at

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
# NEW scoring factors
# ---------------------------------------------------------------------------

def anchor_proximity_score(db: Session, lat: float, lon: float) -> float:
    """
    Score proximity to anchor destinations — malls, universities,
    government buildings, and hospitals. These generate high foot-traffic
    and are strong demand drivers for restaurants.
    """
    anchor_queries = [
        # Overture places: malls & shopping centers
        ("""
            SELECT COUNT(*) FROM restaurant_poi
            WHERE source = 'overture'
              AND (category ILIKE '%%mall%%' OR category ILIKE '%%shopping%%')
              AND geom IS NOT NULL
              AND ST_DWithin(
                  geom::geography,
                  ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                  :radius_m)
        """, 1500, 30),
        # OSM amenities: universities, schools, hospitals
        ("""
            SELECT COUNT(*) FROM osm_roads
            WHERE highway IS NULL
              AND name IS NOT NULL
              AND ST_DWithin(
                  geom::geography,
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

    return 50.0  # neutral default


def zoning_fit_score(db: Session, lat: float, lon: float) -> float:
    """
    Score how well the zoning/land-use at this location supports
    restaurants. Commercial and mixed-use zones score higher.
    """
    try:
        result = db.execute(
            text("""
                SELECT zoning FROM parcel
                WHERE geom IS NOT NULL
                  AND ST_Contains(
                      geom,
                      ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
                  )
                LIMIT 1
            """),
            {"lat": lat, "lon": lon},
        ).scalar()
        if result:
            zoning = result.lower()
            if any(k in zoning for k in ("commercial", "تجاري", "mixed", "متعدد")):
                return 90.0
            if any(k in zoning for k in ("residential", "سكني")):
                return 40.0
            return 60.0
    except Exception:
        pass

    return 50.0  # neutral when no zoning data


def parking_availability_score(db: Session, lat: float, lon: float) -> float:
    """Score based on nearby parking availability."""
    try:
        count = db.execute(
            text("""
                SELECT COUNT(*) FROM overture_buildings
                WHERE (class ILIKE '%%parking%%' OR class ILIKE '%%garage%%')
                  AND ST_DWithin(
                      geom::geography,
                      ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                      500
                  )
            """),
            {"lat": lat, "lon": lon},
        ).scalar() or 0
        if count >= 3:
            return 90.0
        if count >= 1:
            return 70.0
    except Exception:
        pass

    return 50.0  # neutral


# ---------------------------------------------------------------------------
# AI-driven weight prediction
# ---------------------------------------------------------------------------

_MODEL_DIR = os.environ.get("MODEL_DIR", "models")
_MODEL_META_PATH = os.path.join(_MODEL_DIR, "restaurant_score_v0.meta.json")
_cached_ai_weights: dict[str, float] | None = None


def get_ai_weights() -> dict[str, float] | None:
    """
    Load AI-predicted factor weights from the trained model's feature
    importances. Returns None if no model is available.
    """
    global _cached_ai_weights
    if _cached_ai_weights is not None:
        return _cached_ai_weights

    try:
        with open(_MODEL_META_PATH) as f:
            meta = json.load(f)

        importances = meta.get("feature_importances", {})
        if not importances:
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
        logger.info("Loaded AI weights: %s", weights)
        return weights

    except FileNotFoundError:
        logger.debug("No trained model found at %s", _MODEL_META_PATH)
        return None
    except Exception as exc:
        logger.warning("Failed to load AI weights: %s", exc)
        return None


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
    chain_name: str | None = None,
    use_ai_weights: bool = True,
) -> LocationScoreResult:
    """
    Compute an opportunity score (0-100) for a restaurant category at a
    given location, split into demand_score and cost_penalty sub-scores.

    When ``use_ai_weights=True`` (default), the trained ML model's feature
    importances are used to dynamically weight demand factors instead of
    static defaults.
    """
    # 1. Nearby restaurants (single GiST query)
    same_cat, diff_cat = _nearby_restaurants(db, lat, lon, radius_m, category)

    # Count platform listings *within the same radius*
    same_on_platforms = [r for r in same_cat if r.get("source") in PLATFORM_SOURCES]
    all_on_platforms = [r for r in (same_cat + diff_cat) if r.get("source") in PLATFORM_SOURCES]

    # Avg rating of same-category competitors
    rated = [r for r in same_cat if r.get("rating") is not None]
    avg_rating = (
        sum(float(r["rating"]) for r in rated) / len(rated) if rated else None
    )

    # 2. Compute demand factors
    traffic_info = traffic_score_at(db, lat, lon)
    traffic_info_close = traffic_score_at(db, lat, lon, radius_m=100)
    comm_density = commercial_density_score(db, lat, lon)
    pop_score = population_score(db, lat, lon)

    demand_factors = {
        "competition": competition_score(len(same_cat)),
        "complementary": complementary_score(len(diff_cat)),
        "population": pop_score,
        "traffic": traffic_info.get("score", 25.0),
        "road_frontage": traffic_info_close.get("score", 25.0),
        "commercial_density": comm_density,
        "delivery_demand": delivery_demand_score(len(same_on_platforms), len(all_on_platforms)),
        "competitor_rating": competitor_rating_score(avg_rating, len(rated)),
        "anchor_proximity": anchor_proximity_score(db, lat, lon),
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
    rent_resolution = _resolve_rent_aqar(db, lat, lon)
    rent_val = rent_resolution.rent_per_m2
    cost_factors = {
        "rent": rent_score_value(rent_val),
        "parking": parking_availability_score(db, lat, lon),
        "zoning_fit": zoning_fit_score(db, lat, lon),
    }
    cost = _weighted_avg(cost_factors, COST_WEIGHTS)

    # 4. Composite opportunity score (market-only)
    opportunity = 0.80 * demand + 0.20 * cost

    # 5. Merge all factors for full breakdown (opportunity contributions)
    all_factors = {**demand_factors, **cost_factors}
    all_weights = {**demand_w, **COST_WEIGHTS}
    contributions = _build_contributions(all_factors, all_weights)

    # 6. Legacy confidence — based on data availability (kept for backward compat)
    data_count = len(same_cat) + len(diff_cat)
    platform_coverage = len(all_on_platforms)
    confidence = min(1.0, (data_count / 20.0) * 0.6 + (platform_coverage / 10.0) * 0.4)

    # 7. Confidence score — data reliability (0-1 then 0-100)
    confidence_features = _compute_confidence_features(db, lat, lon, same_cat, diff_cat)
    confidence_features["rent_data_quality"] = _rent_data_quality(rent_resolution)
    confidence_score_01 = _aggregate_confidence(confidence_features)
    confidence_score_01 = max(0.0, min(1.0, confidence_score_01))
    confidence_score_100 = round(confidence_score_01 * 100, 1)

    # 8. Final ranking score: opportunity dampened by confidence
    _CONFIDENCE_BASE = 0.60
    final_score = round(
        opportunity * (_CONFIDENCE_BASE + (1 - _CONFIDENCE_BASE) * confidence_score_01),
        1,
    )

    # 9. Confidence contributions (rule-based, explainable)
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
    """
    all_nearby = same_cat + diff_cat

    # 1. has_google: fraction of nearby POIs with a google_place_id
    google_count = 0
    total_with_google_check = 0
    if all_nearby:
        ids = [r.get("id") for r in all_nearby if r.get("id")]
        if ids:
            try:
                from sqlalchemy import text as sa_text
                placeholders = ", ".join([f":id_{i}" for i in range(len(ids))])
                params = {f"id_{i}": id_val for i, id_val in enumerate(ids)}
                result = db.execute(
                    sa_text(
                        f"SELECT COUNT(*) FILTER (WHERE google_place_id IS NOT NULL),"
                        f"       COUNT(*)"
                        f" FROM restaurant_poi WHERE id IN ({placeholders})"
                    ),
                    params,
                ).fetchone()
                if result:
                    google_count = result[0] or 0
                    total_with_google_check = result[1] or 0
            except Exception:
                pass

    has_google = (
        google_count / total_with_google_check
        if total_with_google_check > 0
        else 0.0
    )

    # 2. google_confidence: average google_confidence of nearby POIs
    avg_google_conf = 0.0
    if all_nearby:
        ids = [r.get("id") for r in all_nearby if r.get("id")]
        if ids:
            try:
                from sqlalchemy import text as sa_text
                placeholders = ", ".join([f":id_{i}" for i in range(len(ids))])
                params = {f"id_{i}": id_val for i, id_val in enumerate(ids)}
                result = db.execute(
                    sa_text(
                        f"SELECT AVG(google_confidence)"
                        f" FROM restaurant_poi"
                        f" WHERE id IN ({placeholders})"
                        f"   AND google_confidence IS NOT NULL"
                    ),
                    params,
                ).scalar()
                avg_google_conf = float(result) if result else 0.0
            except Exception:
                pass

    # Clamp to [0, 1] — google_confidence is stored as 0-1
    google_conf_score = max(0.0, min(1.0, avg_google_conf))

    # 3. review_sufficiency: log1p(review_count) / log1p(200) clamped to [0, 1]
    total_reviews = 0
    if all_nearby:
        ids = [r.get("id") for r in all_nearby if r.get("id")]
        if ids:
            try:
                from sqlalchemy import text as sa_text
                placeholders = ", ".join([f":id_{i}" for i in range(len(ids))])
                params = {f"id_{i}": id_val for i, id_val in enumerate(ids)}
                result = db.execute(
                    sa_text(
                        f"SELECT COALESCE(SUM(review_count), 0)"
                        f" FROM restaurant_poi WHERE id IN ({placeholders})"
                    ),
                    params,
                ).scalar()
                total_reviews = int(result) if result else 0
            except Exception:
                pass

    review_sufficiency = min(1.0, math.log1p(total_reviews) / math.log1p(200))

    # 4. nearby_evidence: soft floor for places that have actual nearby market data
    # even when Google enrichment is still sparse.
    nearby_evidence = min(1.0, len(all_nearby) / 8.0) if all_nearby else 0.0

    # 5. source_diversity: reward multiple data sources/platforms in the local ring.
    distinct_sources = len(
        {
            str(r.get("source") or "").strip().lower()
            for r in all_nearby
            if r.get("source")
        }
    )
    source_diversity = min(1.0, distinct_sources / 4.0)

    # 6. rating_coverage: if nearby places at least carry ratings, we should not
    # force confidence to zero just because Google IDs are missing.
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

    return _RentResolution(
        rent_per_m2=None,
        scope="none",
        sample_count=0,
        median_used=None,
        method="none",
    )
