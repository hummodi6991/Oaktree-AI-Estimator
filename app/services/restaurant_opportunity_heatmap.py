"""
City-wide Underserved Demand Heatmap generator.

Uses all Riyadh population_density H3 cells as the grid, scores each cell
for a given restaurant category using batched DB queries and spatial
indexing, then caches the result in ``restaurant_heatmap_cache`` for 7 days.

When a dedicated heatmap AI model is available (``restaurant_heatmap_v1.pkl``),
the generator uses AI-predicted cell-level scores.  Otherwise it falls back
to the original curated static weights.
"""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models.tables import PopulationDensity, RestaurantHeatmapCache
from app.services.restaurant_location import (
    DEMAND_WEIGHTS,
    COST_WEIGHTS,
    PLATFORM_SOURCES,
    competition_score,
    complementary_score,
    delivery_demand_score,
    competitor_rating_score,
    _haversine,
    _weighted_avg,
)
from app.services.restaurant_heatmap_ai import (
    try_load_model as _try_load_heatmap_model,
    predict_cell_scores as _predict_cell_scores,
    get_model_status as _get_heatmap_model_status,
)
from app.ml.restaurant_heatmap_train import build_cell_features

logger = logging.getLogger(__name__)

# Cache TTL — 7 days
_CACHE_TTL = timedelta(days=7)

# Riyadh bounding box (expanded by ~15 km for edge-radius queries)
_RIYADH_BBOX = {
    "min_lat": 24.20,
    "max_lat": 25.10,
    "min_lon": 46.20,
    "max_lon": 47.30,
}


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def _get_cached(
    db: Session, category: str, radius_m: int
) -> dict[str, Any] | None:
    """Return cached payload if fresh enough and AI-consistent, else None.

    Invalidates the cache automatically when:
    - The cached payload used static fallback but the heatmap AI model is now
      available (stale static masking new AI).
    - The cached payload's model_version differs from the currently loaded
      model version (outdated AI version).
    """
    row = (
        db.query(RestaurantHeatmapCache)
        .filter_by(category=category, radius_m=radius_m)
        .first()
    )
    if row is None:
        return None
    if datetime.now(timezone.utc) - row.computed_at > _CACHE_TTL:
        return None

    payload = row.payload
    if not isinstance(payload, dict):
        return payload

    # --- AI-consistency check ---
    cached_meta = payload.get("metadata") or {}
    cached_scoring_mode = cached_meta.get("scoring_mode", "")
    cached_model_version = cached_meta.get("model_version", "")
    cached_ai_used = cached_meta.get("ai_used", False)

    current_status = _get_heatmap_model_status()
    model_available_now = current_status.get("available", False)
    current_model_version = current_status.get("model_version") or ""

    # Case 1: cached payload is static fallback, but AI model is now available
    if not cached_ai_used and model_available_now:
        logger.info(
            "Heatmap cache BYPASS: cached payload is static fallback "
            "(scoring_mode=%s) but heatmap AI model is now available "
            "(version=%s). Recomputing with AI. category=%s",
            cached_scoring_mode,
            current_model_version,
            category,
        )
        return None

    # Case 2: cached payload used AI but model version has changed
    if (
        cached_ai_used
        and model_available_now
        and current_model_version
        and cached_model_version != current_model_version
    ):
        logger.info(
            "Heatmap cache BYPASS: cached model_version=%s differs from "
            "current model_version=%s. Recomputing. category=%s",
            cached_model_version,
            current_model_version,
            category,
        )
        return None

    return payload


def _set_cache(
    db: Session, category: str, radius_m: int, payload: dict[str, Any]
) -> None:
    """Upsert heatmap cache row."""
    existing = (
        db.query(RestaurantHeatmapCache)
        .filter_by(category=category, radius_m=radius_m)
        .first()
    )
    now = datetime.now(timezone.utc)
    if existing:
        existing.payload = payload
        existing.computed_at = now
    else:
        db.add(
            RestaurantHeatmapCache(
                category=category,
                radius_m=radius_m,
                payload=payload,
                computed_at=now,
            )
        )
    db.commit()


# ---------------------------------------------------------------------------
# Batch POI loading
# ---------------------------------------------------------------------------


def _load_pois_in_bbox(
    db: Session, category: str, radius_m: int
) -> list[dict]:
    """
    Load all restaurant POIs within Riyadh bbox (expanded by radius_m)
    in a single query.  Returns list of dicts with id, lat, lon,
    category, rating, review_count, source, chain_name, google_confidence.
    """
    expand_deg = radius_m / 111_000 + 0.02  # extra margin
    min_lat = _RIYADH_BBOX["min_lat"] - expand_deg
    max_lat = _RIYADH_BBOX["max_lat"] + expand_deg
    min_lon = _RIYADH_BBOX["min_lon"] - expand_deg
    max_lon = _RIYADH_BBOX["max_lon"] + expand_deg

    rows = db.execute(
        text("""
            SELECT id, lat, lon, category, rating, review_count,
                   source, chain_name, google_place_id, google_confidence,
                   price_level
            FROM restaurant_poi
            WHERE lat BETWEEN :min_lat AND :max_lat
              AND lon BETWEEN :min_lon AND :max_lon
        """),
        {
            "min_lat": min_lat,
            "max_lat": max_lat,
            "min_lon": min_lon,
            "max_lon": max_lon,
        },
    ).mappings().all()

    return [
        {
            "id": r["id"],
            "lat": float(r["lat"]),
            "lon": float(r["lon"]),
            "category": r["category"],
            "rating": float(r["rating"]) if r["rating"] else None,
            "review_count": int(r["review_count"]) if r["review_count"] else 0,
            "source": r["source"],
            "chain_name": r["chain_name"],
            "google_place_id": r["google_place_id"],
            "google_confidence": (
                float(r["google_confidence"]) if r["google_confidence"] else None
            ),
            "price_level": (
                float(r["price_level"]) if r.get("price_level") else None
            ),
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Spatial index: simple lat/lon grid binning
# ---------------------------------------------------------------------------


class _GridIndex:
    """
    Simple grid-binning spatial index.  Bins POIs into ~1 km cells
    so that neighbor queries are O(1) average instead of O(N).
    """

    def __init__(self, pois: list[dict], cell_deg: float = 0.01):
        self._cell_deg = cell_deg
        self._grid: dict[tuple[int, int], list[dict]] = {}
        for poi in pois:
            key = self._key(poi["lat"], poi["lon"])
            self._grid.setdefault(key, []).append(poi)

    def _key(self, lat: float, lon: float) -> tuple[int, int]:
        return (int(lat / self._cell_deg), int(lon / self._cell_deg))

    def neighbors(
        self, lat: float, lon: float, radius_m: float
    ) -> list[dict]:
        """Return POIs within radius_m of (lat, lon)."""
        # How many grid cells to search in each direction
        span = int(radius_m / (self._cell_deg * 111_000)) + 1
        ck = self._key(lat, lon)
        results = []
        for di in range(-span, span + 1):
            for dj in range(-span, span + 1):
                for poi in self._grid.get((ck[0] + di, ck[1] + dj), []):
                    dist = _haversine(lat, lon, poi["lat"], poi["lon"])
                    if dist <= radius_m:
                        results.append({**poi, "distance_m": dist})
        return results


# ---------------------------------------------------------------------------
# Per-cell scoring (lightweight, no DB queries per cell)
# ---------------------------------------------------------------------------


def _score_cell(
    lat: float,
    lon: float,
    category: str,
    radius_m: float,
    nearby: list[dict],
    population: float,
    demand_w: dict[str, float],
) -> dict[str, Any]:
    """
    Score a single H3 cell using pre-fetched nearby POIs.
    Returns a dict with all score components.
    """
    same_cat = [p for p in nearby if p["category"] == category]
    diff_cat = [p for p in nearby if p["category"] != category]

    same_on_platforms = [r for r in same_cat if r.get("source") in PLATFORM_SOURCES]
    all_on_platforms = [
        r for r in nearby if r.get("source") in PLATFORM_SOURCES
    ]

    rated = [r for r in same_cat if r.get("rating") is not None]
    avg_rating = (
        sum(r["rating"] for r in rated) / len(rated) if rated else None
    )

    # Sum of review counts for same-category competitors
    demand_sum_reviews = sum(r.get("review_count", 0) for r in same_cat)

    competitor_count = len(same_cat)

    # Demand factors (subset that doesn't require per-cell DB queries)
    demand_factors = {
        "competition": competition_score(competitor_count),
        "complementary": complementary_score(len(diff_cat)),
        "population": _population_factor(population),
        "traffic": 50.0,  # neutral — no per-cell OSM query
        "road_frontage": 50.0,
        "commercial_density": 50.0,
        "delivery_demand": delivery_demand_score(
            len(same_on_platforms), len(all_on_platforms)
        ),
        "competitor_rating": competitor_rating_score(avg_rating, len(rated)),
        "anchor_proximity": 50.0,
        "foot_traffic": _foot_traffic_proxy(
            len(nearby), competitor_count, population
        ),
        "chain_gap": _chain_gap_simple(competitor_count),
        "income_proxy": 50.0,
    }

    demand = _weighted_avg(demand_factors, demand_w)

    # Cost factors — neutral defaults (no per-cell queries)
    cost = 50.0

    opportunity = 0.80 * demand + 0.20 * cost

    # Confidence (simplified batch version)
    google_count = sum(
        1 for r in nearby if r.get("google_place_id")
    )
    has_google = google_count / max(1, len(nearby)) if nearby else 0.0
    avg_gconf = 0.0
    gconf_vals = [
        r["google_confidence"]
        for r in nearby
        if r.get("google_confidence") is not None
    ]
    if gconf_vals:
        avg_gconf = sum(gconf_vals) / len(gconf_vals)
    review_suf = min(
        1.0, math.log1p(demand_sum_reviews) / math.log1p(200)
    )

    confidence_01 = 0.35 * has_google + 0.35 * avg_gconf + 0.30 * review_suf
    confidence_01 = max(0.0, min(1.0, confidence_01))
    confidence_score = round(confidence_01 * 100, 1)

    final_score = round(
        opportunity * (0.60 + 0.40 * confidence_01), 1
    )

    # Underserved index
    underserved_index = round(
        (demand_sum_reviews / max(1, competitor_count))
        * math.log1p(population),
        2,
    )

    return {
        "opportunity_score": round(opportunity, 1),
        "confidence_score": confidence_score,
        "final_score": final_score,
        "demand_sum_reviews": demand_sum_reviews,
        "competitor_count": competitor_count,
        "population": round(population, 1),
        "underserved_index": underserved_index,
        "debug_factors": {k: round(v, 1) for k, v in demand_factors.items()},
    }


def _population_factor(pop: float) -> float:
    if pop <= 0:
        return 10.0
    return min(95.0, 10.0 + 85.0 * (1 - math.exp(-pop / 10000)))


def _foot_traffic_proxy(
    total_nearby: int, competitor_count: int, population: float
) -> float:
    density_factor = min(1.0, total_nearby / 30.0) * 40
    pop_factor = min(1.0, population / 5000) * 30
    comp_factor = min(1.0, competitor_count / 10.0) * 25
    return min(95.0, max(10.0, density_factor + pop_factor + comp_factor))


def _chain_gap_simple(competitor_count: int) -> float:
    if competitor_count == 0:
        return 90.0
    if competitor_count <= 3:
        return 80.0
    if competitor_count <= 8:
        return 60.0
    if competitor_count <= 15:
        return 40.0
    return 20.0


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def generate_opportunity_heatmap(
    db: Session,
    category: str,
    radius_m: int = 1200,
    min_confidence: float = 0.3,
    limit_cells: int = 5000,
    cache_bust: bool = False,
) -> dict[str, Any]:
    """
    Generate a city-wide opportunity heatmap for Riyadh.

    Returns a GeoJSON FeatureCollection of H3 cell centers with scores,
    plus metadata including top-30 underserved cells.
    """
    t_start = time.monotonic()

    # 1. Check cache
    if not cache_bust:
        cached = _get_cached(db, category, radius_m)
        if cached is not None:
            logger.info(
                "Opportunity heatmap cache HIT: category=%s radius_m=%d",
                category,
                radius_m,
            )
            return cached

    logger.info(
        "Generating opportunity heatmap: category=%s radius_m=%d",
        category,
        radius_m,
    )

    # 2. Load all population_density cells (Riyadh grid)
    t0 = time.monotonic()
    pop_rows = (
        db.query(PopulationDensity)
        .filter(PopulationDensity.lat.isnot(None))
        .filter(PopulationDensity.lon.isnot(None))
        .all()
    )
    cells = [
        {
            "h3": r.h3_index,
            "lat": float(r.lat),
            "lon": float(r.lon),
            "population": float(r.population or 0),
        }
        for r in pop_rows
    ]
    t_load_cells = time.monotonic() - t0
    logger.info("Loaded %d population cells in %.2fs", len(cells), t_load_cells)

    # 3. Load all POIs in Riyadh bbox (single batch query)
    t0 = time.monotonic()
    pois = _load_pois_in_bbox(db, category, radius_m)
    t_load_pois = time.monotonic() - t0
    logger.info("Loaded %d POIs in %.2fs", len(pois), t_load_pois)

    # 4. Build spatial index
    t0 = time.monotonic()
    idx = _GridIndex(pois)
    t_build_idx = time.monotonic() - t0
    logger.info("Built spatial index in %.2fs", t_build_idx)

    # 5. Try dedicated heatmap AI model; fall back to static weights.
    heatmap_model = _try_load_heatmap_model()
    ai_used = False
    model_version = "curated_static_v1"
    scoring_mode = "curated_static_v1"

    t0 = time.monotonic()
    features: list[dict[str, Any]] = []

    if heatmap_model is not None:
        # ── AI scoring path ──
        # Build feature DataFrame for all cells in one pass, then batch-predict.
        ai_status = _get_heatmap_model_status()
        model_version = ai_status.get("model_version") or "heatmap_ai_v1"
        scoring_mode = "heatmap_ai_v1"
        ps_frozen = frozenset(PLATFORM_SOURCES)

        cell_records: list[dict[str, Any]] = []
        cell_meta: list[dict[str, Any]] = []  # parallel list for geometry + extras

        for cell in cells[:limit_cells]:
            nearby = idx.neighbors(cell["lat"], cell["lon"], radius_m)
            feats = build_cell_features(
                cell["lat"],
                cell["lon"],
                category,
                nearby,
                cell["population"],
                ps_frozen,
            )
            # Keep non-feature data for the GeoJSON output
            same_cat = [p for p in nearby if p.get("category") == category]
            demand_sum_reviews = sum(p.get("review_count", 0) for p in same_cat)
            competitor_count = len(same_cat)

            # Confidence (same simplified formula as static path)
            google_count = sum(1 for r in nearby if r.get("google_place_id"))
            has_google = google_count / max(1, len(nearby)) if nearby else 0.0
            gconf_vals = [
                r["google_confidence"]
                for r in nearby
                if r.get("google_confidence") is not None
            ]
            avg_gconf = sum(gconf_vals) / len(gconf_vals) if gconf_vals else 0.0
            review_suf = min(
                1.0, math.log1p(demand_sum_reviews) / math.log1p(200)
            )
            confidence_01 = max(0.0, min(1.0,
                0.35 * has_google + 0.35 * avg_gconf + 0.30 * review_suf
            ))
            confidence_score = round(confidence_01 * 100, 1)

            underserved_index = round(
                (demand_sum_reviews / max(1, competitor_count))
                * math.log1p(cell["population"]),
                2,
            )

            cell_records.append(feats)
            cell_meta.append({
                "h3": cell["h3"],
                "lat": cell["lat"],
                "lon": cell["lon"],
                "population": round(cell["population"], 1),
                "demand_sum_reviews": demand_sum_reviews,
                "competitor_count": competitor_count,
                "confidence_score": confidence_score,
                "confidence_01": confidence_01,
                "underserved_index": underserved_index,
            })

        if cell_records:
            feat_df = pd.DataFrame(cell_records)
            # One-hot encode category to match training columns
            feat_df = pd.get_dummies(feat_df, columns=["category"], prefix="cat")
            # Drop non-numeric helper columns that may have leaked in
            feat_df = feat_df.drop(
                columns=[c for c in ("h3",) if c in feat_df.columns],
            )

            ai_scores = _predict_cell_scores(feat_df)
            if ai_scores is not None:
                ai_used = True
                for i, (meta, opp_score) in enumerate(zip(cell_meta, ai_scores)):
                    opp = round(float(opp_score), 1)
                    conf_01 = meta["confidence_01"]
                    final = round(opp * (0.60 + 0.40 * conf_01), 1)

                    if meta["confidence_score"] < min_confidence * 100:
                        continue

                    features.append({
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [meta["lon"], meta["lat"]],
                        },
                        "properties": {
                            "h3": meta["h3"],
                            "opportunity_score": opp,
                            "confidence_score": meta["confidence_score"],
                            "final_score": final,
                            "demand_sum_reviews": meta["demand_sum_reviews"],
                            "competitor_count": meta["competitor_count"],
                            "population": meta["population"],
                            "underserved_index": meta["underserved_index"],
                            "ai_used": True,
                            "model_version": model_version,
                            "scoring_mode": scoring_mode,
                        },
                    })
            else:
                # AI prediction failed at runtime — fall through to static
                ai_used = False
                model_version = "curated_static_v1"
                scoring_mode = "curated_static_v1"

    if not ai_used:
        # ── Static (curated) scoring path — original logic ──
        # The parcel AI's feature importances are intentionally NOT used here;
        # they map to a mix of demand + confidence features and would distort
        # the heatmap.  This path uses the curated static DEMAND_WEIGHTS.
        demand_w = DEMAND_WEIGHTS

        for cell in cells[:limit_cells]:
            nearby = idx.neighbors(cell["lat"], cell["lon"], radius_m)
            scores = _score_cell(
                cell["lat"],
                cell["lon"],
                category,
                radius_m,
                nearby,
                cell["population"],
                demand_w,
            )

            if scores["confidence_score"] < min_confidence * 100:
                continue

            features.append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [cell["lon"], cell["lat"]],
                    },
                    "properties": {
                        "h3": cell["h3"],
                        "opportunity_score": scores["opportunity_score"],
                        "confidence_score": scores["confidence_score"],
                        "final_score": scores["final_score"],
                        "demand_sum_reviews": scores["demand_sum_reviews"],
                        "competitor_count": scores["competitor_count"],
                        "population": scores["population"],
                        "underserved_index": scores["underserved_index"],
                        "debug_factors": scores["debug_factors"],
                        "ai_used": False,
                        "model_version": model_version,
                        "scoring_mode": scoring_mode,
                    },
                }
            )

    t_score = time.monotonic() - t0
    logger.info(
        "Scored %d cells in %.2fs (scoring_mode=%s)",
        len(features), t_score, scoring_mode,
    )

    # 7. Top 30 underserved cells
    top_cells = sorted(
        features,
        key=lambda f: f["properties"]["underserved_index"],
        reverse=True,
    )[:30]
    top_cells_summary = [
        {
            "h3": f["properties"]["h3"],
            "lat": f["geometry"]["coordinates"][1],
            "lon": f["geometry"]["coordinates"][0],
            "underserved_index": f["properties"]["underserved_index"],
            "opportunity_score": f["properties"]["opportunity_score"],
            "confidence_score": f["properties"]["confidence_score"],
            "final_score": f["properties"]["final_score"],
            "demand_score": f["properties"].get("demand_score"),
            "cost_penalty": f["properties"].get("cost_penalty"),
            "competitor_count": f["properties"]["competitor_count"],
            "population": f["properties"]["population"],
        }
        for f in top_cells
    ]

    t_total = time.monotonic() - t_start

    payload: dict[str, Any] = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "category": category,
            "radius_m": radius_m,
            "cell_count": len(features),
            "total_population_cells": len(cells),
            "total_pois_loaded": len(pois),
            "ai_used": ai_used,
            "ai_model_available": heatmap_model is not None,
            "model_version": model_version,
            "scoring_mode": scoring_mode,
            "top_underserved": top_cells_summary,
            "timings": {
                "load_cells_s": round(t_load_cells, 2),
                "load_pois_s": round(t_load_pois, 2),
                "build_index_s": round(t_build_idx, 2),
                "score_cells_s": round(t_score, 2),
                "total_s": round(t_total, 2),
            },
        },
    }

    # 8. Store in cache
    _set_cache(db, category, radius_m, payload)
    logger.info(
        "Opportunity heatmap complete: %d features, %.1fs total",
        len(features),
        t_total,
    )

    return payload
