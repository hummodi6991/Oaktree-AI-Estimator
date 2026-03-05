"""
API endpoints for the Restaurant Location Finder feature.

Provides scoring, heatmap generation, category listing,
competitor lookup, parcel scoring, top-parcel recommendation,
AI weight introspection, and data-source registry.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.services.restaurant_categories import list_categories
from app.services.restaurant_heatmap import generate_heatmap
from app.services.restaurant_location import (
    COST_WEIGHTS,
    DEMAND_WEIGHTS,
    get_ai_weights,
    score_location,
)

router = APIRouter(tags=["restaurant-location"])
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lightweight in-memory TTL cache (no external deps)
# ---------------------------------------------------------------------------

_cache: dict[str, tuple[float, Any]] = {}
_CATEGORY_TTL = 3600  # 1 hour
_HEATMAP_TTL = 300  # 5 minutes


def _cache_get(key: str) -> Any | None:
    entry = _cache.get(key)
    if entry is None:
        return None
    expires, value = entry
    if time.monotonic() > expires:
        _cache.pop(key, None)
        return None
    return value


def _cache_set(key: str, value: Any, ttl: float) -> None:
    _cache[key] = (time.monotonic() + ttl, value)


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------


class ScoreRequest(BaseModel):
    lat: float = Field(..., ge=20, le=30, description="Latitude (Riyadh range)")
    lon: float = Field(..., ge=44, le=50, description="Longitude (Riyadh range)")
    category: str = Field(..., description="Restaurant category key")
    radius_m: float = Field(1000, ge=100, le=5000, description="Search radius in meters")
    chain_name: Optional[str] = Field(None, description="Specific chain name for gap analysis")
    use_ai_weights: bool = Field(True, description="Use AI-predicted factor weights when available")


class ScoreResponse(BaseModel):
    opportunity_score: float = Field(..., description="Market opportunity score (0-100) — demand vs competition vs cost")
    confidence_score: float = Field(0.0, description="Data-reliability score (0-100) — Google match, confidence, review volume")
    final_score: float = Field(0.0, description="Ranking score (0-100) = opportunity dampened by confidence")
    demand_score: float = Field(..., description="Demand-potential sub-score (0-100)")
    cost_penalty: float = Field(..., description="Cost sub-score — higher = cheaper = better (0-100)")
    factors: dict[str, float] = Field(default_factory=dict, description="Individual factor scores")
    contributions: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Market feature contributions sorted by weighted impact",
    )
    contributions_confidence: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Confidence/reliability feature contributions",
    )
    confidence: float = Field(..., description="Legacy score confidence (0-1)")
    nearby_competitors: list[dict[str, Any]] = Field(default_factory=list)
    model_version: str = "weighted_v3"
    ai_weights_used: bool = False


class ParcelScoreRequest(BaseModel):
    parcel_id: Optional[str] = None
    geometry: Optional[dict] = None
    category: str
    chain_name: Optional[str] = None


class CategoryResponse(BaseModel):
    key: str
    name_en: str
    name_ar: str


class TopParcelsRequest(BaseModel):
    category: str = Field(..., description="Restaurant category key")
    chain_name: Optional[str] = Field(None, description="Specific chain to check gap for")
    limit: int = Field(20, ge=1, le=100, description="Number of top parcels to return")
    min_lat: float = Field(24.5, description="Bounding box min latitude")
    max_lat: float = Field(24.95, description="Bounding box max latitude")
    min_lon: float = Field(46.5, description="Bounding box min longitude")
    max_lon: float = Field(46.95, description="Bounding box max longitude")
    resolution: int = Field(8, ge=7, le=9, description="H3 resolution for grid")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/restaurant/score", response_model=ScoreResponse)
def score_restaurant_location(req: ScoreRequest, db: Session = Depends(get_db)):
    """
    Compute an opportunity score for a restaurant category at a given location.

    Returns demand_score, cost_penalty, and composite opportunity_score,
    plus per-factor breakdown with weighted contributions and nearby competitors.

    When ``use_ai_weights`` is true (default), the trained ML model's feature
    importances are used to dynamically weight demand factors.
    """
    result = score_location(
        db=db,
        lat=req.lat,
        lon=req.lon,
        category=req.category,
        radius_m=req.radius_m,
        chain_name=req.chain_name,
        use_ai_weights=req.use_ai_weights,
    )
    return ScoreResponse(
        opportunity_score=result.opportunity_score,
        confidence_score=result.confidence_score,
        final_score=result.final_score,
        demand_score=result.demand_score,
        cost_penalty=result.cost_penalty,
        factors=result.factors,
        contributions=result.contributions,
        contributions_confidence=result.contributions_confidence,
        confidence=result.confidence,
        nearby_competitors=result.nearby_competitors,
        model_version=result.model_version,
        ai_weights_used=result.ai_weights_used,
    )


@router.get("/restaurant/heatmap")
def get_restaurant_heatmap(
    category: str = Query(..., description="Restaurant category key"),
    min_lon: float = Query(46.5, description="Bounding box min longitude"),
    min_lat: float = Query(24.5, description="Bounding box min latitude"),
    max_lon: float = Query(47.0, description="Bounding box max longitude"),
    max_lat: float = Query(25.0, description="Bounding box max latitude"),
    resolution: int = Query(8, ge=6, le=10, description="H3 resolution"),
    db: Session = Depends(get_db),
):
    """
    Generate a GeoJSON FeatureCollection heatmap of location scores
    for H3 hexagonal cells covering the bounding box.
    """
    lon_span = max_lon - min_lon
    lat_span = max_lat - min_lat
    if lon_span > 1.0 or lat_span > 1.0:
        raise HTTPException(
            status_code=400,
            detail="Bounding box too large. Max 1 degree span in each direction.",
        )

    # Short-lived cache keyed on (category, bbox, resolution)
    cache_key = f"heatmap:{category}:{min_lon:.3f},{min_lat:.3f},{max_lon:.3f},{max_lat:.3f}:{resolution}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    bbox = (min_lon, min_lat, max_lon, max_lat)
    result = generate_heatmap(db, category, bbox, resolution)
    _cache_set(cache_key, result, _HEATMAP_TTL)
    return result


@router.get("/restaurant/categories", response_model=list[CategoryResponse])
def get_restaurant_categories():
    """Return all supported restaurant categories with display names."""
    cached = _cache_get("categories")
    if cached is not None:
        return cached
    cats = list_categories()
    _cache_set("categories", cats, _CATEGORY_TTL)
    return cats


@router.get("/restaurant/competitors")
def get_restaurant_competitors(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    category: str = Query(..., description="Restaurant category key"),
    radius_m: float = Query(1000, ge=100, le=5000, description="Search radius in meters"),
    db: Session = Depends(get_db),
):
    """List nearby restaurants of the same category, sorted by distance."""
    result = score_location(db, lat, lon, category, radius_m)
    return {
        "category": category,
        "radius_m": radius_m,
        "count": len(result.nearby_competitors),
        "competitors": result.nearby_competitors,
    }


@router.post("/restaurant/score-parcel")
def score_restaurant_parcel(req: ParcelScoreRequest, db: Session = Depends(get_db)):
    """
    Score a specific parcel for a restaurant category.
    Accepts either a parcel_id (looks up centroid) or a GeoJSON geometry.
    """
    lat, lon = None, None

    if req.geometry:
        try:
            from shapely.geometry import shape
            geom = shape(req.geometry)
            centroid = geom.centroid
            lat, lon = centroid.y, centroid.x
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid geometry: {exc}")

    elif req.parcel_id:
        from app.models.tables import Parcel
        parcel = db.query(Parcel).filter_by(id=req.parcel_id).first()
        if not parcel:
            raise HTTPException(status_code=404, detail=f"Parcel {req.parcel_id} not found")
        if parcel.gis_polygon:
            try:
                from shapely.geometry import shape
                geom = shape(parcel.gis_polygon)
                centroid = geom.centroid
                lat, lon = centroid.y, centroid.x
            except Exception:
                raise HTTPException(status_code=400, detail="Cannot compute parcel centroid")
    else:
        raise HTTPException(status_code=400, detail="Provide either parcel_id or geometry")

    if lat is None or lon is None:
        raise HTTPException(status_code=400, detail="Cannot determine location")

    result = score_location(db, lat, lon, req.category, chain_name=req.chain_name)
    return {
        "parcel_id": req.parcel_id,
        "lat": lat,
        "lon": lon,
        "category": req.category,
        "opportunity_score": result.opportunity_score,
        "confidence_score": result.confidence_score,
        "final_score": result.final_score,
        "demand_score": result.demand_score,
        "cost_penalty": result.cost_penalty,
        "factors": result.factors,
        "contributions": result.contributions,
        "contributions_confidence": result.contributions_confidence,
        "confidence": result.confidence,
        "nearby_competitors": result.nearby_competitors,
        "model_version": result.model_version,
        "ai_weights_used": result.ai_weights_used,
    }


# ---------------------------------------------------------------------------
# NEW: Top-parcels recommendation
# ---------------------------------------------------------------------------


@router.post("/restaurant/top-parcels")
def find_top_parcels(req: TopParcelsRequest, db: Session = Depends(get_db)):
    """
    Find the best H3 cells (candidate locations) for a restaurant category
    within a bounding box. Returns top-N cells sorted by opportunity score.

    This endpoint evaluates a grid of H3 hexagonal cells and returns
    the highest-scoring locations — ideal for identifying where to open
    a new restaurant branch.
    """
    try:
        import h3
    except ImportError:
        raise HTTPException(status_code=500, detail="h3 library not installed")

    lon_span = req.max_lon - req.min_lon
    lat_span = req.max_lat - req.min_lat
    if lon_span > 0.6 or lat_span > 0.6:
        raise HTTPException(
            status_code=400,
            detail="Bounding box too large. Max 0.6 degree span for top-parcels.",
        )

    bbox_polygon = {
        "type": "Polygon",
        "coordinates": [[
            [req.min_lon, req.min_lat],
            [req.max_lon, req.min_lat],
            [req.max_lon, req.max_lat],
            [req.min_lon, req.max_lat],
            [req.min_lon, req.min_lat],
        ]],
    }

    try:
        cells = list(h3.geo_to_cells(bbox_polygon, req.resolution))
    except Exception:
        cells = []
        lat_step = (req.max_lat - req.min_lat) / 15
        lon_step = (req.max_lon - req.min_lon) / 15
        for i in range(16):
            for j in range(16):
                lat = req.min_lat + i * lat_step
                lon = req.min_lon + j * lon_step
                cells.append(h3.latlng_to_cell(lat, lon, req.resolution))
        cells = list(set(cells))

    # Cap at 300 cells to keep response time reasonable
    cells = cells[:300]

    scored: list[dict[str, Any]] = []
    for h3_idx in cells:
        lat, lon = h3.cell_to_latlng(h3_idx)
        result = score_location(
            db, lat, lon, req.category,
            chain_name=req.chain_name,
        )
        scored.append({
            "h3_index": h3_idx,
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "opportunity_score": result.opportunity_score,
            "confidence_score": result.confidence_score,
            "final_score": result.final_score,
            "demand_score": result.demand_score,
            "cost_penalty": result.cost_penalty,
            "confidence": result.confidence,
            "top_factors": [
                c for c in result.contributions[:3]
            ],
            "competitor_count": len(result.nearby_competitors),
            "model_version": result.model_version,
        })

    # Sort by final_score (opportunity dampened by confidence) descending
    scored.sort(key=lambda x: x["final_score"], reverse=True)
    top = scored[:req.limit]

    return {
        "category": req.category,
        "chain_name": req.chain_name,
        "total_cells_evaluated": len(cells),
        "resolution": req.resolution,
        "parcels": top,
    }


# ---------------------------------------------------------------------------
# NEW: AI weights introspection
# ---------------------------------------------------------------------------


@router.get("/restaurant/ai-weights")
def get_ai_weight_info():
    """
    Return the current factor weights used by the scoring engine.
    If an AI model is available, returns both AI-predicted and static defaults.
    """
    ai_w = get_ai_weights()
    return {
        "ai_model_available": ai_w is not None,
        "static_demand_weights": DEMAND_WEIGHTS,
        "static_cost_weights": COST_WEIGHTS,
        "ai_demand_weights": ai_w,
        "description": (
            "When AI weights are available, the scoring engine uses "
            "feature importances from the trained GradientBoosting model "
            "to dynamically weight demand factors. This adapts to real "
            "market data patterns rather than relying on static assumptions."
        ),
    }


# ---------------------------------------------------------------------------
# NEW: Data sources registry
# ---------------------------------------------------------------------------


@router.get("/restaurant/data-sources")
def get_data_sources(db: Session = Depends(get_db)):
    """
    Return metadata about all data sources used by the restaurant
    location finder, including scraper coverage and data freshness.
    """
    from app.connectors.delivery_platforms import SCRAPER_REGISTRY
    from sqlalchemy import text as sa_text

    # Get per-source counts from the database
    source_counts: dict[str, int] = {}
    try:
        rows = db.execute(
            sa_text("SELECT source, COUNT(*) as cnt FROM restaurant_poi GROUP BY source")
        ).fetchall()
        for row in rows:
            source_counts[row[0]] = row[1]
    except Exception:
        pass

    platforms = []
    for source, meta in SCRAPER_REGISTRY.items():
        platforms.append({
            "source": source,
            "label": meta["label"],
            "url": meta["url"],
            "poi_count": source_counts.get(source, 0),
            "status": "active" if source_counts.get(source, 0) > 0 else "pending",
        })

    # Add non-platform sources
    for extra_source, label in [
        ("overture", "Overture Maps"),
        ("osm", "OpenStreetMap"),
    ]:
        platforms.append({
            "source": extra_source,
            "label": label,
            "url": "",
            "poi_count": source_counts.get(extra_source, 0),
            "status": "active" if source_counts.get(extra_source, 0) > 0 else "pending",
        })

    total_pois = sum(source_counts.values())

    # Google Reviews enrichment coverage
    google_enriched_count = 0
    google_fresh_count = 0
    try:
        google_enriched_count = db.execute(
            sa_text("SELECT count(*) FROM restaurant_poi WHERE google_place_id IS NOT NULL")
        ).scalar() or 0
        google_fresh_count = db.execute(
            sa_text(
                "SELECT count(*) FROM restaurant_poi"
                " WHERE google_fetched_at >= now() - interval '30 days'"
            )
        ).scalar() or 0
    except Exception:
        pass

    return {
        "total_pois": total_pois,
        "sources": platforms,
        "platform_count": len(SCRAPER_REGISTRY),
        "google_enriched_count": google_enriched_count,
        "google_fresh_count": google_fresh_count,
    }
