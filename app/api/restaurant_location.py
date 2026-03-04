"""
API endpoints for the Restaurant Location Finder feature.

Provides scoring, heatmap generation, category listing,
competitor lookup, and parcel scoring.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.services.restaurant_categories import list_categories
from app.services.restaurant_heatmap import generate_heatmap
from app.services.restaurant_location import score_location

router = APIRouter(tags=["restaurant-location"])
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------


class ScoreRequest(BaseModel):
    lat: float = Field(..., ge=20, le=30, description="Latitude (Riyadh range)")
    lon: float = Field(..., ge=44, le=50, description="Longitude (Riyadh range)")
    category: str = Field(..., description="Restaurant category key")
    radius_m: float = Field(1000, ge=100, le=5000, description="Search radius in meters")


class ScoreResponse(BaseModel):
    score: float = Field(..., description="Overall profitability score (0-100)")
    factors: dict[str, float] = Field(default_factory=dict, description="Individual factor scores")
    confidence: float = Field(..., description="Score confidence (0-1)")
    nearby_competitors: list[dict[str, Any]] = Field(default_factory=list)
    model_version: str = "weighted_v1"


class ParcelScoreRequest(BaseModel):
    parcel_id: Optional[str] = None
    geometry: Optional[dict] = None
    category: str


class CategoryResponse(BaseModel):
    key: str
    name_en: str
    name_ar: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/restaurant/score", response_model=ScoreResponse)
def score_restaurant_location(req: ScoreRequest, db: Session = Depends(get_db)):
    """
    Compute a profitability score for a restaurant category at a given location.
    Returns the overall score, individual factor breakdown, and nearby competitors.
    """
    result = score_location(
        db=db,
        lat=req.lat,
        lon=req.lon,
        category=req.category,
        radius_m=req.radius_m,
    )
    return ScoreResponse(
        score=result.overall_score,
        factors=result.factors,
        confidence=result.confidence,
        nearby_competitors=result.nearby_competitors,
        model_version=result.model_version,
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
    bbox = (min_lon, min_lat, max_lon, max_lat)

    # Validate bbox size to avoid overloading
    lon_span = max_lon - min_lon
    lat_span = max_lat - min_lat
    if lon_span > 1.0 or lat_span > 1.0:
        raise HTTPException(
            status_code=400,
            detail="Bounding box too large. Max 1 degree span in each direction.",
        )

    return generate_heatmap(db, category, bbox, resolution)


@router.get("/restaurant/categories", response_model=list[CategoryResponse])
def get_restaurant_categories():
    """Return all supported restaurant categories with display names."""
    return list_categories()


@router.get("/restaurant/competitors")
def get_restaurant_competitors(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    category: str = Query(..., description="Restaurant category key"),
    radius_m: float = Query(1000, ge=100, le=5000, description="Search radius in meters"),
    db: Session = Depends(get_db),
):
    """
    List nearby restaurants of the same category, sorted by distance.
    """
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
        # Extract centroid from GeoJSON geometry
        try:
            from shapely.geometry import shape

            geom = shape(req.geometry)
            centroid = geom.centroid
            lat, lon = centroid.y, centroid.x
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid geometry: {exc}")

    elif req.parcel_id:
        # Look up parcel centroid from parcel table
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
        raise HTTPException(
            status_code=400,
            detail="Provide either parcel_id or geometry",
        )

    if lat is None or lon is None:
        raise HTTPException(status_code=400, detail="Cannot determine location")

    result = score_location(db, lat, lon, req.category)
    return {
        "parcel_id": req.parcel_id,
        "lat": lat,
        "lon": lon,
        "category": req.category,
        "score": result.overall_score,
        "factors": result.factors,
        "confidence": result.confidence,
        "nearby_competitors": result.nearby_competitors,
        "model_version": result.model_version,
    }
