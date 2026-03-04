"""
Heatmap generator for restaurant location scoring.

Generates a grid of H3 hexagonal cells over a bounding box,
computes scores for each cell, and returns GeoJSON features.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.models.tables import LocationScore
from app.services.restaurant_location import score_location

logger = logging.getLogger(__name__)

# Max cells per heatmap request to avoid overload
MAX_CELLS = 500


def _h3_polygon_coords(h3_index: str) -> list[list[float]]:
    """Get GeoJSON polygon coordinates for an H3 cell."""
    import h3

    boundary = h3.cell_to_boundary(h3_index)
    # h3 returns (lat, lon) tuples; GeoJSON wants [lon, lat]
    ring = [[lon, lat] for lat, lon in boundary]
    ring.append(ring[0])  # close the ring
    return [ring]


def generate_heatmap(
    db: Session,
    category: str,
    bbox: tuple[float, float, float, float],
    resolution: int = 8,
    use_cache: bool = True,
) -> dict[str, Any]:
    """
    Generate a GeoJSON FeatureCollection of H3 hex cells with scores.

    Args:
        db: database session
        category: restaurant category (e.g. 'burger')
        bbox: (min_lon, min_lat, max_lon, max_lat)
        resolution: H3 resolution (7=~1.2km, 8=~460m, 9=~175m)
        use_cache: if True, use cached scores from location_score table

    Returns:
        GeoJSON FeatureCollection
    """
    try:
        import h3
    except ImportError:
        logger.error("h3 is not installed")
        return {"type": "FeatureCollection", "features": []}

    min_lon, min_lat, max_lon, max_lat = bbox

    # Generate H3 cells covering the bbox
    # Use h3.geo_to_cells with a polygon covering the bbox
    bbox_polygon = {
        "type": "Polygon",
        "coordinates": [[
            [min_lon, min_lat],
            [max_lon, min_lat],
            [max_lon, max_lat],
            [min_lon, max_lat],
            [min_lon, min_lat],
        ]],
    }

    try:
        cells = h3.geo_to_cells(bbox_polygon, resolution)
    except Exception:
        # Fallback: manually generate cells from corners
        cells = set()
        lat_step = (max_lat - min_lat) / 20
        lon_step = (max_lon - min_lon) / 20
        for i in range(21):
            for j in range(21):
                lat = min_lat + i * lat_step
                lon = min_lon + j * lon_step
                cells.add(h3.latlng_to_cell(lat, lon, resolution))

    cells = list(cells)[:MAX_CELLS]
    logger.info(
        "Generating heatmap for category=%s, %d H3 cells (res=%d)",
        category, len(cells), resolution,
    )

    features = []
    for h3_idx in cells:
        # Check cache first
        if use_cache:
            cached = (
                db.query(LocationScore)
                .filter_by(h3_index=h3_idx, category=category)
                .first()
            )
            if cached and cached.overall_score is not None:
                lat, lon = h3.cell_to_latlng(h3_idx)
                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": _h3_polygon_coords(h3_idx),
                    },
                    "properties": {
                        "h3_index": h3_idx,
                        "score": float(cached.overall_score),
                        "demand_score": float(cached.demand_score) if cached.demand_score else None,
                        "cost_penalty": float(cached.cost_penalty) if cached.cost_penalty else None,
                        "factors": cached.factors or {},
                        "category": category,
                        "lat": lat,
                        "lon": lon,
                    },
                })
                continue

        # Compute score
        lat, lon = h3.cell_to_latlng(h3_idx)
        result = score_location(db, lat, lon, category)

        # Cache the result
        existing = (
            db.query(LocationScore)
            .filter_by(h3_index=h3_idx, category=category)
            .first()
        )
        if existing:
            existing.overall_score = result.opportunity_score
            existing.demand_score = result.demand_score
            existing.cost_penalty = result.cost_penalty
            existing.factors = result.factors
            existing.model_version = result.model_version
            existing.computed_at = datetime.now(timezone.utc)
        else:
            db.add(
                LocationScore(
                    h3_index=h3_idx,
                    category=category,
                    overall_score=result.opportunity_score,
                    demand_score=result.demand_score,
                    cost_penalty=result.cost_penalty,
                    factors=result.factors,
                    model_version=result.model_version,
                    computed_at=datetime.now(timezone.utc),
                )
            )

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": _h3_polygon_coords(h3_idx),
            },
            "properties": {
                "h3_index": h3_idx,
                "score": result.opportunity_score,
                "demand_score": result.demand_score,
                "cost_penalty": result.cost_penalty,
                "factors": result.factors,
                "category": category,
                "lat": lat,
                "lon": lon,
            },
        })

    db.commit()

    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "category": category,
            "bbox": list(bbox),
            "resolution": resolution,
            "cell_count": len(features),
        },
    }
