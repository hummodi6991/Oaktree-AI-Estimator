"""Utilities for querying ArcGIS FeatureServer layers."""

from typing import Any, Dict, List, Optional
import httpx
import json
from urllib.parse import urljoin


def _headers(token: Optional[str]) -> Dict[str, str]:
    """Build the authorization header payload."""
    return {"Authorization": f"Bearer {token}"} if token else {}


def query_features(
    base_url: str,
    layer_id: int,
    geometry_geojson: Dict[str, Any],
    where: str = "1=1",
    out_fields: str = "*",
    token: Optional[str] = None,
    result_record_count: int = 2000,
) -> List[Dict[str, Any]]:
    """
    Generic ArcGIS FeatureServer query using GeoJSON geometry (intersects).
    Returns list of GeoJSON features (attributes under ['properties'] if f=geojson).
    """
    url = urljoin(base_url.rstrip("/") + "/", f"FeatureServer/{layer_id}/query")
    params = {
        "f": "geojson",
        "where": where,
        "outFields": out_fields,
        "geometry": json.dumps(geometry_geojson),
        "geometryType": "esriGeometryPolygon",
        "spatialRel": "esriSpatialRelIntersects",
        "returnGeometry": "true",
        "resultRecordCount": result_record_count,
    }
    with httpx.Client(timeout=30) as client:
        response = client.get(url, params=params, headers=_headers(token))
        response.raise_for_status()
        data = response.json()
    return data.get("features") or []
