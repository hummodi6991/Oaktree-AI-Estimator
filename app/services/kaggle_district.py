from typing import Any, Dict, Optional

from shapely.geometry import shape
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.ml.name_normalization import norm_city, norm_district


def _haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Return the great-circle distance in meters between two WGS84 points."""

    from math import radians, cos, sin, asin, sqrt

    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    R = 6371000.0
    return R * c


def infer_district_from_kaggle(
    db: Session,
    city: Optional[str],
    lon: Optional[float] = None,
    lat: Optional[float] = None,
    geom_geojson: Optional[Dict[str, Any]] = None,
    max_radius_m: float = 2000.0,
) -> Dict[str, Any]:
    """
    Infer a district from Kaggle Aqar listings near the geometry centroid.

    Returns a dict containing:
    - district_raw
    - district_normalized
    - method
    - distance_m
    - evidence_count
    - confidence (0..1)
    """

    if lon is None or lat is None:
        try:
            centroid = shape(geom_geojson).centroid if geom_geojson else None
        except Exception:
            centroid = None
        if centroid:
            lon = float(centroid.x)
            lat = float(centroid.y)

    if lon is None or lat is None:
        return {
            "district_raw": None,
            "district_normalized": None,
            "method": "kaggle_nearest_listing",
            "distance_m": None,
            "evidence_count": 0,
            "confidence": 0.0,
        }

    params = {"lon": lon, "lat": lat}
    base_sql = """
        SELECT district, city, lon, lat
        FROM aqar.listings
        WHERE lon IS NOT NULL
          AND lat IS NOT NULL
          AND district IS NOT NULL
          AND price_per_sqm IS NOT NULL
    """

    where_clauses = []
    if city:
        where_clauses.append("lower(city) = lower(:city)")

    sql = base_sql
    if where_clauses:
        sql += " AND " + " AND ".join(where_clauses)
    # Pull the closest 500 listings by naive planar distance to keep the client-side
    # haversine search focused on nearby evidence (avoids returning a random slice).
    sql += " ORDER BY ((lon - :lon) * (lon - :lon) + (lat - :lat) * (lat - :lat)) ASC"
    sql += " LIMIT 500"

    try:
        rows = list(db.execute(text(sql), {**params, "city": city}).all())
    except Exception:
        rows = []

    best_row = None
    best_distance = None
    for row in rows:
        try:
            r_lon = float(row.lon)
            r_lat = float(row.lat)
        except Exception:
            continue
        d = _haversine_m(lon, lat, r_lon, r_lat)
        if best_distance is None or d < best_distance:
            best_row = row
            best_distance = d

    city_norm = norm_city(city) if city else None
    district_raw = getattr(best_row, "district", None) if best_row else None
    district_norm = norm_district(city_norm, district_raw) if district_raw else None

    if max_radius_m is not None and best_distance is not None and best_distance > max_radius_m:
        district_raw = None
        district_norm = None

    # Simple confidence heuristic: 1.0 at distance 0, decays linearly to 0 at max_radius_m
    confidence = 0.0
    if best_distance is not None and max_radius_m:
        confidence = max(0.0, min(1.0, 1.0 - float(best_distance) / float(max_radius_m)))

    return {
        "district_raw": district_raw,
        "district_normalized": district_norm,
        "method": "kaggle_nearest_listing",
        "distance_m": float(best_distance) if best_distance is not None else None,
        "evidence_count": len(rows),
        "confidence": confidence,
    }
