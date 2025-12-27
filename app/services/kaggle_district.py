from typing import Any, Dict, Optional

from shapely.geometry import shape
from collections import Counter
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
        # Keep the city predicate first for precision, but we will retry without it
        # if it returns no evidence (this happens when listings use Arabic names
        # while req.city is English, or vice versa).
        where_clauses.append("lower(city) = lower(:city)")

    def _fetch_rows(use_city: bool) -> list[tuple]:
        sql = base_sql
        clauses = list(where_clauses) if use_city else []
        if clauses:
            sql += " AND " + " AND ".join(clauses)
        # Pull the closest 500 listings by naive planar distance to keep the client-side
        # haversine search focused on nearby evidence (avoids returning a random slice).
        sql += " ORDER BY ((lon - :lon) * (lon - :lon) + (lat - :lat) * (lat - :lat)) ASC"
        sql += " LIMIT 500"
        try:
            return list(db.execute(text(sql), {**params, "city": city}).all())
        except Exception:
            return []

    # 1) Try city-filtered for accuracy
    rows = _fetch_rows(use_city=bool(city))
    # 2) If no evidence, retry without city filter for coverage
    if not rows and city:
        rows = _fetch_rows(use_city=False)

    best = None
    best_d = None
    for (d_raw, c_raw, lon2, lat2) in rows:
        d_m = _haversine_m(lon, lat, float(lon2), float(lat2))
        if max_radius_m is not None and d_m > max_radius_m:
            continue
        if best_d is None or d_m < best_d:
            best = (d_raw, c_raw, float(lon2), float(lat2))
            best_d = d_m

    if not best:
        return {
            "district_raw": None,
            "district_normalized": None,
            "method": "kaggle_nearest_listing",
            "distance_m": None,
            "evidence_count": 0,
            "confidence": 0.0,
        }

    district_raw = str(best[0]).strip() if best else None
    # Normalize using the caller-provided city when possible; if city is missing,
    # fall back to the listing city for normalization.
    norm_city_input = norm_city(city) if city else None
    if not norm_city_input:
        norm_city_input = norm_city(best[1]) if best and best[1] else None
    district_norm = norm_district(norm_city_input, district_raw) if (norm_city_input and district_raw) else None

    # Optional: compute a simple confidence based on how many nearby points agree on the district.
    # This helps debugging and future thresholding.
    nearby = [str(r[0]).strip() for r in rows[:50] if r and r[0]]
    counts = Counter(nearby)
    top = counts.most_common(1)[0][1] if counts else 0
    conf = min(0.95, 0.25 + (top / 50.0) * 0.70) if nearby else 0.0

    return {
        "district_raw": district_raw,
        "district_normalized": district_norm,
        "method": "kaggle_nearest_listing",
        "distance_m": float(best_d) if best_d is not None else None,
        "evidence_count": len(rows),
        "confidence": conf,
    }
