import json
import logging
import math
from typing import Any, Dict, Optional, Tuple

from shapely.geometry import MultiPolygon, Polygon, mapping, shape
from shapely.geometry import shape as _shape
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def parse_geojson(gj: Dict[str, Any] | str):
    """
    Accept GeoJSON either as a dict or a JSON string; return a Shapely geometry.
    """
    if isinstance(gj, str):
        try:
            gj = json.loads(gj)
        except Exception as exc:
            raise ValueError("geometry must be a GeoJSON object or JSON-encoded string") from exc
    return shape(gj)


def project_to_xy_meters(lon: float, lat: float, lat0: float) -> Tuple[float, float]:
    R = 6371000.0
    x = math.radians(lon) * R * math.cos(math.radians(lat0))
    y = math.radians(lat) * R
    return x, y
def _ring_area_m2(coords, lat0: float) -> float:
    XY = [project_to_xy_meters(lon, lat, lat0) for lon, lat in coords]
    shoelace = 0.0
    for i in range(len(XY) - 1):
        x1, y1 = XY[i]
        x2, y2 = XY[i + 1]
        shoelace += (x1 * y2 - x2 * y1)
    return abs(shoelace) / 2.0


def _poly_area_m2(poly: Polygon) -> float:
    lat0 = poly.centroid.y
    area = _ring_area_m2(list(poly.exterior.coords), lat0)
    for interior in poly.interiors:
        area -= _ring_area_m2(list(interior.coords), lat0)
    return max(0.0, area)


def area_m2(geom) -> float:
    # Equirectangular approximation around centroid latitude (good enough for MVP)
    if isinstance(geom, MultiPolygon):
        return sum(_poly_area_m2(poly) for poly in geom.geoms)
    if isinstance(geom, Polygon):
        return _poly_area_m2(geom)

    # Fallback for unexpected geometry types: attempt to coerce to polygonal area
    try:
        polygonized = geom.buffer(0)
        if isinstance(polygonized, (Polygon, MultiPolygon)):
            return area_m2(polygonized)
    except Exception:
        pass
    return 0.0


def to_geojson(geom):
    return mapping(geom)


def infer_district_from_features(db, geom, layer: str = "rydpolygons") -> Optional[str]:
    from app.models.tables import ExternalFeature

    rows = db.query(ExternalFeature).filter(ExternalFeature.layer_name == layer).all()
    for r in rows:
        try:
            poly = _shape(r.geometry)
            if poly.contains(geom):
                props = {(k or "").lower(): v for k, v in (r.properties or {}).items()}
                return props.get("district") or props.get("name") or props.get("district_en")
        except Exception:
            continue
    return None


def infer_far_from_features(db, geom, layer: str = "rydpolygons") -> float | None:
    """Infer the maximum FAR from external features intersecting the geometry."""

    from app.models.tables import ExternalFeature

    rows = db.query(ExternalFeature).filter(ExternalFeature.layer_name == layer).all()
    candidates: list[float] = []
    for r in rows:
        try:
            poly = _shape(r.geometry)
            if not poly.intersects(geom):
                continue
            props = {(k or "").lower(): v for k, v in (r.properties or {}).items()}
            for key in ("far", "max_far", "far_max", "z_far"):
                val = props.get(key)
                if val is None:
                    continue
                try:
                    numeric = float(str(val).replace(",", ""))
                    if numeric > 0:
                        candidates.append(numeric)
                except Exception:
                    continue
        except Exception:
            continue
    return max(candidates) if candidates else None


def _landuse_code_from_label(label: str) -> str | None:
    """
    Normalize any upstream land-use/zone label to { 's', 'm' } or None.
    - 's': residential / housing (سكني, house, apartments, residential, …)
    - 'm': mixed/commercial (mixed-use, commercial, retail, office, تجاري, مختلط, …)
    - 'yes'/'true'/'1' → None (ambiguous; caller should fall back to OSM overlay)
    """
    t = (label or "").strip()
    if not t:
        return None
    tl = t.lower()
    if tl in {"s", "m"}:
        return tl

    # Ambiguous boolean-ish values from OSM tags (e.g., building=yes)
    if tl in {"yes", "true", "1", "y"}:
        return None

    # Residential signals
    if ("سكن" in t) or any(k in tl for k in [
        "residential", "residence", "housing", "house", "apart", "apartment", "villa", "dwelling"
    ]):
        return "s"

    # Mixed/commercial signals
    if ("تجاري" in t) or ("مختلط" in t) or any(k in tl for k in [
        "mixed", "mixed-use", "mixed use", "commercial", "retail", "office", "shop", "mall"
    ]):
        return "m"

    return None


def infer_district_from_aqar_listings(
    db: Session,
    point,
    city: str | None = None,
    max_distance_km: float = 3.0,
) -> str | None:
    """
    Infer a district name from the nearest Kaggle Aqar listing.

    - Uses aqar.listings (Kaggle scrape) which should have lat/lng columns.
    - Restricts to the requested city when provided.
    - Ignores results further than `max_distance_km` (to avoid crazy matches).
    """

    if point is None:
        return None

    lon = float(point.x)
    lat = float(point.y)

    # Adjust column names here if your table uses different ones,
    # e.g. "longitude"/"latitude" instead of "lng"/"lat".
    sql = text(
        """
        SELECT district,
               ST_DistanceSphere(
                 ST_SetSRID(ST_MakePoint(:lon, :lat), 4326),
                 ST_SetSRID(ST_MakePoint(lng, lat), 4326)
               ) AS dist_m
        FROM aqar.listings
        WHERE price_per_sqm IS NOT NULL
          AND lat IS NOT NULL
          AND lng IS NOT NULL
          AND (:city IS NULL OR lower(city) = lower(:city))
        ORDER BY dist_m
        LIMIT 1
        """
    )

    row = db.execute(sql, {"lon": lon, "lat": lat, "city": city}).first()
    if not row:
        return None

    dist_m = row.dist_m
    if dist_m is not None and dist_m > max_distance_km * 1000:
        logger.info(
            "Nearest Aqar listing too far away (%.1f m); not using its district", dist_m
        )
        return None

    return row.district
