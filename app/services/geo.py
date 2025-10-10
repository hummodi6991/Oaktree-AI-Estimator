from typing import Tuple, Dict, Any, Optional
from shapely.geometry import shape, mapping, Polygon, MultiPolygon
import json
from shapely.geometry import shape as _shape
import math


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
