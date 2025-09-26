from typing import Tuple, Dict, Any
from shapely.geometry import shape, mapping
import math


def parse_geojson(gj: Dict[str, Any]):
    return shape(gj)


def project_to_xy_meters(lon: float, lat: float, lat0: float) -> Tuple[float, float]:
    R = 6371000.0
    x = math.radians(lon) * R * math.cos(math.radians(lat0))
    y = math.radians(lat) * R
    return x, y


def area_m2(geom) -> float:
    # Equirectangular approximation around centroid latitude (good enough for MVP)
    c = geom.centroid
    lat0 = c.y
    coords = list(geom.exterior.coords)
    XY = [project_to_xy_meters(lon, lat, lat0) for lon, lat in coords]
    # Shoelace
    area = 0.0
    for i in range(len(XY) - 1):
        x1, y1 = XY[i]
        x2, y2 = XY[i + 1]
        area += (x1 * y2 - x2 * y1)
    return abs(area) / 2.0


def to_geojson(geom):
    return mapping(geom)
