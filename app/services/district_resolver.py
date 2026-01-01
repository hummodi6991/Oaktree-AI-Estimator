from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from shapely.geometry import Point
from shapely.geometry import shape as shapely_shape
from sqlalchemy.orm import Session

from app.ml.name_normalization import norm_city, norm_district
from app.services.geo import infer_district_from_features
from app.services.kaggle_district import infer_district_from_kaggle


@dataclass
class DistrictResolution:
    city_norm: str
    district_raw: Optional[str]
    district_norm: Optional[str]
    method: str  # provided | aqar_hull | osm_polygon | aqar_nn | none
    confidence: float
    distance_m: Optional[float] = None
    evidence_count: int = 0
    layer: Optional[str] = None


def resolve_district(
    db: Session,
    *,
    city: str,
    geom_geojson: dict | None = None,
    lon: float | None = None,
    lat: float | None = None,
    district: str | None = None,
    prefer_layers: list[str] | None = None,
) -> DistrictResolution:
    """
    Resolve district using polygon-first approach:
      1) provided district
      2) ExternalFeature polygons (default prefer: aqar_district_hulls, then osm_districts)
      3) Aqar/Kaggle nearest neighbor fallback
    Returns DistrictResolution with method + confidence + evidence.
    """

    city_norm = norm_city(city) or (city.lower() if city else "")

    district_clean = (district or "").strip()
    if district_clean:
        district_norm = norm_district(city_norm, district_clean) if city_norm else None
        return DistrictResolution(
            city_norm=city_norm,
            district_raw=district_clean,
            district_norm=district_norm,
            method="provided",
            confidence=1.0,
        )

    geom = None
    if geom_geojson:
        try:
            geom = shapely_shape(geom_geojson)
        except Exception:
            geom = None
    if geom is None and lon is not None and lat is not None:
        try:
            geom = Point(float(lon), float(lat))
        except Exception:
            geom = None

    layers = prefer_layers or ["aqar_district_hulls", "osm_districts"]
    if geom is not None:
        for layer in layers:
            try:
                district_raw = infer_district_from_features(db, geom, layer=layer)
            except Exception:
                district_raw = None
            if district_raw:
                district_norm = norm_district(city_norm, district_raw) if city_norm else None
                method = (
                    "aqar_hull" if layer == "aqar_district_hulls" else "osm_polygon" if layer == "osm_districts" else "external_feature"
                )
                return DistrictResolution(
                    city_norm=city_norm,
                    district_raw=district_raw,
                    district_norm=district_norm,
                    method=method,
                    confidence=0.95,
                    layer=layer,
                )

    centroid_lon = lon
    centroid_lat = lat
    if (centroid_lon is None or centroid_lat is None) and geom is not None:
        try:
            centroid_lon = float(geom.centroid.x)
            centroid_lat = float(geom.centroid.y)
        except Exception:
            centroid_lon = centroid_lon
            centroid_lat = centroid_lat

    if centroid_lon is not None and centroid_lat is not None:
        try:
            kaggle_result = infer_district_from_kaggle(
                db, city=city_norm, lon=centroid_lon, lat=centroid_lat, max_radius_m=2000
            )
        except Exception:
            kaggle_result = {}

        district_raw = (kaggle_result or {}).get("district_raw")
        district_norm = (kaggle_result or {}).get("district_normalized")
        if not district_norm and district_raw:
            district_norm = norm_district(city_norm, district_raw) if city_norm else None

        method = (kaggle_result or {}).get("method") or "aqar_nn"
        confidence = float((kaggle_result or {}).get("confidence") or 0.0)
        distance_m = (kaggle_result or {}).get("distance_m")
        evidence_count = int((kaggle_result or {}).get("evidence_count") or 0)

        return DistrictResolution(
            city_norm=city_norm,
            district_raw=district_raw,
            district_norm=district_norm,
            method=method if district_raw else "none",
            confidence=confidence if district_raw else 0.0,
            distance_m=distance_m,
            evidence_count=evidence_count,
        )

    return DistrictResolution(
        city_norm=city_norm,
        district_raw=None,
        district_norm=None,
        method="none",
        confidence=0.0,
    )


def resolution_meta(r: DistrictResolution) -> dict[str, Any]:
    return {
        "city_norm": r.city_norm,
        "district_raw": r.district_raw,
        "district_norm": r.district_norm,
        "method": r.method,
        "confidence": r.confidence,
        "distance_m": r.distance_m,
        "evidence_count": r.evidence_count,
        "layer": r.layer,
    }
