import pytest

from app.services import district_resolver
from app.services.district_resolver import resolve_district


def test_resolve_district_prefers_aqar_polygon(monkeypatch):
    def fake_infer(db, geom, layer: str = ""):
        if layer == "aqar_district_hulls":
            return "حي الاختبار"
        return None

    monkeypatch.setattr(district_resolver, "infer_district_from_features", fake_infer)

    resolution = resolve_district(
        db=None,
        city="Riyadh",
        geom_geojson={"type": "Point", "coordinates": [46.7, 24.7]},
    )

    assert resolution.method == "aqar_hull"
    assert resolution.confidence == 0.95
    assert resolution.district_raw == "حي الاختبار"


def test_resolve_district_falls_back_to_kaggle(monkeypatch):
    def fake_infer(db, geom, layer: str = ""):
        return None

    def fake_kaggle(db, city, lon, lat, max_radius_m=2000):
        return {
            "district_raw": "fallback district",
            "district_normalized": "fallback-district",
            "method": "kaggle_nearest_listing",
            "confidence": 0.42,
            "distance_m": 123.0,
            "evidence_count": 5,
        }

    monkeypatch.setattr(district_resolver, "infer_district_from_features", fake_infer)
    monkeypatch.setattr(district_resolver, "infer_district_from_kaggle", fake_kaggle)

    resolution = resolve_district(
        db=None,
        city="Riyadh",
        lon=46.7,
        lat=24.7,
    )

    assert resolution.method == "kaggle_nearest_listing"
    assert resolution.district_raw == "fallback district"
    assert resolution.district_norm == "fallback-district"
    assert resolution.confidence == 0.42
