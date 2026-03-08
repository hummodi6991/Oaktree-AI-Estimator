"""
Geographic resolution for delivery records.

Multi-tier strategy:
  Tier A — Direct coordinates from platform payload
  Tier B — Structured area/district extraction from text
  Tier C — Approximate geocoding using existing known POIs
  Tier D — District centroid fallback

Always tracks geocode_method and location_confidence so downstream
consumers know what precision level to trust.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.delivery.schemas import DeliveryRecord, GeocodeMethod

logger = logging.getLogger(__name__)

# Approximate centroids for major Riyadh districts (WGS84)
# Used as Tier D fallback when only district name is known
RIYADH_DISTRICT_CENTROIDS: dict[str, tuple[float, float]] = {
    "olaya": (24.6905, 46.6853),
    "al olaya": (24.6905, 46.6853),
    "malaz": (24.6651, 46.7218),
    "al malaz": (24.6651, 46.7218),
    "murabba": (24.6488, 46.7103),
    "al murabba": (24.6488, 46.7103),
    "naseem": (24.6775, 46.7723),
    "al naseem": (24.6775, 46.7723),
    "rawdah": (24.7190, 46.7285),
    "al rawdah": (24.7190, 46.7285),
    "sulaymaniyah": (24.6975, 46.6995),
    "al sulaymaniyah": (24.6975, 46.6995),
    "shifa": (24.5803, 46.7200),
    "al shifa": (24.5803, 46.7200),
    "batha": (24.6340, 46.7140),
    "al batha": (24.6340, 46.7140),
    "worood": (24.7019, 46.6640),
    "al worood": (24.7019, 46.6640),
    "nakheel": (24.7685, 46.6365),
    "al nakheel": (24.7685, 46.6365),
    "hamra": (24.7451, 46.7555),
    "al hamra": (24.7451, 46.7555),
    "yasmin": (24.8155, 46.6260),
    "al yasmin": (24.8155, 46.6260),
    "narjis": (24.8295, 46.6365),
    "al narjis": (24.8295, 46.6365),
    "aqiq": (24.7745, 46.6175),
    "al aqiq": (24.7745, 46.6175),
    "sahafah": (24.8085, 46.6535),
    "al sahafah": (24.8085, 46.6535),
    "hittin": (24.7560, 46.6085),
    "rabwah": (24.7135, 46.7025),
    "al rabwah": (24.7135, 46.7025),
    "khaleej": (24.7352, 46.7463),
    "al khaleej": (24.7352, 46.7463),
    "wadi": (24.7429, 46.6512),
    "al wadi": (24.7429, 46.6512),
    "ghadir": (24.7810, 46.6700),
    "al ghadir": (24.7810, 46.6700),
    "arid": (24.8400, 46.6350),
    "al arid": (24.8400, 46.6350),
    "qayrawan": (24.8500, 46.6200),
    "al qayrawan": (24.8500, 46.6200),
    "rimal": (24.7550, 46.8000),
    "al rimal": (24.7550, 46.8000),
    "tuwaiq": (24.6275, 46.6175),
    "dar al baida": (24.5520, 46.7885),
    "al dar al baida": (24.5520, 46.7885),
    "irqah": (24.6800, 46.5750),
    "shuhada": (24.7020, 46.7540),
    "al shuhada": (24.7020, 46.7540),
    "suwaidi": (24.6165, 46.6550),
    "al suwaidi": (24.6165, 46.6550),
    "aziziyah": (24.6285, 46.7760),
    "al aziziyah": (24.6285, 46.7760),
    "khuzama": (24.7125, 46.6440),
    "al khuzama": (24.7125, 46.6440),
    "takhassusi": (24.7150, 46.6800),
    "al takhassusi": (24.7150, 46.6800),
    "andalus": (24.7235, 46.7105),
    "al andalus": (24.7235, 46.7105),
}


def _is_valid_riyadh_coords(lat: float | None, lon: float | None) -> bool:
    """Check if coordinates are plausibly in Riyadh."""
    if lat is None or lon is None:
        return False
    return 24.3 <= lat <= 25.1 and 46.3 <= lon <= 47.1


def _normalize_district_name(raw: str | None) -> str | None:
    """Normalize a district name for centroid lookup."""
    if not raw:
        return None
    name = raw.lower().strip()
    name = re.sub(r"[_\-]", " ", name)
    name = re.sub(r"\s+", " ", name)
    return name


def resolve_location(record: DeliveryRecord, db: Session | None = None) -> DeliveryRecord:
    """
    Apply multi-tier location resolution to a delivery record.

    Mutates and returns the record with updated lat/lon,
    geocode_method, and location_confidence.
    """

    # Tier A: Direct coordinates already present
    if _is_valid_riyadh_coords(record.lat, record.lon):
        if record.geocode_method == GeocodeMethod.NONE:
            record.geocode_method = GeocodeMethod.PLATFORM_PAYLOAD
            record.location_confidence = 0.9
        return record

    # Tier B: Extract district from available text fields
    district = record.district_text
    if not district and record.address_raw:
        district = _extract_district(record.address_raw)
    if not district and record.restaurant_name_raw:
        district = _extract_district(record.restaurant_name_raw)
    if not district and record.source_url:
        district = _extract_district_from_url(record.source_url)

    if district and not record.district_text:
        record.district_text = district

    # Tier C: Match against existing restaurant POIs by name + district
    if db and record.restaurant_name_raw and district:
        try:
            match = _match_existing_poi(
                db, record.restaurant_name_raw, district
            )
            if match:
                record.lat = match["lat"]
                record.lon = match["lon"]
                record.geocode_method = GeocodeMethod.POI_MATCH
                record.location_confidence = 0.7
                return record
        except Exception as exc:
            logger.debug("POI match failed: %s", exc)
            try:
                db.rollback()
            except Exception:
                pass

    # Tier D: District centroid fallback
    if district:
        normalized = _normalize_district_name(district)
        if normalized and normalized in RIYADH_DISTRICT_CENTROIDS:
            lat, lon = RIYADH_DISTRICT_CENTROIDS[normalized]
            record.lat = lat
            record.lon = lon
            record.geocode_method = GeocodeMethod.DISTRICT_CENTROID
            record.location_confidence = 0.3
            return record

    # No location resolved — record still stored with location_confidence=0
    return record


def _extract_district(text_val: str) -> str | None:
    """Extract a known district name from text."""
    if not text_val:
        return None
    lower = text_val.lower()
    for district_key in RIYADH_DISTRICT_CENTROIDS:
        if district_key in lower:
            return district_key.title()
    return None


def _extract_district_from_url(url: str) -> str | None:
    """Extract district from URL path segments."""
    if not url:
        return None
    from urllib.parse import urlparse as _urlparse
    path = _urlparse(url).path.lower().replace("-", " ").replace("_", " ")
    for district_key in RIYADH_DISTRICT_CENTROIDS:
        if district_key in path:
            return district_key.title()
    return None


def _match_existing_poi(
    db: Session, name: str, district: str
) -> dict[str, float] | None:
    """
    Try to find a matching restaurant_poi by name similarity + district.
    Returns {lat, lon} if a confident match is found.
    """
    # Use exact normalized name match within the same district
    result = db.execute(
        text("""
            SELECT lat, lon FROM restaurant_poi
            WHERE LOWER(name) = LOWER(:name)
              AND district IS NOT NULL
              AND LOWER(district) = LOWER(:district)
            LIMIT 1
        """),
        {"name": name.strip(), "district": district.strip()},
    ).first()

    if result:
        return {"lat": float(result[0]), "lon": float(result[1])}

    # Fallback: partial name match within district
    result = db.execute(
        text("""
            SELECT lat, lon FROM restaurant_poi
            WHERE LOWER(name) LIKE :pattern
              AND district IS NOT NULL
              AND LOWER(district) = LOWER(:district)
            ORDER BY LENGTH(name) ASC
            LIMIT 1
        """),
        {
            "pattern": f"%{name.strip().lower()}%",
            "district": district.strip(),
        },
    ).first()

    if result:
        return {"lat": float(result[0]), "lon": float(result[1])}

    return None
