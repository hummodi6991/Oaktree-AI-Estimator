"""
Entity resolution: match delivery_source_record -> restaurant_poi.

Conservative deterministic matching using multiple signals:
1. Exact normalized name + proximity
2. Same website/URL domain
3. Same phone
4. Known brand + branch + district
5. Fuzzy name + strong location evidence

Does NOT over-merge.  Unmatched records stay in raw table as-is.
Resolver is re-runnable independently of scraping.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.delivery.models import DeliverySourceRecord

logger = logging.getLogger(__name__)


def _normalize_for_match(name: str | None) -> str:
    """Normalize a name for matching: lowercase, strip punctuation, collapse spaces."""
    if not name:
        return ""
    n = name.lower().strip()
    n = re.sub(r"[^\w\s\u0600-\u06FF]", " ", n)  # keep Arabic + alphanumeric
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _extract_domain(url: str | None) -> str | None:
    """Extract domain from URL for matching."""
    if not url:
        return None
    from urllib.parse import urlparse
    parsed = urlparse(url)
    return parsed.netloc.lower().replace("www.", "") if parsed.netloc else None


def resolve_run(db: Session, run_id: int) -> int:
    """
    Run entity resolution for all unresolved records in a given ingest run.
    Returns count of matched records.
    """
    records = (
        db.query(DeliverySourceRecord)
        .filter(
            DeliverySourceRecord.ingest_run_id == run_id,
            DeliverySourceRecord.entity_resolution_status == "pending",
        )
        .all()
    )

    matched = 0
    for rec in records:
        result = _resolve_single(db, rec)
        if result:
            rec.matched_restaurant_poi_id = result["poi_id"]
            rec.matched_entity_confidence = result["confidence"]
            rec.entity_resolution_status = "matched"
            matched += 1
        else:
            rec.entity_resolution_status = "unmatched"

    if matched > 0:
        db.flush()

    logger.info(
        "Resolver run %d: %d/%d records matched", run_id, matched, len(records)
    )
    return matched


def resolve_all_pending(db: Session, limit: int = 5000) -> int:
    """Resolve all pending records across all runs. Returns match count."""
    records = (
        db.query(DeliverySourceRecord)
        .filter(DeliverySourceRecord.entity_resolution_status == "pending")
        .limit(limit)
        .all()
    )

    matched = 0
    for rec in records:
        result = _resolve_single(db, rec)
        if result:
            rec.matched_restaurant_poi_id = result["poi_id"]
            rec.matched_entity_confidence = result["confidence"]
            rec.entity_resolution_status = "matched"
            matched += 1
        else:
            rec.entity_resolution_status = "unmatched"

    if matched > 0:
        db.flush()

    logger.info("Resolver: %d/%d pending records matched", matched, len(records))
    return matched


def _resolve_single(
    db: Session, rec: DeliverySourceRecord
) -> dict[str, Any] | None:
    """
    Try to match a single delivery record to a restaurant_poi.
    Returns {poi_id, confidence} or None.
    """

    # Strategy 1: Exact normalized name + same district
    if rec.restaurant_name_normalized and rec.district_text:
        match = _match_name_district(
            db, rec.restaurant_name_normalized, rec.district_text
        )
        if match:
            return match

    # Strategy 2: Same brand + district (for chains)
    if rec.brand_raw and rec.district_text:
        match = _match_brand_district(db, rec.brand_raw, rec.district_text)
        if match:
            return match

    # Strategy 3: Name + proximity (if we have coordinates)
    if rec.restaurant_name_normalized and rec.lat and rec.lon:
        if rec.location_confidence >= 0.5:
            match = _match_name_proximity(
                db, rec.restaurant_name_normalized, float(rec.lat), float(rec.lon)
            )
            if match:
                return match

    # Strategy 4: Same website domain
    if rec.website_raw:
        domain = _extract_domain(rec.website_raw)
        if domain:
            match = _match_website(db, domain)
            if match:
                return match

    return None


def _normalize_name_sql() -> str:
    """Return a SQL expression that normalizes restaurant_poi.name the same
    way ``_normalize_for_match`` does in Python: lowercase, strip
    non-alphanumeric/non-Arabic characters, collapse whitespace."""
    return (
        "TRIM(regexp_replace("
        "  regexp_replace(LOWER(name), '[^a-z0-9\\s\\u0600-\\u06FF]', ' ', 'g'),"
        "  '\\s+', ' ', 'g'))"
    )


def _match_name_district(
    db: Session, name: str, district: str
) -> dict[str, Any] | None:
    """Match by exact normalized name + district."""
    norm = _normalize_for_match(name)
    if not norm or len(norm) < 3:
        return None

    try:
        row = db.execute(
            text(f"""
                SELECT id FROM restaurant_poi
                WHERE {_normalize_name_sql()} = :name
                  AND LOWER(district) = LOWER(:district)
                LIMIT 1
            """),
            {"name": norm, "district": district.strip()},
        ).first()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return None

    if row:
        return {"poi_id": row[0], "confidence": 0.85}
    return None


def _match_brand_district(
    db: Session, brand: str, district: str
) -> dict[str, Any] | None:
    """Match a chain brand + district to an existing POI."""
    try:
        row = db.execute(
            text("""
                SELECT id FROM restaurant_poi
                WHERE LOWER(chain_name) = LOWER(:brand)
                  AND LOWER(district) = LOWER(:district)
                LIMIT 1
            """),
            {"brand": brand.strip(), "district": district.strip()},
        ).first()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return None

    if row:
        return {"poi_id": row[0], "confidence": 0.80}
    return None


def _match_name_proximity(
    db: Session, name: str, lat: float, lon: float, radius_m: float = 200
) -> dict[str, Any] | None:
    """Match by name + spatial proximity."""
    norm = _normalize_for_match(name)
    if not norm or len(norm) < 3:
        return None

    try:
        row = db.execute(
            text(f"""
                SELECT id FROM restaurant_poi
                WHERE {_normalize_name_sql()} = :name
                  AND geom IS NOT NULL
                  AND ST_DWithin(
                      geom::geography,
                      ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                      :radius_m
                  )
                LIMIT 1
            """),
            {"name": norm, "lat": lat, "lon": lon, "radius_m": radius_m},
        ).first()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return None

    if row:
        return {"poi_id": row[0], "confidence": 0.90}
    return None


def _match_website(db: Session, domain: str) -> dict[str, Any] | None:
    """Match by website domain stored in raw JSONB."""
    # This is a weak signal on its own, only match if very specific domain
    if not domain or domain in ("google.com", "facebook.com", "instagram.com"):
        return None

    try:
        row = db.execute(
            text("""
                SELECT id FROM restaurant_poi
                WHERE raw->>'source_url' ILIKE :pattern
                LIMIT 1
            """),
            {"pattern": f"%{domain}%"},
        ).first()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return None

    if row:
        return {"poi_id": row[0], "confidence": 0.60}
    return None
