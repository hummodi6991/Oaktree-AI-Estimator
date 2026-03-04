"""
Ingestion pipeline for restaurant POI data from multiple sources.

Sources:
- Overture Maps Places (DuckDB → S3)
- OSM (Overpass API)
- Delivery platforms (HungerStation, Talabat, Mrsool)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.models.tables import RestaurantPOI
from app.services.restaurant_categories import (
    normalize_category,
    normalize_osm_cuisine,
    normalize_overture_taxonomy,
)

logger = logging.getLogger(__name__)


def _upsert_poi(db: Session, poi: dict) -> bool:
    """Upsert a single restaurant POI. Returns True if new row inserted."""
    row = db.query(RestaurantPOI).filter_by(id=poi["id"]).first()
    if row:
        row.name = poi.get("name") or row.name
        row.category = poi.get("category") or row.category
        row.rating = poi.get("rating") or row.rating
        row.review_count = poi.get("review_count") or row.review_count
        row.observed_at = datetime.now(timezone.utc)
        return False

    db.add(
        RestaurantPOI(
            id=poi["id"],
            name=poi.get("name", "Unknown"),
            name_ar=poi.get("name_ar"),
            category=poi.get("category", "international"),
            subcategory=poi.get("subcategory"),
            source=poi.get("source", "unknown"),
            lat=poi["lat"],
            lon=poi["lon"],
            rating=poi.get("rating"),
            review_count=poi.get("review_count"),
            price_level=poi.get("price_level"),
            chain_name=poi.get("chain_name"),
            district=poi.get("district"),
            raw=poi.get("raw"),
            observed_at=datetime.now(timezone.utc),
        )
    )
    return True


def ingest_overture_restaurants(db: Session) -> int:
    """Ingest restaurant POIs from Overture Maps Places."""
    from app.connectors.overture_places import extract_restaurants_duckdb

    n = 0
    for rec in extract_restaurants_duckdb():
        category = normalize_overture_taxonomy(rec.get("category_raw"))
        poi = {
            "id": rec["id"],
            "name": rec.get("name", "Unknown"),
            "category": category,
            "source": "overture",
            "lat": rec["lat"],
            "lon": rec["lon"],
            "raw": {"confidence": rec.get("confidence")},
        }
        _upsert_poi(db, poi)
        n += 1
        if n % 500 == 0:
            db.flush()

    db.commit()
    logger.info("Ingested %d Overture restaurant POIs", n)
    return n


def ingest_osm_restaurants(db: Session) -> int:
    """Ingest restaurant POIs from OpenStreetMap via Overpass API."""
    import httpx

    overpass_url = "https://overpass-api.de/api/interpreter"
    query = """
    [out:json][timeout:120];
    area["name:en"="Riyadh"]->.a;
    (
      node["amenity"~"restaurant|fast_food|cafe"](area.a);
      way["amenity"~"restaurant|fast_food|cafe"](area.a);
    );
    out center;
    """

    logger.info("Querying Overpass API for Riyadh restaurants...")
    try:
        r = httpx.post(overpass_url, data={"data": query}, timeout=180)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        logger.error("Overpass query failed: %s", exc)
        return 0

    elements = data.get("elements", [])
    n = 0
    for el in elements:
        osm_id = el.get("id")
        tags = el.get("tags", {})
        lat = el.get("lat") or (el.get("center", {}).get("lat"))
        lon = el.get("lon") or (el.get("center", {}).get("lon"))

        if not lat or not lon:
            continue

        cuisine = tags.get("cuisine", "")
        category = normalize_osm_cuisine(cuisine) if cuisine else normalize_category(
            tags.get("amenity", "restaurant")
        )

        poi = {
            "id": f"osm:{osm_id}",
            "name": tags.get("name", tags.get("name:en", "Unknown")),
            "name_ar": tags.get("name:ar"),
            "category": category,
            "source": "osm",
            "lat": float(lat),
            "lon": float(lon),
            "raw": tags,
        }
        _upsert_poi(db, poi)
        n += 1
        if n % 500 == 0:
            db.flush()

    db.commit()
    logger.info("Ingested %d OSM restaurant POIs", n)
    return n


def ingest_delivery_platforms(db: Session) -> int:
    """Ingest restaurant POIs from delivery platform scrapers."""
    from app.connectors.delivery_platforms import (
        scrape_hungerstation_riyadh,
        scrape_mrsool_riyadh,
        scrape_talabat_riyadh,
    )

    n = 0
    for scraper, source in [
        (scrape_hungerstation_riyadh, "hungerstation"),
        (scrape_talabat_riyadh, "talabat"),
        (scrape_mrsool_riyadh, "mrsool"),
    ]:
        try:
            for rec in scraper():
                category = normalize_category(rec.get("category_raw"))
                poi = {
                    "id": rec["id"],
                    "name": rec.get("name", "Unknown"),
                    "category": category,
                    "source": source,
                    "lat": rec.get("lat"),
                    "lon": rec.get("lon"),
                    "raw": {"source_url": rec.get("source_url")},
                }
                if poi["lat"] and poi["lon"]:
                    _upsert_poi(db, poi)
                    n += 1
        except Exception as exc:
            logger.warning("Scraper %s failed: %s", source, exc)

    db.commit()
    logger.info("Ingested %d delivery platform restaurant POIs", n)
    return n


def ingest_all(db: Session) -> dict[str, int]:
    """Run all restaurant POI ingestion pipelines."""
    results = {}
    results["overture"] = ingest_overture_restaurants(db)
    results["osm"] = ingest_osm_restaurants(db)
    results["delivery_platforms"] = ingest_delivery_platforms(db)
    return results


if __name__ == "__main__":
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        results = ingest_all(db)
        print(f"Restaurant POI ingestion complete: {results}")
    finally:
        db.close()
