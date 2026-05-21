"""
Ingestion pipeline for restaurant POI data from multiple sources.

Sources:
- Overture Maps Places (DuckDB -> S3)
- OSM (Overpass API)
- Delivery platforms (16 platforms via SCRAPER_REGISTRY)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy.orm import Session

from app.models.tables import RestaurantPOI
from app.services.restaurant_categories import (
    normalize_category,
    normalize_osm_cuisine,
    normalize_overture_taxonomy,
)

logger = logging.getLogger(__name__)

# Filter constants shared with the legacy Stage 2 block in
# ``ingest_delivery_platforms``. Keep these in lock-step with that block; this
# is intentionally the same allow-list so the decoupled upsert produces the
# same row set the original code would have.
_DELIVERY_POI_GEOCODE_ALLOWLIST = ("platform_payload", "json_ld", "address_geocode")
_DELIVERY_POI_MIN_LOCATION_CONFIDENCE = 0.7
_DEFAULT_DELIVERY_RUN_STATUSES = ("completed", "completed_with_errors")


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

    from app.connectors.delivery_platforms import _UA

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
        # Overpass returns 406 Not Acceptable when the default httpx
        # ``Accept: */*`` is sent without a UA; explicit headers fix it.
        r = httpx.post(
            overpass_url,
            data={"data": query},
            headers={"User-Agent": _UA, "Accept": "application/json"},
            timeout=180,
        )
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


def ingest_delivery_platforms(
    db: Session | None = None,
    sources: list[str] | None = None,
) -> int:
    """
    Ingest restaurant POIs from delivery platform scrapers.

    This function runs the delivery pipeline which:
    1. Stores ALL records into delivery_source_record (even without coords)
    2. Runs location resolution to recover coordinates where possible
    3. Runs entity resolution to match against existing POIs
    4. Only upserts into restaurant_poi when location confidence is adequate

    The raw delivery records are always preserved for analytics even when
    they cannot be placed into restaurant_poi.

    The ``db`` parameter is accepted for API compatibility but the pipeline
    manages its own sessions per-platform for isolation.
    """
    from app.connectors.delivery_platforms import SCRAPER_REGISTRY
    from app.db.session import SessionLocal
    from app.delivery.pipeline import run_all_platforms
    from app.delivery.models import DeliverySourceRecord

    # Run the new pipeline (stores raw records + resolves)
    target_platforms = sources or list(SCRAPER_REGISTRY.keys())
    results = run_all_platforms(
        platforms=target_platforms, run_resolver=True,
    )
    total_inserted = sum(r.get("rows_inserted", 0) for r in results)
    logger.info("Delivery pipeline stored %d raw records", total_inserted)

    # Collect run IDs from this invocation so we only upsert fresh rows
    run_ids = [r["run_id"] for r in results if r.get("run_id")]
    if not run_ids:
        return 0

    # Only upsert rows from *this* run with first-party coordinates.
    # Uses a fresh session for the POI upsert phase.
    poi_db = db if db is not None else SessionLocal()
    n = 0
    try:
        resolved_rows = (
            poi_db.query(DeliverySourceRecord)
            .filter(
                DeliverySourceRecord.ingest_run_id.in_(run_ids),
                DeliverySourceRecord.lat.isnot(None),
                DeliverySourceRecord.lon.isnot(None),
                DeliverySourceRecord.location_confidence
                >= _DELIVERY_POI_MIN_LOCATION_CONFIDENCE,
                DeliverySourceRecord.geocode_method.in_(
                    _DELIVERY_POI_GEOCODE_ALLOWLIST
                ),
            )
            .all()
        )
        for row in resolved_rows:
            category = normalize_category(row.cuisine_raw or row.category_raw)
            poi = {
                "id": f"{row.platform}:{row.source_listing_id or row.id}",
                "name": row.restaurant_name_normalized or row.restaurant_name_raw or "Unknown",
                "category": category,
                "source": row.platform,
                "lat": float(row.lat),
                "lon": float(row.lon),
                "chain_name": row.brand_raw,
                "district": row.district_text,
                "rating": float(row.rating) if row.rating else None,
                "review_count": row.rating_count,
                "raw": {
                    "source_url": row.source_url,
                    "delivery_pipeline": True,
                    "geocode_method": row.geocode_method,
                    "location_confidence": row.location_confidence,
                },
            }
            _upsert_poi(poi_db, poi)
            n += 1

        poi_db.commit()
    except Exception:
        try:
            poi_db.rollback()
        except Exception:
            pass
        raise
    finally:
        if db is None:
            poi_db.close()

    logger.info(
        "Ingested %d delivery platform restaurant POIs (from %d raw records)",
        n, total_inserted,
    )
    return n


def upsert_restaurant_poi_from_delivery(
    db: Session | None = None,
    *,
    since: datetime | None = None,
    platforms: Iterable[str] | None = None,
    statuses: Iterable[str] = _DEFAULT_DELIVERY_RUN_STATUSES,
) -> int:
    """Upsert restaurant_poi rows from existing delivery_source_record rows.

    Independent of the live scrape. Reads the most recent delivery_ingest_run
    rows (filtered by status and optionally by platforms / since), then
    upserts the corresponding delivery_source_record rows into restaurant_poi
    using the same filter conditions as the legacy Stage 2 block in
    ``ingest_delivery_platforms``.

    Args:
        db: Optional existing session. If None, opens its own SessionLocal()
            and closes it before returning.
        since: Only consider delivery_ingest_run rows with finished_at >= since.
            If None, takes the most recent completed run per platform.
        platforms: Restrict to these platform names. If None, all platforms
            with at least one matching run.
        statuses: delivery_ingest_run.status values to include. Defaults to
            successful and partial-success runs.

    Returns:
        Number of restaurant_poi rows upserted (count of _upsert_poi calls).
    """
    from sqlalchemy import func

    from app.db.session import SessionLocal
    from app.delivery.models import DeliveryIngestRun, DeliverySourceRecord

    platforms_list = list(platforms) if platforms is not None else None
    statuses_list = list(statuses)

    own_session = db is None
    if own_session:
        db = SessionLocal()

    try:
        # ── Run selection ────────────────────────────────────────────────
        if since is not None:
            # All runs with finished_at >= since matching status/platforms.
            run_query = db.query(
                DeliveryIngestRun.id,
                DeliveryIngestRun.platform,
                DeliveryIngestRun.finished_at,
            ).filter(
                DeliveryIngestRun.finished_at.isnot(None),
                DeliveryIngestRun.finished_at >= since,
                DeliveryIngestRun.status.in_(statuses_list),
            )
            if platforms_list:
                run_query = run_query.filter(
                    DeliveryIngestRun.platform.in_(platforms_list)
                )
            runs = run_query.all()
        else:
            # Most recent finished_at per platform (semantically equivalent to
            # ``DISTINCT ON (platform) ... ORDER BY platform, finished_at DESC``
            # but expressed portably as GROUP BY + self-join so the function
            # also runs under SQLite in tests).
            latest_subq = db.query(
                DeliveryIngestRun.platform.label("platform"),
                func.max(DeliveryIngestRun.finished_at).label("max_finished"),
            ).filter(
                DeliveryIngestRun.finished_at.isnot(None),
                DeliveryIngestRun.status.in_(statuses_list),
            )
            if platforms_list:
                latest_subq = latest_subq.filter(
                    DeliveryIngestRun.platform.in_(platforms_list)
                )
            latest_subq = latest_subq.group_by(DeliveryIngestRun.platform).subquery()

            runs = (
                db.query(
                    DeliveryIngestRun.id,
                    DeliveryIngestRun.platform,
                    DeliveryIngestRun.finished_at,
                )
                .join(
                    latest_subq,
                    (DeliveryIngestRun.platform == latest_subq.c.platform)
                    & (DeliveryIngestRun.finished_at == latest_subq.c.max_finished),
                )
                .filter(DeliveryIngestRun.status.in_(statuses_list))
                .all()
            )

        run_ids = [r[0] for r in runs]

        if not run_ids:
            logger.warning(
                "upsert_restaurant_poi_from_delivery: no delivery_ingest_run rows "
                "matched filter (since=%s, platforms=%s, statuses=%s); "
                "nothing to upsert",
                since, platforms_list, statuses_list,
            )
            return 0

        logger.info(
            "upsert_restaurant_poi_from_delivery: %d run(s) selected "
            "(since=%s, platforms=%s, statuses=%s)",
            len(run_ids), since, platforms_list, statuses_list,
        )
        for run_id, platform, finished_at in runs:
            logger.debug(
                "  run_id=%s platform=%s finished_at=%s",
                run_id, platform, finished_at,
            )

        # ── Row selection — identical filter to legacy Stage 2 ──────────
        resolved_rows = (
            db.query(DeliverySourceRecord)
            .filter(
                DeliverySourceRecord.ingest_run_id.in_(run_ids),
                DeliverySourceRecord.lat.isnot(None),
                DeliverySourceRecord.lon.isnot(None),
                DeliverySourceRecord.location_confidence
                >= _DELIVERY_POI_MIN_LOCATION_CONFIDENCE,
                DeliverySourceRecord.geocode_method.in_(
                    _DELIVERY_POI_GEOCODE_ALLOWLIST
                ),
            )
            .all()
        )

        n = 0
        for row in resolved_rows:
            category = normalize_category(row.cuisine_raw or row.category_raw)
            poi = {
                "id": f"{row.platform}:{row.source_listing_id or row.id}",
                "name": (
                    row.restaurant_name_normalized
                    or row.restaurant_name_raw
                    or "Unknown"
                ),
                "category": category,
                "source": row.platform,
                "lat": float(row.lat),
                "lon": float(row.lon),
                "chain_name": row.brand_raw,
                "district": row.district_text,
                "rating": float(row.rating) if row.rating else None,
                "review_count": row.rating_count,
                "raw": {
                    "source_url": row.source_url,
                    "delivery_pipeline": True,
                    "geocode_method": row.geocode_method,
                    "location_confidence": row.location_confidence,
                },
            }
            _upsert_poi(db, poi)
            n += 1

        db.commit()
        logger.info(
            "upsert_restaurant_poi_from_delivery: upserted %d restaurant_poi "
            "row(s) from %d delivery_source_record row(s) across %d run(s)",
            n, len(resolved_rows), len(run_ids),
        )
        return n
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        if own_session:
            db.close()


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
