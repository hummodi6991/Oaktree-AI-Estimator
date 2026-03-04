"""
Google Reviews enrichment pipeline for restaurant POIs.

Enriches ``restaurant_poi`` rows with Google Places data:
- rating, review_count (user_ratings_total), price_level
- google_place_id, google_fetched_at, google_confidence

Usage::

    python -m app.ingest.google_reviews_enrich [--limit N] [--force] [--no-only-missing]

Safe to re-run: skips rows already enriched within the last 30 days
unless ``--force`` is passed.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from app.connectors.google_places import (
    clear_caches,
    find_place_candidates,
    get_place_details,
    pick_best_candidate,
)
from app.models.tables import RestaurantPOI

logger = logging.getLogger(__name__)

# Riyadh bounding box
RIYADH_LON_MIN, RIYADH_LON_MAX = 46.20, 47.30
RIYADH_LAT_MIN, RIYADH_LAT_MAX = 24.20, 25.10

STALE_DAYS = 30
BATCH_SIZE = 300


def _build_query(
    db: Session,
    *,
    only_missing: bool = True,
    force: bool = False,
    limit: int | None = None,
):
    """
    Return an ORM query for restaurant_poi rows that need enrichment.

    Filters:
    - Within Riyadh bbox.
    - If only_missing: where review_count IS NULL.
    - If not force: where google_fetched_at IS NULL or older than STALE_DAYS.
    """
    q = (
        db.query(RestaurantPOI)
        .filter(
            RestaurantPOI.lat >= RIYADH_LAT_MIN,
            RestaurantPOI.lat <= RIYADH_LAT_MAX,
            RestaurantPOI.lon >= RIYADH_LON_MIN,
            RestaurantPOI.lon <= RIYADH_LON_MAX,
        )
    )

    if not force:
        stale_cutoff = datetime.now(timezone.utc) - timedelta(days=STALE_DAYS)
        q = q.filter(
            (RestaurantPOI.google_fetched_at.is_(None))
            | (RestaurantPOI.google_fetched_at < stale_cutoff)
        )

    if only_missing:
        q = q.filter(RestaurantPOI.review_count.is_(None))

    q = q.order_by(RestaurantPOI.id)

    if limit:
        q = q.limit(limit)

    return q


def enrich_batch(
    db: Session,
    rows: list[RestaurantPOI],
    stats: dict[str, int],
) -> None:
    """
    Enrich a batch of RestaurantPOI rows with Google Places data.
    Updates stats dict in place.
    """
    for poi in rows:
        stats["processed"] += 1

        name = poi.name or ""
        lat = float(poi.lat)
        lon = float(poi.lon)

        # Find candidates
        try:
            candidates = find_place_candidates(name, lat, lon)
            stats["api_calls"] += 1
        except Exception as exc:
            logger.warning("API error for POI %s: %s", poi.id, exc)
            stats["errors"] += 1
            continue

        if not candidates:
            stats["skipped_no_match"] += 1
            logger.debug("No candidates for %s (%s)", poi.id, name)
            continue

        # Pick best match
        best, confidence = pick_best_candidate(name, lat, lon, candidates)

        if best is None:
            stats["skipped_low_conf"] += 1
            logger.debug("Low confidence for %s (%s)", poi.id, name)
            continue

        # Fetch details for richer data
        place_id = best["place_id"]
        try:
            details = get_place_details(place_id)
            stats["api_calls"] += 1
        except Exception as exc:
            logger.warning("Details API error for %s: %s", place_id, exc)
            # Fall back to candidate data from text search
            details = best

        now = datetime.now(timezone.utc)

        # Update the POI row
        poi.rating = details.get("rating") or best.get("rating")
        poi.review_count = details.get("user_ratings_total") or best.get("user_ratings_total")
        poi.price_level = details.get("price_level") or best.get("price_level")
        poi.google_place_id = place_id
        poi.google_fetched_at = now
        poi.google_confidence = confidence

        # Merge Google data into the raw JSONB column
        existing_raw = poi.raw or {}
        existing_raw["google"] = {
            "place_id": place_id,
            "name": details.get("name", best.get("name")),
            "rating": details.get("rating"),
            "user_ratings_total": details.get("user_ratings_total"),
            "price_level": details.get("price_level"),
            "types": details.get("types", best.get("types", [])),
            "formatted_address": details.get("formatted_address", ""),
            "confidence": confidence,
            "fetched_at": now.isoformat(),
        }
        poi.raw = existing_raw

        stats["updated"] += 1

    db.commit()


def run(
    *,
    limit: int | None = None,
    only_missing: bool = True,
    force: bool = False,
) -> dict[str, int]:
    """
    Main entry point.  Enriches restaurant_poi rows in batches.
    Returns a stats dict.
    """
    from app.db.session import SessionLocal

    db = SessionLocal()
    stats: dict[str, int] = {
        "processed": 0,
        "updated": 0,
        "skipped_low_conf": 0,
        "skipped_no_match": 0,
        "api_calls": 0,
        "errors": 0,
    }

    try:
        clear_caches()

        q = _build_query(db, only_missing=only_missing, force=force, limit=limit)
        total = q.count()
        logger.info(
            "Google Reviews enrichment: %d rows to process (limit=%s, force=%s, only_missing=%s)",
            total, limit, force, only_missing,
        )

        offset = 0
        while True:
            batch = (
                _build_query(db, only_missing=only_missing, force=force, limit=limit)
                .offset(offset)
                .limit(BATCH_SIZE)
                .all()
            )
            if not batch:
                break

            enrich_batch(db, batch, stats)
            offset += BATCH_SIZE

            logger.info(
                "Progress: processed=%d updated=%d skipped_low_conf=%d "
                "skipped_no_match=%d api_calls=%d errors=%d",
                stats["processed"],
                stats["updated"],
                stats["skipped_low_conf"],
                stats["skipped_no_match"],
                stats["api_calls"],
                stats["errors"],
            )

    finally:
        db.close()

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich restaurant_poi rows with Google Places data.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max rows to process (for testing).",
    )
    parser.add_argument(
        "--only-missing", action="store_true", default=True,
        help="Only enrich rows with NULL review_count (default: true).",
    )
    parser.add_argument(
        "--no-only-missing", dest="only_missing", action="store_false",
        help="Include rows that already have review_count.",
    )
    parser.add_argument(
        "--force", action="store_true", default=False,
        help="Re-enrich even if google_fetched_at is recent.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s %(message)s",
    )

    stats = run(
        limit=args.limit,
        only_missing=args.only_missing,
        force=args.force,
    )

    print(f"\nGoogle Reviews enrichment complete:")
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
