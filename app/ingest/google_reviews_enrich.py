"""
Google Reviews enrichment pipeline for restaurant POIs.

Enriches ``restaurant_poi`` rows with Google Places data:
- rating, review_count (user_ratings_total), price_level
- google_place_id, google_fetched_at, google_confidence

Features:
- Resumable: uses ``google_reviews_enrich_state`` cursor table so reruns
  continue where they left off, even after timeouts.
- Async + rate-limited: uses httpx.AsyncClient with semaphore concurrency
  and token-bucket QPS control.
- Optimized API calls: skips Place Details when Text Search already
  provides rating + review_count.
- Multi-variant query strategy: tries name, name_ar, normalized name,
  name + "Riyadh", and category-aware type filters to maximize matches.
- Fast path: if a POI already has google_place_id, skips Text Search
  and only refreshes via Place Details when stale or --force.

Usage::

    python -m app.ingest.google_reviews_enrich [--resume] [--reset] \\
        [--batch-size 200] [--limit N] [--force]

Safe to re-run: skips rows already enriched within the last 30 days
unless ``--force`` is passed.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from app.connectors.google_places_async import (
    AsyncGooglePlacesClient,
    _build_name_variants,
    candidate_has_full_data,
    clear_caches,
    pick_best_candidate,
)
from app.models.tables import RestaurantPOI

logger = logging.getLogger(__name__)

# Riyadh bounding box
RIYADH_LON_MIN, RIYADH_LON_MAX = 46.20, 47.30
RIYADH_LAT_MIN, RIYADH_LAT_MAX = 24.20, 25.10

STALE_DAYS = 30
DEFAULT_BATCH_SIZE = 200


# ---------------------------------------------------------------------------
# Cursor helpers
# ---------------------------------------------------------------------------


def _get_cursor(db: Session) -> str | None:
    """Read last_cursor from the singleton state row."""
    row = db.execute(
        sa_text("SELECT last_cursor FROM google_reviews_enrich_state WHERE id = 1")
    ).first()
    return row[0] if row else None


def _set_cursor(db: Session, cursor: str | None) -> None:
    """Upsert the cursor value."""
    db.execute(
        sa_text(
            "INSERT INTO google_reviews_enrich_state (id, last_cursor, updated_at) "
            "VALUES (1, :cursor, now()) "
            "ON CONFLICT (id) DO UPDATE SET last_cursor = :cursor, updated_at = now()"
        ),
        {"cursor": cursor},
    )
    db.commit()


# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------


def _fetch_batch(
    db: Session,
    *,
    cursor: str | None,
    force: bool,
    only_missing: bool,
    batch_size: int,
) -> list[RestaurantPOI]:
    """
    Fetch next batch of restaurant_poi rows after cursor.

    Filters:
    - Within Riyadh bbox
    - If only_missing: only rows with NULL review_count or NULL google_place_id
    - If not force: only rows with NULL or stale google_fetched_at
    - Ordered by id for stable cursor pagination
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

    if only_missing:
        q = q.filter(
            (RestaurantPOI.review_count.is_(None))
            | (RestaurantPOI.google_place_id.is_(None))
        )

    if not force:
        stale_cutoff = datetime.now(timezone.utc) - timedelta(days=STALE_DAYS)
        q = q.filter(
            (RestaurantPOI.google_fetched_at.is_(None))
            | (RestaurantPOI.google_fetched_at < stale_cutoff)
        )

    if cursor:
        q = q.filter(RestaurantPOI.id > cursor)

    q = q.order_by(RestaurantPOI.id).limit(batch_size)
    return q.all()


# ---------------------------------------------------------------------------
# Single-POI enrichment (async)
# ---------------------------------------------------------------------------


async def _enrich_one(
    poi: RestaurantPOI,
    client: AsyncGooglePlacesClient,
    force: bool,
) -> tuple[str, dict]:
    """
    Enrich a single POI. Returns a tuple of (status, fallback_info):
    - status: 'updated', 'skipped_existing', 'skipped_low_conf', 'no_match',
              'error', 'details_refreshed'
    - fallback_info: dict with query strategy details (empty for non-search paths)
    """
    name = poi.name or ""
    lat = float(poi.lat)
    lon = float(poi.lon)
    now = datetime.now(timezone.utc)
    empty_fallback: dict = {}

    # Fast path: already has google_place_id and not stale -> skip
    if poi.google_place_id and not force:
        if poi.google_fetched_at and poi.google_fetched_at > now - timedelta(days=STALE_DAYS):
            return "skipped_existing", empty_fallback

    # Fast path: has google_place_id, refresh via Details only (stale or force)
    if poi.google_place_id:
        try:
            details = await client.get_place_details(poi.google_place_id)
        except Exception as exc:
            logger.warning("Details refresh error for %s: %s", poi.id, exc)
            return "error", empty_fallback

        if details:
            poi.rating = details.get("rating") or poi.rating
            poi.review_count = details.get("user_ratings_total") or poi.review_count
            poi.price_level = details.get("price_level") or poi.price_level
            poi.google_fetched_at = now
            existing_raw = poi.raw or {}
            existing_raw["google"] = {
                "place_id": poi.google_place_id,
                "name": details.get("name", name),
                "rating": details.get("rating"),
                "user_ratings_total": details.get("user_ratings_total"),
                "price_level": details.get("price_level"),
                "types": details.get("types", []),
                "formatted_address": details.get("formatted_address", ""),
                "confidence": float(poi.google_confidence or 0),
                "fetched_at": now.isoformat(),
            }
            poi.raw = existing_raw
            return "details_refreshed", empty_fallback
        return "error", empty_fallback

    # Normal path: Multi-variant Text Search -> pick best -> optionally Details
    name_ar = poi.name_ar if hasattr(poi, "name_ar") else None
    category = poi.category if hasattr(poi, "category") else None
    name_variants = _build_name_variants(name, name_ar)

    try:
        candidates, fallback_info = await client.find_candidates_multi(
            name_variants, lat, lon, category=category,
        )
    except Exception as exc:
        logger.warning("API error for POI %s: %s", poi.id, exc)
        return "error", empty_fallback

    if not candidates:
        return "no_match", fallback_info

    radius_escalated = fallback_info.get("radius_escalated", False)
    best, confidence = pick_best_candidate(
        name, lat, lon, candidates,
        category=category,
        radius_escalated=radius_escalated,
    )

    if best is None:
        return "skipped_low_conf", fallback_info

    place_id = best["place_id"]

    # Optimization: skip Details if Text Search already has rating + review_count
    if candidate_has_full_data(best):
        details = best
    else:
        # Fallback: call Details only when user_ratings_total is missing
        try:
            details = await client.get_place_details(place_id)
        except Exception as exc:
            logger.warning("Details API error for %s: %s", place_id, exc)
            details = best  # fall back to text search data

    # Update the POI row — only set google_place_id/google_fetched_at on match
    poi.rating = details.get("rating") or best.get("rating")
    poi.review_count = details.get("user_ratings_total") or best.get("user_ratings_total")
    poi.price_level = details.get("price_level") or best.get("price_level")
    poi.google_place_id = place_id
    poi.google_fetched_at = now
    poi.google_confidence = confidence

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
        "fallback_variant": fallback_info.get("variant_used"),
        "fallback_type": fallback_info.get("type_used"),
        "query_attempts": fallback_info.get("attempts", 1),
    }
    poi.raw = existing_raw

    return "updated", fallback_info


# ---------------------------------------------------------------------------
# Batch enrichment (async)
# ---------------------------------------------------------------------------


async def _enrich_batch_async(
    db: Session,
    rows: list[RestaurantPOI],
    client: AsyncGooglePlacesClient,
    force: bool,
) -> dict[str, int]:
    """Enrich a batch concurrently and return per-status counts."""
    counts: dict[str, int] = {
        "processed": 0,
        "updated": 0,
        "details_refreshed": 0,
        "skipped_existing": 0,
        "skipped_low_conf": 0,
        "no_match": 0,
        "error": 0,
        # Fallback stats
        "fallback_used_name_ar": 0,
        "fallback_used_riyadh_suffix": 0,
        "fallback_removed_type": 0,
        "fallback_radius_escalated": 0,
        "total_query_attempts": 0,
    }

    tasks = [_enrich_one(poi, client, force) for poi in rows]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for poi, result in zip(rows, results):
        counts["processed"] += 1
        if isinstance(result, Exception):
            logger.warning("Unhandled error for POI %s: %s", poi.id, result)
            counts["error"] += 1
        else:
            status, fallback_info = result
            counts[status] = counts.get(status, 0) + 1
            # Accumulate fallback stats
            if fallback_info:
                counts["total_query_attempts"] += fallback_info.get("attempts", 1)
                if fallback_info.get("used_name_ar"):
                    counts["fallback_used_name_ar"] += 1
                if fallback_info.get("used_riyadh_suffix"):
                    counts["fallback_used_riyadh_suffix"] += 1
                if fallback_info.get("removed_type"):
                    counts["fallback_removed_type"] += 1
                if fallback_info.get("radius_escalated"):
                    counts["fallback_radius_escalated"] += 1

    db.commit()
    return counts


# ---------------------------------------------------------------------------
# Main run loop
# ---------------------------------------------------------------------------


async def run_async(
    *,
    limit: int | None = None,
    force: bool = False,
    only_missing: bool = True,
    resume: bool = True,
    reset: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, int]:
    """
    Main entry point. Enriches restaurant_poi rows using cursor-based
    batching with async concurrency.
    """
    from app.db.session import SessionLocal

    db = SessionLocal()
    stats: dict[str, int] = {
        "processed": 0,
        "updated": 0,
        "details_refreshed": 0,
        "skipped_existing": 0,
        "skipped_low_conf": 0,
        "no_match": 0,
        "error": 0,
        "api_calls": 0,
        # Fallback stats
        "fallback_used_name_ar": 0,
        "fallback_used_riyadh_suffix": 0,
        "fallback_removed_type": 0,
        "fallback_radius_escalated": 0,
        "total_query_attempts": 0,
    }
    cursor: str | None = None
    start_time = time.monotonic()

    try:
        clear_caches()

        # Handle cursor state
        if reset:
            _set_cursor(db, None)
            logger.info("Cursor reset to NULL")
        elif resume:
            cursor = _get_cursor(db)
            if cursor:
                logger.info("Resuming from cursor: %s", cursor)

        total_processed = 0

        async with AsyncGooglePlacesClient() as client:
            while True:
                batch = _fetch_batch(
                    db,
                    cursor=cursor,
                    force=force,
                    only_missing=only_missing,
                    batch_size=batch_size,
                )
                if not batch:
                    break

                batch_counts = await _enrich_batch_async(db, batch, client, force)

                # Accumulate stats
                for k, v in batch_counts.items():
                    stats[k] = stats.get(k, 0) + v

                # Update cursor to max id in batch
                cursor = batch[-1].id
                _set_cursor(db, cursor)

                total_processed += batch_counts["processed"]
                stats["api_calls"] = client.api_calls
                elapsed = time.monotonic() - start_time

                logger.info(
                    "Progress: processed=%d updated=%d refreshed=%d "
                    "skipped_existing=%d skipped_low_conf=%d no_match=%d "
                    "errors=%d api_calls=%d cursor=%s elapsed=%.0fs | "
                    "fallbacks: name_ar=%d riyadh=%d no_type=%d radius_up=%d "
                    "avg_attempts=%.1f",
                    stats["processed"],
                    stats["updated"],
                    stats["details_refreshed"],
                    stats["skipped_existing"],
                    stats["skipped_low_conf"],
                    stats["no_match"],
                    stats["error"],
                    stats["api_calls"],
                    cursor,
                    elapsed,
                    stats["fallback_used_name_ar"],
                    stats["fallback_used_riyadh_suffix"],
                    stats["fallback_removed_type"],
                    stats["fallback_radius_escalated"],
                    (stats["total_query_attempts"] / max(stats["processed"], 1)),
                )

                if limit and total_processed >= limit:
                    logger.info("Reached limit of %d rows", limit)
                    break

        stats["api_calls"] = client.api_calls

    finally:
        elapsed = time.monotonic() - start_time
        stats["elapsed_seconds"] = int(elapsed)
        stats["cursor"] = cursor  # type: ignore[assignment]
        db.close()

    return stats


def run(
    *,
    limit: int | None = None,
    force: bool = False,
    only_missing: bool = True,
    resume: bool = True,
    reset: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, int]:
    """Sync wrapper around run_async for backwards compatibility."""
    return asyncio.run(
        run_async(
            limit=limit,
            force=force,
            only_missing=only_missing,
            resume=resume,
            reset=reset,
            batch_size=batch_size,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich restaurant_poi rows with Google Places data.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max rows to process (for testing).",
    )
    parser.add_argument(
        "--force", action="store_true", default=False,
        help="Re-enrich even if google_fetched_at is recent.",
    )
    parser.add_argument(
        "--resume", action="store_true", default=True,
        help="Resume from last cursor (default: true).",
    )
    parser.add_argument(
        "--no-resume", dest="resume", action="store_false",
        help="Start from the beginning, ignoring saved cursor.",
    )
    parser.add_argument(
        "--reset", action="store_true", default=False,
        help="Reset cursor to NULL before starting.",
    )
    parser.add_argument(
        "--only-missing", action="store_true", default=True,
        help="Only enrich rows with NULL review_count or google_place_id (default: true).",
    )
    parser.add_argument(
        "--no-only-missing", dest="only_missing", action="store_false",
        help="Enrich all rows, not just those missing review data.",
    )
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"Rows per batch (default: {DEFAULT_BATCH_SIZE}).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s %(message)s",
    )

    stats = run(
        limit=args.limit,
        force=args.force,
        only_missing=args.only_missing,
        resume=args.resume,
        reset=args.reset,
        batch_size=args.batch_size,
    )

    print("\nGoogle Reviews enrichment complete:")
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
