"""
Google Places Grid Search — systematic Nearby Search across Riyadh.

Lays a grid of points across the Riyadh bounding box and calls
Google Places Nearby Search at each point for specified place types.
Results are deduplicated by place_id and upserted into restaurant_poi.

Purely additive: never deletes existing rows from other sources.
Resumable: tracks completed grid cells in google_places_grid_progress table.

Usage:
    python scripts/google_places_grid_search.py --types restaurant,cafe
    python scripts/google_places_grid_search.py --types restaurant --dry-run
    python scripts/google_places_grid_search.py --types meal_takeaway --resume
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy import text as sa_text

# Ensure repo root is on PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.db.session import SessionLocal
from app.models.tables import RestaurantPOI

logger = logging.getLogger("google_places_grid_search")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RIYADH_BBOX = {
    "min_lat": 24.20,
    "max_lat": 25.10,
    "min_lon": 46.20,
    "max_lon": 47.30,
}

DEFAULT_RADIUS_M = 2000
DEFAULT_GRID_SPACING_FACTOR = 1.4  # spacing = radius * factor (slight overlap)

SUPPORTED_TYPES = ["restaurant", "cafe", "bakery", "meal_takeaway", "meal_delivery"]

# Google Places Nearby Search endpoint (legacy)
NEARBY_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

# Rate limiting
MAX_QPS = 8
MIN_INTERVAL = 1.0 / MAX_QPS
BACKOFF_BASE = 1.5
MAX_RETRIES = 4

# Pagination delay required by Google (next_page_token needs ~2s to activate)
PAGE_TOKEN_DELAY = 2.0

# Category normalization mapping from Google types to our categories
GOOGLE_TYPE_TO_CATEGORY = {
    "restaurant": "international",
    "cafe": "cafe",
    "bakery": "bakery",
    "meal_takeaway": "international",
    "meal_delivery": "international",
    "bar": "international",
    "food": "international",
}

# Progress table
PROGRESS_TABLE = "google_places_grid_progress"

# ---------------------------------------------------------------------------
# Grid generation
# ---------------------------------------------------------------------------


def _meters_to_deg_lat(meters: float) -> float:
    """Convert meters to approximate degrees latitude."""
    return meters / 111_320.0


def _meters_to_deg_lon(meters: float, lat: float) -> float:
    """Convert meters to approximate degrees longitude at given latitude."""
    return meters / (111_320.0 * math.cos(math.radians(lat)))


def generate_grid(
    bbox: dict[str, float],
    radius_m: float,
    spacing_factor: float = DEFAULT_GRID_SPACING_FACTOR,
) -> list[tuple[float, float, str]]:
    """
    Generate a grid of (lat, lon, cell_key) tuples across the bounding box.

    Grid spacing = radius_m * spacing_factor to ensure overlap coverage.
    cell_key is a stable string like "24.2000_46.2000" for resumability.
    """
    mid_lat = (bbox["min_lat"] + bbox["max_lat"]) / 2
    spacing_lat = _meters_to_deg_lat(radius_m * spacing_factor)
    spacing_lon = _meters_to_deg_lon(radius_m * spacing_factor, mid_lat)

    cells = []
    lat = bbox["min_lat"]
    while lat <= bbox["max_lat"]:
        lon = bbox["min_lon"]
        while lon <= bbox["max_lon"]:
            cell_key = f"{lat:.4f}_{lon:.4f}"
            cells.append((lat, lon, cell_key))
            lon += spacing_lon
        lat += spacing_lat

    return cells


# ---------------------------------------------------------------------------
# Progress tracking (resumability)
# ---------------------------------------------------------------------------


def _ensure_progress_table(db) -> None:
    """Create the progress tracking table if it doesn't exist."""
    db.execute(sa_text(f"""
        CREATE TABLE IF NOT EXISTS {PROGRESS_TABLE} (
            cell_key    VARCHAR(32) NOT NULL,
            place_type  VARCHAR(32) NOT NULL,
            results_found INTEGER DEFAULT 0,
            completed_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (cell_key, place_type)
        )
    """))
    db.commit()


def _get_completed_cells(db, place_type: str) -> set[str]:
    """Return set of cell_keys already completed for this place_type."""
    rows = db.execute(
        sa_text(f"SELECT cell_key FROM {PROGRESS_TABLE} WHERE place_type = :pt"),
        {"pt": place_type},
    ).fetchall()
    return {r[0] for r in rows}


def _mark_cell_complete(db, cell_key: str, place_type: str, results_found: int) -> None:
    """Record that a grid cell has been fully processed."""
    db.execute(
        sa_text(f"""
            INSERT INTO {PROGRESS_TABLE} (cell_key, place_type, results_found, completed_at)
            VALUES (:ck, :pt, :rf, now())
            ON CONFLICT (cell_key, place_type) DO UPDATE
            SET results_found = :rf, completed_at = now()
        """),
        {"ck": cell_key, "pt": place_type, "rf": results_found},
    )
    db.commit()


# ---------------------------------------------------------------------------
# Google Places API calls
# ---------------------------------------------------------------------------


class GridSearchClient:
    """Synchronous Google Places Nearby Search client with rate limiting."""

    def __init__(self):
        self._api_key = os.environ.get("GOOGLE_PLACES_API_KEY", "")
        if not self._api_key:
            raise RuntimeError("GOOGLE_PLACES_API_KEY environment variable is not set")
        self._last_request_ts: float = 0.0
        self._client = httpx.Client(timeout=30)
        self.api_calls = 0

    def close(self):
        self._client.close()

    def _rate_limit(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_ts
        if elapsed < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed)
        self._last_request_ts = time.monotonic()

    def _request(self, params: dict[str, Any]) -> dict:
        """Single request with rate limiting and retries."""
        for attempt in range(MAX_RETRIES):
            self._rate_limit()
            self.api_calls += 1
            try:
                resp = self._client.get(NEARBY_SEARCH_URL, params=params)
                if resp.status_code == 429 or resp.status_code >= 500:
                    delay = BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        "Google API %s (attempt %d/%d), retrying in %.1fs",
                        resp.status_code, attempt + 1, MAX_RETRIES, delay,
                    )
                    time.sleep(delay)
                    continue
                resp.raise_for_status()
                return resp.json()
            except httpx.TimeoutException:
                delay = BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    "Google API timeout (attempt %d/%d), retrying in %.1fs",
                    attempt + 1, MAX_RETRIES, delay,
                )
                time.sleep(delay)
        raise RuntimeError(f"Google Places API failed after {MAX_RETRIES} retries")

    def nearby_search(
        self,
        lat: float,
        lon: float,
        radius_m: int,
        place_type: str,
    ) -> list[dict]:
        """
        Nearby Search with automatic pagination.
        Returns up to 60 results (3 pages x 20).
        """
        all_results = []
        params = {
            "location": f"{lat},{lon}",
            "radius": str(radius_m),
            "type": place_type,
            "key": self._api_key,
        }

        page = 1
        while True:
            data = self._request(params)
            status = data.get("status", "")

            if status == "ZERO_RESULTS":
                break
            if status not in ("OK",):
                logger.warning("Nearby Search status=%s at (%.4f, %.4f) type=%s",
                               status, lat, lon, place_type)
                break

            results = data.get("results", [])
            all_results.extend(results)

            next_token = data.get("next_page_token")
            if not next_token or page >= 3:
                break

            # Google requires ~2s before next_page_token becomes valid
            time.sleep(PAGE_TOKEN_DELAY)
            params = {
                "pagetoken": next_token,
                "key": self._api_key,
            }
            page += 1

        return all_results


# ---------------------------------------------------------------------------
# Category normalization
# ---------------------------------------------------------------------------


def _normalize_google_category(types: list[str]) -> str:
    """Map Google place types to our restaurant_poi category."""
    # Priority order: check specific types first
    priority = [
        "cafe", "bakery", "meal_takeaway", "meal_delivery",
        "bar", "restaurant", "food",
    ]
    for t in priority:
        if t in types:
            return GOOGLE_TYPE_TO_CATEGORY.get(t, "international")
    return "international"


# ---------------------------------------------------------------------------
# Upsert logic
# ---------------------------------------------------------------------------


def _upsert_poi(db, place: dict) -> tuple[bool, bool]:
    """
    Upsert a single Google Places result into restaurant_poi.

    Returns (is_new, is_updated).
    ID format: google_places:{place_id}
    Source: google_places

    ADDITIVE ONLY: only touches rows where source = 'google_places'.
    Never modifies rows from other sources.
    """
    place_id = place.get("place_id")
    if not place_id:
        return False, False

    geo = place.get("geometry", {}).get("location", {})
    lat = geo.get("lat")
    lng = geo.get("lng")
    if lat is None or lng is None:
        return False, False

    poi_id = f"google_places:{place_id}"
    name = place.get("name", "Unknown")
    types = place.get("types", [])
    category = _normalize_google_category(types)
    rating = place.get("rating")
    review_count = place.get("user_ratings_total")
    price_level = place.get("price_level")
    business_status = place.get("business_status")
    vicinity = place.get("vicinity", "")

    existing = db.query(RestaurantPOI).filter_by(id=poi_id).first()
    if existing:
        # Update only if we have fresher/better data
        changed = False
        if rating is not None and (existing.rating is None or rating != float(existing.rating)):
            existing.rating = rating
            changed = True
        if review_count is not None and (existing.review_count is None or review_count != existing.review_count):
            existing.review_count = review_count
            changed = True
        if price_level is not None and existing.price_level is None:
            existing.price_level = price_level
            changed = True
        if name and name != "Unknown":
            existing.name = name
            changed = True
        existing.observed_at = datetime.now(timezone.utc)
        return False, changed

    db.add(RestaurantPOI(
        id=poi_id,
        name=name,
        category=category,
        source="google_places",
        lat=lat,
        lon=lng,
        rating=rating,
        review_count=review_count,
        price_level=price_level,
        google_place_id=place_id,
        google_fetched_at=datetime.now(timezone.utc),
        google_confidence=0.95,  # direct Nearby Search = high confidence
        district=vicinity[:128] if vicinity else None,
        raw={
            "types": types,
            "business_status": business_status,
            "vicinity": vicinity,
            "grid_search": True,
        },
        observed_at=datetime.now(timezone.utc),
    ))
    return True, False


# ---------------------------------------------------------------------------
# Main sweep
# ---------------------------------------------------------------------------


def run_grid_search(
    types: list[str],
    radius_m: int = DEFAULT_RADIUS_M,
    dry_run: bool = False,
    resume: bool = True,
) -> dict[str, Any]:
    """
    Execute the grid search across Riyadh for specified place types.

    Returns summary stats dict.
    """
    grid = generate_grid(RIYADH_BBOX, radius_m)
    logger.info("Generated %d grid cells (radius=%dm, bbox=%s)", len(grid), radius_m, RIYADH_BBOX)

    if dry_run:
        est_calls = len(grid) * len(types) * 2  # rough avg 2 pages per cell
        est_cost = est_calls * 0.032
        logger.info("DRY RUN: %d cells x %d types ~ %d API calls ~ $%.0f",
                     len(grid), len(types), est_calls, est_cost)
        return {"dry_run": True, "cells": len(grid), "types": types,
                "est_calls": est_calls, "est_cost_usd": round(est_cost, 2)}

    db = SessionLocal()
    _ensure_progress_table(db)

    client = GridSearchClient()
    seen_place_ids: set[str] = set()

    stats = {
        "total_cells": len(grid) * len(types),
        "cells_skipped": 0,
        "cells_processed": 0,
        "cells_empty": 0,
        "total_results_raw": 0,
        "total_inserted": 0,
        "total_updated": 0,
        "total_deduped": 0,
        "api_calls": 0,
        "by_type": {},
    }

    try:
        for place_type in types:
            logger.info("=== Starting type: %s ===", place_type)

            completed = _get_completed_cells(db, place_type) if resume else set()
            type_stats = {"inserted": 0, "updated": 0, "skipped": 0,
                          "empty": 0, "processed": 0, "raw_results": 0}

            cells_remaining = [(lat, lon, ck) for lat, lon, ck in grid if ck not in completed]
            type_stats["skipped"] = len(grid) - len(cells_remaining)
            stats["cells_skipped"] += type_stats["skipped"]

            if type_stats["skipped"] > 0:
                logger.info("Resuming: skipping %d already-completed cells for type=%s",
                            type_stats["skipped"], place_type)

            for i, (lat, lon, cell_key) in enumerate(cells_remaining):
                results = client.nearby_search(lat, lon, radius_m, place_type)
                type_stats["raw_results"] += len(results)
                type_stats["processed"] += 1

                cell_new = 0
                for place in results:
                    pid = place.get("place_id")
                    if not pid or pid in seen_place_ids:
                        stats["total_deduped"] += 1
                        continue
                    seen_place_ids.add(pid)

                    is_new, is_updated = _upsert_poi(db, place)
                    if is_new:
                        type_stats["inserted"] += 1
                        cell_new += 1
                    elif is_updated:
                        type_stats["updated"] += 1

                if len(results) == 0:
                    type_stats["empty"] += 1

                # Commit and record progress every cell
                db.commit()
                _mark_cell_complete(db, cell_key, place_type, len(results))

                # Log progress every 50 cells
                if (i + 1) % 50 == 0 or (i + 1) == len(cells_remaining):
                    logger.info(
                        "[%s] Cell %d/%d | raw=%d new=%d | API calls=%d | "
                        "Total inserted=%d deduped=%d",
                        place_type, i + 1, len(cells_remaining),
                        len(results), cell_new, client.api_calls,
                        type_stats["inserted"], stats["total_deduped"],
                    )

            stats["cells_processed"] += type_stats["processed"]
            stats["cells_empty"] += type_stats["empty"]
            stats["total_results_raw"] += type_stats["raw_results"]
            stats["total_inserted"] += type_stats["inserted"]
            stats["total_updated"] += type_stats["updated"]
            stats["by_type"][place_type] = type_stats

            logger.info(
                "=== Completed type: %s | processed=%d empty=%d inserted=%d updated=%d ===",
                place_type, type_stats["processed"], type_stats["empty"],
                type_stats["inserted"], type_stats["updated"],
            )

    finally:
        stats["api_calls"] = client.api_calls
        stats["est_cost_usd"] = round(client.api_calls * 0.032, 2)
        client.close()
        db.close()

    logger.info("=" * 60)
    logger.info("GRID SEARCH COMPLETE")
    logger.info("  Cells processed: %d (skipped: %d, empty: %d)",
                stats["cells_processed"], stats["cells_skipped"], stats["cells_empty"])
    logger.info("  Raw results: %d -> Inserted: %d, Updated: %d, Deduped: %d",
                stats["total_results_raw"], stats["total_inserted"],
                stats["total_updated"], stats["total_deduped"])
    logger.info("  API calls: %d | Est. cost: $%.2f",
                stats["api_calls"], stats["est_cost_usd"])
    logger.info("=" * 60)

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Google Places Grid Search for Riyadh")
    parser.add_argument(
        "--types",
        default="restaurant",
        help="Comma-separated place types (default: restaurant). "
             f"Supported: {', '.join(SUPPORTED_TYPES)}",
    )
    parser.add_argument(
        "--radius",
        type=int,
        default=DEFAULT_RADIUS_M,
        help=f"Search radius in meters (default: {DEFAULT_RADIUS_M})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print grid stats and estimated cost without making API calls",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore previous progress and re-process all cells",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    types = [t.strip() for t in args.types.split(",")]
    for t in types:
        if t not in SUPPORTED_TYPES:
            logger.error("Unsupported type: %s. Supported: %s", t, SUPPORTED_TYPES)
            sys.exit(1)

    stats = run_grid_search(
        types=types,
        radius_m=args.radius,
        dry_run=args.dry_run,
        resume=not args.no_resume,
    )

    if args.dry_run:
        print(f"\nDry run summary:")
        print(f"  Grid cells: {stats['cells']}")
        print(f"  Types: {stats['types']}")
        print(f"  Est. API calls: {stats['est_calls']}")
        print(f"  Est. cost: ${stats['est_cost_usd']}")


if __name__ == "__main__":
    main()
