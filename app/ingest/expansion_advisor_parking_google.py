"""Expansion Advisor — Google Places parking ingest.

Grid-driven ingestion that queries Google Places Nearby Search at uniformly
spaced points across built-up Riyadh and writes resulting parking POIs into
the existing ``expansion_parking_asset`` table with ``source='google_places'``.

Design choices (see ``cc_investigate_google_parking_ingest.md`` recon):
- Single unified ``expansion_parking_asset`` table; no new schema.
- Cap at 20 results per query (Nearby Search single-page limit), no
  ``next_page_token`` pagination.
- App-level dedup on ``place_id`` via in-process set.
- Resumability: JSON checkpoint at ``/tmp/google_parking_checkpoint.json``;
  flushed every 100 cells; deleted on successful completion.
- Honest defaults for Google-unknown columns: ``amenity_type='unknown'``,
  ``capacity=NULL``, ``covered=NULL``, ``public_access=NULL``.
- Flat ``walk_access_score=65.0`` / ``dropoff_score=55.0`` matching the
  OSM ingest's ``ELSE`` branches.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
from typing import Any

from sqlalchemy import text

from app.connectors.google_places_async import AsyncGooglePlacesClient
from app.ingest.expansion_advisor_common import (
    get_session,
    log_table_counts,
    validate_db_env,
    write_stats,
)

logger = logging.getLogger("expansion_advisor.parking_google")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Tightened bounding box covering built-up Riyadh (avoids the wide desert
# fringe of the configured RIYADH_BBOX).
RIYADH_BUILT_UP_BBOX: dict[str, float] = {
    "min_lon": 46.55,
    "min_lat": 24.55,
    "max_lon": 46.95,
    "max_lat": 24.85,
}

SPACING_M = 500
RADIUS_M = 350
NEARBY_PLACE_TYPE = "parking"

DEFAULT_CHECKPOINT_PATH = "/tmp/google_parking_checkpoint.json"
CHECKPOINT_FLUSH_INTERVAL = 100
PROGRESS_LOG_INTERVAL = 500

NAME_MAX_LEN = 256


INSERT_SQL = text(
    """
    INSERT INTO expansion_parking_asset (
        city, source, name, amenity_type, geom, capacity, covered,
        public_access, walk_access_score, dropoff_score
    ) VALUES (
        :city, 'google_places', :name, 'unknown',
        ST_SetSRID(ST_MakePoint(:lon, :lat), 4326),
        NULL, NULL, NULL, 65.0, 55.0
    )
    """
)


# ---------------------------------------------------------------------------
# Grid generation (cosine-corrected, mirrors scripts/google_places_grid_search.py)
# ---------------------------------------------------------------------------

def _meters_to_deg_lat(meters: float) -> float:
    return meters / 111_320.0


def _meters_to_deg_lon(meters: float, lat: float) -> float:
    return meters / (111_320.0 * math.cos(math.radians(lat)))


def generate_grid_points(
    bbox: dict[str, float],
    spacing_m: float,
) -> list[tuple[float, float, str]]:
    """Return ``(lat, lon, cell_key)`` tuples tiling *bbox* at *spacing_m*.

    Uses a single mid-latitude longitude step so the grid is rectangular
    in degree-space. ``cell_key`` is stable across reruns for checkpoint
    resumability.
    """
    mid_lat = (bbox["min_lat"] + bbox["max_lat"]) / 2
    step_lat = _meters_to_deg_lat(spacing_m)
    step_lon = _meters_to_deg_lon(spacing_m, mid_lat)

    cells: list[tuple[float, float, str]] = []
    lat = bbox["min_lat"]
    while lat <= bbox["max_lat"]:
        lon = bbox["min_lon"]
        while lon <= bbox["max_lon"]:
            cell_key = f"{lat:.4f}_{lon:.4f}"
            cells.append((lat, lon, cell_key))
            lon += step_lon
        lat += step_lat
    return cells


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def load_checkpoint(path: str) -> set[str]:
    """Return the set of completed cell_keys from *path*, or empty set."""
    if not os.path.exists(path):
        return set()
    try:
        with open(path) as f:
            data = json.load(f)
        return set(data.get("completed_cells", []))
    except Exception:
        logger.warning("Failed to load checkpoint at %s, starting fresh", path, exc_info=True)
        return set()


def save_checkpoint(path: str, completed_cells: set[str]) -> None:
    """Atomically write *completed_cells* to *path* as JSON."""
    payload = {"completed_cells": sorted(completed_cells)}
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f)
    os.replace(tmp, path)


def delete_checkpoint(path: str) -> None:
    """Remove the checkpoint file if present; tolerate failure."""
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        logger.warning("Failed to delete checkpoint at %s", path, exc_info=True)


# ---------------------------------------------------------------------------
# Per-cell processing
# ---------------------------------------------------------------------------

async def _process_cell(
    client: AsyncGooglePlacesClient,
    cell_key: str,
    lat: float,
    lon: float,
    radius_m: int,
    seen_place_ids: set[str],
    stats: dict[str, int],
) -> list[dict[str, Any]]:
    """Query one grid cell and return deduped parameter dicts ready to INSERT.

    Mutates *seen_place_ids* and *stats* in place. Returns ``[]`` on API
    error or empty/dedupe results.
    """
    try:
        response = await client.nearby_search(
            lat=lat,
            lon=lon,
            radius_m=radius_m,
            place_type=NEARBY_PLACE_TYPE,
        )
    except Exception as exc:
        logger.warning("Nearby Search call raised for cell %s: %s", cell_key, exc)
        stats["api_errors"] += 1
        return []

    status = response.get("status", "")
    if status not in ("OK", "ZERO_RESULTS"):
        logger.warning("Nearby Search status=%s for cell %s", status, cell_key)
        stats["api_errors"] += 1
        return []

    stats["grid_points_queried_successfully"] += 1

    results = response.get("results") or []
    if results:
        stats["grid_points_with_results"] += 1
    else:
        stats["grid_points_empty"] += 1

    if len(results) >= 20:
        stats["grid_points_saturated_at_20"] += 1

    new_rows: list[dict[str, Any]] = []
    for r in results:
        place_id = r.get("place_id")
        if not place_id or place_id in seen_place_ids:
            continue
        geo = (r.get("geometry") or {}).get("location") or {}
        result_lat = geo.get("lat")
        result_lng = geo.get("lng")
        if result_lat is None or result_lng is None:
            continue
        seen_place_ids.add(place_id)
        name = r.get("name") or "Unnamed parking"
        new_rows.append({
            "city": "riyadh",
            "name": name[:NAME_MAX_LEN],
            "lon": float(result_lng),
            "lat": float(result_lat),
        })
    return new_rows


# ---------------------------------------------------------------------------
# Main async loop
# ---------------------------------------------------------------------------

async def run_ingest(
    db,
    *,
    city: str = "riyadh",
    replace: bool = True,
    bbox: dict[str, float] | None = None,
    spacing_m: float = SPACING_M,
    radius_m: int = RADIUS_M,
    checkpoint_path: str = DEFAULT_CHECKPOINT_PATH,
    write_stats_path: str | None = None,
) -> dict[str, Any]:
    """Run the Google Places parking ingest end-to-end against *db*.

    Returns the stats dict that gets serialised to *write_stats_path*.
    """
    bbox = bbox if bbox is not None else RIYADH_BUILT_UP_BBOX
    grid = generate_grid_points(bbox, spacing_m)
    logger.info(
        "Generated %d grid points for bbox=%s spacing_m=%s",
        len(grid), bbox, spacing_m,
    )

    completed_cells = load_checkpoint(checkpoint_path)
    if completed_cells:
        logger.info("Resuming from checkpoint: %d cells already completed", len(completed_cells))

    if replace and not completed_cells:
        db.execute(
            text(
                "DELETE FROM expansion_parking_asset "
                "WHERE city = :city AND source = 'google_places'"
            ),
            {"city": city},
        )
        db.commit()
        logger.info(
            "Replace mode: deleted existing source='google_places' rows for city=%s",
            city,
        )

    pending = [(lat, lon, ck) for lat, lon, ck in grid if ck not in completed_cells]
    total_pending = len(pending)

    stats: dict[str, Any] = {
        "grid_points_total": len(grid),
        "grid_points_queried_successfully": 0,
        "grid_points_with_results": 0,
        "grid_points_empty": 0,
        "grid_points_saturated_at_20": 0,
        "unique_pois_found": 0,
        "pois_inserted": 0,
        "api_errors": 0,
        "total_api_calls_billed": 0,
    }
    seen_place_ids: set[str] = set()

    async with AsyncGooglePlacesClient() as client:
        for batch_start in range(0, total_pending, CHECKPOINT_FLUSH_INTERVAL):
            batch = pending[batch_start:batch_start + CHECKPOINT_FLUSH_INTERVAL]
            tasks = [
                _process_cell(
                    client, ck, lat, lon, radius_m, seen_place_ids, stats,
                )
                for lat, lon, ck in batch
            ]
            batch_rows = await asyncio.gather(*tasks)

            insert_params = [row for sublist in batch_rows for row in sublist]
            if insert_params:
                db.execute(INSERT_SQL, insert_params)
                stats["pois_inserted"] += len(insert_params)

            for _lat, _lon, ck in batch:
                completed_cells.add(ck)
            db.commit()
            save_checkpoint(checkpoint_path, completed_cells)

            processed = batch_start + len(batch)
            if processed % PROGRESS_LOG_INTERVAL == 0 or processed == total_pending:
                logger.info(
                    "Processed %d/%d cells, %d unique POIs found so far",
                    processed, total_pending, len(seen_place_ids),
                )

        stats["total_api_calls_billed"] = client.api_calls

    stats["unique_pois_found"] = len(seen_place_ids)
    stats["row_counts"] = log_table_counts(db, ["expansion_parking_asset"])

    delete_checkpoint(checkpoint_path)
    logger.info(
        "Ingest complete: %d unique POIs, %d rows inserted, %d API calls, %d errors",
        stats["unique_pois_found"], stats["pois_inserted"],
        stats["total_api_calls_billed"], stats["api_errors"],
    )

    if write_stats_path:
        write_stats(write_stats_path, stats)

    return stats


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Expansion Advisor — Google Places parking ingest",
    )
    parser.add_argument("--city", default="riyadh", help="City filter (default: riyadh)")
    parser.add_argument(
        "--replace",
        type=lambda v: v.lower() in ("true", "1", "yes"),
        default=True,
        help="Replace existing rows when no checkpoint is present (default: true)",
    )
    parser.add_argument("--write-stats", type=str, default=None, help="Write JSON stats to path")
    parser.add_argument(
        "--checkpoint-path",
        type=str,
        default=DEFAULT_CHECKPOINT_PATH,
        help=f"Resumability checkpoint path (default: {DEFAULT_CHECKPOINT_PATH})",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    validate_db_env()

    db = get_session()
    try:
        asyncio.run(
            run_ingest(
                db,
                city=args.city,
                replace=args.replace,
                checkpoint_path=args.checkpoint_path,
                write_stats_path=args.write_stats,
            )
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
