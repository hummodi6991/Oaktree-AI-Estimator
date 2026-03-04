"""
Connector for Overture Maps Places dataset.

Queries the Overture Places Parquet files on S3 using DuckDB.
Filters to restaurant-related categories within the Riyadh bounding box.
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

logger = logging.getLogger(__name__)

# Riyadh metro bounding box (generous)
RIYADH_BBOX = (46.20, 24.20, 47.30, 25.10)

# Overture release — bump when a new release lands
OVERTURE_RELEASE = "2026-02-18.0"
OVERTURE_S3_PATH = (
    f"s3://overturemaps-us-west-2/release/{OVERTURE_RELEASE}/theme=places/type=place/*"
)

# Category keywords that indicate a food/restaurant POI
_FOOD_KEYWORDS = {
    "restaurant", "fast_food", "cafe", "bakery", "food", "coffee",
    "pizza", "burger", "chicken", "seafood", "sushi", "ice_cream",
    "juice", "shawarma", "grill", "diner", "bistro", "eatery",
    "dessert", "pastry", "sandwich", "noodle", "steak",
}


def _is_food_category(cat: str | None) -> bool:
    if not cat:
        return False
    lower = cat.lower()
    return any(kw in lower for kw in _FOOD_KEYWORDS)


def extract_restaurants_duckdb() -> Iterator[dict[str, Any]]:
    """
    Extract restaurant POIs from Overture Places Parquet via DuckDB.

    Requires ``duckdb`` and ``pyarrow`` to be installed.
    Returns an iterator of dicts suitable for upserting into ``restaurant_poi``.
    """
    try:
        from app.connectors.duckdb_conn import get_duckdb_connection
    except ImportError:
        logger.error("duckdb is not installed — run: pip install duckdb")
        return

    min_lon, min_lat, max_lon, max_lat = RIYADH_BBOX
    con = get_duckdb_connection()

    query = f"""
        SELECT
            id,
            names,
            categories,
            confidence,
            ST_X(geometry) AS lon,
            ST_Y(geometry) AS lat,
            sources
        FROM read_parquet('{OVERTURE_S3_PATH}')
        WHERE bbox.xmin >= {min_lon}
          AND bbox.xmax <= {max_lon}
          AND bbox.ymin >= {min_lat}
          AND bbox.ymax <= {max_lat}
          AND confidence > 0.5
    """

    logger.info("Querying Overture Places for Riyadh restaurants...")
    result = con.execute(query).fetchall()
    cols = ["id", "names", "categories", "confidence", "lon", "lat", "sources"]

    count = 0
    for row in result:
        rec = dict(zip(cols, row))

        # Parse categories — Overture stores as struct/dict
        cats = rec.get("categories") or {}
        primary = cats.get("primary", "") if isinstance(cats, dict) else str(cats)

        if not _is_food_category(primary):
            continue

        # Parse names — Overture stores as struct with `primary` and list of alternates
        names = rec.get("names") or {}
        if isinstance(names, dict):
            name = names.get("primary", "Unknown")
        else:
            name = str(names) if names else "Unknown"

        yield {
            "id": f"overture:{rec['id']}",
            "name": name,
            "category_raw": primary,
            "lat": float(rec["lat"]),
            "lon": float(rec["lon"]),
            "confidence": rec.get("confidence"),
            "sources": rec.get("sources"),
        }
        count += 1

    logger.info("Extracted %d restaurant POIs from Overture Places", count)
    con.close()
