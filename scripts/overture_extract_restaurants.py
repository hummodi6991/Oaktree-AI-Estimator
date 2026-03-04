#!/usr/bin/env python3
"""
Extract restaurant POIs from Overture Maps Places via DuckDB.

This script queries the Overture Places Parquet files on S3 and saves
restaurant POIs for Riyadh to a local GeoJSON file.

Usage:
    python scripts/overture_extract_restaurants.py [--output restaurants.geojson]

Requires: duckdb, pyarrow
"""

from __future__ import annotations

import argparse
import json
import sys


def main():
    parser = argparse.ArgumentParser(description="Extract Overture restaurant POIs for Riyadh")
    parser.add_argument("--output", "-o", default="data/riyadh_restaurants.geojson")
    parser.add_argument("--release", default="2026-02-18.0")
    args = parser.parse_args()

    try:
        import duckdb
    except ImportError:
        print("ERROR: duckdb is required. Install with: pip install duckdb", file=sys.stderr)
        sys.exit(1)

    s3_path = f"s3://overturemaps-us-west-2/release/{args.release}/theme=places/type=place/*"

    # Riyadh bounding box
    min_lon, min_lat, max_lon, max_lat = 46.20, 24.20, 47.30, 25.10

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='us-west-2';")

    query = f"""
        SELECT
            id,
            names,
            categories,
            confidence,
            ST_X(geometry) AS lon,
            ST_Y(geometry) AS lat,
            sources
        FROM read_parquet('{s3_path}')
        WHERE bbox.xmin >= {min_lon}
          AND bbox.xmax <= {max_lon}
          AND bbox.ymin >= {min_lat}
          AND bbox.ymax <= {max_lat}
          AND confidence > 0.5
    """

    print(f"Querying Overture Places (release {args.release}) for Riyadh...")
    result = con.execute(query).fetchall()
    cols = ["id", "names", "categories", "confidence", "lon", "lat", "sources"]

    food_keywords = {
        "restaurant", "fast_food", "cafe", "bakery", "food", "coffee",
        "pizza", "burger", "chicken", "seafood", "sushi", "ice_cream",
        "juice", "shawarma", "grill", "diner", "bistro", "eatery",
        "dessert", "pastry", "sandwich", "noodle", "steak",
    }

    features = []
    for row in result:
        rec = dict(zip(cols, row))
        cats = rec.get("categories") or {}
        primary = cats.get("primary", "") if isinstance(cats, dict) else str(cats)

        if not any(kw in primary.lower() for kw in food_keywords):
            continue

        names = rec.get("names") or {}
        name = names.get("primary", "Unknown") if isinstance(names, dict) else str(names)

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(rec["lon"]), float(rec["lat"])],
            },
            "properties": {
                "id": rec["id"],
                "name": name,
                "category": primary,
                "confidence": rec.get("confidence"),
            },
        })

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    import os
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(geojson, f)

    print(f"Extracted {len(features)} restaurant POIs → {args.output}")
    con.close()


if __name__ == "__main__":
    main()
