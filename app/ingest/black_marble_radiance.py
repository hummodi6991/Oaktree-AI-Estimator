"""
Ingestion pipeline for NASA Black Marble VNP46A3 monthly nighttime radiance.

Downloads one h22v06 H5 file per month from the LAADS DAAC archive, aggregates
per-district radiance using OSM-first district polygons, and inserts into
``district_radiance_monthly``. Idempotent per (year_month, source).
"""

from __future__ import annotations

import logging
import tempfile
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.connectors import blackmarble
from app.connectors.blackmarble import (
    PIXEL_COUNT_FLOOR,
    RIYADH_BBOX,
    SOURCE_LABEL,
)
from app.ml.name_normalization import norm_district

logger = logging.getLogger(__name__)


def ingest_blackmarble_month(db: Session, year_month: date, token: str) -> int:
    """Ingest one month of Black Marble VNP46A3 radiance into district_radiance_monthly.

    Idempotent: re-running for an already-ingested month deletes prior rows for that
    (year_month, source) combination and re-inserts. Mirrors the population pre-purge
    pattern.

    Returns: number of rows inserted (one per district).
    """
    ym = date(year_month.year, year_month.month, 1)

    deleted = db.execute(
        text(
            "DELETE FROM district_radiance_monthly "
            "WHERE year_month = :ym AND source = :src"
        ),
        {"ym": ym, "src": SOURCE_LABEL},
    ).rowcount
    logger.info(
        "Pre-purged %s existing rows for year_month=%s source=%s",
        deleted,
        ym.isoformat(),
        SOURCE_LABEL,
    )

    url = blackmarble.discover_h5_url(ym, token)
    logger.info("Resolved Black Marble URL for %s: %s", ym.isoformat(), url)

    polygons = _load_district_polygons(db)
    if not polygons:
        logger.warning("No district polygons loaded; aborting Black Marble ingest")
        return 0

    with tempfile.TemporaryDirectory(prefix="blackmarble_") as tmpdir:
        h5_path = Path(tmpdir) / f"VNP46A3.{ym.isoformat()}.{blackmarble.TILE}.h5"
        blackmarble.download_h5(url, token, h5_path)
        _validate_h5(h5_path)

        rows: list[dict[str, Any]] = list(
            blackmarble.aggregate_per_district(h5_path, polygons, ym)
        )

    if not rows:
        logger.warning("Aggregation produced 0 rows for %s", ym.isoformat())
        return 0

    db.execute(
        text(
            """
            INSERT INTO district_radiance_monthly (
                district_key, year_month, radiance_mean, radiance_median,
                radiance_sum, radiance_p90, pixel_count_total, pixel_count_valid,
                quality_filter, source, tile
            ) VALUES (
                :district_key, :year_month, :radiance_mean, :radiance_median,
                :radiance_sum, :radiance_p90, :pixel_count_total, :pixel_count_valid,
                :quality_filter, :source, :tile
            )
            """
        ),
        rows,
    )
    db.commit()

    confident = sum(1 for r in rows if r["pixel_count_valid"] >= PIXEL_COUNT_FLOOR)
    logger.info(
        "black marble ingest: year_month=%s districts=%d rows_with_pixels>=%d=%d",
        ym.isoformat()[:7],
        len(rows),
        PIXEL_COUNT_FLOOR,
        confident,
    )
    return len(rows)


def _load_district_polygons(db: Session) -> list[dict[str, Any]]:
    """Load Riyadh district polygons (OSM-first) for radiance aggregation.

    Mirrors the osm-first DISTINCT ON pattern in
    ``app/services/expansion_advisor.py:387-409``. Each district name is run
    through :func:`app.ml.name_normalization.norm_district` to produce
    ``district_key``. Polygons whose envelopes fall entirely outside
    ``RIYADH_BBOX`` are rejected.
    """
    try:
        from shapely import wkb as shapely_wkb
    except ImportError as exc:
        logger.error("Missing shapely dependency: %s", exc)
        return []

    west, south, east, north = RIYADH_BBOX

    sql = text(
        """
        SELECT DISTINCT ON (district_label)
            TRIM(COALESCE(ef.properties->>'district_raw',
                          ef.properties->>'district')) AS district_label,
            ST_AsBinary(ef.geom) AS geom_wkb
        FROM external_feature ef
        WHERE ef.layer_name IN ('osm_districts', 'aqar_district_hulls')
          AND ef.geom IS NOT NULL
          AND COALESCE(ef.properties->>'district_raw',
                       ef.properties->>'district') IS NOT NULL
          AND TRIM(COALESCE(ef.properties->>'district_raw',
                            ef.properties->>'district')) <> ''
          AND ST_Intersects(
                ef.geom,
                ST_MakeEnvelope(:west, :south, :east, :north, 4326)
              )
        ORDER BY
            district_label,
            CASE ef.layer_name
                WHEN 'osm_districts'       THEN 1
                WHEN 'aqar_district_hulls' THEN 2
                ELSE 3
            END
        """
    )

    out: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    rows = db.execute(
        sql, {"west": west, "south": south, "east": east, "north": north}
    ).mappings().all()

    for row in rows:
        district_label = row["district_label"]
        geom_wkb = row["geom_wkb"]
        if not district_label or geom_wkb is None:
            continue
        try:
            geom = shapely_wkb.loads(bytes(geom_wkb))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping polygon with bad WKB: %s", exc)
            continue

        # Reject polygons that fall entirely outside the Riyadh metro bbox.
        minx, miny, maxx, maxy = geom.bounds
        if maxx < west or minx > east or maxy < south or miny > north:
            continue

        district_key = norm_district("riyadh", district_label)
        if not district_key or district_key in seen_keys:
            continue
        seen_keys.add(district_key)
        out.append({"district_key": district_key, "geometry": geom})

    logger.info("Loaded %d district polygons for Black Marble aggregation", len(out))
    return out


def _validate_h5(path: str | Path) -> None:
    """Validate that a file is a readable HDF5 container before loading."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"H5 file not found: {p}")

    try:
        import h5py
    except ImportError as exc:
        raise RuntimeError("h5py is required to validate Black Marble H5 files") from exc

    try:
        with h5py.File(p, "r") as fh:
            keys = list(fh.keys())
            print(f"H5 OK: {p.name} top_level_keys={keys}")
    except Exception as exc:
        head_bytes = p.read_bytes()[:32]
        raise RuntimeError(
            f"Cannot open {p} as HDF5.\n"
            f"  first 32 bytes: {head_bytes!r}\n"
            f"  original error: {exc}\n"
            f"Hint: re-run the download or check the LAADS source URL."
        ) from exc


if __name__ == "__main__":
    import argparse
    import os

    from app.db.session import SessionLocal

    parser = argparse.ArgumentParser(
        description="Ingest one month of Black Marble VNP46A3 radiance."
    )
    parser.add_argument(
        "--year-month",
        required=True,
        help="YYYY-MM (e.g. 2026-03)",
    )
    args = parser.parse_args()

    try:
        year, month = (int(x) for x in args.year_month.split("-", 1))
    except ValueError:
        raise SystemExit(f"Invalid --year-month value: {args.year_month!r}")
    ym = date(year, month, 1)

    token = os.environ.get("EDL_TOKEN")
    if not token:
        raise SystemExit("EDL_TOKEN environment variable is required")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    db = SessionLocal()
    try:
        n = ingest_blackmarble_month(db, ym, token)
        print(f"Black Marble ingestion complete: {n} district rows for {ym.isoformat()[:7]}")
    finally:
        db.close()
