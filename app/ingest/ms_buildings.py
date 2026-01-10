from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Iterable

from sqlalchemy import text

from app.db.session import SessionLocal

DEFAULT_SOURCE = "microsoft_globalml"
DEFAULT_COUNTRY = "Saudi Arabia"
DEFAULT_BATCH_SIZE = 2000

INSERT_SQL = text(
    """
    WITH geom_data AS (
        SELECT ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(:geojson), 4326)) AS geom
    )
    INSERT INTO public.ms_buildings_raw (
        source,
        country,
        quadkey,
        source_id,
        geom,
        area_m2,
        observed_at
    )
    SELECT
        :source,
        :country,
        :quadkey,
        :source_id,
        geom,
        ST_Area(ST_Transform(geom, 3857)),
        now()
    FROM geom_data
    ON CONFLICT (source, source_id) DO NOTHING
    """
)


def _iter_files(directory: Path) -> Iterable[Path]:
    yield from sorted(directory.glob("*.csv.gz"))


def _extract_quadkey(path: Path) -> str | None:
    matches = re.findall(r"[0-3]{4,}", path.stem)
    if not matches:
        return None
    return max(matches, key=len)


def _geometry_from_record(record: dict) -> dict | None:
    if "geometry" in record:
        return record.get("geometry")
    if record.get("type") in {"Polygon", "MultiPolygon"}:
        return {"type": record.get("type"), "coordinates": record.get("coordinates")}
    return None


def ingest_ms_buildings(
    directory: Path,
    *,
    country: str | None = DEFAULT_COUNTRY,
    source: str = DEFAULT_SOURCE,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    if not directory.exists():
        raise FileNotFoundError(f"MS_BUILDINGS_DIR not found: {directory}")

    session = SessionLocal()
    total_inserted = 0
    try:
        for file_path in _iter_files(directory):
            quadkey = _extract_quadkey(file_path)
            batch: list[dict] = []

            with gzip.open(file_path, "rt", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    geom = _geometry_from_record(record)
                    if not geom:
                        continue

                    source_id = hashlib.sha1(line.encode("utf-8")).hexdigest()
                    batch.append(
                        {
                            "source": source,
                            "country": country,
                            "quadkey": quadkey,
                            "source_id": source_id,
                            "geojson": json.dumps(geom, separators=(",", ":")),
                        }
                    )

                    if len(batch) >= batch_size:
                        result = session.execute(INSERT_SQL, batch)
                        session.commit()
                        if result.rowcount and result.rowcount > 0:
                            total_inserted += result.rowcount
                        batch.clear()

            if batch:
                result = session.execute(INSERT_SQL, batch)
                session.commit()
                if result.rowcount and result.rowcount > 0:
                    total_inserted += result.rowcount

        return total_inserted
    finally:
        session.close()


def main() -> None:
    dir_value = os.getenv("MS_BUILDINGS_DIR")
    if not dir_value:
        raise SystemExit("MS_BUILDINGS_DIR is required (path to .csv.gz files)")

    directory = Path(dir_value)
    inserted = ingest_ms_buildings(directory)
    print(f"Inserted {inserted} Microsoft GlobalML building footprints into ms_buildings_raw")


if __name__ == "__main__":
    main()
