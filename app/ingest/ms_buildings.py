from __future__ import annotations

import csv
import gzip
import hashlib
import itertools
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from sqlalchemy import text

from app.db.session import SessionLocal

DEFAULT_SOURCE = "microsoft_globalml"
DEFAULT_COUNTRY = "Saudi Arabia"
DEFAULT_BATCH_SIZE = 2000
GEOMETRY_COLUMNS = ["geometry", "geom", "wkt", "polygon", "multipolygon", "GeoJSON", "geojson"]

INSERT_GEOJSON_SQL = text(
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

INSERT_WKT_SQL = text(
    """
    WITH geom_data AS (
        SELECT ST_Multi(ST_SetSRID(ST_GeomFromText(:wkt), 4326)) AS geom
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


@dataclass
class FileStats:
    read_rows: int = 0
    parsed_ok: int = 0
    inserted: int = 0
    skipped_invalid: int = 0


@dataclass
class ParsedRecord:
    source_id: str
    geojson: str | None = None
    wkt: str | None = None


def _iter_files(directory: Path) -> Iterable[Path]:
    yield from sorted(directory.glob("*.csv.gz"))


def _extract_quadkey(path: Path) -> str | None:
    matches = re.findall(r"[0-3]{4,}", path.stem)
    if not matches:
        return None
    return max(matches, key=len)


def _extract_geojson(record: dict) -> dict | None:
    if record.get("type") == "Feature":
        return record.get("geometry")
    if record.get("type") in {"Polygon", "MultiPolygon"}:
        return {"type": record.get("type"), "coordinates": record.get("coordinates")}
    return None


def _geometry_column(fieldnames: list[str] | None) -> str | None:
    if not fieldnames:
        return None
    lowered = {name.lower(): name for name in fieldnames}
    for candidate in GEOMETRY_COLUMNS:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def _parse_json_line(line: str, stats: FileStats) -> ParsedRecord | None:
    stats.read_rows += 1
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        stats.skipped_invalid += 1
        return None

    if not isinstance(record, dict):
        stats.skipped_invalid += 1
        return None

    geom = _extract_geojson(record)
    if not geom:
        stats.skipped_invalid += 1
        return None

    source_id = hashlib.sha1(line.encode("utf-8")).hexdigest()
    stats.parsed_ok += 1
    return ParsedRecord(source_id=source_id, geojson=json.dumps(geom, separators=(",", ":")))


def _stable_row_string(row: dict, fieldnames: list[str]) -> str:
    return "\u001f".join(str(row.get(field, "")) for field in fieldnames)


def _parse_csv_row(
    row: dict,
    fieldnames: list[str],
    geometry_column: str,
    stats: FileStats,
) -> ParsedRecord | None:
    stats.read_rows += 1
    raw_value = row.get(geometry_column)
    if raw_value is None:
        stats.skipped_invalid += 1
        return None

    raw_value = raw_value.strip()
    if not raw_value:
        stats.skipped_invalid += 1
        return None

    source_id = hashlib.sha1(_stable_row_string(row, fieldnames).encode("utf-8")).hexdigest()

    if raw_value.startswith("{") or raw_value.startswith("["):
        try:
            geom = json.loads(raw_value)
        except json.JSONDecodeError:
            stats.skipped_invalid += 1
            return None
        if not isinstance(geom, dict):
            stats.skipped_invalid += 1
            return None
        geometry = _extract_geojson(geom)
        if not geometry:
            stats.skipped_invalid += 1
            return None
        stats.parsed_ok += 1
        return ParsedRecord(source_id=source_id, geojson=json.dumps(geometry, separators=(",", ":")))

    stats.parsed_ok += 1
    return ParsedRecord(source_id=source_id, wkt=raw_value)


def _flush_batches(
    session,
    geojson_batch: list[dict],
    wkt_batch: list[dict],
) -> int:
    inserted = 0
    if geojson_batch:
        result = session.execute(INSERT_GEOJSON_SQL, geojson_batch)
        session.commit()
        if result.rowcount and result.rowcount > 0:
            inserted += result.rowcount
        geojson_batch.clear()
    if wkt_batch:
        result = session.execute(INSERT_WKT_SQL, wkt_batch)
        session.commit()
        if result.rowcount and result.rowcount > 0:
            inserted += result.rowcount
        wkt_batch.clear()
    return inserted


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
    total_stats = FileStats()
    try:
        for file_path in _iter_files(directory):
            quadkey = _extract_quadkey(file_path)
            geojson_batch: list[dict] = []
            wkt_batch: list[dict] = []
            stats = FileStats()

            with gzip.open(file_path, "rt", encoding="utf-8") as handle:
                first_line = ""
                for line in handle:
                    if line.strip():
                        first_line = line
                        break

                if not first_line:
                    print(f"{file_path.name}: empty file")
                    continue

                if first_line.lstrip().startswith("{") or first_line.lstrip().startswith("["):
                    record = _parse_json_line(first_line.strip(), stats)
                    if record:
                        _queue_record(record, geojson_batch, wkt_batch, source, country, quadkey)

                    for line in handle:
                        line = line.strip()
                        if not line:
                            continue
                        record = _parse_json_line(line, stats)
                        if record:
                            _queue_record(record, geojson_batch, wkt_batch, source, country, quadkey)
                        if len(geojson_batch) + len(wkt_batch) >= batch_size:
                            stats.inserted += _flush_batches(session, geojson_batch, wkt_batch)
                else:
                    lines = itertools.chain([first_line], handle)
                    reader = csv.DictReader(lines)
                    geometry_column = _geometry_column(reader.fieldnames)
                    if not geometry_column:
                        stats.skipped_invalid += 1
                    else:
                        for row in reader:
                            record = _parse_csv_row(
                                row,
                                reader.fieldnames or [],
                                geometry_column,
                                stats,
                            )
                            if record:
                                _queue_record(record, geojson_batch, wkt_batch, source, country, quadkey)
                            if len(geojson_batch) + len(wkt_batch) >= batch_size:
                                stats.inserted += _flush_batches(session, geojson_batch, wkt_batch)

            stats.inserted += _flush_batches(session, geojson_batch, wkt_batch)
            total_inserted += stats.inserted
            total_stats.read_rows += stats.read_rows
            total_stats.parsed_ok += stats.parsed_ok
            total_stats.inserted += stats.inserted
            total_stats.skipped_invalid += stats.skipped_invalid
            print(
                f"{file_path.name}: read={stats.read_rows} parsed={stats.parsed_ok} "
                f"inserted={stats.inserted} skipped={stats.skipped_invalid}"
            )

        print(
            "total: "
            f"read={total_stats.read_rows} parsed={total_stats.parsed_ok} "
            f"inserted={total_stats.inserted} skipped={total_stats.skipped_invalid}"
        )
        return total_inserted
    finally:
        session.close()


def _queue_record(
    record: ParsedRecord,
    geojson_batch: list[dict],
    wkt_batch: list[dict],
    source: str,
    country: str | None,
    quadkey: str | None,
) -> None:
    payload = {
        "source": source,
        "country": country,
        "quadkey": quadkey,
        "source_id": record.source_id,
    }
    if record.geojson is not None:
        geojson_batch.append({**payload, "geojson": record.geojson})
        return
    if record.wkt is not None:
        wkt_batch.append({**payload, "wkt": record.wkt})


def main() -> None:
    dir_value = os.getenv("MS_BUILDINGS_DIR")
    if not dir_value:
        raise SystemExit("MS_BUILDINGS_DIR is required (path to .csv.gz files)")

    directory = Path(dir_value)
    inserted = ingest_ms_buildings(directory)
    print(f"Inserted {inserted} Microsoft GlobalML building footprints into ms_buildings_raw")


if __name__ == "__main__":
    main()
