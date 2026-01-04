from __future__ import annotations

import argparse
import json
import hashlib
import math
import sys
from dataclasses import dataclass
from typing import Iterable, Sequence

import httpx
import mapbox_vector_tile
from shapely.geometry import MultiPolygon, shape
from shapely.geometry.base import BaseGeometry
from sqlalchemy import text

from app.db.session import SessionLocal

TILE_URL_TEMPLATE = "https://tiles.suhail.ai/maps/riyadh/{layer}/{z}/{x}/{y}.pbf"
UPDATE_INTERVAL = 25
WEB_MERCATOR_RADIUS = 6378137.0
WORLD_SIZE = 2 * math.pi * WEB_MERCATOR_RADIUS
BBOX_RIYADH_DEFAULT = (46.20, 24.20, 47.30, 25.10)


@dataclass
class TileRange:
    zoom: int
    x_min: int
    x_max: int
    y_min: int
    y_max: int
    tiles: list[tuple[int, int]]


def _lonlat_to_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    n = 2**zoom
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2 * n)
    x = max(0, min(x, n - 1))
    y = max(0, min(y, n - 1))
    return x, y


def _tile_bounds_mercator(x: int, y: int, z: int) -> tuple[float, float, float, float]:
    n = 2**z
    x_min = (x / n - 0.5) * WORLD_SIZE
    x_max = ((x + 1) / n - 0.5) * WORLD_SIZE
    y_max = (0.5 - y / n) * WORLD_SIZE
    y_min = (0.5 - (y + 1) / n) * WORLD_SIZE
    return x_min, y_min, x_max, y_max


def _mercator_to_lonlat(x: float, y: float) -> tuple[float, float]:
    lon = math.degrees(x / WEB_MERCATOR_RADIUS)
    lat = math.degrees(math.atan(math.sinh(y / WEB_MERCATOR_RADIUS)))
    return lon, lat


def _parse_bbox(arg: str | None) -> tuple[float, float, float, float]:
    if not arg:
        return BBOX_RIYADH_DEFAULT
    parts = [p.strip() for p in arg.split(",") if p.strip()]
    if len(parts) != 4:
        raise ValueError("bbox must be minLon,minLat,maxLon,maxLat")
    try:
        west, south, east, north = map(float, parts)
    except ValueError as exc:
        raise ValueError("bbox values must be numeric") from exc

    if west > east:
        west, east = east, west
    if south > north:
        south, north = north, south
    return west, south, east, north


def _build_tile_range(bounds: Sequence[float], zoom: int) -> TileRange:
    west, south, east, north = bounds
    x_min, y_min = _lonlat_to_tile(west, north, zoom)
    x_max, y_max = _lonlat_to_tile(east, south, zoom)
    x_min, x_max = sorted((x_min, x_max))
    y_min, y_max = sorted((y_min, y_max))

    tiles = []
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            tiles.append((x, y))

    return TileRange(zoom=zoom, x_min=x_min, x_max=x_max, y_min=y_min, y_max=y_max, tiles=tiles)


def _scale_point(
    pt: Sequence[float],
    merc_bounds: tuple[float, float, float, float],
    extent: int,
) -> tuple[float, float]:
    x_min, y_min, x_max, y_max = merc_bounds
    merc_x = x_min + (x_max - x_min) * (pt[0] / extent)
    merc_y = y_max - (y_max - y_min) * (pt[1] / extent)
    return _mercator_to_lonlat(merc_x, merc_y)


def _scale_geometry(
    geometry: dict,
    merc_bounds: tuple[float, float, float, float],
    extent: int,
) -> dict | None:
    gtype = geometry.get("type")
    coords = geometry.get("coordinates")
    if gtype not in {"Polygon", "MultiPolygon"} or not coords:
        return None

    if gtype == "Polygon":
        scaled = [[_scale_point(pt, merc_bounds, extent) for pt in ring] for ring in coords]
    else:  # MultiPolygon
        scaled = [
            [[_scale_point(pt, merc_bounds, extent) for pt in ring] for ring in polygon]
            for polygon in coords
        ]
    return {"type": gtype, "coordinates": scaled}


def _clean_geometry(geom: BaseGeometry | None) -> BaseGeometry | None:
    if geom is None or geom.is_empty:
        return None
    if not geom.is_valid:
        geom = geom.buffer(0)
    if geom.is_empty:
        return None
    if geom.geom_type == "Polygon":
        geom = MultiPolygon([geom])
    elif geom.geom_type != "MultiPolygon":
        geom = geom.buffer(0)
        if geom.is_empty:
            return None
        if geom.geom_type == "Polygon":
            geom = MultiPolygon([geom])
        elif geom.geom_type != "MultiPolygon":
            return None
    return geom


def _stable_id(geom: BaseGeometry, props: dict) -> str:
    key_str = "|".join(sorted(props.keys()))
    digest = hashlib.sha1()
    digest.update(geom.wkb)
    digest.update(key_str.encode("utf-8"))
    return digest.hexdigest()[:16]


def _decode_tile(
    data: bytes,
    layer: str,
    x: int,
    y: int,
    z: int,
) -> list[tuple[str, BaseGeometry, dict]]:
    if not data:
        return []

    decoded = mapbox_vector_tile.decode(data)
    layer_key = layer
    candidates = [layer, layer.replace("-", "_")]
    for cand in candidates:
        if cand in decoded:
            layer_key = cand
            break
    else:
        if decoded:
            layer_key = next(iter(decoded.keys()))
        else:
            return []

    layer_data = decoded.get(layer_key)
    if not layer_data:
        return []

    extent = layer_data.get("extent") or 4096
    merc_bounds = _tile_bounds_mercator(x, y, z)

    parcels: list[tuple[str, BaseGeometry, dict]] = []
    for feature in layer_data.get("features", []):
        geom_json = _scale_geometry(feature.get("geometry") or {}, merc_bounds=merc_bounds, extent=extent)
        if not geom_json:
            continue
        try:
            geom = shape(geom_json)
        except Exception:
            continue
        geom = _clean_geometry(geom)
        if geom is None:
            continue

        props = feature.get("properties") or {}
        parcel_id = props.get("id") or props.get("parcel_id")
        if not parcel_id:
            parcel_id = _stable_id(geom, props)

        parcels.append((str(parcel_id), geom, props))

    return parcels


def _upsert_state(
    db,
    tile_range: TileRange,
    map_name: str,
    layer_name: str,
    force_resume_from: int | None,
) -> int:
    row = db.execute(
        text(
            """
            SELECT id, map_name, layer_name, zoom, x_min, x_max, y_min, y_max, last_index
            FROM suhail_tile_ingest_state WHERE id=1
            """
        )
    ).mappings().one_or_none()

    params = {
        "map_name": map_name,
        "layer_name": layer_name,
        "zoom": tile_range.zoom,
        "x_min": tile_range.x_min,
        "x_max": tile_range.x_max,
        "y_min": tile_range.y_min,
        "y_max": tile_range.y_max,
    }

    if not row:
        db.execute(
            text(
                """
                INSERT INTO suhail_tile_ingest_state (id, map_name, layer_name, zoom, x_min, x_max, y_min, y_max, last_index, status)
                VALUES (1, :map_name, :layer_name, :zoom, :x_min, :x_max, :y_min, :y_max, 0, 'running')
                """
            ),
            params,
        )
        last_index = 0
    else:
        reset_needed = any(
            row.get(key) != params[key]
            for key in ("map_name", "layer_name", "zoom", "x_min", "x_max", "y_min", "y_max")
        )
        if reset_needed:
            last_index = 0
        else:
            last_index = int(row.get("last_index") or 0)

        db.execute(
            text(
                """
                UPDATE suhail_tile_ingest_state
                SET map_name=:map_name, layer_name=:layer_name, zoom=:zoom,
                    x_min=:x_min, x_max=:x_max, y_min=:y_min, y_max=:y_max,
                    updated_at=now(), status=CASE WHEN :reset_needed THEN 'reset' ELSE 'running' END,
                    last_index=CASE WHEN :reset_needed THEN 0 ELSE last_index END
                WHERE id=1
                """
            ),
            {**params, "reset_needed": reset_needed},
        )

    if force_resume_from is not None:
        last_index = max(0, force_resume_from)
        db.execute(
            text(
                """
                UPDATE suhail_tile_ingest_state
                SET last_index=:last_index, updated_at=now(), status='forced'
                WHERE id=1
                """
            ),
            {"last_index": last_index},
        )

    db.commit()
    return last_index


def _update_progress(db, last_index: int, status: str | None = None) -> None:
    db.execute(
        text(
            """
            UPDATE suhail_tile_ingest_state
            SET last_index=:last_index, updated_at=now(), status=COALESCE(:status, status)
            WHERE id=1
            """
        ),
        {"last_index": last_index, "status": status},
    )
    db.commit()


def _fetch_tile(layer: str, x: int, y: int, z: int) -> bytes:
    url = TILE_URL_TEMPLATE.format(layer=layer, z=z, x=x, y=y)
    resp = httpx.get(url, timeout=20.0)
    if resp.status_code in (204, 404):
        return b""
    resp.raise_for_status()
    return resp.content


def _upsert_parcels(db, parcels: Iterable[tuple[str, BaseGeometry, dict]], tile_meta: dict) -> int:
    rows = []
    for parcel_id, geom, props in parcels:
        rows.append(
            {
                "id": parcel_id,
                "geom_wkb": geom.wkb,
                "props_json": json.dumps(props, ensure_ascii=False),
                **tile_meta,
            }
        )

    if not rows:
        return 0

    db.execute(
        text(
            """
            INSERT INTO suhail_parcel_raw (id, geom, props, source_layer, z, x, y, observed_at)
            VALUES (:id, ST_Multi(ST_SetSRID(ST_GeomFromWKB(:geom_wkb), 4326)), :props_json::jsonb, :source_layer, :z, :x, :y, now())
            ON CONFLICT (id) DO UPDATE
            SET geom = EXCLUDED.geom,
                props = EXCLUDED.props,
                source_layer = EXCLUDED.source_layer,
                z = EXCLUDED.z,
                x = EXCLUDED.x,
                y = EXCLUDED.y,
                observed_at = EXCLUDED.observed_at
            """
        ),
        rows,
    )
    db.commit()
    return len(rows)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    default_bbox_arg = ",".join(str(v) for v in BBOX_RIYADH_DEFAULT)
    parser = argparse.ArgumentParser(description="Ingest Suhail parcel vector tiles into Postgres")
    parser.add_argument("--zoom", type=int, default=15, help="Tile zoom level (default: 15)")
    parser.add_argument("--layer", type=str, default="parcels-base", help="Layer name (default: parcels-base)")
    parser.add_argument("--force-resume-from", type=int, dest="force_resume_from", help="Override stored last_index")
    parser.add_argument("--max-tiles", type=int, dest="max_tiles", help="Limit number of tiles processed (testing)")
    parser.add_argument("--bbox", type=str, default=default_bbox_arg, help="minLon,minLat,maxLon,maxLat")
    parser.add_argument("--dry-run", action="store_true", help="Decode first tile only (no DB writes)")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])

    bounds = _parse_bbox(args.bbox)
    tile_range = _build_tile_range(bounds, args.zoom)
    tile_count = len(tile_range.tiles)
    print(
        f"Tiles at z={args.zoom}: x[{tile_range.x_min},{tile_range.x_max}] "
        f"y[{tile_range.y_min},{tile_range.y_max}] ({tile_count} tiles)"
    )

    if args.dry_run:
        if not tile_range.tiles:
            print("No tiles generated for the requested bbox/zoom.")
            return 0
        x, y = tile_range.tiles[0]
        tile_bytes = _fetch_tile(args.layer, x, y, args.zoom)
        parcels = _decode_tile(tile_bytes, args.layer, x, y, args.zoom)
        print(f"Dry run tile z={args.zoom} x={x} y={y}: decoded {len(parcels)} features")
        if parcels:
            keys = sorted((parcels[0][2] or {}).keys())
            print(f"First feature property keys: {keys}")
        else:
            print("No features decoded from first tile.")
        return 0

    with SessionLocal() as db:
        last_index = _upsert_state(
            db=db,
            tile_range=tile_range,
            map_name="riyadh",
            layer_name=args.layer,
            force_resume_from=args.force_resume_from,
        )
        if last_index > tile_count:
            last_index = tile_count
        print(f"Resuming from tile index {last_index}")

        processed = 0
        last_processed_index = last_index

        try:
            for idx, (x, y) in enumerate(tile_range.tiles, start=1):
                if idx <= last_index:
                    continue
                if args.max_tiles is not None and processed >= args.max_tiles:
                    print(f"Max tiles {args.max_tiles} reached; stopping early")
                    break

                tile_bytes = _fetch_tile(args.layer, x, y, args.zoom)
                parcels = list(_decode_tile(tile_bytes, args.layer, x, y, args.zoom))
                inserted = _upsert_parcels(
                    db,
                    parcels,
                    {"source_layer": args.layer, "z": args.zoom, "x": x, "y": y},
                )
                processed += 1
                last_processed_index = idx

                if processed % UPDATE_INTERVAL == 0:
                    _update_progress(db, last_processed_index, status="running")

                print(
                    f"Tile {idx}/{tile_count} (x={x}, y={y}) -> {len(parcels)} features, {inserted} upserted"
                )
        finally:
            status = "completed" if last_processed_index >= tile_count else "stopped"
            if args.max_tiles is not None and processed >= args.max_tiles and last_processed_index < tile_count:
                status = "partial"
            _update_progress(db, last_processed_index, status=status)

    print(f"Processed {processed} new tiles; last index={last_processed_index}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
