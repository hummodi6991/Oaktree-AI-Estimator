import logging
import os
import pathlib

import httpx
from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.deps import get_db

router = APIRouter(tags=["map"])

UPSTREAM = os.getenv("TILE_UPSTREAM", "https://tile.openstreetmap.org")
UA = os.getenv("TILE_USER_AGENT", "oaktree-estimator/0.1 (contact: ops@example.com)")
CACHE_DIR = os.getenv("TILE_CACHE_DIR", "/app/tiles_cache")
OFFLINE_ONLY = os.getenv("TILE_OFFLINE_ONLY", "false").lower() in {"1", "true", "yes"}

logger = logging.getLogger(__name__)


def _safe_identifier(value: str | None, fallback: str) -> str:
    candidate = (value or "").strip()
    if not candidate:
        return fallback
    if all(ch.isalnum() or ch in {"_", "."} for ch in candidate):
        return candidate
    logger.warning("Unsafe identifier %s; falling back to %s", value, fallback)
    return fallback


PARCEL_TILE_TABLE = _safe_identifier(
    os.getenv("PARCEL_TILE_TABLE")
    or getattr(settings, "PARCEL_IDENTIFY_TABLE", "osm_parcels_proxy"),
    "osm_parcels_proxy",
)
PARCEL_TILE_GEOM_COLUMN = _safe_identifier(
    os.getenv("PARCEL_TILE_GEOM_COLUMN")
    or getattr(settings, "PARCEL_IDENTIFY_GEOM_COLUMN", "geom"),
    "geom",
)
try:
    SMALL_PARCEL_MAX_AREA_M2 = int(os.getenv("PARCEL_TILE_MAX_AREA_M2") or 50_000)
except ValueError:
    logger.warning("Invalid PARCEL_TILE_MAX_AREA_M2; defaulting to 50000")
    SMALL_PARCEL_MAX_AREA_M2 = 50_000
PARCEL_TILE_SOURCE_FILTER = (os.getenv("PARCEL_TILE_SOURCE_FILTER") or "").strip()

_PARCEL_TILE_SOURCE_FILTER_CLAUSE = ""
if PARCEL_TILE_SOURCE_FILTER:
    _PARCEL_TILE_SOURCE_FILTER_CLAUSE = "  AND p.source = :source_filter"


def _tile_path(z: int, x: int, y: int) -> pathlib.Path:
    return pathlib.Path(CACHE_DIR) / str(z) / str(x) / f"{y}.png"


_OVT_TILE_SQL = text(
    """
    WITH bounds AS (
      SELECT ST_Transform(ST_TileEnvelope(:z,:x,:y), 32638) AS geom
    ),
    mvtgeom AS (
      SELECT
        'ovt:' || id AS id,
        ST_AsMVTGeom(ovt.geom, b.geom, 4096, 64, true) AS geom
      FROM overture_buildings AS ovt
      CROSS JOIN bounds AS b
      WHERE ovt.geom && b.geom
    )
    SELECT ST_AsMVT(mvtgeom, 'buildings', 4096, 'geom') AS tile
    FROM mvtgeom;
    """
)

_PARCEL_TILE_SQL = text(
    f"""
    WITH tile AS (
      SELECT ST_TileEnvelope(:z,:x,:y) AS geom3857
    ),
    parcel_candidates AS (
      SELECT
        p.id,
        p.source,
        p.landuse,
        p.classification,
        p.area_m2,
        p.{PARCEL_TILE_GEOM_COLUMN}
      FROM {PARCEL_TILE_TABLE} p, tile t
      WHERE p.{PARCEL_TILE_GEOM_COLUMN} && ST_Transform(t.geom3857, 4326)
        AND ST_Intersects(p.{PARCEL_TILE_GEOM_COLUMN}, ST_Transform(t.geom3857, 4326))
        AND p.area_m2 <= :max_area_m2
{_PARCEL_TILE_SOURCE_FILTER_CLAUSE}
    ),
    mvtgeom AS (
      SELECT
        id,
        source,
        landuse,
        classification,
        area_m2,
        ST_AsMVTGeom(
          ST_Transform(p.{PARCEL_TILE_GEOM_COLUMN}, 3857),
          t.geom3857,
          4096,
          64,
          true
        ) AS geom
      FROM parcel_candidates p, tile t
    )
    SELECT ST_AsMVT(mvtgeom, 'parcels', 4096, 'geom') AS tile
    FROM mvtgeom;
    """
)


@router.get("/tiles/{z}/{x}/{y}.png")
def tile(z: int, x: int, y: int):
    # 1) Serve from disk if present
    p = _tile_path(z, x, y)
    if p.exists():
        data = p.read_bytes()
        return Response(
            data,
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=31536000"},
        )

    if OFFLINE_ONLY:
        # No network calls in offline mode
        raise HTTPException(status_code=404, detail="tile not in local cache (offline-only)")

    # 2) Else fetch from upstream, write-through to disk cache, then return
    url = f"{UPSTREAM}/{z}/{x}/{y}.png"
    try:
        r = httpx.get(url, headers={"User-Agent": UA}, timeout=20.0)
        r.raise_for_status()
        # save to cache
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(r.content)
        return Response(
            r.content,
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400"},
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"tile upstream error: {exc}")


@router.get("/v1/tiles/ovt/{z}/{x}/{y}.pbf")
def overture_tile(z: int, x: int, y: int, db: Session = Depends(get_db)):
    try:
        tile_bytes = db.execute(_OVT_TILE_SQL, {"z": z, "x": x, "y": y}).scalar()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to render overture tile: {exc}")

    payload = bytes(tile_bytes or b"")
    return Response(
        payload,
        media_type="application/x-protobuf",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@router.get("/v1/tiles/parcels/{z}/{x}/{y}.pbf")
def parcel_tile(z: int, x: int, y: int, db: Session = Depends(get_db)):
    params = {
        "z": z,
        "x": x,
        "y": y,
        "max_area_m2": SMALL_PARCEL_MAX_AREA_M2,
    }
    if PARCEL_TILE_SOURCE_FILTER:
        params["source_filter"] = PARCEL_TILE_SOURCE_FILTER
    try:
        tile_bytes = db.execute(_PARCEL_TILE_SQL, params).scalar()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to render parcel tile: {exc}")

    payload = bytes(tile_bytes or b"")
    if not payload:
        return Response(status_code=204)

    return Response(
        payload,
        media_type="application/x-protobuf",
        headers={"Cache-Control": "public, max-age=3600"},
    )
