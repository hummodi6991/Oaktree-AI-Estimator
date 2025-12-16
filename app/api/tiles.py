from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session
import os, pathlib
import httpx

from app.db.deps import get_db

router = APIRouter(tags=["map"])

UPSTREAM = os.getenv("TILE_UPSTREAM", "https://tile.openstreetmap.org")
UA = os.getenv("TILE_USER_AGENT", "oaktree-estimator/0.1 (contact: ops@example.com)")
CACHE_DIR = os.getenv("TILE_CACHE_DIR", "/app/tiles_cache")
OFFLINE_ONLY = os.getenv("TILE_OFFLINE_ONLY", "false").lower() in {"1","true","yes"}

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
        ST_AsMVTGeom(geom, bounds.geom, 4096, 64, true) AS geom
      FROM overture_buildings, bounds
      WHERE geom && bounds.geom
    )
    SELECT ST_AsMVT(mvtgeom, 'buildings', 4096, 'geom') AS tile
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
