from fastapi import APIRouter, HTTPException, Response
import os, pathlib
import httpx

router = APIRouter(tags=["map"])

UPSTREAM = os.getenv("TILE_UPSTREAM", "https://tile.openstreetmap.org")
UA = os.getenv("TILE_USER_AGENT", "oaktree-estimator/0.1 (contact: ops@example.com)")
CACHE_DIR = os.getenv("TILE_CACHE_DIR", "/app/tiles_cache")
OFFLINE_ONLY = os.getenv("TILE_OFFLINE_ONLY", "false").lower() in {"1","true","yes"}

def _tile_path(z: int, x: int, y: int) -> pathlib.Path:
    return pathlib.Path(CACHE_DIR) / str(z) / str(x) / f"{y}.png"

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
