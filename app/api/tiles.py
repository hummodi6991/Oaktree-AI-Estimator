from fastapi import APIRouter, HTTPException, Response
import os
import httpx

router = APIRouter(tags=["map"])

UPSTREAM = os.getenv("TILE_UPSTREAM", "https://tile.openstreetmap.org")
UA = os.getenv(
    "TILE_USER_AGENT", "oaktree-estimator/0.1 (contact: ops@example.com)"
)


@router.get("/tiles/{z}/{x}/{y}.png")
def tile(z: int, x: int, y: int):
    url = f"{UPSTREAM}/{z}/{x}/{y}.png"
    try:
        r = httpx.get(url, headers={"User-Agent": UA}, timeout=15.0)
        r.raise_for_status()
    except Exception as exc:  # pragma: no cover - passthrough for failure modes
        raise HTTPException(status_code=502, detail=f"tile upstream error: {exc}")
    # cache for a day to be kind to the upstream
    return Response(
        r.content,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=86400"},
    )
