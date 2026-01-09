from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.deps import get_db

router = APIRouter(tags=["map"])

SUHAIL_PARCEL_TABLE = "public.suhail_parcels_mat"


_SUHAIL_PARCEL_TILE_SQL = text(
    f"""
    WITH tile AS (
      SELECT
        tile3857,
        ST_Transform(tile3857, 32638) AS tile32638
      FROM (
        SELECT ST_SetSRID(ST_TileEnvelope(:z,:x,:y), 3857) AS tile3857
      ) t
    ),
    parcel_candidates AS (
      SELECT
        p.id,
        p.landuse,
        p.classification,
        p.area_m2,
        p.perimeter_m,
        p.geom_32638,
        CASE
          WHEN :z <= 15 THEN ST_SimplifyPreserveTopology(p.geom_32638, :simp_z15)
          WHEN :z = 16 THEN ST_SimplifyPreserveTopology(p.geom_32638, :simp_z16)
          ELSE p.geom_32638
        END AS geom_s
      FROM {SUHAIL_PARCEL_TABLE} p, tile t
      WHERE p.geom_32638 && t.tile32638
        AND ST_Intersects(p.geom_32638, t.tile32638)
        AND (
          :z >= 17
          OR (:z = 16 AND p.area_m2 >= :min_area_z16)
          OR (:z <= 15 AND p.area_m2 >= :min_area_z15)
        )
    ),
    mvtgeom AS (
      SELECT
        id,
        landuse,
        classification,
        area_m2,
        perimeter_m,
        ST_AsMVTGeom(
          ST_Transform(p.geom_s, 3857),
          t.tile3857,
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


@router.get("/tiles/parcels/{z}/{x}/{y}.pbf")
@router.get("/v1/tiles/parcels/{z}/{x}/{y}.pbf")
@router.get("/tiles/suhail/{z}/{x}/{y}.pbf")
@router.get("/v1/tiles/suhail/{z}/{x}/{y}.pbf")
def suhail_parcel_tile(z: int, x: int, y: int, db: Session = Depends(get_db)):
    try:
        tile_bytes = db.execute(
            _SUHAIL_PARCEL_TILE_SQL,
            {
                "z": z,
                "x": x,
                "y": y,
                "min_area_z15": settings.SUHAIL_TILE_MIN_AREA_Z15,
                "min_area_z16": settings.SUHAIL_TILE_MIN_AREA_Z16,
                "simp_z15": settings.SUHAIL_TILE_SIMPLIFY_Z15,
                "simp_z16": settings.SUHAIL_TILE_SIMPLIFY_Z16,
            },
        ).scalar()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to render suhail parcel tile: {exc}")

    payload = bytes(tile_bytes or b"")
    if not payload:
        return Response(status_code=204)

    return Response(
        payload,
        media_type="application/x-protobuf",
        headers={"Cache-Control": "public, max-age=3600"},
    )
