import logging

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.deps import get_db

router = APIRouter(tags=["map"])

SUHAIL_PARCEL_TABLE = "public.suhail_parcels_mat"
INFERRED_PARCEL_TABLE = "public.inferred_parcels_v1"
_INFERRED_PARCEL_TABLES = {INFERRED_PARCEL_TABLE, "inferred_parcels_v1"}
_HAS_INFERRED_PARCELS: bool | None = None

logger = logging.getLogger(__name__)


def _safe_identifier(value: str | None, fallback: str) -> str:
    candidate = (value or "").strip()
    if not candidate:
        return fallback
    if all(ch.isalnum() or ch in {"_", "."} for ch in candidate):
        return candidate
    return fallback


PARCEL_TILE_TABLE = _safe_identifier(
    getattr(settings, "PARCEL_TILE_TABLE", "public.ms_buildings_raw"),
    "public.ms_buildings_raw",
)
PARCEL_SIMPLIFY_TOLERANCE_M = getattr(settings, "PARCEL_SIMPLIFY_TOLERANCE_M", 1.0)
PARCEL_TILE_MIN_AREA_M2 = getattr(settings, "PARCEL_TILE_MIN_AREA_M2", 40.0)


_SUHAIL_PARCEL_TILE_SQL = text(
    f"""
    WITH tile AS (
      SELECT ST_SetSRID(ST_TileEnvelope(:z,:x,:y), 3857) AS geom3857
    ),
    parcel_candidates AS (
      SELECT
        p.id,
        p.landuse,
        p.classification,
        p.area_m2,
        p.perimeter_m,
        ST_Transform(p.geom, 3857) AS geom3857
      FROM {SUHAIL_PARCEL_TABLE} p, tile t
      WHERE ST_Transform(p.geom, 3857) && t.geom3857
    ),
    simplified AS (
      SELECT
        id,
        landuse,
        classification,
        area_m2,
        perimeter_m,
        ST_SimplifyPreserveTopology(geom3857, :simplify_tol) AS geom3857
      FROM parcel_candidates
    ),
    clipped AS (
      SELECT
        s.id,
        s.landuse,
        s.classification,
        s.area_m2,
        s.perimeter_m,
        ST_Intersection(s.geom3857, t.geom3857) AS geom3857
      FROM simplified s, tile t
      WHERE ST_Intersects(s.geom3857, t.geom3857)
    ),
    mvtgeom AS (
      SELECT
        id,
        landuse,
        classification,
        area_m2,
        perimeter_m,
        ST_AsMVTGeom(
          geom3857,
          t.geom3857,
          4096,
          64,
          true
        ) AS geom
      FROM clipped c, tile t
    )
    SELECT ST_AsMVT(mvtgeom, 'parcels', 4096, 'geom') AS tile
    FROM mvtgeom;
    """
)

_SUHAIL_PARCEL_ROT_TILE_SQL = text(
    f"""
    WITH tile AS (
      SELECT ST_SetSRID(ST_TileEnvelope(:z,:x,:y), 3857) AS geom3857
    ),
    pivot AS (
      SELECT ST_Transform(ST_SetSRID(ST_Point(:pivot_lng,:pivot_lat), 4326), 3857) AS geom3857
    ),
    parcel_candidates AS (
      SELECT
        p.id,
        p.landuse,
        p.classification,
        p.area_m2,
        p.perimeter_m,
        ST_Transform(p.geom, 3857) AS geom3857
      FROM {SUHAIL_PARCEL_TABLE} p, tile t
      WHERE ST_Transform(p.geom, 3857) && t.geom3857
    ),
    rotated_parcels AS (
      SELECT
        p.id,
        p.landuse,
        p.classification,
        p.area_m2,
        p.perimeter_m,
        ST_Rotate(p.geom3857, radians(:deg), pv.geom3857) AS geom3857
      FROM parcel_candidates p, pivot pv
    ),
    mvtgeom AS (
      SELECT
        p.id,
        p.landuse,
        p.classification,
        p.area_m2,
        p.perimeter_m,
        ST_AsMVTGeom(
          p.geom3857,
          t.geom3857,
          4096,
          64,
          true
        ) AS geom
      FROM rotated_parcels p, tile t
      WHERE p.geom3857 && t.geom3857
    )
    SELECT ST_AsMVT(mvtgeom, 'parcels', 4096, 'geom') AS tile
    FROM mvtgeom;
    """
)

def _has_inferred_parcels(db: Session) -> bool:
    global _HAS_INFERRED_PARCELS
    if _HAS_INFERRED_PARCELS is not None:
        return _HAS_INFERRED_PARCELS
    try:
        row = db.execute(
            text("SELECT EXISTS (SELECT 1 FROM public.inferred_parcels_v1 LIMIT 1)")
        ).scalar()
        _HAS_INFERRED_PARCELS = bool(row)
    except Exception as exc:
        logger.warning("Failed to check inferred parcels availability: %s", exc)
        _HAS_INFERRED_PARCELS = False
    return _HAS_INFERRED_PARCELS


def _ms_buildings_tile_sql(simplify: bool) -> text:
    geom_expr = "p.geom3857"
    if simplify:
        geom_expr = "ST_SimplifyPreserveTopology(p.geom3857, :simplify_tol)"
    return text(
        f"""
        WITH tile3857 AS (
          SELECT ST_SetSRID(ST_TileEnvelope(:z,:x,:y), 3857) AS geom3857
        ),
        tile4326 AS (
          SELECT ST_Transform(t.geom3857, 4326) AS geom4326 FROM tile3857 t
        ),
        parcel_candidates AS (
          SELECT
            b.id AS building_id,
            b.geom AS geom4326
          FROM {PARCEL_TILE_TABLE} b, tile4326 t
          WHERE b.geom && t.geom4326
            AND ST_Intersects(b.geom, t.geom4326)
        ),
        dumped AS (
          SELECT
            c.building_id,
            (d).path[1] AS part_index,
            (d).geom AS geom4326
          FROM parcel_candidates c
          CROSS JOIN LATERAL ST_Dump(c.geom4326) AS d
        ),
        parcels AS (
          SELECT
            concat('ms:', d.building_id, ':', d.part_index) AS parcel_id,
            d.building_id,
            d.part_index,
            ST_Area(d.geom4326::geography) AS area_m2,
            ST_Area(d.geom4326::geography) AS footprint_area_m2,
            ST_Perimeter(d.geom4326::geography) AS perimeter_m,
            'ms_buildings_raw'::text AS method,
            ST_Transform(d.geom4326, 3857) AS geom3857
          FROM dumped d
        ),
        mvtgeom AS (
          SELECT
            parcel_id,
            building_id,
            part_index,
            footprint_area_m2,
            area_m2,
            perimeter_m,
            method,
            ST_AsMVTGeom(
              {geom_expr},
              t.geom3857,
              4096,
              64,
              true
            ) AS geom
          FROM parcels p, tile3857 t
        )
        SELECT ST_AsMVT(mvtgeom, 'parcels', 4096, 'geom') AS tile
        FROM mvtgeom;
        """
    )


def _inferred_parcel_tile_sql(simplify: bool) -> text:
    geom_expr = "p.geom3857"
    if simplify:
        geom_expr = "ST_SimplifyPreserveTopology(p.geom3857, :simplify_tol)"
    return text(
        f"""
        WITH tile3857 AS (
          SELECT ST_SetSRID(ST_TileEnvelope(:z,:x,:y), 3857) AS geom3857
        ),
        tile4326 AS (
          SELECT ST_Transform(t.geom3857, 4326) AS geom4326 FROM tile3857 t
        ),
        parcel_candidates AS (
          SELECT
            p.parcel_id,
            p.building_id,
            p.part_index,
            p.area_m2,
            p.perimeter_m,
            p.footprint_area_m2,
            p.method,
            ST_Transform(p.geom, 3857) AS geom3857
          FROM {INFERRED_PARCEL_TABLE} p, tile4326 t
          WHERE p.geom && t.geom4326
            AND ST_Intersects(p.geom, t.geom4326)
            AND (:z > 16 OR p.area_m2 >= :min_area_m2)
        ),
        mvtgeom AS (
          SELECT
            parcel_id,
            building_id,
            part_index,
            area_m2,
            perimeter_m,
            footprint_area_m2,
            method,
            ST_AsMVTGeom(
              {geom_expr},
              t.geom3857,
              4096,
              64,
              true
            ) AS geom
          FROM parcel_candidates p, tile3857 t
        )
        SELECT ST_AsMVT(mvtgeom, 'parcels', 4096, 'geom') AS tile
        FROM mvtgeom;
        """
    )


def _parcel_tile_sql(simplify: bool, use_inferred: bool) -> text:
    if use_inferred:
        return _inferred_parcel_tile_sql(simplify)
    return _ms_buildings_tile_sql(simplify)


@router.get("/tiles/parcels/{z}/{x}/{y}.pbf")
@router.get("/v1/tiles/parcels/{z}/{x}/{y}.pbf")
def parcel_tile(z: int, x: int, y: int, db: Session = Depends(get_db)):
    if z < 16:
        return Response(status_code=204)

    simplify = z == 16
    try:
        use_inferred = PARCEL_TILE_TABLE in _INFERRED_PARCEL_TABLES and _has_inferred_parcels(db)
        tile_sql = _parcel_tile_sql(simplify, use_inferred=use_inferred)
        params = {"z": z, "x": x, "y": y}
        if use_inferred:
            params["min_area_m2"] = PARCEL_TILE_MIN_AREA_M2
        if simplify:
            params["simplify_tol"] = PARCEL_SIMPLIFY_TOLERANCE_M
        tile_bytes = db.execute(tile_sql, params).scalar()
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


@router.get("/tiles/suhail/{z}/{x}/{y}.pbf")
@router.get("/v1/tiles/suhail/{z}/{x}/{y}.pbf")
def suhail_parcel_tile(z: int, x: int, y: int, db: Session = Depends(get_db)):
    try:
        tile_bytes = db.execute(
            _SUHAIL_PARCEL_TILE_SQL,
            {"z": z, "x": x, "y": y, "simplify_tol": PARCEL_SIMPLIFY_TOLERANCE_M},
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


@router.get("/tiles/suhail-rot/{z}/{x}/{y}.pbf")
@router.get("/v1/tiles/suhail-rot/{z}/{x}/{y}.pbf")
def suhail_parcel_rot_tile(
    z: int,
    x: int,
    y: int,
    deg: float = -20.0,
    pivot_lng: float = 46.67,
    pivot_lat: float = 24.71,
    db: Session = Depends(get_db),
):
    # Debug-only CRS validation endpoint; do not use for production calculations.
    try:
        tile_bytes = db.execute(
            _SUHAIL_PARCEL_ROT_TILE_SQL,
            {
                "z": z,
                "x": x,
                "y": y,
                "deg": deg,
                "pivot_lng": pivot_lng,
                "pivot_lat": pivot_lat,
            },
        ).scalar()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to render suhail rotated tile: {exc}")

    payload = bytes(tile_bytes or b"")
    if not payload:
        return Response(status_code=204)

    return Response(
        payload,
        media_type="application/x-protobuf",
        headers={"Cache-Control": "no-store"},
    )
