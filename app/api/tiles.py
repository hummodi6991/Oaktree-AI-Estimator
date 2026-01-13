from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.deps import get_db

router = APIRouter(tags=["map"])

SUHAIL_PARCEL_TABLE = "public.suhail_parcels_mat"


def _safe_identifier(value: str | None, fallback: str) -> str:
    candidate = (value or "").strip()
    if not candidate:
        return fallback
    if all(ch.isalnum() or ch in {"_", "."} for ch in candidate):
        return candidate
    return fallback


_RAW_PARCEL_TILE_TABLE = getattr(
    settings, "PARCEL_TILE_TABLE", "public.riyadh_parcels_arcgis_proxy"
)
PARCEL_TILE_TABLE = _safe_identifier(
    _RAW_PARCEL_TILE_TABLE, "public.riyadh_parcels_arcgis_proxy"
)
PARCEL_SIMPLIFY_TOLERANCE_M = getattr(settings, "PARCEL_SIMPLIFY_TOLERANCE_M", 1.0)
_ARCGIS_PARCEL_TABLES = {
    "public.riyadh_parcels_arcgis_proxy",
    "riyadh_parcels_arcgis_proxy",
}
_NON_LANDUSE_PARCEL_TABLES = {
    "public.inferred_parcels_v1",
    "inferred_parcels_v1",
    "public.derived_parcels_v1",
    "derived_parcels_v1",
}


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
          c.geom3857,
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


def _safe_column(value: str | None, fallback: str) -> str:
    candidate = (value or "").strip()
    if not candidate:
        return fallback
    if all(ch.isalnum() or ch == "_" for ch in candidate):
        return candidate
    return fallback


def _generic_parcel_tile_sql(table_name: str, simplify: bool, id_col: str = "id") -> text:
    safe_id_col = _safe_column(id_col, "id")
    if table_name in _NON_LANDUSE_PARCEL_TABLES:
        landuse_col = "NULL::text"
        classification_col = "NULL::text"
    elif table_name in _ARCGIS_PARCEL_TABLES:
        landuse_col = "p.landuse_label"
        classification_col = "p.landuse_code"
    else:
        landuse_col = "p.landuse"
        classification_col = "p.classification"
    geom_expr = "p.geom3857"
    if simplify:
        geom_expr = "ST_SimplifyPreserveTopology(p.geom3857, :simplify_tol)"
    return text(
        f"""
        WITH tile AS (
          SELECT ST_SetSRID(ST_TileEnvelope(:z,:x,:y), 3857) AS geom3857
        ),
        tile4326 AS (
          SELECT ST_Transform(geom3857, 4326) AS geom
          FROM tile
        ),
        parcel_candidates AS (
          SELECT
            p.{safe_id_col} AS id,
            {landuse_col} AS landuse,
            {classification_col} AS classification,
            p.area_m2,
            p.perimeter_m,
            ST_Transform(p.geom, 3857) AS geom3857
          FROM {table_name} p, tile4326 t
          WHERE p.geom && t.geom
            AND ST_Intersects(p.geom, t.geom)
        ),
        simplified AS (
          SELECT
            id,
            landuse,
            classification,
            area_m2,
            perimeter_m,
            {geom_expr} AS geom3857
          FROM parcel_candidates p
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
              c.geom3857,
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


@router.get("/tiles/parcels/{z}/{x}/{y}.pbf")
@router.get("/v1/tiles/parcels/{z}/{x}/{y}.pbf")
def parcel_tile(z: int, x: int, y: int, db: Session = Depends(get_db)):
    if z < 16:
        return Response(status_code=204)

    simplify = z == 16
    try:
        if PARCEL_TILE_TABLE in ("public.inferred_parcels_v1", "inferred_parcels_v1"):
            id_col = "parcel_id"
        else:
            id_col = "id"
        tile_sql = _generic_parcel_tile_sql(PARCEL_TILE_TABLE, simplify, id_col=id_col)
        params = {"z": z, "x": x, "y": y}
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
