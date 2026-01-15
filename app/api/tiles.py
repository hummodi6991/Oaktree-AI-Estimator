import os

import mapbox_vector_tile
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


def _empty_mvt(layer_name: str = "parcels") -> bytes:
    """Return a valid empty MVT (MapLibre must receive 200, not 204)."""
    # mapbox_vector_tile.encode expects a list of layers in the form:
    # [{"name": "<layer>", "features": [...]}]
    # Passing a dict can trigger KeyError: 'name' in some versions.
    return mapbox_vector_tile.encode([{"name": layer_name, "features": []}])


# --- Dynamic parcel table proxy (TEST-COMPATIBLE) ---
def _get_parcel_tile_table() -> str:
    # Prefer raw env so tests that monkeypatch env + reload tiles pass reliably.
    env_val = os.getenv("PARCEL_TILE_TABLE")
    return _safe_identifier(
        env_val if env_val is not None else getattr(settings, "PARCEL_TILE_TABLE", None),
        "public.riyadh_parcels_arcgis_proxy",
    )


# IMPORTANT:
# - Tests expect tiles.PARCEL_TILE_TABLE to exist
# - This must NOT be frozen logic-wise
# - Handler will re-read settings again
PARCEL_TILE_TABLE = _get_parcel_tile_table()

PARCEL_SIMPLIFY_TOLERANCE_M = getattr(settings, "PARCEL_SIMPLIFY_TOLERANCE_M", 1.0)
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


def _generic_parcel_tile_sql(
    table_name: str,
    id_col: str = "id",
    simplify_tol: float | None = None,
    min_area_m2: int | None = None,
) -> text:
    safe_id_col = _safe_column(id_col, "id")
    if table_name in _NON_LANDUSE_PARCEL_TABLES:
        landuse_col = "NULL::text"
        classification_col = "NULL::text"
    elif "arcgis" in table_name.lower():
        landuse_col = "p.landuse_label"
        classification_col = "p.landuse_code"
    else:
        landuse_col = "p.landuse"
        classification_col = "p.classification"
    geom_expr = "p.geom3857"
    if simplify_tol is not None and simplify_tol > 0:
        geom_expr = "ST_SimplifyPreserveTopology(p.geom3857, :simplify_tol)"
    area_filter = ""
    if min_area_m2 is not None:
        area_filter = "AND (p.area_m2 >= :min_area_m2 OR p.area_m2 IS NULL)"
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
            {area_filter}
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


def _arcgis_tile_generalization(z: int) -> tuple[float | None, int | None]:
    if z <= 10:
        return 120.0, 50
    if z <= 12:
        return 60.0, 50
    if z <= 14:
        return 20.0, 50
    if z == 15:
        return 8.0, None
    if z == 16:
        return 3.0, None
    return None, None


@router.get("/tiles/parcels/{z}/{x}/{y}.pbf")
@router.get("/v1/tiles/parcels/{z}/{x}/{y}.pbf")
def parcel_tile(z: int, x: int, y: int, db: Session = Depends(get_db)):
    # Resolve parcel table (test- and runtime-safe)
    raw_table = PARCEL_TILE_TABLE or ""
    parcel_table = _safe_identifier(
        raw_table if raw_table.strip() else _get_parcel_tile_table(),
        "public.riyadh_parcels_arcgis_proxy",
    )
    simplify_default = getattr(settings, "PARCEL_SIMPLIFY_TOLERANCE_M", 1.0)

    # ArcGIS mode must be derived from the EFFECTIVE table, not the monkeypatched symbol
    arcgis_mode = "arcgis" in parcel_table.lower()
    try:
        if parcel_table in ("public.inferred_parcels_v1", "inferred_parcels_v1"):
            id_col = "parcel_id"
        else:
            id_col = "id"
        params = {"z": z, "x": x, "y": y}
        if arcgis_mode:
            # ArcGIS parcels are authoritative cadastral data:
            # never aggressively area-filter them, or tiles become empty.
            simplify_tol, min_area_m2 = _arcgis_tile_generalization(z)
            tile_sql = _generic_parcel_tile_sql(
                parcel_table,
                id_col=id_col,
                simplify_tol=simplify_tol,
                min_area_m2=min_area_m2,
            )
            if simplify_tol is not None:
                params["simplify_tol"] = simplify_tol
            if min_area_m2 is not None:
                params["min_area_m2"] = min_area_m2
        else:
            simplify = z == 16
            tile_sql = _generic_parcel_tile_sql(
                parcel_table,
                id_col=id_col,
                simplify_tol=simplify_default if simplify else None,
            )
            if simplify:
                params["simplify_tol"] = simplify_default
        tile_bytes = db.execute(tile_sql, params).scalar()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to render parcel tile: {exc}")

    payload = bytes(tile_bytes or b"")
    if not payload:
        return Response(
            _empty_mvt("parcels"),
            media_type="application/x-protobuf",
            headers={"Cache-Control": "public, max-age=3600"},
        )

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
