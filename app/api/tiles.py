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


PARCEL_TILE_TABLE = _safe_identifier(
    getattr(settings, "PARCEL_TILE_TABLE", "public.ms_buildings_raw"), "public.ms_buildings_raw"
)
PARCEL_SIMPLIFY_TOLERANCE_M = getattr(settings, "PARCEL_SIMPLIFY_TOLERANCE_M", 1.0)
PARCEL_ENVELOPE_PAD_M = getattr(settings, "PARCEL_ENVELOPE_PAD_M", 5.0)


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
        -- IMPORTANT: Filter and clip in the same CRS for stable MVT output.
        -- Transform once to WebMercator (EPSG:3857) and reuse downstream.
        ST_Transform(p.geom, 3857) AS geom3857
      FROM {SUHAIL_PARCEL_TABLE} p, tile t
      WHERE ST_Transform(p.geom, 3857) && t.geom3857
    ),
    mvtgeom AS (
      SELECT
        id,
        landuse,
        classification,
        area_m2,
        perimeter_m,
        ST_AsMVTGeom(
          -- MVT must be generated in WebMercator (EPSG:3857).
          p.geom3857,
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

def _parcel_tile_sql(simplify: bool) -> text:
    geom_expr = "p.padded3857"
    if simplify:
        geom_expr = "ST_SimplifyPreserveTopology(p.padded3857, :simplify_tol)"
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
            concat(d.building_id, ':', d.part_index) AS parcel_id,
            d.building_id,
            ST_Area(d.geom4326::geography) AS footprint_area_m2,
            ST_Transform(d.geom4326, 3857) AS geom3857
          FROM dumped d
        ),
        envelopes AS (
          SELECT
            parcel_id,
            building_id,
            footprint_area_m2,
            ST_Buffer(ST_OrientedEnvelope(geom3857), :pad_m) AS padded3857
          FROM parcels
        ),
        measured AS (
          SELECT
            parcel_id,
            building_id,
            footprint_area_m2,
            ST_Area(padded3857) AS parcel_area_m2,
            ST_Perimeter(padded3857) AS perimeter_m,
            padded3857
          FROM envelopes
        ),
        mvtgeom AS (
          SELECT
            parcel_id,
            building_id,
            footprint_area_m2,
            parcel_area_m2,
            parcel_area_m2 AS area_m2,
            perimeter_m,
            ST_AsMVTGeom(
              {geom_expr},
              t.geom3857,
              4096,
              64,
              true
            ) AS geom
          FROM measured p, tile3857 t
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
        tile_sql = _parcel_tile_sql(simplify)
        params = {"z": z, "x": x, "y": y, "pad_m": PARCEL_ENVELOPE_PAD_M}
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
        tile_bytes = db.execute(_SUHAIL_PARCEL_TILE_SQL, {"z": z, "x": x, "y": y}).scalar()
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
