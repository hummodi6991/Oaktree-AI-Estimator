from __future__ import annotations

import argparse
import logging
import sys
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.db.session import SessionLocal

DEFAULT_BBOX = (46.20, 24.20, 47.30, 25.10)
DEFAULT_ROAD_BUF_M = 9.0
DEFAULT_MIN_BLOCK_AREA_M2 = 5000.0
DEFAULT_MAX_BUILDINGS_PER_BLOCK = 4000
DEFAULT_SEED_PART_INDEX = 1
MIN_ROAD_COUNT = 10000

logger = logging.getLogger(__name__)


def _parse_bbox(value: str | None) -> tuple[float, float, float, float]:
    if not value:
        return DEFAULT_BBOX
    parts = [p.strip() for p in value.split(",") if p.strip()]
    if len(parts) != 4:
        raise ValueError("bbox must be xmin,ymin,xmax,ymax")
    try:
        xmin, ymin, xmax, ymax = map(float, parts)
    except ValueError as exc:
        raise ValueError("bbox values must be numeric") from exc
    if xmin > xmax:
        xmin, xmax = xmax, xmin
    if ymin > ymax:
        ymin, ymax = ymax, ymin
    return xmin, ymin, xmax, ymax


def _table_row_count(db, table: str) -> int | None:
    exists = db.execute(text("SELECT to_regclass(:table_name)"), {"table_name": table}).scalar()
    if exists is None:
        return None
    return db.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0


def _resolve_roads_table(db) -> tuple[str, int]:
    roads_count = _table_row_count(db, "public.planet_osm_roads")
    if roads_count is not None and roads_count >= MIN_ROAD_COUNT:
        return "public.planet_osm_roads", roads_count
    line_count = _table_row_count(db, "public.planet_osm_line")
    if line_count is not None and line_count >= MIN_ROAD_COUNT:
        return "public.planet_osm_line", line_count
    raise RuntimeError(
        "OSM roads import incomplete: expected planet_osm_roads or planet_osm_line "
        f"to have at least {MIN_ROAD_COUNT} rows "
        f"(roads={roads_count}, line={line_count})"
    )


def _resolve_highway_expr(db, table: str, alias: str) -> str:
    table_name = table.split(".")[-1]
    rows = db.execute(
        text(
            """
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = :table_name
              AND column_name IN ('highway', 'tags')
            """
        ),
        {"table_name": table_name},
    ).mappings().all()
    columns = {row["column_name"]: row for row in rows}
    if "highway" in columns:
        return f"{alias}.highway"
    if "tags" in columns:
        tags_info = columns["tags"]
        data_type = tags_info.get("data_type")
        udt_name = tags_info.get("udt_name")
        if data_type == "jsonb" or udt_name == "jsonb":
            return f"{alias}.tags->>'highway'"
        if udt_name == "hstore" or data_type == "hstore":
            return f"{alias}.tags->'highway'"
        raise RuntimeError(f"{table_name}.tags column has unsupported type: {data_type}/{udt_name}")
    raise RuntimeError(f"{table_name} missing highway/tags column")


def _iter_blocks(db, max_blocks: int | None) -> Iterable[dict]:
    sql = "SELECT block_id, ST_AsEWKB(geom) AS geom FROM tmp_blocks ORDER BY block_id"
    params = {}
    if max_blocks is not None:
        sql += " LIMIT :max_blocks"
        params["max_blocks"] = max_blocks
    return db.execute(text(sql), params).mappings().all()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build inferred_parcels_v1 from roads + buildings")
    parser.add_argument(
        "--bbox",
        default=",".join(str(v) for v in DEFAULT_BBOX),
        help="xmin,ymin,xmax,ymax in WGS84",
    )
    parser.add_argument("--road-buf-m", type=float, default=DEFAULT_ROAD_BUF_M)
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--max-blocks", type=int, default=None)
    parser.add_argument("--min-block-area-m2", type=float, default=DEFAULT_MIN_BLOCK_AREA_M2)
    parser.add_argument("--max-buildings-per-block", type=int, default=DEFAULT_MAX_BUILDINGS_PER_BLOCK)
    parser.add_argument("--seed-part-index", type=int, default=DEFAULT_SEED_PART_INDEX)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    try:
        bbox = _parse_bbox(args.bbox)
    except ValueError as exc:
        logger.error("Invalid bbox: %s", exc)
        return 2

    with SessionLocal() as db:
        try:
            roads_table, roads_count = _resolve_roads_table(db)
            highway_expr = _resolve_highway_expr(db, roads_table, "r")
        except RuntimeError as exc:
            logger.error("%s", exc)
            return 1
        logger.info("Using %s (%s rows) for road mask", roads_table, roads_count)

        if args.truncate:
            logger.info("Truncating inferred_parcels_v1")
            db.execute(text("TRUNCATE TABLE public.inferred_parcels_v1"))
            db.commit()

        db.execute(text("DROP TABLE IF EXISTS tmp_blocks"))
        db.execute(text("DROP TABLE IF EXISTS tmp_seeds"))
        db.execute(
            text(
                """
                CREATE TEMP TABLE tmp_blocks (
                    block_id text,
                    geom geometry(Polygon,4326)
                );
                """
            )
        )
        db.execute(
            text(
                """
                CREATE TEMP TABLE tmp_seeds (
                    building_id bigint,
                    part_index int,
                    seed geometry(Point,4326),
                    footprint_area_m2 double precision
                );
                """
            )
        )
        db.execute(text("CREATE INDEX tmp_seeds_seed_gix ON tmp_seeds USING GIST (seed);"))

        xmin, ymin, xmax, ymax = bbox
        db.execute(
            text(
                f"""
                WITH bbox AS (
                  SELECT ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326) AS geom
                ),
                road_mask AS (
                  SELECT ST_UnaryUnion(
                    ST_Collect(ST_Buffer(r.way::geography, :road_buf_m)::geometry)
                  ) AS geom
                  FROM {roads_table} r, bbox b
                  WHERE r.way && b.geom
                    AND {highway_expr} IS NOT NULL
                ),
                free AS (
                  SELECT ST_Difference(
                    b.geom,
                    COALESCE(r.geom, ST_GeomFromText('POLYGON EMPTY',4326))
                  ) AS geom
                  FROM bbox b
                  LEFT JOIN road_mask r ON TRUE
                ),
                blocks AS (
                  SELECT (ST_Dump(ST_Multi(ST_MakeValid(f.geom)))).geom AS geom
                  FROM free f
                  WHERE f.geom IS NOT NULL
                )
                INSERT INTO tmp_blocks (block_id, geom)
                SELECT
                  md5(ST_AsBinary(ST_SnapToGrid(geom, 0.000001))) AS block_id,
                  geom
                FROM blocks
                WHERE ST_Area(geom::geography) >= :min_block_area_m2;
                """
            ),
            {
                "xmin": xmin,
                "ymin": ymin,
                "xmax": xmax,
                "ymax": ymax,
                "road_buf_m": args.road_buf_m,
                "min_block_area_m2": args.min_block_area_m2,
            },
        )
        db.commit()

        blocks = list(_iter_blocks(db, args.max_blocks))
        if blocks:
            area_row = (
                db.execute(
                    text(
                        "SELECT MIN(ST_Area(geom::geography)) AS min_area, "
                        "MAX(ST_Area(geom::geography)) AS max_area FROM tmp_blocks"
                    )
                )
                .mappings()
                .one()
            )
            logger.info(
                "Block areas: min=%.0f m2 max=%.0f m2",
                area_row.get("min_area") or 0.0,
                area_row.get("max_area") or 0.0,
            )
        if len(blocks) <= 1:
            logger.error(
                "Road mask failed to partition bbox; check OSM import counts and highway expr"
            )
            return 1
        logger.info("Processing %d blocks", len(blocks))

        for idx, block in enumerate(blocks, start=1):
            block_id = block["block_id"]
            block_geom = block["geom"]
            logger.info("Block %d/%d (%s)", idx, len(blocks), block_id)

            db.execute(text("TRUNCATE tmp_seeds"))
            db.execute(
                text(
                    """
                    INSERT INTO tmp_seeds (building_id, part_index, seed, footprint_area_m2)
                    SELECT
                      b.id AS building_id,
                      (d).path[1] AS part_index,
                      ST_PointOnSurface((d).geom) AS seed,
                      ST_Area((d).geom::geography) AS footprint_area_m2
                    FROM public.ms_buildings_raw b
                    CROSS JOIN LATERAL ST_Dump(b.geom) AS d
                    WHERE b.geom && ST_GeomFromEWKB(:block_geom)
                      AND ST_Intersects((d).geom, ST_GeomFromEWKB(:block_geom))
                      AND (d).path[1] = :seed_part_index
                    """
                ),
                {"block_geom": block_geom, "seed_part_index": args.seed_part_index},
            )

            seed_count = db.execute(text("SELECT COUNT(*) FROM tmp_seeds")).scalar() or 0
            if seed_count == 0:
                logger.info("Skipping block %s: no buildings", block_id)
                db.commit()
                continue
            if seed_count > args.max_buildings_per_block:
                logger.warning(
                    "Skipping block %s: %s buildings exceeds limit %s",
                    block_id,
                    seed_count,
                    args.max_buildings_per_block,
                )
                db.commit()
                continue

            db.execute(
                text(
                    """
                    WITH block AS (
                      SELECT ST_Transform(ST_GeomFromEWKB(:block_geom), 3857) AS geom3857,
                             :block_id AS block_id
                    ),
                    seeds AS (
                      SELECT building_id,
                             part_index,
                             footprint_area_m2,
                             ST_Transform(seed, 3857) AS seed3857
                      FROM tmp_seeds
                    ),
                    env AS (
                      SELECT ST_Envelope((SELECT geom3857 FROM block)) AS env
                    ),
                    v AS (
                      SELECT (ST_Dump(ST_VoronoiPolygons(ST_Collect(seed3857), 0.0, (SELECT env FROM env)))).geom AS cell
                      FROM seeds
                    ),
                    cells AS (
                      SELECT
                        v.cell,
                        s.building_id,
                        s.part_index,
                        s.footprint_area_m2
                      FROM v
                      JOIN seeds s ON ST_Contains(v.cell, s.seed3857)
                    ),
                    parcels AS (
                      SELECT
                        c.building_id,
                        c.part_index,
                        c.footprint_area_m2,
                        ST_MakeValid(ST_Intersection(c.cell, b.geom3857)) AS geom3857
                      FROM cells c, block b
                    ),
                    final AS (
                      SELECT
                        concat('ms:', building_id, ':', part_index) AS parcel_id,
                        building_id,
                        part_index,
                        ST_Transform(geom3857, 4326) AS geom,
                        footprint_area_m2,
                        'road_block_voronoi_v1'::text AS method,
                        :block_id AS block_id
                      FROM parcels
                      WHERE geom3857 IS NOT NULL AND NOT ST_IsEmpty(geom3857)
                    )
                    INSERT INTO public.inferred_parcels_v1 (
                      parcel_id,
                      building_id,
                      part_index,
                      geom,
                      area_m2,
                      perimeter_m,
                      footprint_area_m2,
                      method,
                      block_id
                    )
                    SELECT
                      parcel_id,
                      building_id,
                      part_index,
                      geom,
                      ST_Area(geom::geography) AS area_m2,
                      ST_Perimeter(geom::geography) AS perimeter_m,
                      footprint_area_m2,
                      method,
                      block_id
                    FROM final
                    ON CONFLICT(parcel_id) DO UPDATE SET
                      geom = EXCLUDED.geom,
                      area_m2 = EXCLUDED.area_m2,
                      perimeter_m = EXCLUDED.perimeter_m,
                      footprint_area_m2 = EXCLUDED.footprint_area_m2,
                      method = EXCLUDED.method,
                      block_id = EXCLUDED.block_id,
                      created_at = now();
                    """
                ),
                {"block_geom": block_geom, "block_id": block_id},
            )
            db.commit()

        total = db.execute(text("SELECT COUNT(*) FROM public.inferred_parcels_v1")).scalar()
        logger.info("Inferred parcel total: %s", total or 0)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SQLAlchemyError as exc:
        logger.error("Database error: %s", exc)
        raise SystemExit(1)
