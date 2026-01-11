from __future__ import annotations

import argparse
import logging
import sys
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.db.session import SessionLocal

DEFAULT_BBOX = (46.20, 24.20, 47.30, 25.10)
DEFAULT_ROAD_BUF_M = 9.0
DEFAULT_MIN_BLOCK_AREA_M2 = 5000.0
DEFAULT_MAX_BUILDINGS_PER_BLOCK = 4000
DEFAULT_SEED_PART_INDEX = 1

logger = logging.getLogger(__name__)


def _build_arg_parser() -> argparse.ArgumentParser:
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
    return parser


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


def _resolve_roads_source(db) -> tuple[str, str]:
    if db.execute(text("SELECT to_regclass('public.osm_roads_line')")).scalar() is not None:
        return "public.osm_roads_line", "geom"
    if db.execute(text("SELECT to_regclass('public.planet_osm_line')")).scalar() is not None:
        return "public.planet_osm_line", "way"
    raise RuntimeError("Missing roads source: expected public.osm_roads_line or public.planet_osm_line")


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    try:
        bbox = _parse_bbox(args.bbox)
    except ValueError as exc:
        logger.error("Invalid bbox: %s", exc)
        return 2

    with SessionLocal() as db:
        try:
            roads_table, roads_geom_col = _resolve_roads_source(db)
        except RuntimeError as exc:
            logger.error("%s", exc)
            return 1

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
                    geom geometry(Polygon,3857)
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
                    seed geometry(Point,3857),
                    footprint_area_m2 double precision
                );
                """
            )
        )
        db.execute(text("CREATE INDEX tmp_seeds_seed_gix ON tmp_seeds USING GIST (seed);"))

        xmin, ymin, xmax, ymax = bbox
        highway_filter = (
            "r.highway IS NOT NULL AND r.highway NOT IN "
            "('footway','path','cycleway','steps')"
        )
        geom_expr = f"r.{roads_geom_col}"

        roads_count = (
            db.execute(
                text(
                    f"""
                    WITH bbox AS (
                      SELECT ST_Transform(ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326), 3857) AS geom
                    )
                    SELECT COUNT(*)
                    FROM {roads_table} r, bbox b
                    WHERE {geom_expr} && b.geom
                      AND {highway_filter};
                    """
                ),
                {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax},
            ).scalar()
            or 0
        )
        logger.info("Using %s for road mask (filtered count=%s)", roads_table, roads_count)

        db.execute(
            text(
                f"""
                WITH bbox AS (
                  SELECT
                    ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326) AS geom4326,
                    ST_Transform(ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326), 3857) AS geom3857
                ),
                roads AS (
                  SELECT {geom_expr} AS geom
                  FROM {roads_table} r, bbox b
                  WHERE {geom_expr} && b.geom3857
                    AND {highway_filter}
                ),
                road_mask AS (
                  SELECT ST_UnaryUnion(
                    ST_Collect(ST_Buffer(r.geom, :road_buf_m))
                  ) AS geom
                  FROM roads r
                ),
                free AS (
                  SELECT ST_Difference(
                    b.geom3857,
                    COALESCE(r.geom, ST_GeomFromText('POLYGON EMPTY',3857))
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
                  md5(ST_AsBinary(ST_SnapToGrid(geom, 1.0))) AS block_id,
                  geom
                FROM blocks
                WHERE ST_Area(geom) >= :min_block_area_m2;
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

        blocks_count = db.execute(text("SELECT COUNT(*) FROM tmp_blocks")).scalar() or 0
        if blocks_count:
            area_row = (
                db.execute(
                    text(
                        "SELECT MIN(ST_Area(geom)) AS min_area, "
                        "MAX(ST_Area(geom)) AS max_area FROM tmp_blocks"
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
        logger.info("Blocks count: %d", blocks_count)
        if blocks_count <= 1:
            logger.error(
                "Road mask failed to partition bbox; check OSM import and road mask inputs"
            )
            return 1

        sql_blocks = "SELECT block_id, ST_AsEWKB(geom) AS geom FROM tmp_blocks ORDER BY block_id"
        params = {}
        if args.max_blocks is not None:
            sql_blocks += " LIMIT :max_blocks"
            params["max_blocks"] = args.max_blocks
        block_rows = db.execute(text(sql_blocks), params).mappings()

        for idx, block in enumerate(block_rows, start=1):
            block_id = block["block_id"]
            block_geom = block["geom"]
            logger.info("Block %d/%d (%s)", idx, blocks_count, block_id)

            db.execute(text("TRUNCATE tmp_seeds"))
            db.execute(
                text(
                    """
                    INSERT INTO tmp_seeds (building_id, part_index, seed, footprint_area_m2)
                    SELECT
                      b.id AS building_id,
                      (d).path[1] AS part_index,
                      ST_Transform(ST_PointOnSurface((d).geom), 3857) AS seed,
                      ST_Area((d).geom::geography) AS footprint_area_m2
                    FROM public.ms_buildings_raw b
                    CROSS JOIN LATERAL ST_Dump(b.geom) AS d
                    WHERE b.geom && ST_Transform(ST_GeomFromEWKB(:block_geom), 4326)
                      AND ST_Intersects((d).geom, ST_Transform(ST_GeomFromEWKB(:block_geom), 4326))
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
                      SELECT ST_GeomFromEWKB(:block_geom) AS geom3857,
                             :block_id AS block_id
                    ),
                    seeds AS (
                      SELECT building_id,
                             part_index,
                             footprint_area_m2,
                             seed AS seed3857
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
