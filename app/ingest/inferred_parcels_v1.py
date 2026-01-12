from __future__ import annotations

import argparse
import hashlib
import logging
import os
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
DEFAULT_SUBBLOCK_SIZE_M = 500
DEFAULT_SUBBLOCK_MAX_BUILDINGS = 1500
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
    configured_table = os.getenv("INFERRED_PARCELS_ROADS_TABLE", "public.osm_roads_line")
    roads_count = _table_row_count(db, configured_table)
    if roads_count is None:
        if configured_table == "public.osm_roads_line":
            fallback_table = "public.planet_osm_line"
            fallback_count = _table_row_count(db, fallback_table)
            if fallback_count is not None and fallback_count >= MIN_ROAD_COUNT:
                return fallback_table, fallback_count
            raise RuntimeError(
                "OSM roads import incomplete: expected public.osm_roads_line view or "
                f"{fallback_table} to have at least {MIN_ROAD_COUNT} rows "
                f"(view=None, line={fallback_count})"
            )
        raise RuntimeError(f"OSM roads table not found: {configured_table}")
    if roads_count < MIN_ROAD_COUNT:
        raise RuntimeError(
            f"OSM roads import incomplete: expected {configured_table} to have at least "
            f"{MIN_ROAD_COUNT} rows (roads={roads_count})"
        )
    return configured_table, roads_count


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


def _resolve_roads_geom_column(db, table: str) -> str:
    if "." in table:
        table_schema, table_name = table.split(".", 1)
    else:
        table_schema, table_name = "public", table
    rows = db.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :table_schema
              AND table_name = :table_name
              AND column_name IN ('geom', 'way')
            """
        ),
        {"table_schema": table_schema, "table_name": table_name},
    ).mappings().all()
    columns = {row["column_name"] for row in rows}
    if "geom" in columns:
        return "geom"
    if "way" in columns:
        return "way"
    raise RuntimeError(f"{table} missing geom/way geometry column")


def _iter_blocks(db, run_id: str, max_blocks: int | None) -> Iterable[dict]:
    sql = (
        "SELECT b.block_id, ST_AsEWKB(b.geom) AS geom "
        "FROM tmp_blocks_with_buildings b "
        "LEFT JOIN public.inferred_parcels_v1_done_blocks d "
        "  ON d.run_id = :run_id AND d.block_id = b.block_id "
        "WHERE d.block_id IS NULL "
        "ORDER BY b.block_id"
    )
    params = {"run_id": run_id}
    if max_blocks is not None:
        sql += " LIMIT :max_blocks"
        params["max_blocks"] = max_blocks
    return db.execute(text(sql), params).mappings().all()


def _run_id_from_params(
    bbox: tuple[float, float, float, float],
    road_buf_m: float,
    min_block_area_m2: float,
    seed_part_index: int,
    max_buildings_per_block: int,
) -> str:
    payload = "|".join(
        [
            ",".join(f"{value:.6f}" for value in bbox),
            f"{road_buf_m:.3f}",
            f"{min_block_area_m2:.3f}",
            str(seed_part_index),
            str(max_buildings_per_block),
        ]
    )
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def _ensure_progress_tables(db) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS public.inferred_parcels_v1_progress (
              run_id text PRIMARY KEY,
              bbox text,
              road_buf_m double precision,
              min_block_area_m2 double precision,
              seed_part_index int,
              max_buildings_per_block int,
              started_at timestamptz default now(),
              updated_at timestamptz default now()
            );
            """
        )
    )
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS public.inferred_parcels_v1_done_blocks (
              run_id text,
              block_id text,
              done_at timestamptz default now(),
              PRIMARY KEY (run_id, block_id)
            );
            """
        )
    )
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS public.inferred_parcels_v1_skipped_blocks (
              run_id text,
              block_id text,
              seed_count int,
              reason text,
              geom geometry(Polygon,4326),
              created_at timestamptz default now(),
              PRIMARY KEY (run_id, block_id)
            );
            """
        )
    )
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS public.inferred_parcels_v1_done_subblocks (
              run_id text,
              parent_block_id text,
              sub_idx int,
              done_at timestamptz default now(),
              PRIMARY KEY (run_id, parent_block_id, sub_idx)
            );
            """
        )
    )


def _upsert_progress(db, run_id: str, args, bbox_text: str) -> None:
    db.execute(
        text(
            """
            INSERT INTO public.inferred_parcels_v1_progress (
              run_id,
              bbox,
              road_buf_m,
              min_block_area_m2,
              seed_part_index,
              max_buildings_per_block,
              started_at,
              updated_at
            )
            VALUES (
              :run_id,
              :bbox,
              :road_buf_m,
              :min_block_area_m2,
              :seed_part_index,
              :max_buildings_per_block,
              now(),
              now()
            )
            ON CONFLICT (run_id) DO UPDATE SET
              bbox = EXCLUDED.bbox,
              road_buf_m = EXCLUDED.road_buf_m,
              min_block_area_m2 = EXCLUDED.min_block_area_m2,
              seed_part_index = EXCLUDED.seed_part_index,
              max_buildings_per_block = EXCLUDED.max_buildings_per_block,
              updated_at = now();
            """
        ),
        {
            "run_id": run_id,
            "bbox": bbox_text,
            "road_buf_m": args.road_buf_m,
            "min_block_area_m2": args.min_block_area_m2,
            "seed_part_index": args.seed_part_index,
            "max_buildings_per_block": args.max_buildings_per_block,
        },
    )


def _touch_progress(db, run_id: str) -> None:
    db.execute(
        text(
            "UPDATE public.inferred_parcels_v1_progress SET updated_at = now() WHERE run_id = :run_id"
        ),
        {"run_id": run_id},
    )


def _ensure_tmp_seeds(db) -> None:
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


def _populate_seeds(db, block_geom: bytes, seed_part_index: int) -> int:
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
        {"block_geom": block_geom, "seed_part_index": seed_part_index},
    )
    return db.execute(text("SELECT COUNT(*) FROM tmp_seeds")).scalar() or 0


def _upsert_done_block(db, run_id: str, block_id: str) -> None:
    db.execute(
        text(
            """
            INSERT INTO public.inferred_parcels_v1_done_blocks (run_id, block_id)
            VALUES (:run_id, :block_id)
            ON CONFLICT DO NOTHING;
            """
        ),
        {"run_id": run_id, "block_id": block_id},
    )


def _upsert_done_subblock(db, run_id: str, parent_block_id: str, sub_idx: int) -> None:
    db.execute(
        text(
            """
            INSERT INTO public.inferred_parcels_v1_done_subblocks (run_id, parent_block_id, sub_idx)
            VALUES (:run_id, :parent_block_id, :sub_idx)
            ON CONFLICT DO NOTHING;
            """
        ),
        {"run_id": run_id, "parent_block_id": parent_block_id, "sub_idx": sub_idx},
    )


def _record_skipped_block(
    db, run_id: str, block_id: str, seed_count: int, block_geom: bytes
) -> None:
    db.execute(
        text(
            """
            INSERT INTO public.inferred_parcels_v1_skipped_blocks (
              run_id,
              block_id,
              seed_count,
              reason,
              geom
            )
            VALUES (
              :run_id,
              :block_id,
              :seed_count,
              'too_many_buildings',
              ST_GeomFromEWKB(:block_geom)
            )
            ON CONFLICT (run_id, block_id) DO UPDATE SET
              seed_count = EXCLUDED.seed_count,
              created_at = now();
            """
        ),
        {
            "run_id": run_id,
            "block_id": block_id,
            "seed_count": seed_count,
            "block_geom": block_geom,
        },
    )


def _insert_parcels_from_seeds(db, block_geom: bytes, block_id: str) -> None:
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
                -- Enforce MultiPolygon output; PostGIS rejects Polygon into MultiPolygon column.
                ST_Multi(ST_Transform(geom3857, 4326)) AS geom,
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


def _iter_subblocks(db, block_geom: bytes, subblock_size_m: int) -> list[dict]:
    return (
        db.execute(
            text(
                """
                WITH block AS (
                  SELECT ST_Transform(ST_GeomFromEWKB(:block_geom), 3857) AS geom
                ),
                bounds AS (
                  SELECT
                    ST_XMin(geom)::numeric AS xmin,
                    ST_YMin(geom)::numeric AS ymin,
                    ST_XMax(geom)::numeric AS xmax,
                    ST_YMax(geom)::numeric AS ymax
                  FROM block
                ),
                grid AS (
                  SELECT
                    row_number() OVER () - 1 AS sub_idx,
                    ST_MakeEnvelope(
                      x::double precision,
                      y::double precision,
                      (x + CAST(:cell_size AS numeric))::double precision,
                      (y + CAST(:cell_size AS numeric))::double precision,
                      3857
                    ) AS cell
                  FROM bounds,
                  generate_series(
                    floor(xmin / CAST(:cell_size AS numeric)) * CAST(:cell_size AS numeric),
                    xmax,
                    CAST(:cell_size AS numeric)
                  ) AS x,
                  generate_series(
                    floor(ymin / CAST(:cell_size AS numeric)) * CAST(:cell_size AS numeric),
                    ymax,
                    CAST(:cell_size AS numeric)
                  ) AS y
                ),
                clipped AS (
                  SELECT sub_idx, ST_Intersection(cell, geom) AS geom
                  FROM grid, block
                  WHERE ST_Intersects(cell, geom)
                )
                SELECT sub_idx, ST_AsEWKB(geom) AS geom
                FROM clipped
                WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
                ORDER BY sub_idx
                """
            ),
            {"block_geom": block_geom, "cell_size": subblock_size_m},
        )
        .mappings()
        .all()
    )


def _process_skipped_blocks(db, args, run_id: str) -> None:
    _ensure_tmp_seeds(db)
    skipped_blocks = (
        db.execute(
            text(
                """
                SELECT block_id, ST_AsEWKB(geom) AS geom, seed_count
                FROM public.inferred_parcels_v1_skipped_blocks
                WHERE run_id = :run_id
                  AND reason = 'too_many_buildings'
                ORDER BY block_id
                """
            ),
            {"run_id": run_id},
        )
        .mappings()
        .all()
    )
    if not skipped_blocks:
        logger.info("No skipped blocks to process for run %s", run_id)
        return

    total_subblocks = 0
    processed_subblocks = 0
    for block in skipped_blocks:
        block_id = block["block_id"]
        block_geom = block["geom"]
        done_subblocks = {
            row["sub_idx"]
            for row in db.execute(
                text(
                    """
                    SELECT sub_idx
                    FROM public.inferred_parcels_v1_done_subblocks
                    WHERE run_id = :run_id AND parent_block_id = :block_id
                    """
                ),
                {"run_id": run_id, "block_id": block_id},
            )
            .mappings()
            .all()
        }
        subblocks = _iter_subblocks(db, block_geom, args.subblock_size_m)
        total_subblocks += len(subblocks)
        logger.info(
            "Skipped block %s: %d sub-blocks (done=%d)",
            block_id,
            len(subblocks),
            len(done_subblocks),
        )
        for subblock in subblocks:
            sub_idx = subblock["sub_idx"]
            if sub_idx in done_subblocks:
                continue
            sub_geom = subblock["geom"]
            seed_count = _populate_seeds(db, sub_geom, args.seed_part_index)
            if seed_count == 0:
                logger.debug("Skipping sub-block %s:%s: no buildings", block_id, sub_idx)
                _upsert_done_subblock(db, run_id, block_id, sub_idx)
                processed_subblocks += 1
            elif seed_count > args.subblock_max_buildings:
                logger.warning(
                    "Skipping sub-block %s:%s: %s buildings exceeds limit %s",
                    block_id,
                    sub_idx,
                    seed_count,
                    args.subblock_max_buildings,
                )
                _upsert_done_subblock(db, run_id, block_id, sub_idx)
                processed_subblocks += 1
            else:
                _insert_parcels_from_seeds(db, sub_geom, block_id)
                _upsert_done_subblock(db, run_id, block_id, sub_idx)
                processed_subblocks += 1

            if processed_subblocks % args.commit_every == 0:
                _touch_progress(db, run_id)
                db.commit()
                percent = (processed_subblocks / max(total_subblocks, 1)) * 100.0
                logger.info(
                    "Sub-block progress: %d/%d (%.1f%%)",
                    processed_subblocks,
                    total_subblocks,
                    percent,
                )
        db.execute(
            text(
                """
                UPDATE public.inferred_parcels_v1_skipped_blocks
                SET reason = 'processed'
                WHERE run_id = :run_id AND block_id = :block_id
                """
            ),
            {"run_id": run_id, "block_id": block_id},
        )

    _touch_progress(db, run_id)
    db.commit()
    logger.info("Processed %d sub-blocks across %d skipped blocks", processed_subblocks, len(skipped_blocks))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build inferred_parcels_v1 from roads + buildings")
    parser.add_argument(
        "--bbox",
        default=",".join(str(v) for v in DEFAULT_BBOX),
        help="xmin,ymin,xmax,ymax in WGS84",
    )
    parser.add_argument("--road-buf-m", type=float, default=DEFAULT_ROAD_BUF_M)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--reset-run", action="store_true")
    parser.add_argument("--commit-every", type=int, default=25)
    parser.add_argument("--max-blocks", type=int, default=None)
    parser.add_argument("--min-block-area-m2", type=float, default=DEFAULT_MIN_BLOCK_AREA_M2)
    parser.add_argument("--max-buildings-per-block", type=int, default=DEFAULT_MAX_BUILDINGS_PER_BLOCK)
    parser.add_argument("--seed-part-index", type=int, default=DEFAULT_SEED_PART_INDEX)
    parser.add_argument("--process-skipped", action="store_true")
    parser.add_argument("--subblock-size-m", type=int, default=DEFAULT_SUBBLOCK_SIZE_M)
    parser.add_argument(
        "--subblock-max-buildings",
        type=int,
        default=DEFAULT_SUBBLOCK_MAX_BUILDINGS,
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    try:
        bbox = _parse_bbox(args.bbox)
    except ValueError as exc:
        logger.error("Invalid bbox: %s", exc)
        return 2
    if args.commit_every <= 0:
        logger.error("--commit-every must be a positive integer")
        return 2
    if args.subblock_size_m <= 0:
        logger.error("--subblock-size-m must be a positive integer")
        return 2
    if args.subblock_max_buildings <= 0:
        logger.error("--subblock-max-buildings must be a positive integer")
        return 2
    if args.reset_run and args.resume:
        logger.error("--reset-run and --resume are mutually exclusive")
        return 2

    run_id = args.run_id or _run_id_from_params(
        bbox,
        args.road_buf_m,
        args.min_block_area_m2,
        args.seed_part_index,
        args.max_buildings_per_block,
    )

    with SessionLocal() as db:
        if not args.process_skipped:
            try:
                roads_table, roads_count = _resolve_roads_table(db)
                highway_expr = _resolve_highway_expr(db, roads_table, "r")
                roads_geom_col = _resolve_roads_geom_column(db, roads_table)
            except RuntimeError as exc:
                logger.error("%s", exc)
                return 1
            logger.info(
                "Using %s (%s rows) for road mask with geometry column %s",
                roads_table,
                roads_count,
                roads_geom_col,
            )

        _ensure_progress_tables(db)
        if args.reset_run:
            logger.info("Resetting run %s", run_id)
            db.execute(
                text("DELETE FROM public.inferred_parcels_v1_done_blocks WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            db.execute(
                text("DELETE FROM public.inferred_parcels_v1_done_subblocks WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            db.execute(
                text("DELETE FROM public.inferred_parcels_v1_skipped_blocks WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            db.execute(
                text("DELETE FROM public.inferred_parcels_v1_progress WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            db.execute(text("TRUNCATE TABLE public.inferred_parcels_v1"))
            db.commit()
        elif not args.resume and not args.process_skipped:
            logger.info("Clearing prior progress for run %s", run_id)
            db.execute(
                text("DELETE FROM public.inferred_parcels_v1_done_blocks WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            db.execute(
                text("DELETE FROM public.inferred_parcels_v1_done_subblocks WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            db.execute(
                text("DELETE FROM public.inferred_parcels_v1_skipped_blocks WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            db.execute(
                text("DELETE FROM public.inferred_parcels_v1_progress WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            db.commit()

        _upsert_progress(db, run_id, args, args.bbox)
        db.commit()

        if args.process_skipped:
            _process_skipped_blocks(db, args, run_id)
            return 0

        db.execute(text("DROP TABLE IF EXISTS tmp_blocks"))
        db.execute(text("DROP TABLE IF EXISTS tmp_blocks_with_buildings"))
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
        _ensure_tmp_seeds(db)

        xmin, ymin, xmax, ymax = bbox
        db.execute(
            text(
                f"""
                WITH bbox AS (
                  SELECT ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326) AS geom
                ),
                bbox3857 AS (
                  SELECT ST_Transform(geom, 3857) AS geom
                  FROM bbox
                ),
                road_mask AS (
                  SELECT ST_Transform(
                    ST_UnaryUnion(
                      ST_Collect(ST_Buffer(r.{roads_geom_col}, :road_buf_m))
                    ),
                    4326
                  ) AS geom
                  FROM {roads_table} r, bbox3857 b
                  WHERE r.{roads_geom_col} && b.geom
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

        db.execute(
            text(
                """
                CREATE TEMP TABLE tmp_blocks_with_buildings AS
                SELECT b.block_id, b.geom
                FROM tmp_blocks b
                WHERE EXISTS (
                  SELECT 1
                  FROM public.ms_buildings_raw m
                  WHERE m.geom && b.geom
                    AND ST_Intersects(m.geom, b.geom)
                );
                """
            )
        )
        db.execute(
            text("CREATE INDEX tmp_blocks_with_buildings_geom_gix ON tmp_blocks_with_buildings USING GIST (geom);")
        )
        db.commit()

        total_blocks = db.execute(text("SELECT COUNT(*) FROM tmp_blocks")).scalar() or 0
        blocks_with_buildings = (
            db.execute(text("SELECT COUNT(*) FROM tmp_blocks_with_buildings")).scalar() or 0
        )
        done_blocks = (
            db.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM tmp_blocks_with_buildings b
                    JOIN public.inferred_parcels_v1_done_blocks d
                      ON d.run_id = :run_id
                     AND d.block_id = b.block_id
                    """
                ),
                {"run_id": run_id},
            ).scalar()
            or 0
        )
        remaining_blocks = max(blocks_with_buildings - done_blocks, 0)
        logger.info("Total blocks: %d", total_blocks)
        logger.info("Blocks with buildings: %d", blocks_with_buildings)
        logger.info("Blocks already done for run %s: %d", run_id, done_blocks)
        logger.info("Blocks remaining for run %s: %d", run_id, remaining_blocks)

        blocks = list(_iter_blocks(db, run_id, args.max_blocks))
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
        if total_blocks <= 1:
            logger.error(
                "Road mask failed to partition bbox; check OSM import counts and highway expr"
            )
            return 1
        if blocks_with_buildings == 0:
            logger.warning("No blocks with buildings to process")
            return 0
        logger.info("Processing %d blocks", len(blocks))

        processed_blocks = 0
        for idx, block in enumerate(blocks, start=1):
            block_id = block["block_id"]
            block_geom = block["geom"]
            logger.info("Block %d/%d (%s)", idx, len(blocks), block_id)

            seed_count = _populate_seeds(db, block_geom, args.seed_part_index)
            if seed_count == 0:
                logger.debug("Skipping block %s: no buildings", block_id)
                _upsert_done_block(db, run_id, block_id)
                processed_blocks += 1
                if processed_blocks % args.commit_every == 0 or processed_blocks == len(blocks):
                    _touch_progress(db, run_id)
                    db.commit()
                continue
            if seed_count > args.max_buildings_per_block:
                logger.warning(
                    "Skipping block %s: %s buildings exceeds limit %s",
                    block_id,
                    seed_count,
                    args.max_buildings_per_block,
                )
                _record_skipped_block(db, run_id, block_id, seed_count, block_geom)
                _upsert_done_block(db, run_id, block_id)
                processed_blocks += 1
                if processed_blocks % args.commit_every == 0 or processed_blocks == len(blocks):
                    _touch_progress(db, run_id)
                    db.commit()
                continue

            _insert_parcels_from_seeds(db, block_geom, block_id)
            _upsert_done_block(db, run_id, block_id)
            processed_blocks += 1
            if processed_blocks % args.commit_every == 0 or processed_blocks == len(blocks):
                _touch_progress(db, run_id)
                db.commit()
                percent = (processed_blocks / max(len(blocks), 1)) * 100.0
                logger.info(
                    "Progress: %d/%d blocks (%.1f%%)",
                    processed_blocks,
                    len(blocks),
                    percent,
                )

        total = db.execute(text("SELECT COUNT(*) FROM public.inferred_parcels_v1")).scalar()
        logger.info("Inferred parcel total: %s", total or 0)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SQLAlchemyError as exc:
        logger.error("Database error: %s", exc)
        raise SystemExit(1)
