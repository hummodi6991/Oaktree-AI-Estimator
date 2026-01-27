import logging
import re
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.deps import get_db

logger = logging.getLogger(__name__)
router = APIRouter()

_TABLE_CACHE: dict[str, bool] = {}


class SearchItem(BaseModel):
    type: str
    id: str
    label: str
    subtitle: str | None = None
    center: list[float]
    bbox: list[float] | None = None


class SearchResponse(BaseModel):
    items: list[SearchItem]


def _table_exists(db: Session, table_name: str) -> bool:
    cached = _TABLE_CACHE.get(table_name)
    if cached is not None:
        return cached
    try:
        row = db.execute(text("SELECT to_regclass(:table_name)"), {"table_name": table_name}).scalar()
        exists = row is not None
    except SQLAlchemyError as exc:
        logger.warning("Search table lookup failed for %s: %s", table_name, exc)
        exists = False
    _TABLE_CACHE[table_name] = exists
    return exists


def _extract_keyword_number(query: str, keywords: list[str]) -> str | None:
    for keyword in keywords:
        match = re.search(rf"{keyword}\s*[:#\-]*\s*(\d+)", query, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _parse_parcel_tokens(query: str) -> tuple[str | None, str | None, str | None]:
    plan = _extract_keyword_number(query, ["plan", "scheme", "مخطط"])
    block = _extract_keyword_number(query, ["block", "blk", "بلوك"])
    parcel = _extract_keyword_number(query, ["parcel", "plot", "قطعة", "قطعه"])

    digits = re.findall(r"\d+", query)
    if len(digits) >= 3:
        plan = plan or digits[0]
        block = block or digits[1]
        parcel = parcel or digits[2]
    return plan, block, parcel


def _bbox_from_row(row: dict[str, Any]) -> list[float] | None:
    keys = ("min_lng", "min_lat", "max_lng", "max_lat")
    if any(row.get(key) is None for key in keys):
        return None
    return [float(row[key]) for key in keys]


def _center_from_row(row: dict[str, Any]) -> list[float] | None:
    if row.get("lng") is None or row.get("lat") is None:
        return None
    return [float(row["lng"]), float(row["lat"])]


def _row_to_item(row: dict[str, Any]) -> SearchItem | None:
    center = _center_from_row(row)
    if not center:
        return None
    bbox = _bbox_from_row(row)
    return SearchItem(
        type=str(row.get("type") or ""),
        id=str(row.get("id") or ""),
        label=str(row.get("label") or ""),
        subtitle=row.get("subtitle"),
        center=center,
        bbox=bbox,
    )


_ROAD_SQL = text(
    """
    WITH candidates AS (
        SELECT
            osm_id,
            name,
            highway,
            ST_Transform(way, 4326) AS geom
        FROM planet_osm_line
        WHERE highway IS NOT NULL
          AND name IS NOT NULL
          AND lower(name) LIKE :q_like_lower
        ORDER BY similarity(lower(name), lower(:q_raw)) DESC
        LIMIT :limit
    )
    SELECT
        'road' AS type,
        'osm_line:' || osm_id AS id,
        name AS label,
        COALESCE(highway, 'road') AS subtitle,
        ST_X(ST_PointOnSurface(geom)) AS lng,
        ST_Y(ST_PointOnSurface(geom)) AS lat,
        ST_XMin(geom) AS min_lng,
        ST_YMin(geom) AS min_lat,
        ST_XMax(geom) AS max_lng,
        ST_YMax(geom) AS max_lat
    FROM candidates
    """
)

_POI_POINT_SQL = text(
    """
    WITH candidates AS (
        SELECT
            osm_id,
            name,
            amenity,
            shop,
            tourism,
            leisure,
            office,
            building,
            landuse,
            man_made,
            sport,
            historic,
            ST_Transform(way, 4326) AS geom
        FROM planet_osm_point
        WHERE name IS NOT NULL
          AND lower(name) LIKE :q_like_lower
        ORDER BY similarity(lower(name), lower(:q_raw)) DESC
        LIMIT :limit
    )
    SELECT
        'poi' AS type,
        'osm_point:' || osm_id AS id,
        name AS label,
        COALESCE(amenity, shop, tourism, leisure, office, building, landuse, man_made, sport, historic) AS subtitle,
        ST_X(geom) AS lng,
        ST_Y(geom) AS lat,
        ST_XMin(geom) AS min_lng,
        ST_YMin(geom) AS min_lat,
        ST_XMax(geom) AS max_lng,
        ST_YMax(geom) AS max_lat
    FROM candidates
    """
)

_POI_POLYGON_SQL = text(
    """
    WITH candidates AS (
        SELECT
            osm_id,
            name,
            amenity,
            shop,
            tourism,
            leisure,
            office,
            building,
            landuse,
            man_made,
            sport,
            historic,
            ST_Transform(way, 4326) AS geom
        FROM planet_osm_polygon
        WHERE name IS NOT NULL
          AND lower(name) LIKE :q_like_lower
          AND (
            amenity IS NOT NULL
            OR shop IS NOT NULL
            OR tourism IS NOT NULL
            OR leisure IS NOT NULL
            OR office IS NOT NULL
            OR building IS NOT NULL
            OR landuse IS NOT NULL
            OR man_made IS NOT NULL
            OR sport IS NOT NULL
            OR historic IS NOT NULL
          )
        ORDER BY similarity(lower(name), lower(:q_raw)) DESC
        LIMIT :limit
    )
    SELECT
        'poi' AS type,
        'osm_polygon:' || osm_id AS id,
        name AS label,
        COALESCE(amenity, shop, tourism, leisure, office, building, landuse, man_made, sport, historic) AS subtitle,
        ST_X(ST_PointOnSurface(geom)) AS lng,
        ST_Y(ST_PointOnSurface(geom)) AS lat,
        ST_XMin(geom) AS min_lng,
        ST_YMin(geom) AS min_lat,
        ST_XMax(geom) AS max_lng,
        ST_YMax(geom) AS max_lat
    FROM candidates
    """
)

_DISTRICT_EXTERNAL_SQL = text(
    """
    WITH candidates AS (
        SELECT
            id,
            properties,
            ST_SetSRID(ST_GeomFromGeoJSON(geometry::text), 4326) AS geom
        FROM external_feature
        WHERE layer_name = 'osm_districts'
          AND (
            lower(properties->>'district_raw') LIKE :q_like_lower
            OR lower(properties->>'district') LIKE :q_like_lower
          )
        ORDER BY similarity(lower(COALESCE(properties->>'district_raw', properties->>'district')), lower(:q_raw)) DESC
        LIMIT :limit
    )
    SELECT
        'district' AS type,
        'osm_district:' || id AS id,
        COALESCE(properties->>'district_raw', properties->>'district') AS label,
        'District' AS subtitle,
        ST_X(ST_PointOnSurface(geom)) AS lng,
        ST_Y(ST_PointOnSurface(geom)) AS lat,
        ST_XMin(geom) AS min_lng,
        ST_YMin(geom) AS min_lat,
        ST_XMax(geom) AS max_lng,
        ST_YMax(geom) AS max_lat
    FROM candidates
    """
)

_DISTRICT_FALLBACK_SQL = text(
    """
    WITH candidates AS (
        SELECT
            osm_id,
            name,
            ST_Transform(way, 4326) AS geom
        FROM planet_osm_polygon
        WHERE name IS NOT NULL
          AND lower(name) LIKE :q_like_lower
          AND (
            place IN ('neighbourhood', 'quarter', 'suburb')
            OR (boundary = 'administrative' AND admin_level IN ('9','10','11'))
          )
        ORDER BY similarity(lower(name), lower(:q_raw)) DESC
        LIMIT :limit
    )
    SELECT
        'district' AS type,
        'osm_district:' || osm_id AS id,
        name AS label,
        'District' AS subtitle,
        ST_X(ST_PointOnSurface(geom)) AS lng,
        ST_Y(ST_PointOnSurface(geom)) AS lat,
        ST_XMin(geom) AS min_lng,
        ST_YMin(geom) AS min_lat,
        ST_XMax(geom) AS max_lng,
        ST_YMax(geom) AS max_lat
    FROM candidates
    """
)

_PARCEL_SQL = text(
    """
    WITH candidates AS (
        SELECT
            id,
            street_name,
            plan_number,
            block_number,
            parcel_number,
            geom
        FROM public.suhail_parcels_mat
        WHERE (
            street_name IS NOT NULL AND lower(street_name) LIKE :q_like_lower
        )
        OR (plan_number IS NOT NULL AND lower(plan_number::text) LIKE :q_like_lower)
        OR (block_number IS NOT NULL AND lower(block_number::text) LIKE :q_like_lower)
        OR (parcel_number IS NOT NULL AND lower(parcel_number::text) LIKE :q_like_lower)
        OR (
            :plan IS NOT NULL
            AND :block IS NOT NULL
            AND :parcel IS NOT NULL
            AND plan_number::text = :plan
            AND block_number::text = :block
            AND parcel_number::text = :parcel
        )
        ORDER BY similarity(lower(COALESCE(street_name, '')), lower(:q_raw)) DESC
        LIMIT :limit
    )
    SELECT
        'parcel' AS type,
        'suhail:' || id AS id,
        COALESCE(street_name, 'Parcel ' || parcel_number::text, 'Parcel') AS label,
        concat_ws(
            ' • ',
            CASE WHEN plan_number IS NOT NULL THEN 'Plan ' || plan_number::text END,
            CASE WHEN block_number IS NOT NULL THEN 'Block ' || block_number::text END,
            CASE WHEN parcel_number IS NOT NULL THEN 'Parcel ' || parcel_number::text END
        ) AS subtitle,
        ST_X(ST_PointOnSurface(geom)) AS lng,
        ST_Y(ST_PointOnSurface(geom)) AS lat,
        ST_XMin(geom) AS min_lng,
        ST_YMin(geom) AS min_lat,
        ST_XMax(geom) AS max_lng,
        ST_YMax(geom) AS max_lat
    FROM candidates
    """
)


@router.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(..., min_length=2, max_length=128),
    limit: int = Query(12, ge=1, le=25),
    db: Session = Depends(get_db),
) -> SearchResponse:
    query = q.strip()
    if not query:
        return SearchResponse(items=[])
    q_like_lower = f"%{query.lower()}%"
    q_raw = query

    per_type_limit = min(limit, 8)
    items: list[SearchItem] = []
    seen: set[tuple[str, str]] = set()

    def add_rows(rows: list[dict[str, Any]]) -> None:
        for row in rows:
            item = _row_to_item(row)
            if not item:
                continue
            key = (item.type, item.id)
            if key in seen:
                continue
            seen.add(key)
            items.append(item)
            if len(items) >= limit:
                return

    def run_query(sql: Any, params: dict[str, Any]) -> list[dict[str, Any]]:
        try:
            return list(db.execute(sql, params).mappings())
        except SQLAlchemyError as exc:
            logger.warning("Search query failed: %s", exc)
            return []

    if _table_exists(db, "public.planet_osm_point"):
        add_rows(
            run_query(
                _POI_POINT_SQL,
                {"q_like_lower": q_like_lower, "q_raw": q_raw, "limit": per_type_limit},
            )
        )
    if len(items) < limit and _table_exists(db, "public.planet_osm_polygon"):
        add_rows(
            run_query(
                _POI_POLYGON_SQL,
                {"q_like_lower": q_like_lower, "q_raw": q_raw, "limit": per_type_limit},
            )
        )

    if len(items) < limit and _table_exists(db, "public.planet_osm_line"):
        add_rows(
            run_query(
                _ROAD_SQL,
                {"q_like_lower": q_like_lower, "q_raw": q_raw, "limit": per_type_limit},
            )
        )

    if len(items) < limit:
        if _table_exists(db, "public.external_feature"):
            add_rows(
                run_query(
                    _DISTRICT_EXTERNAL_SQL,
                    {"q_like_lower": q_like_lower, "q_raw": q_raw, "limit": per_type_limit},
                )
            )
        if len(items) < limit and _table_exists(db, "public.planet_osm_polygon"):
            add_rows(
                run_query(
                    _DISTRICT_FALLBACK_SQL,
                    {"q_like_lower": q_like_lower, "q_raw": q_raw, "limit": per_type_limit},
                )
            )

    if len(items) < limit and _table_exists(db, "public.suhail_parcels_mat"):
        plan, block, parcel = _parse_parcel_tokens(query)
        add_rows(
            run_query(
                _PARCEL_SQL,
                {
                    "q_like_lower": q_like_lower,
                    "q_raw": q_raw,
                    "limit": per_type_limit,
                    "plan": plan,
                    "block": block,
                    "parcel": parcel,
                },
            )
        )

    return SearchResponse(items=items[:limit])
