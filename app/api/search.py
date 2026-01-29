import logging
import re
from typing import Any
from urllib.parse import unquote

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.api.tiles import PARCEL_TILE_TABLE, SUHAIL_PARCEL_TABLE

logger = logging.getLogger(__name__)
router = APIRouter()

_TABLE_CACHE: dict[str, bool] = {}
_COLUMN_CACHE: dict[str, set[str]] = {}
_RIYADH_BBOX = {
    "min_lon": 46.20,
    "min_lat": 24.20,
    "max_lon": 47.30,
    "max_lat": 25.10,
}

_WGS84_LON_MIN = -180.0
_WGS84_LON_MAX = 180.0
_WGS84_LAT_MIN = -90.0
_WGS84_LAT_MAX = 90.0

_AR_DIACRITICS = re.compile(
    r"[\u0610-\u061A\u064B-\u065F\u06D6-\u06DC\u06DF-\u06E8\u06EA-\u06ED]"
)
_PUNCTUATION_RE = re.compile(r"[،,;؛/\\\|:\uFF1A\u2013\u2014\-\(\)\[\]\{\}\.\+]+")
_EXTRA_PUNCTUATION_RE = re.compile(r"[!\"'`~@#$%^&*_=+<>?\u2026]+")
_ARABIC_DIGIT_MAP = str.maketrans(
    {
        "٠": "0",
        "١": "1",
        "٢": "2",
        "٣": "3",
        "٤": "4",
        "٥": "5",
        "٦": "6",
        "٧": "7",
        "٨": "8",
        "٩": "9",
        "۰": "0",
        "۱": "1",
        "۲": "2",
        "۳": "3",
        "۴": "4",
        "۵": "5",
        "۶": "6",
        "۷": "7",
        "۸": "8",
        "۹": "9",
    }
)
_ALEF_VARIANTS = str.maketrans({"أ": "ا", "إ": "ا", "آ": "ا", "ٱ": "ا", "ى": "ي"})
_TA_MARBUTA_VARIANT = str.maketrans({"ة": "ه"})

_COORD_PAIR_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)")
_GOOGLE_MAPS_AT_RE = re.compile(r"@(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)")
_GOOGLE_MAPS_Q_RE = re.compile(r"[?&#](?:q|query|ll)=(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)")


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


def _split_table_name(table_name: str) -> tuple[str, str]:
    if "." in table_name:
        schema_name, table = table_name.split(".", 1)
        return schema_name, table
    return "public", table_name


def _table_columns(db: Session, table_name: str) -> set[str]:
    cached = _COLUMN_CACHE.get(table_name)
    if cached is not None:
        return cached
    schema_name, table = _split_table_name(table_name)
    try:
        rows = db.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema
                  AND table_name = :table
                """
            ),
            {"schema": schema_name, "table": table},
        ).mappings()
        columns = {row["column_name"] for row in rows}
    except SQLAlchemyError as exc:
        logger.warning("Search column lookup failed for %s: %s", table_name, exc)
        columns = set()
    _COLUMN_CACHE[table_name] = columns
    return columns


def normalize_search_text(q: str, *, replace_ta_marbuta: bool = True) -> str:
    if not q:
        return ""
    normalized = unquote(q).replace("\u00A0", " ").strip()
    if not normalized:
        return ""
    normalized = normalized.replace("ـ", "")
    normalized = _AR_DIACRITICS.sub("", normalized)
    normalized = normalized.translate(_ARABIC_DIGIT_MAP)
    normalized = normalized.translate(_ALEF_VARIANTS)
    if replace_ta_marbuta:
        normalized = normalized.translate(_TA_MARBUTA_VARIANT)
    normalized = _PUNCTUATION_RE.sub(" ", normalized)
    normalized = _EXTRA_PUNCTUATION_RE.sub(" ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def parse_coords(q: str) -> tuple[float, float] | None:
    if not q:
        return None
    candidate = unquote(q).strip()
    if not candidate:
        return None
    for regex in (_GOOGLE_MAPS_AT_RE, _GOOGLE_MAPS_Q_RE, _COORD_PAIR_RE):
        match = regex.search(candidate)
        if not match:
            continue
        try:
            lat = float(match.group(1))
            lon = float(match.group(2))
        except ValueError:
            continue
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return lat, lon
    return None


def _safe_identifier(value: str | None, fallback: str) -> str:
    candidate = (value or "").strip()
    if not candidate:
        return fallback
    if all(ch.isalnum() or ch in {"_", "."} for ch in candidate):
        return candidate
    return fallback


def _canonical_parcel_table(value: str | None) -> str:
    schema_name, table_name = _split_table_name(value or "")
    return _safe_identifier(f"{schema_name}.{table_name}", SUHAIL_PARCEL_TABLE)


def _allowed_parcel_tables() -> set[str]:
    return {
        _canonical_parcel_table(SUHAIL_PARCEL_TABLE),
        _canonical_parcel_table(PARCEL_TILE_TABLE),
    }


def _extract_keyword_number(query: str, keywords: list[str]) -> str | None:
    for keyword in keywords:
        match = re.search(rf"{re.escape(keyword)}\s*[:#\-]*\s*(\d+)", query, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _intersect_bbox_with_riyadh(
    bbox: tuple[float, float, float, float] | None,
) -> tuple[float, float, float, float] | None:
    """
    Intersect a viewport bbox with the fixed Riyadh bbox.
    Returns None if bbox is None or if the intersection is empty.
    """
    if bbox is None:
        return None
    min_lon, min_lat, max_lon, max_lat = bbox
    riyadh_bbox = _RIYADH_BBOX
    min_lon = max(min_lon, float(riyadh_bbox["min_lon"]))
    min_lat = max(min_lat, float(riyadh_bbox["min_lat"]))
    max_lon = min(max_lon, float(riyadh_bbox["max_lon"]))
    max_lat = min(max_lat, float(riyadh_bbox["max_lat"]))
    if min_lon >= max_lon or min_lat >= max_lat:
        return None
    return (min_lon, min_lat, max_lon, max_lat)


def _bbox_params(viewport_bbox: tuple[float, float, float, float] | None) -> dict[str, float]:
    """
    Always constrain search to Riyadh metro bbox (product requirement).
    If a viewport bbox is provided, clamp it to Riyadh; otherwise use Riyadh bbox.
    """
    clipped = _intersect_bbox_with_riyadh(viewport_bbox)
    if clipped is None:
        return dict(_RIYADH_BBOX)
    min_lon, min_lat, max_lon, max_lat = clipped
    return {
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
    }


def parse_parcel_tokens(query: str) -> tuple[str | None, str | None, str | None]:
    normalized = normalize_search_text(query, replace_ta_marbuta=True).lower()
    if not normalized:
        return None, None, None
    plan = _extract_keyword_number(normalized, ["plan", "scheme", "مخطط"])
    block = _extract_keyword_number(normalized, ["block", "blk", "بلوك"])
    parcel = _extract_keyword_number(normalized, ["parcel", "plot", "قطعه", "قطعة"])

    digit_query = query.translate(_ARABIC_DIGIT_MAP)
    pattern_match = re.search(r"(\d+)\s*[-/]\s*(\d+)\s*[-/]\s*(\d+)", digit_query)
    if pattern_match:
        plan = plan or pattern_match.group(1)
        block = block or pattern_match.group(2)
        parcel = parcel or pattern_match.group(3)

    digits = re.findall(r"\d+", normalized)
    if len(digits) >= 3:
        plan = plan or digits[0]
        block = block or digits[1]
        parcel = parcel or digits[2]
    return plan, block, parcel


def _parse_viewport_bbox(raw: str | None) -> tuple[float, float, float, float] | None:
    if not raw:
        return None
    parts = [part.strip() for part in raw.split(",")]
    if len(parts) != 4:
        return None
    try:
        min_lon, min_lat, max_lon, max_lat = (float(part) for part in parts)
    except ValueError:
        return None
    if min_lon >= max_lon or min_lat >= max_lat:
        return None
    if (
        min_lon < _WGS84_LON_MIN
        or max_lon > _WGS84_LON_MAX
        or min_lat < _WGS84_LAT_MIN
        or max_lat > _WGS84_LAT_MAX
    ):
        return None
    return (min_lon, min_lat, max_lon, max_lat)


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


def _parcel_source_metadata(table_name: str) -> tuple[str, str]:
    lowered = table_name.lower()
    if "suhail" in lowered:
        return "suhail", "Suhail"
    if "arcgis" in lowered:
        return "arcgis", "ArcGIS"
    if "derived" in lowered:
        return "derived", "Derived"
    if "inferred" in lowered:
        return "inferred", "Inferred"
    return "parcel", "Parcel layer"


def _parcel_search_sql(
    table_name: str,
    columns: set[str],
    id_prefix: str,
    include_source_label: bool,
) -> TextClause | None:
    has_street = "street_name" in columns
    has_municipality = "municipality_name" in columns
    has_neighborhood = "neighborhood_name" in columns
    has_plan = "plan_number" in columns
    has_block = "block_number" in columns
    has_parcel = "parcel_number" in columns

    search_clauses = []
    if has_street:
        search_clauses.append("(street_name IS NOT NULL AND lower(street_name) LIKE :q_like_lower)")
    if has_municipality:
        search_clauses.append("(municipality_name IS NOT NULL AND lower(municipality_name) LIKE :q_like_lower)")
    if has_neighborhood:
        search_clauses.append("(neighborhood_name IS NOT NULL AND lower(neighborhood_name) LIKE :q_like_lower)")
    if has_plan:
        search_clauses.append("(plan_number IS NOT NULL AND lower(plan_number::text) LIKE :q_like_lower)")
    if has_block:
        search_clauses.append("(block_number IS NOT NULL AND lower(block_number::text) LIKE :q_like_lower)")
    if has_parcel:
        search_clauses.append("(parcel_number IS NOT NULL AND lower(parcel_number::text) LIKE :q_like_lower)")
    if has_plan and has_block and has_parcel:
        search_clauses.append(
            """
            (
                :plan IS NOT NULL
                AND :block IS NOT NULL
                AND :parcel IS NOT NULL
                AND plan_number::text = :plan
                AND block_number::text = :block
                AND parcel_number::text = :parcel
            )
            """
        )

    if not search_clauses:
        return None

    def expr_or_null(column: str, cast: str) -> str:
        return f"p.{column}::{cast} AS {column}" if column in columns else f"NULL::{cast} AS {column}"

    def text_field_expr(column: str) -> str:
        return f"p.{column}" if column in columns else "NULL::text"

    similarity_target_parts = [
        text_field_expr("street_name"),
        text_field_expr("neighborhood_name"),
        text_field_expr("municipality_name"),
        "plan_number::text" if has_plan else "NULL::text",
        "block_number::text" if has_block else "NULL::text",
        "parcel_number::text" if has_parcel else "NULL::text",
    ]
    similarity_expr = f"concat_ws(' ', {', '.join(similarity_target_parts)})"

    label_expr = "COALESCE(street_name, neighborhood_name, municipality_name"
    if has_parcel:
        label_expr += ", 'Parcel ' || parcel_number::text"
    label_expr += ", 'Parcel')"

    subtitle_parts = []
    if has_neighborhood:
        subtitle_parts.append("CASE WHEN neighborhood_name IS NOT NULL THEN neighborhood_name END")
    if has_municipality:
        subtitle_parts.append("CASE WHEN municipality_name IS NOT NULL THEN municipality_name END")
    if has_plan:
        subtitle_parts.append("CASE WHEN plan_number IS NOT NULL THEN 'Plan ' || plan_number::text END")
    if has_block:
        subtitle_parts.append("CASE WHEN block_number IS NOT NULL THEN 'Block ' || block_number::text END")
    if has_parcel:
        subtitle_parts.append("CASE WHEN parcel_number IS NOT NULL THEN 'Parcel ' || parcel_number::text END")
    if include_source_label:
        subtitle_parts.append("CASE WHEN :source_label IS NOT NULL THEN 'Source: ' || :source_label END")
    if subtitle_parts:
        subtitle_expr = "concat_ws(' • ', " + ", ".join(subtitle_parts) + ")"
    else:
        subtitle_expr = "NULL::text"

    return text(
        f"""
        WITH candidates AS (
            SELECT
                p.id,
                {expr_or_null("street_name", "text")},
                {expr_or_null("municipality_name", "text")},
                {expr_or_null("neighborhood_name", "text")},
                {expr_or_null("plan_number", "text")},
                {expr_or_null("block_number", "text")},
                {expr_or_null("parcel_number", "text")},
                p.geom
            FROM {table_name} p
            WHERE (
                {' OR '.join(search_clauses)}
            )
              AND p.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
            ORDER BY similarity(lower({similarity_expr}), lower(:q_raw)) DESC
            LIMIT :limit
        )
        SELECT
            'parcel' AS type,
            '{id_prefix}:' || id AS id,
            {label_expr} AS label,
            {subtitle_expr} AS subtitle,
            ST_X(ST_PointOnSurface(geom)) AS lng,
            ST_Y(ST_PointOnSurface(geom)) AS lat,
            ST_XMin(geom) AS min_lng,
            ST_YMin(geom) AS min_lat,
            ST_XMax(geom) AS max_lng,
            ST_YMax(geom) AS max_lat,
            similarity(lower({similarity_expr}), lower(:q_raw)) AS score,
            plan_number,
            block_number,
            parcel_number
        FROM candidates
        """
    )


def _intent_flags(normalized_query: str, plan: str | None, block: str | None, parcel: str | None) -> dict[str, bool]:
    has_parcel_tokens = any([plan, block, parcel])
    intent = {
        "parcel": has_parcel_tokens or any(
            token in normalized_query for token in ["parcel", "plot", "قطعه", "قطعة"]
        ),
        "road": any(token in normalized_query for token in ["road", "street", "st", "rd", "avenue", "ave", "شارع", "طريق"]),
        "district": any(
            token in normalized_query for token in ["district", "neighborhood", "neighbourhood", "حي", "حى"]
        ),
    }
    intent["poi"] = not any(intent.values())
    return intent


def _score_row(
    row: dict[str, Any],
    intent: dict[str, bool],
    viewport_bbox: tuple[float, float, float, float] | None,
    plan: str | None,
    block: str | None,
    parcel: str | None,
) -> float:
    base_score = float(row.get("score") or 0.0)
    row_type = row.get("type")
    boost = 0.0
    if row_type == "parcel":
        if intent.get("parcel"):
            boost += 0.6
        if plan and block and parcel:
            if (
                str(row.get("plan_number")) == plan
                and str(row.get("block_number")) == block
                and str(row.get("parcel_number")) == parcel
            ):
                boost += 1.0
    elif row_type == "road" and intent.get("road"):
        boost += 0.3
    elif row_type == "district" and intent.get("district"):
        boost += 0.3
    elif row_type == "poi" and intent.get("poi"):
        boost += 0.15

    if viewport_bbox and row.get("lng") is not None and row.get("lat") is not None:
        min_lon, min_lat, max_lon, max_lat = viewport_bbox
        lng = float(row["lng"])
        lat = float(row["lat"])
        if min_lon <= lng <= max_lon and min_lat <= lat <= max_lat:
            boost += 0.2
    return base_score + boost


def _merge_round_robin(
    scored_rows: dict[str, list[tuple[float, dict[str, Any]]]],
    limit: int,
    type_order: list[str],
) -> list[SearchItem]:
    queues: dict[str, list[tuple[float, dict[str, Any]]]] = {}
    for row_type, rows in scored_rows.items():
        queues[row_type] = sorted(rows, key=lambda entry: entry[0], reverse=True)

    items: list[SearchItem] = []
    seen: set[tuple[str, str]] = set()
    ordered_types = type_order + [t for t in queues.keys() if t not in type_order]

    while len(items) < limit and any(queues.values()):
        for row_type in ordered_types:
            if len(items) >= limit:
                break
            queue = queues.get(row_type)
            if not queue:
                continue
            _, row = queue.pop(0)
            item = _row_to_item(row)
            if not item:
                continue
            key = (item.type, item.id)
            if key in seen:
                continue
            seen.add(key)
            items.append(item)
    return items


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
          AND way && ST_Transform(
            ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326),
            3857
          )
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
        ST_YMax(geom) AS max_lat,
        similarity(lower(name), lower(:q_raw)) AS score
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
          AND way && ST_Transform(
            ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326),
            3857
          )
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
        ST_YMax(geom) AS max_lat,
        similarity(lower(name), lower(:q_raw)) AS score
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
          AND way && ST_Transform(
            ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326),
            3857
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
        ST_YMax(geom) AS max_lat,
        similarity(lower(name), lower(:q_raw)) AS score
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
          AND ST_SetSRID(ST_GeomFromGeoJSON(geometry::text), 4326)
              && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
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
        ST_YMax(geom) AS max_lat,
        similarity(lower(COALESCE(properties->>'district_raw', properties->>'district')), lower(:q_raw)) AS score
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
          AND way && ST_Transform(
            ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326),
            3857
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
        ST_YMax(geom) AS max_lat,
        similarity(lower(name), lower(:q_raw)) AS score
    FROM candidates
    """
)


@router.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(..., min_length=2, max_length=128),
    limit: int = Query(12, ge=1, le=25),
    viewport_bbox: str | None = Query(None),
    db: Session = Depends(get_db),
) -> SearchResponse:
    query = q.strip()
    coord = parse_coords(query)
    normalized_query = normalize_search_text(query)
    if not normalized_query and coord is None:
        return SearchResponse(items=[])

    q_like_lower = f"%{normalized_query.lower()}%" if normalized_query else ""
    q_raw = normalized_query or query
    per_type_limit = min(limit, 8)
    viewport = _parse_viewport_bbox(viewport_bbox)
    plan, block, parcel = parse_parcel_tokens(query)
    intent = _intent_flags(normalized_query.lower(), plan, block, parcel)

    def run_query(sql: Any, params: dict[str, Any]) -> list[dict[str, Any]]:
        try:
            return list(db.execute(sql, {**params, **_bbox_params(viewport)}).mappings())
        except SQLAlchemyError as exc:
            logger.warning("Search query failed: %s", exc)
            return []

    scored_rows: dict[str, list[tuple[float, dict[str, Any]]]] = {}

    if _table_exists(db, "public.planet_osm_point"):
        rows = run_query(
            _POI_POINT_SQL,
            {"q_like_lower": q_like_lower, "q_raw": q_raw, "limit": per_type_limit},
        )
        scored_rows.setdefault("poi", [])
        for row in rows:
            scored_rows["poi"].append((_score_row(row, intent, viewport, plan, block, parcel), row))

    if _table_exists(db, "public.planet_osm_polygon"):
        rows = run_query(
            _POI_POLYGON_SQL,
            {"q_like_lower": q_like_lower, "q_raw": q_raw, "limit": per_type_limit},
        )
        scored_rows.setdefault("poi", [])
        for row in rows:
            scored_rows["poi"].append((_score_row(row, intent, viewport, plan, block, parcel), row))

    if _table_exists(db, "public.planet_osm_line"):
        rows = run_query(
            _ROAD_SQL,
            {"q_like_lower": q_like_lower, "q_raw": q_raw, "limit": per_type_limit},
        )
        scored_rows["road"] = [
            (_score_row(row, intent, viewport, plan, block, parcel), row) for row in rows
        ]

    district_rows: list[dict[str, Any]] = []
    if _table_exists(db, "public.external_feature"):
        district_rows.extend(
            run_query(
                _DISTRICT_EXTERNAL_SQL,
                {"q_like_lower": q_like_lower, "q_raw": q_raw, "limit": per_type_limit},
            )
        )
    if _table_exists(db, "public.planet_osm_polygon"):
        district_rows.extend(
            run_query(
                _DISTRICT_FALLBACK_SQL,
                {"q_like_lower": q_like_lower, "q_raw": q_raw, "limit": per_type_limit},
            )
        )
    if district_rows:
        scored_rows["district"] = [
            (_score_row(row, intent, viewport, plan, block, parcel), row) for row in district_rows
        ]

    parcel_tables: list[tuple[str, str, str]] = []
    safe_active = _canonical_parcel_table(PARCEL_TILE_TABLE)
    suhail_table = _canonical_parcel_table(SUHAIL_PARCEL_TABLE)
    allowed_tables = {safe_active, suhail_table}
    if safe_active in allowed_tables and _table_exists(db, safe_active):
        prefix, label = _parcel_source_metadata(safe_active)
        parcel_tables.append((safe_active, prefix, label))
    if (
        suhail_table in allowed_tables
        and safe_active != suhail_table
        and _table_exists(db, suhail_table)
    ):
        prefix, label = _parcel_source_metadata(suhail_table)
        parcel_tables.append((suhail_table, prefix, label))

    if parcel_tables:
        include_source_label = len(parcel_tables) > 1
        parcel_rows: list[dict[str, Any]] = []
        for table_name, prefix, label in parcel_tables:
            columns = _table_columns(db, table_name)
            sql = _parcel_search_sql(
                table_name=_safe_identifier(table_name, SUHAIL_PARCEL_TABLE),
                columns=columns,
                id_prefix=prefix,
                include_source_label=include_source_label,
            )
            if sql is None:
                continue
            parcel_rows.extend(
                run_query(
                    sql,
                    {
                        "q_like_lower": q_like_lower,
                        "q_raw": q_raw,
                        "limit": per_type_limit,
                        "plan": plan,
                        "block": block,
                        "parcel": parcel,
                        "source_label": label,
                    },
                )
            )
        if parcel_rows:
            scored_rows["parcel"] = [
                (_score_row(row, intent, viewport, plan, block, parcel), row) for row in parcel_rows
            ]

    type_order = ["parcel", "district", "road", "poi"]
    items: list[SearchItem] = []
    remaining_limit = limit
    if coord is not None and remaining_limit > 0:
        lat, lon = coord
        items.append(
            SearchItem(
                type="coordinate",
                id=f"coord:{lat},{lon}",
                label=f"{lat:.6f}, {lon:.6f}",
                subtitle="Coordinate",
                center=[lon, lat],
                bbox=None,
            )
        )
        remaining_limit -= 1

    if remaining_limit > 0:
        items.extend(_merge_round_robin(scored_rows, remaining_limit, type_order))

    return SearchResponse(items=items[:limit])
