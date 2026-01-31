import logging
import math
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


# --- Search intent-word stripping (per-type "core query") ---
# This fixes cases like "حي النرجس" failing to match district label "النرجس",
# and "شارع الملك" failing when stored name doesn't include the prefix.
_DISTRICT_INTENT_WORDS = {
    # Arabic
    "حي",
    "حى",
    # English
    "district",
    "neighborhood",
    "neighbourhood",
    "hood",
}

_ROAD_INTENT_WORDS = {
    # Arabic
    "شارع",
    "طريق",
    # English (common user prefixes)
    "road",
    "street",
    "st",
    "rd",
    "avenue",
    "ave",
}


def _strip_intent_words(normalized_lower: str, intent_words: set[str]) -> str:
    """
    Remove standalone intent words from a *normalized lowercase* query.
    Keeps the original query if stripping would result in empty.
    """
    if not normalized_lower:
        return ""
    tokens = [t for t in normalized_lower.split() if t and t not in intent_words]
    core = " ".join(tokens).strip()
    return core or normalized_lower


# --- POI category keyword matching (name-less POIs) ---
# If the user types a category (e.g. "hospital" / "مستشفى"), allow matching by amenity type
# even when POI name is missing.
_POI_AMENITY_KEYWORDS: dict[str, list[str]] = {
    # hospitals / clinics
    "hospital": ["hospital"],
    "مستشفى": ["hospital"],
    "عيادة": ["clinic", "doctors"],
    "clinic": ["clinic", "doctors"],
    "doctor": ["doctors"],
    "pharmacy": ["pharmacy"],
    "صيدلية": ["pharmacy"],
    # schools
    "school": ["school"],
    "مدرسة": ["school"],
    "university": ["university", "college"],
    "جامعة": ["university", "college"],
    # mosques
    "mosque": ["place_of_worship"],
    "مسجد": ["place_of_worship"],
    # police / fire
    "police": ["police"],
    "شرطة": ["police"],
    "fire": ["fire_station"],
    "الدفاع": ["fire_station"],
}


def _poi_amenity_values_for_query(normalized_lower: str) -> list[str]:
    """
    If query looks like a category keyword, return amenity values to match.
    Otherwise return empty list.
    """
    if not normalized_lower:
        return []
    tokens = normalized_lower.split()
    values: list[str] = []
    if normalized_lower in _POI_AMENITY_KEYWORDS:
        values.extend(_POI_AMENITY_KEYWORDS[normalized_lower])
    else:
        for token in tokens:
            if token in _POI_AMENITY_KEYWORDS:
                values.extend(_POI_AMENITY_KEYWORDS[token])
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


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


# --- Dynamic OSM road SQL (supports ref + alt labels if present) ---
_ROAD_SQL_CACHE: dict[tuple[str, ...], TextClause] = {}


def _build_road_sql(db: Session) -> TextClause:
    """
    Build a road SQL that can search name, ref, and optional tags-based alt labels
    without breaking environments where those columns don't exist.
    """
    cols = _table_columns(db, "public.planet_osm_line")
    cache_key = tuple(sorted(cols))
    cached = _ROAD_SQL_CACHE.get(cache_key)
    if cached is not None:
        return cached

    def expr_or_null(column: str, cast: str) -> str:
        return f"{column}::{cast} AS {column}" if column in cols else f"NULL::{cast} AS {column}"

    # Use trigram operator (%) as the candidate gate, with LIKE fallback.
    # Keep LIKE for very short queries.
    trigram_gate = "(:q_raw_lower IS NOT NULL AND char_length(:q_raw_lower) >= 3 AND lower({expr}) % :q_raw_lower)"
    like_gate = "lower({expr}) LIKE :q_like_lower"

    search_exprs: list[str] = []
    if "name" in cols:
        search_exprs.append(f"(({trigram_gate.format(expr='name')}) OR ({like_gate.format(expr='name')}))")
    if "ref" in cols:
        search_exprs.append(f"(({trigram_gate.format(expr='ref')}) OR ({like_gate.format(expr='ref')}))")
    if "tags" in cols:
        search_exprs.append(
            f'(({trigram_gate.format(expr="tags->\'name:ar\'")}) OR ({like_gate.format(expr="tags->\'name:ar\'")}))'
        )
        search_exprs.append(
            f'(({trigram_gate.format(expr="tags->\'name:en\'")}) OR ({like_gate.format(expr="tags->\'name:en\'")}))'
        )
        search_exprs.append(
            f'(({trigram_gate.format(expr="tags->\'alt_name\'")}) OR ({like_gate.format(expr="tags->\'alt_name\'")}))'
        )
    if not search_exprs:
        search_exprs = [f"(({trigram_gate.format(expr='name')}) OR ({like_gate.format(expr='name')}))"]

    where_sql = " OR ".join(f"({expr})" for expr in search_exprs)

    label_expr = "COALESCE(name"
    if "ref" in cols:
        label_expr += ", ref"
    if "tags" in cols:
        label_expr += ", tags->'name:ar', tags->'name:en', tags->'alt_name'"
    label_expr += ", '')"

    sql = text(
        f"""
        WITH candidates AS (
            SELECT
                osm_id,
                {expr_or_null("name", "text")},
                {expr_or_null("ref", "text")},
                {expr_or_null("highway", "text")},
                {label_expr} AS label,
                ST_Transform(way, 4326) AS geom
            FROM planet_osm_line
            WHERE highway IS NOT NULL
              AND ({where_sql})
              AND way && ST_Transform(
                ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326),
                3857
              )
            ORDER BY GREATEST(
                similarity(lower({label_expr}), :q_raw_lower),
                word_similarity(lower({label_expr}), :q_raw_lower)
            ) DESC
            LIMIT :limit
        )
        SELECT
            'road' AS type,
            'osm_line:' || osm_id AS id,
            label AS label,
            COALESCE(highway, 'road') AS subtitle,
            ST_X(ST_PointOnSurface(geom)) AS lng,
            ST_Y(ST_PointOnSurface(geom)) AS lat,
            ST_XMin(geom) AS min_lng,
            ST_YMin(geom) AS min_lat,
            ST_XMax(geom) AS max_lng,
            ST_YMax(geom) AS max_lat,
            GREATEST(
                similarity(lower(label), :q_raw_lower),
                word_similarity(lower(label), :q_raw_lower)
            ) AS score
        FROM candidates
        """
    )
    _ROAD_SQL_CACHE[cache_key] = sql
    return sql


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


def _tokenize_query(normalized_lower: str) -> list[str]:
    # normalized_lower is already punctuation-stripped by normalize_search_text()
    tokens = [t for t in normalized_lower.split() if t]
    return tokens


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Distance in meters between two WGS84 points.
    """
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _distance_boost_m(distance_m: float, *, max_boost: float = 0.25, fade_out_m: float = 5000.0) -> float:
    """
    Linear fade: max_boost at 0m, down to 0 at fade_out_m.
    """
    if distance_m <= 0:
        return max_boost
    if distance_m >= fade_out_m:
        return 0.0
    return max_boost * (1.0 - (distance_m / fade_out_m))


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


def _parcel_extra_search_clauses(columns: set[str], *, table_alias: str = "p") -> list[tuple[str, str]]:
    """
    Return extra (column, SQL condition) pairs for parcel search when columns exist.
    """
    extra: list[tuple[str, str]] = []
    if "zoning_id" in columns:
        extra.append(("zoning_id", f"lower({table_alias}.zoning_id) LIKE :q_like_lower"))
    if "zoning_category" in columns:
        extra.append(("zoning_category", f"lower({table_alias}.zoning_category) LIKE :q_like_lower"))
    if "zoning_subcategory" in columns:
        extra.append(("zoning_subcategory", f"lower({table_alias}.zoning_subcategory) LIKE :q_like_lower"))
    if "landuse" in columns:
        extra.append(("landuse", f"lower({table_alias}.landuse) LIKE :q_like_lower"))
    if "classification" in columns:
        extra.append(("classification", f"lower({table_alias}.classification) LIKE :q_like_lower"))
    return extra


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

    extra_clauses = _parcel_extra_search_clauses(columns, table_alias="p")
    search_clauses.extend([clause for _, clause in extra_clauses])

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
    similarity_target_parts.extend([text_field_expr(column) for column, _ in extra_clauses])
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
    query_norm_lower: str,
    query_tokens: list[str],
) -> float:
    base_score = float(row.get("score") or 0.0)
    row_type = str(row.get("type") or "")
    boost = 0.0

    # --- 5.1 Prefix/exact/token coverage boosts ---
    label_raw = str(row.get("label") or "")
    label_norm_lower = normalize_search_text(label_raw, replace_ta_marbuta=True).lower()

    if query_norm_lower and label_norm_lower:
        if label_norm_lower == query_norm_lower:
            boost += 0.35  # exact label match (very strong signal)
        elif label_norm_lower.startswith(query_norm_lower) and len(query_norm_lower) >= 2:
            boost += 0.22  # prefix match (very common user intent)
        if query_tokens:
            # token coverage: all query tokens present in label
            if all(tok in label_norm_lower for tok in query_tokens):
                boost += 0.18

    # --- existing intent boosts ---
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

    # --- 5.2 Distance-to-viewport-center (soft spatial preference) ---
    if viewport_bbox and row.get("lng") is not None and row.get("lat") is not None:
        min_lon, min_lat, max_lon, max_lat = viewport_bbox
        lng = float(row["lng"])
        lat = float(row["lat"])

        # Keep existing binary inside-bbox boost
        if min_lon <= lng <= max_lon and min_lat <= lat <= max_lat:
            boost += 0.2

        # Add smooth distance-to-center boost
        center_lon = (min_lon + max_lon) / 2.0
        center_lat = (min_lat + max_lat) / 2.0
        d_m = _haversine_m(center_lat, center_lon, lat, lng)
        boost += _distance_boost_m(d_m, max_boost=0.25, fade_out_m=5000.0)

    # --- 5.3 Source confidence boosts (Aqar district hulls) ---
    # If external_feature hulls store a point count or similar metadata, use it.
    if row_type == "district":
        # We encode layer_name in id: "district:<layer_name>:<id>"
        row_id = str(row.get("id") or "")
        if ":aqar_district_hulls:" in row_id:
            # Try a few possible property-derived columns included by SQL
            raw_cnt = row.get("point_count") or row.get("points_count") or row.get("n_points")
            try:
                cnt = int(raw_cnt) if raw_cnt is not None else 0
            except (TypeError, ValueError):
                cnt = 0
            if cnt > 0:
                # Log-scaled confidence up to +0.15
                boost += min(0.15, 0.02 * math.log1p(cnt) * 3.0)
    return base_score + boost


def _merge_global_ranked(
    scored_rows: dict[str, list[tuple[float, dict[str, Any]]]],
    limit: int,
    per_type_cap: int | None = None,
) -> list[SearchItem]:
    """
    Merge results by global score (most relevant first), rather than round-robin by type.

    If per_type_cap is set, we limit the number of returned items per type to encourage diversity,
    but still preserve global relevance ordering among eligible candidates.
    """
    candidates: list[tuple[float, dict[str, Any]]] = []
    for rows in scored_rows.values():
        if not rows:
            continue
        candidates.extend(rows)

    # Highest score first
    candidates.sort(key=lambda entry: entry[0], reverse=True)

    items: list[SearchItem] = []
    seen: set[tuple[str, str]] = set()
    per_type_counts: dict[str, int] = {}

    for _score, row in candidates:
        if len(items) >= limit:
            break
        item = _row_to_item(row)
        if not item:
            continue
        key = (item.type, item.id)
        if key in seen:
            continue

        if per_type_cap is not None:
            current = per_type_counts.get(item.type, 0)
            if current >= per_type_cap:
                continue

        seen.add(key)
        per_type_counts[item.type] = per_type_counts.get(item.type, 0) + 1
        items.append(item)
    return items


def _merge_round_robin(
    scored_rows: dict[str, list[tuple[float, dict[str, Any]]]],
    limit: int,
    type_order: list[str] | None = None,
) -> list[SearchItem]:
    """
    Merge results in round-robin order by type, sorted by per-type score.
    """
    if type_order is None:
        type_order = list(scored_rows.keys())
    else:
        remaining = [key for key in scored_rows.keys() if key not in type_order]
        type_order = type_order + remaining

    per_type_rows: dict[str, list[tuple[float, dict[str, Any]]]] = {
        key: sorted(rows, key=lambda entry: entry[0], reverse=True) for key, rows in scored_rows.items()
    }
    items: list[SearchItem] = []
    seen: set[tuple[str, str]] = set()

    while len(items) < limit and any(per_type_rows.get(key) for key in type_order):
        for key in type_order:
            if len(items) >= limit:
                break
            rows = per_type_rows.get(key)
            if not rows:
                continue
            _score, row = rows.pop(0)
            item = _row_to_item(row)
            if not item:
                continue
            item_key = (item.type, item.id)
            if item_key in seen:
                continue
            seen.add(item_key)
            items.append(item)
    return items


_SEARCH_INDEX_SQL = text(
    """
    WITH candidates AS (
      SELECT
        type,
        id,
        label,
        subtitle,
        ST_X(center) AS lng,
        ST_Y(center) AS lat,
        ST_XMin(bbox) AS min_lng,
        ST_YMin(bbox) AS min_lat,
        ST_XMax(bbox) AS max_lng,
        ST_YMax(bbox) AS max_lat,
        (
          -- text relevance (trigram + word similarity)
          GREATEST(
            similarity(label_norm, :q_raw_lower),
            word_similarity(label_norm, :q_raw_lower),
            similarity(alt_text, :q_raw_lower),
            word_similarity(alt_text, :q_raw_lower)
          )
          -- optional token ranking
          + 0.15 * COALESCE(ts_rank_cd(tsv, plainto_tsquery('simple', :q_raw_lower)), 0)
          -- popularity prior (small)
          + 0.02 * LEAST(COALESCE(popularity, 0), 50) / 50.0
        ) AS score
      FROM public.search_index_mat
      -- IMPORTANT:
      -- Use bbox for fast viewport gating and to avoid SRID mismatch edge-cases that can
      -- cause empty results even when text matches exist.
      -- (bbox is constructed in the MV from EPSG:4326 geometries.)
      WHERE bbox && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
        AND (
          (
            char_length(:q_raw_lower) >= 3
            AND (
              label_norm % :q_raw_lower
              OR alt_text % :q_raw_lower
            )
          )
          OR label_norm LIKE :q_like_lower
          OR alt_text LIKE :q_like_lower
        )
      ORDER BY score DESC
      LIMIT :limit
    )
    SELECT * FROM candidates
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
        WHERE (
            (
              name IS NOT NULL
              AND (
                (char_length(:q_raw_lower) >= 3 AND lower(name) % :q_raw_lower)
                OR lower(name) LIKE :q_like_lower
              )
            )
            OR (
                :poi_amenities IS NOT NULL
                AND array_length(:poi_amenities, 1) IS NOT NULL
                AND amenity = ANY(:poi_amenities)
            )
        )
          AND way && ST_Transform(
            ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326),
            3857
          )
        ORDER BY similarity(
            lower(
                COALESCE(
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
                    historic
                )
            ),
            :q_raw_lower
        ) DESC
        LIMIT :limit
    )
    SELECT
        'poi' AS type,
        'osm_point:' || osm_id AS id,
        COALESCE(
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
            historic
        ) AS label,
        COALESCE(amenity, shop, tourism, leisure, office, building, landuse, man_made, sport, historic) AS subtitle,
        ST_X(geom) AS lng,
        ST_Y(geom) AS lat,
        ST_XMin(geom) AS min_lng,
        ST_YMin(geom) AS min_lat,
        ST_XMax(geom) AS max_lng,
        ST_YMax(geom) AS max_lat,
        similarity(
            lower(
                COALESCE(
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
                    historic
                )
            ),
            :q_raw_lower
        ) AS score
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
        WHERE (
            (
              name IS NOT NULL
              AND (
                (char_length(:q_raw_lower) >= 3 AND lower(name) % :q_raw_lower)
                OR lower(name) LIKE :q_like_lower
              )
            )
            OR (
                :poi_amenities IS NOT NULL
                AND array_length(:poi_amenities, 1) IS NOT NULL
                AND amenity = ANY(:poi_amenities)
            )
        )
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
        ORDER BY similarity(
            lower(
                COALESCE(
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
                    historic
                )
            ),
            :q_raw_lower
        ) DESC
        LIMIT :limit
    )
    SELECT
        'poi' AS type,
        'osm_polygon:' || osm_id AS id,
        COALESCE(
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
            historic
        ) AS label,
        COALESCE(amenity, shop, tourism, leisure, office, building, landuse, man_made, sport, historic) AS subtitle,
        ST_X(ST_PointOnSurface(geom)) AS lng,
        ST_Y(ST_PointOnSurface(geom)) AS lat,
        ST_XMin(geom) AS min_lng,
        ST_YMin(geom) AS min_lat,
        ST_XMax(geom) AS max_lng,
        ST_YMax(geom) AS max_lat,
        similarity(
            lower(
                COALESCE(
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
                    historic
                )
            ),
            :q_raw_lower
        ) AS score
    FROM candidates
    """
)

_DISTRICT_EXTERNAL_SQL = text(
    """
    WITH candidates AS (
        SELECT
            id,
            layer_name,
            properties,
            -- optional confidence metadata (varies by ingestion; safe if missing)
            COALESCE(
              NULLIF(properties->>'point_count','')::int,
              NULLIF(properties->>'points_count','')::int,
              NULLIF(properties->>'n_points','')::int,
              NULLIF(properties->>'count','')::int,
              0
            ) AS point_count,
            ST_SetSRID(ST_GeomFromGeoJSON(geometry::text), 4326) AS geom
        FROM external_feature
        WHERE layer_name IN ('osm_districts', 'aqar_district_hulls')
          AND (
            (
              -- Add trigram operator for typo tolerance with LIKE fallback for short queries.
              (char_length(:q_raw_lower) >= 3 AND lower(COALESCE(properties->>'district_raw', properties->>'name', properties->>'district')) % :q_raw_lower)
              OR lower(COALESCE(properties->>'district_raw', properties->>'name', properties->>'district')) LIKE :q_like_lower
            )
          )
          AND ST_SetSRID(ST_GeomFromGeoJSON(geometry::text), 4326)
              && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
        ORDER BY GREATEST(
            similarity(lower(COALESCE(properties->>'district_raw', properties->>'name', properties->>'district')), :q_raw_lower),
            word_similarity(lower(COALESCE(properties->>'district_raw', properties->>'name', properties->>'district')), :q_raw_lower)
        ) DESC
        LIMIT :limit
    )
    SELECT
        'district' AS type,
        'district:' || COALESCE(layer_name, 'external') || ':' || id AS id,
        COALESCE(properties->>'district_raw', properties->>'name', properties->>'district') AS label,
        ('District • ' || COALESCE(layer_name, 'external')) AS subtitle,
        point_count,
        ST_X(ST_PointOnSurface(geom)) AS lng,
        ST_Y(ST_PointOnSurface(geom)) AS lat,
        ST_XMin(geom) AS min_lng,
        ST_YMin(geom) AS min_lat,
        ST_XMax(geom) AS max_lng,
        ST_YMax(geom) AS max_lat,
        GREATEST(
            similarity(lower(COALESCE(properties->>'district_raw', properties->>'name', properties->>'district')), :q_raw_lower),
            word_similarity(lower(COALESCE(properties->>'district_raw', properties->>'name', properties->>'district')), :q_raw_lower)
        ) AS score
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
          AND (
            (char_length(:q_raw_lower) >= 3 AND lower(name) % :q_raw_lower)
            OR lower(name) LIKE :q_like_lower
          )
          AND (
            place IN ('neighbourhood', 'quarter', 'suburb')
            OR (boundary = 'administrative' AND admin_level IN ('9','10','11'))
          )
          AND way && ST_Transform(
            ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326),
            3857
          )
        ORDER BY similarity(lower(name), :q_raw_lower) DESC
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
        similarity(lower(name), :q_raw_lower) AS score
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
    normalized_query = normalize_search_text(query, replace_ta_marbuta=True)
    if not normalized_query and coord is None:
        return SearchResponse(items=[])

    normalized_lower = normalized_query.lower() if normalized_query else ""

    # Default (used for POI + parcels, and as fallback)
    q_like_lower = f"%{normalized_lower}%" if normalized_lower else ""
    q_raw = normalized_query or query
    q_raw_lower = normalized_lower or q_raw.lower()
    query_tokens = _tokenize_query(normalized_lower)

    # POI category mode (name-less POIs)
    poi_amenity_values = _poi_amenity_values_for_query(normalized_lower)

    # Per-type core queries (strip "intent words" like حي/شارع/etc.)
    district_core = _strip_intent_words(normalized_lower, _DISTRICT_INTENT_WORDS)
    road_core = _strip_intent_words(normalized_lower, _ROAD_INTENT_WORDS)
    q_like_lower_district = f"%{district_core}%" if district_core else q_like_lower
    q_raw_district = district_core or q_raw
    q_raw_lower_district = district_core or q_raw_lower
    q_like_lower_road = f"%{road_core}%" if road_core else q_like_lower
    q_raw_road = road_core or q_raw
    q_raw_lower_road = road_core or q_raw_lower

    per_type_limit = min(limit, 8)
    viewport = _parse_viewport_bbox(viewport_bbox)
    plan, block, parcel = parse_parcel_tokens(query)
    intent = _intent_flags(normalized_lower, plan, block, parcel)

    # Use intent-stripped query for both candidate retrieval and scoring when appropriate.
    score_query_lower = normalized_lower
    score_query_tokens = query_tokens
    index_q_like_lower = q_like_lower
    index_q_raw_lower = q_raw_lower
    if intent.get("district") and district_core:
        score_query_lower = district_core
        score_query_tokens = _tokenize_query(district_core)
        index_q_like_lower = q_like_lower_district
        index_q_raw_lower = q_raw_lower_district
    elif intent.get("road") and road_core:
        score_query_lower = road_core
        score_query_tokens = _tokenize_query(road_core)
        index_q_like_lower = q_like_lower_road
        index_q_raw_lower = q_raw_lower_road

    def run_query(sql: Any, params: dict[str, Any]) -> list[dict[str, Any]]:
        try:
            return list(db.execute(sql, {**params, **_bbox_params(viewport)}).mappings())
        except SQLAlchemyError as exc:
            logger.warning("Search query failed: %s", exc)
            return []

    scored_rows: dict[str, list[tuple[float, dict[str, Any]]]] = {}

    # Prefer unified search index if present (fast + comprehensive).
    if _table_exists(db, "public.search_index_mat"):
        # Pull more candidates than the final limit so Python-side rescoring (intent/spatial)
        # has headroom, and dedupe doesn't reduce result count.
        candidate_limit = min(max(limit * 5, 50), 200)
        rows = run_query(
            _SEARCH_INDEX_SQL,
            {
                "q_like_lower": index_q_like_lower,
                "q_raw_lower": index_q_raw_lower,
                "limit": candidate_limit,
            },
        )
        # Rows already include a score; still run through _score_row to add intent + spatial boosts.
        for row in rows:
            scored_rows.setdefault(str(row.get("type") or ""), []).append(
                (_score_row(row, intent, viewport, plan, block, parcel, score_query_lower, score_query_tokens), row)
            )
    else:
        # Fallback to legacy multi-query mode
        if _table_exists(db, "public.planet_osm_point"):
            rows = run_query(
                _POI_POINT_SQL,
                {
                    "q_like_lower": q_like_lower,
                    "q_raw": q_raw,
                    "q_raw_lower": q_raw_lower,
                    "limit": per_type_limit,
                    "poi_amenities": poi_amenity_values,
                },
            )
            scored_rows.setdefault("poi", [])
            for row in rows:
                scored_rows["poi"].append(
                    (_score_row(row, intent, viewport, plan, block, parcel, score_query_lower, score_query_tokens), row)
                )

        if _table_exists(db, "public.planet_osm_polygon"):
            rows = run_query(
                _POI_POLYGON_SQL,
                {
                    "q_like_lower": q_like_lower,
                    "q_raw": q_raw,
                    "q_raw_lower": q_raw_lower,
                    "limit": per_type_limit,
                    "poi_amenities": poi_amenity_values,
                },
            )
            scored_rows.setdefault("poi", [])
            for row in rows:
                scored_rows["poi"].append(
                    (_score_row(row, intent, viewport, plan, block, parcel, score_query_lower, score_query_tokens), row)
                )

        if _table_exists(db, "public.planet_osm_line"):
            rows = run_query(
                _build_road_sql(db),
                {
                    "q_like_lower": q_like_lower_road,
                    "q_raw": q_raw_road,
                    "q_raw_lower": q_raw_lower_road,
                    "limit": per_type_limit,
                },
            )
            scored_rows["road"] = [
                (_score_row(row, intent, viewport, plan, block, parcel, score_query_lower, score_query_tokens), row)
                for row in rows
            ]

        district_rows: list[dict[str, Any]] = []
        if _table_exists(db, "public.external_feature"):
            district_rows.extend(
                run_query(
                    _DISTRICT_EXTERNAL_SQL,
                    {
                        "q_like_lower": q_like_lower_district,
                        "q_raw": q_raw_district,
                        "q_raw_lower": q_raw_lower_district,
                        "limit": per_type_limit,
                    },
                )
            )
        if _table_exists(db, "public.planet_osm_polygon"):
            district_rows.extend(
                run_query(
                    _DISTRICT_FALLBACK_SQL,
                    {
                        "q_like_lower": q_like_lower_district,
                        "q_raw": q_raw_district,
                        "q_raw_lower": q_raw_lower_district,
                        "limit": per_type_limit,
                    },
                )
            )
        if district_rows:
            scored_rows["district"] = [
                (_score_row(row, intent, viewport, plan, block, parcel, score_query_lower, score_query_tokens), row)
                for row in district_rows
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
                    (_score_row(row, intent, viewport, plan, block, parcel, normalized_lower, query_tokens), row)
                    for row in parcel_rows
                ]

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
        # Global ranking (most relevant first). If you want diversity, set per_type_cap (e.g., 6).
        # For now we keep it None so the user sees the true best matches first.
        items.extend(_merge_global_ranked(scored_rows, remaining_limit, per_type_cap=None))

    return SearchResponse(items=items[:limit])
