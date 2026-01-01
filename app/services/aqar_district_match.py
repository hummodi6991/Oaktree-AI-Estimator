from __future__ import annotations

import logging
import re
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


ARABIC_VARIANTS = str.maketrans(
    {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ى": "ي",
    }
)

WHITESPACE_RE = re.compile(r"\s+")
SEPARATOR_VARIANT_RE = re.compile(r"[-/]+")


@dataclass(frozen=True)
class DistrictRow:
    district: str
    price_per_sqm: float
    n: int


@dataclass
class DistrictCacheEntry:
    raw_map: Dict[str, DistrictRow]
    norm_map: Dict[str, DistrictRow]


class _DistrictLRUCache:
    def __init__(self, maxsize: int = 32):
        self.maxsize = maxsize
        self._store: "OrderedDict[Tuple[str, Optional[str]], DistrictCacheEntry]" = OrderedDict()

    def get(self, key: Tuple[str, Optional[str]]) -> Optional[DistrictCacheEntry]:
        if key not in self._store:
            return None
        self._store.move_to_end(key)
        return self._store[key]

    def set(self, key: Tuple[str, Optional[str]], value: DistrictCacheEntry) -> None:
        if key in self._store:
            self._store.move_to_end(key)
        self._store[key] = value
        if len(self._store) > self.maxsize:
            self._store.popitem(last=False)

    def clear(self) -> None:
        self._store.clear()


_DISTRICT_CACHE = _DistrictLRUCache()


def normalize_arabic_text(s: str | None) -> str:
    if s is None:
        return ""
    normalized = s.replace("\u00A0", " ").replace("ـ", "")
    normalized = normalized.translate(ARABIC_VARIANTS)
    normalized = WHITESPACE_RE.sub(" ", normalized).strip()
    return normalized


def normalize_district_key(s: str | None) -> str:
    base = normalize_arabic_text(s)
    if base.startswith("حي "):
        base = base[3:]
    base = WHITESPACE_RE.sub(" ", base).strip()
    return base


def _fetch_city_mv_rows(db: Session, city_ar: str, property_type: str | None) -> Iterable[dict]:
    params = {"aqar_city": city_ar}
    query = [
        "SELECT district, price_per_sqm, n",
        "FROM aqar.mv_city_price_per_sqm",
        "WHERE city = :aqar_city",
    ]
    if property_type:
        query.append("AND property_type = :property_type")
        params["property_type"] = property_type
    query.append("ORDER BY n DESC")
    try:
        result = db.execute(text("\n".join(query)), params)
        result_mappings = result.mappings() if hasattr(result, "mappings") else result
        if hasattr(result_mappings, "all"):
            return result_mappings.all()
        if hasattr(result_mappings, "first"):
            first = result_mappings.first()
            return [first] if first else []
        return []
    except SQLAlchemyError as exc:
        logger.warning("aqar.mv_city_price_per_sqm query failed: %s", exc)
        return []


def _build_cache_entry(rows: Iterable[dict]) -> DistrictCacheEntry:
    raw_map: Dict[str, DistrictRow] = {}
    norm_map: Dict[str, DistrictRow] = {}
    for row in rows:
        district = row.get("district")
        price_per_sqm = row.get("price_per_sqm")
        if district is None or price_per_sqm is None:
            continue
        n_val = int(row.get("n") or 0)
        district_str = str(district)
        district_row = DistrictRow(district=district_str, price_per_sqm=float(price_per_sqm), n=n_val)
        existing_raw = raw_map.get(district_str)
        if not existing_raw or district_row.n > existing_raw.n:
            raw_map[district_str] = district_row
        norm_key = normalize_district_key(district_str)
        if not norm_key:
            continue
        existing_norm = norm_map.get(norm_key)
        if not existing_norm or district_row.n > existing_norm.n:
            norm_map[norm_key] = district_row
    return DistrictCacheEntry(raw_map=raw_map, norm_map=norm_map)


def _get_cached_entry(
    db: Session, city_ar: str, property_type: str | None, *, refresh: bool = False
) -> DistrictCacheEntry:
    key = (city_ar, property_type)
    cached = None if refresh else _DISTRICT_CACHE.get(key)
    if cached:
        return cached
    rows = _fetch_city_mv_rows(db, city_ar, property_type)
    entry = _build_cache_entry(rows)
    _DISTRICT_CACHE.set(key, entry)
    return entry


def find_aqar_mv_district_price(
    db: Session,
    *,
    city_ar: str,
    district_raw: str,
    property_type: str | None = None,
) -> tuple[Optional[float], Dict[str, object]]:
    meta: Dict[str, object] = {
        "source": "aqar.mv_city_price_per_sqm",
        "city_used": city_ar,
        "district_raw": district_raw,
        "district_normed": normalize_district_key(district_raw),
        "matched_district": None,
        "n": None,
        "level": None,
        "method": None,
        "property_type": property_type,
    }

    if not city_ar or not district_raw:
        meta["reason"] = "missing_city_or_district"
        return None, meta

    def _attempt_match(entry: DistrictCacheEntry) -> tuple[Optional[float], Dict[str, object]]:
        district_row = entry.raw_map.get(district_raw)
        if district_row and district_row.price_per_sqm > 0:
            return district_row.price_per_sqm, {
                "matched_district": district_row.district,
                "n": district_row.n,
                "level": "district",
                "method": "aqar_mv_exact",
            }

        district_normed_local = meta["district_normed"] or ""
        if district_normed_local:
            district_row = entry.norm_map.get(str(district_normed_local))
            if district_row and district_row.price_per_sqm > 0:
                return district_row.price_per_sqm, {
                    "matched_district": district_row.district,
                    "n": district_row.n,
                    "level": "district",
                    "method": "aqar_mv_norm",
                }

        variants = []
        if "حي " in district_raw:
            variants.append(normalize_district_key(district_raw.replace("حي ", "")))
        separator_variant = SEPARATOR_VARIANT_RE.sub(" ", district_raw)
        if separator_variant != district_raw:
            variants.append(normalize_district_key(separator_variant))

        for variant_norm in variants:
            if not variant_norm or variant_norm == district_normed_local:
                continue
            district_row = entry.norm_map.get(variant_norm)
            if district_row and district_row.price_per_sqm > 0:
                return district_row.price_per_sqm, {
                    "matched_district": district_row.district,
                    "n": district_row.n,
                    "level": "district",
                    "method": "aqar_mv_variant",
                    "district_normed": variant_norm,
                }
        return None, {}

    entry = _get_cached_entry(db, city_ar, property_type)
    val, updates = _attempt_match(entry)
    if val is None:
        entry = _get_cached_entry(db, city_ar, property_type, refresh=True)
        val, updates = _attempt_match(entry)

    if val is not None:
        meta.update(updates)
        return float(val), meta

    meta["reason"] = "no_district_match"
    return None, meta


def _reset_cache_for_tests() -> None:
    _DISTRICT_CACHE.clear()
