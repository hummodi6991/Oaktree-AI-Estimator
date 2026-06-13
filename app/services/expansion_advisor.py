from __future__ import annotations

import json
import logging
import math
import re
import statistics
import time
import os
import uuid
from bisect import bisect_right
from datetime import datetime, timedelta
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.connectors.blackmarble import (
    QUALITY_FILTER_LABEL,
    evaluate_confidence as _blackmarble_evaluate_confidence,
)
from app.core.config import settings
from app.ingest.expansion_advisor_competitors import (
    _CHAIN_KEY_DENYLIST,
    _CHAIN_NAME_NORM_SQL,
)
from app.ml.name_normalization import norm_district
from app.services.aqar_district_match import (
    is_mojibake,
    normalize_district_key,
    normalize_district_key_sql,
)
from app.services.expansion_rerank import generate_rerank
from app.services.rent import aqar_rent_median

logger = logging.getLogger(__name__)


ARCGIS_PARCELS_TABLE = "public.riyadh_parcels_arcgis_proxy"

# Candidate pool limits
_CANDIDATE_POOL_LIMIT = 3500         # pre-filter ceiling for the multi-platform candidate pool;
                                     # sized for ~2x current Aqar volume plus headroom for Bayut
_PER_DISTRICT_MIN_CAP = 5            # minimum parcels per district in stratified mode
_PER_DISTRICT_MAX_CAP = 200          # upper bound per district — raised for listings-only pool
_PER_DISTRICT_HEADROOM_MULTIPLIER = 3  # pull 3x the fair share per district
                                        # to give scoring/reranking headroom

# Expansion Advisor normalized table names (from config)
_EA_ROADS_TABLE = settings.EXPANSION_ROADS_TABLE
_EA_PARKING_TABLE = settings.EXPANSION_PARKING_TABLE
_EA_DELIVERY_TABLE = settings.EXPANSION_DELIVERY_TABLE
_EA_RENT_TABLE = settings.EXPANSION_RENT_TABLE
_EA_COMPETITOR_TABLE = settings.EXPANSION_COMPETITOR_TABLE

# Sentinel returned by the road-distance COALESCE fallbacks when no
# is_major_road segment exists within the 700 m ST_DWithin search radius.
# Any value >= this sentinel should be treated as "unknown", not as a real
# distance — see _road_signal_from_context.
_ROAD_DISTANCE_SENTINEL_M = 5000.0

# ---------------------------------------------------------------------------
# Gate-key to human-readable label mapping (change #4)
# ---------------------------------------------------------------------------
_GATE_HUMAN_LABELS: dict[str, str] = {
    "zoning_fit_pass": "zoning fit",
    "area_fit_pass": "area fit",
    "frontage_access_pass": "frontage/access",
    "parking_pass": "parking",
    "district_pass": "district",
    "cannibalization_pass": "cannibalization",
    "delivery_market_pass": "delivery market",
    "economics_pass": "economics",
    "radiance_growth_pass": "Market growth signal",
    "population_floor_pass": "Population reach floor",
    "commercial_floor_pass": "Commercial activity floor",
    "construction_proximity_pass": "Construction proximity floor",
}


# Hard-fail gates: only failures of these gates flip ``overall_pass`` to
# False. Other gate failures are advisory and must not be used by downstream
# consumers (e.g., the LLM decision-memo generator) to instruct a "Decline"
# headline. Kept module-level so other modules can import a single source of
# truth instead of redefining the set.
# Build HARD_FAIL_GATES at module load. The two structural gates always
# block; the new hard-floor gates (population, commercial activity) are
# only registered as blocking when their settings knob is non-zero, so
# disabling a gate via env-var disables it everywhere — including the
# hard-fail set consumed by ``_candidate_gate_status`` and downstream
# explanation modules. Idempotent across reloads.
_HARD_FAIL_GATES_BASE: frozenset[str] = frozenset({
    "zoning_fit_pass",
    "area_fit_pass",
})
_OPTIONAL_HARD_GATES: set[str] = set()
if int(getattr(settings, "EXPANSION_VIABILITY_POPULATION_HARD_FLOOR", 0) or 0) > 0:
    _OPTIONAL_HARD_GATES.add("population_floor_pass")
if int(getattr(settings, "EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR", 0) or 0) > 0:
    _OPTIONAL_HARD_GATES.add("commercial_floor_pass")
if float(getattr(settings, "EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M", 0) or 0) > 0:
    _OPTIONAL_HARD_GATES.add("construction_proximity_pass")
HARD_FAIL_GATES: frozenset[str] = _HARD_FAIL_GATES_BASE | frozenset(_OPTIONAL_HARD_GATES)

# Advisory-only gates: presence/absence of signal must NOT collapse the
# overall verdict to indeterminate (None). Surfaced in gate_status for the
# UI but excluded from the unknown count used by ``overall_pass``.
ADVISORY_ONLY_GATES: frozenset[str] = frozenset({
    "radiance_growth_pass",
})


def _humanize_gate_list(values: list[Any] | None, lang: str = "en") -> list[str]:
    labels: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        label = _gate_key_to_label(str(value), lang)
        if not label or label in seen:
            continue
        seen.add(label)
        labels.append(label)
    return labels


def _gate_key_to_label(gate_key: str, lang: str = "en") -> str:
    """Return a human-friendly label for an internal gate key.

    Delegates to the i18n module so the gate vocabulary has a single
    source of truth. ``humanize_gate(key, "en")`` reproduces the legacy
    ``_GATE_HUMAN_LABELS`` lookup + fallback byte-for-byte.
    """
    from app.services.expansion_advisor_i18n import humanize_gate

    return humanize_gate(gate_key, lang)


def _gate_verdict_label(overall_pass: Any) -> str:
    """Map the tri-state overall_pass to a stable string verdict.

    True  -> "pass"
    False -> "fail"
    None  -> "unknown"
    """
    if overall_pass is True:
        return "pass"
    if overall_pass is False:
        return "fail"
    return "unknown"


# ---------------------------------------------------------------------------
# Category alias expansion for delivery & competitor matching
# ---------------------------------------------------------------------------
_CATEGORY_ALIAS_MAP: dict[str, dict] = {
    "fast food": {
        "keys": ["burger", "pizza", "chicken", "fast_food"],
        "raw_patterns": [
            "fast.food", "fast_food", "qsr", "burger", "hamburger",
            "chicken", "broasted", "fried.chicken", "pizza", "pizzeria",
            "وجبات سريعة", "برجر", "دجاج", "بيتزا", "فاست فود",
        ],
    },
    "burger": {
        "keys": ["burger"],
        "raw_patterns": ["burger", "hamburger", "برجر"],
    },
    "pizza": {
        "keys": ["pizza"],
        "raw_patterns": ["pizza", "pizzeria", "بيتزا"],
    },
    "chicken": {
        "keys": ["chicken"],
        "raw_patterns": ["chicken", "broasted", "fried.chicken", "wings", "دجاج"],
    },
    "cafe": {
        "keys": ["cafe", "coffee", "bakery", "dessert"],
        "raw_patterns": [
            "cafe", "coffee", "bakery", "dessert", "pastry",
            "قهوة", "مقهى", "كافيه", "مخبز", "حلويات",
        ],
    },
    "coffee": {
        "keys": ["cafe", "coffee", "bakery", "dessert"],
        "raw_patterns": [
            "coffee", "cafe", "قهوة", "مقهى", "كافيه",
        ],
    },
    "shawarma": {
        "keys": ["shawarma", "traditional"],
        "raw_patterns": [
            "shawarma", "شاورما", "شاورمة",
        ],
    },
    "traditional": {
        "keys": ["traditional"],
        "raw_patterns": [
            "arabic", "middle.eastern", "saudi", "lebanese", "syrian",
            "shawarma", "falafel", "kabsa", "mandi",
            "شعبي", "عربي", "كبسة", "مندي", "شاورما",
        ],
    },
    "indian": {
        "keys": ["indian", "asian"],
        "raw_patterns": [
            "indian", "هندي", "biryani", "بيرياني", "curry",
        ],
    },
    "asian": {
        "keys": ["asian"],
        "raw_patterns": [
            "chinese", "japanese", "sushi", "korean", "thai",
            "indian", "asian", "ramen", "noodle",
        ],
    },
    "seafood": {
        "keys": ["seafood"],
        "raw_patterns": ["seafood", "fish", "shrimp", "سمك", "بحري", "مأكولات بحرية"],
    },
    "healthy": {
        "keys": ["healthy"],
        "raw_patterns": ["salad", "healthy", "vegan", "vegetarian", "poke", "bowl"],
    },
}


# Arabic ↔ English category aliases for delivery marketplace matching.
# Each entry maps a canonical key to all known variants (Arabic + English).
_CATEGORY_ALIASES: dict[str, list[str]] = {
    "burger": ["burger", "برجر", "burgers", "hamburger", "هامبرغر"],
    "fast food": ["fast food", "وجبات سريعة", "فاست فود", "fast_food", "fastfood"],
    "pizza": ["pizza", "بيتزا", "بيتسا"],
    "chicken": ["chicken", "دجاج", "فراخ"],
    "shawarma": ["shawarma", "شاورما", "شاورمة"],
    "coffee": ["coffee", "قهوة", "كافيه", "cafe", "café"],
    "fine dining": ["fine dining", "مطعم فاخر", "فاين داينينق"],
    "seafood": ["seafood", "مأكولات بحرية", "أسماك", "سي فود"],
    "sandwich": ["sandwich", "سندويش", "سندوتش", "سندويتش"],
    "bakery": ["bakery", "مخبز", "مخابز", "معجنات"],
    "dessert": ["dessert", "حلويات", "حلى"],
    "juice": ["juice", "عصير", "عصائر"],
    "healthy": ["healthy", "صحي", "سلطات", "salad"],
    "asian": ["asian", "آسيوي", "صيني", "chinese", "sushi", "سوشي", "ياباني", "japanese"],
    "indian": ["indian", "هندي"],
    "italian": ["italian", "إيطالي", "pasta", "باستا"],
    "breakfast": ["breakfast", "فطور", "إفطار"],
    "grills": ["grills", "مشويات", "مشاوي", "kebab", "كباب"],
    "biryani": ["biryani", "برياني"],
    "broasted": ["broasted", "بروستد", "بروست"],
    "arabic": ["arabic", "arab", "saudi", "lebanese",
               "عربي", "سعودي", "لبناني", "مشويات", "مندي", "كبسة"],
}


# Map user-facing search categories to the broad delivery-table buckets.
# The expansion_delivery_market table normalizes all categories into:
#   international, traditional, coffee_bakery, seafood
_CATEGORY_TO_DELIVERY_BUCKETS: dict[str, list[str]] = {
    "burger": ["international"],
    "fast food": ["international", "traditional"],
    "pizza": ["international"],
    "chicken": ["international", "traditional", "دجاج", "broast"],
    "shawarma": ["traditional", "شاورما", "شاورمة"],
    "coffee": ["coffee_bakery"],
    "cafe": ["coffee", "coffee_bakery"],
    "fine dining": ["international"],
    "seafood": ["seafood", "بحري", "مأكولات بحرية"],
    "sandwich": ["international", "traditional"],
    "bakery": ["coffee_bakery", "مخبوزات", "معجنات"],
    "dessert": ["coffee_bakery", "حلى", "حلويات"],
    "juice": ["coffee_bakery", "عصائر", "مشروبات"],
    "healthy": ["international"],
    "asian": ["international", "آسيوي"],
    "indian": ["international", "هندي"],
    "italian": ["international", "إيطالي"],
    "breakfast": ["coffee_bakery", "traditional", "فطور"],
    "grills": ["traditional", "مشويات", "مشاوي", "سعودي", "مندي", "كبسة"],
    "biryani": ["traditional"],
    "broasted": ["traditional", "دجاج", "مشويات"],
    "arabic": ["traditional", "grills", "عربي", "سعودي", "مشويات", "مندي", "كبسة"],
    "international": ["international"],
    "traditional": ["traditional"],
    "coffee_bakery": ["coffee_bakery"],
}


def _precompute_district_delivery_stats(
    db: Session,
    delivery_table: str,
    category: str,
) -> tuple[dict[str, dict], dict[str, float]]:
    """Pre-compute district-level delivery stats for fallback scoring.

    Returns:
        district_stats: {normalized_district_key: {total, cat_count, platforms,
                         avg_rating, avg_eta, late_night}}
        city_benchmarks: {median_total, median_cat, city_avg_rating, city_avg_eta}
    """
    district_stats: dict[str, dict] = {}
    city_benchmarks: dict[str, float] = {}

    try:
        # 1. Per-district totals
        _rows = db.execute(
            text(f"""
                SELECT
                    lower(COALESCE(district, '')) AS dist,
                    COUNT(*) AS total,
                    COUNT(DISTINCT platform) AS platforms,
                    AVG(rating) FILTER (WHERE rating IS NOT NULL) AS avg_rating,
                    AVG(eta_minutes) FILTER (WHERE eta_minutes IS NOT NULL) AS avg_eta,
                    COUNT(*) FILTER (WHERE supports_late_night IS TRUE) AS late_night
                FROM {delivery_table}
                WHERE city = 'riyadh'
                GROUP BY lower(COALESCE(district, ''))
                HAVING COUNT(*) >= 3
            """)
        ).mappings().all()

        for r in _rows:
            key = normalize_district_key(str(r["dist"]))
            if not key:
                continue
            district_stats[key] = {
                "total": int(r["total"]),
                "cat_count": 0,  # populated below
                "platforms": int(r["platforms"]),
                "avg_rating": float(r["avg_rating"]) if r["avg_rating"] else None,
                "avg_eta": float(r["avg_eta"]) if r["avg_eta"] else None,
                "late_night": int(r["late_night"]),
            }

        # 2. Per-district category counts for the search category
        _cat_terms = _expand_category_terms(category)
        _cat_params = {f"ct_{i}": f"%{t}%" for i, t in enumerate(_cat_terms)}
        _cat_or = " OR ".join(
            f"lower(COALESCE(category, '')) LIKE :ct_{i}"
            for i in range(len(_cat_terms))
        )
        _cat_rows = db.execute(
            text(f"""
                SELECT
                    lower(COALESCE(district, '')) AS dist,
                    COUNT(*) AS cat_count
                FROM {delivery_table}
                WHERE city = 'riyadh' AND ({_cat_or})
                GROUP BY lower(COALESCE(district, ''))
            """),
            _cat_params,
        ).mappings().all()

        for r in _cat_rows:
            key = normalize_district_key(str(r["dist"]))
            if key in district_stats:
                district_stats[key]["cat_count"] = int(r["cat_count"])

        # 3. City-wide benchmarks
        all_totals = [v["total"] for v in district_stats.values()]
        all_cats = [v["cat_count"] for v in district_stats.values()]
        if all_totals:
            _sorted_totals = sorted(all_totals)
            city_benchmarks["median_total"] = float(_sorted_totals[len(_sorted_totals) // 2])
            _sorted_cats = sorted(all_cats)
            city_benchmarks["median_cat"] = float(max(1, _sorted_cats[len(_sorted_cats) // 2]))
            _ratings = [v["avg_rating"] for v in district_stats.values() if v["avg_rating"]]
            if _ratings:
                city_benchmarks["city_avg_rating"] = sum(_ratings) / len(_ratings)
            _etas = [v["avg_eta"] for v in district_stats.values() if v["avg_eta"]]
            if _etas:
                city_benchmarks["city_avg_eta"] = sum(_etas) / len(_etas)

        logger.info(
            "District delivery stats: %d districts, median_total=%.0f, median_cat=%.0f",
            len(district_stats),
            city_benchmarks.get("median_total", 0),
            city_benchmarks.get("median_cat", 0),
        )
    except Exception:
        logger.exception("_precompute_district_delivery_stats failed")

    return district_stats, city_benchmarks


def _district_momentum_score(db: Session) -> dict[str, dict[str, Any]]:
    """Per-search, per-district 30-day activity momentum.

    Joins ``commercial_unit`` against ``external_feature_polygons_mat`` —
    a materialized view of pre-parsed district polygons scoped to
    ``aqar_district_hulls`` (146 Riyadh districts), refreshed on demand
    via the "Refresh external_feature_polygons_mat" GitHub Actions
    workflow. The matview avoids re-parsing GeoJSON polygons on every
    expansion_search request and provides a GIST-indexed ``geom`` column
    for ``ST_Contains``.

    Returns::

        {
            normalize_district_key(label): {
                "momentum_score": float,        # 0-100, fed into _listing_quality_score
                "activity_30d": int,            # creates OR updates in the last 30 days
                "active_in_district": int,
                "percentile_raw": float,        # 0-1, percentile_rank over activity_30d
                "percentile_absolute": float,   # 0-1, percentile_rank over activity_30d / active
                "percentile_composite": float,  # 0-1, 0.5*raw + 0.5*absolute
                "district_label": str,          # raw label from external_feature.properties
                "sample_floor_applied": bool,   # always False in the returned dict
            }
        }

    The Arabic-label key-space matches what the scoring path consumes
    (``cl.district_ar`` on the primary pool, spatial backfill from
    ``riyadh_parcels_arcgis_raw.district_label`` on the fallback pool —
    both Arabic). Lookups via ``normalize_district_key`` match by
    construction; the helper applies uniformly to Tier 1, 2, and 3
    candidates.

    The matview is scoped to ``aqar_district_hulls`` only. The
    ``osm_districts`` layer was dropped after investigation found it
    contaminated with non-Riyadh data; see the cleanup note at
    ``docs/osm_districts_removal.md``. DISTINCT ON (cu.aqar_id)
    ORDER BY cu.aqar_id, dp.feature_id ensures each listing resolves
    to exactly one district deterministically.

    A listing counts toward ``activity_30d`` if EITHER ``aqar_created_at``
    OR ``aqar_updated_at`` falls in the trailing window — null-safe,
    each listing counted at most once via the SQL OR predicate.
    Districts below ``_MOMENTUM_SAMPLE_FLOOR`` are excluded so callers
    resolve them to neutral 50.0 via ``.get(district_norm)`` returning
    None.

    Returns an empty dict on any DB failure so the caller falls back to
    neutral everywhere without raising.
    """
    try:
        rows = db.execute(
            text(
                """
                WITH listing_district AS (
                    SELECT DISTINCT ON (cu.aqar_id)
                        cu.aqar_id,
                        cu.aqar_created_at,
                        cu.aqar_updated_at,
                        dp.district_label
                    FROM commercial_unit cu
                    JOIN external_feature_polygons_mat dp
                      ON ST_Contains(
                           dp.geom,
                           ST_SetSRID(ST_MakePoint(cu.lon, cu.lat), 4326)
                         )
                    WHERE cu.lat IS NOT NULL
                      AND cu.lon IS NOT NULL
                      AND cu.status = 'active'
                      AND dp.district_label IS NOT NULL
                    ORDER BY
                      cu.aqar_id,
                      dp.feature_id
                ),
                district_counts AS (
                    SELECT
                        district_label,
                        COUNT(*) AS active_in_district,
                        COUNT(*) FILTER (
                          WHERE (
                            (aqar_created_at IS NOT NULL
                              AND aqar_created_at >= now() - (:window_days || ' days')::interval)
                            OR
                            (aqar_updated_at IS NOT NULL
                              AND aqar_updated_at >= now() - (:window_days || ' days')::interval)
                          )
                        ) AS activity_30d
                    FROM listing_district
                    GROUP BY district_label
                    HAVING COUNT(*) >= :sample_floor
                ),
                ranked AS (
                    SELECT
                        district_label,
                        activity_30d,
                        active_in_district,
                        (activity_30d::float / NULLIF(active_in_district, 0)::float)
                            AS momentum_raw,
                        percent_rank() OVER (ORDER BY activity_30d) AS percentile_raw,
                        percent_rank() OVER (
                            ORDER BY (activity_30d::float / NULLIF(active_in_district, 0)::float)
                        ) AS percentile_absolute
                    FROM district_counts
                )
                SELECT
                    district_label,
                    activity_30d,
                    active_in_district,
                    COALESCE(percentile_raw, 0.5)      AS percentile_raw,
                    COALESCE(percentile_absolute, 0.5) AS percentile_absolute
                FROM ranked
                """
            ),
            {
                "window_days": _MOMENTUM_WINDOW_DAYS,
                "sample_floor": _MOMENTUM_SAMPLE_FLOOR,
            },
        ).mappings().all()
    except Exception:
        logger.exception("_district_momentum_score failed")
        return {}

    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        raw = str(r["district_label"])
        key = normalize_district_key(raw)
        if not key:
            continue
        p_raw = float(r["percentile_raw"])
        p_abs = float(r["percentile_absolute"])
        p_composite = 0.5 * p_raw + 0.5 * p_abs
        out[key] = {
            "momentum_score": round(_clamp(p_composite * 100.0), 2),
            "activity_30d": int(r["activity_30d"]),
            "active_in_district": int(r["active_in_district"]),
            "percentile_raw": round(p_raw, 4),
            "percentile_absolute": round(p_abs, 4),
            "percentile_composite": round(p_composite, 4),
            "district_label": raw,
            "sample_floor_applied": False,
        }
    return out


def _expand_category_terms(category: str) -> list[str]:
    """Return delivery-table bucket names that match a user search category.

    The expansion_delivery_market table stores only broad buckets
    (international, traditional, coffee_bakery, seafood), not specific
    cuisines. This maps user search terms to the relevant buckets,
    plus keeps the original term and any Arabic aliases for future-proofing.
    """
    cat_lower = category.strip().lower()
    terms = {cat_lower}

    # Add delivery table bucket names
    buckets = _CATEGORY_TO_DELIVERY_BUCKETS.get(cat_lower)
    if buckets:
        terms.update(buckets)
    else:
        # Unknown category — try matching against Arabic aliases
        for _key, aliases in _CATEGORY_ALIASES.items():
            if cat_lower in [a.lower() for a in aliases]:
                bucket_match = _CATEGORY_TO_DELIVERY_BUCKETS.get(_key)
                if bucket_match:
                    terms.update(bucket_match)
                break

    # If still no bucket match, default to international (broadest)
    if not terms.intersection({"international", "traditional", "coffee_bakery", "seafood"}):
        terms.add("international")

    return sorted(terms)


def _expand_category(category: str) -> dict:
    """Expand a search category into matching keys and regex patterns."""
    cat_lower = category.lower().strip()
    aliases = _CATEGORY_ALIAS_MAP.get(cat_lower)

    if aliases:
        keys = aliases["keys"]
        regex = "|".join(re.escape(p).replace(r"\.", ".") for p in aliases["raw_patterns"])
    else:
        keys = [cat_lower.replace(" ", "_")]
        regex = re.escape(cat_lower).replace(r"\ ", ".").replace(r"\.", ".")

    return {
        "keys": keys,
        "regex": regex,
        "like": f"%{cat_lower}%",
    }


def _clean_district_display(raw: str | None) -> str | None:
    """Strip Unicode control chars and BOM from display strings."""
    if not raw:
        return None
    import unicodedata
    # Remove BOM, zero-width chars, and bidi controls
    cleaned = raw.replace("\ufeff", "").replace("\ufffe", "")
    cleaned = "".join(
        ch for ch in cleaned
        if unicodedata.category(ch) not in ("Cc", "Cf") or ch in ("\n", "\r", "\t", " ")
    )
    cleaned = cleaned.strip()
    return cleaned if cleaned else None


def _canonicalize_district_label(
    district_raw: str | None,
    district_lookup: dict[str, dict[str, str]] | None = None,
) -> dict[str, str | None]:
    """Derive canonical district fields from a raw district string.

    Returns a dict with:
      district_key       – normalized key (e.g. "الملقا")
      district_name_ar   – clean Arabic label (from lookup if available)
      district_name_en   – English label (from lookup if available)
      district_display   – best display label (arabic → english → key → fallback)
    """
    if not district_raw or not district_raw.strip():
        return {
            "district_key": None,
            "district_name_ar": None,
            "district_name_en": None,
            "district_display": None,
        }

    norm_key = normalize_district_key(district_raw)
    if not norm_key:
        # Even if normalization fails, try to provide a safe display fallback
        cleaned = _clean_district_display(district_raw)
        if cleaned and not is_mojibake(cleaned):
            return {
                "district_key": None,
                "district_name_ar": None,
                "district_name_en": None,
                "district_display": cleaned,
            }
        return {
            "district_key": None,
            "district_name_ar": None,
            "district_name_en": None,
            "district_display": None,
        }

    # Try canonical lookup first (keyed by normalized district key)
    name_ar: str | None = None
    name_en: str | None = None
    if district_lookup and norm_key in district_lookup:
        entry = district_lookup[norm_key]
        name_ar = _clean_district_display(entry.get("label_ar")) or None
        name_en = _clean_district_display(entry.get("label_en")) or None

    # If no lookup hit, use the raw string as Arabic label if it looks okay
    if not name_ar:
        raw_stripped = _clean_district_display(district_raw)
        name_ar = raw_stripped if raw_stripped and not is_mojibake(raw_stripped) else None

    # Build display: prefer arabic → english → normalized key
    # Fall back if arabic label looks garbled
    if name_ar and is_mojibake(name_ar):
        display = name_en or norm_key.replace("_", " ")
    else:
        display = name_ar or name_en or norm_key.replace("_", " ")

    return {
        "district_key": norm_key,
        "district_name_ar": name_ar,
        "district_name_en": name_en,
        "district_display": display,
    }


def _build_district_lookup(db: Session) -> dict[str, dict[str, str]]:
    """Build a lookup table from external_feature polygons: norm_key → {label_ar, label_en}.

    Used to provide canonical district names for expansion candidates.
    """
    try:
        with db.begin_nested():
            rows = db.execute(
                text(
                    """
                    SELECT
                        COALESCE(
                            NULLIF(ef.properties->>'district', ''),
                            NULLIF(ef.properties->>'district_raw', ''),
                            NULLIF(ef.properties->>'name', '')
                        ) AS label_ar,
                        NULLIF(ef.properties->>'district_en', '') AS label_en
                    FROM external_feature ef
                    WHERE ef.layer_name = 'aqar_district_hulls'
                      AND COALESCE(
                            NULLIF(ef.properties->>'district', ''),
                            NULLIF(ef.properties->>'district_raw', ''),
                            NULLIF(ef.properties->>'name', '')
                      ) IS NOT NULL
                    """
                )
            ).fetchall()
    except Exception:
        logger.debug("_build_district_lookup query failed", exc_info=True)
        return {}

    lookup: dict[str, dict[str, str]] = {}
    for row in rows:
        label_ar = (row[0] or "").strip()
        label_en = (row[1] or "").strip() or None
        if not label_ar:
            continue
        nk = normalize_district_key(label_ar)
        if not nk:
            continue
        existing = lookup.get(nk)
        if existing is None:
            lookup[nk] = {
                "label_ar": label_ar,
                "label_en": label_en,
            }
        elif label_en and not existing.get("label_en"):
            existing["label_en"] = label_en
    return lookup


# ---------------------------------------------------------------------------
# Session-level caches to avoid repeated DB roundtrips within a single request
# ---------------------------------------------------------------------------
_district_lookup_cache: dict[int, dict[str, dict[str, str]]] = {}
_table_avail_cache: dict[str, bool] = {}


def _cached_district_lookup(db: Session) -> dict[str, dict[str, str]]:
    """Return district lookup, cached by db session id within a process."""
    key = id(db)
    if key not in _district_lookup_cache:
        _district_lookup_cache[key] = _build_district_lookup(db)
    return _district_lookup_cache[key]


def _resolve_district_to_ar_key(
    input_value: str,
    lookup: dict[str, dict[str, str]],
) -> str | None:
    """Resolve a district string (Arabic or English) to its canonical Arabic norm-key.

    - Arabic input (already a key in ``lookup``): returned as-is after
      ``normalize_district_key``. Because the first step is normalization,
      raw forms with the ``حي`` prefix or ``أ/إ/آ/ى`` variants resolve to
      the same canonical key — a beneficial side effect over a direct
      dict membership check.
    - English input matching a ``label_en`` in ``lookup`` (case- and
      whitespace-insensitive): the corresponding norm-key is returned.
    - No match: returns ``None``. Callers decide whether to fall back,
      skip, or pass-through the original string.
    """
    if not input_value:
        return None
    normalized = normalize_district_key(input_value)
    if normalized and normalized in lookup:
        return normalized
    input_lower = input_value.strip().lower()
    if not input_lower:
        return None
    for nk, entry in lookup.items():
        label_en = (entry.get("label_en") or "").strip()
        if label_en and label_en.lower() == input_lower:
            return nk
    return None


def _cached_table_available(db: Session, table_name: str) -> bool:
    """Cache table availability checks per table name within a process."""
    if table_name not in _table_avail_cache:
        _table_avail_cache[table_name] = _table_available(db, table_name)
    return _table_avail_cache[table_name]


def _cached_ea_table_has_rows(db: Session, table_name: str) -> bool:
    """Cache EA table row-presence checks."""
    cache_key = f"ea_rows:{table_name}"
    if cache_key not in _table_avail_cache:
        _table_avail_cache[cache_key] = _ea_table_has_rows(db, table_name)
    return _table_avail_cache[cache_key]


def _cached_column_exists(db: Session, table_name: str, column_name: str) -> bool:
    """Check whether *column_name* exists on *table_name*, cached per process."""
    cache_key = f"col:{table_name}.{column_name}"
    if cache_key not in _table_avail_cache:
        try:
            result = db.execute(
                text(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_name = :tbl AND column_name = :col LIMIT 1"
                ),
                {"tbl": table_name, "col": column_name},
            ).scalar()
            _table_avail_cache[cache_key] = result is not None
        except Exception:
            _table_avail_cache[cache_key] = False
    return _table_avail_cache[cache_key]


def clear_expansion_caches() -> None:
    """Clear all in-process caches. Call between requests or in tests."""
    _district_lookup_cache.clear()
    _table_avail_cache.clear()


_EXPANSION_CITY = "riyadh"
_EXPANSION_AQAR_ASSET = "commercial"
_EXPANSION_AQAR_UNIT = "retail"
_EXPANSION_DEFAULT_RENT_SAR_M2_YEAR = 900.0
_EXPANSION_VERSION = "expansion_advisor_v7"
_EXPANSION_PARCEL_SOURCE = "listings_only"
_EXPANSION_EXCLUDED_SOURCES = ["arcgis_parcels", "hungerstation_poi", "suhail", "inferred_parcels"]
_EXPANSION_BULK_PERSIST_CHUNK_SIZE = max(
    10,
    int(os.getenv("EXPANSION_BULK_PERSIST_CHUNK_SIZE", "100")),
)


# ---------------------------------------------------------------------------
# Service-model-aware catchment radii.
# Real-world F&B catchment scales differ by service model:
#   dine_in:        destination restaurants, 15-20 minute drive → 3-5 km
#   delivery_first: bounded by platform delivery radius        → 3 km
#   qsr:            convenience-driven walk/drive-thru          → 1.5 km
#   cafe:           walking + short drives                       → 800 m-1 km
# Using a fixed 1200 m catchment for all four (the legacy default)
# materially under-scores dine-in and delivery-first candidates.
# ---------------------------------------------------------------------------
_CATCHMENT_RADII_M: dict[str, dict[str, float]] = {
    # dine_in competition tightened to 1000 m: a direct-competition trade area
    # distinct from the 3500 m demand catchment, so net-of-supply differencing
    # spans two genuinely different scopes (same-category counts at 3000 m
    # saturated the whitespace curve — p50 ~230 vs a domain ending at 25).
    "dine_in":        {"demand": 3500.0, "competition": 1000.0, "provider": 3500.0},
    # delivery_first competition tightened 2500 -> 1000 m for the same reason
    # as dine_in: same-category counts at 2500 m are both enormous and nearly
    # constant (probe p50/p75/p90 all ~145, max 149, 0% within the curve REF),
    # so the whitespace component floored 100% of the shortlist at 15.0 — dead
    # signal. The discriminating variation lives at 1000 m (probe p50 ~16, p90
    # ~29). demand/provider stay at 3000 m (platform delivery radius).
    "delivery_first": {"demand": 3000.0, "competition": 1000.0, "provider": 3000.0},
    # qsr competition tightened 1200 -> 1000 m for the same reason as dine_in
    # and delivery_first: same-category counts at 1200 m run p50 35 / p75 62
    # on broad scopes — too large for the log-decay domain, flooring 67-82%
    # of a city-wide probe at 15.0. The discriminating variation lives at
    # 1000 m (burger-scope probe p25 4 / p50 16 / p75 24 / p90 34).
    # demand/provider stay at 1500 m (convenience walk/drive-thru catchment).
    "qsr":            {"demand": 1500.0, "competition": 1000.0, "provider": 1500.0},
    "cafe":           {"demand": 1000.0, "competition":  800.0, "provider": 1000.0},
}

# Population-score saturation references must scale with the catchment
# radius. A dine-in site with a 3.5 km catchment in Al Olaya can have
# 200-300k residents; saturating at 80k (the legacy QSR-scaled reference)
# would compress every populous site to the same score.
_POPULATION_SCORE_REFERENCE: dict[str, float] = {
    "dine_in":        250000.0,
    "delivery_first": 180000.0,
    "qsr":             80000.0,
    "cafe":            40000.0,
}


def _catchment_radii(service_model: str | None) -> dict[str, float]:
    """Return the (demand, competition, provider) radii for this service model.

    Falls back to QSR values for unknown service models — QSR's 1500 m
    demand radius is the closest to the legacy 1200 m constant and is
    the safest default for a brief with an unexpected service_model.
    """
    return _CATCHMENT_RADII_M.get(
        (service_model or "qsr").lower(),
        _CATCHMENT_RADII_M["qsr"],
    )


def _population_reference(service_model: str | None) -> float:
    """Return the population-score saturation reference for this service model."""
    return _POPULATION_SCORE_REFERENCE.get(
        (service_model or "qsr").lower(),
        _POPULATION_SCORE_REFERENCE["qsr"],
    )


def _demand_generator_radius_m(service_model: str | None) -> float:
    """Demand-generator enrich/index radius (metres) for this service model.

    Change-1 (Phase-A discrepancy E.2): the L1 demand-generator index must be
    computed at the SAME catchment the model is scored at, so read each model's
    demand radius from ``_CATCHMENT_RADII_M[model]['demand']`` instead of the
    flat ``EXPANSION_DEMAND_GENERATOR_RADIUS_M``:

        dine_in -> 3500 (UNCHANGED — already equal to the flat default),
        qsr     -> 1500, cafe -> 1000, delivery_first -> 3000.

    Any model absent from ``_CATCHMENT_RADII_M`` falls back to the flat
    ``EXPANSION_DEMAND_GENERATOR_RADIUS_M`` (mirrors ``_catchment_radii``'s
    fallback intent without forcing qsr's 1500 m onto an unknown model).
    """
    radii = _CATCHMENT_RADII_M.get((service_model or "").lower())
    if radii and "demand" in radii:
        return float(radii["demand"])
    return float(settings.EXPANSION_DEMAND_GENERATOR_RADIUS_M)


def _chunked(seq: list[Any], size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _dedupe_candidates(
    candidates: list[dict[str, Any]],
    *,
    aggressive: bool = False,
) -> list[dict[str, Any]]:
    """Post-ranking dedupe: collapse near-clone candidates.

    Uses a multi-key approach:
    1. Exact parcel_id match (primary key)
    2. Tight spatial+attribute composite key:
       - snapped centroid (0.0005 degree ≈ 55m grid)
       - normalized district key
       - rounded area bucket (50m² steps)
       - rounded rent bucket (100 SAR steps)
       - nearest-branch distance bucket (500m steps)

    Candidates with distinct non-empty parcel_ids are NEVER collapsed
    by spatial/attribute keys — parcel_id is the strongest identity.

    When *aggressive=True* (used for report shortlist), additional keys:
    - economics-similarity: district + area bucket + economics bucket + rent bucket
    - district+area+score composite key for sub-55m position variants

    Keeps the highest-ranked (first) candidate in each cluster.
    """
    seen_pid: set[str] = set()
    seen_spatial: set[str] = set()
    result: list[dict[str, Any]] = []
    for c in candidates:
        parcel_id = (c.get("parcel_id") or "").strip()
        lat = _safe_float(c.get("lat"))
        lon = _safe_float(c.get("lon"))
        district_key = c.get("district_key") or normalize_district_key(c.get("district"))
        area_bucket = int(round(_safe_float(c.get("area_m2")) / 50.0))
        rent_bucket = int(round(_safe_float(c.get("estimated_rent_sar_m2_year")) / 100.0))
        branch_dist = c.get("distance_to_nearest_branch_m")
        branch_bucket = int(round(_safe_float(branch_dist) / 500.0)) if branch_dist is not None else -1
        economics_bucket = int(round(_safe_float(c.get("economics_score")) / 5.0))

        # 1. Exact parcel_id dedupe
        if parcel_id:
            if parcel_id in seen_pid:
                continue
            seen_pid.add(parcel_id)
            # Candidates with a real parcel_id skip spatial dedupe —
            # different parcels at nearby locations are genuinely distinct.
            result.append(c)
            continue

        # 2. Tight spatial+attribute grid (55m snap vs old 110m)
        spatial_key = (
            f"{round(lat, 4) // 0.0005 * 0.0005:.4f}|{round(lon, 4) // 0.0005 * 0.0005:.4f}|{district_key}"
            f"|{area_bucket}|{rent_bucket}|{branch_bucket}"
        )

        keys: list[str] = [spatial_key]

        # Aggressive mode: extra composite keys for report shortlists.
        # Economics-similarity key only applied in aggressive mode to avoid
        # over-collapsing spatially distinct candidates in the main ranked list.
        if aggressive and district_key:
            econ_key = f"econ:{district_key}|{area_bucket}|{economics_bucket}|{rent_bucket}"
            keys.append(econ_key)
        if aggressive and district_key:
            score_bucket = int(round(_safe_float(c.get("final_score")) / 2.0))
            keys.append(f"dsa:{district_key}|{area_bucket}|{score_bucket}|{rent_bucket}")

        if any(k in seen_spatial for k in keys):
            continue
        for k in keys:
            seen_spatial.add(k)
        result.append(c)
    return result


def _dedupe_score_clones(candidates: list[dict[str, Any]], max_results: int) -> list[dict[str, Any]]:
    """Remove near-duplicate candidates that appear identical to users.

    Two candidates are near-duplicates if they share the same district,
    area within 5%, final score within 0.3 points, and same rent rate.
    Keeps the highest-scored candidate in each cluster.
    """
    if not candidates:
        return candidates
    # Assumes candidates are already sorted by final_score descending.
    kept: list[dict[str, Any]] = []
    for cand in candidates:
        c_dist = cand.get("district", "")
        c_area = cand.get("area_m2", 0) or 0
        c_score = cand.get("final_score", 0) or 0
        c_rent = cand.get("estimated_rent_sar_m2_year", 0) or 0
        is_dup = False
        for ex in kept:
            ex_area = ex.get("area_m2", 0) or 0
            if (
                ex.get("district", "") == c_dist
                and abs(c_score - (ex.get("final_score", 0) or 0)) <= 0.3
                and (ex.get("estimated_rent_sar_m2_year", 0) or 0) == c_rent
                and ex_area > 0
                and abs(c_area - ex_area) / ex_area <= 0.05
            ):
                is_dup = True
                break
        if not is_dup:
            kept.append(cand)
        if len(kept) >= max_results:
            break
    return kept


def _safe_json_dumps(obj: Any, **kwargs: Any) -> str:
    """json.dumps that replaces NaN/Infinity with None to avoid serialization errors."""
    kwargs.setdefault("ensure_ascii", False)
    return json.dumps(obj, default=str, **kwargs)


class _SafeFloatEncoder(json.JSONEncoder):
    """JSON encoder that converts NaN and Infinity to None."""

    def default(self, o: Any) -> Any:
        return super().default(o)

    def encode(self, o: Any) -> str:
        return super().encode(_sanitize_for_json(o))


def _sanitize_for_json(obj: Any) -> Any:
    """Recursively replace NaN/Infinity float values with None."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]
    return obj


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    if math.isnan(value):
        return low
    return max(low, min(high, value))


# Rerank metadata fields attached to every candidate, whether or not the
# bounded LLM reranker ran. Consumers rely on the presence of these keys.
_RERANK_STATUS_FLAG_OFF = "flag_off"
_RERANK_STATUS_BELOW_MIN = "shortlist_below_minimum"
_RERANK_STATUS_LLM_FAILED = "llm_failed"
_RERANK_STATUS_APPLIED = "applied"
_RERANK_STATUS_UNCHANGED = "unchanged"
_RERANK_STATUS_OUTSIDE_CAP = "outside_rerank_cap"


def _apply_rerank_to_candidates(
    candidates: list[dict[str, Any]],
    brand_profile: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Attach rerank metadata to every candidate and, when enabled, apply
    bounded LLM shortlist reranking (Phase 2).

    This function is safe to call whether or not the feature flag is on:
    with the flag off it annotates each candidate with ``rerank_status``
    ``"flag_off"`` and leaves ``final_rank == deterministic_rank``, so the
    candidate list order is unchanged. With the flag on and a valid LLM
    response, the in-shortlist candidates are reordered by ``new_rank`` and
    the returned list is sorted by ``final_rank`` ascending.

    The six metadata fields attached to every candidate:
      - deterministic_rank (1-based position before reranking)
      - final_rank         (1-based position after reranking)
      - rerank_applied     (True iff the LLM moved this candidate)
      - rerank_reason      (structured reason dict when applied, else None)
      - rerank_delta       (final_rank - deterministic_rank; 0 when unchanged)
      - rerank_status      (one of the _RERANK_STATUS_* constants)
    """
    if not candidates:
        return candidates

    # 1. Assign deterministic_rank + default metadata to every candidate.
    for idx, c in enumerate(candidates, start=1):
        c["deterministic_rank"] = idx
        c["final_rank"] = idx
        c["rerank_applied"] = False
        c["rerank_reason"] = None
        c["rerank_delta"] = 0
        c["rerank_status"] = _RERANK_STATUS_FLAG_OFF

    cap = settings.EXPANSION_LLM_RERANK_SHORTLIST_SIZE
    min_size = settings.EXPANSION_LLM_RERANK_MIN_SHORTLIST
    rerank_shortlist_size = min(len(candidates), cap)

    # 2. Mark candidates beyond the shortlist cap up front (their status
    #    stays correct regardless of which fallback branch fires below).
    for c in candidates[rerank_shortlist_size:]:
        c["rerank_status"] = _RERANK_STATUS_OUTSIDE_CAP

    # 3. Call the bounded reranker. Returns None on any failure path
    #    (flag off, below-min, ceiling exceeded, client error, JSON parse
    #    failure, validation failure) — deterministic order is preserved.
    decisions = generate_rerank(candidates[:rerank_shortlist_size], brand_profile)

    if decisions is None:
        # Pick the right status code for the in-shortlist candidates.
        if not settings.EXPANSION_LLM_RERANK_ENABLED:
            status = _RERANK_STATUS_FLAG_OFF
        elif rerank_shortlist_size < min_size:
            status = _RERANK_STATUS_BELOW_MIN
        else:
            status = _RERANK_STATUS_LLM_FAILED
        for c in candidates[:rerank_shortlist_size]:
            c["rerank_status"] = status
        return candidates

    # 4. Apply the rerank. Every decision's parcel_id appears exactly once
    #    in the shortlist (validator guarantees set equality + uniqueness).
    decisions_by_pid: dict[Any, dict[str, Any]] = {
        d["parcel_id"]: d for d in decisions
    }
    moved_count = 0
    max_delta_abs = 0
    for c in candidates[:rerank_shortlist_size]:
        pid = c.get("parcel_id") or c.get("id")
        decision = decisions_by_pid.get(pid)
        if decision is None:
            # Shouldn't happen post-validation, but preserve deterministic
            # order defensively rather than crash.
            c["rerank_status"] = _RERANK_STATUS_LLM_FAILED
            continue
        new_rank = int(decision["new_rank"])
        c["final_rank"] = new_rank
        delta = new_rank - c["deterministic_rank"]
        c["rerank_delta"] = delta
        if delta != 0:
            c["rerank_applied"] = True
            c["rerank_reason"] = decision.get("rerank_reason")
            c["rerank_status"] = _RERANK_STATUS_APPLIED
            moved_count += 1
            max_delta_abs = max(max_delta_abs, abs(delta))
        else:
            c["rerank_status"] = _RERANK_STATUS_UNCHANGED

    # 5. Reorder the candidate list by final_rank ascending. Candidates
    #    beyond the shortlist keep final_rank == deterministic_rank (their
    #    original position), so a stable sort by final_rank preserves their
    #    relative order and places them after the reranked shortlist.
    candidates.sort(key=lambda c: c.get("final_rank", 0))

    logger.info(
        "expansion_rerank applied: candidates=%d shortlist=%d moved=%d "
        "max_delta=%d",
        len(candidates),
        rerank_shortlist_size,
        moved_count,
        max_delta_abs,
    )

    return candidates


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _strip_platform_prefix(value: str | None) -> str | None:
    """Strip a ``<platform>:`` storage prefix from an id for display.

    ``commercial_unit.aqar_id`` carries a ``bayut:<id>`` prefix for Bayut
    rows so the PK stays unique across platforms. The prefix is internal
    storage hygiene and should not surface to API consumers. Returns the
    raw id with the first ``<lower-letters>:`` prefix removed; returns
    ``value`` unchanged if no such prefix is present.
    """
    if not value:
        return value
    m = re.match(r"^[a-z]+:(.+)$", value)
    return m.group(1) if m else value


def _context_checked(value: Any) -> bool:
    """
    Distinguish between:
    - None  => context unavailable / query failed / not computed
    - 0     => context available, but no nearby matches were found
    """
    return value is not None


def _nonnegative_int(value: Any) -> int:
    return max(0, _safe_int(value, 0))


def _derive_site_fit_context(feature_snapshot: dict[str, Any] | None) -> dict[str, Any]:
    """Derive site-fit context metadata from a candidate's feature snapshot.

    Returns score-mode flags so the frontend can distinguish observed
    measurements from fallback/estimated values.
    """
    if not feature_snapshot:
        return {
            "road_context_available": False,
            "parking_context_available": False,
            "frontage_score_mode": "estimated",
            "access_score_mode": "estimated",
            "parking_score_mode": "estimated",
        }
    cs = feature_snapshot.get("context_sources") or {}
    road_avail = bool(cs.get("road_context_available"))
    parking_avail = bool(cs.get("parking_context_available"))
    return {
        "road_context_available": road_avail,
        "parking_context_available": parking_avail,
        "frontage_score_mode": "observed" if road_avail else "estimated",
        "access_score_mode": "observed" if road_avail else "estimated",
        "parking_score_mode": "observed" if parking_avail else "estimated",
    }


def _normalize_gate_status(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_gate_reasons(value: Any, lang: str = "en") -> dict[str, Any]:
    base = {
        "passed": [],
        "failed": [],
        "unknown": [],
        "thresholds": {},
        "explanations": {},
    }
    if isinstance(value, dict):
        base["passed"] = _humanize_gate_list(value.get("passed") or [], lang)
        base["failed"] = _humanize_gate_list(value.get("failed") or [], lang)
        base["unknown"] = _humanize_gate_list(value.get("unknown") or [], lang)
        base["thresholds"] = value.get("thresholds") or {}
        base["explanations"] = value.get("explanations") or {}
    return base


def _normalize_feature_snapshot(value: Any) -> dict[str, Any]:
    raw = dict(value) if isinstance(value, dict) else {}
    raw["context_sources"] = raw.get("context_sources") or {}
    raw["missing_context"] = raw.get("missing_context") or []
    raw["data_completeness_score"] = _safe_int(raw.get("data_completeness_score"), 0)
    return raw


def _normalize_score_breakdown(value: Any, final_score: Any) -> dict[str, Any]:
    raw = dict(value) if isinstance(value, dict) else {}
    raw["weights"] = raw.get("weights") or {}
    raw["inputs"] = raw.get("inputs") or {}
    raw["weighted_components"] = raw.get("weighted_components") or {}
    raw["display"] = raw.get("display") or {}
    raw["final_score"] = _safe_float(raw.get("final_score"), _safe_float(final_score))
    # NOTE: economics_detail does not seed rent_burden defaults here. A
    # candidate that arrives with score_breakdown_json but without
    # economics_detail.rent_burden silently surfaces value_score=None even
    # when the upstream _economics_score did compute one. This shouldn't
    # happen in production paths (the compute path merges via the call site
    # in expansion_search), but it's a fragile contract. Tracked separately;
    # do not fix in this PR (per spec §9.5 / §10).
    raw["economics_detail"] = raw.get("economics_detail") or {}
    return raw


# ── PR #2b structured-record read path ──────────────────────────────
# These render the locale-invariant structured records (PR #2a) into
# the requested language. They always fall back to the English
# persisted column when the structured record is NULL (pre-PR-2a rows)
# or fails to render — so a missing/malformed structured record can
# never degrade the response below the English baseline (rule #4).
_STRUCTURED_CANDIDATE_COLS: tuple[str, ...] = (
    "top_positives_structured_json",
    "top_risks_structured_json",
    "decision_summary_structured_json",
    "demand_thesis_structured_json",
    "cost_thesis_structured_json",
    # PR #3 structured strengths/risks.
    "key_strengths_structured_json",
    "key_risks_structured_json",
)


def _render_structured_list(
    structured: Any,
    english_fallback: Any,
    lang: str,
) -> list[str]:
    """Render a list of structured records; fall back to the English
    persisted list on any failure (rule #4)."""
    from app.services.expansion_advisor_i18n import render

    if isinstance(structured, list) and structured:
        out: list[str] = []
        for rec in structured:
            rendered = render(rec, lang) if isinstance(rec, dict) else ""
            if not rendered:
                # Single-record failure → fall back to the whole
                # English list (safer than mixing rendered + raw).
                return english_fallback or []
            out.append(rendered)
        return out
    return english_fallback or []


def _render_structured_one(
    structured: Any,
    english_fallback: Any,
    lang: str,
) -> str:
    """Render a single structured record; fall back to the English
    persisted string on any failure (rule #4)."""
    from app.services.expansion_advisor_i18n import render

    if isinstance(structured, dict):
        rendered = render(structured, lang)
        return rendered or (english_fallback or "")
    return english_fallback or ""


def _normalize_candidate_payload(
    candidate: dict[str, Any],
    district_lookup: dict[str, dict[str, str]] | None = None,
    lang: str = "en",
) -> dict[str, Any]:
    payload = dict(candidate)

    # R-6 idempotency guard. If this payload was normalized before at a
    # different lang, the structured columns may already be dropped —
    # the only safe path is to stay in en (the persisted English
    # columns are still present and authoritative).
    _prior_lang = payload.pop("_eai_normalized_lang", None)
    if _prior_lang is not None and _prior_lang != lang:
        lang = "en"

    payload["gate_status_json"] = _normalize_gate_status(payload.get("gate_status_json"))
    payload["gate_reasons_json"] = _normalize_gate_reasons(payload.get("gate_reasons_json"), lang)
    payload["feature_snapshot_json"] = _normalize_feature_snapshot(payload.get("feature_snapshot_json"))
    payload["score_breakdown_json"] = _normalize_score_breakdown(payload.get("score_breakdown_json"), payload.get("final_score"))

    if lang == "ar":
        payload["top_positives_json"] = _render_structured_list(
            candidate.get("top_positives_structured_json"),
            payload.get("top_positives_json"), lang)
        payload["top_risks_json"] = _render_structured_list(
            candidate.get("top_risks_structured_json"),
            payload.get("top_risks_json"), lang)
        payload["decision_summary"] = _render_structured_one(
            candidate.get("decision_summary_structured_json"),
            payload.get("decision_summary"), lang)
        payload["demand_thesis"] = _render_structured_one(
            candidate.get("demand_thesis_structured_json"),
            payload.get("demand_thesis"), lang)
        payload["cost_thesis"] = _render_structured_one(
            candidate.get("cost_thesis_structured_json"),
            payload.get("cost_thesis"), lang)
        # PR #3: localize the strengths/risks string lists. These columns
        # feed the four secondary leak paths (candidate.key_strengths /
        # key_risks / main_watchout / main_risk) downstream. Guarded on
        # presence so compare_candidates — which passes neither column
        # (Q2 deferral) — is untouched. NULL structured columns (pre-PR-3
        # rows) fall back to the persisted English list (rule #4).
        if "key_strengths_json" in payload:
            payload["key_strengths_json"] = _render_structured_list(
                candidate.get("key_strengths_structured_json"),
                payload.get("key_strengths_json"), lang)
        if "key_risks_json" in payload:
            payload["key_risks_json"] = _render_structured_list(
                candidate.get("key_risks_structured_json"),
                payload.get("key_risks_json"), lang)
    else:
        # byte-identical to HEAD
        payload["top_positives_json"] = payload.get("top_positives_json") or []
        payload["top_risks_json"] = payload.get("top_risks_json") or []
        payload["decision_summary"] = payload.get("decision_summary") or ""
        payload["demand_thesis"] = payload.get("demand_thesis") or ""
        payload["cost_thesis"] = payload.get("cost_thesis") or ""

    payload["comparable_competitors_json"] = payload.get("comparable_competitors_json") or []
    payload["rank_position"] = payload.get("rank_position") or payload.get("compare_rank")
    payload["confidence_grade"] = payload.get("confidence_grade") or "D"

    # The five structured columns are internal — never part of the API
    # response shape. Drop them from the outgoing payload.
    for _col in _STRUCTURED_CANDIDATE_COLS:
        payload.pop(_col, None)

    # ── Commercial unit fields (pass through) ──
    payload["source_type"] = payload.get("source_type", "parcel")
    payload["commercial_unit_id"] = payload.get("commercial_unit_id")
    payload["listing_url"] = payload.get("listing_url")
    payload["image_url"] = payload.get("image_url")
    payload["unit_price_sar_annual"] = _safe_float(payload.get("unit_price_sar_annual")) if payload.get("unit_price_sar_annual") is not None else None
    payload["unit_area_sqm"] = _safe_float(payload.get("unit_area_sqm")) if payload.get("unit_area_sqm") is not None else None
    payload["unit_street_width_m"] = _safe_float(payload.get("unit_street_width_m")) if payload.get("unit_street_width_m") is not None else None
    payload["unit_neighborhood"] = payload.get("unit_neighborhood")
    payload["unit_listing_type"] = payload.get("unit_listing_type")

    # ── value_score / value_band (derived chip) ──
    # Read from score_breakdown_json["economics_detail"] which is the
    # single source of truth (persisted on the candidate row). Top-level
    # emission is for frontend convenience and OpenAPI discoverability.
    _ed = (payload.get("score_breakdown_json") or {}).get("economics_detail") or {}
    _vs = _ed.get("value_score") if isinstance(_ed, dict) else None
    payload["value_score"] = float(_vs) if isinstance(_vs, (int, float)) else None
    _vb = _ed.get("value_band") if isinstance(_ed, dict) else None
    payload["value_band"] = _vb if _vb in ("best_value", "neutral", "above_market") else None
    payload["value_band_low_confidence"] = bool(
        _ed.get("value_band_low_confidence") if isinstance(_ed, dict) else False
    )
    # Per-search value-band markers (legacy back-compat keys, written by
    # the score-delta accumulation step in run_expansion_search). Default
    # to False/0 so the response shape is stable. Persisted inside
    # score_breakdown_json["value_pass"] because expansion_candidate has
    # no dedicated columns for them; read from the nested location and
    # fall back to top-level (set in-memory during the pass before
    # persistence). Deprecated as of the score-delta refactor — readers
    # should consult score_breakdown_json["bonus_detail"] instead.
    _vp = (payload.get("score_breakdown_json") or {}).get("value_pass") or {}
    if not isinstance(_vp, dict):
        _vp = {}
    payload["value_downrank_applied"] = bool(
        payload.get("value_downrank_applied") or _vp.get("value_downrank_applied", False)
    )
    payload["value_downrank_delta"] = _safe_int(
        payload.get("value_downrank_delta") or _vp.get("value_downrank_delta"), 0
    )
    payload["value_uprank_applied"] = bool(
        payload.get("value_uprank_applied") or _vp.get("value_uprank_applied", False)
    )
    payload["value_uprank_delta"] = _safe_int(
        payload.get("value_uprank_delta") or _vp.get("value_uprank_delta"), 0
    )

    # ── Display-consistent annual rent (presentation only) ──
    # The UI rounds rent/m² to whole SAR for display.  Compute a matching
    # annual figure so the user never sees e.g. "2,000 SAR/m² → SAR 384,008".
    rent_per_m2 = _safe_float(payload.get("estimated_rent_sar_m2_year"))
    area = _safe_float(payload.get("area_m2"))
    if rent_per_m2 > 0 and area > 0:
        payload["display_annual_rent_sar"] = round(round(rent_per_m2) * area, 2)
    else:
        payload["display_annual_rent_sar"] = payload.get("estimated_annual_rent_sar")

    # ── Canonical district fields (additive) ──
    # Only compute if not already present (avoids re-computing on double-normalize).
    if "district_display" not in payload:
        canon = _canonicalize_district_label(payload.get("district"), district_lookup)
        payload["district_key"] = canon["district_key"]
        payload["district_name_ar"] = canon["district_name_ar"]
        payload["district_name_en"] = canon["district_name_en"]
        payload["district_display"] = canon["district_display"]

    return payload


def _normalize_search_payload(search: dict[str, Any] | None) -> dict[str, Any] | None:
    if search is None:
        return None
    payload = dict(search)
    payload["target_districts"] = payload.get("target_districts") or []
    payload["bbox"] = payload.get("bbox") if payload.get("bbox") is not None else None
    payload["request_json"] = payload.get("request_json") or {}
    payload["notes"] = payload.get("notes") or {}
    payload["existing_branches"] = payload.get("existing_branches") or []
    payload["brand_profile"] = payload.get("brand_profile") or {}
    meta = dict(payload.get("meta") or {})
    meta["version"] = _EXPANSION_VERSION
    meta["parcel_source"] = _EXPANSION_PARCEL_SOURCE
    meta["excluded_sources"] = list(_EXPANSION_EXCLUDED_SOURCES)
    payload["meta"] = meta
    return payload


def _normalize_saved_search_payload(
    saved: dict[str, Any] | None,
    *,
    search: dict[str, Any] | None = None,
    candidates: list[dict[str, Any]] | None = None,
    lang: str = "en",
) -> dict[str, Any] | None:
    if saved is None:
        return None
    payload = dict(saved)
    payload["selected_candidate_ids"] = payload.get("selected_candidate_ids") or []
    payload["filters_json"] = payload.get("filters_json") or {}
    payload["ui_state_json"] = payload.get("ui_state_json") or {}
    payload["description"] = payload.get("description")
    payload["search"] = _normalize_search_payload(search if search is not None else payload.get("search"))
    normalized_candidates = candidates if candidates is not None else payload.get("candidates")
    payload["candidates"] = [_normalize_candidate_payload(dict(item), lang=lang) for item in (normalized_candidates or [])]  # district_lookup=None is OK: additive fields filled from raw district

    search_payload = payload.get("search") or {}
    if search_payload.get("brand_profile"):
        payload["brand_profile"] = search_payload.get("brand_profile")
        filters_json = dict(payload.get("filters_json") or {})
        filters_json["brand_profile"] = search_payload.get("brand_profile")
        payload["filters_json"] = filters_json
    return payload


def _default_brand_profile(brand_profile: dict[str, Any] | None = None) -> dict[str, Any]:
    base = {
        "price_tier": None,
        "average_check_sar": None,
        "primary_channel": "balanced",
        "parking_sensitivity": "medium",
        "frontage_sensitivity": "medium",
        "visibility_sensitivity": "medium",
        "expansion_goal": "balanced",
        "brand_archetype": None,
        "cannibalization_tolerance_m": 1800.0,
        "preferred_districts": [],
        "excluded_districts": [],
    }
    if brand_profile:
        base.update({k: v for k, v in brand_profile.items() if v is not None})
    return base


# Brand archetypes — legible weight-profile presets decoupled from
# service_model (brand-brief redesign, investigation
# claude/investigate-brand-brief-redesign-av2ty8).
BRAND_ARCHETYPES = (
    "delivery_led",
    "street_flagship",
    "neighborhood_local",
    "balanced",
)

# Legacy expansion_goal values map onto archetypes at read time so old
# saved searches keep their intent. "balanced" is excluded from the
# resolution shortcut below because every persisted profile carries it as
# the _default_brand_profile fill — it is indistinguishable from "the user
# never touched the goal knob", so the service_model seed must win.
_LEGACY_GOAL_TO_ARCHETYPE = {
    "delivery_led": "delivery_led",
    "flagship": "street_flagship",
    "neighborhood": "neighborhood_local",
    "balanced": "balanced",
}

_SERVICE_MODEL_TO_ARCHETYPE = {
    "qsr": "balanced",
    "delivery_first": "delivery_led",
    "cafe": "neighborhood_local",
    "dine_in": "balanced",
}


def resolve_brand_archetype(
    brand_profile: dict[str, Any] | None,
    service_model: str | None,
) -> str:
    """Resolve the effective brand archetype for a search.

    Resolution order:
      1. explicit ``brand_archetype`` in the profile (user choice);
      2. legacy NON-default ``expansion_goal`` (old saved searches —
         "balanced" is skipped because it is the silent default fill);
      3. seeded from ``service_model`` per _SERVICE_MODEL_TO_ARCHETYPE,
         falling back to "balanced" for unknown models.

    Deterministic and pure; safe to call from both the API persistence
    path and the scoring path so the two always agree.
    """
    profile = brand_profile or {}
    explicit = str(profile.get("brand_archetype") or "").strip().lower()
    if explicit in BRAND_ARCHETYPES:
        return explicit
    goal = str(profile.get("expansion_goal") or "").strip().lower()
    if goal in _LEGACY_GOAL_TO_ARCHETYPE and goal != "balanced":
        return _LEGACY_GOAL_TO_ARCHETYPE[goal]
    return _SERVICE_MODEL_TO_ARCHETYPE.get(
        str(service_model or "").strip().lower(), "balanced"
    )


# One-time warning guard for the "archetype flag on, weight stack v1"
# misconfiguration: archetype profiles are defined on the v2 component set
# (district_momentum, no weighted confidence), so under v1 they are ignored.
# Mirrors the demand-generator log-once pattern above.
_ARCHETYPE_ON_V1_WARNED = False


def _archetype_profiles_active() -> bool:
    """True when archetype weight profiles should drive _score_breakdown:
    EXPANSION_ARCHETYPE_PROFILES on AND weight stack v2. Logs once (and
    returns False) when the flag is on under v1."""
    global _ARCHETYPE_ON_V1_WARNED
    if not bool(getattr(settings, "EXPANSION_ARCHETYPE_PROFILES", False)):
        return False
    if str(getattr(settings, "EXPANSION_WEIGHT_STACK", "v1")) != "v2":
        if not _ARCHETYPE_ON_V1_WARNED:
            _ARCHETYPE_ON_V1_WARNED = True
            logger.warning(
                "EXPANSION_ARCHETYPE_PROFILES is true but EXPANSION_WEIGHT_STACK "
                "is not v2; archetype weight profiles are defined on the v2 "
                "component set, so they are ignored under v1."
            )
        return False
    return True


def _sensitivity_weight(level: str | None) -> float:
    return {"low": 0.3, "medium": 0.6, "high": 1.0}.get(str(level or "medium"), 0.6)


def _channel_fit_score(service_model: str, primary_channel: str | None, provider_density_score: float, multi_platform_presence_score: float) -> float:
    channel = (primary_channel or "balanced").lower()
    if channel == "delivery":
        return _clamp(provider_density_score * 0.7 + multi_platform_presence_score * 0.3)
    if channel == "dine_in":
        dine_signal = 65.0 if service_model == "dine_in" else 50.0
        return _clamp(dine_signal + (100.0 - provider_density_score) * 0.2)
    return _clamp(55.0 + (multi_platform_presence_score - 50.0) * 0.2)


def _brand_fit_score(*, district: str | None, area_m2: float, demand_score: float, fit_score: float, cannibalization_score: float,
    provider_density_score: float, provider_whitespace_score: float, multi_platform_presence_score: float, delivery_competition_score: float,
    visibility_signal: float, parking_signal: float, brand_profile: dict[str, Any], service_model: str,
    target_area_m2: float | None = None) -> float:
    preferred = {normalize_district_key(d) for d in (brand_profile.get("preferred_districts") or []) if normalize_district_key(d)}
    excluded = {normalize_district_key(d) for d in (brand_profile.get("excluded_districts") or []) if normalize_district_key(d)}
    district_norm = normalize_district_key(district) if district else None
    district_component = 60.0
    if district_norm and district_norm in preferred:
        district_component = 88.0
    if district_norm and district_norm in excluded:
        district_component = 20.0

    tolerance = _safe_float(brand_profile.get("cannibalization_tolerance_m"), 1800.0)
    overlap_fit = _clamp(100.0 - abs(cannibalization_score - _clamp((2500.0 - tolerance) / 25.0, 0, 100)) * 0.8)

    if _archetype_profiles_active():
        # Archetype mode: expansion_goal is retired; the same three branches
        # below key off the resolved archetype instead of the goal knob.
        goal = {
            "street_flagship": "flagship",
            "neighborhood_local": "neighborhood",
            "delivery_led": "delivery_led",
        }.get(resolve_brand_archetype(brand_profile, service_model), "balanced")
    else:
        goal = (brand_profile.get("expansion_goal") or "balanced").lower()
    goal_component = 60.0
    if goal == "flagship":
        # Flagship goal rewards listings close to the operator's target
        # area, with visibility and demand as additional inputs. Falls
        # back to 350 m² when target_area_m2 is missing.
        _target = float(target_area_m2) if target_area_m2 and target_area_m2 > 0 else 350.0
        # Ratio-based area signal: full credit at target, taper for
        # significant deviation in either direction.
        _ratio = area_m2 / _target if _target > 0 else 1.0
        if 0.80 <= _ratio <= 1.20:
            _area_component = 100.0
        elif 0.60 <= _ratio <= 1.50:
            _area_component = 80.0
        elif 0.40 <= _ratio <= 2.00:
            _area_component = 55.0
        else:
            _area_component = 30.0
        goal_component = _clamp(_area_component * 0.6 + visibility_signal * 0.4 + demand_score * 0.2)
    elif goal == "neighborhood":
        spacing = 100.0 - abs(cannibalization_score - 45.0)
        goal_component = _clamp(fit_score * 0.45 + spacing * 0.25 + parking_signal * 0.3)
    elif goal == "delivery_led":
        goal_component = _clamp(provider_density_score * 0.35 + provider_whitespace_score * 0.35 + (100.0 - delivery_competition_score) * 0.3)
    else:
        goal_component = _clamp((demand_score + fit_score + provider_whitespace_score) / 3.0)

    channel_component = _channel_fit_score(
        service_model,
        brand_profile.get("primary_channel"),
        provider_density_score,
        multi_platform_presence_score,
    )
    parking_weight = _sensitivity_weight(brand_profile.get("parking_sensitivity"))
    frontage_weight = _sensitivity_weight(brand_profile.get("frontage_sensitivity"))
    visibility_weight = _sensitivity_weight(brand_profile.get("visibility_sensitivity"))

    price_tier = (brand_profile.get("price_tier") or "mid").lower()
    premium_penalty = 0.0
    if price_tier == "premium":
        premium_penalty = max(0.0, 65.0 - visibility_signal) * 0.35 + max(0.0, 60.0 - district_component) * 0.25

    return _clamp(
        district_component * 0.18
        + goal_component * 0.2
        + channel_component * 0.14
        + overlap_fit * 0.14
        + parking_signal * (0.1 + parking_weight * 0.06)
        + fit_score * (0.12 + frontage_weight * 0.03)
        + visibility_signal * (0.08 + visibility_weight * 0.05)
        + provider_whitespace_score * 0.08
        - premium_penalty
    )


def _arcgis_classification_semantics(
    landuse_code: str | int | None,
    landuse_label: str | None,
) -> dict[str, Any]:
    """Interpret ArcGIS numeric parcel classification codes.

    Returns structured metadata:
      normalized_class  – "commercial" | "mixed_use" | "residential" | "public_service" | "industrial" | "unknown"
      score             – 0-100 zoning fitness for restaurant expansion
      verdict_hint      – "pass" | "fail" | "unknown"
      source            – "arcgis_code" | "label_tokens" | "none"
    """
    # ── 1. Try numeric code first (resilient to str/int forms) ──
    code_int: int | None = None
    if landuse_code is not None:
        try:
            code_int = int(str(landuse_code).strip())
        except (ValueError, TypeError):
            pass

    _CODE_MAP: dict[int, tuple[str, int, str]] = {
        # code: (normalized_class, score, verdict_hint)
        2000: ("commercial", 100, "pass"),
        7500: ("mixed_use", 100, "pass"),
        1000: ("residential", 40, "unknown"),   # weak signal, NOT hard fail
        3000: ("public_service", 55, "unknown"),
        4000: ("industrial", 30, "fail"),   # industrial zones are not viable F&B retail locations
    }

    if code_int is not None and code_int in _CODE_MAP:
        cls, score, hint = _CODE_MAP[code_int]
        return {
            "normalized_class": cls,
            "score": score,
            "verdict_hint": hint,
            "source": "arcgis_code",
        }

    # ── 2. Label-token fallback ──
    raw = (landuse_label or "").strip().lower()
    if raw:
        if any(tok in raw for tok in ["industrial", "warehouse", "صناعي", "مستودع"]):
            return {"normalized_class": "industrial", "score": 30, "verdict_hint": "fail", "source": "label_tokens"}
        if any(tok in raw for tok in ["commercial", "retail", "تجاري"]):
            return {"normalized_class": "commercial", "score": 100, "verdict_hint": "pass", "source": "label_tokens"}
        if any(tok in raw for tok in ["mixed", "مختلط"]):
            return {"normalized_class": "mixed_use", "score": 100, "verdict_hint": "pass", "source": "label_tokens"}
        if any(tok in raw for tok in ["residential", "سكني"]):
            return {"normalized_class": "residential", "score": 40, "verdict_hint": "unknown", "source": "label_tokens"}
        # Label present but unrecognized – neutral
        return {"normalized_class": "unknown", "score": 60, "verdict_hint": "unknown", "source": "label_tokens"}

    # ── 3. No signal at all ──
    return {"normalized_class": "unknown", "score": 45, "verdict_hint": "unknown", "source": "none"}


def _landuse_fit(landuse_label: str | None, landuse_code: str | None) -> float:
    """Zoning fitness score (0-100) using ArcGIS classification semantics."""
    sem = _arcgis_classification_semantics(landuse_code, landuse_label)
    return float(sem["score"])


def _zoning_fit_score(landuse_label: str | None, landuse_code: str | None) -> float:
    return _clamp(_landuse_fit(landuse_label, landuse_code))


def _zoning_verdict(landuse_label: str | None, landuse_code: str | None) -> str:
    """Return tri-state verdict hint: 'pass' | 'fail' | 'unknown'."""
    sem = _arcgis_classification_semantics(landuse_code, landuse_label)
    return sem["verdict_hint"]


def _zoning_signal_class(landuse_label: str | None, landuse_code: str | None) -> str:
    """Return normalized ArcGIS class name."""
    sem = _arcgis_classification_semantics(landuse_code, landuse_label)
    return sem["normalized_class"]


def _zoning_signal_source(landuse_label: str | None, landuse_code: str | None) -> str:
    """Return the provenance of the zoning signal."""
    sem = _arcgis_classification_semantics(landuse_code, landuse_label)
    return sem["source"]


def _table_available(db: Session, table_name: str) -> bool:
    schema, _, table = table_name.partition(".")
    if not table:
        schema, table = "public", schema
    try:
        with db.begin_nested():
            row = db.execute(
                text(
                    """
                    SELECT EXISTS(
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = :schema
                          AND table_name = :table
                    ) AS available
                    """
                ),
                {"schema": schema, "table": table},
            ).mappings().first()
            return bool(row and row.get("available"))
    except Exception:
        logger.debug("_table_available check failed for %s", table_name, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Calibrated frontage/access score curves from measured Riyadh Aqar street
# widths (525-row real-store distribution):
#   <10 m: 4%   (poor frontage — side streets, alleys)
#   10-15 m: 1% (weak)
#   15-20 m: 3% (solid)
#   20-30 m: 5% (strong)
#   30-40 m: 20% (arterial-adjacent)
#   40+ m: 66%  (arterial / commercial spine)
#
# 95% of real listings have measured street_width_m — this is a direct
# ground-truth signal and should fully replace the parcel-based fallback
# when it is available.
# ---------------------------------------------------------------------------


def _frontage_score_from_street_width(street_width_m: float) -> float:
    if street_width_m < 8.0:
        return 25.0
    if street_width_m < 12.0:
        # Linear interpolate 25 → 42 across [8, 12)
        return 25.0 + (street_width_m - 8.0) / 4.0 * 17.0
    if street_width_m < 18.0:
        return 42.0 + (street_width_m - 12.0) / 6.0 * 16.0
    if street_width_m < 25.0:
        return 58.0 + (street_width_m - 18.0) / 7.0 * 14.0
    if street_width_m < 35.0:
        return 72.0 + (street_width_m - 25.0) / 10.0 * 10.0
    if street_width_m < 50.0:
        return 82.0 + (street_width_m - 35.0) / 15.0 * 8.0
    return 94.0


def _access_score_from_street_width(street_width_m: float) -> float:
    # Access slightly lower than frontage at extreme widths — very wide
    # arterials (60 m+) reduce pedestrian access even as they improve
    # drive-by visibility.
    if street_width_m < 8.0:
        return 30.0
    if street_width_m < 12.0:
        return 30.0 + (street_width_m - 8.0) / 4.0 * 15.0
    if street_width_m < 18.0:
        return 45.0 + (street_width_m - 12.0) / 6.0 * 13.0
    if street_width_m < 25.0:
        return 58.0 + (street_width_m - 18.0) / 7.0 * 12.0
    if street_width_m < 35.0:
        return 70.0 + (street_width_m - 25.0) / 10.0 * 8.0
    if street_width_m < 50.0:
        return 78.0 + (street_width_m - 35.0) / 15.0 * 7.0
    return 90.0


def _frontage_score(*, unit_street_width_m: float | None = None, parcel_perimeter_m: float, touches_road: bool, nearby_road_count: int, nearest_major_road_m: float | None,
    road_context_available: bool = True) -> float:
    # Listing-aware path: if we have a measured street width from Aqar,
    # use it directly.  This is ground truth for 95% of real stores and
    # should completely replace the parcel-based fallback.
    if unit_street_width_m is not None and unit_street_width_m > 0:
        return _frontage_score_from_street_width(float(unit_street_width_m))
    # Parcel-based path: existing logic, unchanged for parcel candidates.
    if not road_context_available:
        # No listing data, no parcel road context — explicit neutral-unknown.
        # Return 50 (not 55) so the gate evaluator correctly marks this as
        # "unknown" rather than "borderline pass".
        return 50.0
    perimeter_signal = _clamp((parcel_perimeter_m / 260.0) * 100.0)
    touch_signal = 100.0 if touches_road else 40.0
    density_signal = _clamp((nearby_road_count / 6.0) * 100.0)
    major_road_signal = _clamp(100.0 - (_safe_float(nearest_major_road_m, 300.0) / 300.0) * 100.0)
    return _clamp(perimeter_signal * 0.30 + touch_signal * 0.30 + density_signal * 0.20 + major_road_signal * 0.20)


def _access_score(*, unit_street_width_m: float | None = None, touches_road: bool, nearest_major_road_m: float | None, nearby_road_count: int, road_context_available: bool = True) -> float:
    # Listing-aware path: measured street width from Aqar.
    if unit_street_width_m is not None and unit_street_width_m > 0:
        return _access_score_from_street_width(float(unit_street_width_m))
    # Parcel-based path: existing logic, unchanged.
    if not road_context_available:
        return 50.0
    touch_signal = 100.0 if touches_road else 30.0
    major_signal = _clamp(100.0 - (_safe_float(nearest_major_road_m, 500.0) / 500.0) * 100.0)
    road_density = _clamp((nearby_road_count / 8.0) * 100.0)
    return _clamp(touch_signal * 0.40 + major_signal * 0.35 + road_density * 0.25)


def _foot_traffic_score(nearby_amenity_count: int) -> float:
    """Foot-traffic amenity proximity score for cafés.

    Counts schools, mosques, parks, and malls within 500m.
    More nearby amenities = more potential foot traffic for a café.

    Targets:
      0 amenities -> 30 (baseline — no nearby attractors)
      2            -> 50
      5            -> 70
      10+          -> 90 (cap — diminishing returns)
    """
    if nearby_amenity_count <= 0:
        return 30.0
    # Log-scaled: steep gains for first few amenities, diminishing after
    raw = 30.0 + 60.0 * (math.log1p(nearby_amenity_count) / math.log1p(12))
    return min(90.0, max(30.0, raw))


# ── L1 modeled demand-generator index (PR-1, emit-only) ─────────────────────
# Per-candidate demand NUMERATOR built entirely from data we already own:
# catchment population + OSM trip generators + Overture building floor-density +
# a free review_count-weighted F&B-density term (the zero-cost stand-in for
# BestTime busyness — it captures "venues people actually visit"; its only real
# gap vs a paid busyness feed is temporal shape). For dine_in this replaces the
# effectively-absent foot_traffic signal (_foot_traffic_score is a cafe-only
# nudge today). PR-1 only EMITS it into feature_snapshot_json; nothing here is
# read by scoring (that is PR-2).
#
# NET-OF-SUPPLY DISCIPLINE: this index is a demand numerator only. Competition /
# POI density stays in _competition_whitespace_score as the denominator and is
# NOT subtracted here. The F&B review-weighted term correlates with competitor
# count by construction (busy venues ⇒ more nearby F&B); that correlation is
# expected and is exactly why the denominator must stay separate — the index
# must not double as a saturation signal.
#
# PR-1a RECALIBRATION (l1_v2_2026-06): the v1 index pinned at the ceiling — every
# normalization reference sat far below real Riyadh catchment values, so all four
# sub-scores saturated and the composite barely varied (stddev 1.92, 5 distinct
# values; competitor_count 60 scored the same as competitor_count 270). The fix is
# pure calibration: re-anchor every sub-signal to the REAL city-wide distribution
# (winsorize at p99, map p5→p95 onto 0-100, log-transform the wide-spread signals),
# compute the population sub-term at a tighter radius where it actually varies, and
# rebalance the composite onto the discriminating signals. No new sub-signals, no
# new data sources, still emit-only.
_DEMAND_GENERATOR_WEIGHTS_VERSION = "l1_v2_2026-06"

# l1_v3 (QSR re-anchor): the dine-in l1_v2 anchors above were gathered at the
# 3500 m demand radius. At QSR's tighter 1500 m demand radius the raw counts are
# genuinely smaller (QSR fnb p95 ~72k vs dine-in 225k; floors p95 ~6.9k vs 35.6k),
# so reusing l1_v2 would map nearly every QSR candidate near 0. l1_v3 re-anchors
# the SAME four sub-signals (and the SAME 0.40/0.35/0.20/0.05 mix) to the QSR
# 1500 m distribution — a clean re-anchor, NOT a re-mix. QSR-keyed; dine-in keeps
# l1_v2 untouched.
_DEMAND_GENERATOR_WEIGHTS_VERSION_QSR = "l1_v3_qsr_2026-06"

# Per-generator-type weights for the OSM sub-score. Seeded from the heatmap
# path's _ANCHOR_WEIGHTS (restaurant_scoring_factors.py:772-784), regrouped onto
# the seven generator buckets enriched here; transit/mosque buckets (absent from
# the anchor table) take conservative mid/low values. Combined as Σ(count·weight)
# then normalized against the city-wide weighted-total anchors below.
_DEMAND_GENERATOR_OSM_WEIGHTS: dict[str, float] = {
    "offices": 2.0,
    "malls_retail": 4.0,
    "transit": 2.0,
    "mosques": 1.5,
    "schools": 1.75,
    "hospitals": 2.0,
    "hotels": 2.5,
}

# ── PR-1a NORMALIZATION ANCHORS (one clearly-marked, versioned block) ──────────
# Each raw sub-signal is winsorized at p99 then mapped p5→p95 onto 0-100. Tuple is
# (p5, p95, p99, log?). Wide-spread signals (F&B reviews, building floors, the OSM
# weighted total) are log-transformed; the tighter-radius population is mapped
# linearly. Mapping the top anchor to p95 (not the max) leaves headroom so dense
# areas land ~80-90 instead of all pegging at 100.
#
# Set from the Phase A probe (scripts/diagnostics/l1_signal_distributions.sql,
# 538 city-wide Tier-1 primary candidates). The OSM anchors are on the SAME
# Σ(count·weight) the probe reports, so they drop in directly.
_DEMAND_GENERATOR_NORM_ANCHORS: dict[str, tuple[float, float, float, bool]] = {
    #  signal                 p5         p95         p99        log
    "fnb_review_weighted": (4210.0, 224576.0, 241965.0, True),
    "building_floors":     (6805.0,  35612.0,  40102.0, True),
    "osm_weighted_total":  (3.4,      3351.0,   3943.0, True),
    # Population at the tighter EXPANSION_DEMAND_GENERATOR_POP_RADIUS_M (~1500 m).
    "population_local":    (8281.0,   52010.0,  53393.0, False),
}

# l1_v3 QSR anchors — gathered at QSR's 1500 m demand radius (Phase-A probe, 548
# city-wide candidates). Same tuple shape (p5, p95, p99, log?) and same transforms
# as l1_v2; only the anchor values differ, fit to the 1500 m counts.
#   - fnb_review_weighted: n_zero 26/548 (~5%), healthy spread (keeps 0.35).
#   - building_floors:     n_zero 0, clean (keeps 0.20).
#   - osm_weighted_total:  n_zero 45 (~8%), discriminating (keeps 0.40). KNOWN
#       LIMITATION: p95==p99==951.8 is a winsorization plateau, so the highest-OSM
#       QSR candidates won't separate from EACH OTHER at the top; the mid band
#       (p25=15 -> p75=657) still discriminates. Acceptable for v3, revisit later.
#       osm p5: the probe p5 was 0.0; a log anchor needs a positive floor, so we
#       use p5=3.4 (matching l1_v2's positive osm floor). _demand_generator_normalize
#       also clamps a 0 p5 safely (log1p(0)=0, and hi>lo still holds), but 3.4 keeps
#       the low tail from pegging at exactly 0.
#   - population_local:    spread ratio ~1.1 at 1000/1200/1500 m (flat everywhere),
#       so it keeps only the token 0.05 weight; anchors ~= l1_v2 (pop barely differs
#       at 1500 m vs the l1_v2 pop sub-radius, which is also 1500 m).
_DEMAND_GENERATOR_NORM_ANCHORS_QSR: dict[str, tuple[float, float, float, bool]] = {
    #  signal                 p5         p95         p99       log
    "fnb_review_weighted": (186.5,   72026.0, 94509.0, True),
    "building_floors":     (1262.9,   6898.0,  9483.7, True),
    "osm_weighted_total":  (3.4,       951.8,   951.8, True),   # see osm caveat above
    "population_local":    (7410.4,  51979.7, 53392.6, False),  # ~= l1_v2 at 1500 m
}


def _demand_generator_anchors(
    service_model: str | None,
) -> dict[str, tuple[float, float, float, bool]]:
    """Select the L1 normalization anchor set by service model.

    qsr -> l1_v3 (gathered at QSR's 1500 m demand radius); every other model ->
    l1_v2 (the dine-in anchors at 3500 m, UNCHANGED). Defaulting non-qsr to l1_v2
    keeps dine_in / cafe / delivery_first byte-for-byte on the existing anchors.
    """
    if (service_model or "").lower() == "qsr":
        return _DEMAND_GENERATOR_NORM_ANCHORS_QSR
    return _DEMAND_GENERATOR_NORM_ANCHORS


# Top-level composite weights over the four normalized sub-signals (sum 1.0).
# PR-1a rebalance: the discriminating signals (OSM trip generators + free F&B
# review density) drive the spread. Phase A showed the 1500 m population radius
# does NOT discriminate (spread_ratio_1500=1.108 ≈ spread_ratio_3500=1.109), so
# population is cut to a token 0.05 and its weight redistributed to OSM + F&B.
_DEMAND_GENERATOR_COMPOSITE_WEIGHTS: dict[str, float] = {
    "osm_generators": 0.40,
    "fnb_review_weighted": 0.35,
    "building_floors": 0.20,
    "population": 0.05,
}


def _demand_generator_normalize(
    signal: str, value: float, *, service_model: str | None = None
) -> float:
    """Winsorize at p99 then map the p5→p95 anchor band onto 0-100.

    Robust 0-100 normalization shared by every L1 sub-signal: a single busy
    outlier cannot dominate (winsorized at p99) and the dense end keeps headroom
    (top anchor is p95, not the max). Wide-spread signals are log-transformed so
    the bulk of the distribution spreads instead of bunching near zero. Anchors
    are selected per service model (qsr -> l1_v3 at 1500 m, else l1_v2 at 3500 m);
    ``service_model=None`` keeps the l1_v2 default so existing callers are
    unchanged.
    """
    p5, p95, p99, use_log = _demand_generator_anchors(service_model)[signal]
    v = min(max(0.0, _safe_float(value)), p99)  # winsorize the top tail at p99
    if use_log:
        v = math.log1p(v)
        lo = math.log1p(p5)
        hi = math.log1p(p95)
    else:
        lo, hi = p5, p95
    if hi <= lo:
        return 0.0
    return _clamp((v - lo) / (hi - lo) * 100.0)


def _demand_generator_osm_subscore(
    osm_counts: dict[str, int], *, service_model: str | None = None
) -> float:
    """Σ(count·weight) over generator buckets → 0-100 against the city-wide anchor.

    PR-1a: the v1 sigmoid (ref /20) saturated to ~95 for every candidate because
    real Riyadh catchments hold hundreds of offices. The weighted total is now
    log-normalized against the empirical p5→p95 band so offices/retail — the
    strongest raw discriminators — actually drive spread. The OSM bucket weights
    are unchanged; only the normalization anchor band is service-model-aware."""
    weighted_total = 0.0
    for _kind, _w in _DEMAND_GENERATOR_OSM_WEIGHTS.items():
        weighted_total += _w * float(osm_counts.get(_kind, 0) or 0)
    return _demand_generator_normalize(
        "osm_weighted_total", weighted_total, service_model=service_model
    )


# PR-2: one-time warning guard for the "scoring flag on, index flag off"
# misconfiguration. We cannot score a composite that was never computed, so we
# fall back to pop_score and log exactly once per process to avoid log spam in
# the per-candidate scoring loop.
_DG_SCORING_WITHOUT_INDEX_WARNED = False


def _warn_dg_scoring_without_index() -> None:
    global _DG_SCORING_WITHOUT_INDEX_WARNED
    if not _DG_SCORING_WITHOUT_INDEX_WARNED:
        _DG_SCORING_WITHOUT_INDEX_WARNED = True
        logger.warning(
            "EXPANSION_DEMAND_GENERATOR_SCORING_ENABLED is true but "
            "EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED is false; the demand-"
            "generator composite is not computed, so dine-in demand scoring "
            "falls back to pop_score. Enable the index flag to activate PR-2."
        )


# QSR analogue of the guard above: the QSR scoring flag is separate from the
# dine-in one, so it gets its own one-time warning when it is on without the
# index flag (mirrors the dine-in misconfiguration path; QSR falls back to
# pop_score).
_DG_SCORING_QSR_WITHOUT_INDEX_WARNED = False


def _warn_dg_scoring_qsr_without_index() -> None:
    global _DG_SCORING_QSR_WITHOUT_INDEX_WARNED
    if not _DG_SCORING_QSR_WITHOUT_INDEX_WARNED:
        _DG_SCORING_QSR_WITHOUT_INDEX_WARNED = True
        logger.warning(
            "EXPANSION_DEMAND_GENERATOR_SCORING_QSR_ENABLED is true but "
            "EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED is false; the demand-"
            "generator composite is not computed, so QSR demand scoring falls "
            "back to pop_score. Enable the index flag to activate QSR scoring."
        )


def _demand_generator_index(
    *,
    population_reach: float,
    osm_counts: dict[str, int],
    building_floors_proxy_sum: float,
    fnb_review_weighted: float,
    fnb_venue_count: int,
    radius_m: int,
    population_local_reach: float | None = None,
    pop_radius_m: int | None = None,
    service_model: str | None = None,
) -> dict[str, Any]:
    """Compose the emit-only L1 demand-generator index for one candidate.

    PR-1a: each sub-signal is normalized to 0-100 against the real city-wide
    distribution (winsorize at p99, map p5→p95, log-transform wide signals) via
    _demand_generator_normalize, then combined with the top-level composite
    weights. ALL raw sub-values are retained in the returned dict — including BOTH
    the full ``population_reach`` (radius_m) and the tighter ``population_local_reach``
    (pop_radius_m) used for the sub-score — so the next calibration never needs a
    re-enrich.

    The population sub-score uses ``population_local_reach`` at the tighter
    pop_radius_m (default 1500 m): at 3.5 km every dense-Riyadh catchment holds
    ~250k people, so the wide-radius value barely discriminates by construction.
    When the tighter value is not supplied the wide ``population_reach`` is used as
    a fallback so the function stays self-contained.

    NOTE on review_count staleness: Google review enrichment is currently
    disabled, so review_count is a stale snapshot. That is acceptable here — the
    term is used only for RELATIVE cross-candidate ranking, not as a live count.
    """
    # Population: normalize the tighter-radius reach (falls back to the wide one).
    _pop_local = (
        _safe_float(population_local_reach)
        if population_local_reach is not None
        else _safe_float(population_reach)
    )
    pop_sub = _demand_generator_normalize(
        "population_local", _pop_local, service_model=service_model
    )
    # OSM generators: log-normalized weighted count.
    osm_sub = _demand_generator_osm_subscore(osm_counts, service_model=service_model)
    # Building floor-density daytime proxy (log-normalized).
    _floors = max(0.0, _safe_float(building_floors_proxy_sum))
    floors_sub = _demand_generator_normalize(
        "building_floors", _floors, service_model=service_model
    )
    # Free F&B review-weighted density (log-normalized).
    _rw = max(0.0, _safe_float(fnb_review_weighted))
    fnb_sub = _demand_generator_normalize(
        "fnb_review_weighted", _rw, service_model=service_model
    )

    w = _DEMAND_GENERATOR_COMPOSITE_WEIGHTS
    composite = _clamp(
        pop_sub * w["population"]
        + fnb_sub * w["fnb_review_weighted"]
        + osm_sub * w["osm_generators"]
        + floors_sub * w["building_floors"]
    )
    return {
        "composite_0_100": round(composite, 2),
        "weights_version": (
            _DEMAND_GENERATOR_WEIGHTS_VERSION_QSR
            if (service_model or "").lower() == "qsr"
            else _DEMAND_GENERATOR_WEIGHTS_VERSION
        ),
        "radius_m": int(radius_m),
        "population_reach": int(round(_safe_float(population_reach))),
        "pop_radius_m": int(pop_radius_m) if pop_radius_m is not None else int(radius_m),
        "population_local_reach": int(round(_pop_local)),
        "osm_generators": {
            "offices": int(osm_counts.get("offices", 0) or 0),
            "malls_retail": int(osm_counts.get("malls_retail", 0) or 0),
            "transit": int(osm_counts.get("transit", 0) or 0),
            "mosques": int(osm_counts.get("mosques", 0) or 0),
            "schools": int(osm_counts.get("schools", 0) or 0),
            "hospitals": int(osm_counts.get("hospitals", 0) or 0),
            "hotels": int(osm_counts.get("hotels", 0) or 0),
        },
        "building_floors_proxy_sum": round(_floors, 2),
        "fnb_review_weighted_density": round(_rw, 2),
        "fnb_venue_count": int(fnb_venue_count or 0),
        # Derived 0-100 sub-scores retained for transparency / PR-2 calibration.
        "subscores": {
            "population": round(pop_sub, 2),
            "osm_generators": round(osm_sub, 2),
            "building_floors": round(floors_sub, 2),
            "fnb_review_weighted": round(fnb_sub, 2),
        },
    }


def _parking_score(*, area_m2: float, service_model: str, nearby_parking_count: int, access_score: float, parking_context_available: bool = True) -> float:
    area_signal = _clamp((area_m2 / 300.0) * 100.0)
    if not parking_context_available:
        return _clamp(area_signal * 0.50 + access_score * 0.20 + 30.0)
    parking_amenity_signal = _clamp((nearby_parking_count / 6.0) * 100.0)
    model_adjustment = {
        "delivery_first": 80.0,
        "qsr": 70.0,
        "cafe": 62.0,
        "dine_in": 55.0,
    }.get(service_model, 65.0)
    return _clamp(area_signal * 0.35 + parking_amenity_signal * 0.30 + model_adjustment * 0.20 + access_score * 0.15)


def _parking_evidence_band(nearby_parking_count: int | None) -> str:
    """
    Lightweight debug/helper field for UI + memo rendering.
    Helps distinguish 'none found' from 'strong parking supply'.
    """
    if nearby_parking_count is None:
        return "unknown"
    count = _nonnegative_int(nearby_parking_count)
    if count == 0:
        return "none_found"
    if count <= 2:
        return "limited"
    if count <= 5:
        return "moderate"
    return "strong"


def _road_evidence_band(nearby_road_count: int | None, touches_road: bool | None) -> str:
    if nearby_road_count is None and touches_road is None:
        return "unknown"
    roads = _nonnegative_int(nearby_road_count)
    if touches_road:
        return "direct_frontage"
    if roads == 0:
        return "none_found"
    if roads <= 2:
        return "limited"
    if roads <= 5:
        return "moderate"
    return "strong"


def _road_evidence_band_from_street_width(street_width_m: float | None) -> str | None:
    """Listing-aware road evidence band derived from Aqar's measured
    street width. Returns None when input is non-positive so the caller
    can fall back to the OSM-derived band.

    Width breakpoints chosen for Riyadh streets:
      - >= 12 m: direct frontage on a wide road
      - >=  8 m: moderate frontage
      -  > 0 m: limited frontage (narrow side street)
    """
    if street_width_m is None or street_width_m <= 0:
        return None
    if street_width_m >= 12.0:
        return "direct_frontage"
    if street_width_m >= 8.0:
        return "moderate"
    return "limited"


def _parking_evidence_band_for_listing(
    *,
    parking_context_available: bool,
    nearby_parking_amenity_count: int | None,
    parking_score: float | None,
) -> str | None:
    """Listing-aware parking evidence band.

    Returns None when the listing path has no basis for overriding the
    OSM-derived band (caller falls through to existing behavior).

    Rules:
      - parking_context_available is False: we never authoritatively
        looked → return "unknown". Honest about absent data; avoids the
        "None found" claim when we genuinely don't know.
      - parking_context_available is True AND count > 0: do NOT override;
        let the existing OSM-count-based band stand.
      - parking_context_available is True AND count == 0 AND
        parking_score >= 55: the v2 parking scorer found a signal the
        OSM amenity count missed → "limited" (conservative; we do have
        SOME evidence but the OSM tag layer is thin here).
      - All other cases: return None (no override).
    """
    if not parking_context_available:
        return "unknown"
    if (nearby_parking_amenity_count or 0) > 0:
        return None
    if parking_score is not None and parking_score >= 55.0:
        return "limited"
    return None


def _access_visibility_score(*, frontage_score: float, access_score: float, brand_profile: dict[str, Any]) -> float:
    """Blend frontage and access into a pure site measurement on 0-100.

    Brand sensitivity preference is expressed in the WEIGHT domain only
    (``_brand_weight_multipliers`` lifts/trims the access_visibility
    top-level weight from the site-sensitivity knobs). The raw score here
    is deliberately NOT scaled by visibility_sensitivity — a previous
    ``· (0.75 + visibility_weight·0.25)`` multiplier capped medium-
    sensitivity brands at 90 and all-low brands at 82.5, compressing the
    component's spread and skewing cross-brand comparability (weight-audit
    Item 4b). frontage_sensitivity still steers the frontage/access blend.
    """
    frontage_weight = _sensitivity_weight(brand_profile.get("frontage_sensitivity"))
    blend = 0.5 + frontage_weight * 0.2
    access_blend = 1.0 - blend
    return _clamp(frontage_score * blend + access_score * access_blend)


def _ea_table_has_rows(db: Session, table_name: str) -> bool:
    """Check if an Expansion Advisor normalized table exists and has rows."""
    try:
        with db.begin_nested():
            row = db.execute(
                text(f"SELECT EXISTS(SELECT 1 FROM {table_name} LIMIT 1) AS has_rows")
            ).scalar()
            return bool(row)
    except Exception:
        return False


def _candidate_feature_snapshot(db: Session, *, parcel_id: str, lat: float, lon: float, area_m2: float, district: str | None,
    landuse_label: str | None, landuse_code: str | None, provider_listing_count: int, provider_platform_count: int,
    competitor_count: int, nearest_branch_distance_m: float | None, rent_source: str, estimated_rent_sar_m2_year: float,
    economics_score: float, roads_table_available: bool, parking_table_available: bool,
    ea_roads_available: bool | None = None, ea_parking_available: bool | None = None,
    bulk_perimeter: float | None = None, bulk_roads: dict[str, Any] | None = None,
    bulk_parking: int | None = None) -> dict[str, Any]:
    base = {
        "parcel_area_m2": round(_safe_float(area_m2), 2),
        "parcel_perimeter_m": None,
        "district": district,
        "landuse_label": landuse_label,
        "landuse_code": landuse_code,
        "nearest_major_road_distance_m": None,
        "nearby_road_segment_count": 0,
        "touches_road": False,
        "nearby_parking_amenity_count": 0,
        "provider_listing_count": provider_listing_count,
        "provider_platform_count": provider_platform_count,
        "competitor_count": competitor_count,
        "nearest_branch_distance_m": round(_safe_float(nearest_branch_distance_m), 2) if nearest_branch_distance_m is not None else None,
        "rent_source": rent_source,
        "estimated_rent_sar_m2_year": round(_safe_float(estimated_rent_sar_m2_year), 2),
        "economics_score": round(_safe_float(economics_score), 2),
        "context_sources": {
            "roads_table_available": False,
            "parking_table_available": False,
            "road_context_available": False,
            "parking_context_available": False,
        },
        "missing_context": [],
        "data_completeness_score": 0,
    }

    zoning_context_available = bool(str(landuse_label or "").strip() or str(landuse_code or "").strip())
    delivery_observed = provider_listing_count > 0 or provider_platform_count > 0
    base["context_sources"]["zoning_context_available"] = zoning_context_available
    base["context_sources"]["delivery_observed"] = delivery_observed

    base["context_sources"]["roads_table_available"] = roads_table_available
    base["context_sources"]["parking_table_available"] = parking_table_available

    # Track data source provenance for observability
    base["context_sources"]["road_source"] = "estimated"
    base["context_sources"]["parking_source"] = "estimated"
    base["context_sources"]["delivery_source"] = "legacy"
    base["context_sources"]["rent_source"] = rent_source
    base["context_sources"]["competitor_source"] = "legacy"

    # Use pre-computed values when available, otherwise check with cache
    if ea_roads_available is None:
        ea_roads_available = _cached_ea_table_has_rows(db, _EA_ROADS_TABLE)
    if ea_parking_available is None:
        ea_parking_available = _cached_ea_table_has_rows(db, _EA_PARKING_TABLE)

    if ea_roads_available:
        base["context_sources"]["road_source"] = "expansion_road_context"
        roads_table_available = True
        base["context_sources"]["roads_table_available"] = True
    if ea_parking_available:
        base["context_sources"]["parking_source"] = "expansion_parking_asset"
        parking_table_available = True
        base["context_sources"]["parking_table_available"] = True

    if not parcel_id:
        base["missing_context"] = ["missing_parcel_id"]
        base["data_completeness_score"] = 50
        return base
    if bulk_perimeter is not None:
        base["parcel_perimeter_m"] = bulk_perimeter
    else:
        try:
            with db.begin_nested():
                perimeter_row = db.execute(
                    text(
                        f"""
                        SELECT COALESCE(ST_Perimeter(p.geom::geography), 0) AS parcel_perimeter_m
                        FROM {ARCGIS_PARCELS_TABLE} p
                        WHERE p.id::text = :parcel_id
                        LIMIT 1
                        """
                    ),
                    {"parcel_id": str(parcel_id)},
                ).mappings().first()
                if perimeter_row:
                    base["parcel_perimeter_m"] = round(_safe_float(perimeter_row.get("parcel_perimeter_m")), 2)
        except Exception:
            logger.debug("perimeter query failed for parcel_id=%s", parcel_id, exc_info=True)

    # ── Road context: prefer expansion_road_context when populated ──
    _road_data_resolved = False
    if bulk_roads is not None:
        base.update({
            "nearest_major_road_distance_m": bulk_roads["nearest_major_road_distance_m"],
            "nearby_road_segment_count": bulk_roads["nearby_road_segment_count"],
            "touches_road": bulk_roads["touches_road"],
        })
        base["context_sources"]["road_context_available"] = True
        base["context_sources"]["road_source"] = bulk_roads.get("source", "estimated")
        if bulk_roads.get("source") == "expansion_road_context":
            base["context_sources"]["roads_table_available"] = True
        _road_data_resolved = True

    if ea_roads_available and roads_table_available and not _road_data_resolved:
        try:
            with db.begin_nested():
                ea_road_row = db.execute(
                    text(f"""
                        WITH p AS (
                            SELECT geom
                            FROM {ARCGIS_PARCELS_TABLE}
                            WHERE id::text = :parcel_id
                            LIMIT 1
                        )
                        SELECT
                            COALESCE(
                                (SELECT MIN(major_road_distance_m) FROM {_EA_ROADS_TABLE} erc
                                 WHERE erc.is_major_road = TRUE
                                   AND erc.geom IS NOT NULL
                                   AND ST_DWithin(erc.geom::geography, p.geom::geography, 700)),
                                (SELECT MIN(ST_Distance(erc.geom::geography, p.geom::geography))
                                 FROM {_EA_ROADS_TABLE} erc
                                 WHERE erc.is_major_road = TRUE
                                   AND erc.geom IS NOT NULL
                                   AND ST_DWithin(erc.geom::geography, p.geom::geography, 700)),
                                {_ROAD_DISTANCE_SENTINEL_M}
                            ) AS nearest_major_road_distance_m,
                            COALESCE((
                                SELECT COUNT(*)
                                FROM {_EA_ROADS_TABLE} erc
                                WHERE erc.geom IS NOT NULL
                                  AND ST_DWithin(erc.geom::geography, ST_Centroid(p.geom)::geography, 250)
                            ), 0) AS nearby_road_segment_count,
                            EXISTS(
                                SELECT 1 FROM {_EA_ROADS_TABLE} erc
                                WHERE erc.geom IS NOT NULL
                                  AND ST_DWithin(erc.geom::geography, p.geom::geography, 18)
                            ) AS touches_road
                        FROM p
                    """),
                    {"parcel_id": str(parcel_id)},
                ).mappings().first()
                if ea_road_row:
                    base.update({
                        "nearest_major_road_distance_m": round(_safe_float(ea_road_row.get("nearest_major_road_distance_m")), 2),
                        "nearby_road_segment_count": _safe_int(ea_road_row.get("nearby_road_segment_count")),
                        "touches_road": bool(ea_road_row.get("touches_road")),
                    })
                    base["context_sources"]["road_context_available"] = True
                    _road_data_resolved = True
        except Exception:
            logger.debug("expansion_road_context query failed for parcel_id=%s, falling back to OSM", parcel_id, exc_info=True)

    if roads_table_available and not _road_data_resolved:
        try:
            with db.begin_nested():
                road_row = db.execute(
                    text(
                        f"""
                        WITH p AS (
                            SELECT geom
                            FROM {ARCGIS_PARCELS_TABLE}
                            WHERE id::text = :parcel_id
                            LIMIT 1
                        )
                        SELECT
                            COALESCE((
                                SELECT MIN(ST_Distance(l.way::geography, p.geom::geography))
                                FROM planet_osm_line l
                                WHERE l.way IS NOT NULL
                                  AND (l.highway IS NOT NULL OR NULLIF(l.name, '') IS NOT NULL)
                                  AND ST_DWithin(l.way::geography, p.geom::geography, 700)
                                  AND (
                                    l.highway IN ('motorway','trunk','primary','secondary')
                                    OR NULLIF(l.name, '') IS NOT NULL
                                  )
                            ), {_ROAD_DISTANCE_SENTINEL_M}) AS nearest_major_road_distance_m,
                            COALESCE((
                                SELECT COUNT(*)
                                FROM planet_osm_line l
                                WHERE l.way IS NOT NULL
                                  AND l.highway IS NOT NULL
                                  AND ST_DWithin(l.way::geography, ST_Centroid(p.geom)::geography, 250)
                            ), 0) AS nearby_road_segment_count,
                            EXISTS(
                                SELECT 1
                                FROM planet_osm_line l
                                WHERE l.way IS NOT NULL
                                  AND l.highway IS NOT NULL
                                  AND ST_DWithin(l.way::geography, p.geom::geography, 18)
                            ) AS touches_road
                        FROM p
                        """
                    ),
                    {"parcel_id": str(parcel_id)},
                ).mappings().first()
                if road_row:
                    nearby_road_segment_count = _safe_int(road_row.get("nearby_road_segment_count"))
                    touches_road = bool(road_row.get("touches_road"))
                    nearest_major_road_distance_m = _safe_float(road_row.get("nearest_major_road_distance_m"))
                    base.update(
                        {
                            "nearest_major_road_distance_m": round(nearest_major_road_distance_m, 2),
                            "nearby_road_segment_count": nearby_road_segment_count,
                            "touches_road": touches_road,
                        }
                    )
                    # Context is available when the query succeeded and returned
                    # data — even if every count is 0 (meaning "no nearby roads
                    # found").  The old heuristic conflated 0 with unavailable.
                    base["context_sources"]["road_context_available"] = (
                        _context_checked(road_row.get("nearby_road_segment_count"))
                        or _context_checked(road_row.get("touches_road"))
                        or _context_checked(road_row.get("nearest_major_road_distance_m"))
                    )
        except Exception:
            logger.debug("road context query failed for parcel_id=%s", parcel_id, exc_info=True)

    # ── Parking context: prefer expansion_parking_asset when populated ──
    _parking_data_resolved = False
    if bulk_parking is not None:
        base["nearby_parking_amenity_count"] = bulk_parking
        base["context_sources"]["parking_context_available"] = True
        if ea_parking_available:
            base["context_sources"]["parking_source"] = "expansion_parking_asset"
        _parking_data_resolved = True

    if ea_parking_available and parking_table_available and not _parking_data_resolved:
        try:
            with db.begin_nested():
                ea_parking_row = db.execute(
                    text(f"""
                        WITH p AS (
                            SELECT geom
                            FROM {ARCGIS_PARCELS_TABLE}
                            WHERE id::text = :parcel_id
                            LIMIT 1
                        )
                        SELECT COALESCE((
                            SELECT COUNT(*)
                            FROM {_EA_PARKING_TABLE} epa
                            WHERE epa.geom IS NOT NULL
                              AND ST_DWithin(epa.geom::geography, ST_Centroid(p.geom)::geography, 350)
                        ), 0) AS nearby_parking_amenity_count
                        FROM p
                    """),
                    {"parcel_id": str(parcel_id)},
                ).mappings().first()
                if ea_parking_row:
                    base["nearby_parking_amenity_count"] = _safe_int(ea_parking_row.get("nearby_parking_amenity_count"))
                    base["context_sources"]["parking_context_available"] = True
                    _parking_data_resolved = True
        except Exception:
            logger.debug("expansion_parking_asset query failed for parcel_id=%s, falling back to OSM", parcel_id, exc_info=True)

    if parking_table_available and not _parking_data_resolved:
        try:
            with db.begin_nested():
                parking_row = db.execute(
                    text(
                        f"""
                        WITH p AS (
                            SELECT geom
                            FROM {ARCGIS_PARCELS_TABLE}
                            WHERE id::text = :parcel_id
                            LIMIT 1
                        )
                        SELECT COALESCE((
                            SELECT COUNT(*)
                            FROM planet_osm_polygon op
                            WHERE op.way IS NOT NULL
                              AND (
                                lower(COALESCE(op.amenity, '')) = 'parking'
                                OR lower(COALESCE(op.parking, '')) IN ('surface','multi-storey','underground')
                              )
                              AND ST_DWithin(op.way::geography, ST_Centroid(p.geom)::geography, 350)
                        ), 0) AS nearby_parking_amenity_count
                        FROM p
                        """
                    ),
                    {"parcel_id": str(parcel_id)},
                ).mappings().first()
                if parking_row:
                    nearby_parking_amenity_count = _safe_int(parking_row.get("nearby_parking_amenity_count"))
                    base["nearby_parking_amenity_count"] = nearby_parking_amenity_count
                    # Context is available when the query returned a value —
                    # 0 means "looked and found nothing", not "unavailable".
                    base["context_sources"]["parking_context_available"] = _context_checked(
                        parking_row.get("nearby_parking_amenity_count")
                    )
        except Exception:
            logger.debug("parking context query failed for parcel_id=%s", parcel_id, exc_info=True)

    # Add evidence band metadata for UI / memo rendering.
    base["context_sources"]["road_evidence_band"] = _road_evidence_band(
        base.get("nearby_road_segment_count") if base["context_sources"].get("road_context_available") else None,
        base.get("touches_road") if base["context_sources"].get("road_context_available") else None,
    )
    base["context_sources"]["parking_evidence_band"] = _parking_evidence_band(
        base.get("nearby_parking_amenity_count") if base["context_sources"].get("parking_context_available") else None,
    )

    missing_context: list[str] = []
    if not roads_table_available:
        missing_context.append("roads_table_unavailable")
    if not parking_table_available:
        missing_context.append("parking_table_unavailable")
    if roads_table_available and not base["context_sources"].get("road_context_available"):
        missing_context.append("road_context_unavailable")
    if parking_table_available and not base["context_sources"].get("parking_context_available"):
        missing_context.append("parking_context_unavailable")
    if not zoning_context_available:
        missing_context.append("zoning_context_unavailable")
    if not delivery_observed:
        missing_context.append("delivery_observation_unavailable")
    base["missing_context"] = missing_context

    completeness_components = [100.0]
    completeness_components.append(100.0 if zoning_context_available else 0.0)
    completeness_components.append(100.0 if delivery_observed else 0.0)
    completeness_components.append(100.0 if roads_table_available else 0.0)
    completeness_components.append(100.0 if parking_table_available else 0.0)
    completeness_components.append(100.0 if base["context_sources"].get("road_context_available") else 0.0)
    completeness_components.append(100.0 if base["context_sources"].get("parking_context_available") else 0.0)
    base["data_completeness_score"] = int(round(sum(completeness_components) / len(completeness_components)))
    return base


def _area_fit(area_m2: float, target_area_m2: float, min_area_m2: float, max_area_m2: float) -> float:
    if area_m2 <= 0:
        return 0.0
    if area_m2 < min_area_m2 or area_m2 > max_area_m2:
        return 0.0
    span = max(max_area_m2 - min_area_m2, 1.0)
    distance = abs(area_m2 - target_area_m2)
    score = 100.0 - (distance / span) * 100.0
    return _clamp(score)


def _population_score(
    population_reach: float,
    *,
    service_model: str | None = None,
) -> float:
    """Square-root scaled population score with service-model-aware reference.

    The saturation reference scales with the catchment radius so the
    score remains meaningfully distributed across the realistic range:

      service_model    catchment    reference    reach → score examples
      dine_in          3.5 km       250,000      50k→45, 100k→63, 250k→100
      delivery_first   3.0 km       180,000      30k→41, 80k→67, 180k→100
      qsr              1.5 km        80,000       5k→25, 30k→61,  80k→100
      cafe             1.0 km        40,000       5k→35, 15k→61,  40k→100

    Without a service model the function falls back to the QSR reference
    (80k), which matches legacy behavior for any caller that hasn't been
    updated yet.
    """
    if population_reach <= 0:
        return 0.0
    reference = _population_reference(service_model)
    return _clamp((population_reach / reference) ** 0.5 * 100.0)


def _delivery_score(
    delivery_listing_count: int,
    *,
    realized_demand: float | None = None,
    blend_weight: float = 0.5,
    reference: float | None = None,
) -> float:
    """Square-root scaled delivery score for wider dynamic range.

    Two signals are supported:

    * ``delivery_listing_count`` — same-category branches observed in the
      delivery catchment (supply / saturation proxy).  Always available.
    * ``realized_demand`` — Σ Δrating_count across same-category branches in
      the catchment over the trailing window (delivery rating velocity; a
      partial proxy for order volume, since only a fraction of orders —
      typically 5–30% on food-delivery platforms — produce a rating, so the
      figure systematically undercounts true orders).  Only populated when
      ``EXPANSION_REALIZED_DEMAND_ENABLED`` is true and the history table
      has ≥2 snapshots for the catchment.

    When realized demand is available, blend it with the listing-count
    signal: ``score = (1-w) · listing + w · realized``.  Otherwise fall
    back to today's supply-count behavior unchanged.

    ``reference`` is the realized-demand saturation point (realized_demand
    == reference maps to 100).  Callers in the search pipeline pass the
    service-model-aware anchor from ``_realized_demand_reference()``; when
    omitted it falls back to the global
    ``EXPANSION_REALIZED_DEMAND_REFERENCE`` setting, preserving the legacy
    behavior for any caller that hasn't been updated.
    """
    listing_score = (
        0.0
        if delivery_listing_count <= 0
        else _clamp((delivery_listing_count / 40.0) ** 0.5 * 100.0)
    )
    if realized_demand is None or realized_demand <= 0:
        return listing_score
    # Reference point: realized_demand equal to the reference maps to a
    # score of 100.  Square-root scaling mirrors the listing-count term so
    # the two blend cleanly.  Calibrate per service model via
    # scripts/diagnostics/delivery_demand_legs_probe.sql
    # (_REALIZED_DEMAND_REFERENCE), global fallback via
    # scripts/diagnostics/realized_demand_calibration.sql.
    if reference is None:
        reference = settings.EXPANSION_REALIZED_DEMAND_REFERENCE
    realized_score = _clamp((realized_demand / reference) ** 0.5 * 100.0)
    bw = max(0.0, min(1.0, blend_weight))
    return _clamp(listing_score * (1.0 - bw) + realized_score * bw)


def _demand_blend_weights(service_model: str) -> tuple[float, float]:
    """Return (population_weight, delivery_weight) tuned by service model.

    - delivery_first: delivery density is the primary demand signal (0.40 / 0.60)
    - dine_in: population/foot-traffic dominates (0.75 / 0.25)
    - cafe: moderate population bias (0.55 / 0.45 — shifted from 0.70/0.30 to
      reduce score compression from uniform population signals)
    - qsr (default): balanced with slight population lean (0.60 / 0.40)
    """
    _BLENDS: dict[str, tuple[float, float]] = {
        "delivery_first": (0.40, 0.60),
        "qsr":            (0.60, 0.40),
        "cafe":           (0.55, 0.45),
        "dine_in":        (0.75, 0.25),
    }
    return _BLENDS.get(service_model, (0.60, 0.40))


_WHITESPACE_LOG_REF: dict[str, float] = {
    # Per-service-model reference count where the curve reaches the 15.0 floor
    # (``raw = 100·(1 − log1p(count)/log1p(REF))`` floors structurally at
    # ``count = REF``). dine_in scores same-category competitors over a tight
    # 1000 m trade area where in-range counts run p50 ~16 / p75 ~24 / p90 ~32;
    # under the default REF=25 the p50 already sits at the floor, collapsing
    # the whole p50–p75 band to a flat 15. REF=50 keeps that band spread and
    # floors only the genuinely saturated count ≥ ~40 tail.
    #
    # delivery_first has the identical signature once its competition radius is
    # tightened to 1000 m: probe counts run p50 ~16 / p90 ~29 / max ~32, so the
    # default REF=25 would re-floor the p50. REF=50 spreads that band (count
    # 16 → ~28). The 1000 m probe counts are the under-counting approximation
    # (simplified category match, no alias expansion), so true production counts
    # run somewhat higher — REF=50 (matching dine_in) deliberately avoids
    # re-flooring on the alias-expanded counts rather than sizing lower.
    #
    # qsr has the same settlement shape as dine_in and delivery_first once its
    # competition radius is tightened to 1000 m, but its counts run higher, so
    # it takes REF=75 instead of 50: floors only count >= 39 (c* exact 38.69);
    # at the 1000 m counts this spreads the p25–p75 band to ~26–63 (burger
    # scope) and floors 4.9%. KNOWN LIMITATION: broad `fast_food`-scope briefs
    # still floor ~32% at this setting (probe record 15); acceptable because
    # production briefs are narrow-scope; the structural fix would be
    # scope-aware REF — future work, not this PR.
    #
    # All other service models keep the default 25.
    "dine_in": 50.0,
    "delivery_first": 50.0,
    "qsr": 75.0,
}
_WHITESPACE_LOG_REF_DEFAULT: float = 25.0


_REALIZED_DEMAND_REFERENCE: dict[str, float] = {
    # Per-service-model saturation reference for the realized-demand leg of
    # _delivery_score (realized_demand == REF maps to a score of 100 on the
    # square-root curve). Re-anchored 2026-06-10 from a 1,220-candidate /
    # trailing-30d probe (scripts/diagnostics/delivery_demand_legs_probe.sql)
    # using the SAME rule as the original 2026-05-15 calibration of
    # EXPANSION_REALIZED_DEMAND_REFERENCE: anchor at each model's
    # realized_demand_30d p75 so the median lands in the ~70s and only the
    # top quartile saturates. The global p75=263 anchor had drifted low —
    # 62.5% of qsr candidates sat at/over it, pinning the realized leg at 100
    # (probe realized_p50 score: qsr 100, dine_in 98.5).
    #
    # These anchors are calibrated to counts measured at the 1200 m
    # EXPANSION_REALIZED_DEMAND_RADIUS_M catchment — re-derive them from the
    # probe if that radius ever changes.
    #
    # cafe is deliberately absent (no cafe rows in the probe window) and any
    # model not listed here falls back to the
    # EXPANSION_REALIZED_DEMAND_REFERENCE env default (263.0).
    "delivery_first": 307.0,
    "dine_in": 402.0,
    "qsr": 327.0,
}


def _realized_demand_reference(service_model: str | None) -> float:
    """Realized-demand saturation reference for this service model.

    Models absent from ``_REALIZED_DEMAND_REFERENCE`` (e.g. ``cafe``) fall
    back to the ``EXPANSION_REALIZED_DEMAND_REFERENCE`` env default.
    """
    return _REALIZED_DEMAND_REFERENCE.get(
        (service_model or "").lower(),
        settings.EXPANSION_REALIZED_DEMAND_REFERENCE,
    )


def _competition_whitespace_score(
    competitor_count: int,
    *,
    confident: bool | None = None,
    service_model: str | None = None,
) -> float:
    """Log-decay whitespace score over same-category competitor counts.

    The curve is ``raw = 100·(1 − log1p(count)/log1p(REF))`` clamped to a
    15.0 floor, so it decays steeply at low counts and gently at high ones,
    reaching the floor structurally at ``count = REF``. ``REF`` is
    service-model-aware via ``_WHITESPACE_LOG_REF`` (``dine_in`` and
    ``delivery_first`` → 50, ``qsr`` → 75, all other models → 25 default),
    because those three score their competitors over a tighter 1000 m trade
    area whose in-range counts are large enough that the default REF=25
    would floor the p50 (qsr's counts run higher still, hence 75).

    Representative outputs (count → score):
      REF=25 (default):  0→100*, 1→79, 3→57, 6→40, 16→15 (floored at ≤16),
                         25→15 (floor).
      REF=50 (dine_in):  0→100*, 1→82, 6→50, 16→28, 24→18, 32→15, 40→15,
                         50→15 (floor).
      REF=75 (qsr):      0→100*, 4→63, 16→35, 24→26, 32→19, 39→15 (floor
                         onset), 75→15 (floor).
    (*count 0 only when ``confident``; see F4 below.)

    F4 (defensive): ``count=0`` only earns the wide-open 100 when
    ``confident`` is truthy — i.e. the competitor scan actually observed
    broader POI/delivery presence in the radius, so a zero same-category
    count is trustworthy evidence of a genuine greenfield. Both
    ``confident=False`` (scan ran but found thin POI coverage, not a true
    greenfield) and ``confident=None`` (no scan ran / caller supplied no
    flag — e.g. the ArcGIS-fallback pool path) are treated as unknown and
    return the neutral midpoint (50.0). Unknown competitor data must not
    be scored as a confirmed empty market: that fabricates evidence and
    pushes thin-coverage candidates up the ranking.
    """
    if not confident and competitor_count <= 0:
        return 50.0
    if competitor_count <= 0:
        return 100.0
    ref = _WHITESPACE_LOG_REF.get(
        (service_model or "").lower(), _WHITESPACE_LOG_REF_DEFAULT
    )
    # Log-scaled decay: steeper at low counts, gentler at high counts.
    raw = 100.0 * (1.0 - (math.log1p(competitor_count) / math.log1p(ref)))
    # Floor at 15 — even saturated areas get some score so rankings remain
    # distinguishable.
    return _clamp(max(15.0, raw))


def _chain_strength_score(max_chain_strength: float | None) -> float:
    """Pillar 2 scoring input: validation by established brands.

    Higher ``max_chain_strength`` (max chain_strength_score from
    ``expansion_competitor_quality`` across same-category POIs in the
    candidate's competition radius) = stronger evidence the area is
    validated by an established operator. Pro-presence direction.

    When no same-category competitor POIs are in radius (aggregate is
    None), the signal is missing rather than zero — return the neutral
    midpoint 50.0 so thin-data candidates are neither penalized nor
    rewarded. Mirrors the F4 defensive pattern in
    ``_competition_whitespace_score``.
    """
    if max_chain_strength is None:
        return 50.0
    return _clamp(float(max_chain_strength))


def _confidence_score(
    *,
    is_listing: bool = False,
    rent_confidence: str | None = None,
    area_confidence: str | None = None,
    unit_street_width_m: float | None = None,
    image_url: str | None = None,
    landuse_label: str | None = None,
    population_reach: float = 0.0,
    delivery_listing_count: int = 0,
) -> float:
    """Confidence score on a 0-100 scale.

    For listings: rewards measured ground truth from Aqar.
    For parcels: legacy district-context formula, capped at 70.
    """
    if is_listing:
        score = 30.0  # base for being a real listing

        if rent_confidence == "actual":
            score += 20.0

        if area_confidence == "actual":
            score += 15.0

        if unit_street_width_m is not None and unit_street_width_m > 0:
            score += 15.0

        if image_url:
            score += 10.0

        if landuse_label:
            score += 5.0
        if population_reach > 0:
            score += 5.0

        return _clamp(score)

    # Parcel path: legacy formula, capped at 70.
    score = 40.0
    if landuse_label:
        score += 25.0
    if population_reach > 0:
        score += 20.0
    if delivery_listing_count > 0:
        score += 15.0
    return min(70.0, _clamp(score))


def _effective_listing_age_days(
    row: Mapping[str, Any],
) -> tuple[int | None, str]:
    """Return (days since most-recent Aqar date, source tag).

    Picks GREATEST of ``aqar_updated_at``, ``aqar_created_at``, and
    ``first_seen_at`` — an unweighted max that treats a recent "updated"
    timestamp as equally strong as a recent "created" timestamp, per the
    Phase 3a directive. Returns ``(None, "unknown")`` when all three are
    NULL or implausible so the caller can resolve to a neutral band without
    penalty. Tz-aware values are coerced to naive UTC so arithmetic matches
    the existing first_seen_at path. Timestamps more than one day in the
    future are rejected as parser/clock drift rather than clamped, so a
    corrupted source cannot artificially produce a top-band freshness.
    """
    now = datetime.utcnow()
    future_cutoff = now + timedelta(days=1)
    candidates: list[tuple[datetime, str]] = []
    for key, tag in (
        ("unit_aqar_updated_at", "aqar_updated"),
        ("unit_aqar_created_at", "aqar_created"),
        ("unit_first_seen_at", "first_seen"),
    ):
        value = row.get(key)
        if value is None:
            continue
        if getattr(value, "tzinfo", None) is not None:
            value = value.replace(tzinfo=None)
        if value > future_cutoff:
            continue
        candidates.append((value, tag))
    if not candidates:
        return None, "unknown"
    picked, tag = max(candidates, key=lambda pair: pair[0])
    days = (now - picked).days
    if days < 0:
        days = 0
    return days, tag


def _created_basis_age_days(row: Mapping[str, Any]) -> int | None:
    """Return listing age (days) on the ORIGINAL-listing-date basis.

    Uses ``aqar_created_at`` with ``first_seen_at`` as a COALESCE null-guard
    floor ONLY — deliberately NOT the GREATEST-of-three basis of
    ``_effective_listing_age_days``. A long-vacant listing that gets re-posted
    or re-priced is still stale (often more telling), and a GREATEST-of-three
    basis would reset its age to the re-post date and erase exactly the
    staleness signal the relative listing-age percentile is built on. The
    candidate's age and the comparable CASE in ``_percentile_rent_burden`` MUST
    share this created-at basis or the percentile is meaningless. Other callers
    keep ``_effective_listing_age_days``; this is a parallel created-basis age.
    """
    basis = row.get("unit_aqar_created_at")
    if basis is None:
        basis = row.get("unit_first_seen_at")
    if basis is None:
        return None
    now = datetime.utcnow()
    try:
        if getattr(basis, "tzinfo", None) is not None:
            basis = basis.replace(tzinfo=None)
        if basis > now + timedelta(days=1):
            return None
        days = (now - basis).days
    except (TypeError, ValueError):
        return None
    return max(0, days)


# ---------------------------------------------------------------------------
# Phase 3b — district momentum
# ---------------------------------------------------------------------------

# Kill-switch. When False, _listing_quality_score reverts to the pre-3b
# weight tuple (0.30/0.40/0.20/0.10) and ignores district_momentum_score
# entirely. This is the clean revert path because the 3b-rebalanced
# weights sum to 0.85 without momentum; dropping the term without
# reverting the weights would break the 0-100 scale.
_MOMENTUM_ENABLED = True

# Trailing window (days) over which a listing's aqar_created_at OR
# aqar_updated_at must fall to count toward activity_30d.
_MOMENTUM_WINDOW_DAYS = 30

# Minimum active listings per district for the district to earn a
# percentile-ranked momentum score. Districts below the floor resolve
# to a neutral 50.0 (percentile_composite = 0.5) — tri-state "unknown",
# not a penalty. DB verification confirmed 37 districts qualify at 20,
# covering 69.08% of active rows.
_MOMENTUM_SAMPLE_FLOOR = 20

# Phase 4 display-only threshold. A candidate earns the "Top-tier
# market" surfacing (card tag + rationale line) iff district
# momentum_score is at or above this cliff AND sample_floor_applied is
# False. The same constant is mirrored at the frontend call site; the
# two must match numerically by convention, not by shared config. This
# is intentionally not wired to env var / settings / feature flag so a
# future tune-up edits exactly one backend constant and one frontend
# literal. Pill renamed from "Active market" in Phase 4.1 — the
# threshold and math are unchanged.
_MOMENTUM_DISPLAY_THRESHOLD = 70.0

# Weight stack v2: district momentum is paid once, as its own top-level
# component (see _score_breakdown). This helper is the extracted momentum
# sub-signal historically embedded in _listing_quality_score — same 0-100
# raw value (district-level 30-day Aqar activity percentile), with unknown
# (None) resolving to a neutral 50.0 per the tri-state convention.
def _district_momentum_component(district_momentum_score: float | None) -> float:
    if district_momentum_score is None:
        return 50.0
    return _clamp(float(district_momentum_score))


# Phase 4 display-only freshness window. Phase 4.1: a listing earns
# "New" when aqar_created_at is within this window, and "Updated" when
# it is older than the window but aqar_updated_at is within it.
# New and Updated are mutually exclusive and are checked directly
# against the created_days / updated_days fields on
# feature_snapshot_json.listing_age — NOT against the GREATEST()-derived
# `source` tag (which the scraper's daily cadence otherwise biases
# toward aqar_updated on ~93% of rows, making "New" unreachable).
# Mirrored at the frontend call site.
_LISTING_FRESHNESS_DAYS = 7


def _listing_quality_score(
    *,
    is_listing: bool,
    effective_age_days: int | None,
    is_furnished: bool | None,
    unit_restaurant_score: float | None,
    has_image: bool,
    has_drive_thru: bool | None = None,
    llm_suitability_score: int | None = None,
    llm_listing_quality_score: int | None = None,
    district_momentum_score: float | None = None,
) -> float:
    """Pure listing-quality score on a 0-100 scale.

    Freshness is measured as the number of days since the most recent of
    ``aqar_updated_at``, ``aqar_created_at``, or ``first_seen_at`` (the
    "effective listing age," computed by ``_effective_listing_age_days``).
    This implements the directive that both newly uploaded and recently
    updated listings should be prioritised equally — a nine-month-old
    listing that the owner refreshed yesterday is treated as fresh, because
    the refresh signals the opportunity is live. Rows without any known
    date resolve to a neutral 50.0 rather than a penalty, matching the
    codebase's tri-state-gate convention for unknowns.

    Distinct from _confidence_score (which measures whether the data is
    trustworthy). This measures whether the listing itself is a good
    F&B real estate opportunity, independent of how confident we are
    in the underlying data.

    For parcels (or any candidate without a commercial_unit row),
    returns a neutral 50.

    Components (post-2026-05-07 sub-weights when _MOMENTUM_ENABLED is True):
      - Freshness from effective_age_days (30.00%): how recently the listing
        was created or updated on Aqar (bands at 14/30/60/120/240/365 —
        frozen for Phase 3a).
      - Aqar suitability (20.00%): the classifier's assessment — LLM verdict
        when available, structural restaurant_score fallback otherwise
      - Image / fit-out signal (10.00%): LLM-derived listing quality when
        available, binary image presence fallback otherwise
      - Furnished (5.00%): faster open, lower risk, lower fitout
      - District momentum (35.00%): percentile-ranked 30-day activity in
        the district (blended creates + updates on commercial_unit).
        Districts below the sample floor resolve to a neutral 50.0.
      - Drive-thru bonus: small additive (+5) when present

    The 2026-05-07 rebalance lifted freshness (0.2550 → 0.30) and momentum
    (0.1500 → 0.35) and rescaled the remaining sub-weights by
    0.35 / 0.595 = 0.5882353. When _MOMENTUM_ENABLED is False the pre-3b
    tuple (0.30/0.40/0.20/0.10) is used and district_momentum_score is
    ignored.

    Momentum values are always on the same 0-100 scale as the other
    sub-signals. None (or _MOMENTUM_ENABLED=False) selects neutral 50.0.
    """
    if not is_listing:
        return 50.0

    # Freshness band (based on effective listing age — see
    # _effective_listing_age_days: greatest of aqar_updated_at,
    # aqar_created_at, first_seen_at).
    if effective_age_days is None:
        freshness = 50.0
    else:
        days = effective_age_days
        if days <= 14:
            freshness = 100.0
        elif days <= 30:
            freshness = 92.0
        elif days <= 60:
            freshness = 80.0
        elif days <= 120:
            freshness = 65.0
        elif days <= 240:
            freshness = 45.0
        elif days <= 365:
            freshness = 28.0
        else:
            freshness = 15.0

    # Suitability sub-component: prefer the LLM verdict when present.
    # The LLM produces a calibrated 0–100 score directly, so no rescale
    # is needed. Falls back to the Patch 10 structural rescale
    # (score * 2) for rows that haven't been LLM-classified yet — during
    # the rollout window after Patch 12 deploys but before the backfill
    # completes, and as a permanent safety net for any row whose LLM
    # classification returned None.
    if llm_suitability_score is not None:
        suitability = _clamp(float(llm_suitability_score))
    elif unit_restaurant_score is not None and unit_restaurant_score > 0:
        suitability = _clamp(float(unit_restaurant_score) * 2.0)
    else:
        suitability = 50.0

    # Visual / fit-out readiness signal: prefer the LLM-derived quality
    # score when present (it captures fit-out readiness from photos plus
    # description, not just whether an image exists).  Fall back to the
    # binary "has_image" check.
    if llm_listing_quality_score is not None:
        image_signal = _clamp(float(llm_listing_quality_score))
    else:
        image_signal = 100.0 if has_image else 30.0

    # Furnished: faster open, lower risk
    furnished_signal = 100.0 if is_furnished else 50.0

    if str(getattr(settings, "EXPANSION_WEIGHT_STACK", "v1")) == "v2":
        # Weight stack v2: momentum is paid once, as the top-level
        # district_momentum component in _score_breakdown — it no longer
        # contributes here. The remaining momentum-enabled sub-weights
        # (0.30 / 0.20 / 0.10 / 0.05) renormalize by /0.65 so the tuple
        # still sums to 1.0 (≈ 0.4615 / 0.3077 / 0.1538 / 0.0769). The
        # +5 drive-thru bump and the parcel neutral-50 path are unchanged.
        composite = (
            freshness * (0.30 / 0.65)
            + suitability * (0.20 / 0.65)
            + image_signal * (0.10 / 0.65)
            + furnished_signal * (0.05 / 0.65)
        )
    elif _MOMENTUM_ENABLED:
        # Sub-weight rebalance — 2026-05-07 (CEO directive elevation).
        # Audit (branch claude/audit-advisor-ranking-4prR3) found momentum
        # contributing only ~1.65% and freshness only ~2.81% of final_score,
        # below the noise threshold for any real rank movement. Lifted
        # freshness 0.2550→0.30 and momentum 0.1500→0.35; the remaining
        # sub-weights (suitability, image, furnished) were rescaled by
        # 0.35 / 0.595 = 0.5882353 so the tuple still sums to 1.0:
        #   suitability       0.3400 → 0.20
        #   image_signal      0.1700 → 0.10
        #   furnished_signal  0.0850 → 0.05
        # Combined with the listing_quality top-level lift in
        # _score_breakdown (0.11 → 0.22), momentum's share of final_score
        # rises from ~1.65% to ~7.7% and freshness from ~2.81% to ~6.6%.
        # Unknown momentum → neutral 50.0 per the tri-state convention
        # used by freshness and suitability above.
        momentum_signal = _district_momentum_component(district_momentum_score)
        composite = (
            freshness * 0.30
            + suitability * 0.20
            + image_signal * 0.10
            + furnished_signal * 0.05
            + momentum_signal * 0.35
        )
    else:
        # Pre-3b weight tuple. Used by the kill-switch revert path;
        # district_momentum_score is intentionally ignored so the scale
        # stays 0-100 without a 0.85 sub-weight sum.
        composite = (
            freshness * 0.30
            + suitability * 0.40
            + image_signal * 0.20
            + furnished_signal * 0.10
        )

    # Small drive-thru bonus when present (rare on Aqar but valuable for QSR)
    if has_drive_thru:
        composite += 5.0

    return _clamp(composite)


def _candidate_gate_status(
    *,
    fit_score: float,
    area_fit_score: float,
    area_m2: float,
    min_area_m2: float,
    max_area_m2: float,
    zoning_fit_score: float,
    landuse_available: bool,
    frontage_score: float,
    access_score: float,
    parking_score: float | None,
    district: str | None,
    distance_to_nearest_branch_m: float | None,
    provider_density_score: float,
    multi_platform_presence_score: float,
    economics_score: float,
    brand_profile: dict[str, Any],
    road_context_available: bool,
    parking_context_available: bool,
    is_listing: bool = False,
    unit_street_width_m: float | None = None,
    zoning_verdict_hint: str | None = None,
    radiance_growth: dict[str, Any] | None = None,
    radiance_yoy_threshold: float | None = None,
) -> tuple[dict[str, bool | None], dict[str, Any]]:
    thresholds = {
        "area_fit_min": 55.0,
        "zoning_fit_min": 60.0,
        "frontage_access_min": 55.0,
        "parking_min": 45.0,
        "economics_min": settings.EXPANSION_VIABILITY_ECONOMICS_MIN,
        # Calibrated 2026-05-12 from trailing-90d density distribution:
        # passes 92% of candidates; cleanly excludes the dead-zone
        # cluster (density 0-5). See docs/investigations/
        # delivery_market_investigation_2026-05-12.md and
        # scripts/diagnostics/density_threshold_calibration.sql
        "delivery_provider_density_min": 10.0,
        "delivery_platform_presence_min": 35.0,
        "cannibalization_min_distance_m": _safe_float(brand_profile.get("cannibalization_tolerance_m"), 1800.0),
    }
    # Area fit gate: pure range check against the user's stated bounds.
    # target_area_m2 still influences ranking via area_fit_score → fit_score,
    # but no longer acts as a hard constraint.  A 384 m² listing in a 100–500
    # range must not hard-fail because it is "not close enough to target."
    area_fit_pass: bool | None = (min_area_m2 <= area_m2 <= max_area_m2) if area_m2 > 0 else None
    # Tri-state zoning gate using ArcGIS classification semantics:
    #   - "pass" verdict  => True  (clearly commercial/mixed-use)
    #   - "fail" verdict  => False (clearly disallowed)
    #   - "unknown" or weak signal => None (needs verification)
    #   - no landuse data => None
    if not landuse_available:
        zoning_fit_pass: bool | None = None
    elif zoning_verdict_hint == "pass":
        zoning_fit_pass = True
    elif zoning_verdict_hint == "fail":
        zoning_fit_pass = False
    elif zoning_verdict_hint == "unknown":
        # Weak/ambiguous ArcGIS signal: gate is indeterminate, not hard fail.
        # Still use score threshold as a soft check — high enough score
        # (from label tokens) can push to pass, but low score stays unknown.
        if zoning_fit_score >= thresholds["zoning_fit_min"]:
            zoning_fit_pass = True
        else:
            zoning_fit_pass = None
    else:
        # Legacy fallback: plain threshold
        zoning_fit_pass = zoning_fit_score >= thresholds["zoning_fit_min"]
    # Frontage/access gate: tri-state for listings.
    # For listings, the only ground truth we have is unit_street_width_m.
    # Bulk OSM road enrichment is about the surrounding road network, not
    # the listing's own street, so it cannot substitute. If we don't have
    # the listing's street width, mark unknown.
    _has_street_width = bool(unit_street_width_m and unit_street_width_m > 0)
    if is_listing and not _has_street_width:
        frontage_access_pass: bool | None = None
    else:
        frontage_access_pass = (frontage_score >= thresholds["frontage_access_min"]) and (access_score >= thresholds["frontage_access_min"])
    # parking_pass — trust the derived parking_score when it's populated.
    # For Aqar listings, Aqar doesn't expose a structured parking field
    # (their template only has Water/Electricity/Drainage), so the verdict
    # has historically been None. But the scorer already derives a parking
    # context from nearby OSM parking amenities and street-width signals,
    # and that score is populated on virtually all candidates. Treating that
    # score as ground truth for the gate lets overall_pass resolve to a
    # real true/false verdict instead of being stuck at null.
    parking_pass: bool | None
    if parking_score is None:
        parking_pass = None
    elif parking_score >= thresholds["parking_min"]:
        parking_pass = True
    else:
        parking_pass = False

    district_norm = normalize_district_key(district) if district else None
    excluded = {
        normalize_district_key(item)
        for item in (brand_profile.get("excluded_districts") or [])
        if normalize_district_key(item)
    }
    district_pass = not (district_norm and district_norm in excluded)

    cannibalization_pass = distance_to_nearest_branch_m is None or distance_to_nearest_branch_m >= thresholds["cannibalization_min_distance_m"]

    primary_channel = (brand_profile.get("primary_channel") or "balanced").lower()
    if primary_channel == "delivery":
        # Gate rebased onto provider_density_score alone: only HungerStation
        # is producing live data today, so multi_platform_presence_score is
        # not a usable signal. Threshold calibrated against the trailing-90d
        # density distribution (see delivery_provider_density_min comment).
        delivery_market_pass = (
            provider_density_score
            >= thresholds["delivery_provider_density_min"]
        )
    else:
        delivery_market_pass = True

    economics_pass = economics_score >= thresholds["economics_min"]

    # Radiance growth gate (advisory only). True/False when the signal is
    # confident; None when no radiance data is available for the district.
    _radiance_threshold = (
        radiance_yoy_threshold
        if radiance_yoy_threshold is not None
        else getattr(settings, "EXPANSION_VIABILITY_RADIANCE_YOY_THRESHOLD", 0.0)
    )
    radiance_growth_pass: bool | None = None
    if isinstance(radiance_growth, dict) and radiance_growth.get("confident"):
        _yoy = radiance_growth.get("value_yoy_pct")
        if _yoy is not None:
            try:
                radiance_growth_pass = float(_yoy) >= float(_radiance_threshold)
            except (TypeError, ValueError):
                radiance_growth_pass = None

    gate_states: dict[str, bool | None] = {
        "zoning_fit_pass": zoning_fit_pass,
        "area_fit_pass": area_fit_pass,
        "frontage_access_pass": frontage_access_pass if road_context_available else None,
        "parking_pass": parking_pass if parking_context_available else None,
        "district_pass": district_pass,
        "cannibalization_pass": cannibalization_pass,
        "delivery_market_pass": delivery_market_pass,
        "economics_pass": economics_pass,
        "radiance_growth_pass": radiance_growth_pass,
    }
    failed = [k for k, v in gate_states.items() if v is False]
    passed = [k for k, v in gate_states.items() if v is True]
    unknown = [k for k, v in gate_states.items() if v is None]

    # Source of truth for hard-fail / advisory-only gates lives at module
    # scope (HARD_FAIL_GATES / ADVISORY_ONLY_GATES) so other modules can
    # import the same set instead of redefining it locally.
    hard_fail_gates = HARD_FAIL_GATES
    advisory_only_gates = ADVISORY_ONLY_GATES
    unknown_for_overall = [g for g in unknown if g not in advisory_only_gates]

    # Surface advisory failures separately so the frontend can render
    # caution/attention states without labeling the site as a hard FAIL.
    advisory_failures = [gate for gate in failed if gate not in hard_fail_gates]
    blocking_failures = [gate for gate in failed if gate in hard_fail_gates]

    # Three-state verdict:
    #   True  – no blocking failures
    #   False – at least one hard-fail gate failed
    #   None  – no blocking failures, but some gates are unknown/indeterminate
    if len(blocking_failures) > 0:
        overall_pass: bool | None = False
    elif len(unknown_for_overall) > 0:
        overall_pass = None
    else:
        overall_pass = True

    # Expose None (unknown) to callers instead of collapsing to True/False so
    # the frontend can distinguish "not evaluated" from "passed".
    gate_status: dict[str, bool | None] = {
        "zoning_fit_pass": zoning_fit_pass,
        "area_fit_pass": area_fit_pass,
        "frontage_access_pass": frontage_access_pass if road_context_available else None,
        "parking_pass": parking_pass if parking_context_available else None,
        "district_pass": district_pass,
        "cannibalization_pass": cannibalization_pass,
        "delivery_market_pass": delivery_market_pass,
        "economics_pass": economics_pass,
        "radiance_growth_pass": radiance_growth_pass,
        "overall_pass": overall_pass,
    }
    # Determine delivery observation status for honest gate explanations.
    _delivery_observed_for_gate = (
        provider_density_score > 0
        or multi_platform_presence_score > 0
    )
    if primary_channel == "delivery":
        if _delivery_observed_for_gate:
            delivery_explanation = "Delivery-market gate checks observed provider density and platform breadth."
        else:
            delivery_explanation = (
                "Delivery-market gate requires observed provider density and platform breadth, "
                "but no delivery activity was observed near this site. Gate result is based on inferred data."
            )
    else:
        if _delivery_observed_for_gate:
            delivery_explanation = "Delivery-market gate auto-passes for non-delivery channels. Observed delivery activity is available."
        else:
            delivery_explanation = (
                "Delivery-market gate auto-passes for non-delivery channels. "
                "No delivery activity was observed near this site — delivery scores are inferred."
            )
    explanations = {
        "zoning_fit_pass": "Zoning fit compares parcel land-use compatibility against threshold.",
        "area_fit_pass": (
            "Area data not available for this candidate."
            if area_fit_pass is None
            else "Area fit checks candidate area against requested branch range."
        ),
        "frontage_access_pass": (
            "Street width not available in listing; frontage cannot be evaluated."
            if frontage_access_pass is None and is_listing
            else "Frontage/access gate depends on road context and road-adjacent signals."
        ),
        "parking_pass": "Parking context is derived from nearby parking amenities and street geometry; passes when derived score meets the minimum threshold.",
        "district_pass": "District gate fails only for explicitly excluded districts.",
        "cannibalization_pass": "Cannibalization gate checks minimum spacing from existing branches.",
        "delivery_market_pass": delivery_explanation,
        "economics_pass": "Economics gate requires minimum economics score.",
        "radiance_growth_pass": (
            "Advisory market-growth signal from NASA Black Marble VNP46A3 "
            "monthly nighttime radiance (district YoY). Confident positive "
            "growth rescues a candidate from the market-viability conjunction; "
            "this gate is not a hard fail."
        ),
    }
    reasons = {
        "passed": passed,
        "failed": failed,
        "blocking_failures": blocking_failures,
        "advisory_failures": advisory_failures,
        "unknown": unknown,
        "thresholds": thresholds,
        "explanations": explanations,
        "delivery_observation_mode": "observed" if _delivery_observed_for_gate else "inferred",
    }
    return gate_status, reasons


def _landlord_signal_component(landlord_signal_score: int | float | None) -> float:
    """Return the LLM landlord-signal score clamped to 0-100.

    When the row is missing an LLM landlord signal (e.g. structural-fallback
    rows during the rollout window before Patch 12's backfill completes),
    fall back to a neutral 50.0 so the row is neither penalized nor boosted
    for the absence of the signal. Mirrors the None-handling pattern used by
    ``_listing_quality_score`` for ``llm_suitability_score``.
    """
    if landlord_signal_score is None:
        return 50.0
    return _clamp(float(landlord_signal_score))


# Components whose weights brand-brief knobs may re-weight (Finding 1).
_REWEIGHTABLE_COMPONENTS: tuple[str, ...] = (
    "occupancy_economics",
    "listing_quality",
    "brand_fit",
    "landlord_signal",
    "competition_whitespace",
    "chain_strength",
    "demand_potential",
    "access_visibility",
    "delivery_demand",
    "confidence",
    # Weight stack v2 component. No brand-brief knob maps to it yet, so its
    # multiplier stays 1.0; it still participates in the renormalization.
    "district_momentum",
)


# Archetype base weight profiles (EXPANSION_ARCHETYPE_PROFILES, v2 stack
# only). "balanced" IS the v2 stack; the other three move mass toward the
# archetype's decision-driving components while keeping the sum at exactly
# 100. Pathology guard: no profile sets brand_fit above 8 — its
# demand-inverse rank dominance makes higher weights actively harmful until
# the brand_fit de-dup lands (separate queued PR). Backtested (Probe F):
# avg top-5 overlap 4.7–4.8, rank corr 0.96–0.97 vs the balanced control.
_ARCHETYPE_WEIGHT_PROFILES: dict[str, dict[str, float]] = {
    "balanced": {
        "occupancy_economics": 20.0,
        "demand_potential": 18.0,
        "competition_whitespace": 12.0,
        "access_visibility": 11.0,
        "listing_quality": 9.0,
        "brand_fit": 8.0,
        "district_momentum": 7.0,
        "delivery_demand": 6.0,
        "landlord_signal": 5.0,
        "chain_strength": 4.0,
    },
    "delivery_led": {
        "occupancy_economics": 20.0,
        "demand_potential": 18.0,
        "competition_whitespace": 13.0,
        "access_visibility": 6.0,
        "listing_quality": 8.0,
        "brand_fit": 6.0,
        "district_momentum": 7.0,
        "delivery_demand": 13.0,
        "landlord_signal": 5.0,
        "chain_strength": 4.0,
    },
    "street_flagship": {
        "occupancy_economics": 19.0,
        "demand_potential": 19.0,
        "competition_whitespace": 11.0,
        "access_visibility": 17.0,
        "listing_quality": 8.0,
        "brand_fit": 8.0,
        "district_momentum": 6.0,
        "delivery_demand": 2.0,
        "landlord_signal": 5.0,
        "chain_strength": 5.0,
    },
    "neighborhood_local": {
        "occupancy_economics": 22.0,
        "demand_potential": 21.0,
        "competition_whitespace": 10.0,
        "access_visibility": 9.0,
        "listing_quality": 10.0,
        "brand_fit": 7.0,
        "district_momentum": 11.0,
        "delivery_demand": 4.0,
        "landlord_signal": 4.0,
        "chain_strength": 2.0,
    },
}


def _brand_weight_multipliers(
    brand_profile: dict[str, Any] | None,
    *,
    archetype_mode: bool = False,
) -> dict[str, float]:
    """Per-component weight multipliers derived from brand-brief knobs.

    Returns a multiplier (default 1.0) for each top-level scoring component.
    A neutral/empty profile (all "medium"/"balanced") returns all 1.0, so the
    reweighting is a no-op and scores stay byte-identical to the static-weight
    behavior. Gain is env-tunable; 0.0 disables.

    Legacy mapping (``archetype_mode=False`` — byte-identical to the
    pre-archetype behavior; product choice — see PR header):
      * physical-site knobs (parking/frontage/visibility sensitivity) -> access_visibility,
        using the strongest of the three (max), so caring about ANY of them lifts the
        measured-access weight.
      * primary_channel: "delivery" lifts delivery_demand (+g) and competition_whitespace
        (+0.5g); "dine_in" lifts access_visibility (+0.6g) and trims delivery_demand (-0.5g).
      * expansion_goal: "flagship" lifts access_visibility (+0.5g) and brand_fit (+0.5g);
        "delivery_led" lifts delivery_demand (+g) and competition_whitespace (+0.5g);
        "neighborhood" lifts demand_potential (+0.5g).
      * cannibalization_tolerance_m has no clean top-level target; it keeps flowing
        through brand_fit/occupancy_economics unchanged.

    Archetype mapping (``archetype_mode=True`` — EXPANSION_ARCHETYPE_PROFILES
    on under the v2 stack): channel/goal multipliers are retired — that mass
    now lives in the archetype base profile itself — and only the three
    site-sensitivity knobs remain, composed on top of the archetype weights.
    The legacy ``max()`` asymmetry is also fixed here: the signal of the
    strongest-MAGNITUDE knob wins, so a single "low" knob (signal −0.75)
    now trims access_visibility instead of being masked by the two neutral
    "medium" signals (0.0) under a plain max().
    """
    mult = {name: 1.0 for name in _REWEIGHTABLE_COMPONENTS}
    g = float(getattr(settings, "EXPANSION_BRAND_WEIGHT_GAIN", 0.0) or 0.0)
    if not brand_profile or g <= 0.0:
        return mult

    # _sensitivity_weight maps low->0.3, medium->0.6, high->1.0 (0.6 neutral).
    # Normalize to a [-0.75, +1.0] signal around the medium baseline.
    def _sig(level: str | None) -> float:
        return (_sensitivity_weight(level) - 0.6) / 0.4

    site_signals = (
        _sig(brand_profile.get("parking_sensitivity")),
        _sig(brand_profile.get("frontage_sensitivity")),
        _sig(brand_profile.get("visibility_sensitivity")),
    )
    if archetype_mode:
        # Signed signal of the strongest-magnitude knob ("high" +1.0 beats
        # "low" −0.75 on a tie of intent strength).
        site_sig = max(site_signals, key=abs)
    else:
        site_sig = max(site_signals)
    mult["access_visibility"] *= 1.0 + g * site_sig

    if archetype_mode:
        # Channel/goal weight roles retired — the archetype base profile
        # carries them. primary_channel keeps its gate and
        # _channel_fit_score raw-score roles elsewhere, unchanged.
        return {k: max(0.0, v) for k, v in mult.items()}

    channel = str(brand_profile.get("primary_channel") or "balanced").lower()
    if channel == "delivery":
        mult["delivery_demand"] *= 1.0 + g
        mult["competition_whitespace"] *= 1.0 + g * 0.5
    elif channel == "dine_in":
        mult["access_visibility"] *= 1.0 + g * 0.6
        mult["delivery_demand"] *= max(0.0, 1.0 - g * 0.5)

    goal = str(brand_profile.get("expansion_goal") or "balanced").lower()
    if goal == "flagship":
        mult["access_visibility"] *= 1.0 + g * 0.5
        mult["brand_fit"] *= 1.0 + g * 0.5
    elif goal == "delivery_led":
        mult["delivery_demand"] *= 1.0 + g
        mult["competition_whitespace"] *= 1.0 + g * 0.5
    elif goal == "neighborhood":
        mult["demand_potential"] *= 1.0 + g * 0.5

    # Guard against any negative weight from stacked trims.
    return {k: max(0.0, v) for k, v in mult.items()}


def _score_breakdown(
    *,
    demand_score: float,
    whitespace_score: float,
    brand_fit_score: float,
    economics_score: float,
    provider_intelligence_composite: float,
    access_visibility_score: float,
    confidence_score: float,
    listing_quality_score: float,
    landlord_signal_score: int | float | None = None,
    chain_strength_score: float = 50.0,
    chain_strength_max: float | None = None,
    brand_profile: dict[str, Any] | None = None,
    service_model: str | None = None,
    district_momentum_score: float | None = None,
) -> dict[str, Any]:
    """Listings-first weight distribution.

    Post-2026-05-07 weights (CEO directive elevation; see comment block
    on ``component_weights`` below):
      - occupancy_economics (26.2924%): rent burden, fitout, area, cannibalization
      - listing_quality (22%): freshness, momentum, suitability, furnished, image
      - landlord_signal (7.0112%):  LLM read of landlord intent / listing copy
      - access_visibility (8.7640%): measured street width
      - brand_fit (9.6404%): district preference + format fit
      - competition_whitespace (5.7640% post-Patch-B; was 8.7640%)
      - chain_strength (3.0% — Patch B; pulled from competition_whitespace)
      - demand_potential (8.7640%)
      - delivery_demand (4.3820%)
      - confidence (4.3820%): data trust signal

    Patch 13 promoted ``landlord_signal`` to its own first-class component,
    taking points from ``brand_fit`` and ``listing_quality``. The
    2026-05-07 rebalance then lifted ``listing_quality`` from 11 to 22 to
    materially elevate the recency and district-momentum signals; every
    other component was rescaled by 78/89 = 0.8764045 so weights still
    sum to 100. Patch B then carved 3.0 points out of competition_whitespace
    for the new chain_strength leg (pro-presence: established-brand
    validation), keeping the total at 100.

    Weight stack v2 (``EXPANSION_WEIGHT_STACK=v2``, 2026-06): district
    momentum becomes its own single-paid top-level component
    (``district_momentum``, raw input via ``district_momentum_score``),
    confidence is excluded from the weighted sum (raw value kept in the
    breakdown JSON for the UI data-quality grade), and weight mass moves
    toward the high-discrimination components:
      occupancy_economics 20, demand_potential 18, competition_whitespace 12,
      access_visibility 11, listing_quality 9, brand_fit 8,
      district_momentum 7, delivery_demand 6, landlord_signal 5,
      chain_strength 4 (fixed — EXPANSION_CHAIN_STRENGTH_WEIGHT is v1-only).
    Under v1 (default) ``district_momentum_score`` is ignored and the
    output is byte-identical to the pre-v2 behavior.
    """
    # Top-level weight rebalance — 2026-05-07 (CEO directive elevation).
    # Audit (branch claude/audit-advisor-ranking-4prR3) found that even
    # after raising the momentum/freshness sub-weights inside
    # _listing_quality_score, listing_quality at 11% of final_score kept
    # both directives below the noise threshold. Lifted listing_quality
    # 11 → 22; the remaining components were rescaled proportionally by
    # (100 - 22) / (100 - 11) = 78 / 89 = 0.8764045, with the rounding
    # residual absorbed into the largest remaining weight
    # (occupancy_economics):
    #   occupancy_economics    30 → 26.2924  (residual +0.0003 absorbed)
    #   brand_fit              11 →  9.6404
    #   landlord_signal         8 →  7.0112
    #   competition_whitespace 10 →  8.7640
    #   demand_potential       10 →  8.7640
    #   access_visibility      10 →  8.7640
    #   delivery_demand         5 →  4.3820
    #   confidence              5 →  4.3820
    # Net effect: momentum lifts to ~7.7% and freshness to ~6.6% of
    # final_score (up from ~1.65% and ~2.81%). All other listing_quality
    # sub-components also lift proportionally — listing-quality writ
    # large (recency, momentum, suitability, image, furnished) is the
    # CEO-aligned axis.
    # Patch B: chain_strength is a new pro-presence leg pulled from
    # competition_whitespace. The chain_strength weight is env-driven so
    # it can be calibrated without a code change; competition_whitespace
    # absorbs the equal-and-opposite move so the total stays at 100.
    _stack_v2 = str(getattr(settings, "EXPANSION_WEIGHT_STACK", "v1")) == "v2"
    # Archetype profiles (EXPANSION_ARCHETYPE_PROFILES): only meaningful
    # under v2 — under v1 the helper logs once and stays False.
    _archetypes_on = _archetype_profiles_active()
    if _stack_v2:
        # Weight stack v2 — 2026-06 probe-driven rebalance (see docstring).
        # confidence is intentionally absent: weight 0, display-only.
        # chain_strength is the fixed 4.0 of the v2 stack; the
        # EXPANSION_CHAIN_STRENGTH_WEIGHT env var is v1-only.
        # _ARCHETYPE_WEIGHT_PROFILES["balanced"] is this exact dict, so a
        # flag-on balanced search stays byte-identical to flag-off v2.
        component_weights = {
            "occupancy_economics": 20.0,
            "demand_potential": 18.0,
            "competition_whitespace": 12.0,
            "access_visibility": 11.0,
            "listing_quality": 9.0,
            "brand_fit": 8.0,
            "district_momentum": 7.0,
            "delivery_demand": 6.0,
            "landlord_signal": 5.0,
            "chain_strength": 4.0,
        }
        if _archetypes_on:
            _archetype = resolve_brand_archetype(brand_profile, service_model)
            component_weights = dict(_ARCHETYPE_WEIGHT_PROFILES[_archetype])
    else:
        _chain_strength_weight = float(settings.EXPANSION_CHAIN_STRENGTH_WEIGHT)
        _competition_whitespace_weight = round(8.7640 - _chain_strength_weight, 4)
        component_weights = {
            "occupancy_economics": 26.2924,
            "listing_quality": 22.0,
            "brand_fit": 9.6404,
            "landlord_signal": 7.0112,
            "competition_whitespace": _competition_whitespace_weight,
            "chain_strength": _chain_strength_weight,
            "demand_potential": 8.7640,
            "access_visibility": 8.7640,
            "delivery_demand": 4.3820,
            "confidence": 4.3820,
        }
    # Finding 1: brand-brief knobs re-weight components, then renormalize to 100.
    _w_mult = _brand_weight_multipliers(brand_profile, archetype_mode=_archetypes_on)
    if any(abs(m - 1.0) > 1e-9 for m in _w_mult.values()):
        _reweighted = {
            name: component_weights[name] * _w_mult.get(name, 1.0)
            for name in component_weights
        }
        _total = sum(_reweighted.values())
        if _total > 0:
            component_weights = {
                name: round(w * 100.0 / _total, 4) for name, w in _reweighted.items()
            }
            # Absorb the rounding residual into the largest weight so the sum
            # is exactly 100 and the assertion below holds.
            _residual = round(100.0 - sum(component_weights.values()), 4)
            _largest = max(component_weights, key=component_weights.get)
            component_weights[_largest] = round(
                component_weights[_largest] + _residual, 4
            )
    # Invariant: weights must sum to 100 so final_score stays on a 0-100 scale.
    # Tolerance accommodates IEEE-754 rounding of 4-decimal float weights.
    # Catches misconfigured EXPANSION_CHAIN_STRENGTH_WEIGHT at startup
    # rather than producing silently wrong scores.
    assert abs(sum(component_weights.values()) - 100) < 1e-3, (
        f"_score_breakdown component weights must sum to 100, "
        f"got {sum(component_weights.values())}"
    )
    landlord_input = _landlord_signal_component(landlord_signal_score)
    chain_strength_input = _safe_float(chain_strength_score)
    raw_inputs = {
        "occupancy_economics": round(_safe_float(economics_score), 2),
        "listing_quality": round(_safe_float(listing_quality_score), 2),
        "brand_fit": round(_safe_float(brand_fit_score), 2),
        "landlord_signal": round(landlord_input, 2),
        "competition_whitespace": round(_safe_float(whitespace_score), 2),
        "chain_strength": round(chain_strength_input, 2),
        "demand_potential": round(_safe_float(demand_score), 2),
        "access_visibility": round(_safe_float(access_visibility_score), 2),
        "delivery_demand": round(_safe_float(provider_intelligence_composite), 2),
        "confidence": round(_safe_float(confidence_score), 2),
    }
    if _stack_v2:
        raw_inputs["district_momentum"] = round(
            _district_momentum_component(district_momentum_score), 2
        )
    weighted_components = {
        name: round(_safe_float(raw_inputs[name]) * component_weights[name] / 100.0, 2)
        for name in component_weights
    }
    final_score = round(sum(weighted_components.values()), 2)
    display = {
        name: {
            "raw_input_score": raw_inputs[name],
            "weight_percent": component_weights[name],
            "weighted_points": weighted_components[name],
        }
        for name in component_weights
    }
    breakdown = {
        "weights": component_weights,
        "inputs": {
            **raw_inputs,
            "chain_strength_max": (
                round(float(chain_strength_max), 2)
                if chain_strength_max is not None
                else None
            ),
        },
        "weighted_components": weighted_components,
        "display": display,
        "final_score": round(_clamp(final_score), 2),
    }
    if _stack_v2 and _archetypes_on:
        # Additive, flag-on only: lets the UI card and memo name the weight
        # profile that produced these weights. Flag-off JSON is unchanged.
        breakdown["brand_archetype"] = _archetype
    if _stack_v2:
        # Confidence is computed and surfaced (inputs.confidence + this
        # block) but contributes 0 weighted points — the UI renders it as
        # a data-quality grade, not a weighted row.
        breakdown["display_only"] = {
            "confidence": {
                "raw_input_score": raw_inputs["confidence"],
                "weight_percent": 0.0,
            }
        }
    return breakdown


def _top_positives_and_risks(
    *,
    candidate: dict[str, Any],
    gate_reasons: dict[str, Any],
) -> tuple[list[str], list[str], list[dict[str, Any]], list[dict[str, Any]]]:
    positives: list[str] = []
    risks: list[str] = []
    # PR #2a: parallel structured records built from the same inputs as the
    # English strings. The English-rendering lines are NOT edited; each
    # structured append is paired one-to-one with its English append (same
    # order, same firing condition) so element i of a structured list
    # corresponds to element i of its English list after the identical
    # [:5] / [:6] slice. The two outputs are deliberately NOT DRY'd.
    positives_structured: list[dict[str, Any]] = []
    risks_structured: list[dict[str, Any]] = []

    # Determine delivery observation status upfront so wording can be qualified.
    delivery_observed = (
        _safe_float(candidate.get("provider_density_score")) > 0
        or _safe_float(candidate.get("multi_platform_presence_score")) > 0
        or _safe_float(candidate.get("delivery_competition_score")) > 0
    )

    if _safe_float(candidate.get("demand_score")) >= 70:
        positives.append("Demand potential is strong for this district.")
        positives_structured.append({"id": "pos.demand_strong", "params": {}})
    if _safe_float(candidate.get("whitespace_score")) >= 65:
        if delivery_observed and _safe_float(candidate.get("provider_whitespace_score")) >= 25:
            positives.append("Brick-and-mortar competitor whitespace remains favorable.")
            positives_structured.append({"id": "pos.bnm_whitespace_favorable", "params": {}})
        elif not delivery_observed:
            # Whitespace is high only because no delivery activity was observed —
            # phrase as inferred opportunity, not observed strength.
            positives.append("Inferred competitor whitespace opportunity — low observed delivery activity nearby.")
            positives_structured.append({"id": "pos.inferred_whitespace", "params": {}})
    if _safe_float(candidate.get("brand_fit_score")) >= 70:
        positives.append("Brand-fit profile aligns with site characteristics.")
        positives_structured.append({"id": "pos.brand_fit_aligned", "params": {}})
    if _safe_float(candidate.get("economics_score")) >= 65:
        positives.append("Economics profile meets target screening band.")
        positives_structured.append({"id": "pos.economics_meets_band", "params": {}})
    overall = (candidate.get("gate_status_json") or {}).get("overall_pass")
    if overall is True:
        positives.append("All required gates pass under available context.")
        positives_structured.append({"id": "pos.all_gates_pass", "params": {}})

    if _safe_float(candidate.get("cannibalization_score")) >= 70:
        risks.append("Cannibalization risk is elevated versus branch network.")
        risks_structured.append({"id": "risk.cannibalization_elevated", "params": {}})
    if _safe_float(candidate.get("economics_score")) < 50:
        risks.append("Economics score is below preferred threshold.")
        risks_structured.append({"id": "risk.economics_below_threshold", "params": {}})
    if delivery_observed and _safe_float(candidate.get("delivery_competition_score")) >= 65:
        risks.append("Delivery competition intensity is high.")
        risks_structured.append({"id": "risk.delivery_competition_high", "params": {}})
    if delivery_observed and _safe_float(candidate.get("provider_whitespace_score")) < 25 and _safe_float(candidate.get("delivery_competition_score")) >= 80:
        risks.append("Delivery platform competition is dense — limited delivery-channel whitespace.")
        risks_structured.append({"id": "risk.delivery_whitespace_limited", "params": {}})
    for gate in gate_reasons.get("failed") or []:
        label = _gate_key_to_label(gate)
        risks.append(f"{label.capitalize()} gate failed.")
        risks_structured.append({"id": "risk.gate_failed", "params": {"gate_key": str(gate)}})
    for gate in gate_reasons.get("unknown") or []:
        label = _gate_key_to_label(gate)
        risks.append(f"{label.capitalize()} could not be verified from current data.")
        risks_structured.append({"id": "risk.gate_unknown", "params": {"gate_key": str(gate)}})
    # Flag when delivery scores are inferred (no observed listings).
    if not delivery_observed:
        if _safe_float(candidate.get("provider_density_score")) > 0:
            risks.append("Delivery data is based on district-level estimates — no listings observed within 1.2 km.")
            risks_structured.append({"id": "risk.delivery_district_estimates", "params": {}})
        else:
            risks.append("Delivery market data is inferred — no observed listings near site.")
            risks_structured.append({"id": "risk.delivery_inferred", "params": {}})

    # ── Area utilization signal ──
    area_m2 = _safe_float(candidate.get("area_m2"))
    min_area = _safe_float(candidate.get("min_area_m2"), 80)
    max_area = _safe_float(candidate.get("max_area_m2"), 500)
    if area_m2 > 0 and max_area > min_area:
        mid_area = (min_area + max_area) / 2.0
        if abs(area_m2 - mid_area) / max(mid_area, 1.0) < 0.15:
            positives.append("Site area is well-aligned with target range.")
            positives_structured.append({"id": "pos.area_well_aligned", "params": {}})
        elif area_m2 < min_area * 1.1:
            risks.append(
                f"Area ({area_m2:.0f} m\u00b2) is near the minimum of the requested range."
            )
            risks_structured.append({"id": "risk.area_near_min", "params": {"area_m2": area_m2}})
        elif area_m2 > max_area * 0.9:
            risks.append(
                f"Area ({area_m2:.0f} m\u00b2) is near the maximum \u2014 may increase fit-out cost."
            )
            risks_structured.append({"id": "risk.area_near_max", "params": {"area_m2": area_m2}})

    # ── Rent economics signal ──
    economics = _safe_float(candidate.get("economics_score"))
    if economics >= 70:
        positives.append("Strong economics with favorable rent-to-revenue ratio.")
        positives_structured.append({"id": "pos.strong_economics", "params": {}})
    elif economics < 55:
        risks.append(
            "Economics are marginal \u2014 rent burden may be high relative to revenue potential."
        )
        risks_structured.append({"id": "risk.economics_marginal", "params": {}})

    # ── Cannibalization proximity signal ──
    nearest_m = _safe_float(candidate.get("distance_to_nearest_branch_m"))
    if nearest_m is not None and nearest_m > 0:
        nearest_km = nearest_m / 1000.0
        if nearest_km < 1.5:
            risks.append(
                f"Nearest own branch is only {nearest_km:.1f} km away \u2014 high overlap risk."
            )
            risks_structured.append({"id": "risk.nearest_branch_close", "params": {"nearest_km": nearest_km}})
        elif nearest_km > 5.0:
            positives.append(
                f"Well-separated from nearest branch ({nearest_km:.1f} km) \u2014 low overlap."
            )
            positives_structured.append({"id": "pos.well_separated_branch", "params": {"nearest_km": nearest_km}})

    # ── Competitor density signal ──
    competitor_count = _safe_int(candidate.get("competitor_count"))
    if competitor_count >= 8:
        risks.append(
            f"High competitor density ({competitor_count} nearby) \u2014 market may be saturated."
        )
        risks_structured.append({"id": "risk.high_competitor_density", "params": {"count": competitor_count}})
    elif competitor_count <= 2 and competitor_count >= 0:
        positives.append("Low same-category competitor density \u2014 potential first-mover advantage.")
        positives_structured.append({"id": "pos.low_competitor_density", "params": {}})

    # Phase 4 - listing recency + district momentum callouts.
    # Mirrors the badge/tag logic on the card so the rationale line matches
    # what the UI displays. Appended AFTER all other positive-emitting
    # logic in this function so these Phase 4 strings only claim
    # positives[0] on cards where no higher-priority rationale fired.
    # Thresholds mirror the frontend call site by convention; see
    # _LISTING_FRESHNESS_DAYS and _MOMENTUM_DISPLAY_THRESHOLD. English-only
    # for this patch - Arabic parity for _top_positives_and_risks is
    # tracked as a separate 3c item.
    #
    # Phase 4.1: read created_days and updated_days independently instead
    # of branching on the `source` tag from _effective_listing_age_days.
    # The GREATEST() winner there shadows aqar_created_at in ~93% of rows
    # due to scraper cadence, which would otherwise make "New" unreachable
    # even on genuinely new listings that have also been recently refreshed.
    # `effective_age_days` / `source` are retained in the snapshot for
    # memo/rerank back-compat but must NOT drive this pill logic.
    fs = candidate.get("feature_snapshot_json") or {}
    listing_age = fs.get("listing_age") or {}
    momentum = fs.get("district_momentum") or {}

    created_days = listing_age.get("created_days")
    updated_days = listing_age.get("updated_days")
    is_new = (
        isinstance(created_days, (int, float))
        and created_days <= _LISTING_FRESHNESS_DAYS
    )
    is_updated = (
        not is_new
        and isinstance(updated_days, (int, float))
        and updated_days <= _LISTING_FRESHNESS_DAYS
    )

    momentum_score = momentum.get("momentum_score")
    is_top_tier_market = (
        isinstance(momentum_score, (int, float))
        and float(momentum_score) >= _MOMENTUM_DISPLAY_THRESHOLD
        and momentum.get("sample_floor_applied") is False
    )

    if is_new and is_top_tier_market:
        positives.append("Newly listed in a top-tier market.")
        positives_structured.append({"id": "pos.new_in_top_market", "params": {}})
    elif is_updated and is_top_tier_market:
        positives.append("Recently refreshed listing in a top-tier market.")
        positives_structured.append({"id": "pos.refreshed_in_top_market", "params": {}})
    elif is_new:
        positives.append("Newly listed within the last week.")
        positives_structured.append({"id": "pos.newly_listed", "params": {}})
    elif is_updated:
        positives.append("Listing refreshed by the owner within the last week.")
        positives_structured.append({"id": "pos.refreshed_listing", "params": {}})
    elif is_top_tier_market:
        positives.append("District ranks in the top tier for recent listing activity.")
        positives_structured.append({"id": "pos.top_tier_market", "params": {}})

    return positives[:5], risks[:6], positives_structured[:5], risks_structured[:6]


def _confidence_grade(
    *,
    confidence_score: float,
    district: str | None,
    provider_platform_count: int | None,
    multi_platform_presence_score: float | None,
    rent_source: str,
    road_context_available: bool = True,
    parking_context_available: bool = True,
    zoning_available: bool = True,
    delivery_observed: bool = True,
    data_completeness_score: int | float = 0,
    is_listing: bool = False,
    road_evidence_band: str | None = None,
    parking_evidence_band: str | None = None,
) -> str:
    """Map a 0-100 confidence score to an A/B/C/D grade.

    For listings, the score already encodes data quality (measured rent,
    area, street width, image) — parcel-era critical-missing checks are
    irrelevant. Use the score directly.

    For parcels, the score is from district-context enrichment, so the
    critical-missing flags meaningfully indicate thin context.
    """
    adjusted = _safe_float(confidence_score)
    if district:
        adjusted += 2.5
    # Do not award a bonus merely because the field exists with value 0.0.
    if float(multi_platform_presence_score or 0.0) > 0:
        adjusted += 2.5
    if rent_source != "conservative_default":
        adjusted += 3.0

    def _band_missing(band: str | None) -> bool:
        if band is None:
            return True
        return band.strip().lower() in {"none", "none_found", "unknown"}

    if is_listing:
        # Listings: trust the score for the score-derived band, but let the
        # road and parking evidence bands gate the ceiling. When both bands
        # are missing we cap at B (not C) so the headline grade stays in
        # range of the score signal while preventing UI contradictions like
        # "Data: A" rendered next to "Road access evidence: None found /
        # Parking evidence: None found"; capping below B would amount to a
        # cohort re-grade, which the score itself does not justify.
        missing_bands = sum(
            1
            for band in (road_evidence_band, parking_evidence_band)
            if _band_missing(band)
        )
        if adjusted >= 85.0:
            grade = "A"
        elif adjusted >= 70.0:
            grade = "B"
        elif adjusted >= 50.0:
            grade = "C"
        else:
            grade = "D"
        if missing_bands >= 2 and grade == "A":
            grade = "B"
        return grade

    # Parcel path: legacy logic, unchanged.
    critical_missing = 0
    if not zoning_available:
        critical_missing += 1
    if not delivery_observed:
        critical_missing += 1
    if not road_context_available:
        critical_missing += 1
    if not parking_context_available:
        critical_missing += 1

    # Also factor in data completeness — default to 0 so missing
    # completeness never inflates the grade.
    completeness = _safe_float(data_completeness_score, 0)

    if adjusted >= 85.0 and critical_missing == 0 and completeness >= 85:
        return "A"
    if adjusted >= 70.0 and critical_missing <= 1:
        return "B"
    if adjusted >= 50.0:
        return "C"
    return "D"


def _build_demand_thesis(
    *,
    demand_score: float,
    population_reach: float,
    provider_density_score: float,
    provider_whitespace_score: float,
    delivery_competition_score: float,
    delivery_observed: bool = True,
) -> tuple[str, dict[str, Any]]:
    demand_label = "strong" if demand_score >= 70 else "moderate" if demand_score >= 50 else "limited"
    if not delivery_observed and provider_density_score > 0:
        # District-level fallback: real district data but no spatial-radius data
        provider_label = "district-level estimate" if provider_density_score >= 30 else "limited district data"
        whitespace_label = "district-inferred" if provider_whitespace_score >= 50 else "potentially tight (district-level)"
        competition_label = "district-level estimate"
        # PR #2a: locale-invariant token mirroring the English branch above.
        provider_token = "district_estimate" if provider_density_score >= 30 else "limited_district"
        whitespace_token = "district_inferred" if provider_whitespace_score >= 50 else "tight_district"
        competition_token = "district_estimate"
    elif not delivery_observed:
        # No delivery data at all — fully inferred
        provider_label = "not observed (inferred)"
        whitespace_label = "inferred whitespace opportunity"
        competition_label = "not directly observed"
        provider_token = "not_observed"
        whitespace_token = "inferred_opportunity"
        competition_token = "not_directly_observed"
    else:
        provider_label = "dense" if provider_density_score >= 65 else "steady" if provider_density_score >= 45 else "thin"
        whitespace_label = "attractive" if provider_whitespace_score >= 60 else "balanced" if provider_whitespace_score >= 40 else "tight"
        competition_label = "intense" if delivery_competition_score >= 65 else "manageable"
        provider_token = "dense" if provider_density_score >= 65 else "steady" if provider_density_score >= 45 else "thin"
        whitespace_token = "attractive" if provider_whitespace_score >= 60 else "balanced" if provider_whitespace_score >= 40 else "tight"
        competition_token = "intense" if delivery_competition_score >= 65 else "manageable"
    english = (
        f"Demand is {demand_label} (score {demand_score:.1f}) with population reach around {population_reach:.0f}; "
        f"provider activity is {provider_label}, whitespace is {whitespace_label}, and delivery competition is {competition_label}."
    )
    # PR #2a: structured record built from the same inputs as the English
    # string above. demand_label is itself a locale-invariant token.
    structured = {
        "id": "demand_thesis",
        "params": {
            "demand_score": demand_score,
            "population_reach": population_reach,
            "demand_label": demand_label,
            "provider_label": provider_token,
            "whitespace_label": whitespace_token,
            "competition_label": competition_token,
        },
    }
    return english, structured


def _build_cost_thesis(
    *,
    estimated_rent_sar_m2_year: float,
    estimated_annual_rent_sar: float,
    estimated_fitout_cost_sar: float,
) -> tuple[str, dict[str, Any]]:
    english = (
        f"Estimated rent is {estimated_rent_sar_m2_year:.0f} SAR/m²/year (~{estimated_annual_rent_sar:,.0f} SAR annually), "
        f"fit-out is ~{estimated_fitout_cost_sar:,.0f} SAR."
    )
    # PR #2a: structured record built from the same inputs as the English
    # string above. Numbers stored raw; PR #2b's template carries the format
    # spec so the English re-render is byte-identical.
    structured = {
        "id": "cost_thesis",
        "params": {
            "estimated_rent_sar_m2_year": estimated_rent_sar_m2_year,
            "estimated_annual_rent_sar": estimated_annual_rent_sar,
            "estimated_fitout_cost_sar": estimated_fitout_cost_sar,
        },
    }
    return english, structured


def _comparable_competitors(
    db: Session,
    *,
    category: str,
    lat: float | None,
    lon: float | None,
    ea_competitor_populated: bool | None = None,
) -> list[dict[str, Any]]:
    if lat is None or lon is None:
        return []

    # Prefer expansion_competitor_quality when populated
    if ea_competitor_populated is None:
        ea_competitor_populated = _ea_table_has_rows(db, _EA_COMPETITOR_TABLE)
    if ea_competitor_populated:
        try:
            with db.begin_nested():
                rows = db.execute(
                    text(f"""
                        WITH candidate_point AS (
                            SELECT ST_SetSRID(ST_MakePoint(:lon, :lat), 4326) AS geom
                        ),
                        ranked AS (
                            SELECT
                                ecq.restaurant_poi_id AS id,
                                ecq.brand_name AS name,
                                ecq.category,
                                ecq.district,
                                ecq.review_score / 20.0 AS rating,
                                ecq.review_count,
                                'expansion_competitor_quality' AS source,
                                ecq.overall_quality_score,
                                ecq.canonical_brand_id,
                                ecq.display_name_en,
                                ecq.display_name_ar,
                                ST_Distance(ecq.geom::geography, cp.geom::geography) AS distance_m,
                                COALESCE(ecq.canonical_brand_id, 'poi:' || ecq.restaurant_poi_id::text) AS dedup_key
                            FROM {_EA_COMPETITOR_TABLE} ecq
                            CROSS JOIN candidate_point cp
                            WHERE ecq.geom IS NOT NULL
                              AND lower(COALESCE(ecq.category, '')) = lower(:category)
                              AND ST_DWithin(ecq.geom::geography, cp.geom::geography, 1500)
                        )
                        SELECT DISTINCT ON (dedup_key)
                            id, name, category, district, rating, review_count,
                            source, overall_quality_score, canonical_brand_id,
                            display_name_en, display_name_ar, distance_m
                        FROM ranked
                        ORDER BY dedup_key, distance_m ASC
                    """),
                    {"lat": lat, "lon": lon, "category": category},
                ).mappings().all()
            if rows:
                rows = sorted(rows, key=lambda r: r.get("distance_m") or 0.0)[:5]
                return [
                    {
                        "id": row.get("id"),
                        "name": row.get("name"),
                        "category": row.get("category"),
                        "district": row.get("district"),
                        "rating": _safe_float(row.get("rating"), default=0.0) if row.get("rating") is not None else None,
                        "review_count": _safe_int(row.get("review_count"), default=0) if row.get("review_count") is not None else None,
                        "distance_m": round(_safe_float(row.get("distance_m"), default=0.0), 2),
                        "source": row.get("source"),
                        "overall_quality_score": _safe_float(row.get("overall_quality_score")),
                        "canonical_brand_id": row.get("canonical_brand_id"),
                        "display_name_en": row.get("display_name_en"),
                        "display_name_ar": row.get("display_name_ar"),
                    }
                    for row in rows
                ]
        except Exception:
            logger.debug("expansion_competitor_quality query failed, falling back to restaurant_poi", exc_info=True)

    # Fallback: legacy restaurant_poi query
    try:
        with db.begin_nested():
            rows = db.execute(
                text(
                    """
                    WITH candidate_point AS (
                        SELECT ST_SetSRID(ST_MakePoint(:lon, :lat), 4326) AS geom
                    ),
                    poi_base AS (
                        SELECT
                            rp.id,
                            rp.name,
                            rp.category,
                            rp.district,
                            rp.rating,
                            rp.review_count,
                            rp.source,
                            COALESCE(
                                rp.geom,
                                CASE
                                    WHEN rp.lon IS NOT NULL AND rp.lat IS NOT NULL THEN ST_SetSRID(ST_MakePoint(rp.lon, rp.lat), 4326)
                                    ELSE NULL
                                END
                            ) AS poi_geom
                        FROM restaurant_poi rp
                        WHERE lower(COALESCE(rp.category, '')) = lower(:category)
                    )
                    SELECT
                        p.id,
                        p.name,
                        p.category,
                        p.district,
                        p.rating,
                        p.review_count,
                        p.source,
                        ST_Distance(p.poi_geom::geography, cp.geom::geography) AS distance_m
                    FROM poi_base p
                    CROSS JOIN candidate_point cp
                    WHERE p.poi_geom IS NOT NULL
                      AND ST_DWithin(p.poi_geom::geography, cp.geom::geography, 1500)
                    ORDER BY distance_m ASC
                    LIMIT 5
                    """
                ),
                {"lat": lat, "lon": lon, "category": category},
            ).mappings().all()
    except Exception:
        logger.warning("comparable_competitors query failed for category=%s lat=%s lon=%s", category, lat, lon, exc_info=True)
        return []

    return [
        {
            "id": row.get("id"),
            "name": row.get("name"),
            "category": row.get("category"),
            "district": row.get("district"),
            "rating": _safe_float(row.get("rating"), default=0.0) if row.get("rating") is not None else None,
            "review_count": _safe_int(row.get("review_count"), default=0) if row.get("review_count") is not None else None,
            "distance_m": round(_safe_float(row.get("distance_m"), default=0.0), 2),
            "source": row.get("source"),
        }
        for row in rows
    ]


def _nearest_branch_distance_m(lat: float, lon: float, existing_branches: list[dict[str, Any]]) -> float | None:
    if not existing_branches:
        return None
    nearest: float | None = None
    for branch in existing_branches:
        branch_lat = _safe_float(branch.get("lat"), default=float("nan"))
        branch_lon = _safe_float(branch.get("lon"), default=float("nan"))
        if branch_lat != branch_lat or branch_lon != branch_lon:
            continue
        dx = branch_lon - lon
        dy = branch_lat - lat
        # Fast deterministic approximation for Riyadh-scale distances.
        dist_m = (((dx * 101200.0) ** 2) + ((dy * 111320.0) ** 2)) ** 0.5
        if nearest is None or dist_m < nearest:
            nearest = dist_m
    return nearest


def _cannibalization_score(distance_m: float | None, service_model: str) -> float:
    """Continuous exponential-decay cannibalization risk.
    Returns 0-100 where higher = more cannibalization risk.
    Uses a smooth curve so that every candidate gets a distinct score,
    enabling meaningful ranking differentiation.
    """
    if distance_m is None:
        # No existing branches — zero cannibalization risk.
        return 0.0
    # Service-model-specific parameters:
    #   half_life_m  — distance at which risk drops to 50% of maximum
    #   ceiling      — maximum risk score at distance=0
    params = {
        "qsr":            {"half_life_m": 1200.0, "ceiling": 82.0},
        "cafe":           {"half_life_m": 1000.0, "ceiling": 80.0},
        "delivery_first": {"half_life_m":  800.0, "ceiling": 78.0},
        "dine_in":        {"half_life_m": 1800.0, "ceiling": 92.0},
    }
    p = params.get(service_model, {"half_life_m": 1400.0, "ceiling": 85.0})
    half_life = p["half_life_m"]
    ceiling = p["ceiling"]
    # Exponential decay: risk = ceiling * 2^(-distance / half_life)
    # At distance=0 → ceiling, at distance=half_life → ceiling/2,
    # at distance=2*half_life → ceiling/4, etc.
    decay = math.pow(2.0, -distance_m / half_life)
    base = ceiling * decay
    # Extra overlap penalty for delivery-first when extremely close
    if service_model == "delivery_first" and distance_m < 400:
        base += 7.0 * (1.0 - distance_m / 400.0)
    return _clamp(base)


def _build_explanation(
    *,
    area_m2: float,
    population_reach: float,
    competitor_count: int,
    delivery_listing_count: int,
    landuse_label: str | None,
    landuse_code: str | None,
    cannibalization_score: float,
    distance_to_nearest_branch_m: float | None,
    economics_score: float,
    estimated_rent_sar_m2_year: float,
    estimated_annual_rent_sar: float,
    estimated_fitout_cost_sar: float,
    estimated_revenue_index: float,
    rent_source: str,
    final_score: float,
) -> dict[str, Any]:
    positives: list[str] = []
    risks: list[str] = []

    if population_reach >= 12000:
        positives.append("Strong surrounding population reach")
    elif population_reach >= 7000:
        positives.append("Healthy surrounding population reach")

    if delivery_listing_count >= 15:
        positives.append("Good delivery-market activity nearby")

    if competitor_count <= 3:
        positives.append("Relatively open competitive whitespace")
    elif competitor_count >= 8:
        risks.append("Dense same-category competition nearby")

    if landuse_label:
        positives.append(f"ArcGIS land-use label available: {landuse_label}")
    else:
        risks.append("Weak parcel land-use labeling")

    if area_m2 < 100:
        risks.append("Small parcel footprint for larger branch formats")
    elif area_m2 > 600:
        risks.append("Parcel may be oversized for lean branch formats")

    if distance_to_nearest_branch_m is None:
        positives.append("No existing branches — cannibalization risk is zero")
    elif distance_to_nearest_branch_m < 1000:
        risks.append("Very close to an existing branch (high cannibalization risk)")
    elif distance_to_nearest_branch_m <= 2500:
        risks.append("Moderate overlap risk with existing branch coverage")
    else:
        positives.append("Healthy spacing from existing branch network")

    return {
        "summary": f"Candidate scored {final_score:.1f}/100 using ArcGIS parcel fit, demand, whitespace, confidence, and cannibalization.",
        "positives": positives,
        "risks": risks,
        "inputs": {
            "area_m2": area_m2,
            "population_reach": population_reach,
            "competitor_count": competitor_count,
            "delivery_listing_count": delivery_listing_count,
            "landuse_label": landuse_label,
            "landuse_code": landuse_code,
            "cannibalization_score": cannibalization_score,
            "distance_to_nearest_branch_m": distance_to_nearest_branch_m,
            "economics_score": economics_score,
            "estimated_rent_sar_m2_year": estimated_rent_sar_m2_year,
            "estimated_annual_rent_sar": estimated_annual_rent_sar,
            "estimated_fitout_cost_sar": estimated_fitout_cost_sar,
            "estimated_revenue_index": estimated_revenue_index,
            "rent_source": rent_source,
        },
    }


def _road_signal_from_context(road_context: dict | None) -> float:
    """Compute a normalized road-quality signal in [0, 1] from bulk_roads data.

    The signal blends two components:
      - touches_road (70% weight): binary, captures whether the candidate
        has direct street frontage.
      - arterial proximity (30% weight): distance to nearest major road,
        normalized so 0m -> 1.0, 500m+ -> 0.0.

    Returns 0.5 (neutral) when road_context is missing, so candidates
    without enrichment data are not penalized. A distance at or above
    _ROAD_DISTANCE_SENTINEL_M is also treated as unknown, since it is the
    COALESCE fallback emitted when no major road was found within the
    700 m search radius — not a real measurement.
    """
    if not road_context:
        return 0.5

    touches = bool(road_context.get("touches_road"))
    distance_m = road_context.get("nearest_major_road_distance_m")

    touches_component = 1.0 if touches else 0.0

    if distance_m is None:
        distance_component = 0.5
    else:
        try:
            d = float(distance_m)
            if d >= _ROAD_DISTANCE_SENTINEL_M:
                distance_component = 0.5
            elif d <= 0:
                distance_component = 1.0
            elif d >= 500:
                distance_component = 0.0
            else:
                distance_component = 1.0 - (d / 500.0)
        except (TypeError, ValueError):
            distance_component = 0.5

    return round(touches_component * 0.70 + distance_component * 0.30, 4)


def _rent_micro_location_multiplier(
    *,
    provider_listing_count: int,
    delivery_competition_count: int,
    population_reach: float,
    competitor_count: int,
    district_delivery_stats: dict | None = None,
    city_benchmarks: dict | None = None,
    road_context: dict | None = None,
) -> tuple[float, dict]:
    """Compute a per-parcel rent multiplier based on local commercial activity.

    Uses delivery density, population, and competition as proxies for
    micro-location rent variation within a district. Returns a multiplier
    in [0.70, 1.35] and a metadata dict for observability.

    Signals:
    - Delivery density (provider_listing_count): more nearby restaurants
      = higher commercial activity = rent premium
    - Population reach: higher population = more foot traffic = premium
    - Competition count: more same-category competitors = commercial
      corridor = premium
    - District delivery stats: parcel's local density vs district average
      provides relative positioning within the district

    All signals are normalized to [0, 1] and blended into a composite
    that maps to the multiplier range.
    """
    meta: dict = {}

    # Normalize each signal to [0, 1]
    # Delivery density: 0 listings → 0.0, 30+ listings → 1.0
    density_signal = min(1.0, provider_listing_count / 30.0)

    # Population: 0 → 0.0, 50K+ → 1.0
    pop_signal = min(1.0, population_reach / 50000.0)

    # Competition: 0 → 0.0, 8+ same-category → 1.0
    comp_signal = min(1.0, competitor_count / 8.0)

    # Category competition from delivery: 0 → 0.0, 15+ → 1.0
    cat_comp_signal = min(1.0, delivery_competition_count / 15.0)

    # District-relative signal: if we know the district's average delivery
    # density, measure how this parcel compares. Above average → premium,
    # below average → discount.
    district_relative = 0.5  # neutral default
    if district_delivery_stats and district_delivery_stats.get("total", 0) > 0:
        district_avg_per_parcel = district_delivery_stats["total"] / max(1, district_delivery_stats.get("total", 1))
        # Compare parcel's listing count to district average
        # (district total is all restaurants; parcel count is within 1.2km)
        # Typical district has 50-300 restaurants, parcel radius sees 5-50
        district_density_proxy = district_delivery_stats["total"]
        if city_benchmarks and city_benchmarks.get("median_total", 0) > 0:
            # How dense is this district relative to city median?
            district_vs_city = district_density_proxy / city_benchmarks["median_total"]
            # Parcel's local density vs district: above-average parcel in
            # above-average district = double premium signal
            parcel_vs_district = provider_listing_count / max(1, district_density_proxy * 0.05)
            district_relative = min(1.0, max(0.0,
                (district_vs_city * 0.4 + min(2.0, parcel_vs_district) * 0.3) / 1.4
            ))

    # Road signal from bulk_roads enrichment (touches_road + arterial distance)
    road_signal = _road_signal_from_context(road_context)

    # Blend signals into composite score [0, 1]
    # Weights reduced proportionally to make room for road_signal (0.20)
    composite = (
        density_signal * 0.28
        + pop_signal * 0.16
        + comp_signal * 0.12
        + cat_comp_signal * 0.12
        + district_relative * 0.12
        + road_signal * 0.20
    )

    # Map composite [0, 1] → multiplier [0.70, 1.35]
    # 0.0 → 0.70 (quiet residential side street)
    # 0.5 → 1.025 (roughly district average)
    # 1.0 → 1.35 (prime commercial corridor)
    multiplier = 0.70 + composite * 0.65

    meta = {
        "density_signal": round(density_signal, 3),
        "pop_signal": round(pop_signal, 3),
        "comp_signal": round(comp_signal, 3),
        "cat_comp_signal": round(cat_comp_signal, 3),
        "district_relative": round(district_relative, 3),
        "road_signal": round(road_signal, 3),
        "composite": round(composite, 3),
        "multiplier": round(multiplier, 3),
    }

    return round(multiplier, 4), meta


def _estimate_rent_from_expansion_table(db: Session, district: str | None) -> tuple[float, str] | None:
    """Try to get rent estimate from the normalized expansion_rent_comp table.

    Uses commercial/retail rents for F&B location scoring.
    Fallback chain: retail district → commercial district → retail city → commercial city.

    English-bound callers (e.g. commercial_unit candidates whose neighborhood
    field is the Aqar English label) are translated to the Arabic norm-key
    via the AR↔EN crosswalk before the join. The SQL WHERE clause additionally
    applies ``normalize_district_key`` semantics on both sides so حي-prefix
    and alef/ya variants on the column side don't break the match.
    """
    try:
        with db.begin_nested():
            has_rows = db.execute(
                text(f"SELECT EXISTS(SELECT 1 FROM {_EA_RENT_TABLE} WHERE city = 'riyadh' LIMIT 1)")
            ).scalar()
            if not has_rows:
                return None

            # Resolve EN→AR via the district crosswalk; pass-through on miss
            # so callers that already supply Arabic keep working.
            district_normalized: str | None = None
            if district:
                lookup = _cached_district_lookup(db)
                resolved = _resolve_district_to_ar_key(district, lookup)
                if resolved:
                    district_normalized = resolved
                    if normalize_district_key(district) == resolved:
                        logger.debug("rent_lookup: AR direct match for %s", district)
                    else:
                        logger.debug("rent_lookup: EN→AR match %s → %s", district, resolved)
                else:
                    district_normalized = normalize_district_key(district) or district
                    logger.debug(
                        "rent_lookup: no district resolution for %s, using pass-through",
                        district,
                    )

            district_where = (
                f"AND LOWER({normalize_district_key_sql('district')}) = LOWER(:district_normalized)"
            )

            # Filters to try in priority order: narrowest (retail + district) to broadest (commercial + city)
            filters = []
            if district and district_normalized:
                filters.append((
                    f"{district_where} AND asset_type = 'commercial' AND unit_type = 'retail'",
                    {"district_normalized": district_normalized},
                    3,
                    "expansion_rent_district_retail",
                ))
                filters.append((
                    f"{district_where} AND asset_type = 'commercial'",
                    {"district_normalized": district_normalized},
                    3,
                    "expansion_rent_district_commercial",
                ))
            filters.append(("AND asset_type = 'commercial' AND unit_type = 'retail'", {}, 0, "expansion_rent_city_retail"))
            filters.append(("AND asset_type = 'commercial'", {}, 0, "expansion_rent_city_commercial"))

            for where_clause, params, min_n, source_label in filters:
                row = db.execute(
                    text(f"""
                        SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rent_sar_m2_year) AS median,
                               COUNT(*) AS n
                        FROM {_EA_RENT_TABLE}
                        WHERE city = 'riyadh'
                          AND rent_sar_m2_year IS NOT NULL
                          AND rent_sar_m2_year > 0
                          {where_clause}
                    """),
                    params,
                ).mappings().first()
                if row and row["median"] is not None and int(row["n"]) >= max(min_n, 1):
                    return float(row["median"]), source_label

    except Exception:
        logger.debug("expansion_rent_comp query failed for district=%s", district, exc_info=True)
    return None


def _estimate_rent_sar_m2_year(db: Session, district: str | None) -> tuple[float, str]:
    # Prefer normalized Expansion Advisor rent comps when populated
    ea_result = _estimate_rent_from_expansion_table(db, district)
    if ea_result is not None:
        return ea_result

    try:
        # Use a SAVEPOINT so that a failed ORM query inside aqar_rent_median
        # does not corrupt the outer transaction (which would cause every
        # subsequent db.execute() to raise InFailedSqlTransaction).
        with db.begin_nested():
            result = aqar_rent_median(
                db,
                city=_EXPANSION_CITY,
                district=district,
                asset_type=_EXPANSION_AQAR_ASSET,
                unit_type=_EXPANSION_AQAR_UNIT,
                since_days=730,
            )
        if result.district_median is not None and result.n_district >= 5:
            return float(result.district_median) * 12.0, "aqar_district"
        if result.district_median is not None and result.n_district > 0 and result.city_median is not None:
            district_weight = min(1.0, result.n_district / 5.0)
            blended = float(result.district_median) * district_weight + float(result.city_median) * (1.0 - district_weight)
            return blended * 12.0, "aqar_district_shrinkage"
        if result.city_median is not None:
            return float(result.city_median) * 12.0, "aqar_city"
        if result.city_asset_median is not None:
            return float(result.city_asset_median) * 12.0, "aqar_city_asset"
    except Exception:
        logger.warning(
            "aqar_rent_median failed for district=%s; falling back to default",
            district,
            exc_info=True,
        )
    return _EXPANSION_DEFAULT_RENT_SAR_M2_YEAR, "conservative_default"


# Furnished commercial units arrive with kitchen equipment, finished
# walls/floors/ceiling, working HVAC, and an existing facade. The
# operator typically only needs branding, light refurbishment, and
# equipment top-up rather than a full bare-shell buildout. The standard
# discount range is 30-40%; we use the midpoint.
_FURNISHED_FITOUT_DISCOUNT = 0.35


def _estimate_fitout_cost_sar(
    area_m2: float,
    service_model: str,
    *,
    is_furnished: bool = False,
) -> float:
    cost_per_m2 = {
        "delivery_first": 1900.0,
        "qsr": 2600.0,
        "cafe": 2800.0,
        "dine_in": 3600.0,
    }.get(service_model, 2600.0)
    base_cost = max(0.0, area_m2 * cost_per_m2)
    if is_furnished:
        return base_cost * (1.0 - _FURNISHED_FITOUT_DISCOUNT)
    return base_cost


# Implied average check (SAR) by price_tier × category.
# Used as a ticket-size multiplier in revenue estimation.
# Sources: Riyadh F&B market norms, 2024-2025 aggregated ranges.
_IMPLIED_CHECK_SAR: dict[str, dict[str, float]] = {
    "value": {
        "burger": 30.0,
        "shawarma": 22.0,
        "fried_chicken": 28.0,
        "coffee": 18.0,
        "cafe": 25.0,
        "pizza": 30.0,
        "sandwich": 22.0,
        "healthy": 32.0,
        "grills": 40.0,
        "indian": 30.0,
        "asian": 32.0,
        "_default": 28.0,
    },
    "mid": {
        "burger": 55.0,
        "shawarma": 38.0,
        "fried_chicken": 45.0,
        "coffee": 35.0,
        "cafe": 48.0,
        "pizza": 50.0,
        "sandwich": 40.0,
        "healthy": 55.0,
        "grills": 70.0,
        "indian": 55.0,
        "asian": 58.0,
        "_default": 50.0,
    },
    "premium": {
        "burger": 95.0,
        "shawarma": 65.0,
        "fried_chicken": 70.0,
        "coffee": 60.0,
        "cafe": 80.0,
        "pizza": 85.0,
        "sandwich": 65.0,
        "healthy": 90.0,
        "grills": 130.0,
        "indian": 100.0,
        "asian": 110.0,
        "_default": 85.0,
    },
}
_IMPLIED_CHECK_BASELINE_SAR = 50.0  # neutral midpoint when tier is unset


def _implied_average_check(price_tier: str | None, category: str | None) -> float:
    """Return implied average check SAR from price tier and category."""
    tier = (price_tier or "").lower().strip()
    cat = (category or "").lower().strip()
    tier_map = _IMPLIED_CHECK_SAR.get(tier)
    if not tier_map:
        return _IMPLIED_CHECK_BASELINE_SAR
    if cat in tier_map:
        return tier_map[cat]
    for key, val in tier_map.items():
        if key != "_default" and key in cat:
            return val
    return tier_map.get("_default", _IMPLIED_CHECK_BASELINE_SAR)


# Category throughput multipliers — high-frequency F&B categories have
# higher average transaction velocity than their demand score implies.
_CATEGORY_THROUGHPUT: dict[str, float] = {
    "burger": 1.10,
    "shawarma": 1.12,
    "fried chicken": 1.10,
    "coffee": 1.08,
    "cafe": 1.05,
    "pizza": 1.07,
    "sandwich": 1.06,
    "healthy": 0.95,   # lower average ticket velocity
    "grills": 0.92,    # slower table turns / dine-in focused
}


def _category_throughput_factor(category: str | None) -> float:
    if not category:
        return 1.0
    cat_lower = (category or "").lower().strip()
    for key, factor in _CATEGORY_THROUGHPUT.items():
        if key in cat_lower:
            return factor
    return 1.0


def _estimate_revenue_index(
    *,
    # Primary listing features (drive 70% of base)
    area_m2: float,
    target_area_m2: float | None = None,
    unit_street_width_m: float | None = None,
    unit_listing_type: str | None = None,
    # District context (drives 30% of base, soft modifier)
    demand_score: float = 50.0,
    whitespace_score: float = 50.0,
    # Multipliers (unchanged from original)
    category: str | None = None,
    price_tier: str | None = None,
    # Legacy parcel inputs (preserved as fallbacks; do not contribute when
    # listing fields are present)
    delivery_listing_count: int = 0,
    population_reach: float = 0.0,
    road_context: dict | None = None,
    return_detail: bool = False,
) -> float | tuple[float, dict[str, float]]:
    """Listings-grounded revenue index.

    Primary inputs: listing-level features (street width as drive-by
    traffic proxy, area as throughput capacity, listing_type as
    visibility signal). District demand/whitespace are secondary
    soft modifiers, not primary drivers.

    For parcels: street width and listing_type fall to neutral defaults
    and the district inputs effectively dominate, preserving legacy
    behavior for the parcel path.

    With return_detail=True, returns ``(index, detail)`` where detail
    carries ``tier_blind_index`` — clamp(base × category_factor), i.e.
    everything except the price-tier-driven ticket multiplier — and the
    excluded ``ticket_multiplier``. The tier-blind index is the basis for
    value_score, which must stay tier-blind (see _RENT_CEILING_TIER_MULT
    note). The scalar return path is byte-identical to the legacy behavior.
    """
    # Street width as drive-by traffic proxy (35% of base)
    if unit_street_width_m is not None and unit_street_width_m > 0:
        street_signal = _frontage_score_from_street_width(float(unit_street_width_m))
    else:
        street_signal = 50.0

    # Area as throughput capacity (20% of base).
    # The brief specifies target_area_m2 — the operator's stated ideal
    # branch size. Score the listing by how close it is to that target,
    # not against a hardcoded QSR-shaped sweet spot. Listings within ±20%
    # of target get full credit; the curve tapers gently in either
    # direction so candidates that are close-but-not-perfect still rank
    # well.
    #
    # Falls back to the hardcoded QSR sweet spot (centered on 225 m²)
    # when target_area_m2 is missing — preserves legacy behavior for any
    # caller that doesn't pass the new parameter.
    _target = float(target_area_m2) if target_area_m2 and target_area_m2 > 0 else 225.0
    if area_m2 <= 0:
        area_signal = 50.0
    else:
        ratio = area_m2 / _target
        if 0.80 <= ratio <= 1.20:
            area_signal = 100.0
        elif 0.60 <= ratio < 0.80:
            # Tapering down from 100 to 80 as ratio drops 0.80 → 0.60
            area_signal = 80.0 + (ratio - 0.60) / 0.20 * 20.0
        elif 1.20 < ratio <= 1.50:
            # Tapering down from 100 to 80 as ratio rises 1.20 → 1.50
            area_signal = 100.0 - (ratio - 1.20) / 0.30 * 20.0
        elif 0.40 <= ratio < 0.60:
            area_signal = 55.0 + (ratio - 0.40) / 0.20 * 25.0
        elif 1.50 < ratio <= 2.00:
            area_signal = 80.0 - (ratio - 1.50) / 0.50 * 25.0
        elif 0.25 <= ratio < 0.40:
            area_signal = 35.0 + (ratio - 0.25) / 0.15 * 20.0
        elif 2.00 < ratio <= 3.00:
            area_signal = 55.0 - (ratio - 2.00) / 1.00 * 20.0
        else:
            # Either way too small or way too big.
            area_signal = 25.0

    # Listing type signal (15% of base)
    # Showrooms typically sit on corners with better visibility than
    # interior store units.
    lt = (unit_listing_type or "").lower()
    if lt == "showroom":
        type_signal = 80.0
    elif lt == "store":
        type_signal = 65.0
    else:
        type_signal = 50.0

    # District demand as soft modifier (20% of base)
    demand_signal = _safe_float(demand_score, 50.0)

    # District whitespace as soft modifier (10% of base)
    whitespace_signal = _safe_float(whitespace_score, 50.0)

    base = _clamp(
        street_signal * 0.35
        + area_signal * 0.20
        + type_signal * 0.15
        + demand_signal * 0.20
        + whitespace_signal * 0.10
    )

    # Category throughput factor (preserved from original)
    factor = max(0.88, min(1.12, _category_throughput_factor(category)))

    # Ticket-size multiplier (preserved from original)
    implied_check = _implied_average_check(price_tier, category)
    ticket_multiplier = max(0.5, min(2.5, implied_check / _IMPLIED_CHECK_BASELINE_SAR))

    index = _clamp(base * factor * ticket_multiplier)
    if return_detail:
        # Category throughput stays in the tier-blind index: it is
        # category-driven and constant within a search, not a tier leak.
        return index, {
            "tier_blind_index": _clamp(base * factor),
            "ticket_multiplier": ticket_multiplier,
        }
    return index


# ---------------------------------------------------------------------------
# Percentile-based rent burden helpers
# ---------------------------------------------------------------------------

# Sanity bounds for comparable rent rows in SAR/m²/month.
# Rows outside this range are excluded from the percentile comparable set
# because they are overwhelmingly scraper artifacts (multi-year lease totals
# stored as annual, whole-building listings, parsing bugs, etc.) rather than
# real F&B-comparable rents in the Riyadh retail market.
#
# Floor (15): below this, listings are almost always multi-year totals,
#             whole-building listings, or far-suburb storefronts that aren't
#             realistic F&B comparables.
# Ceiling (350): above this, listings are scraper bugs — prime Riyadh F&B
#                retail tops out around 250 SAR/m²/month even on Tahlia.
_RENT_COMP_MIN_SAR_M2_MONTH: float = 15.0
_RENT_COMP_MAX_SAR_M2_MONTH: float = 350.0

# Maximum area for a comparable listing (m²).  Rows above this are typically
# whole-building or land-plot listings whose per-m² rate is not representative
# of a typical F&B retail unit.
_RENT_COMP_MAX_AREA_SQM: float = 1000.0

# Property types that should never appear in the comparable set.  These are
# structurally incompatible with F&B retail rents and distort the percentile
# distribution when included.
_RENT_COMP_EXCLUDED_PROPERTY_TYPES: tuple[str, ...] = (
    "warehouse",
    "building",
    "land",
    "rest_house",
    "farm",
)

# Area bands (m²) used to bucket comparable listings for rent percentiles.
_RENT_COMP_AREA_BANDS: list[tuple[float, float]] = [
    (0, 100),
    (100, 200),
    (200, 400),
    (400, 800),
    (800, 1e9),
]


def _area_band_bounds(area_m2: float) -> tuple[float, float]:
    for lo, hi in _RENT_COMP_AREA_BANDS:
        if lo <= area_m2 < hi:
            return lo, hi
    return _RENT_COMP_AREA_BANDS[-1]


def _rent_burden_confidence(source_label: str | None, n_comparable: int | None) -> float:
    """Confidence multiplier for rent_burden's 20% weight in the economics composite.

    Narrow fix: only damp the specific pathology where _percentile_rent_burden
    silently falls back to a citywide comp pool but the caller treats it as a
    real district hit. All other paths (district hits, envelope flags,
    absolute_legacy, absolute_fallback, unknown labels, missing metadata)
    preserve full weight to avoid unintended behavior changes.
    """
    if source_label is None:
        return 1.0  # preserve legacy behavior — no damping

    n = int(n_comparable) if n_comparable is not None else 0

    if source_label in ("district_band_type", "district_type", "district"):
        # District tiers self-enforce min_n inside _percentile_rent_burden;
        # if one of these labels is present, n should already be >= 8.
        return 1.0

    if source_label == "city_band_type":
        return 0.25 if n >= 12 else 0.0
    if source_label == "city":
        return 0.15 if n >= 20 else 0.0

    # Unknown / envelope / absolute paths: preserve full weight.
    return 1.0


def _is_plausible_neighborhood(value: str | None) -> bool:
    """Reject scraper-garbage neighborhood values that cause false-positive
    comp pool matches. Currently observed garbage in commercial_unit:
    pure-digit strings like "2", "3", "4" (87 rows across 600 active
    listings). The conservative rule: require at least one non-digit,
    non-whitespace character AND >=3 chars after strip.
    """
    if not value:
        return False
    stripped = value.strip()
    if len(stripped) < 3:
        return False
    if stripped.isdigit():
        return False
    return True


def _percentile_rent_burden(
    db: Session,
    *,
    listing_monthly_rent_per_m2: float,
    district: str | None,
    area_m2: float,
    listing_type: str | None = None,
    unit_neighborhood_raw: str | None = None,
    cand_age_days: int | None = None,
) -> dict[str, Any] | None:
    """Score a listing's rent/m² against comparable real listings.

    Returns a dict with burden_score, percentile, n_comparable,
    source_label, median_monthly_rent_per_m2, listing_monthly_rent_per_m2.
    Returns None when no comparable cell meets the minimum N threshold.

    When ``cand_age_days`` (the candidate's age on the ORIGINAL-listing-date
    basis — created_at with first_seen_at as a null-guard floor; see
    ``_created_basis_age_days``) is supplied, the same comparable aggregate
    also yields ``age_percentile`` = share of comparables AS OLD OR OLDER than
    the candidate (HIGH = old relative to peers) and ``n_comparables`` (the n
    it was computed over). Both are emitted only off the >= min-N comparable
    set the rent percentile uses; ``age_percentile`` is null when
    ``cand_age_days`` is None.
    """
    if listing_monthly_rent_per_m2 <= 0 or area_m2 <= 0:
        return None

    # Listing rate falls outside the defensible Riyadh F&B retail envelope.
    # Don't compute a percentile against a bounded population that doesn't
    # contain the listing's own rate — return a neutral / penalized burden
    # score with a clear meta flag instead.
    if listing_monthly_rent_per_m2 < _RENT_COMP_MIN_SAR_M2_MONTH:
        return {
            "burden_score": 50.0,
            "percentile": None,
            "n_comparable": 0,
            "source_label": "listing_below_envelope",
            "median_monthly_rent_per_m2": None,
            "listing_monthly_rent_per_m2": round(float(listing_monthly_rent_per_m2), 2),
            "comparable_bounds": {
                "min_sar_m2_month": _RENT_COMP_MIN_SAR_M2_MONTH,
                "max_sar_m2_month": _RENT_COMP_MAX_SAR_M2_MONTH,
                "max_area_sqm": _RENT_COMP_MAX_AREA_SQM,
                "excluded_property_types": list(_RENT_COMP_EXCLUDED_PROPERTY_TYPES),
            },
            "note": "listing rent below sanity envelope — likely data quality issue, neutral burden assigned",
        }
    if listing_monthly_rent_per_m2 > _RENT_COMP_MAX_SAR_M2_MONTH:
        return {
            "burden_score": 15.0,
            "percentile": None,
            "n_comparable": 0,
            "source_label": "listing_above_envelope",
            "median_monthly_rent_per_m2": None,
            "listing_monthly_rent_per_m2": round(float(listing_monthly_rent_per_m2), 2),
            "comparable_bounds": {
                "min_sar_m2_month": _RENT_COMP_MIN_SAR_M2_MONTH,
                "max_sar_m2_month": _RENT_COMP_MAX_SAR_M2_MONTH,
                "max_area_sqm": _RENT_COMP_MAX_AREA_SQM,
                "excluded_property_types": list(_RENT_COMP_EXCLUDED_PROPERTY_TYPES),
            },
            "note": "listing rent above sanity envelope — heavy burden penalty assigned",
        }

    band_lo, band_hi = _area_band_bounds(area_m2)
    district_norm = normalize_district_key(district) if district else None

    # Build a SQL-safe literal for the excluded property types.
    _excluded_pt_sql = ", ".join(
        f"'{pt}'" for pt in _RENT_COMP_EXCLUDED_PROPERTY_TYPES
    )

    base_where = f"""
        FROM commercial_unit
        WHERE restaurant_suitable = true
          AND price_sar_annual IS NOT NULL
          AND price_sar_annual > 0
          AND area_sqm IS NOT NULL
          AND area_sqm > 0
          AND status = 'active'
          AND (price_sar_annual / area_sqm / 12.0) BETWEEN :rent_floor AND :rent_ceiling
          AND area_sqm <= :max_comp_area
          AND (property_type IS NULL OR lower(property_type) NOT IN ({_excluded_pt_sql}))
    """

    # Fallback chain: narrowest → broadest.
    # Each entry: (extra_where, params, min_n, label)
    chains: list[tuple[str, dict[str, Any], int, str]] = []

    # District tier match: prefer the listing's own English neighborhood
    # string from Aqar (commercial_unit.neighborhood) when available. The
    # Arabic-normalized district key never matches the English neighborhood
    # values stored on commercial_unit rows, so the legacy district_norm
    # match silently returned zero rows for every lookup and every burden
    # was computed against a citywide comparable set. The
    # unit_neighborhood_raw value is in the same English namespace as the
    # comparable set's neighborhood column, so the match works directly.
    neighborhood_match_value: str | None = None
    if unit_neighborhood_raw and _is_plausible_neighborhood(unit_neighborhood_raw):
        neighborhood_match_value = unit_neighborhood_raw.strip().lower()
    elif district_norm and _is_plausible_neighborhood(district_norm):
        # Fallback: try the Arabic-normalized district, which only matches
        # the rare commercial_unit rows whose neighborhood happens to be
        # stored in Arabic. Almost always returns zero, but cheap to try.
        neighborhood_match_value = district_norm
    # If both fail the plausibility check, neighborhood_match_value stays None;
    # the district tier chains short-circuit and the function falls to the
    # city_band_type tier (correctly damped at confidence 0.25 by PR #1114).

    if neighborhood_match_value:
        chains.append((
            "AND lower(neighborhood) = :neighborhood AND area_sqm >= :band_lo AND area_sqm < :band_hi AND listing_type = :ltype",
            {"neighborhood": neighborhood_match_value, "band_lo": band_lo, "band_hi": band_hi, "ltype": listing_type or "store"},
            8,
            "district_band_type",
        ))
        chains.append((
            "AND lower(neighborhood) = :neighborhood AND listing_type = :ltype",
            {"neighborhood": neighborhood_match_value, "ltype": listing_type or "store"},
            8,
            "district_type",
        ))
        chains.append((
            "AND lower(neighborhood) = :neighborhood",
            {"neighborhood": neighborhood_match_value},
            8,
            "district",
        ))

    chains.append((
        "AND area_sqm >= :band_lo AND area_sqm < :band_hi AND listing_type = :ltype",
        {"band_lo": band_lo, "band_hi": band_hi, "ltype": listing_type or "store"},
        12,
        "city_band_type",
    ))
    chains.append((
        "",
        {},
        20,
        "city",
    ))

    for extra_where, params, min_n, label in chains:
        try:
            with db.begin_nested():
                agg = db.execute(
                    text(f"""
                        SELECT
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (price_sar_annual / area_sqm / 12.0)) AS median_monthly_per_m2,
                            COUNT(*) AS n,
                            SUM(CASE WHEN (price_sar_annual / area_sqm / 12.0) <= :listing_rate THEN 1 ELSE 0 END) AS n_below,
                            -- Relative listing-age signal: comparables AS OLD OR
                            -- OLDER than the candidate on the ORIGINAL-listing-date
                            -- basis (aqar_created_at, first_seen_at as null-guard
                            -- floor only). NOT GREATEST-of-three: a re-posted
                            -- long-vacant listing is still stale, and resetting its
                            -- age to the re-post date would erase the signal.
                            -- Candidate and comparables share the created_at basis
                            -- (cand_age_days from _created_basis_age_days) or the
                            -- percentile would be meaningless.
                            SUM(CASE WHEN EXTRACT(DAY FROM now() - COALESCE(aqar_created_at, first_seen_at)) >= :cand_age_days THEN 1 ELSE 0 END) AS n_older
                        {base_where}
                        {extra_where}
                    """),
                    {
                        **params,
                        "listing_rate": float(listing_monthly_rent_per_m2),
                        "rent_floor": _RENT_COMP_MIN_SAR_M2_MONTH,
                        "rent_ceiling": _RENT_COMP_MAX_SAR_M2_MONTH,
                        "max_comp_area": _RENT_COMP_MAX_AREA_SQM,
                        "cand_age_days": cand_age_days,
                    },
                ).mappings().first()
        except Exception:
            logger.debug("percentile rent comp failed for label=%s", label, exc_info=True)
            continue

        if not agg or agg["n"] is None or int(agg["n"]) < min_n:
            continue

        n = int(agg["n"])
        n_below = int(agg["n_below"] or 0)
        percentile = max(0.0, min(1.0, n_below / n))

        # Relative listing-age percentile, computed off the SAME >= min-N
        # comparable set as the rent percentile (so it is honest about its
        # support). HIGH = old relative to peers. Null when the candidate has
        # no created/first_seen basis date to compare against.
        if cand_age_days is not None:
            n_older = int(agg["n_older"] or 0)
            age_percentile: float | None = round(max(0.0, min(1.0, n_older / n)), 3)
        else:
            age_percentile = None

        # Map percentile → burden score using anchor interpolation:
        #   p10 → 92, p50 → 60, p90 → 18.
        if percentile <= 0.10:
            burden_score = 92.0 + (0.10 - percentile) / 0.10 * 5.0
        elif percentile <= 0.50:
            burden_score = 60.0 + (0.50 - percentile) / 0.40 * 32.0
        elif percentile <= 0.90:
            burden_score = 18.0 + (0.90 - percentile) / 0.40 * 42.0
        else:
            burden_score = 18.0 - (percentile - 0.90) / 0.10 * 15.0

        burden_score = _clamp(burden_score)

        return {
            "burden_score": round(burden_score, 2),
            "percentile": round(percentile, 3),
            "n_comparable": n,
            "source_label": label,
            "median_monthly_rent_per_m2": round(float(agg["median_monthly_per_m2"] or 0.0), 2),
            "listing_monthly_rent_per_m2": round(float(listing_monthly_rent_per_m2), 2),
            "age_percentile": age_percentile,
            "n_comparables": n,
            "comparable_bounds": {
                "min_sar_m2_month": _RENT_COMP_MIN_SAR_M2_MONTH,
                "max_sar_m2_month": _RENT_COMP_MAX_SAR_M2_MONTH,
                "max_area_sqm": _RENT_COMP_MAX_AREA_SQM,
                "excluded_property_types": list(_RENT_COMP_EXCLUDED_PROPERTY_TYPES),
            },
        }

    return None


# ---------------------------------------------------------------------------
# Economics composite score
# ---------------------------------------------------------------------------

# Finding 2: price-tier multiplier applied to the ABSOLUTE rent ceilings only
# (absolute_fallback 220 / absolute_legacy 180). A premium brand can sustain a
# higher rent/m² before the burden penalty bites; a value brand less. The
# percentile path is intentionally NOT tier-adjusted — it is peer-relative and
# feeds value_score, which must stay tier-blind. Tier vocab matches
# _IMPLIED_CHECK_SAR ("value"/"mid"/"premium"); unknown/None -> 1.0 (no change).
_RENT_CEILING_TIER_MULT: dict[str, float] = {
    "value": 0.85,
    "mid": 1.0,
    "premium": 1.30,
}


def _rent_ceiling_tier_multiplier(price_tier: str | None) -> float:
    return _RENT_CEILING_TIER_MULT.get(str(price_tier or "").lower().strip(), 1.0)


def _economics_score(
    *,
    estimated_revenue_index: float,
    estimated_annual_rent_sar: float,
    estimated_fitout_cost_sar: float,
    area_m2: float,
    cannibalization_score: float,
    fit_score: float,
    db: Session | None = None,
    is_listing: bool = False,
    district: str | None = None,
    listing_type: str | None = None,
    unit_neighborhood_raw: str | None = None,
    price_tier: str | None = None,
    cand_age_days: int | None = None,
    revenue_index_detail: dict[str, Any] | None = None,
) -> tuple[float, dict[str, Any]]:
    monthly_rent_per_m2 = estimated_annual_rent_sar / max(area_m2 * 12.0, 1.0)

    # value_score must stay tier-blind: use the tier-blind revenue index
    # (ticket multiplier excluded) from _estimate_revenue_index detail when
    # the caller provides it. Callers that don't pass the detail fall back
    # to the tier-multiplied index (legacy behavior). The composite score
    # below keeps using the tier-multiplied estimated_revenue_index —
    # ranking semantics outside value_score are unchanged.
    _rev_detail = revenue_index_detail if isinstance(revenue_index_detail, dict) else {}
    value_revenue_basis = _safe_float(
        _rev_detail.get("tier_blind_index"), estimated_revenue_index
    )
    _ticket_multiplier = _rev_detail.get("ticket_multiplier")

    _tier_mult = _rent_ceiling_tier_multiplier(price_tier)
    _fallback_ceiling = 220.0 * _tier_mult
    _legacy_ceiling = 180.0 * _tier_mult

    rent_burden_meta: dict[str, Any] = {"mode": "absolute_legacy"}
    rent_burden_score: float

    if is_listing and db is not None:
        comp = _percentile_rent_burden(
            db,
            listing_monthly_rent_per_m2=monthly_rent_per_m2,
            district=district,
            area_m2=area_m2,
            listing_type=listing_type,
            unit_neighborhood_raw=unit_neighborhood_raw,
            cand_age_days=cand_age_days,
        )
        if comp is not None:
            rent_burden_score = comp["burden_score"]
            rent_burden_meta = {"mode": "percentile", **comp}
        else:
            rent_burden_score = _clamp(100.0 - (monthly_rent_per_m2 / _fallback_ceiling) * 100.0)
            rent_burden_meta = {
                "mode": "absolute_fallback",
                "listing_monthly_rent_per_m2": round(monthly_rent_per_m2, 2),
                "ceiling": round(_fallback_ceiling, 2),
                "ceiling_base": 220.0,
                "ceiling_tier_multiplier": round(_tier_mult, 3),
                "price_tier": (str(price_tier).lower().strip() if price_tier else None),
            }
    else:
        rent_burden_score = _clamp(100.0 - (monthly_rent_per_m2 / _legacy_ceiling) * 100.0)
        rent_burden_meta = {
            "mode": "absolute_legacy",
            "monthly_rent_per_m2": round(monthly_rent_per_m2, 2),
            "ceiling": round(_legacy_ceiling, 2),
            "ceiling_base": 180.0,
            "ceiling_tier_multiplier": round(_tier_mult, 3),
            "price_tier": (str(price_tier).lower().strip() if price_tier else None),
        }

    fitout_cost_per_m2 = estimated_fitout_cost_sar / max(area_m2, 1.0)
    fitout_burden_score = _clamp(100.0 - ((fitout_cost_per_m2 - 1800.0) / 2600.0) * 100.0)
    cannibalization_component = 100.0 - cannibalization_score

    rb_confidence = _rent_burden_confidence(
        rent_burden_meta.get("source_label") if isinstance(rent_burden_meta, dict) else None,
        rent_burden_meta.get("n_comparable") if isinstance(rent_burden_meta, dict) else None,
    )
    rb_weight = 0.20 * rb_confidence
    revenue_weight = 0.38 + (0.20 - rb_weight)  # absorb deficit into most reliable component
    score = _clamp(
        estimated_revenue_index * revenue_weight
        + rent_burden_score * rb_weight
        + fitout_burden_score * 0.14
        + cannibalization_component * 0.13
        + fit_score * 0.15
    )

    # Derived "best price-to-value" chip. Computed inside _economics_score so
    # the inputs (revenue_index, rent_burden_score, source_label,
    # n_comparable) are all in scope without re-reading score_breakdown_json.
    # Only published when rent_burden ran in percentile mode — the
    # absolute_legacy / absolute_fallback / envelope paths produce a
    # rent_burden_score that isn't peer-relative, so a value_score derived
    # from them would mis-classify candidates and the UI would badge them
    # incorrectly. value_score == None propagates as "value not available".
    value_score: float | None
    value_band: str | None
    value_band_low_confidence = False
    if (
        settings.EXPANSION_VALUE_SCORE_ENABLED
        and isinstance(rent_burden_meta, dict)
        and rent_burden_meta.get("mode") == "percentile"
    ):
        value_score = _value_score(value_revenue_basis, rent_burden_score)
        value_band = _classify_value_band(value_score)
        value_band_low_confidence = _value_band_is_low_confidence(
            rent_burden_meta.get("source_label"),
            rent_burden_meta.get("n_comparable"),
        )
    else:
        value_score = None
        value_band = None

    return score, {
        "rent_burden_score": round(rent_burden_score, 2),
        "rent_burden": rent_burden_meta,
        "rent_burden_confidence": round(rb_confidence, 3),
        "rent_burden_weight": round(rb_weight, 4),
        "revenue_weight": round(revenue_weight, 4),
        "fitout_burden_score": round(fitout_burden_score, 2),
        "monthly_rent_per_m2": round(monthly_rent_per_m2, 2),
        "value_score": round(value_score, 2) if value_score is not None else None,
        "value_band": value_band,
        "value_band_low_confidence": value_band_low_confidence,
        "value_revenue_basis": round(value_revenue_basis, 2),
        "ticket_multiplier": (
            round(float(_ticket_multiplier), 4) if _ticket_multiplier is not None else None
        ),
    }


# ---------------------------------------------------------------------------
# value_score: "strong location at a fair price" derived chip.
# ---------------------------------------------------------------------------

# Bands per Faisal's directive (locked):
#   value_score >= 75   → "best_value"   (green badge, eligible for soft uprank)
#   25 <= value_score < 75 → "neutral"   (no badge)
#   value_score < 25    → "above_market" (red/amber badge, soft downrank)
_VALUE_BAND_BEST_VALUE_MIN: float = 75.0
_VALUE_BAND_ABOVE_MARKET_MAX: float = 25.0


def _value_score(revenue_index: float, rent_burden_score: float) -> float:
    """Geometric mean of revenue_index and rent_burden_score, clamped 0-100.

    Both inputs are pre-clamped to [0,100] by their producers
    (_estimate_revenue_index returns through _clamp; _percentile_rent_burden
    burden_score is _clamp'd). The eps=1.0 floor avoids a single zero
    collapsing the score and keeps the function defensible against transient
    zeros from downstream callers.

    Monotonic in both inputs by construction: d/dx of sqrt(x*y) > 0 when y > 0.
    """
    eps = 1.0
    rev = max(_safe_float(revenue_index), eps)
    rb = max(_safe_float(rent_burden_score), eps)
    return _clamp(math.sqrt(rev * rb))


def _classify_value_band(value_score: float | None) -> str | None:
    if value_score is None:
        return None
    if value_score >= _VALUE_BAND_BEST_VALUE_MIN:
        return "best_value"
    if value_score < _VALUE_BAND_ABOVE_MARKET_MAX:
        return "above_market"
    return "neutral"


def _value_band_is_low_confidence(source_label: Any, n_comparable: Any) -> bool:
    """True when the comp pool backing this score is citywide rather than
    district-scoped. The percentile fallback chain self-enforces min_n=12 for
    city_band_type and min_n=20 for city, so any production row with one of
    these labels is by definition citywide and therefore low-confidence for
    UI promotion purposes. Defensive against future relaxation of those
    thresholds.
    """
    if not isinstance(source_label, str):
        return False
    return source_label in {"city_band_type", "city"}


def _candidate_value_band(c: dict[str, Any]) -> tuple[str | None, bool]:
    """Read (value_band, low_confidence) for a candidate.

    Production candidates carry the band inside
    ``score_breakdown_json["economics_detail"]`` (the persisted source of
    truth). The top-level keys are only set after _normalize_candidate_payload
    runs, which is post-persistence and post-pass. Read from the nested
    location first, then fall back to top-level so synthetic test fixtures
    that set value_band directly continue to work.
    """
    sb = c.get("score_breakdown_json")
    ed = sb.get("economics_detail") if isinstance(sb, dict) else None
    if isinstance(ed, dict):
        band = ed.get("value_band")
        low_conf = bool(ed.get("value_band_low_confidence"))
        if band is not None:
            return band, low_conf
    return c.get("value_band"), bool(c.get("value_band_low_confidence"))


def _apply_score_deltas_and_sort(
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Fold value-band, viability, freshness, and momentum deltas into
    ``final_score`` and re-sort by ``final_score DESC, parcel_id ASC``.

    Inputs:
      * ``final_score`` is the current base_deterministic score (output of
        ``_score_breakdown``).
      * ``viability_legs_fired`` and ``viability_delta`` are populated by
        ``_apply_market_viability_pass`` (transient working fields, dropped
        before this function returns).
      * ``feature_snapshot_json["listing_age"]`` and
        ``feature_snapshot_json["district_momentum"]`` drive the freshness
        and momentum bonuses respectively.

    Output mutations on every candidate:
      * ``final_score`` is overwritten with ``clamp(base + Σ deltas, 0, 100)``.
      * ``score_breakdown_json["final_score"]`` mirrors the clamped score.
      * ``score_breakdown_json["bonus_detail"]`` records every input delta,
        the clamping flag, and the freshness label so callers can audit how
        the bonus stacked.
      * Legacy back-compat keys ``value_uprank_*`` / ``value_downrank_*``
        (top-level + ``score_breakdown_json["value_pass"]``) are written
        when the value-band leg fires; readers should migrate to
        ``bonus_detail.value_band_delta``. Deprecated.

    Sort is strict: ``(-final_score, parcel_id)``. The parcel_id tie-break
    guarantees identical orderings on re-runs even when scores collide.

    Weight stack v2: the +2 momentum bonus is removed (momentum is paid
    once, via the district_momentum component in ``_score_breakdown``) and
    ``bonus_detail`` omits the ``momentum_bonus`` key. All other deltas
    (freshness, value band, viability) are unchanged in both stacks.
    """
    _stack_v2 = str(getattr(settings, "EXPANSION_WEIGHT_STACK", "v1")) == "v2"
    for _c in candidates:
        base = _safe_float(_c.get("final_score"), 0.0)

        value_band_delta = _value_band_score_delta(_c)

        viability_legs_fired = list(_c.get("viability_legs_fired") or [])
        viability_delta = float(_c.get("viability_delta", 0.0) or 0.0)

        # Freshness: "New" (created within window) takes precedence over
        # "Updated" (refreshed within window). Mutually exclusive by design
        # so a fresh-and-recently-refreshed listing earns +2, not +3.
        fs = _c.get("feature_snapshot_json") or {}
        listing_age = fs.get("listing_age") or {}
        created_days = listing_age.get("created_days")
        updated_days = listing_age.get("updated_days")
        is_new = (
            isinstance(created_days, (int, float))
            and not isinstance(created_days, bool)
            and 0 <= float(created_days) <= _LISTING_FRESHNESS_DAYS
        )
        is_updated = (
            (not is_new)
            and isinstance(updated_days, (int, float))
            and not isinstance(updated_days, bool)
            and 0 <= float(updated_days) <= _LISTING_FRESHNESS_DAYS
        )
        if is_new:
            freshness_bonus = 2.0
            freshness_label: str | None = "new"
        elif is_updated:
            freshness_bonus = 1.0
            freshness_label = "updated"
        else:
            freshness_bonus = 0.0
            freshness_label = None

        if _stack_v2:
            # v2: momentum is already paid via the district_momentum
            # component; the +2 bonus would double-pay it.
            momentum_bonus = 0.0
        else:
            momentum = fs.get("district_momentum") or {}
            momentum_score = momentum.get("momentum_score")
            if (
                isinstance(momentum_score, (int, float))
                and not isinstance(momentum_score, bool)
                and float(momentum_score) >= _MOMENTUM_DISPLAY_THRESHOLD
                and momentum.get("sample_floor_applied") is False
            ):
                momentum_bonus = 2.0
            else:
                momentum_bonus = 0.0

        total_delta = (
            value_band_delta + viability_delta + freshness_bonus + momentum_bonus
        )
        raw_final = base + total_delta
        final_clamped = (raw_final < 0.0) or (raw_final > 100.0)
        new_final = _clamp(raw_final, 0.0, 100.0)

        sb = _c.get("score_breakdown_json")
        if not isinstance(sb, dict):
            sb = {}
            _c["score_breakdown_json"] = sb
        sb["bonus_detail"] = {
            "base_deterministic": round(base, 2),
            "value_band_delta": float(value_band_delta),
            "viability_legs_fired": viability_legs_fired,
            "viability_delta": float(viability_delta),
            "freshness_bonus": float(freshness_bonus),
            "freshness_label": freshness_label,
            # v2 omits momentum_bonus entirely — the leg no longer exists.
            **({} if _stack_v2 else {"momentum_bonus": float(momentum_bonus)}),
            "total_delta": float(total_delta),
            "final_score_clamped": bool(final_clamped),
        }
        sb["final_score"] = round(new_final, 2)
        _c["final_score"] = round(new_final, 2)

        # Back-compat: legacy value_pass keys + top-level mirrors so existing
        # saved-study consumers and the frontend Why-#N chip continue to read
        # the uprank/downrank flag for one release cycle. The "delta" fields
        # carry the magnitude of the score delta (4 for +4 best_value, 6 for
        # -6 above_market) so the chip displays an accurate change. Readers
        # should migrate to bonus_detail.value_band_delta for the signed
        # value. Deprecated as of score-delta refactor.
        if value_band_delta == 4.0:
            vp = sb.setdefault("value_pass", {})
            vp["value_uprank_applied"] = True
            vp["value_uprank_delta"] = 4
            _c["value_uprank_applied"] = True
            _c["value_uprank_delta"] = 4
        elif value_band_delta == -6.0:
            vp = sb.setdefault("value_pass", {})
            vp["value_downrank_applied"] = True
            vp["value_downrank_delta"] = 6
            _c["value_downrank_applied"] = True
            _c["value_downrank_delta"] = 6

        # Drop transient working fields populated by the viability pass so
        # they do not leak into persistence or downstream consumers.
        _c.pop("viability_legs_fired", None)
        _c.pop("viability_delta", None)

    candidates.sort(
        key=lambda _c: (
            -_safe_float(_c.get("final_score"), 0.0),
            str(_c.get("parcel_id", "")),
        )
    )
    return candidates


def _value_band_score_delta(c: dict[str, Any]) -> float:
    """Return the score delta contributed by the candidate's value_band.

    +4 for high-confidence ``best_value``; -6 for high-confidence
    ``above_market``; 0 otherwise (low-confidence pools and neutral/missing
    bands are intentionally inert — same skip semantics as the deleted
    ``_apply_value_band_pass`` positional nudge).
    """
    band, low_conf = _candidate_value_band(c)
    if low_conf:
        return 0.0
    if band == "best_value":
        return 4.0
    if band == "above_market":
        return -6.0
    return 0.0


def _select_final_candidates(
    candidates: list[dict[str, Any]],
    target_districts: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    """Final shortlist selection over the score-sorted viability survivors.

    City-wide / single-district searches (fewer than 2 target districts)
    take the plain top-``limit`` slice — identical to the pre-balancing
    behavior.

    Multi-district searches apply a per-district quota of
    ``max(1, limit // len(target_districts))``. The representation
    guarantee is best-effort-within-limit and applies to hard-floor
    SURVIVORS only: a district whose every candidate fails a hard floor in
    ``_apply_market_viability_pass`` is legitimately unrepresented.
    Candidates whose district does not normalize to a usable key
    (``_unknown``) get NO quota — they compete only in the fill phase.

    Selection happens in two walks over the sorted input, and the output is
    emitted in input order (a filtered subsequence of the sorted list), so
    the result stays in final_score order by construction:

      1. Quota walk: accept a candidate when its district's quota is
         unfilled, never exceeding ``limit`` total.
      2. Fill walk: top up the remaining slots strictly by rank, skipping
         already-selected candidates.
    """
    if len(target_districts) < 2:
        return candidates[:limit]

    quota = max(1, limit // len(target_districts))
    taken_per_district: dict[str, int] = {}
    chosen: set[int] = set()

    for idx, c in enumerate(candidates):
        if len(chosen) >= limit:
            break
        dk = normalize_district_key(c.get("district")) or "_unknown"
        if dk == "_unknown":
            continue
        if taken_per_district.get(dk, 0) >= quota:
            continue
        chosen.add(idx)
        taken_per_district[dk] = taken_per_district.get(dk, 0) + 1

    for idx in range(len(candidates)):
        if len(chosen) >= limit:
            break
        chosen.add(idx)

    return [candidates[idx] for idx in sorted(chosen)]


def _apply_market_viability_pass(
    candidates: list[dict[str, Any]],
    *,
    search_id: str | None = None,
    rent_pct_threshold: float | None = None,
    pop_percentile_threshold: float | None = None,
    radiance_yoy_threshold: float | None = None,
    population_hard_floor: int | None = None,
    commercial_hard_floor: int | None = None,
    construction_buffer_m: float | None = None,
    diagnostics: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Demote candidates that are confidently bad on the CEO-directive legs.

    Five independent legs, soft-demote on any (single demote, never compounded):

      * Population leg (clause 1, "high population density") — fires when the
        per-search bottom-quartile gate fails AND the radiance growth signal
        does not rescue the candidate. Mirrors the original 2-of-2 pop half.
      * Rent leg (clause 2, "reasonable or affordable pricing") — fires when
        the rent percentile is confidently high AND growth does not rescue.
        Decoupled from population: a high-rent candidate in low-growth area
        is demoted on its own merit, not only when population is also low.
      * Economics leg (clause 3, "strong potential for profitability") —
        fires when ``economics_score < EXPANSION_VIABILITY_ECONOMICS_MIN``.
        No growth rescue: economics_score is a current-quality composite,
        not a forward-looking signal; rescuing it on growth would conflate
        clause 3 with future-looking signals already covered by the other
        legs.
      * Demand leg (clause 2 of Faisal's directive, "strong potential for
        sales", B3) — fires when ``feature_snapshot_json["realized_demand_30d"]``
        is in the per-search bottom quartile AND the catchment carries at
        least ``EXPANSION_VIABILITY_DEMAND_MIN_BRANCHES`` distinct
        contributing branches (mirroring the snapshot writer's confidence
        gate). No growth rescue, by the same precedent as the economics
        leg: realized demand is a present-state signal, not a forward-state
        one, and conflating it with radiance growth re-introduces the
        false-positive class the leg exists to suppress. Disable in
        isolation via ``EXPANSION_VIABILITY_DEMAND_LEG_ENABLED=false``;
        the snapshot pipeline (and the ``realized_demand_30d`` /
        ``realized_demand_branches`` annotation) remain fully populated.
      * Radiance-growth leg (Pillar 3 of Faisal's directive, "strong
        potential for business growth", B1+B2) — fires when
        ``feature_snapshot_json["radiance_growth"].confident`` is True
        AND ``value_yoy_pct`` < ``EXPANSION_VIABILITY_RADIANCE_YOY_DEMOTE_THRESHOLD``
        (operator strict ``<``). NO growth rescue: a leg whose own signal
        is forward-looking cannot self-rescue; mirrors the economics and
        demand legs. Distinct from
        ``EXPANSION_VIABILITY_RADIANCE_YOY_THRESHOLD`` which drives the
        rescue side (operator ``>=``) for the population and rent legs;
        splitting the knobs prevents calibrating one from silently
        affecting the other. Disable in isolation via
        ``EXPANSION_VIABILITY_RADIANCE_GROWTH_LEG_ENABLED=false``; the
        advisory ``radiance_growth_pass`` gate emission remains intact —
        only the soft-demote behavior is suppressed. Ships at the
        calibrated default ``0.0`` (recalibrated 2026-05-10, superseding
        the original 2026-05-06 default of ``2.0``): the 2.0 default
        demoted ~49% of confident candidates and compressed top scores,
        so the leg was re-anchored to ``0.0`` to isolate only the
        "confidently shrinking" tier (value_yoy_pct strictly below 0%).
        Env-overridable; the live production value is this ``0.0``
        default (not set in the deployment secret). See the
        ``EXPANSION_VIABILITY_RADIANCE_YOY_DEMOTE_THRESHOLD`` block in
        ``app/core/config.py`` for the full recalibration rationale.

    The growth-rescue signal reads ``feature_snapshot_json["radiance_growth"]``
    (NASA Black Marble VNP46A3). When that signal is confident and YoY growth
    meets the threshold, the candidate is rescued from the population and rent
    legs (not from the economics or demand legs).

    Conservative: each measured leg requires its underlying signal to be
    CONFIDENT (rent scope not citywide; pop reach > 0; economics_score
    populated; realized_demand_30d present with branches >= the configured
    minimum).

    Score-delta refactor: this function no longer reorders the candidate
    list. For each survivor of the hard-floor drops it computes which legs
    fired and stashes ``viability_legs_fired`` (list[str]) and
    ``viability_delta`` (float, ``-10`` per fired leg) on the candidate dict
    so the caller can fold the delta into ``final_score`` once. It still
    writes ``market_viability_flag`` to ``score_breakdown_json`` with the
    legs that fired (stable order: rpc, population, rent, economics, demand,
    radiance_growth) and the per-leg booleans / threshold context used by
    the saved-study UI.
    """
    if not candidates:
        return candidates

    rent_pct_threshold = (
        rent_pct_threshold
        if rent_pct_threshold is not None
        else settings.EXPANSION_VIABILITY_RENT_PCT_THRESHOLD
    )
    pop_percentile_threshold = (
        pop_percentile_threshold
        if pop_percentile_threshold is not None
        else settings.EXPANSION_VIABILITY_POP_PERCENTILE
    )
    radiance_yoy_threshold = (
        radiance_yoy_threshold
        if radiance_yoy_threshold is not None
        else settings.EXPANSION_VIABILITY_RADIANCE_YOY_THRESHOLD
    )
    demand_percentile_threshold = float(getattr(
        settings, "EXPANSION_VIABILITY_DEMAND_PERCENTILE", 0.25
    ))
    demand_min_branches = int(getattr(
        settings, "EXPANSION_VIABILITY_DEMAND_MIN_BRANCHES", 3
    ))
    demand_leg_enabled = bool(getattr(
        settings, "EXPANSION_VIABILITY_DEMAND_LEG_ENABLED", True
    ))
    radiance_yoy_demote_threshold = float(getattr(
        settings, "EXPANSION_VIABILITY_RADIANCE_YOY_DEMOTE_THRESHOLD", 0.0
    ))
    radiance_growth_leg_enabled = bool(getattr(
        settings, "EXPANSION_VIABILITY_RADIANCE_GROWTH_LEG_ENABLED", True
    ))

    # ── CEO directive: hard floors (broader data + filter low-potential) ──
    # Two absolute drops applied BEFORE the 3-of-3 conjunction below. Keys
    # are read once at function entry; 0 disables the corresponding gate
    # entirely. Missing fields → pass (defensive: protects the pre-2026-04-26
    # backfill cohort and any candidates whose pop_reach is unmeasured).
    pop_floor = (
        int(population_hard_floor)
        if population_hard_floor is not None
        else int(getattr(settings, "EXPANSION_VIABILITY_POPULATION_HARD_FLOOR", 0) or 0)
    )
    bp_floor = (
        int(commercial_hard_floor)
        if commercial_hard_floor is not None
        else int(getattr(settings, "EXPANSION_VIABILITY_BRAND_PRESENCE_HARD_FLOOR", 0) or 0)
    )
    cp_buffer_m = (
        float(construction_buffer_m)
        if construction_buffer_m is not None
        else float(getattr(settings, "EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M", 0) or 0)
    )

    survivors: list[dict[str, Any]] = []
    dropped_population = 0
    dropped_commercial = 0
    dropped_construction = 0
    for c in candidates:
        fs = c.get("feature_snapshot_json") if isinstance(c, dict) else None

        # Population floor: pass when disabled, or when the value is truly
        # unmeasured (None — no population-grid coverage in the catchment;
        # don't drop on absent data, the soft pop leg handles low-pop
        # demotion). A MEASURED value — including a genuine 0.0 — is
        # evaluated against the floor: a 0-pop covered site must fail the
        # same floor that drops a 19,999-pop site (it no longer bypasses it
        # via a None-vs-0 merge). The rpc/soft legs keep their own guards.
        pop_raw = fs.get("population_reach") if isinstance(fs, dict) else None
        if pop_floor <= 0:
            population_floor_pass = True
        elif pop_raw is None:
            population_floor_pass = True
        else:
            try:
                pop_val = float(pop_raw)
            except (TypeError, ValueError):
                # Unparseable measured value → treat as unmeasured (pass).
                population_floor_pass = True
            else:
                population_floor_pass = pop_val >= pop_floor

        # Commercial-activity floor: pass when disabled, or when
        # brand_presence is absent (defensive — backfill cliff cohort), or
        # when unique_brands within 500 m meets the floor.
        bp_block = fs.get("brand_presence") if isinstance(fs, dict) else None
        if bp_floor <= 0:
            commercial_floor_pass = True
        elif not isinstance(bp_block, dict) or "unique_brands" not in bp_block:
            commercial_floor_pass = True
        else:
            try:
                unique_brands = int(bp_block.get("unique_brands") or 0)
            except (TypeError, ValueError):
                unique_brands = 0
            commercial_floor_pass = unique_brands >= bp_floor

        # Construction-proximity floor: pass when disabled, when the
        # construction_proximity block is missing/malformed (defensive —
        # backfill cliff cohort, OSM table absent, bulk query failed), or
        # when zero polygons were observed within the buffer. Otherwise
        # fail (drop the candidate).
        cp_block = fs.get("construction_proximity") if isinstance(fs, dict) else None
        if cp_buffer_m <= 0:
            construction_proximity_pass = True
        elif not isinstance(cp_block, dict) or "polygon_count" not in cp_block:
            construction_proximity_pass = True
        else:
            try:
                cp_count = int(cp_block.get("polygon_count") or 0)
            except (TypeError, ValueError):
                cp_count = 0
            construction_proximity_pass = cp_count <= 0

        gate_status = c.get("gate_status_json")
        if not isinstance(gate_status, dict):
            gate_status = {}
            c["gate_status_json"] = gate_status
        gate_status["population_floor_pass"] = population_floor_pass
        gate_status["commercial_floor_pass"] = commercial_floor_pass
        gate_status["construction_proximity_pass"] = construction_proximity_pass

        if not population_floor_pass:
            dropped_population += 1
            continue
        if not commercial_floor_pass:
            dropped_commercial += 1
            continue
        if not construction_proximity_pass:
            dropped_construction += 1
            continue
        survivors.append(c)

    if dropped_population or dropped_commercial or dropped_construction:
        logger.info(
            "market_viability_hard_floors",
            extra={
                "search_id": search_id,
                "dropped_population": dropped_population,
                "dropped_commercial": dropped_commercial,
                "dropped_construction": dropped_construction,
                "remaining": len(survivors),
                "pop_floor": pop_floor,
                "bp_floor": bp_floor,
                "construction_buffer_m": cp_buffer_m,
            },
        )

    # Surface the per-leg drop counts and the thresholds in effect to the
    # caller so the API meta can explain unsaturated-limit responses without
    # needing kubectl logs. Always populated when ``diagnostics`` is provided
    # — even when nothing was dropped, so the response shape stays stable.
    if diagnostics is not None:
        diagnostics["hard_floors"] = {
            "drops": {
                "dropped_population": dropped_population,
                "dropped_commercial": dropped_commercial,
                "dropped_construction": dropped_construction,
                "remaining": len(survivors),
            },
            "thresholds": {
                "hard_floor_pop_threshold": pop_floor,
                "hard_floor_brand_threshold": bp_floor,
                "hard_floor_construction_buffer_m": cp_buffer_m,
            },
        }

    candidates = survivors
    if not candidates or len(candidates) < 4:
        return candidates

    out = list(candidates)
    n = len(out)

    # Collect populated, positive population_reach values across the cohort
    # to compute the per-search bottom-quartile threshold.
    pop_values: list[float] = []
    for c in out:
        fs = c.get("feature_snapshot_json")
        if not isinstance(fs, dict):
            continue
        raw = fs.get("population_reach")
        if raw is None:
            continue
        v = _safe_float(raw, default=-1.0)
        if v > 0:
            pop_values.append(v)

    if len(pop_values) < 4:
        return out

    # statistics.quantiles with n=100 + index gives a percentile cut. Use
    # method="inclusive" so endpoints behave as expected on small samples.
    pct_index = max(1, min(99, int(round(pop_percentile_threshold * 100))))
    try:
        cutoffs = statistics.quantiles(pop_values, n=100, method="inclusive")
        pop_threshold = float(cutoffs[pct_index - 1])
    except Exception:
        return out

    economics_min = float(settings.EXPANSION_VIABILITY_ECONOMICS_MIN)

    # ── Demand leg (B3) — per-search bottom-quartile cutoff over confident
    # realized_demand_30d values. The snapshot writer omits the field
    # entirely when branches < 3, so presence already implies confidence;
    # we still re-check the branches gate per candidate so the threshold
    # used here can be read independently of the writer's gate. When the
    # kill switch is off, or fewer than 4 confident values exist, the
    # threshold is None and the leg silently does not fire.
    demand_values: list[float] = []
    if demand_leg_enabled:
        for c in out:
            fs = c.get("feature_snapshot_json")
            if not isinstance(fs, dict):
                continue
            raw = fs.get("realized_demand_30d")
            if raw is None:
                continue
            v = _safe_float(raw, default=-1.0)
            if v > 0:
                demand_values.append(v)

    demand_threshold: float | None
    if not demand_leg_enabled or len(demand_values) < 4:
        demand_threshold = None
    else:
        demand_pct_index = max(
            1, min(99, int(round(demand_percentile_threshold * 100)))
        )
        try:
            demand_cutoffs = statistics.quantiles(
                demand_values, n=100, method="inclusive"
            )
            demand_threshold = float(demand_cutoffs[demand_pct_index - 1])
        except Exception:
            demand_threshold = None

    # ── rent_per_capita demote leg (CEO "low-pop + high-rent" anti-pattern) ──
    # Evaluated BEFORE the existing legs. Cohort percentile on
    # estimated_annual_rent_sar / population_reach, mirroring the
    # pop_demote percentile pattern (statistics.quantiles, inclusive).
    # Below ``EXPANSION_VIABILITY_RPC_MIN_COHORT`` valid candidates the leg
    # is skipped entirely (no demotions, no flag writes). Independent of
    # the rent_burden / population_reach legs above: this catches the
    # joint pattern (~30K pop with ~200K SAR rent) that those independent
    # legs only catch when both happen to fire on the same candidate.
    rpc_percentile_threshold = float(getattr(
        settings, "EXPANSION_VIABILITY_RPC_PERCENTILE", 0.75
    ))
    rpc_min_cohort = int(getattr(
        settings, "EXPANSION_VIABILITY_RPC_MIN_COHORT", 10
    ))
    rpc_per_candidate_id: dict[int, float | None] = {}
    for c in out:
        fs = c.get("feature_snapshot_json")
        rpc: float | None = None
        if isinstance(fs, dict):
            rent_raw = fs.get("estimated_annual_rent_sar")
            pop_raw = fs.get("population_reach")
            if rent_raw is not None and pop_raw is not None:
                rent_v = _safe_float(rent_raw, default=-1.0)
                pop_v = _safe_float(pop_raw, default=-1.0)
                if rent_v > 0 and pop_v > 0:
                    rpc = rent_v / pop_v
        rpc_per_candidate_id[id(c)] = rpc

    valid_rpc = sorted(v for v in rpc_per_candidate_id.values() if v is not None)
    rpc_active = len(valid_rpc) >= rpc_min_cohort
    rpc_threshold: float | None = None
    if rpc_active:
        rpc_pct_index = max(1, min(99, int(round(rpc_percentile_threshold * 100))))
        try:
            rpc_cutoffs = statistics.quantiles(
                valid_rpc, n=100, method="inclusive"
            )
            rpc_threshold = float(rpc_cutoffs[rpc_pct_index - 1])
        except Exception:
            rpc_threshold = None
            rpc_active = False

    rpc_telemetry_by_id: dict[int, dict[str, Any]] = {}
    rpc_demote_by_id: dict[int, bool] = {}
    if rpc_active and rpc_threshold is not None:
        cohort_size = len(valid_rpc)
        for c in out:
            cid = id(c)
            rpc = rpc_per_candidate_id[cid]
            if rpc is None:
                rpc_telemetry_by_id[cid] = {
                    "rent_per_capita_sar": None,
                    "rent_per_capita_pct": None,
                    "rent_per_capita_demote": None,
                }
                rpc_demote_by_id[cid] = False
            else:
                rank = bisect_right(valid_rpc, rpc)
                pct = rank / cohort_size if cohort_size else 0.0
                # At-most-once demote: ``rpc`` runs first, so the
                # already-demoted check is a defensive no-op (the existing
                # demote loop applies a single positional swap per
                # candidate regardless of how many legs fire).
                do_demote = rpc >= rpc_threshold
                rpc_telemetry_by_id[cid] = {
                    "rent_per_capita_sar": round(rpc, 4),
                    "rent_per_capita_pct": float(pct),
                    "rent_per_capita_demote": bool(do_demote),
                }
                rpc_demote_by_id[cid] = do_demote

    def _flag_inputs(
        c: dict[str, Any]
    ) -> tuple[
        bool, bool, bool, bool, bool,
        float, str | None, float, float | None, dict[str, Any],
        float | None, int | None,
    ]:
        # Returns:
        #   (pop_demote, rent_demote, econ_demote, demand_demote,
        #    radiance_growth_demote,
        #    rent_pct, rent_scope, pop_reach, economics_score, radiance_meta,
        #    demand_value, demand_branches)
        radiance_meta: dict[str, Any] = {
            "radiance_growth_pct": None,
            "radiance_confident": None,
            "radiance_pixel_count": None,
            "radiance_year_month": None,
        }

        # ── Clause 3 (economics) — independent of rent/pop signal availability.
        econ_raw = c.get("economics_score") if isinstance(c, dict) else None
        economics_score: float | None
        if econ_raw is None:
            economics_score = None
        else:
            try:
                ev = float(econ_raw)
                economics_score = ev if not (math.isnan(ev) or math.isinf(ev)) else None
            except (TypeError, ValueError):
                economics_score = None
        econ_demote = economics_score is not None and economics_score < economics_min

        # ── Growth signal: NASA Black Marble VNP46A3 YoY radiance growth.
        # Rescues the population and rent legs when confident & growing.
        # Independent of rent/pop data presence.
        fs = c.get("feature_snapshot_json")
        rad = fs.get("radiance_growth") if isinstance(fs, dict) else None
        rad_confident = False
        rad_yoy_pct: float | None = None
        if isinstance(rad, dict):
            rad_confident = bool(rad.get("confident"))
            yoy_raw = rad.get("value_yoy_pct")
            if yoy_raw is not None:
                try:
                    candidate_yoy = float(yoy_raw)
                    if not (math.isnan(candidate_yoy) or math.isinf(candidate_yoy)):
                        rad_yoy_pct = candidate_yoy
                except (TypeError, ValueError):
                    rad_yoy_pct = None
            radiance_meta = {
                "radiance_growth_pct": rad_yoy_pct,
                "radiance_confident": rad_confident,
                "radiance_pixel_count": rad.get("pixel_count"),
                "radiance_year_month": rad.get("year_month"),
            }
        growth_rescue = bool(
            rad_confident
            and rad_yoy_pct is not None
            and rad_yoy_pct >= radiance_yoy_threshold
        )

        # ── Clause 2 (rent) — evaluate independently against rent_burden block.
        # Missing rent data → rent_demote stays False (defensive: don't penalize
        # candidates whose rent percentile we can't measure).
        sb = c.get("score_breakdown_json")
        ed = sb.get("economics_detail") if isinstance(sb, dict) else None
        rb = ed.get("rent_burden") if isinstance(ed, dict) else None
        rent_scope: str | None = None
        rent_pct: float = 0.0
        rent_demote = False
        if isinstance(rb, dict):
            rent_scope = rb.get("source_label")
            rent_pct_val = rb.get("percentile")
            if rent_pct_val is not None:
                rent_pct = _safe_float(rent_pct_val, default=-1.0)
                rent_confident = rent_scope not in ("city_band_type", "city")
                rent_high = rent_pct >= rent_pct_threshold
                rent_demote = bool(
                    rent_confident and rent_high and not growth_rescue
                )

        # ── Clause 1 (population) — evaluate independently against feature_snapshot.
        # Missing population_reach → pop_demote stays False (defensive: don't
        # penalize candidates whose population we couldn't measure).
        pop_reach: float = 0.0
        pop_demote = False
        if isinstance(fs, dict):
            pop_raw = fs.get("population_reach")
            if pop_raw is not None:
                pop_reach = _safe_float(pop_raw, default=-1.0)
                # Confident when a value is PRESENT (not None), so a measured
                # 0.0 can demote. -1.0 is the parse-failure sentinel from
                # _safe_float and stays non-confident (treated as unmeasured).
                pop_confident = pop_reach >= 0
                pop_low = pop_reach < pop_threshold
                pop_demote = bool(
                    pop_confident and pop_low and not growth_rescue
                )

        # ── Clause "strong potential for sales" (B3) — realized-demand leg.
        # NO growth_rescue — sales potential and growth are distinct pillars
        # in the directive; conflating them re-introduces the false-positive
        # class this leg exists to suppress. Mirrors the economics-leg
        # precedent in the docstring above.
        demand_value_raw = fs.get("realized_demand_30d") if isinstance(fs, dict) else None
        demand_branches_raw = fs.get("realized_demand_branches") if isinstance(fs, dict) else None
        demand_value: float | None
        if isinstance(demand_value_raw, (int, float)) and not isinstance(demand_value_raw, bool):
            dv = float(demand_value_raw)
            demand_value = dv if not (math.isnan(dv) or math.isinf(dv)) else None
        else:
            demand_value = None
        demand_branches: int | None
        if isinstance(demand_branches_raw, (int, float)) and not isinstance(demand_branches_raw, bool):
            demand_branches = int(demand_branches_raw)
        else:
            demand_branches = None
        demand_confident = (
            demand_threshold is not None
            and demand_branches is not None
            and demand_branches >= demand_min_branches
            and demand_value is not None
        )
        demand_low = (
            demand_confident
            and demand_value is not None
            and demand_value < demand_threshold
        )
        demand_demote = bool(demand_confident and demand_low)

        # ── Pillar 3 (radiance growth) — NASA Black Marble VNP46A3 YoY
        # demote leg. Fires when ``radiance_growth.confident`` is True AND
        # ``value_yoy_pct`` < the demote threshold (operator strict ``<``).
        # NO growth-rescue (mirrors the economics and demand legs): a leg
        # whose own signal is forward-looking cannot self-rescue. Defensive
        # silent-pass when the kill switch is off, the block is missing,
        # the signal is not confident, or the value is None.
        radiance_growth_demote = False
        if radiance_growth_leg_enabled and isinstance(rad, dict):
            if rad_confident and rad_yoy_pct is not None:
                radiance_growth_demote = bool(
                    rad_yoy_pct < radiance_yoy_demote_threshold
                )

        return (
            pop_demote, rent_demote, econ_demote, demand_demote,
            radiance_growth_demote,
            rent_pct, rent_scope, pop_reach, economics_score, radiance_meta,
            demand_value, demand_branches,
        )

    pre_eval = [_flag_inputs(c) for c in out]

    # Score-delta refactor: instead of swapping list positions, attach the
    # legs that fired and the resulting delta (-10 each, stacking) to every
    # candidate. The caller folds ``viability_delta`` into final_score once
    # and re-sorts. ``market_viability_flag`` is still written for every
    # candidate where any leg fired, mirroring the legacy persisted shape
    # (minus the now-meaningless ``demotion_steps`` key).
    demoted = 0
    for i in range(n):
        c = out[i]
        (
            pop_demote, rent_demote, econ_demote, demand_demote,
            radiance_growth_demote,
            rent_pct, rent_scope, pop_reach, economics_score, radiance_meta,
            demand_value, demand_branches,
        ) = pre_eval[i]
        rpc_demote = rpc_demote_by_id.get(id(c), False)
        # Stable-order annotation: rpc (rent_per_capita), clause 1 (pop),
        # clause 2 (rent), clause 3 (economics), clause "sales potential"
        # (demand), clause "growth potential" (radiance growth).
        reasons: list[str] = []
        if rpc_demote:
            reasons.append("rent_per_capita_high")
        if pop_demote:
            reasons.append("population_below_quartile")
        if rent_demote:
            reasons.append("rent_high")
        if econ_demote:
            reasons.append("economics_below_threshold")
        if demand_demote:
            reasons.append("demand_low")
        if radiance_growth_demote:
            reasons.append("radiance_growth_low")

        any_leg_fired = bool(reasons)
        viability_delta = -10.0 * len(reasons)
        # Stash on the candidate dict so the caller can apply the delta.
        # These are transient working fields and the caller drops them
        # after folding into bonus_detail.
        c["viability_legs_fired"] = list(reasons)
        c["viability_delta"] = float(viability_delta)

        if any_leg_fired:
            sb = c.get("score_breakdown_json")
            if not isinstance(sb, dict):
                sb = {}
                c["score_breakdown_json"] = sb
            flag_dict: dict[str, Any] = {
                "demoted": True,
                "rent_percentile": float(rent_pct),
                "rent_source_label": rent_scope,
                "population_reach": float(pop_reach),
                "population_threshold": float(pop_threshold),
                "economics_score": (
                    float(economics_score) if economics_score is not None else None
                ),
                "economics_threshold": float(economics_min),
                "population_demote": pop_demote,
                "rent_demote": rent_demote,
                "economics_demote": econ_demote,
                "realized_demand_30d": (
                    float(demand_value) if demand_value is not None else None
                ),
                "realized_demand_branches": demand_branches,
                "realized_demand_threshold": (
                    float(demand_threshold) if demand_threshold is not None else None
                ),
                "demand_demote": demand_demote,
                "radiance_growth_demote": radiance_growth_demote,
                "radiance_yoy_demote_threshold": float(radiance_yoy_demote_threshold),
                "reason": "_and_".join(reasons),
                "radiance_growth_pct": radiance_meta["radiance_growth_pct"],
                "radiance_confident": radiance_meta["radiance_confident"],
                "radiance_pixel_count": radiance_meta["radiance_pixel_count"],
                "radiance_year_month": radiance_meta["radiance_year_month"],
            }
            if rpc_active:
                flag_dict.update(rpc_telemetry_by_id.get(id(c), {
                    "rent_per_capita_sar": None,
                    "rent_per_capita_pct": None,
                    "rent_per_capita_demote": None,
                }))
            sb["market_viability_flag"] = flag_dict
            demoted += 1

    # Write rpc telemetry to candidates the rpc leg evaluated but no other
    # leg fired (no market_viability_flag block above). When the rpc leg is
    # inactive (cohort below the min-cohort floor), no flag writes happen —
    # per the leg's contract.
    if rpc_active:
        for c in out:
            cid = id(c)
            telemetry = rpc_telemetry_by_id.get(cid)
            if telemetry is None:
                continue
            sb = c.get("score_breakdown_json")
            if not isinstance(sb, dict):
                sb = {}
                c["score_breakdown_json"] = sb
            mvf = sb.get("market_viability_flag")
            if not isinstance(mvf, dict):
                mvf = {}
                sb["market_viability_flag"] = mvf
            mvf.update(telemetry)

    pop_demoted = sum(1 for pe in pre_eval if pe[0])
    rent_demoted = sum(1 for pe in pre_eval if pe[1])
    econ_demoted = sum(1 for pe in pre_eval if pe[2])
    demand_demoted = sum(1 for pe in pre_eval if pe[3])
    radiance_demoted = sum(1 for pe in pre_eval if pe[4])
    rpc_demoted = sum(1 for v in rpc_demote_by_id.values() if v)
    if demoted:
        logger.info(
            "expansion_market_viability_pass: search_id=%s demoted=%d "
            "pop_leg=%d rent_leg=%d econ_leg=%d demand_leg=%d "
            "radiance_growth_leg=%d "
            "rent_pct_threshold=%.2f pop_percentile=%.2f pop_threshold=%.0f "
            "economics_min=%.2f demand_percentile=%.2f demand_threshold=%s "
            "radiance_yoy_demote_threshold=%.2f "
            "cohort_n=%d demand_cohort_n=%d",
            search_id, demoted, pop_demoted, rent_demoted, econ_demoted,
            demand_demoted, radiance_demoted,
            rent_pct_threshold, pop_percentile_threshold, pop_threshold,
            economics_min, demand_percentile_threshold,
            ("%.2f" % demand_threshold) if demand_threshold is not None else "None",
            radiance_yoy_demote_threshold,
            len(pop_values), len(demand_values),
        )

    # Per-leg diagnostics: parallel block to ``hard_floors`` (written
    # earlier in this function before the soft-demote loop). Placed AFTER
    # the pre_eval loop so the per-leg counters are populated. Always
    # written when ``diagnostics`` is provided so the response shape stays
    # stable even when no leg fired.
    if diagnostics is not None:
        diagnostics["demote_legs"] = {
            "drops": {
                "dropped_population": pop_demoted,
                "dropped_rent": rent_demoted,
                "dropped_economics": econ_demoted,
                "dropped_demand": demand_demoted,
                "dropped_radiance_growth": radiance_demoted,
                "dropped_rent_per_capita": rpc_demoted,
            },
            "thresholds": {
                "rent_pct_threshold": float(rent_pct_threshold),
                "pop_percentile": float(pop_percentile_threshold),
                "pop_threshold": (
                    float(pop_threshold) if pop_threshold is not None else None
                ),
                "economics_min": float(economics_min),
                "demand_percentile": float(demand_percentile_threshold),
                "demand_threshold": (
                    float(demand_threshold) if demand_threshold is not None else None
                ),
                "demand_min_branches": int(demand_min_branches),
                "radiance_yoy_demote_threshold": float(radiance_yoy_demote_threshold),
                "rpc_percentile": float(rpc_percentile_threshold),
                "rpc_threshold": (
                    float(rpc_threshold) if rpc_threshold is not None else None
                ),
                "rpc_min_cohort": int(rpc_min_cohort),
                "rpc_cohort_n": len(valid_rpc),
            },
            "leg_enabled": {
                "demand": demand_leg_enabled,
                "radiance_growth": radiance_growth_leg_enabled,
                "rent_per_capita": rpc_active,
            },
        }
    return out


def _build_strengths_and_risks(
    *,
    demand_score: float,
    whitespace_score: float,
    fit_score: float,
    cannibalization_score: float,
    rent_source: str,
) -> tuple[list[str], list[str], list[dict[str, Any]], list[dict[str, Any]]]:
    strengths: list[str] = []
    risks: list[str] = []
    # PR #3: parallel locale-invariant structured records. The English
    # append literals below are byte-untouched; the structured side
    # mirrors the same six firing conditions in the same order
    # (deliberately NOT DRY'd — same convention as
    # _recommended_use_case / _recommended_use_case_token). All six
    # conditions are zero-param, so params is always {}.
    strengths_structured: list[dict[str, Any]] = []
    risks_structured: list[dict[str, Any]] = []
    if demand_score >= 70:
        strengths.append("High demand index supports branch throughput")
        strengths_structured.append({"id": "S1", "params": {}})
    if whitespace_score >= 65:
        strengths.append("Competitive whitespace remains attractive")
        strengths_structured.append({"id": "S2", "params": {}})
    if fit_score >= 70:
        strengths.append("Parcel characteristics align with target format")
        strengths_structured.append({"id": "S3", "params": {}})
    if rent_source == "conservative_default":
        risks.append("Rent benchmark fell back to conservative city default (lower confidence)")
        risks_structured.append({"id": "R1", "params": {}})
    if cannibalization_score >= 70:
        risks.append("High overlap risk with existing branches")
        risks_structured.append({"id": "R2", "params": {}})
    if whitespace_score <= 45:
        risks.append("Competitive density may pressure launch economics")
        risks_structured.append({"id": "R3", "params": {}})
    return strengths[:4], risks[:4], strengths_structured[:4], risks_structured[:4]


# PR #4d: Arabic phrases for the recommended use case, keyed by the
# locale-invariant token from _recommended_use_case_token. The English
# branches below are byte-untouched; the AR branch dispatches through
# the existing token mirror so the two stay in lockstep.
_RECOMMENDED_USE_CASE_AR: dict[str, str] = {
    "flagship_dine_in": "مطعم رئيسي للتناول في الموقع",
    "neighborhood_dine_in": "مطعم تناول في الموقع للحي",
    "delivery_led_branch": "فرع يعتمد على التوصيل",
    "compact_cafe": "مقهى صغير",
    "destination_cafe": "مقهى وجهة",
    "neighborhood_qsr": "مطعم خدمة سريعة للحي",
}


def _recommended_use_case(service_model: str, area_m2: float, lang: str = "en") -> str:
    if lang == "ar":
        token = _recommended_use_case_token(service_model, area_m2)
        return _RECOMMENDED_USE_CASE_AR.get(token, _RECOMMENDED_USE_CASE_AR["neighborhood_qsr"])
    if service_model == "dine_in":
        return "flagship dine-in" if area_m2 >= 260 else "neighborhood dine-in"
    if service_model == "delivery_first":
        return "delivery-led branch"
    if service_model == "cafe":
        return "compact cafe" if area_m2 < 180 else "destination cafe"
    return "neighborhood qsr"


def _recommended_use_case_token(service_model: str, area_m2: float) -> str:
    """PR #2a: locale-invariant token mirror of _recommended_use_case.

    The English-returning _recommended_use_case above is byte-untouched;
    this sibling returns the matching translation key for the structured
    record. The branch logic is intentionally duplicated (NOT DRY'd).
    """
    if service_model == "dine_in":
        return "flagship_dine_in" if area_m2 >= 260 else "neighborhood_dine_in"
    if service_model == "delivery_first":
        return "delivery_led_branch"
    if service_model == "cafe":
        return "compact_cafe" if area_m2 < 180 else "destination_cafe"
    return "neighborhood_qsr"


def _decision_summary(
    *,
    district: str | None,
    final_score: float,
    economics_score: float,
    key_risks: list[str],
    service_model: str,
    area_m2: float,
    key_risks_structured: list[dict[str, Any]] | None = None,
) -> tuple[str, dict[str, Any]]:
    area_label = "compact" if area_m2 < 180 else "standard"
    district_label = district or "the target district"
    if key_risks:
        risk_text = key_risks[0]
    elif economics_score < 55:
        risk_text = (
            "rent economics are tight and should be validated with actual lease terms"
        )
    else:
        risk_text = (
            "execution risk should be managed during leasing and design"
        )
    summary = (
        f"This {area_label} candidate in {district_label} scores {final_score:.1f}/100 overall with an economics score of {economics_score:.1f}/100. "
        f"It is a practical first-pass option for {_recommended_use_case(service_model, area_m2)}."
    )
    risk_sentence = risk_text.strip().rstrip(".").strip()
    if risk_sentence:
        if not risk_sentence[0].isupper():
            risk_sentence = risk_sentence[0].upper() + risk_sentence[1:]
        summary = f"{summary} Biggest commercial risk: {risk_sentence}."
    # PR #2a: structured record built from the same inputs as the English
    # summary above. The English branches are NOT edited; risk_kind is
    # re-derived in this parallel block (deliberately NOT DRY'd).
    if key_risks:
        risk_kind = "from_key_risks"
    elif economics_score < 55:
        risk_kind = "tight_economics"
    else:
        risk_kind = "execution"
    # risk_text_en is retained as the dual-read bridge: post-PR-2a-pre-PR-3
    # rows carry only risk_text_en, and the ar renderer falls back to it
    # when no risk_id is present (PR #3 Q3). PR #3 adds the sibling
    # risk_id below so post-PR-3 rows render a localized Arabic clause.
    structured = {
        "id": "decision_summary",
        "params": {
            "area_label": area_label,
            "district_label": district,
            "final_score": final_score,
            "economics_score": economics_score,
            "use_case": _recommended_use_case_token(service_model, area_m2),
            "risk_kind": risk_kind,
            "risk_text_en": risk_text,
        },
    }
    # PR #3: when the risk clause is spliced from key_risks, persist the
    # structured risk id (index-aligned with key_risks — both lists are
    # built from the same six firing conditions in _build_strengths_and_risks)
    # so the Arabic read path can render a localized clause.
    if risk_kind == "from_key_risks" and key_risks_structured:
        _first_risk = key_risks_structured[0]
        if isinstance(_first_risk, dict) and _first_risk.get("id"):
            structured["params"]["risk_id"] = _first_risk["id"]
    return summary, structured


def persist_existing_branches(db: Session, search_id: str, existing_branches: list[dict[str, Any]]) -> None:
    if not existing_branches:
        return
    insert_sql = text(
        """
        INSERT INTO expansion_branch (
            id,
            search_id,
            name,
            lat,
            lon,
            district,
            source
        ) VALUES (
            :id,
            :search_id,
            :name,
            :lat,
            :lon,
            :district,
            :source
        )
        """
    )
    for branch in existing_branches:
        try:
            with db.begin_nested():
                db.execute(
                    insert_sql,
                    {
                        "id": str(uuid.uuid4()),
                        "search_id": search_id,
                        "name": branch.get("name"),
                        "lat": _safe_float(branch.get("lat")),
                        "lon": _safe_float(branch.get("lon")),
                        "district": branch.get("district"),
                        "source": branch.get("source") or "manual",
                    },
                )
        except Exception:
            logger.warning(
                "Failed to persist existing branch name=%s search_id=%s – skipping",
                branch.get("name"), search_id,
                exc_info=True,
            )




def persist_brand_profile(db: Session, search_id: str, brand_profile: dict[str, Any]) -> None:
    profile = _default_brand_profile(brand_profile)
    # "Describe your brand" extraction audit metadata (design §6.1). Present
    # only when the user typed a brief; columns stay NULL otherwise.
    extraction_meta = profile.get("brief_extraction") or {}
    if not isinstance(extraction_meta, dict):
        extraction_meta = {}
    extraction_json = extraction_meta.get("extraction_json")
    edited_fields = extraction_meta.get("edited_fields")
    try:
        with db.begin_nested():
            db.execute(
                text(
                    """
                    INSERT INTO expansion_brand_profile (
                        id, search_id, price_tier, average_check_sar, primary_channel,
                        parking_sensitivity, frontage_sensitivity, visibility_sensitivity,
                        expansion_goal, brand_archetype, cannibalization_tolerance_m,
                        preferred_districts_json, excluded_districts_json,
                        brief_text, brief_extraction_json, brief_extraction_model,
                        brief_extraction_prompt_version, brief_extraction_accepted,
                        brief_extraction_edited_fields_json
                    ) VALUES (
                        :id, :search_id, :price_tier, :average_check_sar, :primary_channel,
                        :parking_sensitivity, :frontage_sensitivity, :visibility_sensitivity,
                        :expansion_goal, :brand_archetype, :cannibalization_tolerance_m,
                        CAST(:preferred_districts_json AS jsonb), CAST(:excluded_districts_json AS jsonb),
                        :brief_text, CAST(:brief_extraction_json AS jsonb), :brief_extraction_model,
                        :brief_extraction_prompt_version, :brief_extraction_accepted,
                        CAST(:brief_extraction_edited_fields_json AS jsonb)
                    )
                    ON CONFLICT (search_id) DO UPDATE SET
                        price_tier = EXCLUDED.price_tier,
                        average_check_sar = EXCLUDED.average_check_sar,
                        primary_channel = EXCLUDED.primary_channel,
                        parking_sensitivity = EXCLUDED.parking_sensitivity,
                        frontage_sensitivity = EXCLUDED.frontage_sensitivity,
                        visibility_sensitivity = EXCLUDED.visibility_sensitivity,
                        expansion_goal = EXCLUDED.expansion_goal,
                        brand_archetype = EXCLUDED.brand_archetype,
                        cannibalization_tolerance_m = EXCLUDED.cannibalization_tolerance_m,
                        preferred_districts_json = EXCLUDED.preferred_districts_json,
                        excluded_districts_json = EXCLUDED.excluded_districts_json,
                        brief_text = EXCLUDED.brief_text,
                        brief_extraction_json = EXCLUDED.brief_extraction_json,
                        brief_extraction_model = EXCLUDED.brief_extraction_model,
                        brief_extraction_prompt_version = EXCLUDED.brief_extraction_prompt_version,
                        brief_extraction_accepted = EXCLUDED.brief_extraction_accepted,
                        brief_extraction_edited_fields_json = EXCLUDED.brief_extraction_edited_fields_json,
                        updated_at = now()
                    """
                ),
                {
                    "id": str(uuid.uuid4()),
                    "search_id": search_id,
                    "price_tier": profile.get("price_tier"),
                    "average_check_sar": profile.get("average_check_sar"),
                    "primary_channel": profile.get("primary_channel"),
                    "parking_sensitivity": profile.get("parking_sensitivity"),
                    "frontage_sensitivity": profile.get("frontage_sensitivity"),
                    "visibility_sensitivity": profile.get("visibility_sensitivity"),
                    "expansion_goal": profile.get("expansion_goal"),
                    "brand_archetype": profile.get("brand_archetype"),
                    "cannibalization_tolerance_m": profile.get("cannibalization_tolerance_m"),
                    "preferred_districts_json": json.dumps(profile.get("preferred_districts") or [], ensure_ascii=False),
                    "excluded_districts_json": json.dumps(profile.get("excluded_districts") or [], ensure_ascii=False),
                    "brief_text": profile.get("brief_text"),
                    "brief_extraction_json": (
                        json.dumps(extraction_json, ensure_ascii=False)
                        if extraction_json is not None
                        else None
                    ),
                    "brief_extraction_model": extraction_meta.get("model"),
                    "brief_extraction_prompt_version": extraction_meta.get("prompt_version"),
                    "brief_extraction_accepted": extraction_meta.get("accepted"),
                    "brief_extraction_edited_fields_json": (
                        json.dumps(edited_fields, ensure_ascii=False)
                        if edited_fields is not None
                        else None
                    ),
                },
            )
    except Exception:
        logger.warning(
            "Failed to persist brand profile search_id=%s – continuing without it",
            search_id,
            exc_info=True,
        )


def get_brand_profile(db: Session, search_id: str) -> dict[str, Any] | None:
    row = db.execute(text("""
        SELECT price_tier, average_check_sar, primary_channel, parking_sensitivity, frontage_sensitivity,
               visibility_sensitivity, expansion_goal, brand_archetype, cannibalization_tolerance_m,
               preferred_districts_json, excluded_districts_json, brief_text
        FROM expansion_brand_profile WHERE search_id = :search_id
    """), {"search_id": search_id}).mappings().first()
    if not row:
        return None
    data = dict(row)
    data["preferred_districts"] = data.pop("preferred_districts_json") or []
    data["excluded_districts"] = data.pop("excluded_districts_json") or []
    # brief_text rides into memo context as operator color (design §6.2);
    # dropped when NULL so legacy profiles keep their exact shape.
    if data.get("brief_text") is None:
        data.pop("brief_text", None)
    return data


_NUMERIC_COORD_RE = r"'^[-+]?[0-9]*\.?[0-9]+$'"


def _coord_text(alias: str, column: str) -> str:
    """Return trimmed text expression for a lon/lat SQL column.
    Works for both numeric and text-backed schemas."""
    return f"BTRIM(CAST({alias}.{column} AS text))"


def _log_dirty_coord_samples(db: Session, search_id: str) -> None:
    """Log up to 10 non-numeric lat/lon samples from delivery_source_record
    and population_density.  Called only when the main candidate query fails,
    to aid root-cause diagnosis.  Best-effort: any error is swallowed."""
    for table, alias in [("delivery_source_record", "dsr"), ("population_density", "pd")]:
        try:
            lon_text = _coord_text(alias, "lon")
            lat_text = _coord_text(alias, "lat")
            sample_sql = text(
                f"SELECT {alias}.lat, {alias}.lon"
                f" FROM {table} {alias}"
                f" WHERE ({alias}.lat IS NOT NULL OR {alias}.lon IS NOT NULL)"
                f"   AND (NULLIF({lon_text}, '') !~ {_NUMERIC_COORD_RE}"
                f"        OR NULLIF({lat_text}, '') !~ {_NUMERIC_COORD_RE})"
                f" LIMIT 10"
            )
            with db.begin_nested():
                bad_rows = db.execute(sample_sql).mappings().all()
            if bad_rows:
                samples = [(r["lat"], r["lon"]) for r in bad_rows]
                logger.warning(
                    "Dirty coordinate samples in %s (search_id=%s): %s",
                    table, search_id, samples,
                )
        except Exception:
            logger.debug(
                "Could not query dirty coord samples from %s", table, exc_info=True,
            )


def _query_candidate_location_pool(
    db: Session,
    *,
    target_district_norm: set[str],
    min_area_m2: float,
    max_area_m2: float,
    target_area_m2: float,
    per_district_cap: int = 40,
    limit: int = 600,
) -> list[dict]:
    """Query candidate_location table for the expansion advisor candidate pool.

    Returns rows with the same column names the scoring loop expects:
    parcel_id, lat, lon, area_m2, district, landuse_label, landuse_code,
    plus commercial-unit fields for Tier 1 candidates.

    Uses stratified sampling: ROW_NUMBER per district, capped at per_district_cap,
    with global limit. Prioritizes Tier 1 > Tier 2 > Tier 3.
    """
    from sqlalchemy import text as sa_text

    # Build district filter
    district_filter = ""
    params: dict[str, Any] = {
        "min_area": min_area_m2,
        "max_area": max_area_m2,
        "target_area": target_area_m2,
        "per_district_cap": per_district_cap,
        "limit": limit,
    }

    # Arabic normalization in SQL: must mirror Python normalize_district_key().
    # 1. Strip NBSP (\u00A0), bidi marks (\u200F \u200E \u202A-\u202E \u2066-\u2069),
    #    zero-width chars (\u200B-\u200D \uFEFF).
    # 2. TRANSLATE: أ→ا إ→ا آ→ا ى→ي, delete tatweel.
    # 3. REGEXP_REPLACE: strip leading "حي " prefix.
    _CL_STRIP_INVISIBLE = (
        "REGEXP_REPLACE("
        "REPLACE(COALESCE(cl.district_ar, ''), E'\\u00A0', ' '), "
        "E'[\\u200B-\\u200F\\u202A-\\u202E\\u2066-\\u2069\\uFEFF]', '', 'g'"
        ")"
    )
    _CL_NORM_SQL = (
        "TRIM(REGEXP_REPLACE("
        "TRANSLATE("
        f"{_CL_STRIP_INVISIBLE}, "
        "E'\\u0623\\u0625\\u0622\\u0649\\u0640', "
        "E'\\u0627\\u0627\\u0627\\u064A'"
        "), "
        "E'^\\u062D\\u064A\\\\s+', '', 'g'"
        "))"
    )

    if target_district_norm:
        district_clauses = []
        for i, td in enumerate(sorted(target_district_norm)):
            pname = f"td_{i}"
            district_clauses.append(f"lower({_CL_NORM_SQL}) = :{pname}")
            params[pname] = td.lower()
        district_filter = "AND (" + " OR ".join(district_clauses) + ")"

        # ── Debug: log resolved district filter values and per-district row counts ──
        _debug_params = {f"td_{i}": td.lower() for i, td in enumerate(sorted(target_district_norm))}
        logger.info(
            "candidate_location_pool district filter: resolved_arabic_values=%s, "
            "sql_param_values=%s, num_districts=%d",
            sorted(target_district_norm),
            _debug_params,
            len(target_district_norm),
        )
        # Count matching rows per district_ar BEFORE the district_rank window
        # to diagnose which districts the SQL TRANSLATE normalization actually matches.
        try:
            _diag_sql = sa_text(f"""
                SELECT
                    cl.district_ar,
                    {_CL_NORM_SQL} AS norm_district,
                    COUNT(*) AS cnt
                FROM candidate_location cl
                WHERE cl.is_cluster_primary = TRUE
                  AND cl.source_tier = 1
                  AND cl.geom IS NOT NULL
                  AND COALESCE(cl.area_sqm, 120) BETWEEN :min_area AND :max_area
                  AND (cl.rent_sar_m2_month IS NULL OR cl.rent_sar_m2_month >= 12)
                  {district_filter}
                GROUP BY cl.district_ar, {_CL_NORM_SQL}
                ORDER BY cnt DESC
            """)
            _diag_rows = db.execute(_diag_sql, params).mappings().all()
            _diag_summary = {
                str(r["district_ar"]): {"norm": str(r["norm_district"]), "count": int(r["cnt"])}
                for r in _diag_rows
            }
            logger.info(
                "candidate_location_pool district diagnostics: matched_districts=%s",
                _diag_summary,
            )
        except Exception as _diag_exc:
            logger.warning("candidate_location_pool district diagnostics failed: %s", _diag_exc)

    sql = sa_text(f"""
        WITH ranked AS (
            SELECT
                cl.id,
                cl.source_tier,
                cl.source_type,
                cl.source_id,
                cl.lat::float AS lat,
                cl.lon::float AS lon,
                COALESCE(cl.area_sqm, 120)::float AS area_m2,
                cl.district_ar AS district,
                COALESCE(cl.landuse_label, 'commercial') AS landuse_label,
                COALESCE(cl.landuse_code, 2000) AS landuse_code,
                -- Commercial unit fields (Tier 1)
                CASE WHEN cl.source_tier = 1 THEN cl.source_id ELSE NULL END AS commercial_unit_id,
                CASE WHEN cl.source_tier = 1 THEN cl.rent_sar_annual::float ELSE NULL END AS unit_price_sar_annual,
                cl.area_sqm::float AS unit_area_sqm,
                cl.street_width_m::float AS unit_street_width_m,
                cl.listing_url,
                cl.image_url,
                cl.listing_type AS unit_listing_type,
                -- Candidate metadata (passed through to response)
                cl.is_vacant,
                cl.current_tenant,
                cl.current_category,
                cl.rent_confidence,
                cl.area_confidence,
                cl.rent_sar_m2_month::float AS cl_rent_m2_month,
                cl.rent_sar_annual::float AS cl_rent_annual,
                cl.avg_rating::float AS cl_avg_rating,
                cl.total_rating_count,
                cl.platform_count AS cl_platform_count,
                cl.profitability_score::float AS profitability_score,
                -- Commercial unit signals (Tier 1 only, via LEFT JOIN)
                cu.is_furnished AS unit_is_furnished,
                cu.first_seen_at AS unit_first_seen_at,
                cu.last_seen_at AS unit_last_seen_at,
                cu.aqar_created_at AS unit_aqar_created_at,
                cu.aqar_updated_at AS unit_aqar_updated_at,
                cu.restaurant_score AS unit_restaurant_score,
                cu.has_drive_thru AS unit_has_drive_thru,
                cu.neighborhood AS unit_neighborhood_raw,
                cu.llm_suitability_score AS unit_llm_suitability_score,
                cu.llm_listing_quality_score AS unit_llm_listing_quality_score,
                cu.llm_landlord_signal_score AS unit_llm_landlord_signal_score,
                cu.llm_suitability_verdict AS unit_llm_suitability_verdict,
                cu.llm_classified_at AS unit_llm_classified_at,
                -- Scoring helpers
                ABS(COALESCE(cl.area_sqm, 120) - :target_area) AS area_distance,
                0 AS delivery_listing_count,
                0 AS delivery_cat_count,
                0 AS delivery_platform_count,
                -- Placeholder; overwritten by _bulk_enrich_population. NULL
                -- (not 0) so a row that never gets enriched reads as
                -- "unmeasured", never "measured zero", on the viability path.
                CAST(NULL AS double precision) AS population_reach,
                ROW_NUMBER() OVER (
                    PARTITION BY cl.district_ar
                    ORDER BY
                        cl.source_tier ASC,
                        ABS(COALESCE(cl.area_sqm, 120) - :target_area) ASC,
                        cl.id ASC
                ) AS district_rank
            FROM candidate_location cl
            INNER JOIN commercial_unit cu
                   ON cl.source_tier = 1
                  AND cl.source_id = cu.aqar_id
                  AND cu.listing_type IN ('store', 'showroom')
                  -- Patch 12 design decision (revisit in Patch 13):
                  -- This clause is recall-favoring.  We accept both
                  -- 'suitable' and 'uncertain' LLM verdicts because
                  -- sparse listings with thin scraper-extracted
                  -- descriptions are mostly legitimate listings the
                  -- scraper extracted poorly, not non-F&B junk.  The
                  -- Patch 08 listing_type whitelist still protects
                  -- against the worst structural cases.  Patch 13
                  -- should measure the production rate of 'uncertain'
                  -- verdicts and tighten this to 'suitable'-only if
                  -- the rate is below ~5%.
                  AND (
                      (cu.llm_classified_at IS NOT NULL
                       AND cu.llm_suitability_verdict IN ('suitable', 'uncertain'))
                      OR
                      -- Structural fallback for rows not yet LLM-classified.
                      (cu.llm_classified_at IS NULL
                       AND cu.restaurant_suitable = TRUE)
                  )
            WHERE cl.is_cluster_primary = TRUE
              AND cl.source_tier = 1
              AND cl.geom IS NOT NULL
              AND COALESCE(cl.area_sqm, 120) BETWEEN :min_area AND :max_area
              AND (cl.rent_sar_m2_month IS NULL OR cl.rent_sar_m2_month >= 12)
              {district_filter}
        )
        SELECT
            COALESCE(source_id, id::text) AS parcel_id,
            source_tier,
            source_type,
            lat, lon, area_m2,
            district,
            landuse_label, landuse_code,
            commercial_unit_id,
            unit_price_sar_annual,
            unit_area_sqm,
            unit_street_width_m,
            listing_url, image_url,
            unit_listing_type,
            is_vacant,
            current_tenant,
            current_category,
            rent_confidence,
            area_confidence,
            cl_rent_m2_month,
            cl_rent_annual,
            cl_avg_rating,
            total_rating_count,
            cl_platform_count,
            profitability_score,
            unit_is_furnished,
            unit_first_seen_at,
            unit_last_seen_at,
            unit_aqar_created_at,
            unit_aqar_updated_at,
            unit_restaurant_score,
            unit_has_drive_thru,
            unit_neighborhood_raw,
            unit_llm_suitability_score,
            unit_llm_listing_quality_score,
            unit_llm_landlord_signal_score,
            unit_llm_suitability_verdict,
            unit_llm_classified_at,
            delivery_listing_count,
            delivery_cat_count,
            delivery_platform_count,
            population_reach
        FROM ranked
        WHERE district_rank <= CAST(:per_district_cap AS integer)
        ORDER BY
            district_rank ASC,
            source_tier ASC,
            ABS(COALESCE(area_m2, 120) - :target_area) ASC,
            id ASC
        LIMIT :limit
    """)

    rows = db.execute(sql, params).mappings().all()
    return [dict(r) for r in rows]


def _query_commercial_unit_candidates(
    db: Session,
    target_district_norm: set[str],
    min_area_m2: float,
    max_area_m2: float,
    limit: int = 200,
) -> list[dict]:
    """Query commercial_unit table for restaurant-suitable units.

    Returns rows with the same key fields the scoring loop expects:
    parcel_id (mapped from aqar_id), lat, lon, area_m2, district,
    landuse_label, landuse_code, plus commercial-unit-specific fields.

    District filtering uses spatial proximity instead of name matching,
    because commercial units store English neighborhood names (from Aqar)
    while searches pass Arabic district names.  We look up the district
    centroid from riyadh_parcels_arcgis_raw and filter units within ~3 km.
    """
    from sqlalchemy import text as sa_text

    filters = [
        "cu.status = 'active'",
        "cu.lat IS NOT NULL",
        "cu.lon IS NOT NULL",
    ]
    params: dict[str, Any] = {}
    district_filter_mode = "none"

    # ── Spatial district filtering ──────────────────────────────────
    # Look up approximate centroid of the target district(s) from the
    # parcel table (which stores Arabic district_label) and filter
    # commercial units within a 3 km radius.
    if target_district_norm:
        try:
            district_values = ", ".join(
                f"(:td_{i})" for i in range(len(target_district_norm))
            )
            for i, td in enumerate(sorted(target_district_norm)):
                params[f"td_{i}"] = td

            # Mirrors normalize_district_key(): folds alef/ya variants,
            # strips حي prefix, trims whitespace.
            _NORM_SQL = normalize_district_key_sql("COALESCE(p.district_label, '')")

            centroid_sql = sa_text(f"""
                SELECT
                    AVG(ST_X(ST_Centroid(p.geom))) AS clon,
                    AVG(ST_Y(ST_Centroid(p.geom))) AS clat
                FROM public.riyadh_parcels_arcgis_raw p
                WHERE p.geom IS NOT NULL
                  AND {_NORM_SQL} IN (
                      SELECT td.val FROM (VALUES {district_values}) AS td(val)
                  )
            """)
            with db.begin_nested():
                centroid_row = db.execute(centroid_sql, params).mappings().first()

            if centroid_row and centroid_row["clon"] is not None and centroid_row["clat"] is not None:
                # Centroid clip radius is configurable. 0 disables the clip
                # entirely (no spatial restriction). Default 10 km — wide
                # enough to capture micro-markets on the periphery of the
                # named district while still anchoring the search.
                _clip_km = float(getattr(settings, "EXPANSION_CENTROID_CLIP_KM", 10.0) or 0.0)
                if _clip_km > 0:
                    params["district_clon"] = float(centroid_row["clon"])
                    params["district_clat"] = float(centroid_row["clat"])
                    params["district_clip_m"] = _clip_km * 1000.0
                    filters.append(
                        "ST_DWithin("
                        "  ST_SetSRID(ST_MakePoint(cu.lon::float, cu.lat::float), 4326)::geography,"
                        "  ST_SetSRID(ST_MakePoint(:district_clon, :district_clat), 4326)::geography,"
                        "  :district_clip_m"
                        ")"
                    )
                    district_filter_mode = "spatial"
                else:
                    district_filter_mode = "spatial_disabled"
            else:
                # Centroid lookup returned no rows – skip district filtering;
                # the scoring layer will still prefer the target district.
                district_filter_mode = "fallback_no_centroid"
        except Exception as exc:
            logger.warning(
                "commercial_unit spatial district lookup failed, skipping district filter: %s", exc,
            )
            district_filter_mode = "fallback_error"

    logger.info(
        "commercial_unit district filter: mode=%s, target_districts=%s",
        district_filter_mode, sorted(target_district_norm) if target_district_norm else [],
    )

    if min_area_m2 and min_area_m2 > 0:
        filters.append("cu.area_sqm >= :min_area")
        params["min_area"] = min_area_m2

    if max_area_m2 and max_area_m2 < 999999:
        filters.append("cu.area_sqm <= :max_area")
        params["max_area"] = max_area_m2

    where_clause = " AND ".join(filters)

    sql = sa_text(f"""
        SELECT
            cu.aqar_id AS parcel_id,
            cu.lat::float AS lat,
            cu.lon::float AS lon,
            COALESCE(cu.area_sqm, 100)::float AS area_m2,
            cu.neighborhood AS district,
            'commercial' AS landuse_label,
            2000 AS landuse_code,
            cu.price_sar_annual::float AS unit_price_sar_annual,
            cu.area_sqm::float AS unit_area_sqm,
            cu.street_width_m::float AS unit_street_width_m,
            cu.listing_url,
            cu.image_url,
            cu.aqar_id AS commercial_unit_id,
            cu.listing_type AS unit_listing_type,
            cu.restaurant_score,
            0 AS delivery_listing_count,
            0 AS delivery_cat_count,
            0 AS delivery_platform_count,
            -- Placeholder; overwritten by _bulk_enrich_population. NULL
            -- (not 0) so a row that never gets enriched reads as
            -- "unmeasured", never "measured zero", on the viability path.
            CAST(NULL AS double precision) AS population_reach
        FROM commercial_unit cu
        WHERE {where_clause}
        ORDER BY cu.restaurant_score DESC NULLS LAST, cu.price_sar_annual ASC NULLS LAST
        LIMIT :limit
    """)
    params["limit"] = limit

    rows = db.execute(sql, params).mappings().all()
    return [dict(r) for r in rows]


def _bulk_enrich_population(
    db: Session,
    rows: list[dict],
    demand_radius_m: float | None = None,
    *,
    service_model: str | None = None,
) -> dict[str, float | None]:
    """Bulk-compute population_reach for a set of candidate locations.

    Returns ``{parcel_id: population_reach}`` for all rows that have
    lat/lon. ``population_reach`` is ``None`` when the catchment has **no**
    population-grid coverage (zero grid rows), and a real summed value
    (including a genuine ``0.0``) when at least one grid row is in range.
    This lets callers distinguish "unmeasured" from "measured zero" — the
    viability floor/soft-leg must treat them differently. Scoring reads
    coalesce ``None`` → ``0.0`` via ``_safe_float`` and are unaffected.
    Uses a single SQL query with unnest + LATERAL to avoid N+1.

    The catchment radius comes from one of three sources, in priority
    order:
      1. An explicit ``demand_radius_m`` argument (for direct callers).
      2. The service_model lookup via ``_catchment_radii`` (for callers
         that know the brief's service model).
      3. The legacy 1200 m default (for safety; preserves old behavior
         if neither is supplied).
    """
    if demand_radius_m is None:
        if service_model is not None:
            demand_radius_m = _catchment_radii(service_model)["demand"]
        else:
            demand_radius_m = 1200.0

    if not rows:
        return {}

    # Build arrays of (parcel_id, lon, lat)
    pids = []
    lons = []
    lats = []
    for r in rows:
        pid = str(r.get("parcel_id") or r.get("id") or "")
        lon = r.get("lon")
        lat = r.get("lat")
        if pid and lon is not None and lat is not None:
            pids.append(pid)
            lons.append(float(lon))
            lats.append(float(lat))

    if not pids:
        return {}

    # Check if population_density has a geom column
    _pd_has_geom = False
    try:
        _pd_has_geom = bool(db.execute(text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'population_density' AND column_name = 'geom' LIMIT 1"
        )).scalar())
    except Exception:
        pass

    if _pd_has_geom:
        _pd_geo = "pd.geom::geography"
        _pd_where = "pd.geom IS NOT NULL"
    else:
        _pd_geo = "ST_SetSRID(ST_MakePoint(pd.lon::double precision, pd.lat::double precision), 4326)::geography"
        _pd_where = "pd.lat IS NOT NULL AND pd.lon IS NOT NULL"

    try:
        with db.begin_nested():
            result = db.execute(
                text(f"""
                    WITH inputs AS (
                        SELECT
                            unnest(CAST(:pids AS text[])) AS parcel_id,
                            unnest(CAST(:lons AS double precision[])) AS lon,
                            unnest(CAST(:lats AS double precision[])) AS lat
                    )
                    SELECT
                        i.parcel_id,
                        COALESCE(pop.population_reach, 0) AS population_reach,
                        COALESCE(pop.coverage_count, 0) AS coverage_count
                    FROM inputs i
                    LEFT JOIN LATERAL (
                        SELECT
                            COALESCE(SUM(pd.population), 0) AS population_reach,
                            COUNT(*) AS coverage_count
                        FROM population_density pd
                        WHERE {_pd_where}
                          AND ST_DWithin(
                              ST_SetSRID(ST_MakePoint(i.lon, i.lat), 4326)::geography,
                              {_pd_geo},
                              :radius_m
                          )
                    ) pop ON TRUE
                """),
                {"pids": pids, "lons": lons, "lats": lats, "radius_m": demand_radius_m},
            ).mappings().all()

        # coverage_count == 0 → no grid rows in catchment → unmeasured (None).
        # coverage_count > 0 → measured sum (which may legitimately be 0.0).
        return {
            str(r["parcel_id"]): (
                float(r["population_reach"])
                if int(r["coverage_count"] or 0) > 0
                else None
            )
            for r in result
        }
    except Exception as exc:
        logger.warning("Bulk population enrichment failed: %s", exc, exc_info=True)
        return {}


def _bulk_enrich_competitors(
    db: Session,
    rows: list[dict],
    category: str,
    competition_radius_m: float | None = None,
    *,
    service_model: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Bulk-compute competitor_count for a set of candidate locations.

    Returns ``{parcel_id: {"competitor_count": int, "confident": bool,
    "max_chain_strength": float | None, "chain_strength_share": float | None,
    "top_chain_strength_name": str | None}}`` for all rows that have lat/lon.
    Uses a single SQL query with unnest + LATERAL to avoid N+1.

    Searches both restaurant_poi (Google Places data) and
    delivery_source_record (HungerStation / delivery marketplace data) via
    UNION to ensure categories like shawarma and indian that only exist in
    delivery data are counted.

    F4: ``confident`` is True iff at least one observation (in any
    category) was returned by the underlying tables in the candidate's
    search radius. When both tables return zero rows, ``confident`` is
    False — the radius has thin POI coverage and the zero same-category
    count cannot be trusted as evidence of a true greenfield. The
    downstream whitespace scorer falls back to a neutral midpoint when
    confidence is False.

    Patch B: ``max_chain_strength`` is the MAX of
    ``expansion_competitor_quality.chain_strength_score`` across same-
    category restaurant_poi rows in the radius. None when there are no
    same-category POI matches (or the join produces no chain_strength
    rows). Delivery_source_record rows do not contribute (no POI mapping
    to chain quality), so the signal is from the Google Places side only.
    ``max_chain_strength`` is RETAINED only for the ``chain_strength_max``
    JSON diagnostic — it is no longer the leg input because a MAX saturates
    at 100 for any radius containing a single big chain.

    ``chain_strength_share`` is the chain_strength leg input: among same-
    category, ECQ-matched POIs in the radius (``chain_strength IS NOT NULL``
    excludes the NULL delivery side), the percentage whose
    ``chain_strength_score >= EXPANSION_CHAIN_STRONG_THRESHOLD``. NULL when
    fewer than ``EXPANSION_CHAIN_MIN_MATCHED`` matched POIs are in radius, so
    thin-evidence radii flow to Python None and ``_chain_strength_score``
    keeps returning the neutral 50.0 (never COALESCE'd to 0).

    The competition radius follows the same priority as
    ``_bulk_enrich_population``'s demand radius: explicit arg >
    service_model lookup > legacy 1000 m default.
    """
    if competition_radius_m is None:
        if service_model is not None:
            competition_radius_m = _catchment_radii(service_model)["competition"]
        else:
            competition_radius_m = 1000.0

    if not rows:
        return {}

    # Build arrays of (parcel_id, lon, lat)
    pids = []
    lons = []
    lats = []
    for r in rows:
        pid = str(r.get("parcel_id") or r.get("id") or "")
        lon = r.get("lon")
        lat = r.get("lat")
        if pid and lon is not None and lat is not None:
            pids.append(pid)
            lons.append(float(lon))
            lats.append(float(lat))

    if not pids:
        return {}

    # Build category keys and regex the same way the legacy path does
    _cat_expanded = _expand_category(category)
    category_keys = _cat_expanded["keys"]
    category_regex = _cat_expanded["regex"]

    # Check if delivery_source_record has a geom column (Patch-5 migration)
    _dsr_has_geom = _cached_column_exists(db, "delivery_source_record", "geom")

    if _dsr_has_geom:
        _dsr_geo = "dsr.geom::geography"
        _dsr_where = "dsr.geom IS NOT NULL"
    else:
        _dsr_geo = "ST_SetSRID(ST_MakePoint(dsr.lon::double precision, dsr.lat::double precision), 4326)::geography"
        _dsr_where = "dsr.lat IS NOT NULL AND dsr.lon IS NOT NULL"

    try:
        with db.begin_nested():
            result = db.execute(
                text(f"""
                    WITH inputs AS (
                        SELECT
                            unnest(CAST(:pids AS text[])) AS parcel_id,
                            unnest(CAST(:lons AS double precision[])) AS lon,
                            unnest(CAST(:lats AS double precision[])) AS lat
                    )
                    SELECT
                        i.parcel_id,
                        COALESCE(comp.competitor_count, 0) AS competitor_count,
                        COALESCE(comp.broader_count, 0) AS broader_count,
                        comp.max_chain_strength AS max_chain_strength,
                        comp.chain_strength_share AS chain_strength_share,
                        comp.top_chain_strength_name AS top_chain_strength_name
                    FROM inputs i
                    LEFT JOIN LATERAL (
                        SELECT
                            COUNT(*) FILTER (WHERE in_category) AS competitor_count,
                            COUNT(*) AS broader_count,
                            MAX(chain_strength) FILTER (WHERE in_category) AS max_chain_strength,
                            -- Strong-chain SHARE: among same-category POIs that
                            -- carry an ECQ chain_strength_score (chain_strength
                            -- IS NOT NULL excludes the NULL delivery side), the
                            -- percentage that are "strong" (>= threshold). This
                            -- is the chain_strength leg input; it replaces the
                            -- MAX above, which saturated at 100 for any radius
                            -- containing a single big chain. NULL when fewer
                            -- than :chain_min_matched matched POIs exist, so
                            -- thin-evidence radii flow to Python None and
                            -- _chain_strength_score keeps its neutral 50.0 (no
                            -- COALESCE). MAX is retained above purely for the
                            -- chain_strength_max JSON diagnostic. The
                            -- competition radius also bounds these share
                            -- counts: tightening it (qsr 1200 -> 1000 m, like
                            -- delivery_first before it) shrinks matched counts
                            -- and pushes a few more candidates under
                            -- :chain_min_matched into the neutral 50 —
                            -- expected and accepted.
                            CASE
                              WHEN COUNT(*) FILTER (WHERE in_category AND chain_strength IS NOT NULL)
                                   >= :chain_min_matched
                              THEN 100.0
                                   * COUNT(*) FILTER (WHERE in_category AND chain_strength >= :chain_strong_threshold)
                                   / COUNT(*) FILTER (WHERE in_category AND chain_strength IS NOT NULL)
                              ELSE NULL
                            END AS chain_strength_share,
                            -- Brand provenance for the chain_strength leg:
                            -- the name attached to the same-category row that
                            -- carried the MAX chain_strength_score above.
                            -- Distinct from brand_presence.top_chains[0],
                            -- which orders by branch_count.
                            (array_agg(brand_name ORDER BY chain_strength DESC NULLS LAST)
                                FILTER (WHERE in_category AND chain_strength IS NOT NULL))[1]
                                AS top_chain_strength_name
                        FROM (
                            -- Source 1: restaurant_poi (Google Places).
                            -- LEFT JOIN expansion_competitor_quality so the
                            -- chain_strength signal is captured per same-
                            -- category POI row; rows without an ECQ match
                            -- contribute NULL (ignored by MAX).
                            SELECT (lower(rp.category) = ANY(:category_keys)) AS in_category,
                                   ecq.chain_strength_score AS chain_strength,
                                   rp.name AS brand_name
                            FROM restaurant_poi rp
                            LEFT JOIN expansion_competitor_quality ecq
                                   ON ecq.restaurant_poi_id = rp.id
                                  AND ecq.city = 'riyadh'
                            -- Exclude closed venues: a CLOSED_PERMANENTLY /
                            -- CLOSED_TEMPORARILY restaurant is not live
                            -- competition. NULL is kept for non-Google
                            -- sources and pre-backfill rows.
                            WHERE (rp.business_status IS NULL
                                   OR rp.business_status = 'OPERATIONAL')
                              AND ST_DWithin(
                                  rp.geom::geography,
                                  ST_SetSRID(ST_MakePoint(i.lon, i.lat), 4326)::geography,
                                  :radius_m
                              )
                            UNION ALL
                            -- Source 2: delivery_source_record (HungerStation etc.).
                            -- No POI mapping to chain quality, so chain_strength
                            -- is NULL on every delivery-side row.
                            SELECT (lower(COALESCE(dsr.category_raw, '')) ~* :category_regex
                                    OR lower(COALESCE(dsr.cuisine_raw, '')) ~* :category_regex
                                   ) AS in_category,
                                   NULL::double precision AS chain_strength,
                                   NULL::text AS brand_name
                            FROM delivery_source_record dsr
                            WHERE {_dsr_where}
                              AND ST_DWithin(
                                  {_dsr_geo},
                                  ST_SetSRID(ST_MakePoint(i.lon, i.lat), 4326)::geography,
                                  :radius_m
                              )
                        ) combined
                    ) comp ON TRUE
                """),
                {"pids": pids, "lons": lons, "lats": lats,
                 "category_keys": category_keys, "category_regex": category_regex,
                 "radius_m": competition_radius_m,
                 "chain_strong_threshold": float(settings.EXPANSION_CHAIN_STRONG_THRESHOLD),
                 "chain_min_matched": int(settings.EXPANSION_CHAIN_MIN_MATCHED)},
            ).mappings().all()

        return {
            str(r["parcel_id"]): {
                "competitor_count": int(r["competitor_count"]),
                "confident": int(r["broader_count"]) > 0,
                "max_chain_strength": (
                    float(r["max_chain_strength"])
                    if r["max_chain_strength"] is not None
                    else None
                ),
                "chain_strength_share": (
                    float(r["chain_strength_share"])
                    if r["chain_strength_share"] is not None
                    else None
                ),
                "top_chain_strength_name": (
                    str(r["top_chain_strength_name"])
                    if r.get("top_chain_strength_name") is not None
                    else None
                ),
            }
            for r in result
        }
    except Exception as exc:
        logger.warning("Bulk competitor enrichment failed: %s", exc, exc_info=True)
        return {}


def run_expansion_search(
    db: Session,
    *,
    search_id: str,
    brand_name: str,
    category: str,
    service_model: str,
    min_area_m2: float,
    max_area_m2: float,
    target_area_m2: float,
    limit: int,
    bbox: dict[str, float] | None = None,
    target_districts: list[str] | None = None,
    existing_branches: list[dict[str, Any]] | None = None,
    brand_profile: dict[str, Any] | None = None,
    lang: str = "en",
) -> list[dict[str, Any]]:
    t_start = time.monotonic()
    bbox = bbox or {}
    min_lon = bbox.get("min_lon")
    min_lat = bbox.get("min_lat")
    max_lon = bbox.get("max_lon")
    max_lat = bbox.get("max_lat")

    existing_branches = existing_branches or []
    target_districts = target_districts or []
    target_district_norm = {normalize_district_key(item) for item in target_districts if normalize_district_key(item)}
    effective_brand_profile = _default_brand_profile(brand_profile)
    # Resolve the archetype here (not only in the API layer) so scoring sees
    # the same resolved value regardless of caller — flag state does not
    # matter for resolution, only for whether weights consume it.
    effective_brand_profile["brand_archetype"] = resolve_brand_archetype(
        effective_brand_profile, service_model
    )

    # ArcGIS-only candidate generation.
    # Build optional target-district SQL filter when districts are specified.
    def _build_district_filter_sql(td_norm: set[str]) -> str:
        """Build SQL filter that matches district_label against target districts.

        Applies the same Arabic normalization in SQL (via TRANSLATE) that
        normalize_district_key() does in Python: Alef variants → bare Alef,
        Alef-Maksura → Ya, strip "حي " prefix.  This ensures districts like
        حطين and النخيل match even when stored with variant characters.
        """
        if not td_norm:
            return ""
        _district_values = ", ".join(
            f"(:td_{i})" for i in range(len(td_norm))
        )
        # Strip NBSP, bidi marks, zero-width chars, then TRANSLATE Arabic
        # variants, then strip leading "حي " prefix.  Mirrors Python
        # normalize_district_key() to avoid multi-district filter mismatches.
        _P_STRIP_INVISIBLE = (
            "REGEXP_REPLACE("
            "REPLACE(COALESCE(p.district_label, ''), E'\\u00A0', ' '), "
            "E'[\\u200B-\\u200F\\u202A-\\u202E\\u2066-\\u2069\\uFEFF]', '', 'g'"
            ")"
        )
        _NORM_SQL = (
            "TRIM(REGEXP_REPLACE("
            "TRANSLATE("
            f"{_P_STRIP_INVISIBLE}, "
            "E'\\u0623\\u0625\\u0622\\u0649\\u0640', "
            "E'\\u0627\\u0627\\u0627\\u064A'"
            "), "
            "E'^\\u062D\\u064A\\\\s+', '', 'g'"
            "))"
        )
        return f"""
            AND {_NORM_SQL} IN (
                SELECT td.val FROM (VALUES {_district_values}) AS td(val)
            )
        """

    # SQL-safe coordinate regex: accepts signed decimals, rejects blanks/malformed.
    _COORD_REGEX = r"'^[-+]?[0-9]*\.?[0-9]+$'"

    def _safe_geo(alias: str) -> str:
        """Return a SQL CASE expression that builds a geography point only when
        both lon and lat for *alias* (e.g. 'dsr', 'pd') are valid numeric
        strings.  Returns NULL for dirty/blank/non-numeric values."""
        lon_text = _coord_text(alias, "lon")
        lat_text = _coord_text(alias, "lat")
        return (
            f"CASE"
            f"  WHEN NULLIF({lon_text}, '') ~ {_COORD_REGEX}"
            f"   AND NULLIF({lat_text}, '') ~ {_COORD_REGEX}"
            f"  THEN ST_SetSRID(ST_MakePoint("
            f"    CAST({lon_text} AS double precision),"
            f"    CAST({lat_text} AS double precision)"
            f"  ), 4326)::geography"
            f"  ELSE NULL"
            f" END"
        )

    # Predicate fragment: only rows with valid numeric coords participate.
    def _safe_coord_where(alias: str) -> str:
        lon_text = _coord_text(alias, "lon")
        lat_text = _coord_text(alias, "lat")
        return (
            f"NULLIF({lon_text}, '') ~ {_COORD_REGEX}"
            f" AND NULLIF({lat_text}, '') ~ {_COORD_REGEX}"
        )

    # When the Patch-5 migration has been applied, delivery_source_record and
    # population_density have a pre-computed + indexed `geom` column.  Use it
    # directly instead of the expensive regex-validate-cast-construct pattern.
    _dsr_has_geom = _cached_column_exists(db, "delivery_source_record", "geom")
    _pd_has_geom = _cached_column_exists(db, "population_density", "geom")

    if _dsr_has_geom:
        _SAFE_DSR_GEO = "dsr.geom::geography"
        _SAFE_DSR_COORD_WHERE = "dsr.geom IS NOT NULL"
    else:
        _SAFE_DSR_GEO = _safe_geo("dsr")
        _SAFE_DSR_COORD_WHERE = _safe_coord_where("dsr")

    if _pd_has_geom:
        _SAFE_PD_GEO = "pd.geom::geography"
        _SAFE_PD_COORD_WHERE = "pd.geom IS NOT NULL"
    else:
        _SAFE_PD_GEO = _safe_geo("pd")
        _SAFE_PD_COORD_WHERE = _safe_coord_where("pd")

    # SQL-safe landuse_code ordering: landuse_code is numeric in production,
    # so compare directly — no BTRIM/CAST/regex on landuse_code.
    _SAFE_LANDUSE_ORDER = (
        "CASE"
        " WHEN p.landuse_code IN (2000, 7500) THEN 0"
        " WHEN p.landuse_code IN (3000, 4000) THEN 1"
        " WHEN p.landuse_code IS NULL AND NULLIF(BTRIM(COALESCE(p.landuse_label, '')), '') IS NULL THEN 2"
        " WHEN p.landuse_code = 1000 THEN 3"
        " ELSE 1 END"
    )

    def _build_candidate_sql(
        district_filter_sql: str,
        *,
        stratified: bool = False,
        skip_delivery: bool = False,
    ) -> text:
        """Build candidate query using LATERAL JOINs for enrichment.

        When *skip_delivery* is True the 4 delivery columns return constant 0
        — the caller will fill real values via bulk EA delivery enrichment,
        avoiding ~2 400 full seq-scans of delivery_source_record.
        """
        # Compute a landuse_priority integer in the base CTE so the
        # stratified window and final ORDER BY can reference it without
        # repeating the CASE on the raw column with p. alias.
        _LANDUSE_PRIORITY_EXPR = (
            "CASE"
            " WHEN p.landuse_code IN (2000, 7500) THEN 0"
            " WHEN p.landuse_code IN (3000, 4000) THEN 1"
            " WHEN p.landuse_code IS NULL AND NULLIF(BTRIM(COALESCE(p.landuse_label, '')), '') IS NULL THEN 2"
            " WHEN p.landuse_code = 1000 THEN 3"
            " ELSE 1 END"
        )

        _BASE_CTE = f"""
            SELECT
                p.id AS parcel_id,
                p.landuse_label,
                p.landuse_code,
                p.area_m2,
                p.geom,
                ST_X(ST_Centroid(p.geom)) AS lon,
                ST_Y(ST_Centroid(p.geom)) AS lat,
                ABS(p.area_m2 - CAST(:target_area_m2 AS double precision)) AS area_distance,
                {_LANDUSE_PRIORITY_EXPR} AS landuse_priority,
                p.district_label AS district
            FROM {ARCGIS_PARCELS_TABLE} p
            WHERE p.geom IS NOT NULL
              AND p.area_m2 BETWEEN :min_area_m2 AND :max_area_m2
              AND (CAST(:min_lon AS double precision) IS NULL OR ST_X(ST_Centroid(p.geom)) >= CAST(:min_lon AS double precision))
              AND (CAST(:max_lon AS double precision) IS NULL OR ST_X(ST_Centroid(p.geom)) <= CAST(:max_lon AS double precision))
              AND (CAST(:min_lat AS double precision) IS NULL OR ST_Y(ST_Centroid(p.geom)) >= CAST(:min_lat AS double precision))
              AND (CAST(:max_lat AS double precision) IS NULL OR ST_Y(ST_Centroid(p.geom)) <= CAST(:max_lat AS double precision))
              {district_filter_sql}
        """

        if stratified:
            # City-wide mode: allocate slots per district to ensure geographic spread.
            # 1. Rank parcels within each district by quality.
            # 2. Keep up to :per_district_cap per district.
            # 3. Apply global limit on the combined result.
            _CANDIDATE_CTE = f"""
            WITH candidate_raw AS (
                {_BASE_CTE}
            ),
            candidate_base AS (
                SELECT
                    parcel_id, landuse_label, landuse_code, area_m2, geom,
                    lon, lat, area_distance, landuse_priority, district
                FROM (
                    SELECT
                        cr.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY cr.district
                            ORDER BY
                                cr.landuse_priority ASC,
                                cr.area_distance ASC,
                                CASE WHEN cr.landuse_label IS NOT NULL THEN 0 ELSE 1 END,
                                cr.parcel_id ASC
                        ) AS district_rank
                    FROM candidate_raw cr
                ) ranked
                WHERE district_rank <= CAST(:per_district_cap AS integer)
                ORDER BY
                    district_rank ASC,
                    landuse_priority ASC,
                    area_distance ASC,
                    CASE WHEN landuse_label IS NOT NULL THEN 0 ELSE 1 END,
                    parcel_id ASC
                LIMIT {_CANDIDATE_POOL_LIMIT}
            )
            """
        else:
            # Targeted mode (districts specified or fallback): original behavior.
            _CANDIDATE_CTE = f"""
            WITH candidate_base AS (
                SELECT
                    parcel_id, landuse_label, landuse_code, area_m2, geom,
                    lon, lat, area_distance, landuse_priority, district
                FROM (
                    {_BASE_CTE}
                ) _inner
                ORDER BY
                    landuse_priority ASC,
                    area_distance ASC,
                    CASE WHEN landuse_label IS NOT NULL THEN 0 ELSE 1 END,
                    parcel_id ASC
                LIMIT {_CANDIDATE_POOL_LIMIT}
            )
            """

        # ── Enrichment via LATERAL JOINs (replaces 6 correlated subqueries) ──

        # Population: single LATERAL join
        _POP_LATERAL = f"""
        LEFT JOIN LATERAL (
            SELECT COALESCE(SUM(pd.population), 0) AS population_reach
            FROM population_density pd
            WHERE {_SAFE_PD_COORD_WHERE}
              AND ST_DWithin(
                  ST_SetSRID(ST_MakePoint(b.lon, b.lat), 4326)::geography,
                  {_SAFE_PD_GEO},
                  :demand_radius_m
              )
        ) pop ON TRUE
        """

        # Competitor: single LATERAL join
        _COMP_LATERAL = f"""
        LEFT JOIN LATERAL (
            SELECT COALESCE(COUNT(*), 0) AS competitor_count
            FROM restaurant_poi rp
            WHERE lower(rp.category) = ANY(:category_keys)
              AND ST_DWithin(
                  rp.geom::geography,
                  ST_SetSRID(ST_MakePoint(b.lon, b.lat), 4326)::geography,
                  :competition_radius_m
              )
        ) comp ON TRUE
        """

        # Delivery: skip (return 0s) or single merged LATERAL (replaces 4 subqueries)
        if skip_delivery:
            _DEL_LATERAL = ""
            _DEL_COLUMNS = (
                "0 AS delivery_listing_count,\n"
                "            0 AS provider_listing_count,\n"
                "            0 AS provider_platform_count,\n"
                "            0 AS delivery_competition_count"
            )
        else:
            _DEL_LATERAL = f"""
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (
                        WHERE (lower(COALESCE(dsr.category_raw, '')) ~* :category_regex
                               OR lower(COALESCE(dsr.cuisine_raw, '')) ~* :category_regex)
                          AND ST_DWithin(
                              {_SAFE_DSR_GEO},
                              ST_SetSRID(ST_MakePoint(b.lon, b.lat), 4326)::geography,
                              :demand_radius_m)
                    ) AS delivery_listing_count,
                    COUNT(*) AS provider_listing_count,
                    COUNT(DISTINCT lower(COALESCE(dsr.platform, 'unknown'))) AS provider_platform_count,
                    COUNT(*) FILTER (
                        WHERE lower(COALESCE(dsr.category_raw, '')) ~* :category_regex
                           OR lower(COALESCE(dsr.cuisine_raw, '')) ~* :category_regex
                    ) AS delivery_competition_count
                FROM delivery_source_record dsr
                WHERE {_SAFE_DSR_COORD_WHERE}
                  AND ST_DWithin(
                      {_SAFE_DSR_GEO},
                      ST_SetSRID(ST_MakePoint(b.lon, b.lat), 4326)::geography,
                      :provider_radius_m
                  )
            ) del ON TRUE
            """
            _DEL_COLUMNS = (
                "COALESCE(del.delivery_listing_count, 0) AS delivery_listing_count,\n"
                "            COALESCE(del.provider_listing_count, 0) AS provider_listing_count,\n"
                "            COALESCE(del.provider_platform_count, 0) AS provider_platform_count,\n"
                "            COALESCE(del.delivery_competition_count, 0) AS delivery_competition_count"
            )

        return text(
            f"""
        {_CANDIDATE_CTE}
        SELECT
            b.parcel_id,
            b.landuse_label,
            b.landuse_code,
            b.area_m2,
            b.lon,
            b.lat,
            b.district,
            COALESCE(pop.population_reach, 0) AS population_reach,
            COALESCE(comp.competitor_count, 0) AS competitor_count,
            {_DEL_COLUMNS}
        FROM candidate_base b
        {_POP_LATERAL}
        {_COMP_LATERAL}
        {_DEL_LATERAL}
        """
        )

    def _build_candidate_sql_no_district(*, skip_delivery: bool = False) -> text:
        """Last-resort candidate query that skips the external_feature district
        subselect entirely.  Used when ST_GeomFromGeoJSON fails on corrupt
        geometry data so the search can still return results (without district
        labels).

        Uses LATERAL JOINs for enrichment and supports *skip_delivery* to
        avoid expensive delivery_source_record scans when bulk EA delivery
        enrichment is available.
        """
        # ── Population LATERAL ──
        _POP_LATERAL = f"""
        LEFT JOIN LATERAL (
            SELECT COALESCE(SUM(pd.population), 0) AS population_reach
            FROM population_density pd
            WHERE {_SAFE_PD_COORD_WHERE}
              AND ST_DWithin(
                  ST_SetSRID(ST_MakePoint(b.lon, b.lat), 4326)::geography,
                  {_SAFE_PD_GEO},
                  :demand_radius_m
              )
        ) pop ON TRUE
        """

        # ── Competitor LATERAL ──
        _COMP_LATERAL = f"""
        LEFT JOIN LATERAL (
            SELECT COALESCE(COUNT(*), 0) AS competitor_count
            FROM restaurant_poi rp
            WHERE lower(rp.category) = ANY(:category_keys)
              AND ST_DWithin(
                  rp.geom::geography,
                  ST_SetSRID(ST_MakePoint(b.lon, b.lat), 4326)::geography,
                  :competition_radius_m
              )
        ) comp ON TRUE
        """

        # ── Delivery: skip or merged LATERAL ──
        if skip_delivery:
            _DEL_LATERAL = ""
            _DEL_COLUMNS = (
                "0 AS delivery_listing_count,\n"
                "            0 AS provider_listing_count,\n"
                "            0 AS provider_platform_count,\n"
                "            0 AS delivery_competition_count"
            )
        else:
            _DEL_LATERAL = f"""
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (
                        WHERE (lower(COALESCE(dsr.category_raw, '')) ~* :category_regex
                               OR lower(COALESCE(dsr.cuisine_raw, '')) ~* :category_regex)
                          AND ST_DWithin(
                              {_SAFE_DSR_GEO},
                              ST_SetSRID(ST_MakePoint(b.lon, b.lat), 4326)::geography,
                              :demand_radius_m)
                    ) AS delivery_listing_count,
                    COUNT(*) AS provider_listing_count,
                    COUNT(DISTINCT lower(COALESCE(dsr.platform, 'unknown'))) AS provider_platform_count,
                    COUNT(*) FILTER (
                        WHERE lower(COALESCE(dsr.category_raw, '')) ~* :category_regex
                           OR lower(COALESCE(dsr.cuisine_raw, '')) ~* :category_regex
                    ) AS delivery_competition_count
                FROM delivery_source_record dsr
                WHERE {_SAFE_DSR_COORD_WHERE}
                  AND ST_DWithin(
                      {_SAFE_DSR_GEO},
                      ST_SetSRID(ST_MakePoint(b.lon, b.lat), 4326)::geography,
                      :provider_radius_m
                  )
            ) del ON TRUE
            """
            _DEL_COLUMNS = (
                "COALESCE(del.delivery_listing_count, 0) AS delivery_listing_count,\n"
                "            COALESCE(del.provider_listing_count, 0) AS provider_listing_count,\n"
                "            COALESCE(del.provider_platform_count, 0) AS provider_platform_count,\n"
                "            COALESCE(del.delivery_competition_count, 0) AS delivery_competition_count"
            )

        return text(
            f"""
        WITH candidate_base AS (
            SELECT
                p.id AS parcel_id,
                p.landuse_label,
                p.landuse_code,
                p.area_m2,
                p.geom,
                ST_X(ST_Centroid(p.geom)) AS lon,
                ST_Y(ST_Centroid(p.geom)) AS lat,
                ABS(p.area_m2 - CAST(:target_area_m2 AS double precision)) AS area_distance,
                p.district_label AS district
            FROM {ARCGIS_PARCELS_TABLE} p
            WHERE p.geom IS NOT NULL
              AND p.area_m2 BETWEEN :min_area_m2 AND :max_area_m2
              AND (CAST(:min_lon AS double precision) IS NULL OR ST_X(ST_Centroid(p.geom)) >= CAST(:min_lon AS double precision))
              AND (CAST(:max_lon AS double precision) IS NULL OR ST_X(ST_Centroid(p.geom)) <= CAST(:max_lon AS double precision))
              AND (CAST(:min_lat AS double precision) IS NULL OR ST_Y(ST_Centroid(p.geom)) >= CAST(:min_lat AS double precision))
              AND (CAST(:max_lat AS double precision) IS NULL OR ST_Y(ST_Centroid(p.geom)) <= CAST(:max_lat AS double precision))
            ORDER BY
                {_SAFE_LANDUSE_ORDER},
                ABS(p.area_m2 - CAST(:target_area_m2 AS double precision)) ASC,
                CASE WHEN p.landuse_label IS NOT NULL THEN 0 ELSE 1 END,
                p.id ASC
            LIMIT {_CANDIDATE_POOL_LIMIT}
        )
        SELECT
            b.parcel_id,
            b.landuse_label,
            b.landuse_code,
            b.area_m2,
            b.lon,
            b.lat,
            b.district,
            COALESCE(pop.population_reach, 0) AS population_reach,
            COALESCE(comp.competitor_count, 0) AS competitor_count,
            {_DEL_COLUMNS}
        FROM candidate_base b
        {_POP_LATERAL}
        {_COMP_LATERAL}
        {_DEL_LATERAL}
        """
        )

    # City-wide mode: no target districts → use stratified sampling.
    # Also stratify when 2+ target districts to guarantee each district gets
    # representation instead of one district hoarding all slots via the
    # global LIMIT (e.g. العليا's commercial parcels exhausting the pool
    # before حطين/النخيل get any slots).
    is_city_wide = not target_district_norm
    use_stratified = is_city_wide or len(target_district_norm) >= 2

    # ── Probe candidate_location availability up front ──
    # The active retrieval path is candidate_location when ≥10 Tier-1
    # cluster primaries exist. The per-district cap denominator must match
    # the retrieval universe; the arcgis_raw district count (~148) is the
    # wrong denominator when listings live in candidate_location (only ~26
    # districts have qualifying listings under default search params).
    _cl_count = 0
    try:
        _cl_count = int(db.execute(text(
            "SELECT COUNT(*) FROM candidate_location "
            "WHERE is_cluster_primary = TRUE AND source_tier = 1 AND geom IS NOT NULL"
        )).scalar() or 0)
    except Exception as exc:
        logger.warning("candidate_location count query failed, falling back to legacy: %s", exc)

    use_candidate_location = _cl_count >= 10

    # Compute per-district cap dynamically.
    # Goal: spread _CANDIDATE_POOL_LIMIT slots across districts.
    # We estimate the district count from external_feature to set the cap,
    # bounded by _PER_DISTRICT_MIN_CAP and _PER_DISTRICT_MAX_CAP.
    per_district_cap = _PER_DISTRICT_MAX_CAP
    if use_stratified and target_district_norm:
        # Multi-district targeted: each district gets headroom of
        # MULTIPLIER * (limit / N), bounded by [MIN_CAP, MAX_CAP]. This
        # supersedes the old formula which used _CANDIDATE_POOL_LIMIT // N
        # and silently collapsed to _PER_DISTRICT_MAX_CAP for any realistic N.
        n_districts = max(len(target_district_norm), 1)
        effective_limit = max(limit, 25)  # floor prevents tiny-limit starvation
        fair_share_headroom = (
            effective_limit * _PER_DISTRICT_HEADROOM_MULTIPLIER
        ) // n_districts
        per_district_cap = max(
            _PER_DISTRICT_MIN_CAP,
            min(_PER_DISTRICT_MAX_CAP, fair_share_headroom),
        )
        logger.info(
            "expansion_search stratified multi-district mode: target_count=%d per_district_cap=%d search_id=%s",
            len(target_district_norm), per_district_cap, search_id,
        )
    elif is_city_wide and use_candidate_location:
        # candidate_location path: the retrieval universe is Tier-1 cluster
        # primaries (~83 districts; only ~26 with qualifying listings under
        # default params). Deriving the cap from arcgis_raw's ~148 districts
        # silently collapses the pool — e.g. 2000 // 148 ≈ 13, clamped by
        # _PER_DISTRICT_MIN_CAP to 5, gave a 36-row cap on the production
        # QSR/store-showroom/80–500m² combination whose structural ceiling
        # is 58. At current inventory no district has more than ~14
        # qualifying listings, so MAX_CAP is functionally unbounded.
        per_district_cap = _PER_DISTRICT_MAX_CAP
        logger.info(
            "expansion_search city-wide candidate_location mode: cl_count=%d per_district_cap=%d search_id=%s",
            _cl_count, per_district_cap, search_id,
        )
    elif is_city_wide:
        try:
            district_count_row = db.execute(text(
                "SELECT COUNT(DISTINCT district_label) "
                "FROM public.riyadh_parcels_arcgis_raw "
                "WHERE geom IS NOT NULL AND district_label IS NOT NULL"
            )).scalar() or 1
            per_district_cap = max(
                _PER_DISTRICT_MIN_CAP,
                min(_PER_DISTRICT_MAX_CAP, _CANDIDATE_POOL_LIMIT // max(district_count_row, 1)),
            )
            logger.info(
                "expansion_search stratified mode: district_count=%d per_district_cap=%d search_id=%s",
                district_count_row, per_district_cap, search_id,
            )
        except Exception:
            logger.warning("expansion_search: could not count districts for cap, using default=%d", per_district_cap, exc_info=True)

    _cat_expanded = _expand_category(category)
    sql_params: dict[str, Any] = {
        "min_area_m2": min_area_m2,
        "max_area_m2": max_area_m2,
        "target_area_m2": target_area_m2,
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
        "category_keys": _cat_expanded["keys"],
        "category_regex": _cat_expanded["regex"],
        "category_like": _cat_expanded["like"],
        # Catchment radii derived from the brief's service model. See
        # _catchment_radii() for the lookup table and rationale.
        "demand_radius_m":      _catchment_radii(service_model)["demand"],
        "competition_radius_m": _catchment_radii(service_model)["competition"],
        "provider_radius_m":    _catchment_radii(service_model)["provider"],
        "per_district_cap": per_district_cap,
    }
    logger.info(
        "expansion_search category expansion: input=%r keys=%s regex_len=%d search_id=%s",
        category, _cat_expanded["keys"], len(_cat_expanded["regex"]), search_id,
    )
    # Bind target-district values when district SQL filter is active.
    for i, td_val in enumerate(sorted(target_district_norm)):
        sql_params[f"td_{i}"] = td_val.lower()

    # Pre-check EA delivery table so we can skip expensive delivery subqueries
    # when bulk enrichment will overwrite them anyway (Patch 1 optimisation).
    ea_delivery_populated = _cached_ea_table_has_rows(db, _EA_DELIVERY_TABLE)

    # Count active delivery platforms for scoring denominator
    _active_platform_count = 5  # fallback
    if ea_delivery_populated:
        try:
            _apc_row = db.execute(text(f"SELECT COUNT(DISTINCT platform) FROM {_EA_DELIVERY_TABLE} WHERE city = 'riyadh'")).scalar()
            if _apc_row and int(_apc_row) > 0:
                _active_platform_count = int(_apc_row)
        except Exception:
            pass

    # candidate_location availability (_cl_count, use_candidate_location)
    # was probed earlier so the per-district cap could match the active
    # retrieval universe.
    use_commercial_units = False

    logger.info(
        "expansion_advisor.candidate_pool_cap search_id=%s is_city_wide=%s "
        "use_candidate_location=%s per_district_cap=%d candidate_pool_limit=%d cl_count=%d",
        search_id, is_city_wide, use_candidate_location,
        per_district_cap, _CANDIDATE_POOL_LIMIT, _cl_count,
    )

    if use_candidate_location:
        rows = _query_candidate_location_pool(
            db,
            target_district_norm=target_district_norm,
            min_area_m2=min_area_m2,
            max_area_m2=max_area_m2,
            target_area_m2=target_area_m2,
            per_district_cap=per_district_cap,
            limit=_CANDIDATE_POOL_LIMIT,
        )
        # Log district distribution for diagnostics
        _district_dist: dict[str, int] = {}
        for _r in rows:
            _d = _r.get("district") or "UNKNOWN"
            _district_dist[_d] = _district_dist.get(_d, 0) + 1
        logger.info(
            "expansion_search using candidate_location: %d candidates from %d primaries, "
            "district_distribution=%s, target_districts=%s, search_id=%s",
            len(rows), _cl_count, dict(sorted(_district_dist.items(), key=lambda x: -x[1])[:10]),
            sorted(target_district_norm) if target_district_norm else [],
            search_id,
        )
        # Bulk-enrich population reach for candidate_location rows
        # (candidate_location path returns population_reach=0; we need real values)
        _bulk_pop = _bulk_enrich_population(db, rows, service_model=service_model)
        if _bulk_pop:
            for _r in rows:
                _pid = str(_r.get("parcel_id") or _r.get("id") or "")
                if _pid in _bulk_pop:
                    _r["population_reach"] = _bulk_pop[_pid]
            logger.info(
                "expansion_search: bulk population enrichment applied to %d/%d candidates, search_id=%s",
                len(_bulk_pop), len(rows), search_id,
            )
        # Bulk-enrich competitor counts for candidate_location rows
        # (candidate_location path does not compute competitor_count)
        _bulk_comp = _bulk_enrich_competitors(db, rows, category, service_model=service_model)
        if _bulk_comp:
            for _r in rows:
                _pid = str(_r.get("parcel_id") or _r.get("id") or "")
                if _pid in _bulk_comp:
                    _entry = _bulk_comp[_pid]
                    _r["competitor_count"] = _entry["competitor_count"]
                    _r["competitor_count_confident"] = _entry["confident"]
                    _r["max_chain_strength"] = _entry.get("max_chain_strength")
                    _r["chain_strength_share"] = _entry.get("chain_strength_share")
                    _r["top_chain_strength_name"] = _entry.get("top_chain_strength_name")
            logger.info(
                "expansion_search: bulk competitor enrichment applied to %d/%d candidates, search_id=%s",
                len(_bulk_comp), len(rows), search_id,
            )
    else:
        logger.info(
            "expansion_search: candidate_location has %d Tier 1 listings (< 10), using direct commercial_unit query, search_id=%s",
            _cl_count, search_id,
        )

    if not use_candidate_location:
        # ── Direct commercial_unit query (fallback when candidate_location not populated) ──
        rows = _query_commercial_unit_candidates(
            db,
            target_district_norm=target_district_norm,
            min_area_m2=min_area_m2,
            max_area_m2=max_area_m2,
            limit=600,
        )
        logger.info(
            "expansion_search using direct commercial_unit query: %d candidates, search_id=%s",
            len(rows), search_id,
        )

        if rows:
            # Bulk-enrich population reach for commercial_unit rows
            _bulk_pop = _bulk_enrich_population(db, rows, service_model=service_model)
            if _bulk_pop:
                for _r in rows:
                    _pid = str(_r.get("parcel_id") or _r.get("id") or "")
                    if _pid in _bulk_pop:
                        _r["population_reach"] = _bulk_pop[_pid]

            # Bulk-enrich competitor counts for commercial_unit rows
            _bulk_comp = _bulk_enrich_competitors(db, rows, category, service_model=service_model)
            if _bulk_comp:
                for _r in rows:
                    _pid = str(_r.get("parcel_id") or _r.get("id") or "")
                    if _pid in _bulk_comp:
                        _entry = _bulk_comp[_pid]
                        _r["competitor_count"] = _entry["competitor_count"]
                        _r["competitor_count_confident"] = _entry["confident"]
                        _r["max_chain_strength"] = _entry.get("max_chain_strength")
                        _r["chain_strength_share"] = _entry.get("chain_strength_share")
                        _r["top_chain_strength_name"] = _entry.get("top_chain_strength_name")

            # ── Resolve commercial unit districts to Arabic names ──────────
            # Commercial units store English neighborhood names from Aqar,
            # but the scoring loop expects Arabic district names matching
            # district_lookup built from riyadh_parcels_arcgis_raw.
            try:
                from sqlalchemy import text as _sa_text

                # Build VALUES list of (index, lon, lat) for all candidates
                values_parts = []
                resolve_params: dict[str, Any] = {}
                for idx, r in enumerate(rows):
                    if r.get("lat") is not None and r.get("lon") is not None:
                        values_parts.append(f"(:_ri_{idx}, :_rlon_{idx}, :_rlat_{idx})")
                        resolve_params[f"_ri_{idx}"] = idx
                        resolve_params[f"_rlon_{idx}"] = float(r["lon"])
                        resolve_params[f"_rlat_{idx}"] = float(r["lat"])

                if values_parts:
                    values_sql = ", ".join(values_parts)
                    resolve_sql = _sa_text(f"""
                        SELECT v.idx, lat_res.district_label
                        FROM (VALUES {values_sql}) AS v(idx, lon, lat)
                        LEFT JOIN LATERAL (
                            SELECT DISTINCT district_label
                            FROM riyadh_parcels_arcgis_raw
                            WHERE geom IS NOT NULL
                              AND ST_DWithin(
                                  geom::geography,
                                  ST_SetSRID(ST_MakePoint(v.lon, v.lat), 4326)::geography,
                                  500
                              )
                            LIMIT 1
                        ) lat_res ON true
                    """)
                    with db.begin_nested():
                        resolved_rows = db.execute(resolve_sql, resolve_params).mappings().all()

                    resolved_count = 0
                    for rr in resolved_rows:
                        idx = int(rr["idx"])
                        if rr["district_label"]:
                            rows[idx]["district"] = rr["district_label"]
                            resolved_count += 1

                    unresolved_count = len(rows) - resolved_count
                    logger.info(
                        "commercial_unit district resolution: resolved=%d, unresolved=%d",
                        resolved_count, unresolved_count,
                    )
            except Exception:
                logger.warning(
                    "commercial_unit district resolution failed, keeping English names",
                    exc_info=True,
                )

    # Debug: log sample non-numeric landuse_code values for diagnosis
    _bad_landuse_sample: list[str] = []
    for row in rows:
        lc = row.get("landuse_code")
        if lc is not None:
            lc_stripped = str(lc).strip()
            if lc_stripped and not lc_stripped.isdigit() and lc_stripped not in _bad_landuse_sample:
                _bad_landuse_sample.append(lc_stripped)
                if len(_bad_landuse_sample) >= 10:
                    break
    if _bad_landuse_sample:
        logger.info(
            "Expansion search non-numeric landuse_code samples (search_id=%s): %s",
            search_id, _bad_landuse_sample,
        )

    t_query_done = time.monotonic()
    logger.info(
        "expansion_search timing: candidate_query=%.2fs search_id=%s rows=%d",
        t_query_done - t_start, search_id, len(rows),
    )

    candidates: list[dict[str, Any]] = []
    prepared: list[dict[str, Any]] = []
    district_lookup = _cached_district_lookup(db)
    # Check normalized Expansion Advisor tables first, then legacy OSM tables
    ea_roads_populated = _cached_ea_table_has_rows(db, _EA_ROADS_TABLE)
    ea_parking_populated = _cached_ea_table_has_rows(db, _EA_PARKING_TABLE)
    # ea_delivery_populated already resolved before candidate query (Patch 1).
    ea_competitor_populated = _cached_ea_table_has_rows(db, _EA_COMPETITOR_TABLE)
    roads_table_available = ea_roads_populated or _cached_table_available(db, "public.planet_osm_line")
    parking_table_available = ea_parking_populated or _cached_table_available(db, "public.planet_osm_polygon")
    # ── Bulk delivery enrichment (replaces per-candidate N+1 pattern) ──
    _bulk_delivery: dict[str, dict[str, int]] = {}
    _bulk_foot_traffic: dict[str, int] = {}
    # L1 demand-generator index (PR-1, emit-only). Populated only when the
    # feature flag is on; otherwise these stay empty and nothing is emitted.
    _bulk_osm_generators: dict[str, dict[str, int]] = {}
    _bulk_building_floors: dict[str, float] = {}
    _bulk_fnb_density: dict[str, dict[str, float]] = {}
    _bulk_dg_pop_local: dict[str, float] = {}
    if ea_delivery_populated:
        try:
            # Build a VALUES list of (parcel_id, lon, lat) for all candidates
            _del_values_parts: list[str] = []
            _cat_terms = _expand_category_terms(category)
            _cat_conditions = " OR ".join(
                f"lower(COALESCE(d.category, '')) LIKE :cat_{i}"
                for i in range(len(_cat_terms))
            )
            _del_params: dict[str, Any] = {
                f"cat_{i}": f"%{term}%"
                for i, term in enumerate(_cat_terms)
            }
            # Delivery-market count radius. This bulk enrichment deliberately
            # overrides the pool SQL's model-aware :demand_radius_m count for
            # ALL service models: both delivery legs (this listing count and
            # the realized-demand Δrating_count below) share the single
            # EXPANSION_REALIZED_DEMAND_RADIUS_M catchment (default 1200 m).
            # The _REALIZED_DEMAND_REFERENCE anchors are calibrated to counts
            # at THIS radius — re-anchor them if it ever changes.
            _del_params["del_radius_m"] = int(
                settings.EXPANSION_REALIZED_DEMAND_RADIUS_M
            )
            for _idx, _r in enumerate(rows):
                _pid = str(_r.get("parcel_id") or "")
                _lon = _safe_float(_r.get("lon"))
                _lat = _safe_float(_r.get("lat"))
                if _pid and _lon != 0.0 and _lat != 0.0:
                    _del_values_parts.append(f"(:dp_{_idx}, :dx_{_idx}, :dy_{_idx})")
                    _del_params[f"dp_{_idx}"] = _pid
                    _del_params[f"dx_{_idx}"] = _lon
                    _del_params[f"dy_{_idx}"] = _lat
            if _del_values_parts:
                _del_values_sql = ", ".join(_del_values_parts)
                with db.begin_nested():
                    _del_rows = db.execute(
                        text(f"""
                            WITH candidates(parcel_id, lon, lat) AS (
                                VALUES {_del_values_sql}
                            )
                            SELECT
                                c.parcel_id,
                                COUNT(d.*) AS listing_count,
                                COUNT(DISTINCT d.platform) AS platform_count,
                                COUNT(d.*) FILTER (
                                    WHERE ({_cat_conditions})
                                ) AS cat_count
                            FROM candidates c
                            LEFT JOIN {_EA_DELIVERY_TABLE} d
                              ON d.geom IS NOT NULL
                             AND ST_DWithin(
                                 d.geom::geography,
                                 ST_SetSRID(ST_MakePoint(c.lon::double precision, c.lat::double precision), 4326)::geography,
                                 :del_radius_m
                             )
                            GROUP BY c.parcel_id
                        """),
                        _del_params,
                    ).mappings().all()
                for _dr in _del_rows:
                    _bulk_delivery[str(_dr["parcel_id"])] = {
                        "listing_count": _safe_int(_dr.get("listing_count")),
                        "platform_count": _safe_int(_dr.get("platform_count")),
                        "cat_count": _safe_int(_dr.get("cat_count")),
                    }
                logger.info(
                    "expansion_search bulk delivery enrichment: search_id=%s enriched=%d/%d",
                    search_id, len(_bulk_delivery), len(rows),
                )
                _cat_match_count = sum(1 for v in _bulk_delivery.values() if v.get("cat_count", 0) > 0)
                logger.info(
                    "expansion_search delivery category match: search_id=%s category=%s "
                    "terms=%s parcels_with_cat_match=%d/%d",
                    search_id, category, _cat_terms,
                    _cat_match_count, len(_bulk_delivery),
                )
        except Exception:
            logger.warning("expansion_search bulk delivery enrichment failed, using legacy counts", exc_info=True)

    # ── Bulk realized-demand enrichment (Δrating_count over trailing window) ──
    # Category-specific realized-demand signal derived from the rating_count
    # history table.  Runs only when the feature flag is on AND the history
    # table is present with data.  Listing-count-only behavior is preserved
    # when disabled or when the history table is empty for the catchment.
    if (
        settings.EXPANSION_REALIZED_DEMAND_ENABLED
        and _bulk_delivery
        and _cached_table_available(db, "expansion_delivery_rating_history")
    ):
        try:
            _window_days = int(settings.EXPANSION_REALIZED_DEMAND_WINDOW_DAYS)
            _rd_radius_m = int(settings.EXPANSION_REALIZED_DEMAND_RADIUS_M)
            _rd_values_parts: list[str] = []
            _rd_cat_terms = _expand_category_terms(category)
            _rd_cat_conditions = " OR ".join(
                f"(lower(COALESCE(h.category_raw, '')) LIKE :rd_cat_{i} "
                f"OR lower(COALESCE(h.cuisine_raw, '')) LIKE :rd_cat_{i})"
                for i in range(len(_rd_cat_terms))
            )
            _rd_params: dict[str, Any] = {
                f"rd_cat_{i}": f"%{term}%"
                for i, term in enumerate(_rd_cat_terms)
            }
            _rd_params["rd_window_days"] = _window_days
            _rd_params["rd_radius_m"] = _rd_radius_m
            for _idx, _r in enumerate(rows):
                _pid = str(_r.get("parcel_id") or "")
                _lon = _safe_float(_r.get("lon"))
                _lat = _safe_float(_r.get("lat"))
                if _pid and _lon != 0.0 and _lat != 0.0:
                    _rd_values_parts.append(f"(:rdp_{_idx}, :rdx_{_idx}, :rdy_{_idx})")
                    _rd_params[f"rdp_{_idx}"] = _pid
                    _rd_params[f"rdx_{_idx}"] = _lon
                    _rd_params[f"rdy_{_idx}"] = _lat
            if _rd_values_parts:
                _rd_values_sql = ", ".join(_rd_values_parts)
                with db.begin_nested():
                    _rd_rows = db.execute(
                        text(f"""
                            WITH cands(parcel_id, lon, lat) AS (
                                VALUES {_rd_values_sql}
                            ),
                            branch_delta AS (
                                SELECT
                                    h.source_record_id,
                                    (ARRAY_AGG(h.geom ORDER BY h.captured_at DESC))[1] AS geom,
                                    GREATEST(
                                        0,
                                        MAX(h.rating_count) - MIN(h.rating_count)
                                    ) AS delta
                                FROM expansion_delivery_rating_history h
                                WHERE h.captured_at >= now() - (:rd_window_days || ' days')::interval
                                  AND h.rating_count IS NOT NULL
                                  AND h.geom IS NOT NULL
                                  AND ({_rd_cat_conditions})
                                GROUP BY h.source_record_id
                                HAVING COUNT(*) >= 2
                            )
                            SELECT
                                c.parcel_id,
                                COALESCE(SUM(bd.delta), 0) AS realized_demand,
                                COUNT(DISTINCT bd.source_record_id) AS contributing_branches
                            FROM cands c
                            LEFT JOIN branch_delta bd
                              ON bd.geom IS NOT NULL
                             AND ST_DWithin(
                                 bd.geom::geography,
                                 ST_SetSRID(ST_MakePoint(
                                     c.lon::double precision, c.lat::double precision
                                 ), 4326)::geography,
                                 :rd_radius_m
                             )
                            GROUP BY c.parcel_id
                        """),
                        _rd_params,
                    ).mappings().all()
                _rd_hits = 0
                for _dr in _rd_rows:
                    _pid_key = str(_dr["parcel_id"])
                    _rd_val = _safe_int(_dr.get("realized_demand"))
                    _rd_branches = _safe_int(_dr.get("contributing_branches"))
                    if _pid_key in _bulk_delivery:
                        _bulk_delivery[_pid_key]["realized_demand_30d"] = _rd_val
                        _bulk_delivery[_pid_key]["realized_demand_branches"] = _rd_branches
                        if _rd_branches > 0:
                            _rd_hits += 1
                logger.info(
                    "expansion_search realized-demand enrichment: search_id=%s "
                    "window_days=%d radius_m=%d parcels_with_signal=%d/%d",
                    search_id, _window_days, _rd_radius_m, _rd_hits, len(_bulk_delivery),
                )
        except Exception:
            logger.warning(
                "expansion_search realized-demand enrichment failed; falling back to listing-count only",
                exc_info=True,
            )

    t_delivery_enrich_done = time.monotonic()
    logger.info(
        "expansion_search timing: delivery_enrichment=%.2fs search_id=%s",
        t_delivery_enrich_done - t_query_done, search_id,
    )

    # ── Pre-compute district-level delivery stats for fallback scoring ──
    _district_delivery_stats: dict[str, dict] = {}
    _city_delivery_benchmarks: dict[str, float] = {}
    if ea_delivery_populated:
        _district_delivery_stats, _city_delivery_benchmarks = _precompute_district_delivery_stats(
            db, _EA_DELIVERY_TABLE, category,
        )
    t_district_stats_done = time.monotonic()
    logger.info(
        "expansion_search timing: district_delivery_stats=%.2fs districts=%d search_id=%s",
        t_district_stats_done - t_delivery_enrich_done,
        len(_district_delivery_stats),
        search_id,
    )

    # ── Pre-compute district momentum (Phase 3b) ──
    # One aggregation round-trip (~2ms in DB verification) keyed by
    # normalize_district_key(neighborhood). Districts below the sample
    # floor are absent from the dict; scoring call sites resolve absent
    # keys to neutral 50.0 via .get(district_norm).
    _district_momentum = _district_momentum_score(db)
    t_district_momentum_done = time.monotonic()
    logger.info(
        "expansion_search timing: district_momentum=%.3fs districts=%d search_id=%s",
        t_district_momentum_done - t_district_stats_done,
        len(_district_momentum),
        search_id,
    )

    # ── Pre-warm rent cache for all districts (avoids N serial DB calls in scoring loop) ──
    # Map normalized key → first raw district string seen, so _estimate_rent_sar_m2_year
    # receives the raw value (matching the scoring loop contract — the aqar fallback
    # inside that function matches on the raw/display district string, not the
    # normalized key).
    _norm_to_raw: dict[str | None, str | None] = {}
    for _r in rows:
        _d = _r.get("district")
        _dn = normalize_district_key(_d) if _d else None
        if _dn not in _norm_to_raw:
            _norm_to_raw[_dn] = _d
    rent_cache: dict[str | None, tuple[float, str]] = {}
    for _dk, _raw_d in _norm_to_raw.items():
        try:
            rent_cache[_dk] = _estimate_rent_sar_m2_year(db, _raw_d)
        except Exception:
            logger.debug("rent pre-warm failed for district=%s", _dk, exc_info=True)
    t_rent_prewarm_done = time.monotonic()
    logger.info(
        "expansion_search timing: rent_prewarm=%.2fs districts=%d search_id=%s",
        t_rent_prewarm_done - t_district_momentum_done, len(_norm_to_raw), search_id,
    )

    # ── Pre-compute Black Marble VNP46A3 radiance YoY signal per district ──
    # Latest available month vs. same calendar month one year earlier (simple
    # YoY). Pre-loaded in one query before the candidate loop to avoid N+1.
    # Keyed by norm_district("riyadh", district_label) — matches the key used
    # at ingest time in app.ingest.black_marble_radiance.
    #
    # District areas (Patch A1). One per-search query against the same
    # polygon source the radiance ingest uses (external_feature, layer
    # 'aqar_district_hulls'); normalized in Python via norm_district to
    # mirror ingest semantics exactly. Used by evaluate_confidence to
    # apply the small-district floor / large-district outlier WARNING.
    _district_area_km2: dict[str, float] = {}
    try:
        _area_rows = db.execute(text("""
            SELECT
                TRIM(COALESCE(ef.properties->>'district_raw',
                              ef.properties->>'district')) AS district_label,
                ST_Area(
                    ST_SetSRID(ST_GeomFromGeoJSON(ef.geometry::text), 4326)::geography
                ) / 1e6 AS area_km2
            FROM external_feature ef
            WHERE ef.layer_name = 'aqar_district_hulls'
              AND ef.geometry IS NOT NULL
              AND jsonb_typeof(ef.geometry) = 'object'
              AND COALESCE(ef.properties->>'district_raw',
                           ef.properties->>'district') IS NOT NULL
              AND TRIM(COALESCE(ef.properties->>'district_raw',
                                ef.properties->>'district')) <> ''
        """)).mappings().all()
        for _ar in _area_rows:
            _label = _ar["district_label"]
            _area = _ar["area_km2"]
            if not _label or _area is None:
                continue
            _key = norm_district("riyadh", str(_label).strip())
            if not _key:
                continue
            # Duplicate district_label rows occasionally exist in
            # external_feature; keep the largest polygon to be safe (a
            # merged sibling would still be flagged by the outlier rule).
            _area_f = float(_area)
            if _key not in _district_area_km2 or _area_f > _district_area_km2[_key]:
                _district_area_km2[_key] = _area_f
    except Exception:
        # Polygon table missing / schema mismatch — degrade silently. The
        # confidence helper treats area_km2 is None as "unknown, don't
        # penalize", so the legacy pixel-only rule applies.
        logger.debug("district area lookup failed", exc_info=True)

    _radiance_lookup: dict[str, dict[str, Any]] = {}
    try:
        _radiance_rows = db.execute(text("""
            WITH ordered AS (
                SELECT
                    district_key,
                    year_month,
                    radiance_mean,
                    pixel_count_valid,
                    ROW_NUMBER() OVER (
                        PARTITION BY district_key ORDER BY year_month
                    ) AS rn
                FROM district_radiance_monthly
                WHERE source = :src AND quality_filter = :ql
            ),
            windowed AS (
                SELECT
                    district_key,
                    year_month,
                    rn,
                    pixel_count_valid,
                    -- Pixel-weighted mean over the latest 6 months [rn-5, rn]
                    SUM(radiance_mean * pixel_count_valid)
                        FILTER (WHERE radiance_mean IS NOT NULL AND pixel_count_valid > 0)
                        OVER w_cur
                    / NULLIF(
                        SUM(pixel_count_valid)
                            FILTER (WHERE radiance_mean IS NOT NULL AND pixel_count_valid > 0)
                            OVER w_cur,
                        0
                    ) AS rad_cur6,
                    -- Pixel-weighted mean over same 6 calendar positions a year ago [rn-17, rn-12]
                    SUM(radiance_mean * pixel_count_valid)
                        FILTER (WHERE radiance_mean IS NOT NULL AND pixel_count_valid > 0)
                        OVER w_prev
                    / NULLIF(
                        SUM(pixel_count_valid)
                            FILTER (WHERE radiance_mean IS NOT NULL AND pixel_count_valid > 0)
                            OVER w_prev,
                        0
                    ) AS rad_prev6,
                    -- Confidence gate: worst-case pixel count across each 6-month window
                    MIN(pixel_count_valid) OVER w_cur  AS min_pixels_cur6,
                    MIN(pixel_count_valid) OVER w_prev AS min_pixels_prev6,
                    -- Row counts must be 6 (guards districts added mid-backfill)
                    COUNT(*) OVER w_cur  AS rows_cur6,
                    COUNT(*) OVER w_prev AS rows_prev6
                FROM ordered
                WINDOW
                    w_cur  AS (PARTITION BY district_key ORDER BY year_month
                               ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
                    w_prev AS (PARTITION BY district_key ORDER BY year_month
                               ROWS BETWEEN 17 PRECEDING AND 12 PRECEDING)
            ),
            latest_per_district AS (
                SELECT *
                FROM windowed
                WHERE (district_key, rn) IN (
                    SELECT district_key, MAX(rn) FROM windowed GROUP BY district_key
                )
            )
            SELECT
                district_key,
                rad_cur6                    AS rad_cur,
                min_pixels_cur6             AS pixels_cur,
                year_month                  AS ym_cur,
                rad_prev6                   AS rad_prev,
                min_pixels_prev6            AS pixels_prev,
                rows_cur6, rows_prev6
            FROM latest_per_district
            WHERE rad_cur6  IS NOT NULL
              AND rad_prev6  IS NOT NULL
              AND rows_cur6  = 6
              AND rows_prev6 = 6
        """), {
            "src": "nasa_blackmarble_vnp46a3_c2",
            "ql": QUALITY_FILTER_LABEL,
        }).mappings().all()
    except Exception:
        # Table may not exist yet (migration not applied) — degrade silently.
        logger.debug("district_radiance_monthly lookup failed", exc_info=True)
        _radiance_rows = []

    for _r in _radiance_rows:
        _dk = _r["district_key"]
        _pixels_cur = int(_r["pixels_cur"] or 0)
        _pixels_prev = int(_r["pixels_prev"] or 0)
        _area_km2 = _district_area_km2.get(_dk)
        _confident, _confidence_reason = _blackmarble_evaluate_confidence(
            pixels_cur=_pixels_cur,
            pixels_prev=_pixels_prev,
            area_km2=_area_km2,
            district_key=_dk,
        )
        _yoy_pct: float | None = None
        if _confident and _r["rad_prev"] and float(_r["rad_prev"]) > 0:
            _yoy_pct = (
                float(_r["rad_cur"]) - float(_r["rad_prev"])
            ) / float(_r["rad_prev"]) * 100.0
        _radiance_lookup[_dk] = {
            "value_yoy_pct": round(_yoy_pct, 2) if _yoy_pct is not None else None,
            "source_label": "blackmarble_district_yoy_rolling6",
            "confident": _confident and _yoy_pct is not None,
            "confidence_reason": _confidence_reason,
            "pixel_count": _pixels_cur,
            "year_month": str(_r["ym_cur"]),
        }
    logger.info(
        "expansion_search radiance lookup: districts=%d search_id=%s",
        len(_radiance_lookup), search_id,
    )

    for row in rows:
      try:
        area_m2 = _safe_float(row.get("area_m2"))
        # ── Search-time area cap for Tier 3 (parcel-derived) candidates ──
        # Even after batch tiered conversion, some converted areas may exceed
        # the user's requested max. Cap at max_area_m2 so the UI never shows
        # a unit larger than what the operator asked for.
        _source_tier = row.get("source_tier")
        if _source_tier == 3 and max_area_m2 and area_m2 and area_m2 > max_area_m2:
            area_m2 = max_area_m2
        # Scoring read: None/garbage → 0.0 (ranking math byte-identical).
        population_reach = _safe_float(row.get("population_reach"))
        # Snapshot read: preserve None (no population-grid coverage) so the
        # persisted feature_snapshot_json distinguishes "unmeasured" from a
        # genuine "measured 0.0". Only the viability pass keys off this.
        _pop_reach_raw = row.get("population_reach")
        population_reach_measured: float | None = (
            population_reach if _pop_reach_raw is not None else None
        )
        competitor_count = _safe_int(row.get("competitor_count"))
        # F4: confidence flag emitted by _bulk_enrich_competitors. Falls
        # through as None on the ARCGIS-fallback candidate-pool SQL path
        # (preserves legacy behavior for rows that bypass bulk enrichment).
        _cc_confident_raw = row.get("competitor_count_confident")
        competitor_count_confident: bool | None = (
            bool(_cc_confident_raw) if _cc_confident_raw is not None else None
        )
        # Patch B: max chain_strength_score across same-category POIs in
        # the candidate's competition radius. Retained ONLY for the
        # chain_strength_max JSON diagnostic; it is NOT the leg input. None
        # when the bulk enrichment path was bypassed OR when no same-category
        # POI rows joined to expansion_competitor_quality.
        _max_chain_strength_raw = row.get("max_chain_strength")
        max_chain_strength: float | None = (
            float(_max_chain_strength_raw)
            if _max_chain_strength_raw is not None
            else None
        )
        # Strong-chain SHARE: the chain_strength leg input. None when the
        # bulk enrichment path was bypassed OR fewer than
        # EXPANSION_CHAIN_MIN_MATCHED in-category ECQ-matched POIs were in
        # radius. _chain_strength_score() converts None to a neutral 50 so
        # thin-data candidates aren't penalized.
        _chain_strength_share_raw = row.get("chain_strength_share")
        chain_strength_share: float | None = (
            float(_chain_strength_share_raw)
            if _chain_strength_share_raw is not None
            else None
        )
        delivery_listing_count = _safe_int(row.get("delivery_listing_count"))
        provider_listing_count = _safe_int(row.get("provider_listing_count"))
        provider_platform_count = _safe_int(row.get("provider_platform_count"))
        delivery_competition_count = _safe_int(row.get("delivery_competition_count"))
        landuse_label = row.get("landuse_label")
        landuse_code = row.get("landuse_code")
        # District resolution fallback chain. The primary source is the
        # Arabic district label populated by spatial matching against
        # external_feature polygons (aliased to "district" in the pool
        # SQL); when that fails (≈1.3% of rows in the current pool),
        # fall back through the English district, then the raw Aqar
        # neighborhood string from commercial_unit.neighborhood (already
        # projected as unit_neighborhood_raw since Patch 07), then a
        # clean placeholder. The English fallback values are surfaced
        # as-is rather than being translated or wrapped — the mixed
        # Arabic/English list is mild and the alternative ("—" or a
        # leaked aqar_id in the UI) is materially worse.
        district = (
            row.get("district")
            or row.get("district_en")
            or row.get("unit_neighborhood_raw")
            or "District unknown"
        )
        # ── Apply bulk delivery enrichment results ──
        _pid_key = str(row.get("parcel_id") or "")
        _realized_demand_30d: float | None = None
        _realized_demand_branches: int = 0
        if _pid_key and _pid_key in _bulk_delivery:
            _del_stats = _bulk_delivery[_pid_key]
            provider_listing_count = _del_stats["listing_count"]
            provider_platform_count = _del_stats["platform_count"]
            delivery_listing_count = _del_stats["cat_count"]
            delivery_competition_count = delivery_listing_count
            # Realized-demand signal (populated only when feature flag is on
            # AND ≥2 rating_count snapshots exist for branches in the catchment).
            _rd_raw = _del_stats.get("realized_demand_30d")
            _realized_demand_branches = int(_del_stats.get("realized_demand_branches") or 0)
            if _rd_raw is not None and _realized_demand_branches >= 3:
                _realized_demand_30d = float(_rd_raw)

        district_norm = normalize_district_key(district)
        if target_district_norm and (not district_norm or district_norm not in target_district_norm):
            continue

        pop_score = _population_score(population_reach, service_model=service_model)
        delivery_score = _delivery_score(
            delivery_listing_count,
            realized_demand=_realized_demand_30d,
            blend_weight=settings.EXPANSION_REALIZED_DEMAND_BLEND,
            reference=_realized_demand_reference(service_model),
        )
        _pop_w, _del_w = _demand_blend_weights(service_model)
        demand_score = _clamp(pop_score * _pop_w + delivery_score * _del_w)

        whitespace_score = _competition_whitespace_score(
            competitor_count,
            confident=competitor_count_confident,
            service_model=service_model,
        )
        chain_strength_score = _chain_strength_score(chain_strength_share)

        area_fit = _area_fit(area_m2, target_area_m2, min_area_m2, max_area_m2)
        zoning_fit_score = _zoning_fit_score(landuse_label, landuse_code)
        fit_score = _clamp(area_fit * 0.55 + zoning_fit_score * 0.45)

        # Hard exclusion: industrial parcels are never suitable for
        # customer-facing F&B formats (cafe, dine_in).
        _zoning_class = _zoning_signal_class(landuse_label, landuse_code)
        if _zoning_class == "industrial" and service_model in ("cafe", "dine_in"):
            continue  # skip this parcel entirely

        # Guard: when no delivery data is observed, scores must reflect
        # *uncertainty* (neutral 50), not opportunity (100).  Without this,
        # the whitespace formula yields 100 for zero-data candidates.
        # Require a minimum signal threshold before treating delivery data as
        # meaningful.  A single incidental listing (e.g. one non-category
        # restaurant) is noise, not a market signal — it would otherwise drive
        # provider_whitespace_score to ~100, indistinguishable from a genuinely
        # uncontested area.  Thresholds: ≥5 total listings OR ≥2 platforms OR
        # ≥2 same-category competitors in the delivery radius.
        _delivery_observed = (
            provider_listing_count >= 5
            or provider_platform_count >= 2
            or delivery_competition_count >= 2
        )
        if _delivery_observed:
            # Log-scale provider density to avoid saturation in dense districts
            provider_density_score = _clamp(
                (math.log1p(provider_listing_count) / math.log1p(150)) * 100.0
            )
            _raw_whitespace = _clamp(
                100.0
                - max(0.0, (delivery_competition_count - 6) * 6.0)
                - min(35.0, provider_density_score * 0.2)
            )
            # Dampen whitespace when delivery data is thin (confidence scaling).
            _data_confidence = min(1.0, max(0.3, provider_listing_count / 20.0))
            _absolute_whitespace = 50.0 + (_raw_whitespace - 50.0) * _data_confidence

            # Relative whitespace: preserve intra-district differentiation even
            # in fully-saturated zones.  Floor at 10 so competitors at different
            # competition densities are still distinguishable.  The gate check
            # uses delivery_competition_score (below) to flag saturation.
            provider_whitespace_score = max(10.0, _absolute_whitespace)

            # Platform presence: score relative to platforms that *actually have
            # data*.  Do not floor at 2 — that produces a systematic 50 for
            # single-platform environments.
            if _active_platform_count >= 1:
                multi_platform_presence_score = _clamp(
                    (provider_platform_count / float(_active_platform_count)) * 100.0
                )
            else:
                multi_platform_presence_score = 50.0  # unknown, not zero, not 100

            # Log-scale delivery competition to avoid saturation in dense districts
            delivery_competition_score = _clamp(
                (math.log1p(delivery_competition_count) / math.log1p(80)) * 100.0
            )
        else:
            # Spatial radius returned insufficient data. Try district-level
            # fallback before defaulting to neutral/zero scores.
            _dd = _district_delivery_stats.get(district_norm) if district_norm else None
            if _dd and _dd["total"] >= 5:
                # District has real delivery data — use it with a confidence
                # penalty (max 0.65) reflecting the coarser resolution.
                _dd_conf = min(0.65, _dd["total"] / 200.0)

                provider_density_score = _clamp(
                    (math.log1p(_dd["total"]) / math.log1p(500)) * 100.0
                ) * _dd_conf

                # Category saturation: fewer same-category restaurants relative
                # to city median = more whitespace opportunity.
                _dd_cat = _dd.get("cat_count", 0)
                _city_med_cat = _city_delivery_benchmarks.get("median_cat", 10)
                _cat_ratio = min(2.0, _dd_cat / max(1, _city_med_cat))
                provider_whitespace_score = _clamp(
                    50.0 + (1.0 - _cat_ratio) * 30.0
                )

                if _active_platform_count >= 1:
                    multi_platform_presence_score = _clamp(
                        (_dd["platforms"] / float(_active_platform_count)) * 100.0
                    )
                else:
                    multi_platform_presence_score = 50.0

                delivery_competition_score = _clamp(
                    (math.log1p(_dd_cat) / math.log1p(80)) * 100.0
                ) * _dd_conf

                # Feed district signal into delivery_listing_count for demand_score.
                # Scale down to reflect that this is district-wide, not 1.2km radius.
                if delivery_listing_count == 0 and _dd_cat > 0:
                    delivery_listing_count = max(1, int(_dd_cat * 0.15))
            else:
                # No spatial data AND no district data — truly unknown.
                provider_density_score = 0.0
                provider_whitespace_score = 50.0   # unknown ≠ excellent
                multi_platform_presence_score = 0.0
                delivery_competition_score = 0.0

        # Recompute demand_score if district fallback modified delivery_listing_count.
        # Realized-demand signal (if any) still applies at catchment level, so
        # pass it through — the district fallback only adjusts listing_count.
        delivery_score = _delivery_score(
            delivery_listing_count,
            realized_demand=_realized_demand_30d,
            blend_weight=settings.EXPANSION_REALIZED_DEMAND_BLEND,
            reference=_realized_demand_reference(service_model),
        )
        demand_score = _clamp(pop_score * _pop_w + delivery_score * _del_w)

        _is_listing = bool(row.get("commercial_unit_id"))
        confidence_score = _confidence_score(
            is_listing=_is_listing,
            rent_confidence=row.get("rent_confidence"),
            area_confidence=row.get("area_confidence"),
            unit_street_width_m=_safe_float(row.get("unit_street_width_m")) if row.get("unit_street_width_m") else None,
            image_url=row.get("image_url"),
            landuse_label=landuse_label,
            population_reach=population_reach,
            delivery_listing_count=delivery_listing_count,
        )
        distance_to_nearest_branch_m = _nearest_branch_distance_m(
            _safe_float(row.get("lat")),
            _safe_float(row.get("lon")),
            existing_branches,
        )
        cannibalization_score = _cannibalization_score(distance_to_nearest_branch_m, service_model)

        # ── Rent estimation: use actual rent for commercial units, estimated for parcels ──
        _cu_actual_rent = row.get("unit_price_sar_annual")
        _cu_actual_area = row.get("unit_area_sqm") or area_m2
        if row.get("commercial_unit_id") and _cu_actual_rent and _cu_actual_area and _cu_actual_area > 0:
            estimated_rent_sar_m2_year = round(float(_cu_actual_rent) / float(_cu_actual_area), 2)
            estimated_annual_rent_sar = round(float(_cu_actual_rent))
            rent_source = "commercial_unit_actual"
            _rent_micro_meta = {"source": "commercial_unit", "actual_rent": True}
            _base_rent_sar_m2_year = estimated_rent_sar_m2_year
        else:
            rent_cache_key = district_norm or None
            if rent_cache_key not in rent_cache:
                rent_cache[rent_cache_key] = _estimate_rent_sar_m2_year(db, district)
            _base_rent_sar_m2_year, rent_source = rent_cache[rent_cache_key]

            # Micro-location rent adjustment: vary district median by local
            # commercial activity signals (delivery density, population,
            # competition) to differentiate parcels within the same district.
            _rent_multiplier, _rent_micro_meta = _rent_micro_location_multiplier(
                provider_listing_count=provider_listing_count,
                delivery_competition_count=delivery_competition_count,
                population_reach=population_reach,
                competitor_count=competitor_count,
                district_delivery_stats=_district_delivery_stats.get(district_norm) if district_norm else None,
                city_benchmarks=_city_delivery_benchmarks,
            )
            estimated_rent_sar_m2_year = round(_base_rent_sar_m2_year * _rent_multiplier, 2)
            if abs(_rent_multiplier - 1.0) > 0.01:
                rent_source = f"{rent_source}+micro"
            estimated_annual_rent_sar = round(area_m2 * estimated_rent_sar_m2_year)
        estimated_fitout_cost_sar = round(_estimate_fitout_cost_sar(area_m2, service_model, is_furnished=bool(row.get("unit_is_furnished"))))
        estimated_revenue_index, _revenue_index_detail = _estimate_revenue_index(
            area_m2=area_m2,
            target_area_m2=target_area_m2,
            unit_street_width_m=_safe_float(row.get("unit_street_width_m")) if row.get("unit_street_width_m") else None,
            unit_listing_type=row.get("unit_listing_type"),
            demand_score=demand_score,
            whitespace_score=whitespace_score,
            category=category,
            price_tier=effective_brand_profile.get("price_tier"),
            return_detail=True,
        )
        economics_score, economics_meta = _economics_score(
            estimated_revenue_index=estimated_revenue_index,
            estimated_annual_rent_sar=estimated_annual_rent_sar,
            estimated_fitout_cost_sar=estimated_fitout_cost_sar,
            area_m2=area_m2,
            cannibalization_score=cannibalization_score,
            fit_score=fit_score,
            db=db,
            is_listing=_is_listing,
            district=district,
            listing_type=row.get("unit_listing_type"),
            unit_neighborhood_raw=row.get("unit_neighborhood_raw"),
            price_tier=effective_brand_profile.get("price_tier"),
            revenue_index_detail=_revenue_index_detail,
        )
        _unit_street_width = _safe_float(row.get("unit_street_width_m")) if row.get("unit_street_width_m") else None
        frontage_score = _frontage_score(
            unit_street_width_m=_unit_street_width,
            parcel_perimeter_m=0.0,
            touches_road=False,
            nearby_road_count=0,
            nearest_major_road_m=None,
            road_context_available=False,
        )
        access_score = _access_score(
            unit_street_width_m=_unit_street_width,
            touches_road=False,
            nearest_major_road_m=None,
            nearby_road_count=0,
            road_context_available=False,
        )
        parking_score = _parking_score(
            area_m2=area_m2,
            service_model=service_model,
            nearby_parking_count=0,
            access_score=access_score,
            parking_context_available=False,
        )
        access_visibility_score = _access_visibility_score(
            frontage_score=frontage_score,
            access_score=access_score,
            brand_profile=effective_brand_profile,
        )
        brand_fit_score = _brand_fit_score(
            district=district,
            area_m2=area_m2,
            target_area_m2=target_area_m2,
            demand_score=demand_score,
            fit_score=fit_score,
            cannibalization_score=cannibalization_score,
            provider_density_score=provider_density_score,
            provider_whitespace_score=provider_whitespace_score,
            multi_platform_presence_score=multi_platform_presence_score,
            delivery_competition_score=delivery_competition_score,
            visibility_signal=access_visibility_score,
            parking_signal=parking_score,
            brand_profile=effective_brand_profile,
            service_model=service_model,
        )
        # multi_platform_presence_score is excluded from the composite because
        # only 1 of 14 delivery scrapers (HungerStation) is producing data
        # today, which collapses the signal to 0 for ~0.15% of candidates and
        # 100 for ~99.85% — pure noise. The remaining three inputs keep their
        # original ratio (0.28 : 0.30 : 0.20) renormalized to sum to 1.0.
        provider_intelligence_composite = _clamp(
            provider_density_score * 0.36
            + provider_whitespace_score * 0.38
            + (100.0 - delivery_competition_score) * 0.26
        )

        effective_age_days, _ = _effective_listing_age_days(row)
        # district_norm is computed at line ~5757 earlier in this loop; reuse it
        _momentum_entry = _district_momentum.get(district_norm) if district_norm else None
        _district_momentum_score_val = (
            _momentum_entry["momentum_score"] if _momentum_entry else None
        )
        listing_quality = _listing_quality_score(
            is_listing=_is_listing,
            effective_age_days=effective_age_days,
            is_furnished=row.get("unit_is_furnished"),
            unit_restaurant_score=_safe_float(row.get("unit_restaurant_score")) if row.get("unit_restaurant_score") is not None else None,
            has_image=bool(row.get("image_url")),
            has_drive_thru=row.get("unit_has_drive_thru"),
            llm_suitability_score=row.get("unit_llm_suitability_score"),
            llm_listing_quality_score=row.get("unit_llm_listing_quality_score"),
            district_momentum_score=_district_momentum_score_val,
        )
        preliminary_breakdown = _score_breakdown(
            demand_score=demand_score,
            whitespace_score=whitespace_score,
            brand_fit_score=brand_fit_score,
            economics_score=economics_score,
            provider_intelligence_composite=provider_intelligence_composite,
            access_visibility_score=access_visibility_score,
            confidence_score=confidence_score,
            listing_quality_score=listing_quality,
            landlord_signal_score=row.get("unit_llm_landlord_signal_score"),
            chain_strength_score=chain_strength_score,
            chain_strength_max=max_chain_strength,
            brand_profile=effective_brand_profile,
            service_model=service_model,
            district_momentum_score=_district_momentum_score_val,
        )
        prepared.append(
            {
                "row": dict(row),
                "area_m2": area_m2,
                "population_reach": population_reach,
                "population_reach_measured": population_reach_measured,
                "competitor_count": competitor_count,
                "competitor_count_confident": competitor_count_confident,
                "max_chain_strength": max_chain_strength,
                "chain_strength_share": chain_strength_share,
                "chain_strength_score": chain_strength_score,
                "delivery_listing_count": delivery_listing_count,
                "provider_listing_count": provider_listing_count,
                "provider_platform_count": provider_platform_count,
                "delivery_competition_count": delivery_competition_count,
                "landuse_label": landuse_label,
                "landuse_code": landuse_code,
                "district": district,
                "demand_score": demand_score,
                # PR-2: the final first-pass delivery term, kept so the second
                # pass can re-blend demand_score with the L1 composite numerator
                # without recomputing _delivery_score.
                "delivery_score": delivery_score,
                "whitespace_score": whitespace_score,
                "fit_score": fit_score,
                "area_fit": area_fit,
                "zoning_fit_score": zoning_fit_score,
                "provider_density_score": provider_density_score,
                "provider_whitespace_score": provider_whitespace_score,
                "multi_platform_presence_score": multi_platform_presence_score,
                "delivery_competition_score": delivery_competition_score,
                "confidence_score": confidence_score,
                "distance_to_nearest_branch_m": distance_to_nearest_branch_m,
                "cannibalization_score": cannibalization_score,
                "estimated_rent_sar_m2_year": estimated_rent_sar_m2_year,
                "rent_source": rent_source,
                "rent_micro_meta": _rent_micro_meta,
                "rent_base_sar_m2_year": _base_rent_sar_m2_year,
                "estimated_annual_rent_sar": estimated_annual_rent_sar,
                "estimated_fitout_cost_sar": estimated_fitout_cost_sar,
                "estimated_revenue_index": estimated_revenue_index,
                "economics_score": economics_score,
                "economics_meta": economics_meta,
                "provider_intelligence_composite": provider_intelligence_composite,
                "preliminary_final_score": _safe_float(preliminary_breakdown.get("final_score")),
            }
        )
      except Exception:
        logger.warning(
            "Expansion search: skipping candidate parcel_id=%s due to scoring error: search_id=%s",
            row.get("parcel_id"), search_id,
            exc_info=True,
        )

    t_coarse_done = time.monotonic()

    prepared.sort(key=lambda item: item["preliminary_final_score"], reverse=True)
    shortlist_size = min(len(prepared), max(limit, 25))

    # ── Bulk spatial queries for feature snapshot (replaces per-candidate N+1) ──
    _shortlist_parcel_ids = [
        str(p["row"].get("parcel_id") or "")
        for p in prepared[:shortlist_size]
        if p["row"].get("parcel_id")
    ]
    # Coordinate lookup for spatial enrichment (works for all candidate sources)
    _shortlist_coords: dict[str, tuple[float, float]] = {}
    for p in prepared[:shortlist_size]:
        _pid = str(p["row"].get("parcel_id") or "")
        _slon = _safe_float(p["row"].get("lon"))
        _slat = _safe_float(p["row"].get("lat"))
        if _pid and _slon != 0.0 and _slat != 0.0:
            _shortlist_coords[_pid] = (_slon, _slat)
    _bulk_perimeter: dict[str, float] = {}
    _bulk_roads: dict[str, dict[str, Any]] = {}
    _bulk_parking: dict[str, int] = {}

    if _shortlist_parcel_ids:
        # ── Bulk perimeter ──
        t_perim_start = time.monotonic()
        try:
            # Try ArcGIS parcel join first (works for legacy parcel candidates)
            _perim_values = ", ".join(f"(:pid_{i})" for i in range(len(_shortlist_parcel_ids)))
            _perim_params = {f"pid_{i}": pid for i, pid in enumerate(_shortlist_parcel_ids)}
            with db.begin_nested():
                _perim_rows = db.execute(
                    text(f"""
                        WITH pids(parcel_id) AS (VALUES {_perim_values})
                        SELECT p.id::text AS parcel_id,
                               COALESCE(ST_Perimeter(p.geom::geography), 0) AS parcel_perimeter_m
                        FROM pids
                        JOIN {ARCGIS_PARCELS_TABLE} p ON p.id::text = pids.parcel_id
                    """),
                    _perim_params,
                ).mappings().all()
            for r in _perim_rows:
                _bulk_perimeter[str(r["parcel_id"])] = round(_safe_float(r.get("parcel_perimeter_m")), 2)
        except Exception:
            logger.debug("expansion_search bulk perimeter (arcgis) failed", exc_info=True)
        # Estimate perimeter for candidates not matched via ArcGIS (CU/CL sources)
        for _pid in _shortlist_parcel_ids:
            if _pid not in _bulk_perimeter:
                # Square approximation: perimeter ≈ 4 * sqrt(area)
                _area = 0.0
                for p in prepared[:shortlist_size]:
                    if str(p["row"].get("parcel_id") or "") == _pid:
                        _area = _safe_float(p.get("area_m2"))
                        break
                if _area > 0:
                    _bulk_perimeter[_pid] = round(4.0 * (_area ** 0.5), 2)
        logger.info("expansion_search bulk perimeter: enriched=%d/%d search_id=%s",
                    len(_bulk_perimeter), len(_shortlist_parcel_ids), search_id)
        t_perim_done = time.monotonic()
        logger.info("expansion_search timing: bulk_perimeter=%.2fs search_id=%s",
                     t_perim_done - t_perim_start, search_id)

        # ── Bulk roads ──
        t_roads_start = time.monotonic()
        _roads_source_table = None
        if ea_roads_populated or roads_table_available:
            # Build VALUES with coordinates for spatial queries
            _road_value_parts: list[str] = []
            _road_params: dict[str, Any] = {}
            for i, pid in enumerate(_shortlist_parcel_ids):
                coords = _shortlist_coords.get(pid)
                if coords:
                    _road_value_parts.append(f"(:rpid_{i}, CAST(:rlon_{i} AS double precision), CAST(:rlat_{i} AS double precision))")
                    _road_params[f"rpid_{i}"] = pid
                    _road_params[f"rlon_{i}"] = coords[0]
                    _road_params[f"rlat_{i}"] = coords[1]

            if _road_value_parts:
                _road_values_sql = ", ".join(_road_value_parts)

                if ea_roads_populated:
                    _roads_source_table = "expansion_road_context"
                    _roads_query = f"""
                        WITH pids(parcel_id, lon, lat) AS (VALUES {_road_values_sql})
                        SELECT
                            pids.parcel_id,
                            COALESCE(
                                (SELECT MIN(ST_Distance(erc.geom::geography,
                                    ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography))
                                 FROM {_EA_ROADS_TABLE} erc
                                 WHERE erc.is_major_road = TRUE AND erc.geom IS NOT NULL
                                   AND ST_DWithin(erc.geom::geography,
                                       ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography, 700)),
                                {_ROAD_DISTANCE_SENTINEL_M}
                            ) AS nearest_major_road_distance_m,
                            COALESCE((
                                SELECT COUNT(*) FROM {_EA_ROADS_TABLE} erc
                                WHERE erc.geom IS NOT NULL
                                  AND ST_DWithin(erc.geom::geography,
                                      ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography, 250)
                            ), 0) AS nearby_road_segment_count,
                            EXISTS(
                                SELECT 1 FROM {_EA_ROADS_TABLE} erc
                                WHERE erc.geom IS NOT NULL
                                  AND ST_DWithin(erc.geom::geography,
                                      ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography, 18)
                            ) AS touches_road
                        FROM pids
                    """
                else:
                    _roads_source_table = "planet_osm_line"
                    _roads_query = f"""
                        WITH pids(parcel_id, lon, lat) AS (VALUES {_road_values_sql})
                        SELECT
                            pids.parcel_id,
                            COALESCE((
                                SELECT MIN(ST_Distance(l.way::geography,
                                    ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography))
                                FROM planet_osm_line l
                                WHERE l.way IS NOT NULL
                                  AND (l.highway IN ('motorway','trunk','primary','secondary')
                                       OR NULLIF(l.name, '') IS NOT NULL)
                                  AND ST_DWithin(l.way::geography,
                                      ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography, 700)
                            ), {_ROAD_DISTANCE_SENTINEL_M}) AS nearest_major_road_distance_m,
                            COALESCE((
                                SELECT COUNT(*) FROM planet_osm_line l
                                WHERE l.way IS NOT NULL AND l.highway IS NOT NULL
                                  AND ST_DWithin(l.way::geography,
                                      ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography, 250)
                            ), 0) AS nearby_road_segment_count,
                            EXISTS(
                                SELECT 1 FROM planet_osm_line l
                                WHERE l.way IS NOT NULL AND l.highway IS NOT NULL
                                  AND ST_DWithin(l.way::geography,
                                      ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography, 18)
                            ) AS touches_road
                        FROM pids
                    """

                try:
                    with db.begin_nested():
                        _road_rows = db.execute(
                            text(_roads_query),
                            _road_params,
                        ).mappings().all()
                    for r in _road_rows:
                        _bulk_roads[str(r["parcel_id"])] = {
                            "nearest_major_road_distance_m": round(_safe_float(r.get("nearest_major_road_distance_m")), 2),
                            "nearby_road_segment_count": _safe_int(r.get("nearby_road_segment_count")),
                            "touches_road": bool(r.get("touches_road")),
                            "source": _roads_source_table,
                        }
                    logger.info("expansion_search bulk roads: enriched=%d/%d search_id=%s",
                                len(_bulk_roads), len(_shortlist_parcel_ids), search_id)
                except Exception:
                    logger.debug("expansion_search bulk roads failed", exc_info=True)
        t_roads_done = time.monotonic()
        logger.info("expansion_search timing: bulk_roads=%.2fs search_id=%s",
                     t_roads_done - t_roads_start, search_id)

        # ── Bulk parking ──
        t_parking_start = time.monotonic()
        if ea_parking_populated or parking_table_available:
            _park_value_parts: list[str] = []
            _park_params: dict[str, Any] = {}
            for i, pid in enumerate(_shortlist_parcel_ids):
                coords = _shortlist_coords.get(pid)
                if coords:
                    _park_value_parts.append(f"(:ppid_{i}, CAST(:plon_{i} AS double precision), CAST(:plat_{i} AS double precision))")
                    _park_params[f"ppid_{i}"] = pid
                    _park_params[f"plon_{i}"] = coords[0]
                    _park_params[f"plat_{i}"] = coords[1]

            if _park_value_parts:
                _park_values_sql = ", ".join(_park_value_parts)

                if ea_parking_populated:
                    _parking_query = f"""
                        WITH pids(parcel_id, lon, lat) AS (VALUES {_park_values_sql})
                        SELECT pids.parcel_id,
                            COALESCE((
                                SELECT COUNT(*) FROM {_EA_PARKING_TABLE} epa
                                WHERE epa.geom IS NOT NULL
                                  AND ST_DWithin(epa.geom::geography,
                                      ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography, 350)
                            ), 0) AS nearby_parking_amenity_count
                        FROM pids
                    """
                else:
                    _parking_query = f"""
                        WITH pids(parcel_id, lon, lat) AS (VALUES {_park_values_sql})
                        SELECT pids.parcel_id,
                            COALESCE((
                                SELECT COUNT(*) FROM planet_osm_polygon op
                                WHERE op.way IS NOT NULL
                                  AND (lower(COALESCE(op.amenity, '')) = 'parking'
                                       OR lower(COALESCE(op.parking, '')) IN ('surface','multi-storey','underground'))
                                  AND ST_DWithin(op.way::geography,
                                      ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography, 350)
                            ), 0) AS nearby_parking_amenity_count
                        FROM pids
                    """

                try:
                    with db.begin_nested():
                        _park_rows = db.execute(
                            text(_parking_query),
                            _park_params,
                        ).mappings().all()
                    for r in _park_rows:
                        _bulk_parking[str(r["parcel_id"])] = _safe_int(r.get("nearby_parking_amenity_count"))
                    logger.info("expansion_search bulk parking: enriched=%d/%d search_id=%s",
                                len(_bulk_parking), len(_shortlist_parcel_ids), search_id)
                except Exception:
                    logger.debug("expansion_search bulk parking failed", exc_info=True)
        t_parking_done = time.monotonic()
        logger.info("expansion_search timing: bulk_parking=%.2fs search_id=%s",
                     t_parking_done - t_parking_start, search_id)

    # ── Bulk foot-traffic amenities (cafés only) ──
    if service_model == "cafe" and _shortlist_parcel_ids:
        t_ft_start = time.monotonic()
        # Query OSM for schools, mosques, parks, malls within 500m
        _ft_query = None
        if ea_parking_populated or parking_table_available:
            # Use planet_osm_polygon + planet_osm_point if available
            _ft_parts: list[str] = []
            if _cached_table_available(db, "planet_osm_polygon"):
                _ft_parts.append("""
                    SELECT ST_Centroid(op.way) AS geom
                    FROM planet_osm_polygon op
                    WHERE op.way IS NOT NULL
                      AND (
                        lower(COALESCE(op.amenity, '')) IN ('school', 'university', 'college', 'place_of_worship', 'mosque')
                        OR lower(COALESCE(op.leisure, '')) IN ('park', 'garden', 'playground')
                        OR lower(COALESCE(op.shop, '')) = 'mall'
                        OR lower(COALESCE(op.building, '')) IN ('mosque', 'school', 'university')
                      )
                """)
            if _cached_table_available(db, "planet_osm_point"):
                _ft_parts.append("""
                    SELECT pt.way AS geom
                    FROM planet_osm_point pt
                    WHERE pt.way IS NOT NULL
                      AND (
                        lower(COALESCE(pt.amenity, '')) IN ('school', 'university', 'college', 'place_of_worship', 'mosque')
                        OR lower(COALESCE(pt.leisure, '')) IN ('park', 'garden', 'playground')
                        OR lower(COALESCE(pt.shop, '')) = 'mall'
                      )
                """)
            if _ft_parts:
                _ft_union = " UNION ALL ".join(_ft_parts)
                _ft_query = f"""
                    WITH pids(parcel_id, lon, lat) AS (VALUES {{values}}),
                         foot_traffic_pois AS ({_ft_union})
                    SELECT pids.parcel_id,
                        COALESCE((
                            SELECT COUNT(*) FROM foot_traffic_pois fp
                            WHERE ST_DWithin(fp.geom::geography,
                                ST_SetSRID(ST_MakePoint(pids.lon, pids.lat), 4326)::geography, 500)
                        ), 0) AS nearby_foot_traffic_count
                    FROM pids
                """
        if _ft_query:
            try:
                _ft_value_parts: list[str] = []
                _ft_params: dict[str, Any] = {}
                for i, pid in enumerate(_shortlist_parcel_ids):
                    coords = _shortlist_coords.get(pid)
                    if coords:
                        _ft_value_parts.append(f"(:fpid_{i}, CAST(:flon_{i} AS double precision), CAST(:flat_{i} AS double precision))")
                        _ft_params[f"fpid_{i}"] = pid
                        _ft_params[f"flon_{i}"] = coords[0]
                        _ft_params[f"flat_{i}"] = coords[1]
                _ft_values = ", ".join(_ft_value_parts)
                with db.begin_nested():
                    _ft_rows = db.execute(
                        text(_ft_query.format(values=_ft_values)),
                        _ft_params,
                    ).mappings().all()
                for r in _ft_rows:
                    _bulk_foot_traffic[str(r["parcel_id"])] = _safe_int(r.get("nearby_foot_traffic_count"))
                logger.info("expansion_search bulk foot_traffic: enriched=%d/%d search_id=%s",
                            len(_bulk_foot_traffic), len(_shortlist_parcel_ids), search_id)
            except Exception:
                logger.debug("expansion_search bulk foot_traffic failed", exc_info=True)
        t_ft_done = time.monotonic()
        logger.info("expansion_search timing: bulk_foot_traffic=%.2fs search_id=%s",
                     t_ft_done - t_ft_start, search_id)

    # ── L1 demand-generator index enrichment (PR-1, emit-only, flag-gated) ──
    # Builds the per-candidate demand NUMERATOR signals for
    # feature_snapshot_json["demand_generator_index"]. Coordinate-based (the
    # _shortlist_coords / ST_MakePoint pattern), one bulk query per source (no
    # per-candidate N+1), each independently guarded by _cached_table_available
    # so a missing externally-imported table no-ops without affecting the rest.
    # NOTHING here feeds scoring (PR-2 wires it in).
    if settings.EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED and _shortlist_coords:
        t_dg_start = time.monotonic()
        # Change-1 (E.2): enrich at THIS service model's demand catchment, not the
        # flat EXPANSION_DEMAND_GENERATOR_RADIUS_M. dine_in stays 3500 (identical),
        # qsr -> 1500, cafe -> 1000, delivery_first -> 3000. Required so QSR's l1_v3
        # 1500 m anchors are applied to 1500 m counts (not 3500 m counts).
        _dg_radius = _demand_generator_radius_m(service_model)
        # Shared VALUES list of (parcel_id, lon, lat) for the whole shortlist.
        _dg_value_parts: list[str] = []
        _dg_coord_params: dict[str, Any] = {}
        for _dgi, (_dg_pid, _dg_co) in enumerate(_shortlist_coords.items()):
            _dg_value_parts.append(
                f"(:dgp_{_dgi}, CAST(:dgx_{_dgi} AS double precision), CAST(:dgy_{_dgi} AS double precision))"
            )
            _dg_coord_params[f"dgp_{_dgi}"] = _dg_pid
            _dg_coord_params[f"dgx_{_dgi}"] = _dg_co[0]
            _dg_coord_params[f"dgy_{_dgi}"] = _dg_co[1]
        _dg_values_sql = ", ".join(_dg_value_parts)

        # 1) OSM trip generators (planet_osm_point + planet_osm_polygon; way=4326
        #    via osm2pgsql --latlong, but ST_Transform(...,4326) is applied
        #    defensively to mirror restaurant_scoring_factors._demand_anchor_score).
        _dg_osm_point_ok = _cached_table_available(db, "public.planet_osm_point")
        _dg_osm_poly_ok = _cached_table_available(db, "public.planet_osm_polygon")
        if _dg_values_sql and (_dg_osm_point_ok or _dg_osm_poly_ok):
            # CASE tags each feature with its generator bucket using the
            # default.style dedicated columns the repo already relies on
            # (amenity/shop/building) plus standard office/railway/tourism.
            # TODO(L1): public_transport lives only in the hstore `tags` column;
            # excluded here to avoid an hstore dependency. Add later via
            # tags->'public_transport' behind an hstore-safe guard.
            _dg_osm_case = """
                CASE
                  WHEN (lower(COALESCE(office,'')) <> '' OR lower(COALESCE(building,'')) IN ('office','commercial')) THEN 'offices'
                  WHEN (lower(COALESCE(shop,'')) IN ('mall','supermarket','department_store','wholesale') OR lower(COALESCE(amenity,'')) = 'marketplace') THEN 'malls_retail'
                  WHEN (lower(COALESCE(railway,'')) IN ('station','halt','tram_stop','subway_entrance','stop') OR lower(COALESCE(amenity,'')) IN ('bus_station')) THEN 'transit'
                  WHEN (lower(COALESCE(amenity,'')) IN ('place_of_worship','mosque') OR lower(COALESCE(building,'')) = 'mosque') THEN 'mosques'
                  WHEN (lower(COALESCE(amenity,'')) IN ('school','college','university','kindergarten') OR lower(COALESCE(building,'')) IN ('school','university')) THEN 'schools'
                  WHEN (lower(COALESCE(amenity,'')) IN ('hospital','clinic','doctors')) THEN 'hospitals'
                  WHEN (lower(COALESCE(tourism,'')) IN ('hotel','motel','hostel','guest_house') OR lower(COALESCE(building,'')) = 'hotel') THEN 'hotels'
                  ELSE NULL
                END
            """
            _dg_osm_union: list[str] = []
            if _dg_osm_point_ok:
                _dg_osm_union.append(f"""
                    SELECT {_dg_osm_case} AS kind
                    FROM planet_osm_point p
                    WHERE p.way IS NOT NULL
                      AND ST_DWithin(
                          ST_Transform(p.way, 4326)::geography,
                          ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography,
                          :dg_radius)
                """)
            if _dg_osm_poly_ok:
                _dg_osm_union.append(f"""
                    SELECT {_dg_osm_case} AS kind
                    FROM planet_osm_polygon pg
                    WHERE pg.way IS NOT NULL
                      AND ST_DWithin(
                          ST_Transform(pg.way, 4326)::geography,
                          ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography,
                          :dg_radius)
                """)
            _dg_osm_query = f"""
                WITH cand(parcel_id, lon, lat) AS (VALUES {_dg_values_sql})
                SELECT c.parcel_id,
                    COUNT(*) FILTER (WHERE g.kind = 'offices')      AS offices,
                    COUNT(*) FILTER (WHERE g.kind = 'malls_retail') AS malls_retail,
                    COUNT(*) FILTER (WHERE g.kind = 'transit')      AS transit,
                    COUNT(*) FILTER (WHERE g.kind = 'mosques')      AS mosques,
                    COUNT(*) FILTER (WHERE g.kind = 'schools')      AS schools,
                    COUNT(*) FILTER (WHERE g.kind = 'hospitals')    AS hospitals,
                    COUNT(*) FILTER (WHERE g.kind = 'hotels')       AS hotels
                FROM cand c
                LEFT JOIN LATERAL ({" UNION ALL ".join(_dg_osm_union)}) g
                    ON g.kind IS NOT NULL
                GROUP BY c.parcel_id
            """
            try:
                with db.begin_nested():
                    _dg_osm_rows = db.execute(
                        text(_dg_osm_query),
                        {**_dg_coord_params, "dg_radius": _dg_radius},
                    ).mappings().all()
                for _r in _dg_osm_rows:
                    _bulk_osm_generators[str(_r["parcel_id"])] = {
                        "offices": _safe_int(_r.get("offices")),
                        "malls_retail": _safe_int(_r.get("malls_retail")),
                        "transit": _safe_int(_r.get("transit")),
                        "mosques": _safe_int(_r.get("mosques")),
                        "schools": _safe_int(_r.get("schools")),
                        "hospitals": _safe_int(_r.get("hospitals")),
                        "hotels": _safe_int(_r.get("hotels")),
                    }
                logger.info("expansion_search L1 osm_generators: enriched=%d/%d search_id=%s",
                            len(_bulk_osm_generators), len(_shortlist_coords), search_id)
            except Exception:
                logger.debug("expansion_search L1 osm_generators failed", exc_info=True)

        # 2) Overture building floor-density (daytime-population proxy). geom is
        #    declared SRID 32638; ST_Transform(...,4326) handles it either way.
        #    floors_proxy mirrors overture_buildings_metrics.py:43-46; buildings
        #    without floor/height tags contribute 1 (footprint presence).
        if _dg_values_sql and _cached_table_available(db, "public.overture_buildings"):
            _dg_floors_query = f"""
                WITH cand(parcel_id, lon, lat) AS (VALUES {_dg_values_sql})
                SELECT c.parcel_id, COALESCE(b.floors_sum, 0) AS floors_sum
                FROM cand c
                LEFT JOIN LATERAL (
                    SELECT SUM(
                        CASE
                          WHEN o.num_floors IS NOT NULL AND o.num_floors > 0 THEN LEAST(60, GREATEST(1, round(o.num_floors)::int))
                          WHEN o.height IS NOT NULL AND o.height > 0 THEN LEAST(60, GREATEST(1, round(o.height / 3.2)::int))
                          ELSE 1
                        END
                    ) AS floors_sum
                    FROM overture_buildings o
                    WHERE o.geom IS NOT NULL
                      AND ST_DWithin(
                          ST_Transform(o.geom, 4326)::geography,
                          ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography,
                          :dg_radius)
                ) b ON TRUE
            """
            try:
                with db.begin_nested():
                    _dg_fl_rows = db.execute(
                        text(_dg_floors_query),
                        {**_dg_coord_params, "dg_radius": _dg_radius},
                    ).mappings().all()
                for _r in _dg_fl_rows:
                    _bulk_building_floors[str(_r["parcel_id"])] = _safe_float(_r.get("floors_sum"))
                logger.info("expansion_search L1 building_floors: enriched=%d/%d search_id=%s",
                            len(_bulk_building_floors), len(_shortlist_coords), search_id)
            except Exception:
                logger.debug("expansion_search L1 building_floors failed", exc_info=True)

        # 3) Free review_count-weighted F&B density (zero-cost BestTime stand-in).
        #    Uses the Advisor's own F&B category filter (keys from
        #    _expand_category) and excludes permanently-closed venues. Numerator
        #    only — competitor density stays in _competition_whitespace_score.
        _dg_category_keys = list(_cat_expanded["keys"] or [])
        if _dg_values_sql and _dg_category_keys and _cached_table_available(db, "public.restaurant_poi"):
            _dg_fnb_query = f"""
                WITH cand(parcel_id, lon, lat) AS (VALUES {_dg_values_sql})
                SELECT c.parcel_id,
                    COALESCE(f.review_weighted, 0) AS review_weighted,
                    COALESCE(f.venue_count, 0) AS venue_count
                FROM cand c
                LEFT JOIN LATERAL (
                    SELECT SUM(COALESCE(rp.review_count, 0)) AS review_weighted,
                           COUNT(*) AS venue_count
                    FROM restaurant_poi rp
                    WHERE rp.geom IS NOT NULL
                      AND rp.business_status IS DISTINCT FROM 'CLOSED_PERMANENTLY'
                      AND lower(rp.category) = ANY(:dg_category_keys)
                      AND ST_DWithin(
                          rp.geom::geography,
                          ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography,
                          :dg_radius)
                ) f ON TRUE
            """
            try:
                with db.begin_nested():
                    _dg_fnb_rows = db.execute(
                        text(_dg_fnb_query),
                        {
                            **_dg_coord_params,
                            "dg_radius": _dg_radius,
                            "dg_category_keys": _dg_category_keys,
                        },
                    ).mappings().all()
                for _r in _dg_fnb_rows:
                    _bulk_fnb_density[str(_r["parcel_id"])] = {
                        "review_weighted": _safe_float(_r.get("review_weighted")),
                        "venue_count": _safe_int(_r.get("venue_count")),
                    }
                logger.info("expansion_search L1 fnb_density: enriched=%d/%d search_id=%s",
                            len(_bulk_fnb_density), len(_shortlist_coords), search_id)
            except Exception:
                logger.debug("expansion_search L1 fnb_density failed", exc_info=True)

        # 4) Tighter-radius catchment population (PR-1a). The index's population
        #    SUB-SCORE is computed at EXPANSION_DEMAND_GENERATOR_POP_RADIUS_M
        #    (~1500 m) where population actually varies; at the 3500 m demand
        #    radius every dense-Riyadh catchment holds ~250k people and the term
        #    barely discriminates. Mirrors _bulk_enrich_population's SQL but reuses
        #    the shared coord VALUES list. The full 3500 m population_reach is still
        #    retained raw in the snapshot.
        _dg_pop_radius = float(settings.EXPANSION_DEMAND_GENERATOR_POP_RADIUS_M)
        if _dg_values_sql and _cached_table_available(db, "public.population_density"):
            # population_density may be stored as a geom column or as lat/lon.
            _dg_pd_has_geom = False
            try:
                _dg_pd_has_geom = bool(db.execute(text(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_name = 'population_density' AND column_name = 'geom' LIMIT 1"
                )).scalar())
            except Exception:
                pass
            if _dg_pd_has_geom:
                _dg_pd_geo = "pd.geom::geography"
                _dg_pd_where = "pd.geom IS NOT NULL"
            else:
                _dg_pd_geo = "ST_SetSRID(ST_MakePoint(pd.lon::double precision, pd.lat::double precision), 4326)::geography"
                _dg_pd_where = "pd.lat IS NOT NULL AND pd.lon IS NOT NULL"
            _dg_pop_query = f"""
                WITH cand(parcel_id, lon, lat) AS (VALUES {_dg_values_sql})
                SELECT c.parcel_id, COALESCE(p.population_reach, 0) AS population_reach
                FROM cand c
                LEFT JOIN LATERAL (
                    SELECT COALESCE(SUM(pd.population), 0) AS population_reach
                    FROM population_density pd
                    WHERE {_dg_pd_where}
                      AND ST_DWithin(
                          ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography,
                          {_dg_pd_geo},
                          :dg_pop_radius)
                ) p ON TRUE
            """
            try:
                with db.begin_nested():
                    _dg_pop_rows = db.execute(
                        text(_dg_pop_query),
                        {**_dg_coord_params, "dg_pop_radius": _dg_pop_radius},
                    ).mappings().all()
                for _r in _dg_pop_rows:
                    _bulk_dg_pop_local[str(_r["parcel_id"])] = _safe_float(_r.get("population_reach"))
                logger.info("expansion_search L1 pop_local: enriched=%d/%d search_id=%s",
                            len(_bulk_dg_pop_local), len(_shortlist_coords), search_id)
            except Exception:
                logger.debug("expansion_search L1 pop_local failed", exc_info=True)

        logger.info("expansion_search timing: bulk_demand_generator=%.2fs search_id=%s",
                     time.monotonic() - t_dg_start, search_id)

    _bulk_competitors: dict[str, list[dict[str, Any]]] = {}
    t_comp_start = time.monotonic()
    if _shortlist_parcel_ids:
        _comp_source = "expansion_competitor_quality" if ea_competitor_populated else "restaurant_poi"
        try:
            _comp_params: dict[str, Any] = {"category": category}
            _comp_union_parts: list[str] = []
            for _ci, _cp in enumerate(prepared[:shortlist_size]):
                _clat = _safe_float(_cp["row"].get("lat"))
                _clon = _safe_float(_cp["row"].get("lon"))
                _cpid = str(_cp["row"].get("parcel_id") or "")
                if not (_clat and _clon and _cpid):
                    continue
                _comp_params[f"lat_{_ci}"] = _clat
                _comp_params[f"lon_{_ci}"] = _clon
                _comp_params[f"pid_{_ci}"] = _cpid

                if ea_competitor_populated:
                    _comp_union_parts.append(f"""
                        (SELECT * FROM (
                            SELECT DISTINCT ON (dedup_key)
                                candidate_pid, id, name, category, district,
                                rating, review_count, source, overall_quality_score,
                                canonical_brand_id, display_name_en, display_name_ar,
                                distance_m
                            FROM (
                                SELECT :pid_{_ci} AS candidate_pid,
                                       ecq.restaurant_poi_id AS id,
                                       ecq.brand_name AS name,
                                       ecq.category, ecq.district,
                                       ecq.review_score / 20.0 AS rating,
                                       ecq.review_count,
                                       '{_comp_source}' AS source,
                                       ecq.overall_quality_score,
                                       ecq.canonical_brand_id,
                                       ecq.display_name_en,
                                       ecq.display_name_ar,
                                       ST_Distance(ecq.geom::geography,
                                         ST_SetSRID(ST_MakePoint(:lon_{_ci}, :lat_{_ci}), 4326)::geography) AS distance_m,
                                       COALESCE(ecq.canonical_brand_id,
                                                'poi:' || ecq.restaurant_poi_id::text) AS dedup_key
                                FROM {_EA_COMPETITOR_TABLE} ecq
                                WHERE ecq.geom IS NOT NULL
                                  AND lower(COALESCE(ecq.category, '')) = lower(:category)
                                  AND ST_DWithin(ecq.geom::geography,
                                        ST_SetSRID(ST_MakePoint(:lon_{_ci}, :lat_{_ci}), 4326)::geography, 1500)
                            ) raw
                            ORDER BY dedup_key, distance_m ASC
                        ) deduped
                        ORDER BY distance_m ASC LIMIT 5)
                    """)
                else:
                    _comp_union_parts.append(f"""
                        (SELECT :pid_{_ci} AS candidate_pid,
                               rp.id, rp.name, rp.category, rp.district,
                               rp.rating, rp.review_count, rp.source,
                               NULL::double precision AS overall_quality_score,
                               ST_Distance(
                                   COALESCE(rp.geom, CASE WHEN rp.lon IS NOT NULL AND rp.lat IS NOT NULL
                                       THEN ST_SetSRID(ST_MakePoint(rp.lon, rp.lat), 4326) ELSE NULL END
                                   )::geography,
                                   ST_SetSRID(ST_MakePoint(:lon_{_ci}, :lat_{_ci}), 4326)::geography
                               ) AS distance_m
                        FROM restaurant_poi rp
                        WHERE lower(COALESCE(rp.category, '')) = lower(:category)
                          AND COALESCE(rp.geom, CASE WHEN rp.lon IS NOT NULL AND rp.lat IS NOT NULL
                              THEN ST_SetSRID(ST_MakePoint(rp.lon, rp.lat), 4326) ELSE NULL END) IS NOT NULL
                          AND ST_DWithin(
                              COALESCE(rp.geom, ST_SetSRID(ST_MakePoint(rp.lon, rp.lat), 4326))::geography,
                              ST_SetSRID(ST_MakePoint(:lon_{_ci}, :lat_{_ci}), 4326)::geography, 1500)
                        ORDER BY distance_m ASC LIMIT 5)
                    """)

            if _comp_union_parts:
                with db.begin_nested():
                    _comp_rows = db.execute(
                        text(" UNION ALL ".join(_comp_union_parts)),
                        _comp_params,
                    ).mappings().all()
                for r in _comp_rows:
                    _cpid_key = str(r["candidate_pid"])
                    if _cpid_key not in _bulk_competitors:
                        _bulk_competitors[_cpid_key] = []
                    _bulk_competitors[_cpid_key].append({
                        "id": r.get("id"),
                        "name": r.get("name"),
                        "category": r.get("category"),
                        "district": r.get("district"),
                        "rating": _safe_float(r.get("rating"), default=0.0) if r.get("rating") is not None else None,
                        "review_count": _safe_int(r.get("review_count"), default=0) if r.get("review_count") is not None else None,
                        "distance_m": round(_safe_float(r.get("distance_m"), default=0.0), 2),
                        "source": r.get("source"),
                        "overall_quality_score": _safe_float(r.get("overall_quality_score")) if r.get("overall_quality_score") is not None else None,
                        "canonical_brand_id": r.get("canonical_brand_id"),
                        "display_name_en": r.get("display_name_en"),
                        "display_name_ar": r.get("display_name_ar"),
                    })
                logger.info("expansion_search bulk competitors: enriched=%d/%d search_id=%s",
                            len(_bulk_competitors), len(_shortlist_parcel_ids), search_id)
        except Exception:
            logger.warning("expansion_search bulk competitors failed, falling back to per-candidate", exc_info=True)
    t_comp_done = time.monotonic()
    logger.info("expansion_search timing: bulk_competitors=%.2fs search_id=%s",
                 t_comp_done - t_comp_start, search_id)

    # ───────────────────────────────────────────────────────────────────
    # Brand presence aggregate (PR C): per candidate, count branches per
    # brand within 500m. Used by the Breakdown tab's Brand Presence block
    # to surface "major chains operating in this micro-market". Distinct
    # from the proximity-competitor query above (5 closest unique chains
    # within 1500m for the Market tab). The two run in parallel.
    #
    # Two-tier counting (Patch 02): canonical_brand_id covers ~19% of
    # ECQ rows in production. Filtering to canonical-only made ~81% of
    # competitor rows invisible to unique_brands / top_chains. Each
    # candidate's per-paren block now UNION ALLs:
    #   • canonical rows grouped by canonical_brand_id, AND
    #   • non-canonical rows grouped by a normalized name key (the same
    #     _CHAIN_NAME_NORM_SQL that brand_alias.alias_key is keyed on),
    #     excluding keys that already match a canonical alias and the
    #     ingest-time generic-word denylist (cafe, restaurant, …).
    # ───────────────────────────────────────────────────────────────────
    _bulk_brand_presence: dict[str, list[dict]] = {}
    if ea_competitor_populated and _shortlist_coords:
        _bp_norm_sql = _CHAIN_NAME_NORM_SQL.format(col="ecq.brand_name")
        _bp_denylist_sql = "(" + ", ".join(f"'{k}'" for k in _CHAIN_KEY_DENYLIST) + ")"
        _bp_union_parts: list[str] = []
        _bp_params: dict = {}
        for _bpi, (_bp_pid, (_bp_lon, _bp_lat)) in enumerate(_shortlist_coords.items()):
            _bp_params[f"bp_pid_{_bpi}"] = str(_bp_pid)
            _bp_params[f"bp_lon_{_bpi}"] = _bp_lon
            _bp_params[f"bp_lat_{_bpi}"] = _bp_lat
            _bp_union_parts.append(f"""
                (SELECT :bp_pid_{_bpi} AS candidate_pid,
                        ecq.canonical_brand_id,
                        NULL::text AS norm_name_key,
                        MAX(ecq.display_name_en) AS display_name_en,
                        MAX(ecq.display_name_ar) AS display_name_ar,
                        COUNT(*) AS branch_count,
                        MIN(ST_Distance(
                            ecq.geom::geography,
                            ST_SetSRID(ST_MakePoint(:bp_lon_{_bpi}, :bp_lat_{_bpi}), 4326)::geography
                        )) AS nearest_distance_m
                 FROM {_EA_COMPETITOR_TABLE} ecq
                 WHERE ecq.geom IS NOT NULL
                   AND ecq.canonical_brand_id IS NOT NULL
                   AND ST_DWithin(
                       ecq.geom::geography,
                       ST_SetSRID(ST_MakePoint(:bp_lon_{_bpi}, :bp_lat_{_bpi}), 4326)::geography,
                       500
                   )
                 GROUP BY ecq.canonical_brand_id)
                UNION ALL
                (SELECT candidate_pid,
                        NULL::varchar AS canonical_brand_id,
                        norm_name_key,
                        MAX(brand_name_repr) AS display_name_en,
                        NULL::varchar AS display_name_ar,
                        COUNT(*) AS branch_count,
                        MIN(distance_m) AS nearest_distance_m
                 FROM (
                     SELECT :bp_pid_{_bpi} AS candidate_pid,
                            ecq.brand_name AS brand_name_repr,
                            {_bp_norm_sql} AS norm_name_key,
                            ST_Distance(
                                ecq.geom::geography,
                                ST_SetSRID(ST_MakePoint(:bp_lon_{_bpi}, :bp_lat_{_bpi}), 4326)::geography
                            ) AS distance_m
                     FROM {_EA_COMPETITOR_TABLE} ecq
                     WHERE ecq.geom IS NOT NULL
                       AND ecq.canonical_brand_id IS NULL
                       AND ST_DWithin(
                           ecq.geom::geography,
                           ST_SetSRID(ST_MakePoint(:bp_lon_{_bpi}, :bp_lat_{_bpi}), 4326)::geography,
                           500
                       )
                 ) raw
                 WHERE raw.norm_name_key <> ''
                   AND raw.norm_name_key NOT IN {_bp_denylist_sql}
                   AND NOT EXISTS (
                       SELECT 1 FROM brand_alias ba
                       WHERE ba.alias_key = raw.norm_name_key
                   )
                 GROUP BY candidate_pid, norm_name_key)
            """)

        if _bp_union_parts:
            _bp_sql = " UNION ALL ".join(_bp_union_parts)
            try:
                with db.begin_nested():
                    _bp_rows = db.execute(text(_bp_sql), _bp_params).mappings().all()
            except Exception as exc:
                logger.warning(
                    "expansion_search bulk brand_presence query failed (continuing): %s",
                    exc,
                )
                _bp_rows = []

            # Group rows by candidate, sort, take top 5. Canonical entries
            # sort first (so memo top_chains is biased toward known chains),
            # then by branch_count DESC, nearest_distance_m ASC, key ASC.
            _per_candidate_brands: dict[str, list[dict]] = {}
            for _r in _bp_rows:
                _key = str(_r["candidate_pid"])
                _per_candidate_brands.setdefault(_key, []).append({
                    "canonical_brand_id": _r["canonical_brand_id"],
                    "norm_name_key": _r.get("norm_name_key"),
                    "display_name_en": _r.get("display_name_en"),
                    "display_name_ar": _r.get("display_name_ar"),
                    "branch_count": int(_r["branch_count"] or 0),
                    "nearest_distance_m": _safe_float(_r.get("nearest_distance_m"), default=0.0),
                })

            for _key, _brands in _per_candidate_brands.items():
                _brands.sort(key=lambda b: (
                    b.get("canonical_brand_id") is None,  # False (canonical) sorts first
                    -b["branch_count"],
                    b.get("nearest_distance_m") or 0.0,
                    b.get("canonical_brand_id") or b.get("norm_name_key") or "",
                ))
                _bulk_brand_presence[_key] = _brands

            logger.info(
                "expansion_search bulk brand_presence: enriched=%d/%d search_id=%s",
                len(_bulk_brand_presence), len(_shortlist_coords), search_id,
            )

    # ───────────────────────────────────────────────────────────────────
    # Construction proximity (CEO directive — exclude areas with heavy
    # construction). For each shortlisted candidate count the number of
    # ``planet_osm_polygon`` rows tagged ``landuse='construction'`` OR
    # ``building='construction'`` within ``buffer_m`` meters of the
    # candidate point, and capture the nearest distance. Result is
    # written to ``feature_snapshot_json["construction_proximity"]`` and
    # consumed by the gate in ``_apply_market_viability_pass``.
    #
    # SRID note: ``planet_osm_polygon.way`` is SRID 3857 (Web Mercator)
    # — every geography cast must be preceded by ST_Transform(way, 4326).
    # Mirrors the brand-presence Shape A pattern above.
    # ───────────────────────────────────────────────────────────────────
    _construction_buffer_m = float(
        getattr(settings, "EXPANSION_VIABILITY_CONSTRUCTION_BUFFER_M", 0) or 0
    )
    _bulk_construction_proximity: dict[str, dict[str, Any]] = {}
    if (
        _construction_buffer_m > 0
        and _shortlist_coords
        and _cached_table_available(db, "planet_osm_polygon")
    ):
        _cp_union_parts: list[str] = []
        _cp_params: dict = {"buffer_m": _construction_buffer_m}
        for _cpi, (_cp_pid, (_cp_lon, _cp_lat)) in enumerate(_shortlist_coords.items()):
            _cp_params[f"cp_pid_{_cpi}"] = str(_cp_pid)
            _cp_params[f"cp_lon_{_cpi}"] = _cp_lon
            _cp_params[f"cp_lat_{_cpi}"] = _cp_lat
            _cp_union_parts.append(f"""
                (SELECT :cp_pid_{_cpi} AS candidate_pid,
                        COUNT(*) AS polygon_count,
                        COALESCE(MIN(
                            ST_Distance(
                                ST_Transform(p.way, 4326)::geography,
                                ST_SetSRID(ST_MakePoint(:cp_lon_{_cpi}, :cp_lat_{_cpi}), 4326)::geography
                            )
                        ), -1) AS nearest_distance_m
                 FROM planet_osm_polygon p
                 WHERE (p.landuse = 'construction' OR p.building = 'construction')
                   AND ST_DWithin(
                       ST_Transform(p.way, 4326)::geography,
                       ST_SetSRID(ST_MakePoint(:cp_lon_{_cpi}, :cp_lat_{_cpi}), 4326)::geography,
                       :buffer_m
                   ))
            """)

        if _cp_union_parts:
            _cp_sql = " UNION ALL ".join(_cp_union_parts)
            try:
                with db.begin_nested():
                    _cp_rows = db.execute(text(_cp_sql), _cp_params).mappings().all()
                for _r in _cp_rows:
                    _key = str(_r["candidate_pid"])
                    _nd_raw = _r.get("nearest_distance_m")
                    try:
                        _nd_val = float(_nd_raw) if _nd_raw is not None else -1.0
                    except (TypeError, ValueError):
                        _nd_val = -1.0
                    _bulk_construction_proximity[_key] = {
                        "polygon_count": int(_r.get("polygon_count") or 0),
                        "nearest_distance_m": _nd_val if _nd_val >= 0 else None,
                    }
                logger.info(
                    "expansion_search bulk construction_proximity: enriched=%d/%d search_id=%s",
                    len(_bulk_construction_proximity), len(_shortlist_coords), search_id,
                )
            except Exception as exc:
                logger.warning(
                    "expansion_search bulk construction_proximity query failed (continuing): %s",
                    exc,
                )
                _bulk_construction_proximity = {}

    t_bulk_enrich_done = time.monotonic()

    for prepared_item in prepared[:shortlist_size]:
      try:
        row = prepared_item["row"]
        _pid_str = str(row.get("parcel_id") or "")
        area_m2 = prepared_item["area_m2"]
        population_reach = prepared_item["population_reach"]
        # None when the catchment had no population-grid coverage; the
        # scoring `population_reach` above is the 0.0-coalesced counterpart.
        population_reach_measured = prepared_item.get("population_reach_measured")
        competitor_count = prepared_item["competitor_count"]
        competitor_count_confident = prepared_item.get("competitor_count_confident")
        max_chain_strength = prepared_item.get("max_chain_strength")
        chain_strength_share = prepared_item.get("chain_strength_share")
        chain_strength_score = prepared_item.get("chain_strength_score")
        if chain_strength_score is None:
            # Leg input is the strong-chain SHARE (None → neutral 50.0).
            chain_strength_score = _chain_strength_score(chain_strength_share)
        delivery_listing_count = prepared_item["delivery_listing_count"]
        provider_listing_count = prepared_item["provider_listing_count"]
        provider_platform_count = prepared_item["provider_platform_count"]
        landuse_label = prepared_item["landuse_label"]
        landuse_code = prepared_item["landuse_code"]
        # District resolution fallback chain (mirror of first construction
        # loop). Re-resolving from row here keeps the two sites symmetric
        # and guarantees the final candidate dict never leaks a raw
        # aqar_id or em-dash into the district slot.
        district = (
            row.get("district")
            or row.get("district_en")
            or row.get("unit_neighborhood_raw")
            or "District unknown"
        )
        # Final-pass district_norm. The first scoring pass assigns
        # district_norm at line ~5757 but that binding leaks across
        # iterations of the per-row loop and does NOT track this
        # shortlist iteration's candidate. Recompute locally.
        district_norm_final = normalize_district_key(district) if district else None
        demand_score = prepared_item["demand_score"]

        # ── L1 demand-generator index (PR-1 emit + PR-2 dine-in scoring) ──
        # Compute the composite ONCE here, when the index flag is on, so the
        # single in-memory result feeds BOTH the snapshot transparency emit
        # (below) and — when the PR-2 scoring flag is on for dine_in — the
        # demand blend. Numerator-only; no re-read from the JSON snapshot.
        _dg_index_result: dict[str, Any] | None = None
        if settings.EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED:
            _dg_fnb = _bulk_fnb_density.get(_pid_str, {})
            _dg_index_result = _demand_generator_index(
                population_reach=population_reach,
                population_local_reach=_bulk_dg_pop_local.get(_pid_str),
                osm_counts=_bulk_osm_generators.get(_pid_str, {}),
                building_floors_proxy_sum=_bulk_building_floors.get(_pid_str, 0.0),
                fnb_review_weighted=_dg_fnb.get("review_weighted", 0.0),
                fnb_venue_count=_dg_fnb.get("venue_count", 0),
                # Change-1: emit the model's true enrich radius (dine_in 3500 —
                # unchanged) so the snapshot radius_m matches the counts above.
                radius_m=int(_demand_generator_radius_m(service_model)),
                pop_radius_m=settings.EXPANSION_DEMAND_GENERATOR_POP_RADIUS_M,
                # Change-2: select l1_v3 anchors for qsr, l1_v2 for everything else.
                service_model=service_model,
            )

        # ── PR-2: gated swap of pop_score → dg_composite in the DINE-IN demand
        # blend. Default OFF, dine-in only. Keeps the blend weights and delivery
        # term unchanged: demand_score = _clamp(dg_composite·_pop_w +
        # delivery_score·_del_w). Falls back SILENTLY to the first-pass
        # pop_score blend when the scoring flag is off, the service model is not
        # dine_in, the index flag is off (logged once), or the composite is
        # missing — a missing composite never zeroes out demand.
        _demand_score_source = "pop_score"
        if (
            settings.EXPANSION_DEMAND_GENERATOR_SCORING_ENABLED
            and service_model == "dine_in"
        ):
            if not settings.EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED:
                _warn_dg_scoring_without_index()
            else:
                _dg_composite = (
                    _dg_index_result.get("composite_0_100")
                    if _dg_index_result
                    else None
                )
                if _dg_composite is not None:
                    _pop_w, _del_w = _demand_blend_weights(service_model)
                    demand_score = _clamp(
                        float(_dg_composite) * _pop_w
                        + prepared_item["delivery_score"] * _del_w
                    )
                    _demand_score_source = "dg_index"
        # ── Change-3: gated swap of pop_score → dg_composite in the QSR demand
        # blend, mirroring the dine-in swap above but behind a SEPARATE flag
        # (EXPANSION_DEMAND_GENERATOR_SCORING_QSR_ENABLED) so QSR deploys inert and
        # is flipped to validate independently of the already-live dine-in flag.
        # QSR uses l1_v3 anchors (selected in the composite above) and the same
        # blend shape: demand_score = _clamp(dg_composite·_pop_w +
        # delivery_score·_del_w), (_pop_w,_del_w)=_demand_blend_weights("qsr")=0.60/0.40.
        # Reuses the in-memory composite; falls back SILENTLY to pop_score when the
        # composite is missing or the index flag is off (logged once).
        elif (
            settings.EXPANSION_DEMAND_GENERATOR_SCORING_QSR_ENABLED
            and service_model == "qsr"
        ):
            if not settings.EXPANSION_DEMAND_GENERATOR_INDEX_ENABLED:
                _warn_dg_scoring_qsr_without_index()
            else:
                _dg_composite = (
                    _dg_index_result.get("composite_0_100")
                    if _dg_index_result
                    else None
                )
                if _dg_composite is not None:
                    _pop_w, _del_w = _demand_blend_weights(service_model)
                    demand_score = _clamp(
                        float(_dg_composite) * _pop_w
                        + prepared_item["delivery_score"] * _del_w
                    )
                    _demand_score_source = "dg_index"

        # Café foot-traffic amenity bonus (applied in second pass
        # after bulk enrichment has populated _bulk_foot_traffic).
        if service_model == "cafe" and _pid_str in _bulk_foot_traffic:
            _ft_count = _bulk_foot_traffic[_pid_str]
            _ft_bonus = (_foot_traffic_score(_ft_count) - 30.0) / 60.0 * 12.0
            demand_score = _clamp(demand_score + _ft_bonus)

        whitespace_score = prepared_item["whitespace_score"]
        fit_score = prepared_item["fit_score"]
        area_fit = float(prepared_item.get("area_fit") or 0.0)
        zoning_fit_score = prepared_item["zoning_fit_score"]
        provider_density_score = prepared_item["provider_density_score"]
        provider_whitespace_score = prepared_item["provider_whitespace_score"]
        multi_platform_presence_score = prepared_item["multi_platform_presence_score"]
        delivery_competition_score = prepared_item["delivery_competition_score"]
        confidence_score = prepared_item["confidence_score"]
        distance_to_nearest_branch_m = prepared_item["distance_to_nearest_branch_m"]
        cannibalization_score = prepared_item["cannibalization_score"]
        estimated_rent_sar_m2_year = prepared_item["estimated_rent_sar_m2_year"]
        rent_source = prepared_item["rent_source"]
        rent_fallback_used = rent_source == "conservative_default"
        estimated_annual_rent_sar = prepared_item["estimated_annual_rent_sar"]
        estimated_fitout_cost_sar = prepared_item["estimated_fitout_cost_sar"]
        estimated_revenue_index = prepared_item["estimated_revenue_index"]
        economics_score = prepared_item["economics_score"]
        provider_intelligence_composite = prepared_item["provider_intelligence_composite"]

        # ── Recompute revenue index and rent with road context ──
        # Road enrichment (_bulk_roads) is only available after shortlisting,
        # so the first scoring pass runs without it. Recompute here for
        # final scores using the road signal.
        _road_ctx = _bulk_roads.get(_pid_str)
        estimated_revenue_index, _revenue_index_detail = _estimate_revenue_index(
            area_m2=area_m2,
            target_area_m2=target_area_m2,
            unit_street_width_m=_safe_float(row.get("unit_street_width_m")) if row.get("unit_street_width_m") else None,
            unit_listing_type=row.get("unit_listing_type"),
            demand_score=demand_score,
            whitespace_score=whitespace_score,
            category=category,
            price_tier=effective_brand_profile.get("price_tier"),
            return_detail=True,
        )
        if rent_source != "commercial_unit_actual":
            _base_rent_sar_m2_year = prepared_item.get("rent_base_sar_m2_year", estimated_rent_sar_m2_year)
            _district_norm_2 = normalize_district_key(district) if district else None
            _rent_multiplier, _rent_micro_meta = _rent_micro_location_multiplier(
                provider_listing_count=provider_listing_count,
                delivery_competition_count=prepared_item.get("delivery_competition_count", 0),
                population_reach=population_reach,
                competitor_count=competitor_count,
                district_delivery_stats=_district_delivery_stats.get(_district_norm_2) if _district_norm_2 else None,
                city_benchmarks=_city_delivery_benchmarks,
                road_context=_road_ctx,
            )
            estimated_rent_sar_m2_year = round(_base_rent_sar_m2_year * _rent_multiplier, 2)
            if abs(_rent_multiplier - 1.0) > 0.01:
                rent_source = f"{rent_source}+micro"
            estimated_annual_rent_sar = round(area_m2 * estimated_rent_sar_m2_year)
        _is_listing = bool(row.get("commercial_unit_id"))
        economics_score, economics_meta = _economics_score(
            estimated_revenue_index=estimated_revenue_index,
            estimated_annual_rent_sar=estimated_annual_rent_sar,
            estimated_fitout_cost_sar=estimated_fitout_cost_sar,
            area_m2=area_m2,
            cannibalization_score=cannibalization_score,
            fit_score=fit_score,
            db=db,
            is_listing=_is_listing,
            district=district,
            listing_type=row.get("unit_listing_type"),
            unit_neighborhood_raw=row.get("unit_neighborhood_raw"),
            price_tier=effective_brand_profile.get("price_tier"),
            cand_age_days=_created_basis_age_days(row),
            revenue_index_detail=_revenue_index_detail,
        )
        effective_age_days, effective_age_source = _effective_listing_age_days(row)
        feature_snapshot_json = _candidate_feature_snapshot(
            db,
            parcel_id=_pid_str,
            lat=_safe_float(row.get("lat")),
            lon=_safe_float(row.get("lon")),
            area_m2=area_m2,
            district=district,
            landuse_label=landuse_label,
            landuse_code=landuse_code,
            provider_listing_count=provider_listing_count,
            provider_platform_count=provider_platform_count,
            competitor_count=competitor_count,
            nearest_branch_distance_m=distance_to_nearest_branch_m,
            rent_source=rent_source,
            estimated_rent_sar_m2_year=estimated_rent_sar_m2_year,
            economics_score=economics_score,
            roads_table_available=roads_table_available,
            parking_table_available=parking_table_available,
            ea_roads_available=ea_roads_populated,
            ea_parking_available=ea_parking_populated,
            bulk_perimeter=_bulk_perimeter.get(_pid_str),
            bulk_roads=_bulk_roads.get(_pid_str),
            bulk_parking=_bulk_parking.get(_pid_str),
        )
        # ── L1 demand-generator index emit (PR-1) + scoring transparency (PR-2) ──
        # Reuse the composite computed ONCE at the top of this iteration (do not
        # recompute / re-query). When the index flag is off it is None and no
        # key is emitted, so feature_snapshot_json and rankings are byte-for-byte
        # unchanged. The demand_score_source field records which numerator fed
        # the demand blend; it is emitted only when the PR-2 scoring flag is on
        # (off → snapshot unchanged) and is never read by scoring.
        if _dg_index_result is not None:
            feature_snapshot_json["demand_generator_index"] = _dg_index_result
        # Emit the transparency field when the flag governing THIS candidate's
        # service model is on: the dine-in flag (existing behaviour for all models)
        # or the QSR flag for qsr searches. Both-flags-off → no key (snapshot
        # unchanged); QSR flag off → no qsr key from the QSR path.
        if settings.EXPANSION_DEMAND_GENERATOR_SCORING_ENABLED or (
            settings.EXPANSION_DEMAND_GENERATOR_SCORING_QSR_ENABLED
            and service_model == "qsr"
        ):
            feature_snapshot_json["demand_score_source"] = _demand_score_source
            # PR-D rider: persist the demand-blend transparency block alongside
            # demand_score_source so the frontend Demand Strength card can show
            # the delivery leg and the weights actually used. Display-only —
            # never read by scoring. listing_realized_split reads the LIVE
            # setting (repo default 0.5; production overrides via env), never a
            # hardcoded constant.
            _blend_pop_w, _blend_del_w = _demand_blend_weights(service_model)
            feature_snapshot_json["demand_blend"] = {
                "pop_or_index_weight": _blend_pop_w,
                "delivery_weight": _blend_del_w,
                "delivery_score": round(
                    _safe_float(prepared_item["delivery_score"]), 2
                ),
                "listing_realized_split": float(
                    settings.EXPANSION_REALIZED_DEMAND_BLEND
                ),
            }
        # Compute the two raw ages independently so the pill logic on the
        # frontend and in _top_positives_and_risks can decide "New" vs
        # "Updated" without relying on which timestamp won the GREATEST()
        # tie-break inside _effective_listing_age_days. The scraper's daily
        # cadence makes aqar_updated_at shadow aqar_created_at in ~93% of
        # rows, which otherwise makes the "New" pill unreachable.
        _now_for_age = datetime.utcnow()
        _future_cutoff_for_age = _now_for_age + timedelta(days=1)

        def _raw_age_days(value: Any) -> int | None:
            if value is None:
                return None
            try:
                if getattr(value, "tzinfo", None) is not None:
                    value = value.replace(tzinfo=None)
                if value > _future_cutoff_for_age:
                    return None
                delta_days = (_now_for_age - value).days
            except (TypeError, ValueError):
                return None
            return max(0, delta_days)

        _created_days = _raw_age_days(row.get("unit_aqar_created_at"))
        _updated_days = _raw_age_days(row.get("unit_aqar_updated_at"))

        # Relative listing-age percentile (parallel created-basis signal — does
        # NOT alter effective_age_days/created_days/updated_days above). Only
        # populated when rent burden ran in percentile mode off the >= min-N
        # comparable set; otherwise null (never emit a percentile off a set
        # smaller than the rent percentile's min-N gate).
        _rent_burden_meta = economics_meta.get("rent_burden") if isinstance(economics_meta, dict) else None
        if isinstance(_rent_burden_meta, dict) and _rent_burden_meta.get("mode") == "percentile":
            _age_percentile = _rent_burden_meta.get("age_percentile")
            _n_comparables = _rent_burden_meta.get("n_comparables")
        else:
            _age_percentile = None
            _n_comparables = None

        feature_snapshot_json["listing_age"] = {
            "effective_age_days": effective_age_days,
            "source": effective_age_source,
            "created_days": _created_days,
            "updated_days": _updated_days,
            "age_percentile": _age_percentile,
            "n_comparables": _n_comparables,
        }
        # Phase 3b — district momentum snapshot. Districts below the
        # sample floor, blank-district candidates, and any normalization
        # miss all resolve to the neutral fallback shape so the downstream
        # contract is consistent (dict is always present, keys are stable).
        _momentum_entry_final = (
            _district_momentum.get(district_norm_final) if district_norm_final else None
        )
        if _momentum_entry_final is not None:
            feature_snapshot_json["district_momentum"] = dict(_momentum_entry_final)
        else:
            feature_snapshot_json["district_momentum"] = {
                "momentum_score": 50.0,
                "activity_30d": 0,
                "active_in_district": 0,
                "percentile_raw": 0.5,
                "percentile_absolute": 0.5,
                "percentile_composite": 0.5,
                "district_label": district if isinstance(district, str) else None,
                "sample_floor_applied": True,
            }
        # Black Marble VNP46A3 radiance growth (third leg of market viability).
        # Key produced via norm_district mirrors the ingest path (see
        # app/ingest/black_marble_radiance.py). Width mismatch note for the
        # underlying join: expansion_candidate.parcel_id is varchar(128) and
        # commercial_unit.aqar_id is varchar(64); the lookup is keyed by
        # district string here, not id, so widths don't apply at this site.
        if _radiance_lookup and isinstance(district, str) and district:
            _district_key_for_radiance = norm_district("riyadh", district)
            _radiance_signal = _radiance_lookup.get(_district_key_for_radiance)
            if _radiance_signal is not None:
                feature_snapshot_json["radiance_growth"] = dict(_radiance_signal)
        # Brand presence in the candidate's 500m micro-market (PR C).
        # Top 5 by branch count, with unique brand count and total branch
        # count summarized for the Breakdown tab header.
        _bp_brands_for_candidate = _bulk_brand_presence.get(_pid_str, [])
        # top_chain_strength_name: brand whose chain_strength_score was the
        # MAX in the competition catchment (surfaced from
        # _bulk_enrich_competitors). Distinct from top_chains[0], which is
        # ordered by branch_count. None when no same-category POI carried a
        # non-NULL chain_strength_score.
        _top_chain_strength_name = row.get("top_chain_strength_name")
        if _bp_brands_for_candidate:
            _bp_canonical_count = sum(
                1 for b in _bp_brands_for_candidate
                if b.get("canonical_brand_id") is not None
            )
            feature_snapshot_json["brand_presence"] = {
                "radius_m": 500,
                # unique_brands stays the field commercial_floor_pass reads;
                # post Patch 02 it is the union of canonical and name-deduped
                # non-canonical brands. unique_brands_canonical preserves the
                # pre-patch number for diagnostic comparison.
                "unique_brands": len(_bp_brands_for_candidate),
                "unique_brands_canonical": _bp_canonical_count,
                "unique_brands_total": len(_bp_brands_for_candidate),
                "total_branches": sum(b["branch_count"] for b in _bp_brands_for_candidate),
                "top_chains": _bp_brands_for_candidate[:5],
                "top_chain_strength_name": _top_chain_strength_name,
            }
        else:
            feature_snapshot_json["brand_presence"] = {
                "radius_m": 500,
                "unique_brands": 0,
                "unique_brands_canonical": 0,
                "unique_brands_total": 0,
                "total_branches": 0,
                "top_chains": [],
                "top_chain_strength_name": _top_chain_strength_name,
            }
        # Construction proximity (CEO directive — exclude areas with heavy
        # construction). Every persisted candidate must have this key set,
        # mirroring the brand_presence pattern above. When the bulk query
        # was skipped (buffer disabled, table missing) the candidate falls
        # through to the zero-block branch and the gate later passes it on
        # the "field absent / zero polygons" semantics.
        _cp_data = _bulk_construction_proximity.get(_pid_str)
        if _cp_data is not None:
            feature_snapshot_json["construction_proximity"] = {
                "buffer_m": int(_construction_buffer_m),
                "polygon_count": int(_cp_data["polygon_count"]),
                "nearest_distance_m": (
                    float(_cp_data["nearest_distance_m"])
                    if _cp_data["nearest_distance_m"] is not None
                    else None
                ),
            }
        else:
            feature_snapshot_json["construction_proximity"] = {
                "buffer_m": int(_construction_buffer_m),
                "polygon_count": 0,
                "nearest_distance_m": None,
            }
        # Enrich feature snapshot with candidate_location metadata
        if row.get("source_tier") is not None:
            feature_snapshot_json["candidate_location"] = {
                "source_tier": row.get("source_tier"),
                "source_type": row.get("source_type"),
                "is_vacant": row.get("is_vacant"),
                "current_tenant": row.get("current_tenant"),
                "current_category": row.get("current_category"),
                "rent_confidence": row.get("rent_confidence"),
                "cl_rent_m2_month": row.get("cl_rent_m2_month"),
                "cl_platform_count": row.get("cl_platform_count"),
                "cl_avg_rating": row.get("cl_avg_rating"),
                "profitability_score": row.get("profitability_score"),
            }
        road_context_available = bool((feature_snapshot_json.get("context_sources") or {}).get("road_context_available"))
        # Listing street width is direct ground truth — treat it as road
        # context so the frontage/access gate evaluates instead of returning
        # None (unknown).
        _unit_sw = _safe_float(row.get("unit_street_width_m")) if row.get("unit_street_width_m") else None
        if _unit_sw and _unit_sw > 0:
            road_context_available = True
        parking_context_available = bool((feature_snapshot_json.get("context_sources") or {}).get("parking_context_available"))
        # Add Expansion Advisor data provenance to context_sources
        cs = feature_snapshot_json.setdefault("context_sources", {})
        _dd_used = (
            not _delivery_observed
            and district_norm
            and district_norm in _district_delivery_stats
            and _district_delivery_stats[district_norm].get("total", 0) >= 5
        )
        cs["delivery_source"] = (
            "district_fallback" if _dd_used
            else "expansion_delivery_market" if ea_delivery_populated
            else "delivery_source_record"
        )
        cs["competitor_source"] = "expansion_competitor_quality" if ea_competitor_populated else "restaurant_poi"
        cs["delivery_observed"] = provider_listing_count > 0
        # Realized-demand evidence (rating_count velocity over the trailing
        # window).  Only populated when the feature flag is on and the
        # history table has ≥3 contributing branches in the catchment;
        # otherwise the field is explicitly None so the UI can distinguish
        # "not computed" from "zero demand observed".
        #
        # Look up per-parcel from _bulk_delivery here — the outer-scope
        # `_realized_demand_30d` / `_realized_demand_branches` are written
        # in the first scoring pass (the `for row in rows:` loop above)
        # and retain whatever value the final iteration produced, which
        # would broadcast a single catchment's figure to every candidate
        # surfaced in this shortlist loop.
        _rd_stats_this = _bulk_delivery.get(_pid_str) or {}
        _rd_value_this = _rd_stats_this.get("realized_demand_30d")
        _rd_branches_this = int(_rd_stats_this.get("realized_demand_branches") or 0)
        if _rd_value_this is not None and _rd_branches_this >= 3:
            feature_snapshot_json["realized_demand_30d"] = float(_rd_value_this)
            feature_snapshot_json["realized_demand_branches"] = _rd_branches_this
            feature_snapshot_json["realized_demand_window_days"] = int(
                settings.EXPANSION_REALIZED_DEMAND_WINDOW_DAYS
            )
            cs["realized_demand_source"] = "expansion_delivery_rating_history"
        else:
            cs["realized_demand_source"] = (
                "history_unavailable"
                if not settings.EXPANSION_REALIZED_DEMAND_ENABLED
                else "insufficient_history"
            )
        cs["rent_micro_adjustment"] = prepared_item.get("rent_micro_meta")
        cs["rent_base_sar_m2_year"] = prepared_item.get("rent_base_sar_m2_year")
        _unit_street_width = _safe_float(row.get("unit_street_width_m")) if row.get("unit_street_width_m") else None
        frontage_score = _frontage_score(
            unit_street_width_m=_unit_street_width,
            parcel_perimeter_m=_safe_float(feature_snapshot_json.get("parcel_perimeter_m")),
            touches_road=bool(feature_snapshot_json.get("touches_road")),
            nearby_road_count=_nonnegative_int(feature_snapshot_json.get("nearby_road_segment_count")),
            nearest_major_road_m=_safe_float(feature_snapshot_json.get("nearest_major_road_distance_m")),
            road_context_available=road_context_available,
        )
        access_score = _access_score(
            unit_street_width_m=_unit_street_width,
            touches_road=bool(feature_snapshot_json.get("touches_road")),
            nearest_major_road_m=_safe_float(feature_snapshot_json.get("nearest_major_road_distance_m")),
            nearby_road_count=_nonnegative_int(feature_snapshot_json.get("nearby_road_segment_count")),
            road_context_available=road_context_available,
        )
        parking_score = _parking_score(
            area_m2=area_m2,
            service_model=service_model,
            nearby_parking_count=_nonnegative_int(feature_snapshot_json.get("nearby_parking_amenity_count")),
            access_score=access_score,
            parking_context_available=parking_context_available,
        )
        # Listing-aware override of evidence bands. Mirrors the listing-aware
        # short-circuits already used by _frontage_score / _access_score: when
        # we have direct ground truth from the listing, the OSM-derived band
        # computed in the parcel-context snapshot pass is the wrong source.
        if _is_listing:
            _ctx_sources = feature_snapshot_json.setdefault("context_sources", {})

            _road_override = _road_evidence_band_from_street_width(_unit_street_width)
            if _road_override is not None:
                _ctx_sources["road_evidence_band"] = _road_override

            _parking_override = _parking_evidence_band_for_listing(
                parking_context_available=bool(_ctx_sources.get("parking_context_available")),
                nearby_parking_amenity_count=feature_snapshot_json.get("nearby_parking_amenity_count"),
                parking_score=parking_score,
            )
            if _parking_override is not None:
                _ctx_sources["parking_evidence_band"] = _parking_override
        access_visibility_score = _access_visibility_score(
            frontage_score=frontage_score,
            access_score=access_score,
            brand_profile=effective_brand_profile,
        )
        brand_fit_score = _brand_fit_score(
            district=district,
            area_m2=area_m2,
            target_area_m2=target_area_m2,
            demand_score=demand_score,
            fit_score=fit_score,
            cannibalization_score=cannibalization_score,
            provider_density_score=provider_density_score,
            provider_whitespace_score=provider_whitespace_score,
            multi_platform_presence_score=multi_platform_presence_score,
            delivery_competition_score=delivery_competition_score,
            visibility_signal=access_visibility_score,
            parking_signal=parking_score,
            brand_profile=effective_brand_profile,
            service_model=service_model,
        )
        _final_momentum_score_val = (
            _momentum_entry_final["momentum_score"] if _momentum_entry_final else None
        )
        listing_quality = _listing_quality_score(
            is_listing=_is_listing,
            effective_age_days=effective_age_days,
            is_furnished=row.get("unit_is_furnished"),
            unit_restaurant_score=_safe_float(row.get("unit_restaurant_score")) if row.get("unit_restaurant_score") is not None else None,
            has_image=bool(row.get("image_url")),
            has_drive_thru=row.get("unit_has_drive_thru"),
            llm_suitability_score=row.get("unit_llm_suitability_score"),
            llm_listing_quality_score=row.get("unit_llm_listing_quality_score"),
            district_momentum_score=_final_momentum_score_val,
        )
        score_breakdown_json = _score_breakdown(
            demand_score=demand_score,
            whitespace_score=whitespace_score,
            brand_fit_score=brand_fit_score,
            economics_score=economics_score,
            provider_intelligence_composite=provider_intelligence_composite,
            access_visibility_score=access_visibility_score,
            confidence_score=confidence_score,
            listing_quality_score=listing_quality,
            landlord_signal_score=row.get("unit_llm_landlord_signal_score"),
            chain_strength_score=chain_strength_score,
            chain_strength_max=max_chain_strength,
            brand_profile=effective_brand_profile,
            service_model=service_model,
            district_momentum_score=_final_momentum_score_val,
        )
        score_breakdown_json["inputs"]["rent_fallback_used"] = rent_fallback_used
        # F4: surface the whitespace confidence flag so the API response
        # meta exposes whether the boost-to-100-on-zero path was gated.
        score_breakdown_json["inputs"]["competition_whitespace_confident"] = (
            competitor_count_confident
        )
        score_breakdown_json["inputs"]["parking_context_available"] = bool(feature_snapshot_json["context_sources"].get("parking_context_available"))
        score_breakdown_json["inputs"]["road_context_available"] = bool(feature_snapshot_json["context_sources"].get("road_context_available"))
        score_breakdown_json["inputs"]["parking_evidence_band"] = feature_snapshot_json["context_sources"].get("parking_evidence_band")
        score_breakdown_json["inputs"]["road_evidence_band"] = feature_snapshot_json["context_sources"].get("road_evidence_band")
        # Surface percentile rent burden context in the breakdown.
        if isinstance(score_breakdown_json, dict):
            score_breakdown_json.setdefault("economics_detail", {}).update(economics_meta)
        final_score = _safe_float(score_breakdown_json.get("final_score"))
        (
            key_strengths_json,
            key_risks_json,
            key_strengths_structured,
            key_risks_structured,
        ) = _build_strengths_and_risks(
            demand_score=demand_score,
            whitespace_score=whitespace_score,
            fit_score=fit_score,
            cannibalization_score=cannibalization_score,
            rent_source=rent_source,
        )
        zoning_hint = _zoning_verdict(landuse_label, landuse_code)
        zoning_class = _zoning_signal_class(landuse_label, landuse_code)
        zoning_source = _zoning_signal_source(landuse_label, landuse_code)
        gate_status_json, gate_reasons_json = _candidate_gate_status(
            fit_score=fit_score,
            area_fit_score=area_fit,
            area_m2=area_m2,
            min_area_m2=min_area_m2,
            max_area_m2=max_area_m2,
            zoning_fit_score=zoning_fit_score,
            landuse_available=bool(landuse_label or landuse_code),
            frontage_score=frontage_score,
            access_score=access_score,
            parking_score=parking_score,
            district=district,
            distance_to_nearest_branch_m=distance_to_nearest_branch_m,
            provider_density_score=provider_density_score,
            multi_platform_presence_score=multi_platform_presence_score,
            economics_score=economics_score,
            brand_profile=effective_brand_profile,
            road_context_available=road_context_available,
            parking_context_available=parking_context_available,
            is_listing=_is_listing,
            unit_street_width_m=_unit_street_width,
            zoning_verdict_hint=zoning_hint,
            radiance_growth=feature_snapshot_json.get("radiance_growth"),
        )
        confidence_grade = _confidence_grade(
            confidence_score=confidence_score,
            district=district,
            provider_platform_count=provider_platform_count,
            multi_platform_presence_score=multi_platform_presence_score,
            rent_source=rent_source,
            road_context_available=road_context_available,
            parking_context_available=parking_context_available,
            zoning_available=bool(landuse_label or landuse_code),
            delivery_observed=provider_listing_count > 0,
            data_completeness_score=feature_snapshot_json.get("data_completeness_score", 0),
            is_listing=_is_listing,
            road_evidence_band=feature_snapshot_json.get("context_sources", {}).get("road_evidence_band"),
            parking_evidence_band=feature_snapshot_json.get("context_sources", {}).get("parking_evidence_band"),
        )
        demand_thesis, demand_thesis_structured = _build_demand_thesis(
            demand_score=demand_score,
            population_reach=population_reach,
            provider_density_score=provider_density_score,
            provider_whitespace_score=provider_whitespace_score,
            delivery_competition_score=delivery_competition_score,
            delivery_observed=provider_listing_count > 0,
        )
        cost_thesis, cost_thesis_structured = _build_cost_thesis(
            estimated_rent_sar_m2_year=estimated_rent_sar_m2_year,
            estimated_annual_rent_sar=estimated_annual_rent_sar,
            estimated_fitout_cost_sar=estimated_fitout_cost_sar,
        )
        if _pid_str and _pid_str in _bulk_competitors:
            comparable_competitors_json = _bulk_competitors[_pid_str]
        else:
            comparable_competitors_json = _comparable_competitors(
                db,
                category=category,
                lat=_safe_float(row.get("lat")),
                lon=_safe_float(row.get("lon")),
                ea_competitor_populated=ea_competitor_populated,
            )
        explanation = _build_explanation(
            area_m2=area_m2,
            population_reach=population_reach,
            competitor_count=competitor_count,
            delivery_listing_count=delivery_listing_count,
            landuse_label=landuse_label,
            landuse_code=landuse_code,
            cannibalization_score=cannibalization_score,
            distance_to_nearest_branch_m=distance_to_nearest_branch_m,
            economics_score=economics_score,
            estimated_rent_sar_m2_year=estimated_rent_sar_m2_year,
            estimated_annual_rent_sar=estimated_annual_rent_sar,
            estimated_fitout_cost_sar=estimated_fitout_cost_sar,
            estimated_revenue_index=estimated_revenue_index,
            rent_source=rent_source,
            final_score=final_score,
        )
        seed_candidate = {
            "demand_score": demand_score,
            "whitespace_score": whitespace_score,
            "brand_fit_score": brand_fit_score,
            "economics_score": economics_score,
            "delivery_competition_score": delivery_competition_score,
            "cannibalization_score": cannibalization_score,
            "gate_status_json": gate_status_json,
            "provider_density_score": provider_density_score,
            "multi_platform_presence_score": multi_platform_presence_score,
            "area_m2": area_m2,
            "min_area_m2": min_area_m2,
            "max_area_m2": max_area_m2,
            "distance_to_nearest_branch_m": distance_to_nearest_branch_m,
            "competitor_count": competitor_count,
            "provider_whitespace_score": provider_whitespace_score,
        }
        (
            top_positives_json,
            top_risks_json,
            top_positives_structured,
            top_risks_structured,
        ) = _top_positives_and_risks(candidate=seed_candidate, gate_reasons=gate_reasons_json)
        district_canon = _canonicalize_district_label(district, district_lookup)
        decision_summary, decision_summary_structured = _decision_summary(
            district=district_canon["district_display"] or district,
            final_score=final_score,
            economics_score=economics_score,
            key_risks=key_risks_json,
            service_model=service_model,
            area_m2=area_m2,
            key_risks_structured=key_risks_structured,
        )

        # ── Advisory-grade snapshot plumbing (PR #1) ──
        # Copy candidate-level scalars and a few derivations into
        # feature_snapshot_json so the memo LLM can make grounded
        # financial / market / risk arguments. Skip None values to
        # keep the snapshot lean; the memo path tolerates absent keys.
        if area_m2 is not None:
            feature_snapshot_json["area_m2"] = area_m2
        _unit_area_sqm_val = (
            _safe_float(row.get("unit_area_sqm"))
            if row.get("unit_area_sqm") is not None
            else None
        )
        if _unit_area_sqm_val is not None:
            feature_snapshot_json["unit_area_sqm"] = _unit_area_sqm_val
        if _unit_street_width is not None:
            feature_snapshot_json["unit_street_width_m"] = _unit_street_width
        if estimated_annual_rent_sar is not None:
            feature_snapshot_json["estimated_annual_rent_sar"] = round(
                estimated_annual_rent_sar, 2
            )
        # Replicate display_annual_rent_sar rounding from
        # _normalize_candidate_payload — that helper runs after persistence
        # so the persisted snapshot would otherwise miss this key.
        if (
            estimated_rent_sar_m2_year is not None
            and estimated_rent_sar_m2_year > 0
            and area_m2 is not None
            and area_m2 > 0
        ):
            feature_snapshot_json["display_annual_rent_sar"] = round(
                round(estimated_rent_sar_m2_year) * area_m2, 2
            )
        elif estimated_annual_rent_sar is not None:
            feature_snapshot_json["display_annual_rent_sar"] = round(
                estimated_annual_rent_sar, 2
            )
        # Persist the MEASURED value (null when no population-grid coverage)
        # so the viability floor/soft-leg can tell "unmeasured" from
        # "measured zero". Scoring readers (_population_score, demand blend,
        # dg-index, _safe_float reads) keep coalescing null → 0.0, so
        # ranking is byte-identical for covered candidates.
        feature_snapshot_json["population_reach"] = population_reach_measured
        if delivery_listing_count is not None:
            feature_snapshot_json["delivery_listing_count"] = delivery_listing_count
        if access_visibility_score is not None:
            feature_snapshot_json["access_visibility_score"] = round(
                access_visibility_score, 2
            )
        if parking_score is not None:
            feature_snapshot_json["parking_score"] = round(parking_score, 2)
        if frontage_score is not None:
            feature_snapshot_json["frontage_score"] = round(frontage_score, 2)
        _district_display_val = district_canon.get("district_display")
        if _district_display_val is not None:
            feature_snapshot_json["district_display"] = _district_display_val

        # landlord_signal — pull from score_breakdown_json["inputs"] so
        # the memo LLM sees the same scalar the rerank LLM consumes.
        _landlord_signal_val = (
            (score_breakdown_json or {}).get("inputs", {}).get("landlord_signal")
        )
        if _landlord_signal_val is not None:
            feature_snapshot_json["landlord_signal"] = _landlord_signal_val

        # Comparable rent context — derive only when rent_burden ran in
        # percentile mode. Named comparable_* (not district_*) because
        # the percentile fallback chain reaches a city-band scope on
        # ~42% of rows in production; comparable_source_label carries
        # the actual scope so downstream callers can stay honest.
        _rent_burden_meta = (
            (score_breakdown_json or {})
            .get("economics_detail", {})
            .get("rent_burden", {})
        )
        if isinstance(_rent_burden_meta, dict) and _rent_burden_meta.get("mode") == "percentile":
            _median_per_m2_month = _rent_burden_meta.get("median_monthly_rent_per_m2")
            if _median_per_m2_month is not None and area_m2 is not None:
                feature_snapshot_json["comparable_median_annual_rent_sar"] = round(
                    float(_median_per_m2_month) * 12.0 * float(area_m2), 2
                )
            _n_comparable = _rent_burden_meta.get("n_comparable")
            if _n_comparable is not None:
                feature_snapshot_json["comparable_n"] = int(_n_comparable)
            _comparable_source_label = _rent_burden_meta.get("source_label")
            if _comparable_source_label is not None:
                feature_snapshot_json["comparable_source_label"] = str(
                    _comparable_source_label
                )

        # ── Score Contributions diagnostics surface ──
        # Per-input candidate values that the score functions read but the
        # snapshot did not previously persist. Purely additive — no score
        # formula change. The frontend Decision Memo Diagnostics tab reads
        # these to show per-component candidate inputs.
        feature_snapshot_json["is_listing"] = bool(_is_listing)
        _area_confidence_val = row.get("area_confidence")
        feature_snapshot_json["area_confidence"] = (
            str(_area_confidence_val) if _area_confidence_val is not None else None
        )
        # listing_quality_signals: only meaningful for listing-backed
        # candidates. For parcel candidates, emit an empty dict (consumers
        # treat missing keys as "not applicable"); do not raise when the
        # unit_* columns are absent.
        if _is_listing:
            def _opt_bool(value: Any) -> bool | None:
                return None if value is None else bool(value)

            feature_snapshot_json["listing_quality_signals"] = {
                "llm_suitability_score": _safe_float(row.get("unit_llm_suitability_score"))
                    if row.get("unit_llm_suitability_score") is not None
                    else None,
                "llm_listing_quality_score": _safe_float(
                    row.get("unit_llm_listing_quality_score")
                ) if row.get("unit_llm_listing_quality_score") is not None else None,
                "is_furnished": _opt_bool(row.get("unit_is_furnished")),
                "has_drive_thru": _opt_bool(row.get("unit_has_drive_thru")),
                "unit_restaurant_score": _safe_float(row.get("unit_restaurant_score"))
                    if row.get("unit_restaurant_score") is not None
                    else None,
            }
        else:
            feature_snapshot_json["listing_quality_signals"] = {}

        candidates.append(
            {
                "id": str(uuid.uuid4()),
                "search_id": search_id,
                "parcel_id": str(row["parcel_id"]),
                "lat": _safe_float(row.get("lat")),
                "lon": _safe_float(row.get("lon")),
                "area_m2": area_m2,
                "district": district,
                "district_key": district_canon["district_key"],
                "district_name_ar": district_canon["district_name_ar"],
                "district_name_en": district_canon["district_name_en"],
                "district_display": district_canon["district_display"],
                "landuse_label": landuse_label,
                "landuse_code": landuse_code,
                "population_reach": population_reach,
                "competitor_count": competitor_count,
                "delivery_listing_count": delivery_listing_count,
                "demand_score": round(demand_score, 2),
                "whitespace_score": round(whitespace_score, 2),
                "fit_score": round(fit_score, 2),
                "zoning_fit_score": round(zoning_fit_score, 2),
                "frontage_score": round(frontage_score, 2),
                "access_score": round(access_score, 2),
                "parking_score": round(parking_score, 2),
                "access_visibility_score": round(access_visibility_score, 2),
                "confidence_score": round(confidence_score, 2),
                "cannibalization_score": round(cannibalization_score, 2),
                "distance_to_nearest_branch_m": round(distance_to_nearest_branch_m, 2)
                if distance_to_nearest_branch_m is not None
                else None,
                "estimated_rent_sar_m2_year": round(estimated_rent_sar_m2_year, 2),
                "estimated_annual_rent_sar": round(estimated_annual_rent_sar, 2),
                "estimated_fitout_cost_sar": round(estimated_fitout_cost_sar, 2),
                "estimated_revenue_index": round(estimated_revenue_index, 2),
                "economics_score": round(economics_score, 2),
                "brand_fit_score": round(brand_fit_score, 2),
                "provider_density_score": round(provider_density_score, 2),
                "provider_whitespace_score": round(provider_whitespace_score, 2),
                "multi_platform_presence_score": round(multi_platform_presence_score, 2),
                "delivery_competition_score": round(delivery_competition_score, 2),
                "gate_status_json": gate_status_json,
                "gate_reasons_json": gate_reasons_json,
                "feature_snapshot_json": feature_snapshot_json,
                "score_breakdown_json": score_breakdown_json,
                "confidence_grade": confidence_grade,
                "demand_thesis": demand_thesis,
                "cost_thesis": cost_thesis,
                "top_positives_json": top_positives_json,
                "top_risks_json": top_risks_json,
                # PR #2a: parallel structured records (write-only this PR).
                "top_positives_structured_json": top_positives_structured,
                "top_risks_structured_json": top_risks_structured,
                "demand_thesis_structured_json": demand_thesis_structured,
                "cost_thesis_structured_json": cost_thesis_structured,
                "decision_summary_structured_json": decision_summary_structured,
                "comparable_competitors_json": comparable_competitors_json,
                "decision_summary": decision_summary,
                "key_risks_json": key_risks_json,
                "key_strengths_json": key_strengths_json,
                # PR #3: parallel structured records for the Arabic read path.
                "key_strengths_structured_json": key_strengths_structured,
                "key_risks_structured_json": key_risks_structured,
                "final_score": round(final_score, 2),
                "explanation": explanation,
                "zoning_signal_source": zoning_source,
                "zoning_signal_class": zoning_class,
                "zoning_verification_needed": zoning_hint != "pass",
                "site_fit_context": {
                    "road_context_available": road_context_available,
                    "parking_context_available": parking_context_available,
                    "frontage_score_mode": "observed" if road_context_available else "estimated",
                    "access_score_mode": "observed" if road_context_available else "estimated",
                    "parking_score_mode": "observed" if parking_context_available else "estimated",
                },
                # ── Commercial unit metadata ──
                "source_type": (
                    "commercial_unit" if row.get("commercial_unit_id")
                    else {"2": "delivery_poi", "3": "arcgis_parcel"}.get(
                        str(row.get("source_tier", "")), "parcel"
                    )
                ),
                # Platform discriminator surfaced to the frontend. Reads the
                # raw ``row`` column (``aqar``/``bayut``) before the
                # ``source_type`` key above collapses it to ``commercial_unit``.
                "platform": (
                    row.get("source_type")
                    if row.get("commercial_unit_id") and row.get("source_type") in ("aqar", "bayut")
                    else None
                ),
                # Prefix-stripped id for display only; identity keys
                # (parcel_id / commercial_unit_id / source_id) keep their shape.
                "display_id": _strip_platform_prefix(
                    row.get("source_id") or row.get("commercial_unit_id")
                ),
                "source_tier": row.get("source_tier"),
                "is_vacant": row.get("is_vacant"),
                "current_tenant": row.get("current_tenant"),
                "current_category": row.get("current_category"),
                "rent_confidence": row.get("rent_confidence"),
                "commercial_unit_id": row.get("commercial_unit_id"),
                "listing_url": row.get("listing_url"),
                "image_url": row.get("image_url"),
                "unit_price_sar_annual": _safe_float(row.get("unit_price_sar_annual")) if row.get("unit_price_sar_annual") is not None else None,
                "unit_area_sqm": _safe_float(row.get("unit_area_sqm")) if row.get("unit_area_sqm") is not None else None,
                "unit_street_width_m": _safe_float(row.get("unit_street_width_m")) if row.get("unit_street_width_m") is not None else None,
                "unit_neighborhood": row.get("district"),
                "unit_listing_type": row.get("unit_listing_type"),
                # LLM signals kept on the candidate for post-sort tiebreak.
                "unit_llm_suitability_score": row.get("unit_llm_suitability_score"),
                "unit_llm_landlord_signal_score": row.get("unit_llm_landlord_signal_score"),
                "unit_llm_listing_quality_score": row.get("unit_llm_listing_quality_score"),
            }
        )
      except Exception:
        logger.warning(
            "Expansion search: skipping shortlist candidate parcel_id=%s due to enrichment error: search_id=%s",
            prepared_item.get("row", {}).get("parcel_id"), search_id,
            exc_info=True,
        )

    _ZONING_CLASS_RANK = {
        "commercial": 0,
        "mixed_use": 0,
        "unknown": 1,
        "public_service": 1,
        "industrial": 1,
        "residential": 2,
    }

    def _rank_sort_key(item: dict[str, Any]) -> tuple:
        """Deterministic ranking with rich tie-breakers (change #6).

        Priority (descending preference):
        1. Higher final_score
        2. Better gate verdict: pass > unknown > fail
        3. Zoning class priority: commercial/mixed first, then neutral, then residential
        4. Smaller area distance to target
        5. Higher economics_score
        6. Lower cannibalization_score
        7. Stable parcel_id as ultimate tie-breaker
        """
        overall = (item.get("gate_status_json") or {}).get("overall_pass")
        gate_rank = {True: 0, None: 1, False: 2}.get(overall, 2)
        zoning_class = item.get("zoning_signal_class", "unknown")
        zoning_rank = _ZONING_CLASS_RANK.get(zoning_class, 1)
        area_dist = abs(item.get("area_m2", 0) - target_area_m2)
        return (
            -item.get("final_score", 0),
            gate_rank,
            zoning_rank,
            area_dist,
            -item.get("economics_score", 0),
            item.get("cannibalization_score", 100),
            str(item.get("parcel_id", "")),
        )

    t_enrich_done = time.monotonic()

    candidates.sort(key=_rank_sort_key)
    # Dedupe near-clone candidates before limiting
    candidates = _dedupe_candidates(candidates)
    # Score-aware dedup: collapse candidates that look identical to users
    _pre_score_dedup = len(candidates)
    candidates = _dedupe_score_clones(candidates, max_results=max(limit * 3, len(candidates)))
    if len(candidates) < _pre_score_dedup:
        logger.info(
            "expansion_search score-dedup: search_id=%s before=%d after=%d",
            search_id, _pre_score_dedup, len(candidates),
        )

    # ── Score-delta refactor ──
    # The viability pass applies hard-floor drops AND attaches per-leg
    # decisions (viability_legs_fired, viability_delta) on each survivor,
    # without any positional reorder. We then fold the value_band, viability,
    # freshness, and momentum deltas into final_score and re-sort strictly
    # by (final_score DESC, parcel_id ASC). The LLM rerank pass that follows
    # is a no-op in production (EXPANSION_LLM_RERANK_ENABLED=False).
    #
    # Pipeline-order fix (2026-06): both passes run on the FULL deduped pool,
    # BEFORE any truncation or district selection, so that (a) hard-floor
    # drops are backfilled from the rest of the pool instead of under-filling
    # the response, (b) the viability percentile cohorts carry the same
    # statistical meaning for multi-district and city-wide searches, and
    # (c) district selection operates on final post-delta scores and cannot
    # be voided by a later re-sort.
    viability_diagnostics: dict[str, Any] = {}
    candidates = _apply_market_viability_pass(
        candidates,
        search_id=search_id,
        diagnostics=viability_diagnostics,
    )

    candidates = _apply_score_deltas_and_sort(candidates)

    # ── Final selection ──
    # City-wide / single-district: plain top-``limit`` slice, identical to
    # the pre-balancing behavior. Multi-district: per-district quota over
    # the score-sorted hard-floor survivors. The representation guarantee
    # is best-effort-within-limit and applies to floor SURVIVORS only — a
    # district whose every candidate fails a hard floor is legitimately
    # unrepresented.
    candidates = _select_final_candidates(candidates, target_districts, limit)

    # Phase 2: bounded LLM shortlist reranking. Annotates every candidate
    # with rerank metadata (deterministic_rank, final_rank, rerank_applied,
    # rerank_reason, rerank_delta, rerank_status) and, when
    # EXPANSION_LLM_RERANK_ENABLED is True, reorders the top
    # min(len(candidates), EXPANSION_LLM_RERANK_SHORTLIST_SIZE) within
    # ±EXPANSION_LLM_RERANK_MAX_MOVE ranks of their deterministic position.
    # With the flag off (the default) this is a no-op for ordering: every
    # candidate keeps final_rank == deterministic_rank and the list is
    # unchanged.
    candidates = _apply_rerank_to_candidates(candidates, effective_brand_profile)

    for index, candidate in enumerate(candidates, start=1):
        candidate["compare_rank"] = index
        candidate["rank_position"] = index

    # ── Display score ──
    # Show the real final_score directly. Cap at 99 (avoid showing 100 since
    # nothing in the system is ever a perfect candidate) and floor at 1
    # (operators interpret 0 as "broken" rather than "very low scoring").
    # The previous redistribution forced a 50-95 spread regardless of
    # underlying spread, which actively hid the differentiation produced
    # by the new scoring architecture.
    for _c in candidates:
        raw = _safe_float(_c.get("final_score"), 0.0)
        _c["display_score"] = round(max(1.0, min(99.0, raw)), 1)

    # Store display_score inside score_breakdown_json for frontend access
    for _c in candidates:
        if isinstance(_c.get("score_breakdown_json"), dict):
            _c["score_breakdown_json"]["display_score"] = _c["display_score"]

    insert_sql = text(
        """
        INSERT INTO expansion_candidate (
            id,
            search_id,
            parcel_id,
            lat,
            lon,
            area_m2,
            district,
            landuse_label,
            landuse_code,
            population_reach,
            competitor_count,
            delivery_listing_count,
            demand_score,
            whitespace_score,
            fit_score,
            confidence_score,
            zoning_fit_score,
            frontage_score,
            access_score,
            parking_score,
            access_visibility_score,
            cannibalization_score,
            distance_to_nearest_branch_m,
            final_score,
            estimated_rent_sar_m2_year,
            estimated_annual_rent_sar,
            estimated_fitout_cost_sar,
            estimated_revenue_index,
            economics_score,
            brand_fit_score,
            provider_density_score,
            provider_whitespace_score,
            multi_platform_presence_score,
            delivery_competition_score,
            gate_status_json,
            gate_reasons_json,
            feature_snapshot_json,
            score_breakdown_json,
            confidence_grade,
            demand_thesis,
            cost_thesis,
            top_positives_json,
            top_risks_json,
            top_positives_structured_json,
            top_risks_structured_json,
            demand_thesis_structured_json,
            cost_thesis_structured_json,
            decision_summary_structured_json,
            comparable_competitors_json,
            decision_summary,
            key_risks_json,
            key_strengths_json,
            key_strengths_structured_json,
            key_risks_structured_json,
            compare_rank,
            rank_position,
            explanation,
            source_type,
            commercial_unit_id,
            listing_url,
            image_url,
            unit_price_sar_annual,
            unit_area_sqm,
            unit_street_width_m,
            unit_neighborhood,
            unit_listing_type,
            deterministic_rank,
            final_rank,
            rerank_applied,
            rerank_reason,
            rerank_delta,
            rerank_status
        ) VALUES (
            :id,
            :search_id,
            :parcel_id,
            :lat,
            :lon,
            :area_m2,
            :district,
            :landuse_label,
            :landuse_code,
            :population_reach,
            :competitor_count,
            :delivery_listing_count,
            :demand_score,
            :whitespace_score,
            :fit_score,
            :confidence_score,
            :zoning_fit_score,
            :frontage_score,
            :access_score,
            :parking_score,
            :access_visibility_score,
            :cannibalization_score,
            :distance_to_nearest_branch_m,
            :final_score,
            :estimated_rent_sar_m2_year,
            :estimated_annual_rent_sar,
            :estimated_fitout_cost_sar,
            :estimated_revenue_index,
            :economics_score,
            :brand_fit_score,
            :provider_density_score,
            :provider_whitespace_score,
            :multi_platform_presence_score,
            :delivery_competition_score,
            CAST(:gate_status_json AS jsonb),
            CAST(:gate_reasons_json AS jsonb),
            CAST(:feature_snapshot_json AS jsonb),
            CAST(:score_breakdown_json AS jsonb),
            :confidence_grade,
            :demand_thesis,
            :cost_thesis,
            CAST(:top_positives_json AS jsonb),
            CAST(:top_risks_json AS jsonb),
            CAST(:top_positives_structured_json AS jsonb),
            CAST(:top_risks_structured_json AS jsonb),
            CAST(:demand_thesis_structured_json AS jsonb),
            CAST(:cost_thesis_structured_json AS jsonb),
            CAST(:decision_summary_structured_json AS jsonb),
            CAST(:comparable_competitors_json AS jsonb),
            :decision_summary,
            CAST(:key_risks_json AS jsonb),
            CAST(:key_strengths_json AS jsonb),
            CAST(:key_strengths_structured_json AS jsonb),
            CAST(:key_risks_structured_json AS jsonb),
            :compare_rank,
            :rank_position,
            CAST(:explanation AS jsonb),
            :source_type,
            :commercial_unit_id,
            :listing_url,
            :image_url,
            :unit_price_sar_annual,
            :unit_area_sqm,
            :unit_street_width_m,
            :unit_neighborhood,
            :unit_listing_type,
            :deterministic_rank,
            :final_rank,
            :rerank_applied,
            CAST(:rerank_reason AS jsonb),
            :rerank_delta,
            :rerank_status
        )
        """
    )

    def _candidate_insert_params(candidate: dict[str, Any]) -> dict[str, Any]:
        safe_candidate = _sanitize_for_json(candidate)
        return {
            **safe_candidate,
            "explanation": json.dumps(_sanitize_for_json(candidate["explanation"]), ensure_ascii=False),
            "key_risks_json": json.dumps(_sanitize_for_json(candidate["key_risks_json"]), ensure_ascii=False),
            "key_strengths_json": json.dumps(_sanitize_for_json(candidate["key_strengths_json"]), ensure_ascii=False),
            "key_strengths_structured_json": json.dumps(_sanitize_for_json(candidate["key_strengths_structured_json"]), ensure_ascii=False),
            "key_risks_structured_json": json.dumps(_sanitize_for_json(candidate["key_risks_structured_json"]), ensure_ascii=False),
            "gate_status_json": json.dumps(_sanitize_for_json(candidate["gate_status_json"]), ensure_ascii=False),
            "gate_reasons_json": json.dumps(_sanitize_for_json(candidate["gate_reasons_json"]), ensure_ascii=False),
            "feature_snapshot_json": json.dumps(_sanitize_for_json(candidate["feature_snapshot_json"]), ensure_ascii=False),
            "score_breakdown_json": json.dumps(_sanitize_for_json(candidate["score_breakdown_json"]), ensure_ascii=False),
            "top_positives_json": json.dumps(_sanitize_for_json(candidate["top_positives_json"]), ensure_ascii=False),
            "top_risks_json": json.dumps(_sanitize_for_json(candidate["top_risks_json"]), ensure_ascii=False),
            "top_positives_structured_json": json.dumps(_sanitize_for_json(candidate["top_positives_structured_json"]), ensure_ascii=False),
            "top_risks_structured_json": json.dumps(_sanitize_for_json(candidate["top_risks_structured_json"]), ensure_ascii=False),
            "demand_thesis_structured_json": json.dumps(_sanitize_for_json(candidate["demand_thesis_structured_json"]), ensure_ascii=False),
            "cost_thesis_structured_json": json.dumps(_sanitize_for_json(candidate["cost_thesis_structured_json"]), ensure_ascii=False),
            "decision_summary_structured_json": json.dumps(_sanitize_for_json(candidate["decision_summary_structured_json"]), ensure_ascii=False),
            "comparable_competitors_json": json.dumps(_sanitize_for_json(candidate["comparable_competitors_json"]), ensure_ascii=False),
            "rerank_reason": (
                json.dumps(_sanitize_for_json(candidate["rerank_reason"]), ensure_ascii=False)
                if candidate.get("rerank_reason") is not None
                else None
            ),
        }

    persisted_candidates: list[dict[str, Any]] = []
    for batch in _chunked(candidates, _EXPANSION_BULK_PERSIST_CHUNK_SIZE):
        batch_params = [_candidate_insert_params(candidate) for candidate in batch]
        try:
            with db.begin_nested():
                db.execute(insert_sql, batch_params)
            persisted_candidates.extend(batch)
        except Exception:
            logger.warning(
                "Bulk persist failed for expansion candidates search_id=%s batch_size=%d; falling back to row-wise inserts",
                search_id,
                len(batch),
                exc_info=True,
            )
            for candidate in batch:
                try:
                    with db.begin_nested():
                        db.execute(insert_sql, _candidate_insert_params(candidate))
                    persisted_candidates.append(candidate)
                except Exception:
                    logger.warning(
                        "Failed to persist expansion candidate id=%s search_id=%s parcel_id=%s – skipping",
                        candidate.get("id"),
                        search_id,
                        candidate.get("parcel_id"),
                        exc_info=True,
                    )

    result: list[dict[str, Any]] = []
    for candidate in persisted_candidates:
        try:
            result.append(_normalize_candidate_payload(candidate, district_lookup, lang=lang))
        except Exception:
            logger.warning(
                "Failed to normalize candidate id=%s search_id=%s – skipping",
                candidate.get("id"), search_id,
                exc_info=True,
            )

    # ── Surface districts with no matching parcels ──
    _districts_with_no_candidates: list[str] = []
    if target_district_norm:
        _districts_found = set()
        for _c in persisted_candidates:
            _cd = normalize_district_key(_c.get("district"))
            if _cd:
                _districts_found.add(_cd)
        _districts_missing_norm = [
            d for d in target_district_norm if d not in _districts_found
        ]
        if _districts_missing_norm:
            # Map back to original user-supplied display names
            _td_original = target_districts  # the raw list from the request
            for _mn in _districts_missing_norm:
                _matched = False
                for _orig in _td_original:
                    if normalize_district_key(_orig) == _mn:
                        _districts_with_no_candidates.append(_orig)
                        _matched = True
                        break
                if not _matched:
                    _districts_with_no_candidates.append(_mn)
            logger.warning(
                "expansion_search: districts with no candidates: "
                "search_id=%s missing=%s",
                search_id,
                _districts_with_no_candidates,
            )

    # ── Coverage metadata: update search notes with district stats ──
    search_notes: dict[str, Any] = {}
    try:
        districts_in_result = set()
        for c in persisted_candidates:
            d = c.get("district") or c.get("district_display")
            if d:
                districts_in_result.add(d)
        candidate_sources_observed = sorted({
            r.get("source_type") for r in rows
            if r.get("source_tier") == 1 and r.get("source_type")
        })
        coverage_meta = {
            "parcel_source": "listings_only",
            "candidate_sources_observed": candidate_sources_observed,
            "candidate_selection": "stratified" if use_stratified else "targeted",
            "per_district_cap": per_district_cap,
            "candidates_evaluated": len(rows),
            "candidates_scored": len(prepared),
            "candidates_persisted": len(persisted_candidates),
            "districts_represented": len(districts_in_result),
            "districts_list": sorted(districts_in_result),
            # Surface data gaps explicitly for frontend consumption
            "districts_with_no_candidates": _districts_with_no_candidates,
            "districts_with_no_candidates_count": len(_districts_with_no_candidates),
            "data_gap": len(_districts_with_no_candidates) > 0,
            "data_gap_message": (
                f"No commercial listings found in: "
                f"{', '.join(_districts_with_no_candidates)}. "
                "These districts may lack listing data in the current dataset. "
                "Try a broader area search or remove these districts."
            ) if _districts_with_no_candidates else None,
        }
        search_notes: dict[str, Any] = {"coverage": coverage_meta}
        # Surface the hard-floor pre-pass diagnostics — per-leg drop counts
        # and the thresholds in effect — so the API meta can explain why the
        # requested limit may not be saturated. Operators otherwise have no
        # signal beyond pool_size/rows_returned.
        if viability_diagnostics:
            search_notes["viability"] = viability_diagnostics
        db.execute(
            text(
                "UPDATE expansion_search "
                "SET notes = COALESCE(notes, '{}'::jsonb) || CAST(:notes_patch AS jsonb) "
                "WHERE id = :search_id"
            ),
            {
                "search_id": search_id,
                "notes_patch": json.dumps(search_notes, ensure_ascii=False),
            },
        )
    except Exception:
        logger.warning("expansion_search: failed to persist coverage metadata search_id=%s", search_id, exc_info=True)

    t_persist_done = time.monotonic()
    t_end = time.monotonic()
    logger.info(
        "expansion_search timing: total=%.2fs query=%.2fs coarse_score=%.2fs "
        "bulk_enrich=%.2fs enrichment=%.2fs persist=%.2fs normalize=%.2fs "
        "search_id=%s raw_rows=%d prepared=%d shortlisted=%d persisted=%d final=%d",
        t_end - t_start,
        t_query_done - t_start,
        t_coarse_done - t_query_done,
        t_bulk_enrich_done - t_coarse_done,
        t_enrich_done - t_bulk_enrich_done,
        t_persist_done - t_enrich_done,
        t_end - t_persist_done,
        search_id,
        len(rows),
        len(prepared),
        shortlist_size,
        len(persisted_candidates),
        len(result),
    )
    return {"items": result, "notes": search_notes}


def get_search(db: Session, search_id: str) -> dict[str, Any] | None:
    row = db.execute(
        text(
            """
            SELECT
                id,
                created_at,
                brand_name,
                category,
                service_model,
                target_districts,
                min_area_m2,
                max_area_m2,
                target_area_m2,
                bbox,
                request_json,
                notes,
                (
                    SELECT COALESCE(
                        json_agg(
                            json_build_object(
                                'id', eb.id,
                                'name', eb.name,
                                'lat', eb.lat,
                                'lon', eb.lon,
                                'district', eb.district,
                                'source', eb.source,
                                'created_at', eb.created_at
                            )
                            ORDER BY eb.created_at ASC
                        ),
                        '[]'::json
                    )
                    FROM expansion_branch eb
                    WHERE eb.search_id = expansion_search.id
                ) AS existing_branches
            FROM expansion_search
            WHERE id = :search_id
            """
        ),
        {"search_id": search_id},
    ).mappings().first()
    if not row:
        return None
    payload = dict(row)
    payload["brand_profile"] = get_brand_profile(db, search_id)
    return _normalize_search_payload(payload)


def get_candidates(db: Session, search_id: str, district_lookup: dict[str, dict[str, str]] | None = None, lang: str = "en") -> list[dict[str, Any]]:
    if district_lookup is None:
        district_lookup = _cached_district_lookup(db)
    rows = db.execute(
        text(
            """
            SELECT
                id,
                search_id,
                parcel_id,
                lat,
                lon,
                area_m2,
                district,
                landuse_label,
                landuse_code,
                population_reach,
                competitor_count,
                delivery_listing_count,
                demand_score,
                whitespace_score,
                fit_score,
                zoning_fit_score,
                frontage_score,
                access_score,
                parking_score,
                access_visibility_score,
                confidence_score,
                confidence_grade,
                gate_status_json,
                gate_reasons_json,
                feature_snapshot_json,
                score_breakdown_json,
                demand_thesis,
                cost_thesis,
                top_positives_json,
                top_risks_json,
                top_positives_structured_json,
                top_risks_structured_json,
                decision_summary_structured_json,
                demand_thesis_structured_json,
                cost_thesis_structured_json,
                comparable_competitors_json,
                cannibalization_score,
                distance_to_nearest_branch_m,
                estimated_rent_sar_m2_year,
                estimated_annual_rent_sar,
                estimated_fitout_cost_sar,
                estimated_revenue_index,
                economics_score,
                brand_fit_score,
                provider_density_score,
                provider_whitespace_score,
                multi_platform_presence_score,
                delivery_competition_score,
                decision_summary,
                key_risks_json,
                key_strengths_json,
                key_strengths_structured_json,
                key_risks_structured_json,
                final_score,
                compare_rank,
                rank_position,
                explanation,
                computed_at,
                source_type,
                commercial_unit_id,
                listing_url,
                image_url,
                unit_price_sar_annual,
                unit_area_sqm,
                unit_street_width_m,
                unit_neighborhood,
                unit_listing_type,
                deterministic_rank,
                final_rank,
                rerank_applied,
                rerank_reason,
                rerank_delta,
                rerank_status,
                (decision_memo_json IS NOT NULL OR decision_memo IS NOT NULL) AS decision_memo_present
            FROM expansion_candidate
            WHERE search_id = :search_id
              AND (
                source_type != 'commercial_unit'
                OR EXISTS (
                  SELECT 1 FROM commercial_unit cu
                  WHERE cu.aqar_id = expansion_candidate.parcel_id
                    AND cu.status = 'active'
                )
              )
            ORDER BY rank_position ASC NULLS LAST, compare_rank ASC NULLS LAST, final_score DESC, computed_at DESC
            """
        ),
        {"search_id": search_id},
    ).mappings().all()
    # NOTE: ``decision_memo`` (text) and ``decision_memo_json`` (multi-KB
    # structured object) are intentionally NOT included in the list
    # response — the frontend reads ``decision_memo_present`` to know
    # whether to enable the "View decision memo" affordance and fetches
    # the full memo via GET /candidates/{id}/memo on demand.
    return [_normalize_candidate_payload(dict(row), district_lookup, lang=lang) for row in rows]




def create_saved_search(
    db: Session,
    *,
    search_id: str,
    title: str,
    description: str | None,
    status: str,
    selected_candidate_ids: list[str] | None,
    filters_json: dict[str, Any] | None,
    ui_state_json: dict[str, Any] | None,
    lang: str = "en",
) -> dict[str, Any]:
    saved_id = str(uuid.uuid4())
    row = db.execute(
        text(
            """
            INSERT INTO expansion_saved_search (
                id,
                search_id,
                title,
                description,
                status,
                selected_candidate_ids,
                filters_json,
                ui_state_json
            ) VALUES (
                :id,
                :search_id,
                :title,
                :description,
                :status,
                CAST(:selected_candidate_ids AS jsonb),
                CAST(:filters_json AS jsonb),
                CAST(:ui_state_json AS jsonb)
            )
            RETURNING
                id,
                search_id,
                title,
                description,
                status,
                selected_candidate_ids,
                filters_json,
                ui_state_json,
                created_at,
                updated_at
            """
        ),
        {
            "id": saved_id,
            "search_id": search_id,
            "title": title,
            "description": description,
            "status": status,
            "selected_candidate_ids": json.dumps(selected_candidate_ids, ensure_ascii=False)
            if selected_candidate_ids is not None
            else None,
            "filters_json": json.dumps(filters_json, ensure_ascii=False) if filters_json is not None else None,
            "ui_state_json": json.dumps(ui_state_json, ensure_ascii=False) if ui_state_json is not None else None,
        },
    ).mappings().first()
    return _normalize_saved_search_payload(dict(row) if row else {}, lang=lang)


def list_saved_searches(
    db: Session,
    *,
    status: str | None,
    limit: int,
    lang: str = "en",
) -> list[dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT
                id,
                search_id,
                title,
                description,
                status,
                selected_candidate_ids,
                filters_json,
                ui_state_json,
                created_at,
                updated_at
            FROM expansion_saved_search
            WHERE (:status IS NULL OR status = :status)
            ORDER BY updated_at DESC
            LIMIT :limit
            """
        ),
        {"status": status, "limit": limit},
    ).mappings().all()
    return [_normalize_saved_search_payload(dict(row), lang=lang) for row in rows]


def get_saved_search(db: Session, saved_id: str, lang: str = "en") -> dict[str, Any] | None:
    row = db.execute(
        text(
            """
            SELECT
                id,
                search_id,
                title,
                description,
                status,
                selected_candidate_ids,
                filters_json,
                ui_state_json,
                created_at,
                updated_at
            FROM expansion_saved_search
            WHERE id = :saved_id
            """
        ),
        {"saved_id": saved_id},
    ).mappings().first()
    if not row:
        return None

    saved = dict(row)
    search = get_search(db, str(saved["search_id"]))
    candidates = get_candidates(db, str(saved["search_id"]), lang=lang)
    return _normalize_saved_search_payload(saved, search=search, candidates=candidates, lang=lang)


def update_saved_search(
    db: Session,
    saved_id: str,
    payload: dict[str, Any],
    lang: str = "en",
) -> dict[str, Any] | None:
    if not payload:
        row = db.execute(
            text(
                """
                SELECT
                    id,
                    search_id,
                    title,
                    description,
                    status,
                    selected_candidate_ids,
                    filters_json,
                    ui_state_json,
                    created_at,
                    updated_at
                FROM expansion_saved_search
                WHERE id = :saved_id
                """
            ),
            {"saved_id": saved_id},
        ).mappings().first()
        return _normalize_saved_search_payload(dict(row)) if row else None

    updates: list[str] = []
    params: dict[str, Any] = {"saved_id": saved_id}
    simple_fields = ["title", "description", "status"]
    for field in simple_fields:
        if field in payload:
            updates.append(f"{field} = :{field}")
            params[field] = payload[field]

    for field in ["selected_candidate_ids", "filters_json", "ui_state_json"]:
        if field in payload:
            updates.append(f"{field} = CAST(:{field} AS jsonb)")
            params[field] = json.dumps(payload[field], ensure_ascii=False) if payload[field] is not None else None

    updates.append("updated_at = now()")

    row = db.execute(
        text(
            f"""
            UPDATE expansion_saved_search
            SET {', '.join(updates)}
            WHERE id = :saved_id
            RETURNING
                id,
                search_id,
                title,
                description,
                status,
                selected_candidate_ids,
                filters_json,
                ui_state_json,
                created_at,
                updated_at
            """
        ),
        params,
    ).mappings().first()
    return _normalize_saved_search_payload(dict(row), lang=lang) if row else None


_COMPARE_SUMMARY_KEYS = [
    "best_overall_candidate_id",
    "lowest_cannibalization_candidate_id",
    "highest_demand_candidate_id",
    "best_fit_candidate_id",
    "best_economics_candidate_id",
    "best_brand_fit_candidate_id",
    "strongest_delivery_market_candidate_id",
    "strongest_whitespace_candidate_id",
    # lowest_rent_burden_candidate_id: smallest absolute annual rent across
    # the compared set. Intentionally distinct from best_value_candidate_id;
    # this field stays as-is to preserve the Compare panel's existing
    # "Lowest Rent Burden" tile semantics.
    "lowest_rent_burden_candidate_id",
    # best_value_candidate_id: highest derived value_score (geometric mean
    # of estimated_revenue_index and rent_burden_score). Independent peer
    # of lowest_rent_burden_candidate_id — both are populated.
    "best_value_candidate_id",
    "most_confident_candidate_id",
    "best_gate_pass_candidate_id",
]


def _empty_compare_summary() -> dict[str, Any]:
    return {key: None for key in _COMPARE_SUMMARY_KEYS}

def delete_saved_search(db: Session, saved_id: str) -> bool:
    row = db.execute(
        text("DELETE FROM expansion_saved_search WHERE id = :saved_id RETURNING id"),
        {"saved_id": saved_id},
    ).first()
    return bool(row)
def compare_candidates(db: Session, search_id: str, candidate_ids: list[str], lang: str = "en") -> dict[str, Any]:
    search = db.execute(text("SELECT id FROM expansion_search WHERE id = :search_id"), {"search_id": search_id}).first()
    if not search:
        raise ValueError("not_found")
    district_lookup = _build_district_lookup(db)

    rows = db.execute(
        text(
            """
            SELECT
                id,
                parcel_id,
                district,
                area_m2,
                final_score,
                demand_score,
                whitespace_score,
                fit_score,
                zoning_fit_score,
                frontage_score,
                access_score,
                parking_score,
                access_visibility_score,
                confidence_score,
                confidence_grade,
                gate_status_json,
                gate_reasons_json,
                feature_snapshot_json,
                score_breakdown_json,
                demand_thesis,
                cost_thesis,
                top_positives_json,
                top_risks_json,
                comparable_competitors_json,
                cannibalization_score,
                distance_to_nearest_branch_m,
                estimated_rent_sar_m2_year,
                estimated_annual_rent_sar,
                estimated_fitout_cost_sar,
                estimated_revenue_index,
                economics_score,
                brand_fit_score,
                provider_density_score,
                provider_whitespace_score,
                multi_platform_presence_score,
                delivery_competition_score,
                competitor_count,
                delivery_listing_count,
                population_reach,
                landuse_label,
                rank_position,
                source_type,
                commercial_unit_id,
                listing_url,
                image_url,
                unit_price_sar_annual,
                unit_area_sqm,
                unit_street_width_m,
                unit_neighborhood,
                unit_listing_type
            FROM expansion_candidate
            WHERE search_id = :search_id
              AND id = ANY(:candidate_ids)
              AND (
                source_type != 'commercial_unit'
                OR EXISTS (
                  SELECT 1 FROM commercial_unit cu
                  WHERE cu.aqar_id = expansion_candidate.parcel_id
                    AND cu.status = 'active'
                )
              )
            """
        ),
        {"search_id": search_id, "candidate_ids": candidate_ids},
    ).mappings().all()

    row_by_id = {str(row["id"]): dict(row) for row in rows}
    if len(row_by_id) != len(candidate_ids):
        raise ValueError("not_found")

    items: list[dict[str, Any]] = []
    for candidate_id in candidate_ids:
        row = row_by_id[candidate_id]
        pros: list[str] = []
        cons: list[str] = []
        if _safe_float(row.get("demand_score")) >= 70:
            pros.append("Strong demand score")
        if _safe_float(row.get("whitespace_score")) >= 65:
            pros.append("Good competitive whitespace")
        if _safe_float(row.get("fit_score")) >= 70:
            pros.append("High parcel-format fit")
        if _safe_float(row.get("cannibalization_score")) <= 35:
            pros.append("Low cannibalization risk")
        if _safe_float(row.get("cannibalization_score")) >= 70:
            cons.append("High cannibalization risk")
        if _safe_int(row.get("competitor_count")) >= 8:
            cons.append("Dense same-category competition")

        item = _normalize_candidate_payload({
            "candidate_id": row["id"],
            "parcel_id": row.get("parcel_id"),
            "district": row.get("district"),
            "area_m2": row.get("area_m2"),
            "final_score": row.get("final_score"),
            "demand_score": row.get("demand_score"),
            "whitespace_score": row.get("whitespace_score"),
            "fit_score": row.get("fit_score"),
            "zoning_fit_score": row.get("zoning_fit_score"),
            "frontage_score": row.get("frontage_score"),
            "access_score": row.get("access_score"),
            "parking_score": row.get("parking_score"),
            "access_visibility_score": row.get("access_visibility_score"),
            "confidence_score": row.get("confidence_score"),
            "confidence_grade": row.get("confidence_grade"),
            "gate_status_json": row.get("gate_status_json"),
            "gate_reasons_json": row.get("gate_reasons_json"),
            "feature_snapshot_json": row.get("feature_snapshot_json"),
            "score_breakdown_json": row.get("score_breakdown_json"),
            "demand_thesis": row.get("demand_thesis"),
            "cost_thesis": row.get("cost_thesis"),
            "top_positives_json": row.get("top_positives_json"),
            "top_risks_json": row.get("top_risks_json"),
            "comparable_competitors_json": row.get("comparable_competitors_json"),
            "cannibalization_score": row.get("cannibalization_score"),
            "distance_to_nearest_branch_m": row.get("distance_to_nearest_branch_m"),
            "estimated_rent_sar_m2_year": row.get("estimated_rent_sar_m2_year"),
            "estimated_annual_rent_sar": row.get("estimated_annual_rent_sar"),
            "estimated_fitout_cost_sar": row.get("estimated_fitout_cost_sar"),
            "estimated_revenue_index": row.get("estimated_revenue_index"),
            "economics_score": row.get("economics_score"),
            "brand_fit_score": row.get("brand_fit_score"),
            "provider_density_score": row.get("provider_density_score"),
            "provider_whitespace_score": row.get("provider_whitespace_score"),
            "multi_platform_presence_score": row.get("multi_platform_presence_score"),
            "delivery_competition_score": row.get("delivery_competition_score"),
            "competitor_count": row.get("competitor_count"),
            "delivery_listing_count": row.get("delivery_listing_count"),
            "population_reach": row.get("population_reach"),
            "landuse_label": row.get("landuse_label"),
            "rank_position": row.get("rank_position"),
            # value_score / value_band are extracted from
            # score_breakdown_json["economics_detail"] inside
            # _normalize_candidate_payload — the empty string lookups here
            # rely on that path. Keep the keys present so the Pydantic
            # model surfaces them on every item.
            # Same defensive coercion as get_candidate_memo: commercial_unit_id
            # is a string identifier per the API contract, but the DB column
            # may yield an int for numeric Aqar IDs. Keep both emission paths
            # symmetric so a future consumer declaring either field strictly
            # doesn't reintroduce the memo-endpoint 500.
            "source_type": (
                str(row.get("source_type"))
                if row.get("source_type") is not None
                else None
            ),
            "commercial_unit_id": (
                str(row.get("commercial_unit_id"))
                if row.get("commercial_unit_id") is not None
                else None
            ),
            "listing_url": row.get("listing_url"),
            "image_url": row.get("image_url"),
            "unit_price_sar_annual": row.get("unit_price_sar_annual"),
            "unit_area_sqm": row.get("unit_area_sqm"),
            "unit_street_width_m": row.get("unit_street_width_m"),
            "unit_neighborhood": row.get("unit_neighborhood"),
            "unit_listing_type": row.get("unit_listing_type"),
        }, district_lookup, lang=lang)
        item["pros"] = pros
        item["cons"] = cons
        items.append(item)

    summary = _empty_compare_summary()
    if items:
        best_overall = max(items, key=lambda item: _safe_float(item.get("final_score")))["candidate_id"]
        lowest_cannibalization = min(items, key=lambda item: _safe_float(item.get("cannibalization_score"), 9999.0))["candidate_id"]
        highest_demand = max(items, key=lambda item: _safe_float(item.get("demand_score")))["candidate_id"]
        best_fit = max(items, key=lambda item: _safe_float(item.get("fit_score")))["candidate_id"]
        best_economics = max(items, key=lambda item: _safe_float(item.get("economics_score")))["candidate_id"]
        best_brand_fit = max(items, key=lambda item: _safe_float(item.get("brand_fit_score")))["candidate_id"]
        strongest_delivery_market = max(items, key=lambda item: _safe_float(item.get("provider_density_score")) + _safe_float(item.get("multi_platform_presence_score")))["candidate_id"]
        strongest_whitespace = max(items, key=lambda item: _safe_float(item.get("provider_whitespace_score")))["candidate_id"]
        # "Lowest rent burden" = smallest absolute annual rent across the
        # compared set. NOT the value-score winner; that's
        # best_value_candidate_id below. Two independent fields; both
        # surface in the Compare panel as their own tiles.
        lowest_rent_burden = min(items, key=lambda item: _safe_float(item.get("estimated_annual_rent_sar"), 10**12))["candidate_id"]
        # best_value: highest published value_score. Items with no
        # value_score (absolute_legacy / fallback rows) sink to the
        # bottom; if no item has a value_score, the field is None.
        def _value_or_neg(item: dict[str, Any]) -> float:
            v = item.get("value_score")
            return float(v) if isinstance(v, (int, float)) else -1.0
        _bv = max(items, key=_value_or_neg)
        best_value = _bv["candidate_id"] if _value_or_neg(_bv) >= 0 else None
        grade_order = {"A": 4, "B": 3, "C": 2, "D": 1}
        most_confident = max(
            items,
            key=lambda item: (
                grade_order.get(str(item.get("confidence_grade") or "D"), 0),
                _safe_float(item.get("confidence_score")),
            ),
        )["candidate_id"]
        pass_items = [item for item in items if bool((item.get("gate_status_json") or {}).get("overall_pass"))]
        best_gate_pass = max(pass_items or items, key=lambda item: _safe_float(item.get("final_score")))["candidate_id"]

        summary.update({
            "best_overall_candidate_id": best_overall,
            "lowest_cannibalization_candidate_id": lowest_cannibalization,
            "highest_demand_candidate_id": highest_demand,
            "best_fit_candidate_id": best_fit,
            "best_economics_candidate_id": best_economics,
            "best_brand_fit_candidate_id": best_brand_fit,
            "strongest_delivery_market_candidate_id": strongest_delivery_market,
            "strongest_whitespace_candidate_id": strongest_whitespace,
            "lowest_rent_burden_candidate_id": lowest_rent_burden,
            "best_value_candidate_id": best_value,
            "most_confident_candidate_id": most_confident,
            "best_gate_pass_candidate_id": best_gate_pass,
        })

    return {"items": items, "summary": summary}


def _regenerate_candidate_memo_in_locale(
    db: Session,
    raw_candidate: dict[str, Any],
    brand_profile: dict[str, Any] | None,
    lang: str,
) -> tuple[str | None, dict[str, Any]] | None:
    """Regenerate and persist a structured decision memo in ``lang``.

    Returns ``(memo_text, memo_json)`` on success, or None when structured
    generation is unavailable (flag off, LLM error, renderer failure) — the
    caller then serves the existing persisted memo unchanged.

    PR #4a: invoked by :func:`get_candidate_memo` on a locale mismatch.
    """
    from app.services.llm_decision_memo import (
        MEMO_PROMPT_VERSION,
        build_memo_context,
        generate_structured_memo,
        render_structured_memo_as_text,
    )

    cid = raw_candidate.get("candidate_id") or raw_candidate.get("id")
    try:
        ctx = build_memo_context(
            candidate=raw_candidate,
            brief={"brand_profile": brand_profile or {}},
            lang=lang,
        )
        memo_json = generate_structured_memo(ctx)
        if memo_json is None:
            return None
        memo_text = render_structured_memo_as_text(memo_json, lang)
    except Exception as exc:
        logger.warning("decision memo regenerate failed for %s: %s", cid, exc)
        return None

    try:
        db.execute(
            text(
                "UPDATE expansion_candidate "
                "SET decision_memo = :txt, "
                "    decision_memo_json = CAST(:j AS JSONB), "
                "    decision_memo_prompt_version = :ver, "
                "    decision_memo_lang = :lang "
                "WHERE id = :cid"
            ),
            {
                "txt": memo_text,
                "j": json.dumps(memo_json, ensure_ascii=False),
                "ver": MEMO_PROMPT_VERSION,
                "lang": lang,
                "cid": cid,
            },
        )
        db.commit()
    except Exception as exc:
        logger.warning("decision memo regenerate persist failed for %s: %s", cid, exc)
        try:
            db.rollback()
        except Exception:
            pass

    return memo_text, memo_json


def get_candidate_memo(db: Session, candidate_id: str, lang: str = "en") -> dict[str, Any] | None:
    t_start = time.monotonic()
    row = db.execute(
        text(
            """
            SELECT
                c.id AS candidate_id,
                c.search_id,
                s.brand_name,
                s.category,
                s.service_model,
                c.parcel_id,
                c.district,
                c.area_m2,
                c.landuse_label,
                c.final_score,
                c.economics_score,
                c.brand_fit_score,
                c.provider_density_score,
                c.provider_whitespace_score,
                c.multi_platform_presence_score,
                c.delivery_competition_score,
                c.demand_score,
                c.whitespace_score,
                c.fit_score,
                c.zoning_fit_score,
                c.frontage_score,
                c.access_score,
                c.parking_score,
                c.access_visibility_score,
                c.confidence_score,
                c.confidence_grade,
                c.gate_status_json,
                c.gate_reasons_json,
                c.feature_snapshot_json,
                c.score_breakdown_json,
                c.demand_thesis,
                c.cost_thesis,
                c.top_positives_json,
                c.top_risks_json,
                c.top_positives_structured_json,
                c.top_risks_structured_json,
                c.decision_summary_structured_json,
                c.demand_thesis_structured_json,
                c.cost_thesis_structured_json,
                c.comparable_competitors_json,
                c.cannibalization_score,
                c.distance_to_nearest_branch_m,
                c.estimated_rent_sar_m2_year,
                c.estimated_annual_rent_sar,
                c.estimated_fitout_cost_sar,
                c.estimated_revenue_index,
                c.key_strengths_json,
                c.key_risks_json,
                c.key_strengths_structured_json,
                c.key_risks_structured_json,
                c.decision_summary,
                c.rank_position,
                c.source_type,
                c.commercial_unit_id,
                c.listing_url,
                c.image_url,
                c.unit_price_sar_annual,
                c.unit_area_sqm,
                c.unit_street_width_m,
                c.unit_neighborhood,
                c.unit_listing_type,
                c.deterministic_rank,
                c.final_rank,
                c.rerank_applied,
                c.rerank_reason,
                c.rerank_delta,
                c.rerank_status,
                c.decision_memo,
                c.decision_memo_json,
                c.decision_memo_lang
            FROM expansion_candidate c
            JOIN expansion_search s ON s.id = c.search_id
            WHERE c.id = :candidate_id
              AND (
                c.source_type != 'commercial_unit'
                OR EXISTS (
                  SELECT 1 FROM commercial_unit cu
                  WHERE cu.aqar_id = c.parcel_id
                    AND cu.status = 'active'
                )
              )
            """
        ),
        {"candidate_id": candidate_id},
    ).mappings().first()
    if not row:
        return None

    district_lookup = _cached_district_lookup(db)
    candidate = _normalize_candidate_payload(dict(row), district_lookup, lang=lang)
    brand_profile = get_brand_profile(db, str(candidate.get("search_id"))) or {}
    strengths = candidate.get("key_strengths_json") or []
    risks = candidate.get("key_risks_json") or []
    final_score = _safe_float(candidate.get("final_score"))
    economics_score = _safe_float(candidate.get("economics_score"))
    cannibalization_score = _safe_float(candidate.get("cannibalization_score"))

    if final_score >= 78 and economics_score >= 70 and cannibalization_score <= 55:
        verdict = "go"
    elif final_score >= 58 and economics_score >= 45 and cannibalization_score <= 75:
        verdict = "consider"
    else:
        verdict = "caution"

    best_use_case = _recommended_use_case(
        str(candidate.get("service_model") or "qsr"),
        _safe_float(candidate.get("area_m2")),
        lang=lang,
    )
    main_watchout = risks[0] if risks else "Validate lease and capex assumptions before commitment"
    district = candidate.get("district_display") or candidate.get("district") or "Riyadh"
    headline = f"{verdict.upper()}: {district} parcel shows {economics_score:.1f}/100 economics for {best_use_case}"
    logger.info(
        "expansion_memo timing: total=%.2fs candidate_id=%s search_id=%s verdict=%s",
        time.monotonic() - t_start, candidate_id,
        candidate.get("search_id"), verdict,
    )

    result = {
        "candidate_id": candidate["candidate_id"],
        "search_id": candidate["search_id"],
        "brand_profile": {
            "brand_name": candidate.get("brand_name"),
            "category": candidate.get("category"),
            "service_model": candidate.get("service_model"),
            **brand_profile,
        },
        "candidate": {
            "parcel_id": candidate.get("parcel_id"),
            "district": candidate.get("district"),
            "district_key": candidate.get("district_key"),
            "district_name_ar": candidate.get("district_name_ar"),
            "district_name_en": candidate.get("district_name_en"),
            "district_display": candidate.get("district_display"),
            "area_m2": candidate.get("area_m2"),
            "landuse_label": candidate.get("landuse_label"),
            "final_score": candidate.get("final_score"),
            "economics_score": candidate.get("economics_score"),
            "brand_fit_score": candidate.get("brand_fit_score"),
            "provider_density_score": candidate.get("provider_density_score"),
            "provider_whitespace_score": candidate.get("provider_whitespace_score"),
            "multi_platform_presence_score": candidate.get("multi_platform_presence_score"),
            "delivery_competition_score": candidate.get("delivery_competition_score"),
            "demand_score": candidate.get("demand_score"),
            "whitespace_score": candidate.get("whitespace_score"),
            "fit_score": candidate.get("fit_score"),
            "zoning_fit_score": candidate.get("zoning_fit_score"),
            "frontage_score": candidate.get("frontage_score"),
            "access_score": candidate.get("access_score"),
            "parking_score": candidate.get("parking_score"),
            "access_visibility_score": candidate.get("access_visibility_score"),
            "confidence_score": candidate.get("confidence_score"),
            "confidence_grade": candidate.get("confidence_grade") or "D",
            "gate_status": candidate.get("gate_status_json"),
            "gate_reasons": candidate.get("gate_reasons_json"),
            "feature_snapshot": candidate.get("feature_snapshot_json"),
            "score_breakdown_json": candidate.get("score_breakdown_json"),
            "demand_thesis": candidate.get("demand_thesis") or "",
            "cost_thesis": candidate.get("cost_thesis") or "",
            "top_positives_json": candidate.get("top_positives_json"),
            "top_risks_json": candidate.get("top_risks_json"),
            "comparable_competitors": candidate.get("comparable_competitors_json"),
            "cannibalization_score": candidate.get("cannibalization_score"),
            "distance_to_nearest_branch_m": candidate.get("distance_to_nearest_branch_m"),
            "estimated_rent_sar_m2_year": candidate.get("estimated_rent_sar_m2_year"),
            "estimated_annual_rent_sar": candidate.get("estimated_annual_rent_sar"),
            "estimated_fitout_cost_sar": candidate.get("estimated_fitout_cost_sar"),
            "estimated_revenue_index": candidate.get("estimated_revenue_index"),
            "key_strengths": strengths,
            "key_risks": risks,
            "decision_summary": candidate.get("decision_summary") or "",
            "rank_position": candidate.get("rank_position"),
            "site_fit_context": _derive_site_fit_context(candidate.get("feature_snapshot_json")),
            # Commercial-unit / listing fields. The list endpoint emits these
            # via `_normalize_candidate_payload`; the memo endpoint must
            # expose them on the same nested `candidate` shape so the memo
            # quick-facts row (Area, Street width) and any listing-card UI
            # render the same values shown in the candidate list.
            # commercial_unit_id is a string identifier in the API contract, but
            # the underlying DB column (Text) can hold numeric Aqar IDs that
            # SQLAlchemy surfaces as int. Coerce here so Pydantic's str | None
            # validator on CandidateMemoCandidateResponse does not 500 on real
            # rows. source_type gets the same treatment defensively for any
            # future ingestion path that writes non-string codes.
            "source_type": (
                str(candidate.get("source_type"))
                if candidate.get("source_type") is not None
                else None
            ),
            "commercial_unit_id": (
                str(candidate.get("commercial_unit_id"))
                if candidate.get("commercial_unit_id") is not None
                else None
            ),
            "listing_url": candidate.get("listing_url"),
            "image_url": candidate.get("image_url"),
            "unit_price_sar_annual": candidate.get("unit_price_sar_annual"),
            "unit_area_sqm": candidate.get("unit_area_sqm"),
            "unit_street_width_m": candidate.get("unit_street_width_m"),
            "display_annual_rent_sar": candidate.get("display_annual_rent_sar"),
            # Rerank metadata. Persisted on expansion_candidate so it survives
            # a page reload. With EXPANSION_LLM_RERANK_ENABLED=False (the
            # default) deterministic_rank == final_rank and rerank_status is
            # "flag_off". Lives on the nested candidate object — same shape
            # the list endpoint exposes — so DecisionLogicCard reads it from
            # `data.candidate.*` like every other candidate-scoped field.
            "deterministic_rank": candidate.get("deterministic_rank"),
            "final_rank": candidate.get("final_rank"),
            "rerank_applied": bool(candidate.get("rerank_applied")),
            "rerank_reason": candidate.get("rerank_reason"),
            "rerank_delta": _safe_int(candidate.get("rerank_delta"), 0),
            "rerank_status": candidate.get("rerank_status"),
        },
        "recommendation": {
            "headline": headline,
            "verdict": verdict,
            "best_use_case": best_use_case,
            "main_watchout": main_watchout,
            "gate_verdict": _gate_verdict_label((candidate.get("gate_status_json") or {}).get("overall_pass")),
        },
        "market_research": {},
        # decision_memo / decision_memo_json describe THIS memo (the envelope),
        # not a per-candidate property — they stay at the top level alongside
        # candidate_id, search_id, brand_profile, recommendation, market_research.
        # decision_memo_json is None until POST /decision-memo (or the pre-warm
        # background task on POST /searches) populates it.
        "decision_memo": candidate.get("decision_memo"),
        "decision_memo_json": candidate.get("decision_memo_json"),
    }

    # PR #4a (Q3 (b)): regenerate-on-mismatch. The persisted memo carries
    # the locale it was generated in (decision_memo_lang — read from the row
    # internally; intentionally NOT surfaced on the response since
    # CandidateMemoResponse forbids extra fields and the frontend contract
    # must not drift). When the requested ``lang`` does not match — treating
    # a NULL column on pre-PR-4a rows as English — the memo is regenerated
    # in the requested locale and persisted.
    # COST NOTE: this fires a synchronous model call from a GET handler.
    # Pre-warm hardcodes lang="en" (Q4 defer), so the first AR view of a
    # candidate not yet POSTed in Arabic pays this regeneration cost once.
    stored_lang = candidate.get("decision_memo_lang")
    has_memo = (
        candidate.get("decision_memo_json") is not None
        or candidate.get("decision_memo") is not None
    )
    if has_memo and lang != (stored_lang or "en"):
        regenerated = _regenerate_candidate_memo_in_locale(
            db, dict(row), brand_profile, lang,
        )
        if regenerated is not None:
            regen_text, regen_json = regenerated
            result["decision_memo"] = regen_text
            result["decision_memo_json"] = regen_json

    return result


def get_recommendation_report(db: Session, search_id: str, lang: str = "en") -> dict[str, Any] | None:
    t_start = time.monotonic()
    search = get_search(db, search_id)
    if not search:
        return None
    district_lookup = _cached_district_lookup(db)
    t_lookup = time.monotonic()
    try:
        raw_candidates = get_candidates(db, search_id, district_lookup=district_lookup, lang=lang)
    except TypeError:
        # Legacy-compat fallback for a 2-arg get_candidates (e.g. a test
        # double). lang cannot be threaded through such a callable —
        # degrade to the English read path.
        raw_candidates = get_candidates(db, search_id)
    t_candidates = time.monotonic()
    # Candidates are already normalized by get_candidates — skip redundant re-normalization
    normalized_candidates = raw_candidates

    # Dedupe top candidates to avoid near-clone rows in the report
    normalized_candidates = _dedupe_candidates(normalized_candidates, aggressive=True)

    def _sort_key(item: dict[str, Any]) -> tuple[int, float]:
        rank = item.get("rank_position")
        if rank is None:
            return (10**9, -_safe_float(item.get("final_score")))
        return (_safe_int(rank, 10**9), -_safe_float(item.get("final_score")))

    top = sorted(normalized_candidates, key=_sort_key)[:3]

    if not normalized_candidates:
        logger.info(
            "expansion_report timing: total=%.2fs search_id=%s candidates=0 (empty)",
            time.monotonic() - t_start, search_id,
        )
        return {
            "search_id": search_id,
            "brand_profile": search.get("brand_profile") or {},
            "meta": {"version": _EXPANSION_VERSION},
            "top_candidates": [],
            "recommendation": {
                "best_candidate_id": None,
                "runner_up_candidate_id": None,
                "best_pass_candidate_id": None,
                "best_confidence_candidate_id": None,
                "highest_demand_candidate_id": None,
                "best_economics_candidate_id": None,
                "best_brand_fit_candidate_id": None,
                "strongest_whitespace_candidate_id": None,
                "most_confident_candidate_id": None,
                "best_value_candidate_id": None,
                "why_best": "",
                "main_risk": "",
                "best_format": "",
                "summary": "",
                "report_summary": "",
            },
            "assumptions": {
                "parcel_source": _EXPANSION_PARCEL_SOURCE,
                "city": "riyadh",
                "heuristic_metrics": [
                    "provider_density_score",
                    "provider_whitespace_score",
                    "multi_platform_presence_score",
                    "delivery_competition_score",
                    "brand_fit_score",
                ],
            },
        }

    best = max(normalized_candidates, key=lambda item: _safe_float(item.get("final_score")))
    ranked_by_score = sorted(normalized_candidates, key=lambda item: _safe_float(item.get("final_score"), 0.0), reverse=True)
    runner_item = ranked_by_score[1] if len(ranked_by_score) > 1 else None
    grade_order = {"A": 4, "B": 3, "C": 2, "D": 1}
    best_confidence = max(
        normalized_candidates,
        key=lambda item: (
            grade_order.get(str(item.get("confidence_grade") or "D"), 0),
            _safe_float(item.get("confidence_score")),
        ),
    )
    pass_candidates = [c for c in normalized_candidates if (c.get("gate_status_json") or {}).get("overall_pass") is True]
    # Candidates with no blocking failures but some unknown/unresolved gates (overall_pass=None)
    unknown_candidates = [
        c for c in normalized_candidates
        if (c.get("gate_status_json") or {}).get("overall_pass") is None
        and not (c.get("gate_reasons_json") or {}).get("blocking_failures")
    ]
    best_pass = max(pass_candidates, key=lambda item: _safe_float(item.get("final_score"))) if pass_candidates else None
    # pass_count is strict: only truly passing candidates (overall_pass is True).
    # validation_clear_count tracks candidates with no blocking failures but unresolved gates.
    pass_count = len(pass_candidates)
    validation_clear_count = len(unknown_candidates)

    # Dimension winners. The frontend report panel reads each *_candidate_id
    # off the recommendation block and renders a tile per populated id; if
    # the field is null the tile collapses. Five of these were declared on
    # the frontend type but never populated server-side (Bug B in the
    # investigation report). Compute them across the full normalized
    # candidate list (not just `top`, which is pre-truncated to 3).
    def _max_id_by(items: list[dict[str, Any]], key) -> str | None:
        valid = [i for i in items if isinstance(key(i), (int, float))]
        if not valid:
            return None
        return max(valid, key=key).get("id")

    highest_demand_id = _max_id_by(normalized_candidates, lambda i: _safe_float(i.get("demand_score")))
    best_economics_id = _max_id_by(normalized_candidates, lambda i: _safe_float(i.get("economics_score")))
    best_brand_fit_id = _max_id_by(normalized_candidates, lambda i: _safe_float(i.get("brand_fit_score")))
    strongest_whitespace_id = _max_id_by(normalized_candidates, lambda i: _safe_float(i.get("provider_whitespace_score")))
    most_confident_item = max(
        normalized_candidates,
        key=lambda i: (
            grade_order.get(str(i.get("confidence_grade") or "D"), 0),
            _safe_float(i.get("confidence_score")),
        ),
    )
    most_confident_id = most_confident_item.get("id")

    # best_value: highest published value_score across the full result set.
    # Independent peer of lowest_rent_burden_candidate_id (which is not
    # exposed on this endpoint today, but if/when it's added it stays
    # semantically distinct: smallest absolute rent vs. value-score winner).
    def _value_or_neg_report(item: dict[str, Any]) -> float:
        v = item.get("value_score")
        return float(v) if isinstance(v, (int, float)) else -1.0

    _bv_item = max(normalized_candidates, key=_value_or_neg_report)
    best_value_id = _bv_item.get("id") if _value_or_neg_report(_bv_item) >= 0 else None

    top_payload: list[dict[str, Any]] = []
    for item in top:
        snapshot = item.get("feature_snapshot_json") or {}
        score_breakdown = item.get("score_breakdown_json") or {}
        top_payload.append(
            {
                "id": item.get("id"),
                "final_score": item.get("final_score"),
                "rank_position": item.get("rank_position"),
                "confidence_grade": item.get("confidence_grade") or "D",
                "gate_verdict": _gate_verdict_label((item.get("gate_status_json") or {}).get("overall_pass")),
                "top_positives_json": (item.get("top_positives_json") or [])[:3],
                "top_risks_json": (item.get("top_risks_json") or [])[:3],
                "district": item.get("district"),
                "district_key": item.get("district_key"),
                "district_name_ar": item.get("district_name_ar"),
                "district_name_en": item.get("district_name_en"),
                "district_display": item.get("district_display"),
                "feature_snapshot_json": {
                    "district": snapshot.get("district"),
                    "parcel_area_m2": snapshot.get("parcel_area_m2"),
                    "data_completeness_score": snapshot.get("data_completeness_score"),
                    "missing_context": snapshot.get("missing_context") or [],
                    "touches_road": snapshot.get("touches_road"),
                    "nearby_road_segment_count": snapshot.get("nearby_road_segment_count"),
                    "nearest_major_road_distance_m": snapshot.get("nearest_major_road_distance_m"),
                    "nearby_parking_amenity_count": snapshot.get("nearby_parking_amenity_count"),
                    "context_sources": snapshot.get("context_sources") or {},
                },
                "score_breakdown_json": {
                    "weights": score_breakdown.get("weights") or {},
                    "inputs": score_breakdown.get("inputs") or {},
                    "weighted_components": score_breakdown.get("weighted_components") or {},
                    "display": score_breakdown.get("display") or {},
                    "final_score": _safe_float(score_breakdown.get("final_score"), _safe_float(item.get("final_score"))),
                    # economics_detail carries rent_burden / value_score /
                    # value_band — the report panel renders these on the
                    # top-3 cards, so projecting them is required for parity
                    # with /candidates.
                    "economics_detail": score_breakdown.get("economics_detail") or {},
                },
                # value_score / value_band lifted from the normalized
                # candidate so the report panel's top-3 cards can render
                # the same chip the candidate list shows.
                "value_score": item.get("value_score"),
                "value_band": item.get("value_band"),
                "value_band_low_confidence": bool(item.get("value_band_low_confidence")),
            }
        )

    # Build recommendation language — consistent with strict pass_count.
    # Three states: pass (gates clear), validation-clear (no blocking failures but unresolved), fail.
    # Each string is routed through expansion_advisor_i18n.render() for the
    # requested lang; render() returns "" on any failure, in which case we
    # fall back to the English f-string below. The fallback path keeps the
    # English output byte-identical to HEAD (F5 Stage 2 — AR parity only).
    from app.services.expansion_advisor_i18n import render as _render_i18n

    def _localized(template_id: str, params: dict[str, Any], fallback: str) -> str:
        rendered = _render_i18n({"id": template_id, "params": params}, lang)
        return rendered or fallback

    best_district = best.get("district_display") or best.get("district") or "the top district"
    runner_district = (runner_item.get("district_display") or runner_item.get("district")) if runner_item else "backup options"
    if pass_candidates:
        # At least one candidate truly passes all gates
        _bf = _safe_float(best.get("brand_fit_score"))
        _ec = _safe_float(best.get("economics_score"))
        why_best = _localized(
            "report.why_best.pass",
            {"bf": _bf, "ec": _ec},
            f"Highest blended final score with brand fit {_bf:.1f}/100 and economics {_ec:.1f}/100.",
        )
        summary_text = _localized(
            "report.summary.pass",
            {"best": best_district, "runner": runner_district},
            f"Recommend {best_district} first, then sequence {runner_district} as runner-up.",
        )
        report_summary_text = summary_text
    elif unknown_candidates:
        # No strict passes, but some candidates have no blocking failures — needs field validation
        _fs = _safe_float(best.get("final_score"))
        why_best = _localized(
            "report.why_best.validation_clear",
            {"fs": _fs, "n": validation_clear_count},
            (
                f"Top-ranked candidate scores {_fs:.1f}/100 "
                f"with {validation_clear_count} candidate(s) pending gate validation."
            ),
        )
        summary_text = _localized(
            "report.summary.validation_clear",
            {"n": validation_clear_count, "best": best_district},
            (
                f"No candidate has fully passed all gates yet. "
                f"{validation_clear_count} candidate(s) have no blocking failures but need field validation. "
                f"Consider {best_district} as the exploratory lead."
            ),
        )
        report_summary_text = summary_text
    else:
        _fs = _safe_float(best.get("final_score"))
        why_best = _localized(
            "report.why_best.fail",
            {"fs": _fs},
            (
                f"Top-ranked candidate scores {_fs:.1f}/100 "
                f"but does not yet pass all gates — unresolved items need validation."
            ),
        )
        summary_text = _localized(
            "report.summary.fail",
            {"pc": pass_count, "total": len(normalized_candidates), "best": best_district},
            (
                f"No candidate currently passes all required gates ({pass_count} of {len(normalized_candidates)} pass). "
                f"Consider {best_district} as an exploratory lead pending further validation."
            ),
        )
        report_summary_text = summary_text

    t_report_done = time.monotonic()
    logger.info(
        "expansion_report timing: total=%.2fs lookup=%.2fs candidates=%.2fs build=%.2fs "
        "search_id=%s candidates=%d pass_count=%d validation_clear=%d",
        t_report_done - t_start,
        t_lookup - t_start,
        t_candidates - t_lookup,
        t_report_done - t_candidates,
        search_id, len(normalized_candidates), pass_count, validation_clear_count,
    )

    return {
        "search_id": search_id,
        "brand_profile": search.get("brand_profile") or {},
        "meta": {"version": _EXPANSION_VERSION},
        "top_candidates": top_payload,
        "recommendation": {
            "best_candidate_id": best.get("id"),
            "runner_up_candidate_id": runner_item.get("id") if runner_item else None,
            "best_pass_candidate_id": best_pass.get("id") if best_pass else None,
            "best_confidence_candidate_id": best_confidence.get("id"),
            # Dimension Winners — populated server-side as of this PR.
            # Frontend (ExpansionReportPanel.tsx) was reading these and
            # rendering nothing; the panel's Dimension Winners tile now
            # surfaces them. best_value_candidate_id is a new peer of the
            # rest — derived from value_score, independent of
            # lowest_rent_burden_candidate_id (which remains "smallest
            # absolute annual rent" in compare_candidates).
            "highest_demand_candidate_id": highest_demand_id,
            "best_economics_candidate_id": best_economics_id,
            "best_brand_fit_candidate_id": best_brand_fit_id,
            "strongest_whitespace_candidate_id": strongest_whitespace_id,
            "most_confident_candidate_id": most_confident_id,
            "best_value_candidate_id": best_value_id,
            "pass_count": pass_count,
            "validation_clear_count": validation_clear_count,
            "why_best": why_best,
            # Primary value is key_risks_json[0] (already AR-localized
            # upstream); only the fallback literal is localized here.
            "main_risk": (
                best.get("key_risks_json")
                or [_localized("report.main_risk_fallback", {}, "Validate lease and execution assumptions")]
            )[0],
            "best_format": _recommended_use_case(str(search.get("service_model") or "qsr"), _safe_float(best.get("area_m2")), lang=lang),
            "summary": summary_text,
            "report_summary": report_summary_text,
        },
        "assumptions": {
            "parcel_source": _EXPANSION_PARCEL_SOURCE,
            "city": "riyadh",
            "heuristic_metrics": [
                "provider_density_score",
                "provider_whitespace_score",
                "multi_platform_presence_score",
                "delivery_competition_score",
                "brand_fit_score",
            ],
        },
    }
