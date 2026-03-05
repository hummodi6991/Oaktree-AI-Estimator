"""
Async connector for the Google Places API with concurrency and rate limiting.

Uses httpx.AsyncClient with:
- Token-bucket rate limiter (QPS <= 8)
- Semaphore for max concurrent in-flight requests (default 20)
- Exponential backoff on 429 / 5xx
- In-memory caches for deduplication

Public API mirrors google_places.py but all functions are async.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import re
import time
import unicodedata
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_API_KEY: str | None = None
_BASE_URL = "https://maps.googleapis.com/maps/api/place"

# Rate-limit defaults
MAX_QPS = 8
MAX_CONCURRENCY = 20

# Retry / backoff
_MAX_RETRIES = 4
_BACKOFF_BASE = 1.5

# Matching constants
_RESTAURANT_TYPES = frozenset({
    "restaurant", "cafe", "meal_takeaway", "meal_delivery",
    "food", "bakery", "bar",
})

_GENERIC_NAMES = frozenset(
    {
        "مطعم",
        "مقهى",
        "كافيه",
        "restaurant",
        "cafe",
        "coffee",
        "food",
        "bakery",
        "grill",
    }
)

# ---------------------------------------------------------------------------
# Name normalization helpers
# ---------------------------------------------------------------------------

# Arabic diacritics (tashkeel) Unicode range
_ARABIC_DIACRITICS = re.compile(r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7-\u06E8\u06EA-\u06ED]")

# Arabic Alef variants → bare Alef
_ALEF_VARIANTS = re.compile(r"[\u0622\u0623\u0625\u0671]")  # آ أ إ ٱ

# Ta marbuta → Ha
_TA_MARBUTA = "\u0629"  # ة
_HA = "\u0647"  # ه

# Arabic-Indic digits → ASCII
_ARABIC_DIGIT_MAP = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

# Generic tokens to strip for similarity (both Arabic and English)
_STRIP_TOKENS_AR = frozenset({
    "مطعم", "كافيه", "مقهى", "كوفي", "مطاعم", "كافيتريا",
    "مخبز", "حلويات", "عصير", "محل", "فرع", "الرياض", "السعودية", "شارع",
})
_STRIP_TOKENS_EN = frozenset({
    "restaurant", "restaurants", "cafe", "coffee", "juice",
    "branch", "riyadh", "ksa", "saudi", "store", "shop",
    "mall", "grill", "kitchen", "house", "bakery",
})
_STRIP_TOKENS = _STRIP_TOKENS_AR | _STRIP_TOKENS_EN

# Category → ordered list of Google Place types to try
_CATEGORY_TYPE_MAP: dict[str, list[str | None]] = {
    "coffee_bakery": ["cafe", "bakery", None],
    "healthy": ["restaurant", "meal_takeaway", None],
}
_DEFAULT_TYPE_ORDER: list[str | None] = ["restaurant", None]

# In-memory caches
_candidates_cache: dict[tuple[str, float, float, str | None, int], list[dict]] = {}
_details_cache: dict[str, dict] = {}


def _get_api_key() -> str:
    global _API_KEY
    if _API_KEY is None:
        _API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY", "")
    if not _API_KEY:
        raise RuntimeError("GOOGLE_PLACES_API_KEY environment variable is not set")
    return _API_KEY


def normalize_name(name: str) -> str:
    """
    Normalize a restaurant name for similarity comparison.

    Steps:
    - Lowercase
    - Remove Arabic diacritics (tashkeel)
    - Normalize Alef variants to bare Alef
    - Normalize ta marbuta to ha
    - Normalize Arabic-Indic digits to ASCII
    - Remove punctuation
    - Collapse whitespace
    """
    s = name.lower()
    # Remove Arabic diacritics
    s = _ARABIC_DIACRITICS.sub("", s)
    # Normalize Alef variants
    s = _ALEF_VARIANTS.sub("\u0627", s)  # → ا
    # Ta marbuta → Ha
    s = s.replace(_TA_MARBUTA, _HA)
    # Arabic digits → ASCII
    s = s.translate(_ARABIC_DIGIT_MAP)
    # Remove punctuation (keep letters, digits, spaces)
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _strip_diacritics_latin(s: str) -> str:
    """Remove Latin diacritics (e.g., é → e)."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalized_token_set(name: str) -> set[str]:
    """
    Produce a normalized token set for similarity comparison.
    Strips generic tokens and applies full normalization.
    """
    normed = normalize_name(name)
    normed = _strip_diacritics_latin(normed)
    tokens = set(normed.split())
    tokens -= _STRIP_TOKENS
    return tokens


def _build_name_variants(name: str, name_ar: str | None) -> list[str]:
    """
    Build a list of query name variants for a POI, ordered by specificity.
    """
    variants: list[str] = []
    seen: set[str] = set()

    def _add(v: str) -> None:
        v = v.strip()
        if v and v.lower() not in seen:
            seen.add(v.lower())
            variants.append(v)

    # 1. Original name as-is
    _add(name)

    # 2. Arabic name if present and different
    if name_ar:
        _add(name_ar)

    # 3. Normalized name (strip punctuation/diacritics, collapse spaces)
    normed = normalize_name(name)
    _add(normed)

    # 4. Name + "Riyadh" / name + "الرياض"
    _add(f"{name} Riyadh")
    _add(f"{name} الرياض")
    if name_ar:
        _add(f"{name_ar} الرياض")

    return variants


class TokenBucketRateLimiter:
    """Simple async token-bucket rate limiter."""

    def __init__(self, qps: float = MAX_QPS):
        self._interval = 1.0 / qps
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._last + self._interval - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.monotonic()


class AsyncGooglePlacesClient:
    """Async Google Places client with rate limiting and concurrency control."""

    def __init__(
        self,
        qps: float = MAX_QPS,
        max_concurrency: int = MAX_CONCURRENCY,
    ):
        self._rate_limiter = TokenBucketRateLimiter(qps)
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._client: httpx.AsyncClient | None = None
        self.api_calls = 0

    async def __aenter__(self) -> "AsyncGooglePlacesClient":
        self._client = httpx.AsyncClient(timeout=30)
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _request(self, url: str, params: dict[str, Any]) -> dict:
        """GET with rate limiting, concurrency control, and retries."""
        assert self._client is not None, "Use as async context manager"

        async with self._semaphore:
            for attempt in range(_MAX_RETRIES):
                await self._rate_limiter.acquire()
                self.api_calls += 1
                try:
                    resp = await self._client.get(url, params=params)
                    if resp.status_code == 429 or resp.status_code >= 500:
                        delay = _BACKOFF_BASE * (2 ** attempt)
                        logger.warning(
                            "Google Places API %s (attempt %d/%d), retrying in %.1fs",
                            resp.status_code, attempt + 1, _MAX_RETRIES, delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    resp.raise_for_status()
                    return resp.json()
                except httpx.TimeoutException:
                    delay = _BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        "Google Places API timeout (attempt %d/%d), retrying in %.1fs",
                        attempt + 1, _MAX_RETRIES, delay,
                    )
                    await asyncio.sleep(delay)
                except httpx.HTTPStatusError:
                    raise

        raise RuntimeError(f"Google Places API request failed after {_MAX_RETRIES} retries")

    async def _text_search(
        self,
        query: str,
        lat: float,
        lon: float,
        radius_m: int = 500,
        place_type: str | None = "restaurant",
        max_results: int = 5,
    ) -> list[dict]:
        """Single Text Search call. Returns parsed candidate list."""
        cache_key = (query.strip().lower(), round(lat, 5), round(lon, 5), place_type, radius_m)
        if cache_key in _candidates_cache:
            return _candidates_cache[cache_key]

        key = _get_api_key()
        params: dict[str, Any] = {
            "query": query,
            "location": f"{lat},{lon}",
            "radius": str(radius_m),
            "key": key,
        }
        if place_type:
            params["type"] = place_type

        data = await self._request(f"{_BASE_URL}/textsearch/json", params)

        status = data.get("status", "")
        if status not in ("OK", "ZERO_RESULTS"):
            logger.error("Google Places Text Search status=%s for %r", status, query)
            _candidates_cache[cache_key] = []
            return []

        results = data.get("results", [])[:max_results]
        candidates = []
        for r in results:
            candidates.append({
                "place_id": r.get("place_id"),
                "name": r.get("name", ""),
                "lat": r.get("geometry", {}).get("location", {}).get("lat"),
                "lng": r.get("geometry", {}).get("location", {}).get("lng"),
                "rating": r.get("rating"),
                "user_ratings_total": r.get("user_ratings_total"),
                "price_level": r.get("price_level"),
                "types": r.get("types", []),
                "business_status": r.get("business_status"),
                "formatted_address": r.get("formatted_address", ""),
            })

        _candidates_cache[cache_key] = candidates
        return candidates

    async def find_place_candidates(
        self,
        name: str,
        lat: float,
        lon: float,
        radius_m: int = 500,
        max_results: int = 5,
    ) -> list[dict]:
        """
        Text Search for restaurant candidates, limited to max_results.

        Legacy single-query interface (kept for backward compatibility).
        Prefer find_candidates_multi for better coverage.
        """
        return await self._text_search(name, lat, lon, radius_m, "restaurant", max_results)

    async def find_candidates_multi(
        self,
        name_variants: list[str],
        lat: float,
        lon: float,
        category: str | None = None,
        radius_m: int = 500,
        max_results: int = 5,
    ) -> tuple[list[dict], dict[str, Any]]:
        """
        Try multiple query variants and type combinations to find candidates.

        Returns (candidates, fallback_info) where fallback_info tracks which
        strategies were used.

        Strategy order:
        1. For each name variant, try category-appropriate types
        2. If all fail at radius_m, escalate to 1500m
        """
        fallback_info: dict[str, Any] = {
            "attempts": 0,
            "variant_used": None,
            "type_used": None,
            "radius_used": radius_m,
            "used_name_ar": False,
            "used_riyadh_suffix": False,
            "removed_type": False,
            "radius_escalated": False,
        }

        # Determine type order based on category
        type_order = _CATEGORY_TYPE_MAP.get(category or "", _DEFAULT_TYPE_ORDER)

        # Pass 1: normal radius
        for variant in name_variants:
            for place_type in type_order:
                fallback_info["attempts"] += 1
                candidates = await self._text_search(
                    variant, lat, lon, radius_m, place_type, max_results,
                )
                if candidates:
                    fallback_info["variant_used"] = variant
                    fallback_info["type_used"] = place_type
                    # Track which fallback was used
                    if variant != name_variants[0]:
                        if "Riyadh" in variant or "الرياض" in variant:
                            fallback_info["used_riyadh_suffix"] = True
                        elif name_ar and variant == name_ar:
                            fallback_info["used_name_ar"] = True
                    if place_type is None:
                        fallback_info["removed_type"] = True
                    return candidates, fallback_info

        # Pass 2: escalated radius (1500m) — only try first 2 variants with first type
        escalated_radius = 1500
        for variant in name_variants[:2]:
            for place_type in type_order[:1]:  # Only primary type at wider radius
                fallback_info["attempts"] += 1
                candidates = await self._text_search(
                    variant, lat, lon, escalated_radius, place_type, max_results,
                )
                if candidates:
                    fallback_info["variant_used"] = variant
                    fallback_info["type_used"] = place_type
                    fallback_info["radius_used"] = escalated_radius
                    fallback_info["radius_escalated"] = True
                    return candidates, fallback_info

        # Pass 3: escalated radius with no type constraint (last resort)
        for variant in name_variants[:2]:
            fallback_info["attempts"] += 1
            candidates = await self._text_search(
                variant, lat, lon, escalated_radius, None, max_results,
            )
            if candidates:
                fallback_info["variant_used"] = variant
                fallback_info["type_used"] = None
                fallback_info["radius_used"] = escalated_radius
                fallback_info["radius_escalated"] = True
                fallback_info["removed_type"] = True
                return candidates, fallback_info

        return [], fallback_info

    async def get_place_details(self, place_id: str) -> dict:
        """Fetch Place Details for a specific place_id."""
        if place_id in _details_cache:
            return _details_cache[place_id]

        key = _get_api_key()
        fields = (
            "place_id,name,rating,user_ratings_total,price_level,"
            "geometry,types,formatted_address"
        )
        params = {
            "place_id": place_id,
            "fields": fields,
            "key": key,
        }
        data = await self._request(f"{_BASE_URL}/details/json", params)

        status = data.get("status", "")
        if status != "OK":
            logger.error("Google Places Details status=%s for %s", status, place_id)
            return {}

        r = data.get("result", {})
        details = {
            "place_id": r.get("place_id", place_id),
            "name": r.get("name", ""),
            "rating": r.get("rating"),
            "user_ratings_total": r.get("user_ratings_total"),
            "price_level": r.get("price_level"),
            "lat": r.get("geometry", {}).get("location", {}).get("lat"),
            "lng": r.get("geometry", {}).get("location", {}).get("lng"),
            "types": r.get("types", []),
            "formatted_address": r.get("formatted_address", ""),
        }
        _details_cache[place_id] = details
        return details


# ---------------------------------------------------------------------------
# Matching helpers
# ---------------------------------------------------------------------------


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _name_similarity(a: str, b: str) -> float:
    """
    Normalized token-overlap similarity in [0, 1].

    Uses full normalization: lowercase, strip diacritics, remove generic
    tokens, then compute Jaccard-style overlap.
    """
    ta = _normalized_token_set(a)
    tb = _normalized_token_set(b)
    if not ta or not tb:
        # Fallback: basic lowercase split if normalization stripped everything
        ta = set(normalize_name(a).split())
        tb = set(normalize_name(b).split())
    if not ta or not tb:
        return 0.0
    intersection = len(ta & tb)
    union = len(ta | tb)
    return intersection / union if union > 0 else 0.0


def _is_generic_name(name: str) -> bool:
    """Check if the restaurant name is too generic for reliable matching."""
    tokens = set(name.lower().split())
    return len(tokens) <= 1 and bool(tokens & _GENERIC_NAMES)


def _expected_types_for_category(category: str | None) -> frozenset[str]:
    """Return expected Google Place types for a given POI category."""
    if category == "coffee_bakery":
        return frozenset({"cafe", "bakery"})
    elif category == "healthy":
        return frozenset({"restaurant", "meal_takeaway"})
    else:
        return frozenset({"restaurant"})


def pick_best_candidate(
    name: str,
    lat: float,
    lon: float,
    candidates: list[dict],
    max_distance_m: float = 250.0,
    min_confidence: float = 0.4,
    category: str | None = None,
    radius_escalated: bool = False,
) -> tuple[dict | None, float]:
    """
    Choose the best Google Places candidate for a restaurant POI.

    Scoring:
    - 40% distance score
    - 35% name similarity
    - 15% type bonus
    - 10% proximity/context bonuses

    For generic names, require closer distance (<=100m).
    For escalated radius, require stronger name match.
    """
    generic = _is_generic_name(name)
    effective_max_dist = 100.0 if generic else max_distance_m
    # When radius was escalated, require higher name similarity
    effective_min_conf = min_confidence
    if radius_escalated:
        effective_min_conf = max(min_confidence, 0.50)

    expected_types = _expected_types_for_category(category)

    best: dict | None = None
    best_conf = 0.0
    candidates_within_75m = []

    for c in candidates:
        clat, clng = c.get("lat"), c.get("lng")
        if clat is None or clng is None:
            continue

        dist = _haversine_m(lat, lon, clat, clng)
        if dist > effective_max_dist:
            continue

        # distance score: perfect at <=50m, drops linearly
        if dist <= 50:
            dist_score = 1.0
        else:
            dist_score = max(0.0, 1.0 - (dist - 50) / (effective_max_dist - 50))

        name_score = _name_similarity(name, c.get("name", ""))

        types = set(c.get("types", []))
        type_bonus = 1.0 if types & _RESTAURANT_TYPES else 0.0

        # Extra bonuses
        bonus = 0.0

        # Strong proximity bonus (<=75m)
        if dist <= 75:
            bonus += 0.3
            candidates_within_75m.append((c, name_score))

        # Category type match bonus
        if types & expected_types:
            bonus += 0.15

        # Penalty for very generic candidate name when POI name is non-generic
        cand_name = c.get("name", "")
        if not generic and _is_generic_name(cand_name):
            bonus -= 0.2

        conf = 0.40 * dist_score + 0.35 * name_score + 0.15 * type_bonus + 0.10 * min(bonus, 1.0)

        if conf > best_conf:
            best_conf = conf
            best = c

    # Special rule: if exactly one candidate within 75m and moderate name similarity,
    # accept it even if overall score is slightly below threshold
    if best is None or best_conf < effective_min_conf:
        if len(candidates_within_75m) == 1:
            sole_candidate, sole_name_score = candidates_within_75m[0]
            if sole_name_score >= 0.15:  # at least some token overlap
                # Force accept with a minimum confidence
                return sole_candidate, round(max(best_conf, 0.40), 4)

    if best_conf < effective_min_conf:
        return None, 0.0

    return best, round(best_conf, 4)


def candidate_has_full_data(candidate: dict) -> bool:
    """Check if text search result already has rating + review_count."""
    return (
        candidate.get("rating") is not None
        and candidate.get("user_ratings_total") is not None
    )


def clear_caches() -> None:
    """Clear in-memory caches."""
    _candidates_cache.clear()
    _details_cache.clear()
