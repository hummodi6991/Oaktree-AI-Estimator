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
import time
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

# In-memory caches
_candidates_cache: dict[tuple[str, float, float], list[dict]] = {}
_details_cache: dict[str, dict] = {}


def _get_api_key() -> str:
    global _API_KEY
    if _API_KEY is None:
        _API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY", "")
    if not _API_KEY:
        raise RuntimeError("GOOGLE_PLACES_API_KEY environment variable is not set")
    return _API_KEY


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

    async def find_place_candidates(
        self,
        name: str,
        lat: float,
        lon: float,
        radius_m: int = 500,
        max_results: int = 5,
        type_filter: str | None = "restaurant",
    ) -> list[dict]:
        """Text Search for restaurant candidates, limited to max_results.

        When *type_filter* is ``None`` the ``type`` query-param is omitted so
        that Google returns results across all establishment categories.
        """
        cache_key = (name.strip().lower(), round(lat, 5), round(lon, 5), type_filter or "", radius_m)
        if cache_key in _candidates_cache:
            return _candidates_cache[cache_key]

        key = _get_api_key()
        params: dict[str, str] = {
            "query": name,
            "location": f"{lat},{lon}",
            "radius": str(radius_m),
            "key": key,
        }
        if type_filter:
            params["type"] = type_filter

        data = await self._request(f"{_BASE_URL}/textsearch/json", params)

        status = data.get("status", "")
        if status not in ("OK", "ZERO_RESULTS"):
            logger.error("Google Places Text Search status=%s for %r", status, name)
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
# Matching helpers (same logic as sync, kept here for self-containment)
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
    """Simple case-insensitive token-overlap similarity in [0, 1]."""
    ta = set(a.lower().split())
    tb = set(b.lower().split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(len(ta), len(tb))


def _is_generic_name(name: str) -> bool:
    """Check if the restaurant name is too generic for reliable matching."""
    tokens = set(name.lower().split())
    return len(tokens) <= 1 and bool(tokens & _GENERIC_NAMES)


def pick_best_candidate(
    name: str,
    lat: float,
    lon: float,
    candidates: list[dict],
    max_distance_m: float = 500.0,
    min_confidence: float = 0.15,
) -> tuple[dict | None, float]:
    """
    Choose the best Google Places candidate for a restaurant POI.

    For generic names, require closer distance (<=150m).
    """
    generic = _is_generic_name(name)
    effective_max_dist = 150.0 if generic else max_distance_m

    best: dict | None = None
    best_conf = 0.0

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

        conf = 0.45 * dist_score + 0.40 * name_score + 0.15 * type_bonus

        if conf > best_conf:
            best_conf = conf
            best = c

    if best_conf < min_confidence:
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
