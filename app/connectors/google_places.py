"""
Connector for the Google Places API.

Uses the official REST endpoints:
- Find Place (Text Search) to locate candidates by name + location.
- Place Details to fetch rating, user_ratings_total, price_level, etc.

API key is read from env var ``GOOGLE_PLACES_API_KEY``.
"""

from __future__ import annotations

import logging
import math
import os
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_API_KEY: str | None = None

# Rate-limiter: max QPS to stay within Google's per-second quota.
_MAX_QPS = 8
_MIN_INTERVAL = 1.0 / _MAX_QPS  # seconds between requests
_last_request_ts: float = 0.0

# Retry / backoff
_MAX_RETRIES = 4
_BACKOFF_BASE = 1.5  # 1.5, 3, 6, 12 seconds

# In-memory cache keyed by (name, lat, lon) -> candidates list
_candidates_cache: dict[tuple[str, float, float], list[dict]] = {}
# In-memory cache keyed by place_id -> details dict
_details_cache: dict[str, dict] = {}

_BASE_URL = "https://maps.googleapis.com/maps/api/place"


def _get_api_key() -> str:
    global _API_KEY
    if _API_KEY is None:
        _API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY", "")
    if not _API_KEY:
        raise RuntimeError(
            "GOOGLE_PLACES_API_KEY environment variable is not set"
        )
    return _API_KEY


def _rate_limit() -> None:
    """Sleep if necessary to honour the QPS cap."""
    global _last_request_ts
    now = time.monotonic()
    elapsed = now - _last_request_ts
    if elapsed < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - elapsed)
    _last_request_ts = time.monotonic()


def _request_with_retry(url: str, params: dict[str, Any]) -> dict:
    """
    GET request with rate-limiting and exponential backoff on 429 / 5xx.
    Returns the parsed JSON body.
    """
    for attempt in range(_MAX_RETRIES):
        _rate_limit()
        try:
            resp = httpx.get(url, params=params, timeout=30)
            if resp.status_code == 429 or resp.status_code >= 500:
                delay = _BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    "Google Places API %s (attempt %d/%d), retrying in %.1fs",
                    resp.status_code, attempt + 1, _MAX_RETRIES, delay,
                )
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp.json()
        except httpx.TimeoutException:
            delay = _BACKOFF_BASE * (2 ** attempt)
            logger.warning(
                "Google Places API timeout (attempt %d/%d), retrying in %.1fs",
                attempt + 1, _MAX_RETRIES, delay,
            )
            time.sleep(delay)
        except httpx.HTTPStatusError:
            raise

    raise RuntimeError(f"Google Places API request failed after {_MAX_RETRIES} retries")


def find_place_candidates(
    name: str,
    lat: float,
    lon: float,
    radius_m: int = 500,
) -> list[dict]:
    """
    Search for restaurant candidates near the given coordinates.

    Uses the Text Search endpoint scoped to a circular area.
    Returns a list of candidate dicts with keys:
        place_id, name, geometry (lat/lng), rating, user_ratings_total,
        price_level, types, business_status.
    """
    cache_key = (name.strip().lower(), round(lat, 5), round(lon, 5))
    if cache_key in _candidates_cache:
        return _candidates_cache[cache_key]

    key = _get_api_key()
    params = {
        "query": name,
        "location": f"{lat},{lon}",
        "radius": str(radius_m),
        "type": "restaurant",
        "key": key,
    }
    data = _request_with_retry(f"{_BASE_URL}/textsearch/json", params)

    status = data.get("status", "")
    if status not in ("OK", "ZERO_RESULTS"):
        logger.error("Google Places Text Search status=%s for %r", status, name)
        _candidates_cache[cache_key] = []
        return []

    results = data.get("results", [])
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
        })

    _candidates_cache[cache_key] = candidates
    return candidates


def get_place_details(place_id: str) -> dict:
    """
    Fetch detailed info for a specific Google Place.

    Returns a dict with keys:
        place_id, name, rating, user_ratings_total, price_level,
        lat, lng, types, formatted_address.
    """
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
    data = _request_with_retry(f"{_BASE_URL}/details/json", params)

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

_RESTAURANT_TYPES = frozenset({
    "restaurant", "cafe", "meal_takeaway", "meal_delivery",
    "food", "bakery", "bar",
})


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres between two lat/lon pairs."""
    R = 6_371_000  # Earth radius in metres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _name_similarity(a: str, b: str) -> float:
    """
    Simple case-insensitive token-overlap similarity in [0, 1].
    """
    ta = set(a.lower().split())
    tb = set(b.lower().split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(len(ta), len(tb))


def pick_best_candidate(
    name: str,
    lat: float,
    lon: float,
    candidates: list[dict],
    max_distance_m: float = 250.0,
    min_confidence: float = 0.4,
) -> tuple[dict | None, float]:
    """
    Choose the best Google Places candidate for a restaurant POI.

    Scoring:
        - distance_score: 1.0 when <=50m, linearly to 0.0 at max_distance_m
        - name_score: token-overlap similarity
        - type_bonus: 0.15 if types include a restaurant-related category
        - confidence = 0.45 * distance_score + 0.40 * name_score + 0.15 * type_bonus

    Returns (best_candidate, confidence).  If no candidate exceeds
    min_confidence, returns (None, 0.0).
    """
    best: dict | None = None
    best_conf = 0.0

    for c in candidates:
        clat, clng = c.get("lat"), c.get("lng")
        if clat is None or clng is None:
            continue

        dist = _haversine_m(lat, lon, clat, clng)
        if dist > max_distance_m:
            continue

        # distance score: perfect at <=50m, drops linearly
        if dist <= 50:
            dist_score = 1.0
        else:
            dist_score = max(0.0, 1.0 - (dist - 50) / (max_distance_m - 50))

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


def clear_caches() -> None:
    """Clear in-memory caches (useful between runs in tests)."""
    _candidates_cache.clear()
    _details_cache.clear()
