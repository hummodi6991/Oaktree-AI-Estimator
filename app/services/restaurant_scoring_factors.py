"""
Upgraded scoring factors for restaurant site selection.

Replaces the weak zoning_fit_score, parking_availability_score, and
commercial_density_score with genuinely informative, evidence-based
implementations that use ArcGIS parcel data, road context, and
weighted anchor/building signals.

Each factor returns a ScoredFactor with score, confidence, and rationale.

Performance notes (Phase 5 optimization):
- parking_availability_score consolidates 3 overture_buildings queries → 1
- Building-coverage ratio from a single overture_buildings aggregate replaces
  weak nearby-anchor sub-scores for parking
- Street-parking capacity signal derived from road width classes already
  fetched by _road_access_score (no extra query)
- Timing instrumentation via ``time.perf_counter()`` logs per-factor latency
  at DEBUG level so production dashboards can trace bottlenecks.
"""

from __future__ import annotations

import logging
import math
import re
import time
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Set to True once we detect osm_roads is absent (e.g. "relation does not
# exist" error).  Subsequent calls skip osm_roads queries entirely.
_OSM_ROADS_MISSING = False


def _is_relation_missing(exc: Exception) -> bool:
    """Return True if *exc* indicates a missing table/relation."""
    msg = str(exc).lower()
    return "does not exist" in msg and "relation" in msg


# ---------------------------------------------------------------------------
# Scored factor result — every upgraded factor returns this
# ---------------------------------------------------------------------------

@dataclass
class ScoredFactor:
    """Result from a scoring factor with confidence and rationale."""
    score: float  # 0-100
    confidence: float  # 0.0-1.0 (evidence strength)
    rationale: str = ""
    meta: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# ZONING FIT — Phase 2
# ---------------------------------------------------------------------------

# Deterministic landuse → restaurant feasibility mapping.
# Keys are lowercase substrings matched against the ArcGIS landuse_label.
# Values are (score, rationale) tuples.
# The order matters: first match wins.

_ZONING_RULES: list[tuple[list[str], float, str]] = [
    # Clearly favorable
    (["تجاري", "commercial"], 92.0, "commercial_zone"),
    (["مختلط", "mixed", "متعدد"], 90.0, "mixed_use_zone"),
    (["محلات", "retail", "shop"], 88.0, "retail_zone"),
    (["فندق", "hotel", "hospitality", "ضيافة"], 85.0, "hospitality_zone"),
    (["استثماري", "investment"], 85.0, "investment_zone"),
    (["مكاتب", "office", "اداري", "إداري"], 82.0, "office_zone"),
    (["ترفيه", "entertainment", "recreation"], 80.0, "entertainment_zone"),

    # Conditional — some restaurant potential
    (["خدمات", "service", "services"], 70.0, "service_zone"),
    (["تعليم", "education", "school", "university", "college", "جامع"], 55.0, "educational_zone"),
    (["صحي", "health", "hospital", "medical", "clinic", "مستشفى"], 55.0, "health_zone"),
    (["ديني", "religious", "mosque", "مسجد", "جامع"], 45.0, "religious_zone"),
    (["نقل", "transport", "station", "محطة"], 65.0, "transport_zone"),

    # Weak/unfavorable
    (["سكني", "residential", "سكن", "villa", "فيلا"], 35.0, "residential_zone"),
    (["صناعي", "industrial", "warehouse", "مستودع"], 25.0, "industrial_zone"),
    (["زراعي", "agricultural", "farm"], 20.0, "agricultural_zone"),
    (["حكومي", "government", "civic", "حكومة"], 40.0, "government_zone"),
    (["حديقة", "park", "garden", "open_space", "مساحة خضراء"], 22.0, "open_space_zone"),
    (["مرافق", "utility", "infrastructure"], 20.0, "utility_zone"),
]

# Landuse code (integer parcelsubt) → restaurant feasibility score.
# Based on common Riyadh ArcGIS parcel subtype codes.
_ZONING_CODE_MAP: dict[int, tuple[float, str]] = {
    1: (92.0, "commercial_code"),
    2: (35.0, "residential_code"),
    3: (25.0, "industrial_code"),
    4: (90.0, "mixed_use_code"),
    5: (55.0, "educational_code"),
    6: (55.0, "health_code"),
    7: (40.0, "government_code"),
    8: (22.0, "open_space_code"),
    9: (65.0, "transport_code"),
    10: (85.0, "investment_code"),
    11: (20.0, "utility_code"),
    12: (45.0, "religious_code"),
}


def _match_landuse_label(label: str) -> tuple[float, str] | None:
    """Match a landuse label against the zoning rules. Returns (score, rationale) or None."""
    if not label:
        return None
    ll = label.lower().strip()
    # Strip diacritics for Arabic
    ll_clean = re.sub(r"[\u064B-\u065F\u0670\u06D6-\u06ED]", "", ll)

    for keywords, score, rationale in _ZONING_RULES:
        for kw in keywords:
            if kw in ll_clean:
                return (score, rationale)
    return None


def _road_adjacency_bonus(db: Session, lat: float, lon: float) -> float:
    """
    Small bonus/penalty based on road adjacency for restaurant viability.
    Primary/secondary roads = better for restaurants; motorway-only = worse.
    Returns adjustment in [-5, +8] range.
    """
    global _OSM_ROADS_MISSING
    if _OSM_ROADS_MISSING:
        return 0.0
    try:
        rows = db.execute(
            text("""
                SELECT highway,
                       ST_Distance(
                           ST_Transform(geom, 32638),
                           ST_Transform(ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), 32638)
                       ) AS distance_m
                FROM osm_roads
                WHERE ST_DWithin(
                    geom::geography,
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                    150
                )
                ORDER BY distance_m
                LIMIT 5
            """),
            {"lat": lat, "lon": lon},
        ).mappings().all()
    except Exception as exc:
        if _is_relation_missing(exc):
            _OSM_ROADS_MISSING = True
        try:
            db.rollback()
        except Exception:
            pass
        return 0.0

    if not rows:
        return -3.0  # no nearby roads = slightly worse for restaurant access

    road_classes = {r.get("highway", "").lower() for r in rows}
    nearest_dist = float(rows[0].get("distance_m", 100))

    # Primary/secondary frontage within 50m is excellent for restaurants
    has_primary = bool(road_classes & {"primary", "primary_link", "secondary", "secondary_link"})
    has_service = "service" in road_classes
    has_motorway = bool(road_classes & {"motorway", "motorway_link", "trunk", "trunk_link"})

    bonus = 0.0
    if has_primary and nearest_dist < 50:
        bonus += 6.0
    elif has_primary:
        bonus += 3.0

    if has_service:
        bonus += 2.0  # service road access is good for restaurants in Riyadh

    if has_motorway and not has_primary and not has_service:
        bonus -= 3.0  # motorway-only access is bad for restaurants

    return max(-5.0, min(8.0, bonus))


def zoning_fit_score(db: Session, lat: float, lon: float) -> ScoredFactor:
    """
    Score how well the zoning/land-use at this location supports restaurants.

    Uses ArcGIS parcel data from riyadh_parcels_arcgis_proxy with a
    deterministic rule engine, district-level fallback, and road adjacency
    adjustment.
    """
    t0 = time.perf_counter()

    score = None
    confidence = 0.0
    rationale = "no_data"
    meta: dict[str, Any] = {}

    # --- Step 1: Direct parcel lookup from ArcGIS proxy ---
    try:
        row = db.execute(
            text("""
                SELECT landuse_label, landuse_code, area_m2, perimeter_m
                FROM public.riyadh_parcels_arcgis_proxy
                WHERE geom IS NOT NULL
                  AND ST_Contains(
                      geom,
                      ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
                  )
                LIMIT 1
            """),
            {"lat": lat, "lon": lon},
        ).mappings().first()

        if row:
            label = row.get("landuse_label")
            code = row.get("landuse_code")
            area = row.get("area_m2")
            meta["arcgis_label"] = label
            meta["arcgis_code"] = code
            meta["parcel_area_m2"] = area

            # Try label-based matching first (richer)
            match = _match_landuse_label(str(label)) if label else None
            if match:
                score, rationale = match
                confidence = 0.9
                meta["match_source"] = "arcgis_label"
            elif code is not None:
                # Try code-based mapping
                code_match = _ZONING_CODE_MAP.get(int(code))
                if code_match:
                    score, rationale = code_match
                    confidence = 0.8
                    meta["match_source"] = "arcgis_code"
                else:
                    # Unknown code, slightly better than no data
                    score = 55.0
                    rationale = "unknown_code"
                    confidence = 0.5
                    meta["match_source"] = "arcgis_unknown_code"
            else:
                # Parcel found but no landuse data at all
                score = 50.0
                rationale = "parcel_no_landuse"
                confidence = 0.3
                meta["match_source"] = "arcgis_empty"
    except Exception as exc:
        logger.debug("ArcGIS parcel zoning lookup failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass

    # --- Step 2: Fallback to old parcel table ---
    if score is None:
        try:
            result = db.execute(
                text("""
                    SELECT zoning FROM parcel
                    WHERE geom IS NOT NULL
                      AND ST_Contains(
                          geom,
                          ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
                      )
                    LIMIT 1
                """),
                {"lat": lat, "lon": lon},
            ).scalar()
            if result:
                zoning = result.lower()
                if any(k in zoning for k in ("commercial", "تجاري")):
                    score, rationale, confidence = 90.0, "parcel_commercial", 0.7
                elif any(k in zoning for k in ("mixed", "متعدد", "مختلط")):
                    score, rationale, confidence = 88.0, "parcel_mixed", 0.7
                elif any(k in zoning for k in ("residential", "سكني")):
                    score, rationale, confidence = 35.0, "parcel_residential", 0.7
                else:
                    score, rationale, confidence = 55.0, "parcel_other", 0.5
                meta["match_source"] = "legacy_parcel"
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass

    # --- Step 3: District-level fallback from nearby ArcGIS parcels ---
    if score is None:
        try:
            district_row = db.execute(
                text("""
                    SELECT
                        landuse_label,
                        COUNT(*) AS cnt,
                        COUNT(*) FILTER (WHERE landuse_label ILIKE '%%تجاري%%'
                                          OR landuse_label ILIKE '%%commercial%%'
                                          OR landuse_label ILIKE '%%مختلط%%'
                                          OR landuse_label ILIKE '%%mixed%%') AS commercial_cnt
                    FROM public.riyadh_parcels_arcgis_proxy
                    WHERE ST_DWithin(
                        geom::geography,
                        ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                        500
                    )
                    GROUP BY landuse_label
                    ORDER BY cnt DESC
                    LIMIT 5
                """),
                {"lat": lat, "lon": lon},
            ).mappings().all()

            if district_row:
                total_parcels = sum(int(r.get("cnt", 0)) for r in district_row)
                commercial_parcels = sum(int(r.get("commercial_cnt", 0)) for r in district_row)
                meta["district_parcels_total"] = total_parcels
                meta["district_parcels_commercial"] = commercial_parcels

                if total_parcels > 0:
                    commercial_ratio = commercial_parcels / total_parcels
                    # Weighted score based on commercial ratio in district
                    score = 30.0 + 55.0 * commercial_ratio  # range: 30-85
                    rationale = "district_fallback"
                    # Confidence depends on how many parcels we sampled
                    confidence = min(0.6, 0.15 + 0.05 * total_parcels)
                    meta["match_source"] = "district_nearby"
                    meta["commercial_ratio"] = round(commercial_ratio, 3)
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass

    # --- Step 4: Final default ---
    if score is None:
        score = 45.0  # slightly below neutral — genuinely unknown is not good
        rationale = "no_data_default"
        confidence = 0.1
        meta["match_source"] = "default"

    # --- Step 5: Road adjacency adjustment ---
    road_bonus = _road_adjacency_bonus(db, lat, lon)
    meta["road_bonus"] = round(road_bonus, 1)
    score = max(10.0, min(95.0, score + road_bonus))

    elapsed_ms = (time.perf_counter() - t0) * 1000
    meta["elapsed_ms"] = round(elapsed_ms, 1)
    logger.debug("zoning_fit_score: %.1f ms  score=%.1f", elapsed_ms, score)

    return ScoredFactor(
        score=round(score, 1),
        confidence=round(confidence, 3),
        rationale=rationale,
        meta=meta,
    )


# ---------------------------------------------------------------------------
# PARKING AVAILABILITY — Phase 3 + Phase 5 (optimized)
# ---------------------------------------------------------------------------

def _parcel_parking_feasibility(db: Session, lat: float, lon: float) -> tuple[float, dict]:
    """
    Assess on-site parking feasibility from parcel geometry.
    Returns (sub_score 0-100, meta).
    """
    meta: dict[str, Any] = {}
    try:
        row = db.execute(
            text("""
                SELECT area_m2, perimeter_m,
                       (4.0 * 3.14159 * area_m2) / NULLIF(perimeter_m * perimeter_m, 0) AS compactness
                FROM public.riyadh_parcels_arcgis_proxy
                WHERE geom IS NOT NULL
                  AND ST_Contains(
                      geom,
                      ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
                  )
                LIMIT 1
            """),
            {"lat": lat, "lon": lon},
        ).mappings().first()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return 50.0, {"source": "error"}

    if not row:
        return 45.0, {"source": "no_parcel"}

    area = float(row.get("area_m2") or 0)
    perimeter = float(row.get("perimeter_m") or 0)
    compactness = float(row.get("compactness") or 0)
    meta["area_m2"] = area
    meta["perimeter_m"] = perimeter
    meta["compactness"] = round(compactness, 3)

    # Restaurants in Riyadh typically need 200-2000m² for building + parking
    # Larger parcels can accommodate more on-site parking
    if area <= 0:
        return 40.0, meta

    # Area component: steeper sigmoid centered around 500m² to widen score
    # spread across typical Riyadh parcels (200-3000m²).
    # Old curve: 20 + 70*(1-exp(-area/800)) → clustered 40-65 for 200-1200m²
    # New curve: wider range with sharper knee — tiny parcels score low,
    # large parcels score high, mid parcels spread meaningfully.
    if area < 200:
        area_score = 15.0 + 15.0 * (area / 200.0)  # 15-30 for tiny
    elif area < 600:
        area_score = 30.0 + 30.0 * ((area - 200.0) / 400.0)  # 30-60
    elif area < 1500:
        area_score = 60.0 + 20.0 * ((area - 600.0) / 900.0)  # 60-80
    else:
        area_score = min(92.0, 80.0 + 12.0 * (1 - math.exp(-(area - 1500) / 2000.0)))

    # Compactness penalty: elongated/irregular parcels are harder for parking layout
    # Isoperimetric ratio: 1.0 = perfect circle, lower = more irregular
    compact_adj = 0.0
    if compactness > 0:
        if compactness < 0.3:
            compact_adj = -12.0  # very irregular, hard to park
        elif compactness < 0.5:
            compact_adj = -5.0
        elif compactness > 0.7:
            compact_adj = 3.0  # compact parcels bonus for parking layout

    # Frontage estimate from perimeter (rough proxy for access width)
    if perimeter > 0 and area > 0:
        est_frontage = area / (perimeter / 4.0)  # rough width estimate
        meta["est_frontage_m"] = round(est_frontage, 1)
        if est_frontage < 8:
            compact_adj -= 8.0  # very narrow frontage = poor access
        elif est_frontage < 12:
            compact_adj -= 3.0  # narrow frontage
        elif est_frontage > 25:
            compact_adj += 4.0  # wide frontage good for parking entry

    score = max(10.0, min(95.0, area_score + compact_adj))
    meta["source"] = "parcel_geometry"
    return score, meta


def _road_access_and_street_parking(
    db: Session, lat: float, lon: float,
) -> tuple[float, float, dict]:
    """
    Assess road access quality AND estimate on-street parking capacity.

    Returns (access_sub_score 0-100, street_parking_sub_score 0-100, meta).

    The street-parking score is a new discriminative signal derived from the
    same road query — wider/calmer roads offer more curbside parking.
    No extra DB round-trip.
    """
    meta: dict[str, Any] = {}
    global _OSM_ROADS_MISSING
    if _OSM_ROADS_MISSING:
        return 45.0, 30.0, {"source": "no_osm_roads_table"}
    try:
        rows = db.execute(
            text("""
                SELECT highway, name,
                       ST_Distance(
                           ST_Transform(geom, 32638),
                           ST_Transform(ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), 32638)
                       ) AS distance_m
                FROM osm_roads
                WHERE ST_DWithin(
                    geom::geography,
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                    300
                )
                ORDER BY distance_m
                LIMIT 10
            """),
            {"lat": lat, "lon": lon},
        ).mappings().all()
    except Exception as exc:
        if _is_relation_missing(exc):
            _OSM_ROADS_MISSING = True
        try:
            db.rollback()
        except Exception:
            pass
        return 45.0, 30.0, {"source": "error"}

    if not rows:
        return 20.0, 10.0, {"source": "no_roads", "road_count": 0}

    road_classes = set()
    nearest_dist = float(rows[0].get("distance_m", 100))
    meta["road_count"] = len(rows)
    meta["nearest_road_dist_m"] = round(nearest_dist, 1)
    meta["nearest_road_class"] = rows[0].get("highway")

    for r in rows:
        hw = (r.get("highway") or "").lower()
        road_classes.add(hw)

    has_service = "service" in road_classes
    has_secondary = bool(road_classes & {"secondary", "secondary_link", "tertiary", "tertiary_link"})
    has_primary = bool(road_classes & {"primary", "primary_link"})
    has_residential = "residential" in road_classes
    has_motorway = bool(road_classes & {"motorway", "motorway_link", "trunk", "trunk_link"})

    # --- Access score (same logic as before) ---
    access = 40.0
    if has_service:
        access += 20.0
    if has_secondary:
        access += 15.0
    if has_primary:
        access += 10.0
    if has_residential:
        access += 5.0
    if has_motorway and not has_service and not has_secondary:
        access -= 10.0

    if nearest_dist < 20:
        access += 5.0
    elif nearest_dist > 100:
        access -= 10.0

    access = max(10.0, min(95.0, access))

    # --- Street-parking capacity estimate (NEW) ---
    # Different road classes imply different curbside parking potential.
    # Service roads in Riyadh typically allow parallel parking.
    # Secondary/tertiary have occasional parallel parking.
    # Primary roads and highways generally do NOT allow curbside parking.
    # Residential streets have limited curbside space.
    street_parking = 20.0  # baseline: minimal on-street parking

    # Service roads are the best for on-street parking in Riyadh
    service_count = sum(1 for r in rows if (r.get("highway") or "").lower() == "service")
    street_parking += min(30.0, service_count * 15.0)

    # Secondary/tertiary roads offer some curbside opportunity
    secondary_count = sum(
        1 for r in rows
        if (r.get("highway") or "").lower() in {"secondary", "secondary_link", "tertiary", "tertiary_link"}
    )
    street_parking += min(20.0, secondary_count * 8.0)

    # Residential streets have some curbside parking
    residential_count = sum(1 for r in rows if (r.get("highway") or "").lower() == "residential")
    street_parking += min(10.0, residential_count * 5.0)

    # Primary/trunk roads penalize street parking (no stopping zones)
    if has_primary and not has_service:
        street_parking -= 10.0
    if has_motorway:
        street_parking -= 15.0

    # Multiple nearby roads = more total curb space
    if len(rows) >= 4:
        street_parking += 5.0

    street_parking = max(5.0, min(95.0, street_parking))
    meta["street_parking_score"] = round(street_parking, 1)
    meta["source"] = "road_analysis"
    return access, street_parking, meta


def _nearby_parking_supply_consolidated(
    db: Session, lat: float, lon: float,
) -> tuple[float, float, dict]:
    """
    Single consolidated overture_buildings query returning both parking-supply
    score AND building-coverage ratio score.

    Replaces the old 3-query ``_nearby_parking_supply`` with one query that
    categorizes buildings via CASE expressions.

    Returns (supply_sub_score 0-100, coverage_sub_score 0-100, meta).
    """
    meta: dict[str, Any] = {}

    try:
        row = db.execute(
            text("""
                SELECT
                    COUNT(*) FILTER (
                        WHERE (class ILIKE '%%parking%%' OR class ILIKE '%%garage%%')
                    ) AS parking_count,
                    COUNT(*) FILTER (
                        WHERE (class ILIKE '%%mall%%'
                            OR class ILIKE '%%shopping%%'
                            OR class ILIKE '%%retail%%')
                    ) AS mall_count,
                    COUNT(*) AS total_buildings,
                    COALESCE(SUM(ST_Area(geom)), 0) AS total_footprint_m2,
                    COUNT(*) FILTER (
                        WHERE ST_Area(geom) > 2000
                    ) AS large_building_count,
                    COALESCE(SUM(ST_Area(geom)) FILTER (
                        WHERE ST_Area(geom) > 2000
                    ), 0) AS large_footprint_m2
                FROM overture_buildings
                WHERE ST_DWithin(
                    ST_Transform(geom, 4326)::geography,
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                    800
                )
            """),
            {"lat": lat, "lon": lon},
        ).mappings().first()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return 25.0, 40.0, {"source": "error"}

    if not row:
        return 15.0, 20.0, {"source": "no_data"}

    parking_count = int(row.get("parking_count", 0))
    mall_count = int(row.get("mall_count", 0))
    total_buildings = int(row.get("total_buildings", 0))
    total_footprint = float(row.get("total_footprint_m2", 0))
    large_count = int(row.get("large_building_count", 0))
    large_footprint = float(row.get("large_footprint_m2", 0))

    meta["parking_structures_800m"] = parking_count
    meta["malls_800m"] = mall_count
    meta["total_buildings_800m"] = total_buildings
    meta["total_footprint_m2"] = round(total_footprint, 0)
    meta["large_buildings_800m"] = large_count
    meta["large_footprint_m2"] = round(large_footprint, 0)
    meta["source"] = "overture_consolidated"

    # --- Supply score (similar to old logic but from single query) ---
    supply = 20.0  # lower baseline than before for wider spread
    supply += min(30.0, parking_count * 12.0)
    supply += min(25.0, mall_count * 15.0)
    supply += min(15.0, large_count * 3.0)
    supply = max(10.0, min(95.0, supply))

    # --- Building coverage ratio (NEW discriminative signal) ---
    # The ratio of total building footprint to the ~800m-radius area
    # (pi * 800² ≈ 2,010,619 m²) tells us how built-up the neighborhood is.
    # Dense urban = more shared/structured parking. Sparse suburban = less.
    # But we use the Overture footprint in the native 32638 projection which
    # is already in m², so we compare to the circle area.
    neighborhood_area_m2 = math.pi * 800.0 * 800.0  # ~2.01 million m²
    coverage_ratio = total_footprint / neighborhood_area_m2 if neighborhood_area_m2 > 0 else 0
    meta["building_coverage_ratio"] = round(coverage_ratio, 4)

    # High coverage = dense urban → more structured parking, fewer surface lots
    # but overall better parking infrastructure (garages, basement parking).
    # Very low coverage = empty area, probably no parking at all.
    # Sweet spot for restaurant parking: moderate coverage (~0.05-0.15).
    if coverage_ratio < 0.005:
        coverage_score = 12.0  # basically empty
    elif coverage_ratio < 0.02:
        coverage_score = 25.0 + 25.0 * ((coverage_ratio - 0.005) / 0.015)  # 25-50
    elif coverage_ratio < 0.08:
        coverage_score = 50.0 + 35.0 * ((coverage_ratio - 0.02) / 0.06)  # 50-85
    elif coverage_ratio < 0.20:
        coverage_score = 85.0  # dense, well-served
    else:
        # Hyper-dense: parking might actually be constrained
        coverage_score = max(55.0, 85.0 - 60.0 * ((coverage_ratio - 0.20) / 0.30))

    coverage_score = max(10.0, min(95.0, coverage_score))
    meta["coverage_score"] = round(coverage_score, 1)

    return supply, coverage_score, meta


def parking_availability_score(db: Session, lat: float, lon: float) -> ScoredFactor:
    """
    Composite parking suitability score for restaurant site selection.

    Combines (Phase 5 — reweighted for better discrimination):
    - Parcel geometry feasibility (area, compactness, frontage): 25%
    - Road access quality (hierarchy, service roads): 20%
    - Street-parking capacity (from road classes): 15%
    - Nearby parking supply (structures, malls, large buildings): 15%
    - Building coverage ratio (neighborhood density proxy): 25%

    The old 35/35/30 split over-weighted clustered sub-signals. The new
    split introduces street-parking and coverage-ratio signals that vary
    meaningfully across Riyadh and reduce the weighting of the formerly
    dominant (but flat) supply sub-score.
    """
    t0 = time.perf_counter()

    W_PARCEL = 0.25
    W_ACCESS = 0.20
    W_STREET = 0.15
    W_SUPPLY = 0.15
    W_COVERAGE = 0.25

    parcel_score, parcel_meta = _parcel_parking_feasibility(db, lat, lon)
    access_score, street_score, access_meta = _road_access_and_street_parking(db, lat, lon)
    supply_score, coverage_score, supply_meta = _nearby_parking_supply_consolidated(db, lat, lon)

    composite = (
        W_PARCEL * parcel_score
        + W_ACCESS * access_score
        + W_STREET * street_score
        + W_SUPPLY * supply_score
        + W_COVERAGE * coverage_score
    )
    composite = max(10.0, min(95.0, composite))

    # Confidence based on data availability
    has_parcel = parcel_meta.get("source") not in ("error", "no_parcel")
    has_roads = access_meta.get("road_count", 0) > 0
    has_supply_data = (
        supply_meta.get("parking_structures_800m", 0) > 0
        or supply_meta.get("malls_800m", 0) > 0
        or supply_meta.get("large_buildings_800m", 0) > 0
    )
    has_coverage = supply_meta.get("total_buildings_800m", 0) > 0

    evidence_count = sum([has_parcel, has_roads, has_supply_data, has_coverage])
    confidence = {0: 0.15, 1: 0.35, 2: 0.6, 3: 0.8, 4: 0.92}.get(evidence_count, 0.15)

    parts = []
    if has_parcel:
        parts.append(f"parcel={parcel_score:.0f}")
    if has_roads:
        parts.append(f"access={access_score:.0f}")
        parts.append(f"street={street_score:.0f}")
    if has_supply_data:
        parts.append(f"supply={supply_score:.0f}")
    if has_coverage:
        parts.append(f"coverage={coverage_score:.0f}")
    rationale = "composite:" + "+".join(parts) if parts else "no_evidence"

    elapsed_ms = (time.perf_counter() - t0) * 1000
    logger.debug("parking_availability_score: %.1f ms  score=%.1f", elapsed_ms, composite)

    return ScoredFactor(
        score=round(composite, 1),
        confidence=round(confidence, 3),
        rationale=rationale,
        meta={
            "parcel": {"score": round(parcel_score, 1), **parcel_meta},
            "access": {"score": round(access_score, 1), **access_meta},
            "street_parking": {"score": round(street_score, 1)},
            "supply": {"score": round(supply_score, 1), **supply_meta},
            "coverage": {"score": round(coverage_score, 1)},
            "weights": {
                "parcel": W_PARCEL,
                "access": W_ACCESS,
                "street_parking": W_STREET,
                "supply": W_SUPPLY,
                "coverage": W_COVERAGE,
            },
            "elapsed_ms": round(elapsed_ms, 1),
        },
    )


# ---------------------------------------------------------------------------
# COMMERCIAL DENSITY — Phase 4
# ---------------------------------------------------------------------------

# Category weights for demand anchor scoring
_ANCHOR_WEIGHTS: dict[str, float] = {
    "mall": 4.0,
    "shopping": 3.5,
    "supermarket": 3.0,
    "hypermarket": 3.0,
    "cinema": 2.5,
    "office": 2.0,
    "hotel": 2.5,
    "hospital": 2.0,
    "clinic": 1.5,
    "school": 1.5,
    "university": 2.5,
    "college": 2.0,
    "fuel": 1.0,
    "gas_station": 1.0,
    "petrol": 1.0,
    "gym": 1.5,
    "fitness": 1.5,
    "bank": 1.0,
    "government": 1.5,
}


def _nearby_building_intensity(db: Session, lat: float, lon: float) -> tuple[float, dict]:
    """
    Non-residential building intensity — weighted by footprint area, not just count.
    Returns (sub_score 0-100, meta).
    """
    meta: dict[str, Any] = {}
    try:
        row = db.execute(
            text("""
                SELECT
                    COUNT(*) AS total_buildings,
                    COALESCE(SUM(ST_Area(ST_Transform(geom, 32638))), 0) AS total_footprint_m2,
                    COUNT(*) FILTER (
                        WHERE class NOT ILIKE '%%residential%%'
                          AND class NOT ILIKE '%%house%%'
                          AND class NOT ILIKE '%%apartment%%'
                    ) AS non_residential_count,
                    COALESCE(SUM(ST_Area(ST_Transform(geom, 32638))) FILTER (
                        WHERE class NOT ILIKE '%%residential%%'
                          AND class NOT ILIKE '%%house%%'
                          AND class NOT ILIKE '%%apartment%%'
                    ), 0) AS non_residential_footprint_m2
                FROM overture_buildings
                WHERE ST_DWithin(
                    ST_Transform(geom, 4326)::geography,
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                    500
                )
            """),
            {"lat": lat, "lon": lon},
        ).mappings().first()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return 40.0, {"source": "error"}

    if not row:
        return 10.0, {"source": "no_data"}

    total = int(row.get("total_buildings", 0))
    non_res = int(row.get("non_residential_count", 0))
    non_res_fp = float(row.get("non_residential_footprint_m2", 0))
    meta["total_buildings_500m"] = total
    meta["non_residential_count"] = non_res
    meta["non_residential_footprint_m2"] = round(non_res_fp, 0)
    meta["source"] = "overture_buildings"

    if total == 0:
        return 10.0, meta

    # Weighted score: combine count and footprint signals
    # Count: sigmoid centered at 30 non-residential buildings
    count_score = min(90.0, 10.0 + 80.0 * (1 - math.exp(-non_res / 30.0)))
    # Footprint: sigmoid centered at 20000 m² total non-residential footprint
    footprint_score = min(90.0, 10.0 + 80.0 * (1 - math.exp(-non_res_fp / 20000.0)))
    # Combine 40% count + 60% footprint (footprint is a richer signal)
    score = 0.4 * count_score + 0.6 * footprint_score

    return max(10.0, min(95.0, score)), meta


def _demand_anchor_score(db: Session, lat: float, lon: float) -> tuple[float, dict]:
    """
    Score based on proximity to demand anchors (malls, offices, universities, etc.).
    Uses overture_buildings (building class) and planet_osm_polygon (amenity/shop).
    Returns (sub_score 0-100, meta).
    """
    meta: dict[str, Any] = {}
    weighted_total = 0.0
    anchor_hits: dict[str, int] = {}

    # 1. Overture buildings — commercial building classes (mall, office, hotel, etc.)
    try:
        rows = db.execute(
            text("""
                SELECT class, subtype FROM overture_buildings
                WHERE class IS NOT NULL
                  AND ST_DWithin(
                      ST_Transform(geom, 4326)::geography,
                      ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                      1000
                  )
            """),
            {"lat": lat, "lon": lon},
        ).mappings().all()

        for r in rows:
            cls = (r.get("class") or "").lower()
            subtype = (r.get("subtype") or "").lower()
            combined = f"{cls} {subtype}"
            for keyword, weight in _ANCHOR_WEIGHTS.items():
                if keyword in combined:
                    weighted_total += weight
                    anchor_hits[keyword] = anchor_hits.get(keyword, 0) + 1
                    break  # only count each building once
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass

    # 2. OSM amenities/shops from planet_osm_polygon (schools, hospitals, malls)
    try:
        rows = db.execute(
            text("""
                SELECT amenity, shop, name FROM planet_osm_polygon
                WHERE (amenity IS NOT NULL OR shop IS NOT NULL)
                  AND ST_DWithin(
                      ST_Transform(way, 4326)::geography,
                      ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                      800
                  )
            """),
            {"lat": lat, "lon": lon},
        ).mappings().all()

        for r in rows:
            amenity = (r.get("amenity") or "").lower()
            shop = (r.get("shop") or "").lower()
            name = (r.get("name") or "").lower()
            combined = f"{amenity} {shop} {name}"
            for keyword, weight in _ANCHOR_WEIGHTS.items():
                if keyword in combined:
                    weighted_total += weight
                    anchor_hits[keyword] = anchor_hits.get(keyword, 0) + 1
                    break
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass

    meta["anchor_hits"] = anchor_hits
    meta["weighted_total"] = round(weighted_total, 1)
    meta["source"] = "demand_anchors"

    # Sigmoid: saturates around weighted_total=25
    score = min(95.0, 10.0 + 85.0 * (1 - math.exp(-weighted_total / 20.0)))
    return max(10.0, score), meta


def _nearby_commercial_parcels(db: Session, lat: float, lon: float) -> tuple[float, dict]:
    """
    Score based on share of nearby ArcGIS parcels that are commercial/mixed-use.
    Returns (sub_score 0-100, meta).
    """
    meta: dict[str, Any] = {}
    try:
        row = db.execute(
            text("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (
                        WHERE landuse_label ILIKE '%%تجاري%%'
                           OR landuse_label ILIKE '%%commercial%%'
                           OR landuse_label ILIKE '%%مختلط%%'
                           OR landuse_label ILIKE '%%mixed%%'
                           OR landuse_label ILIKE '%%retail%%'
                           OR landuse_label ILIKE '%%محلات%%'
                           OR landuse_label ILIKE '%%استثماري%%'
                           OR landuse_label ILIKE '%%investment%%'
                           OR landuse_label ILIKE '%%office%%'
                           OR landuse_label ILIKE '%%مكاتب%%'
                    ) AS commercial_count
                FROM public.riyadh_parcels_arcgis_proxy
                WHERE ST_DWithin(
                    geom::geography,
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                    500
                )
            """),
            {"lat": lat, "lon": lon},
        ).mappings().first()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return 40.0, {"source": "error"}

    if not row:
        return 30.0, {"source": "no_data"}

    total = int(row.get("total", 0))
    commercial = int(row.get("commercial_count", 0))
    meta["nearby_parcels_total"] = total
    meta["nearby_parcels_commercial"] = commercial
    meta["source"] = "arcgis_parcels"

    if total == 0:
        return 30.0, meta

    ratio = commercial / total
    meta["commercial_ratio"] = round(ratio, 3)

    # Score: 30 baseline + up to 60 points based on commercial ratio
    score = 30.0 + 60.0 * min(1.0, ratio / 0.5)  # saturates at 50% commercial
    return max(10.0, min(95.0, score)), meta


def _nearby_poi_ecosystem(db: Session, lat: float, lon: float) -> tuple[float, dict]:
    """
    Score based on non-restaurant POI density — footfall generators.
    Returns (sub_score 0-100, meta).
    """
    meta: dict[str, Any] = {}
    try:
        row = db.execute(
            text("""
                SELECT
                    COUNT(*) AS total_pois,
                    COUNT(*) FILTER (
                        WHERE source != 'overture'
                          AND category NOT IN (
                              SELECT UNNEST(ARRAY[
                                  'burger', 'pizza', 'coffee', 'cafe', 'bakery',
                                  'shawarma', 'chicken', 'seafood', 'asian',
                                  'indian', 'italian', 'steak', 'sushi',
                                  'healthy', 'dessert', 'juice', 'ice_cream',
                                  'middle_eastern', 'fast_food', 'arabic',
                                  'breakfast', 'sandwich', 'turkish'
                              ])
                          )
                    ) AS non_restaurant_pois,
                    COUNT(DISTINCT source) AS source_diversity
                FROM restaurant_poi
                WHERE geom IS NOT NULL
                  AND ST_DWithin(
                      ST_Transform(geom, 4326)::geography,
                      ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                      800
                  )
            """),
            {"lat": lat, "lon": lon},
        ).mappings().first()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return 35.0, {"source": "error"}

    if not row:
        return 15.0, {"source": "no_data"}

    total = int(row.get("total_pois", 0))
    non_rest = int(row.get("non_restaurant_pois", 0))
    diversity = int(row.get("source_diversity", 0))
    meta["total_pois_800m"] = total
    meta["non_restaurant_pois"] = non_rest
    meta["source_diversity"] = diversity
    meta["source"] = "poi_ecosystem"

    # Even total POI density (including restaurants) signals commercial activity
    score = min(90.0, 15.0 + 75.0 * (1 - math.exp(-total / 40.0)))

    # Small bonus for source diversity (richer data = more confidence)
    if diversity >= 3:
        score += 5.0

    return max(10.0, min(95.0, score)), meta


def commercial_density_score(db: Session, lat: float, lon: float, radius_m: float = 500) -> ScoredFactor:
    """
    Upgraded commercial density score using multiple weighted signals.

    Combines:
    - Non-residential building intensity (count + footprint)
    - Demand anchor proximity (malls, offices, schools, etc.)
    - Commercial parcel ratio from ArcGIS
    - POI ecosystem density

    Weight rationale:
    - Buildings (30%): physical commercial infrastructure
    - Anchors (25%): specific trip generators
    - Commercial parcels (25%): zoning/land-use context
    - POI ecosystem (20%): observed commercial activity
    """
    t0 = time.perf_counter()

    W_BUILDINGS = 0.30
    W_ANCHORS = 0.25
    W_PARCELS = 0.25
    W_POI = 0.20

    bld_score, bld_meta = _nearby_building_intensity(db, lat, lon)
    anc_score, anc_meta = _demand_anchor_score(db, lat, lon)
    prc_score, prc_meta = _nearby_commercial_parcels(db, lat, lon)
    poi_score, poi_meta = _nearby_poi_ecosystem(db, lat, lon)

    composite = (
        W_BUILDINGS * bld_score
        + W_ANCHORS * anc_score
        + W_PARCELS * prc_score
        + W_POI * poi_score
    )
    composite = max(10.0, min(95.0, composite))

    # Confidence: based on which sub-signals had real data
    has_buildings = bld_meta.get("source") not in ("error", "no_data")
    has_anchors = anc_meta.get("weighted_total", 0) > 0
    has_parcels = prc_meta.get("source") not in ("error", "no_data")
    has_pois = poi_meta.get("source") not in ("error", "no_data")

    evidence_count = sum([has_buildings, has_anchors, has_parcels, has_pois])
    confidence = {0: 0.1, 1: 0.35, 2: 0.6, 3: 0.8, 4: 0.95}.get(evidence_count, 0.1)

    parts = []
    if has_buildings:
        parts.append(f"bld={bld_score:.0f}")
    if has_anchors:
        parts.append(f"anc={anc_score:.0f}")
    if has_parcels:
        parts.append(f"prc={prc_score:.0f}")
    if has_pois:
        parts.append(f"poi={poi_score:.0f}")
    rationale = "composite:" + "+".join(parts) if parts else "no_evidence"

    elapsed_ms = (time.perf_counter() - t0) * 1000
    logger.debug("commercial_density_score: %.1f ms  score=%.1f", elapsed_ms, composite)

    return ScoredFactor(
        score=round(composite, 1),
        confidence=round(confidence, 3),
        rationale=rationale,
        meta={
            "buildings": {"score": round(bld_score, 1), **bld_meta},
            "anchors": {"score": round(anc_score, 1), **anc_meta},
            "parcels": {"score": round(prc_score, 1), **prc_meta},
            "poi_ecosystem": {"score": round(poi_score, 1), **poi_meta},
            "weights": {
                "buildings": W_BUILDINGS,
                "anchors": W_ANCHORS,
                "parcels": W_PARCELS,
                "poi_ecosystem": W_POI,
            },
            "elapsed_ms": round(elapsed_ms, 1),
        },
    )


# ---------------------------------------------------------------------------
# BATCH FACTOR COMPUTATION — consolidated queries for parcel-click perf
# ---------------------------------------------------------------------------


@dataclass
class BatchFactorsResult:
    """All three upgraded factors plus pre-fetched context for reuse."""

    zoning: ScoredFactor
    parking: ScoredFactor
    commercial_density: ScoredFactor
    # Shared context reused by score_location for traffic / anchor factors
    roads_300m: list[dict]
    overture_commercial_1500: int
    osm_amenity_count_1000: int
    overture_anchor_weighted_1000: float
    osm_anchor_weighted_800: float
    timing: dict  # per-query elapsed_ms


# ---- Consolidated query helpers ----


def _batch_fetch_roads(db: Session, lat: float, lon: float) -> list[dict]:
    """Fetch all roads within 300m (widest radius used). Single query."""
    global _OSM_ROADS_MISSING
    if _OSM_ROADS_MISSING:
        return []
    try:
        rows = db.execute(
            text("""
                SELECT highway, name,
                       ST_Distance(
                           ST_Transform(geom, 32638),
                           ST_Transform(ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), 32638)
                       ) AS distance_m
                FROM osm_roads
                WHERE ST_DWithin(
                    geom::geography,
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                    300
                )
                ORDER BY distance_m
                LIMIT 15
            """),
            {"lat": lat, "lon": lon},
        ).mappings().all()
        return [dict(r) for r in rows]
    except Exception as exc:
        if _is_relation_missing(exc):
            _OSM_ROADS_MISSING = True
        try:
            db.rollback()
        except Exception:
            pass
        return []


_BATCH_BUILDINGS_SQL = text("""
    WITH nb AS (
        SELECT class, subtype,
               ST_Area(ST_Transform(geom, 32638)) AS area_m2,
               ST_Distance(
                   ST_Transform(geom, 4326)::geography,
                   ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography
               ) AS dist_m
        FROM overture_buildings
        WHERE ST_DWithin(
            ST_Transform(geom, 4326)::geography,
            ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
            1500
        )
    )
    SELECT
        -- 500m band: building intensity
        COUNT(*) FILTER (WHERE dist_m <= 500) AS total_bld_500,
        COALESCE(SUM(area_m2) FILTER (WHERE dist_m <= 500), 0) AS total_fp_500,
        COUNT(*) FILTER (WHERE dist_m <= 500
            AND LOWER(COALESCE(class,'')) NOT LIKE '%%residential%%'
            AND LOWER(COALESCE(class,'')) NOT LIKE '%%house%%'
            AND LOWER(COALESCE(class,'')) NOT LIKE '%%apartment%%'
        ) AS non_res_cnt_500,
        COALESCE(SUM(area_m2) FILTER (WHERE dist_m <= 500
            AND LOWER(COALESCE(class,'')) NOT LIKE '%%residential%%'
            AND LOWER(COALESCE(class,'')) NOT LIKE '%%house%%'
            AND LOWER(COALESCE(class,'')) NOT LIKE '%%apartment%%'
        ), 0) AS non_res_fp_500,

        -- 800m band: parking supply + coverage
        COUNT(*) FILTER (WHERE dist_m <= 800
            AND (LOWER(COALESCE(class,'')) LIKE '%%parking%%'
                 OR LOWER(COALESCE(class,'')) LIKE '%%garage%%')
        ) AS parking_cnt_800,
        COUNT(*) FILTER (WHERE dist_m <= 800
            AND (LOWER(COALESCE(class,'')) LIKE '%%mall%%'
                 OR LOWER(COALESCE(class,'')) LIKE '%%shopping%%'
                 OR LOWER(COALESCE(class,'')) LIKE '%%retail%%')
        ) AS mall_cnt_800,
        COUNT(*) FILTER (WHERE dist_m <= 800) AS total_bld_800,
        COALESCE(SUM(area_m2) FILTER (WHERE dist_m <= 800), 0) AS total_fp_800,
        COUNT(*) FILTER (WHERE dist_m <= 800 AND area_m2 > 2000) AS large_cnt_800,
        COALESCE(SUM(area_m2) FILTER (WHERE dist_m <= 800 AND area_m2 > 2000), 0) AS large_fp_800,

        -- 1000m band: demand anchor weighted score (CASE priority matches _ANCHOR_WEIGHTS)
        COALESCE(SUM(
            CASE
                WHEN dist_m > 1000 THEN 0
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%mall%%' THEN 4.0
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%shopping%%' THEN 3.5
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%supermarket%%' THEN 3.0
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%hypermarket%%' THEN 3.0
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%cinema%%' THEN 2.5
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%hotel%%' THEN 2.5
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%university%%' THEN 2.5
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%college%%' THEN 2.0
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%office%%' THEN 2.0
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%hospital%%' THEN 2.0
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%clinic%%' THEN 1.5
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%school%%' THEN 1.5
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%gym%%' THEN 1.5
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%fitness%%' THEN 1.5
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%government%%' THEN 1.5
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%bank%%' THEN 1.0
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%fuel%%' THEN 1.0
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%gas_station%%' THEN 1.0
                WHEN LOWER(COALESCE(class,'') || ' ' || COALESCE(subtype,'')) LIKE '%%petrol%%' THEN 1.0
                ELSE 0
            END
        ), 0) AS anchor_weighted_1000,

        -- 1500m band: commercial anchor count for anchor_proximity
        COUNT(*) FILTER (WHERE dist_m <= 1500
            AND (LOWER(COALESCE(class,'')) LIKE '%%mall%%'
                 OR LOWER(COALESCE(class,'')) LIKE '%%shopping%%'
                 OR LOWER(COALESCE(class,'')) LIKE '%%retail%%'
                 OR LOWER(COALESCE(class,'')) LIKE '%%commercial%%')
        ) AS commercial_anchors_1500
    FROM nb
""")


def _batch_fetch_buildings(db: Session, lat: float, lon: float) -> dict:
    """Fetch consolidated building aggregates at 500/800/1000/1500m bands."""
    try:
        row = db.execute(
            _BATCH_BUILDINGS_SQL, {"lat": lat, "lon": lon},
        ).mappings().first()
        return dict(row) if row else {}
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return {}


def _batch_fetch_osm_amenities(db: Session, lat: float, lon: float) -> list[dict]:
    """Fetch OSM amenity/shop features within 1000m. Single query."""
    try:
        rows = db.execute(
            text("""
                SELECT amenity, shop, name,
                       ST_Distance(
                           ST_Transform(way, 4326)::geography,
                           ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography
                       ) AS dist_m
                FROM planet_osm_polygon
                WHERE (amenity IS NOT NULL OR shop IS NOT NULL)
                  AND ST_DWithin(
                      ST_Transform(way, 4326)::geography,
                      ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                      1000
                  )
            """),
            {"lat": lat, "lon": lon},
        ).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return []


def _batch_fetch_parcel_at_point(db: Session, lat: float, lon: float) -> dict | None:
    """Fetch ArcGIS parcel containing the point (ST_Contains). Single query.
    Also returns compactness for parking feasibility."""
    try:
        row = db.execute(
            text("""
                SELECT landuse_label, landuse_code, area_m2, perimeter_m,
                       (4.0 * 3.14159 * area_m2)
                           / NULLIF(perimeter_m * perimeter_m, 0) AS compactness
                FROM public.riyadh_parcels_arcgis_proxy
                WHERE geom IS NOT NULL
                  AND ST_Contains(
                      geom,
                      ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
                  )
                LIMIT 1
            """),
            {"lat": lat, "lon": lon},
        ).mappings().first()
        return dict(row) if row else None
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return None


def _batch_fetch_parcels_nearby(db: Session, lat: float, lon: float) -> dict:
    """Fetch aggregated ArcGIS parcel stats within 500m. Single query.
    Returns total count, commercial count."""
    try:
        row = db.execute(
            text("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (
                        WHERE landuse_label ILIKE '%%تجاري%%'
                           OR landuse_label ILIKE '%%commercial%%'
                           OR landuse_label ILIKE '%%مختلط%%'
                           OR landuse_label ILIKE '%%mixed%%'
                           OR landuse_label ILIKE '%%retail%%'
                           OR landuse_label ILIKE '%%محلات%%'
                           OR landuse_label ILIKE '%%استثماري%%'
                           OR landuse_label ILIKE '%%investment%%'
                           OR landuse_label ILIKE '%%office%%'
                           OR landuse_label ILIKE '%%مكاتب%%'
                    ) AS commercial_count
                FROM public.riyadh_parcels_arcgis_proxy
                WHERE ST_DWithin(
                    geom::geography,
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                    500
                )
            """),
            {"lat": lat, "lon": lon},
        ).mappings().first()
        return dict(row) if row else {"total": 0, "commercial_count": 0}
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return {"total": 0, "commercial_count": 0}


# ---- Score computation from pre-fetched context ----


def _compute_osm_anchor_weighted(
    osm_rows: list[dict], max_dist: float,
) -> dict:
    """Compute weighted anchor sum from OSM amenity rows."""
    weighted_total = 0.0
    anchor_hits: dict[str, int] = {}
    for r in osm_rows:
        if float(r.get("dist_m", 9999)) > max_dist:
            continue
        amenity = (r.get("amenity") or "").lower()
        shop = (r.get("shop") or "").lower()
        name = (r.get("name") or "").lower()
        combined = f"{amenity} {shop} {name}"
        for keyword, weight in _ANCHOR_WEIGHTS.items():
            if keyword in combined:
                weighted_total += weight
                anchor_hits[keyword] = anchor_hits.get(keyword, 0) + 1
                break
    return {"weighted_total": weighted_total, "anchor_hits": anchor_hits}


def _zoning_from_context(
    parcel: dict | None,
    parcels_nearby: dict,
    roads: list[dict],
) -> ScoredFactor:
    """Compute zoning_fit score from pre-fetched data (no DB queries)."""
    score = None
    confidence = 0.0
    rationale = "no_data"
    meta: dict[str, Any] = {}

    # Step 1: Direct parcel lookup
    if parcel:
        label = parcel.get("landuse_label")
        code = parcel.get("landuse_code")
        area = parcel.get("area_m2")
        meta["arcgis_label"] = label
        meta["arcgis_code"] = code
        meta["parcel_area_m2"] = area

        match = _match_landuse_label(str(label)) if label else None
        if match:
            score, rationale = match
            confidence = 0.9
            meta["match_source"] = "arcgis_label"
        elif code is not None:
            code_match = _ZONING_CODE_MAP.get(int(code))
            if code_match:
                score, rationale = code_match
                confidence = 0.8
                meta["match_source"] = "arcgis_code"
            else:
                score = 55.0
                rationale = "unknown_code"
                confidence = 0.5
                meta["match_source"] = "arcgis_unknown_code"
        else:
            score = 50.0
            rationale = "parcel_no_landuse"
            confidence = 0.3
            meta["match_source"] = "arcgis_empty"

    # Step 2: District-level fallback from nearby parcels
    if score is None:
        total = int(parcels_nearby.get("total", 0))
        commercial = int(parcels_nearby.get("commercial_count", 0))
        if total > 0:
            commercial_ratio = commercial / total
            meta["district_parcels_total"] = total
            meta["district_parcels_commercial"] = commercial
            score = 30.0 + 55.0 * commercial_ratio
            rationale = "district_fallback"
            confidence = min(0.6, 0.15 + 0.05 * total)
            meta["match_source"] = "district_nearby"
            meta["commercial_ratio"] = round(commercial_ratio, 3)

    # Step 3: Final default
    if score is None:
        score = 45.0
        rationale = "no_data_default"
        confidence = 0.1
        meta["match_source"] = "default"

    # Step 4: Road adjacency bonus (from pre-fetched roads)
    road_bonus = _road_bonus_from_context(roads)
    meta["road_bonus"] = round(road_bonus, 1)
    score = max(10.0, min(95.0, score + road_bonus))

    return ScoredFactor(
        score=round(score, 1),
        confidence=round(confidence, 3),
        rationale=rationale,
        meta=meta,
    )


def _road_bonus_from_context(roads: list[dict]) -> float:
    """Compute road adjacency bonus from pre-fetched roads within 150m."""
    nearby = [r for r in roads if float(r.get("distance_m", 999)) <= 150]
    if not nearby:
        return -3.0

    road_classes = {(r.get("highway") or "").lower() for r in nearby}
    nearest_dist = float(nearby[0].get("distance_m", 100))

    has_primary = bool(road_classes & {"primary", "primary_link", "secondary", "secondary_link"})
    has_service = "service" in road_classes
    has_motorway = bool(road_classes & {"motorway", "motorway_link", "trunk", "trunk_link"})

    bonus = 0.0
    if has_primary and nearest_dist < 50:
        bonus += 6.0
    elif has_primary:
        bonus += 3.0
    if has_service:
        bonus += 2.0
    if has_motorway and not has_primary and not has_service:
        bonus -= 3.0

    return max(-5.0, min(8.0, bonus))


def _parking_from_context(
    parcel: dict | None, roads: list[dict], bld: dict,
) -> ScoredFactor:
    """Compute parking score from pre-fetched data (no DB queries)."""
    W_PARCEL = 0.25
    W_ACCESS = 0.20
    W_STREET = 0.15
    W_SUPPLY = 0.15
    W_COVERAGE = 0.25

    parcel_score, parcel_meta = _parcel_feasibility_from_context(parcel)
    access_score, street_score, access_meta = _road_access_from_context(roads)
    supply_score, coverage_score, supply_meta = _parking_supply_from_context(bld)

    composite = (
        W_PARCEL * parcel_score
        + W_ACCESS * access_score
        + W_STREET * street_score
        + W_SUPPLY * supply_score
        + W_COVERAGE * coverage_score
    )
    composite = max(10.0, min(95.0, composite))

    has_parcel = parcel_meta.get("source") not in ("error", "no_parcel")
    has_roads = access_meta.get("road_count", 0) > 0
    has_supply_data = (
        supply_meta.get("parking_structures_800m", 0) > 0
        or supply_meta.get("malls_800m", 0) > 0
        or supply_meta.get("large_buildings_800m", 0) > 0
    )
    has_coverage = supply_meta.get("total_buildings_800m", 0) > 0

    evidence_count = sum([has_parcel, has_roads, has_supply_data, has_coverage])
    confidence = {0: 0.15, 1: 0.35, 2: 0.6, 3: 0.8, 4: 0.92}.get(evidence_count, 0.15)

    parts = []
    if has_parcel:
        parts.append(f"parcel={parcel_score:.0f}")
    if has_roads:
        parts.append(f"access={access_score:.0f}")
        parts.append(f"street={street_score:.0f}")
    if has_supply_data:
        parts.append(f"supply={supply_score:.0f}")
    if has_coverage:
        parts.append(f"coverage={coverage_score:.0f}")
    rationale = "composite:" + "+".join(parts) if parts else "no_evidence"

    return ScoredFactor(
        score=round(composite, 1),
        confidence=round(confidence, 3),
        rationale=rationale,
        meta={
            "parcel": {"score": round(parcel_score, 1), **parcel_meta},
            "access": {"score": round(access_score, 1), **access_meta},
            "street_parking": {"score": round(street_score, 1)},
            "supply": {"score": round(supply_score, 1), **supply_meta},
            "coverage": {"score": round(coverage_score, 1)},
            "weights": {
                "parcel": W_PARCEL, "access": W_ACCESS,
                "street_parking": W_STREET, "supply": W_SUPPLY,
                "coverage": W_COVERAGE,
            },
        },
    )


def _parcel_feasibility_from_context(parcel: dict | None) -> tuple[float, dict]:
    """Parcel parking feasibility from pre-fetched parcel data."""
    if not parcel:
        return 45.0, {"source": "no_parcel"}

    area = float(parcel.get("area_m2") or 0)
    perimeter = float(parcel.get("perimeter_m") or 0)
    compactness = float(parcel.get("compactness") or 0)
    meta: dict[str, Any] = {
        "area_m2": area,
        "perimeter_m": perimeter,
        "compactness": round(compactness, 3),
    }

    if area <= 0:
        return 40.0, meta

    if area < 200:
        area_score = 15.0 + 15.0 * (area / 200.0)
    elif area < 600:
        area_score = 30.0 + 30.0 * ((area - 200.0) / 400.0)
    elif area < 1500:
        area_score = 60.0 + 20.0 * ((area - 600.0) / 900.0)
    else:
        area_score = min(92.0, 80.0 + 12.0 * (1 - math.exp(-(area - 1500) / 2000.0)))

    compact_adj = 0.0
    if compactness > 0:
        if compactness < 0.3:
            compact_adj = -12.0
        elif compactness < 0.5:
            compact_adj = -5.0
        elif compactness > 0.7:
            compact_adj = 3.0

    if perimeter > 0 and area > 0:
        est_frontage = area / (perimeter / 4.0)
        meta["est_frontage_m"] = round(est_frontage, 1)
        if est_frontage < 8:
            compact_adj -= 8.0
        elif est_frontage < 12:
            compact_adj -= 3.0
        elif est_frontage > 25:
            compact_adj += 4.0

    score = max(10.0, min(95.0, area_score + compact_adj))
    meta["source"] = "parcel_geometry"
    return score, meta


def _road_access_from_context(
    roads: list[dict],
) -> tuple[float, float, dict]:
    """Road access + street parking from pre-fetched roads within 300m."""
    meta: dict[str, Any] = {}
    if not roads:
        return 20.0, 10.0, {"source": "no_roads", "road_count": 0}

    road_classes: set[str] = set()
    nearest_dist = float(roads[0].get("distance_m", 100))
    meta["road_count"] = len(roads)
    meta["nearest_road_dist_m"] = round(nearest_dist, 1)
    meta["nearest_road_class"] = roads[0].get("highway")

    for r in roads:
        hw = (r.get("highway") or "").lower()
        road_classes.add(hw)

    has_service = "service" in road_classes
    has_secondary = bool(road_classes & {"secondary", "secondary_link", "tertiary", "tertiary_link"})
    has_primary = bool(road_classes & {"primary", "primary_link"})
    has_residential = "residential" in road_classes
    has_motorway = bool(road_classes & {"motorway", "motorway_link", "trunk", "trunk_link"})

    access = 40.0
    if has_service:
        access += 20.0
    if has_secondary:
        access += 15.0
    if has_primary:
        access += 10.0
    if has_residential:
        access += 5.0
    if has_motorway and not has_service and not has_secondary:
        access -= 10.0
    if nearest_dist < 20:
        access += 5.0
    elif nearest_dist > 100:
        access -= 10.0
    access = max(10.0, min(95.0, access))

    street_parking = 20.0
    service_count = sum(1 for r in roads if (r.get("highway") or "").lower() == "service")
    street_parking += min(30.0, service_count * 15.0)
    secondary_count = sum(
        1 for r in roads
        if (r.get("highway") or "").lower() in {"secondary", "secondary_link", "tertiary", "tertiary_link"}
    )
    street_parking += min(20.0, secondary_count * 8.0)
    residential_count = sum(1 for r in roads if (r.get("highway") or "").lower() == "residential")
    street_parking += min(10.0, residential_count * 5.0)
    if has_primary and not has_service:
        street_parking -= 10.0
    if has_motorway:
        street_parking -= 15.0
    if len(roads) >= 4:
        street_parking += 5.0
    street_parking = max(5.0, min(95.0, street_parking))

    meta["street_parking_score"] = round(street_parking, 1)
    meta["source"] = "road_analysis"
    return access, street_parking, meta


def _parking_supply_from_context(bld: dict) -> tuple[float, float, dict]:
    """Parking supply + coverage from pre-fetched building aggregates at 800m band."""
    meta: dict[str, Any] = {}
    if not bld:
        return 15.0, 20.0, {"source": "no_data"}

    parking_count = int(bld.get("parking_cnt_800", 0))
    mall_count = int(bld.get("mall_cnt_800", 0))
    total_buildings = int(bld.get("total_bld_800", 0))
    total_footprint = float(bld.get("total_fp_800", 0))
    large_count = int(bld.get("large_cnt_800", 0))

    meta["parking_structures_800m"] = parking_count
    meta["malls_800m"] = mall_count
    meta["total_buildings_800m"] = total_buildings
    meta["total_footprint_m2"] = round(total_footprint, 0)
    meta["large_buildings_800m"] = large_count
    meta["source"] = "overture_consolidated"

    supply = 20.0
    supply += min(30.0, parking_count * 12.0)
    supply += min(25.0, mall_count * 15.0)
    supply += min(15.0, large_count * 3.0)
    supply = max(10.0, min(95.0, supply))

    neighborhood_area_m2 = math.pi * 800.0 * 800.0
    coverage_ratio = total_footprint / neighborhood_area_m2 if neighborhood_area_m2 > 0 else 0
    meta["building_coverage_ratio"] = round(coverage_ratio, 4)

    if coverage_ratio < 0.005:
        coverage_score = 12.0
    elif coverage_ratio < 0.02:
        coverage_score = 25.0 + 25.0 * ((coverage_ratio - 0.005) / 0.015)
    elif coverage_ratio < 0.08:
        coverage_score = 50.0 + 35.0 * ((coverage_ratio - 0.02) / 0.06)
    elif coverage_ratio < 0.20:
        coverage_score = 85.0
    else:
        coverage_score = max(55.0, 85.0 - 60.0 * ((coverage_ratio - 0.20) / 0.30))

    coverage_score = max(10.0, min(95.0, coverage_score))
    meta["coverage_score"] = round(coverage_score, 1)
    return supply, coverage_score, meta


def _commercial_density_from_context(
    bld: dict,
    osm_rows: list[dict],
    parcels_nearby: dict,
    osm_anchor: dict,
    poi_sub_score: float = 50.0,
) -> ScoredFactor:
    """Compute commercial density from pre-fetched data.

    ``poi_sub_score`` is computed separately by the caller from
    the nearby-restaurants result set (avoids an extra query).
    """
    W_BUILDINGS = 0.30
    W_ANCHORS = 0.25
    W_PARCELS = 0.25
    W_POI = 0.20

    bld_score, bld_meta = _building_intensity_from_context(bld)

    overture_anchor_weighted = float(bld.get("anchor_weighted_1000", 0))
    total_anchor_weighted = overture_anchor_weighted + osm_anchor.get("weighted_total", 0)
    anc_score = min(95.0, 10.0 + 85.0 * (1 - math.exp(-total_anchor_weighted / 20.0)))
    anc_score = max(10.0, anc_score)
    anc_meta = {
        "weighted_total": round(total_anchor_weighted, 1),
        "anchor_hits": osm_anchor.get("anchor_hits", {}),
        "source": "demand_anchors",
    }

    prc_score, prc_meta = _commercial_parcels_from_context(parcels_nearby)

    poi_meta = {"source": "poi_ecosystem_from_nearby"}

    composite = (
        W_BUILDINGS * bld_score
        + W_ANCHORS * anc_score
        + W_PARCELS * prc_score
        + W_POI * poi_sub_score
    )
    composite = max(10.0, min(95.0, composite))

    has_buildings = bld_meta.get("source") not in ("error", "no_data")
    has_anchors = total_anchor_weighted > 0
    has_parcels = prc_meta.get("source") not in ("error", "no_data")
    has_pois = poi_sub_score > 15.0

    evidence_count = sum([has_buildings, has_anchors, has_parcels, has_pois])
    confidence = {0: 0.1, 1: 0.35, 2: 0.6, 3: 0.8, 4: 0.95}.get(evidence_count, 0.1)

    parts = []
    if has_buildings:
        parts.append(f"bld={bld_score:.0f}")
    if has_anchors:
        parts.append(f"anc={anc_score:.0f}")
    if has_parcels:
        parts.append(f"prc={prc_score:.0f}")
    if has_pois:
        parts.append(f"poi={poi_sub_score:.0f}")
    rationale = "composite:" + "+".join(parts) if parts else "no_evidence"

    return ScoredFactor(
        score=round(composite, 1),
        confidence=round(confidence, 3),
        rationale=rationale,
        meta={
            "buildings": {"score": round(bld_score, 1), **bld_meta},
            "anchors": {"score": round(anc_score, 1), **anc_meta},
            "parcels": {"score": round(prc_score, 1), **prc_meta},
            "poi_ecosystem": {"score": round(poi_sub_score, 1), **poi_meta},
            "weights": {
                "buildings": W_BUILDINGS, "anchors": W_ANCHORS,
                "parcels": W_PARCELS, "poi_ecosystem": W_POI,
            },
        },
    )


def _building_intensity_from_context(bld: dict) -> tuple[float, dict]:
    """Building intensity from pre-fetched 500m band aggregates."""
    if not bld:
        return 10.0, {"source": "no_data"}

    total = int(bld.get("total_bld_500", 0))
    non_res = int(bld.get("non_res_cnt_500", 0))
    non_res_fp = float(bld.get("non_res_fp_500", 0))
    meta = {
        "total_buildings_500m": total,
        "non_residential_count": non_res,
        "non_residential_footprint_m2": round(non_res_fp, 0),
        "source": "overture_buildings",
    }

    if total == 0:
        return 10.0, meta

    count_score = min(90.0, 10.0 + 80.0 * (1 - math.exp(-non_res / 30.0)))
    footprint_score = min(90.0, 10.0 + 80.0 * (1 - math.exp(-non_res_fp / 20000.0)))
    score = 0.4 * count_score + 0.6 * footprint_score
    return max(10.0, min(95.0, score)), meta


def _commercial_parcels_from_context(parcels_nearby: dict) -> tuple[float, dict]:
    """Commercial parcel ratio from pre-fetched nearby parcels data."""
    total = int(parcels_nearby.get("total", 0))
    commercial = int(parcels_nearby.get("commercial_count", 0))
    meta = {
        "nearby_parcels_total": total,
        "nearby_parcels_commercial": commercial,
        "source": "arcgis_parcels",
    }

    if total == 0:
        return 30.0, meta

    ratio = commercial / total
    meta["commercial_ratio"] = round(ratio, 3)
    score = 30.0 + 60.0 * min(1.0, ratio / 0.5)
    return max(10.0, min(95.0, score)), meta


def compute_factors_batch(
    db: Session, lat: float, lon: float,
) -> BatchFactorsResult:
    """
    Compute zoning_fit, parking, and commercial_density using 5 consolidated
    spatial queries instead of ~12 individual ones.

    Also returns pre-fetched spatial context (roads, building/OSM anchors)
    that score_location() reuses for traffic and anchor_proximity factors,
    eliminating 4+ additional queries.
    """
    timing: dict[str, float] = {}

    t = time.perf_counter()
    roads = _batch_fetch_roads(db, lat, lon)
    timing["roads_ms"] = round((time.perf_counter() - t) * 1000, 1)

    t = time.perf_counter()
    bld = _batch_fetch_buildings(db, lat, lon)
    timing["buildings_ms"] = round((time.perf_counter() - t) * 1000, 1)

    t = time.perf_counter()
    osm_rows = _batch_fetch_osm_amenities(db, lat, lon)
    timing["osm_amenities_ms"] = round((time.perf_counter() - t) * 1000, 1)

    t = time.perf_counter()
    parcel = _batch_fetch_parcel_at_point(db, lat, lon)
    timing["parcel_at_point_ms"] = round((time.perf_counter() - t) * 1000, 1)

    t = time.perf_counter()
    parcels_nearby = _batch_fetch_parcels_nearby(db, lat, lon)
    timing["parcels_nearby_ms"] = round((time.perf_counter() - t) * 1000, 1)

    # Compute factors from pre-fetched data (no further DB access)
    t = time.perf_counter()
    osm_anchor_800 = _compute_osm_anchor_weighted(osm_rows, max_dist=800)
    zoning = _zoning_from_context(parcel, parcels_nearby, roads)
    parking = _parking_from_context(parcel, roads, bld)
    commercial = _commercial_density_from_context(
        bld, osm_rows, parcels_nearby, osm_anchor_800,
    )
    timing["compute_ms"] = round((time.perf_counter() - t) * 1000, 1)

    total_ms = sum(timing.values())
    timing["total_ms"] = round(total_ms, 1)
    logger.debug(
        "compute_factors_batch: %.1f ms (roads=%.1f bld=%.1f osm=%.1f parcel=%.1f nearby=%.1f)",
        total_ms,
        timing["roads_ms"], timing["buildings_ms"], timing["osm_amenities_ms"],
        timing["parcel_at_point_ms"], timing["parcels_nearby_ms"],
    )

    return BatchFactorsResult(
        zoning=zoning,
        parking=parking,
        commercial_density=commercial,
        roads_300m=roads,
        overture_commercial_1500=int(bld.get("commercial_anchors_1500", 0)),
        osm_amenity_count_1000=len(osm_rows),
        overture_anchor_weighted_1000=float(bld.get("anchor_weighted_1000", 0)),
        osm_anchor_weighted_800=osm_anchor_800.get("weighted_total", 0),
        timing=timing,
    )
