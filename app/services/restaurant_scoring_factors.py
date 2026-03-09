"""
Upgraded scoring factors for restaurant site selection.

Replaces the weak zoning_fit_score, parking_availability_score, and
commercial_density_score with genuinely informative, evidence-based
implementations that use ArcGIS parcel data, road context, and
weighted anchor/building signals.

Each factor returns a ScoredFactor with score, confidence, and rationale.
"""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


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
    except Exception:
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

    return ScoredFactor(
        score=round(score, 1),
        confidence=round(confidence, 3),
        rationale=rationale,
        meta=meta,
    )


# ---------------------------------------------------------------------------
# PARKING AVAILABILITY — Phase 3
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

    # Area component: sigmoid centered around 600m² (typical Riyadh restaurant parcel)
    area_score = min(90.0, 20.0 + 70.0 * (1 - math.exp(-area / 800.0)))

    # Compactness penalty: elongated/irregular parcels are harder for parking layout
    # Isoperimetric ratio: 1.0 = perfect circle, lower = more irregular
    compact_adj = 0.0
    if compactness > 0:
        if compactness < 0.3:
            compact_adj = -10.0  # very irregular, hard to park
        elif compactness < 0.5:
            compact_adj = -3.0
        # compact parcels get no bonus (already factored into area score)

    # Frontage estimate from perimeter (rough proxy for access width)
    if perimeter > 0 and area > 0:
        est_frontage = area / (perimeter / 4.0)  # rough width estimate
        meta["est_frontage_m"] = round(est_frontage, 1)
        if est_frontage < 10:
            compact_adj -= 5.0  # narrow frontage = poor access

    score = max(15.0, min(90.0, area_score + compact_adj))
    meta["source"] = "parcel_geometry"
    return score, meta


def _road_access_score(db: Session, lat: float, lon: float) -> tuple[float, dict]:
    """
    Assess road access quality for parking/customer arrival.
    Returns (sub_score 0-100, meta).
    """
    meta: dict[str, Any] = {}
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
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return 45.0, {"source": "error"}

    if not rows:
        return 20.0, {"source": "no_roads", "road_count": 0}

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

    # Best for restaurants: secondary/tertiary with service road nearby
    score = 40.0

    if has_service:
        score += 20.0  # service road = easy ingress/egress
    if has_secondary:
        score += 15.0  # good visibility + accessible
    if has_primary:
        score += 10.0  # high visibility but sometimes hard to turn into
    if has_residential:
        score += 5.0  # some local access
    if has_motorway and not has_service and not has_secondary:
        score -= 10.0  # motorway-only = bad for restaurant parking access

    # Distance bonus: closer to road = better access
    if nearest_dist < 20:
        score += 5.0
    elif nearest_dist > 100:
        score -= 10.0

    meta["source"] = "road_analysis"
    return max(10.0, min(95.0, score)), meta


def _nearby_parking_supply(db: Session, lat: float, lon: float) -> tuple[float, dict]:
    """
    Count nearby parking structures, malls, and large commercial anchors
    that provide parking options.
    Returns (sub_score 0-100, meta).
    """
    meta: dict[str, Any] = {}

    # 1. Direct parking structures from Overture buildings
    parking_count = 0
    try:
        parking_count = db.execute(
            text("""
                SELECT COUNT(*) FROM overture_buildings
                WHERE (class ILIKE '%%parking%%' OR class ILIKE '%%garage%%')
                  AND ST_DWithin(
                      ST_Transform(geom, 4326)::geography,
                      ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                      500
                  )
            """),
            {"lat": lat, "lon": lon},
        ).scalar() or 0
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
    meta["parking_structures_500m"] = parking_count

    # 2. Malls/shopping centers nearby (typically have large parking)
    #    Uses overture_buildings (class column) — restaurant_poi only has food POIs.
    mall_count = 0
    try:
        mall_count = db.execute(
            text("""
                SELECT COUNT(*) FROM overture_buildings
                WHERE (class ILIKE '%%mall%%'
                    OR class ILIKE '%%shopping%%'
                    OR class ILIKE '%%retail%%')
                  AND ST_DWithin(
                      ST_Transform(geom, 4326)::geography,
                      ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                      800
                  )
            """),
            {"lat": lat, "lon": lon},
        ).scalar() or 0
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
    meta["malls_800m"] = mall_count

    # 3. Large Overture buildings (>2000m² footprint) that likely have parking
    large_buildings = 0
    try:
        large_buildings = db.execute(
            text("""
                SELECT COUNT(*) FROM overture_buildings
                WHERE ST_Area(ST_Transform(geom, 32638)) > 2000
                  AND ST_DWithin(
                      ST_Transform(geom, 4326)::geography,
                      ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                      500
                  )
            """),
            {"lat": lat, "lon": lon},
        ).scalar() or 0
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
    meta["large_buildings_500m"] = large_buildings

    # Composite score
    score = 25.0  # baseline: no parking evidence
    score += min(30.0, parking_count * 12.0)  # parking structures: up to 30pts
    score += min(20.0, mall_count * 15.0)  # malls: up to 20pts
    score += min(15.0, large_buildings * 3.0)  # large buildings: up to 15pts

    meta["source"] = "nearby_supply"
    return max(10.0, min(95.0, score)), meta


def parking_availability_score(db: Session, lat: float, lon: float) -> ScoredFactor:
    """
    Composite parking suitability score for restaurant site selection.

    Combines:
    - Parcel geometry feasibility (area, compactness, frontage)
    - Road access quality (road hierarchy, service roads, ingress/egress)
    - Nearby parking supply (structures, malls, large buildings)
    """
    # Component weights for the composite
    W_PARCEL = 0.35
    W_ACCESS = 0.35
    W_SUPPLY = 0.30

    parcel_score, parcel_meta = _parcel_parking_feasibility(db, lat, lon)
    access_score, access_meta = _road_access_score(db, lat, lon)
    supply_score, supply_meta = _nearby_parking_supply(db, lat, lon)

    composite = W_PARCEL * parcel_score + W_ACCESS * access_score + W_SUPPLY * supply_score
    composite = max(10.0, min(95.0, composite))

    # Confidence based on data availability
    has_parcel = parcel_meta.get("source") not in ("error", "no_parcel")
    has_roads = access_meta.get("road_count", 0) > 0
    has_supply_data = (
        supply_meta.get("parking_structures_500m", 0) > 0
        or supply_meta.get("malls_800m", 0) > 0
        or supply_meta.get("large_buildings_500m", 0) > 0
    )

    evidence_count = sum([has_parcel, has_roads, has_supply_data])
    confidence = {0: 0.15, 1: 0.4, 2: 0.7, 3: 0.9}.get(evidence_count, 0.15)

    parts = []
    if has_parcel:
        parts.append(f"parcel={parcel_score:.0f}")
    if has_roads:
        parts.append(f"access={access_score:.0f}")
    if has_supply_data:
        parts.append(f"supply={supply_score:.0f}")
    rationale = "composite:" + "+".join(parts) if parts else "no_evidence"

    return ScoredFactor(
        score=round(composite, 1),
        confidence=round(confidence, 3),
        rationale=rationale,
        meta={
            "parcel": {"score": round(parcel_score, 1), **parcel_meta},
            "access": {"score": round(access_score, 1), **access_meta},
            "supply": {"score": round(supply_score, 1), **supply_meta},
            "weights": {"parcel": W_PARCEL, "access": W_ACCESS, "supply": W_SUPPLY},
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
        },
    )
