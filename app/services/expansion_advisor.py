from __future__ import annotations

import json
import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


ARCGIS_PARCELS_TABLE = "public.riyadh_parcels_arcgis_proxy"


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _landuse_fit(landuse_label: str | None, landuse_code: str | None) -> float:
    raw = f"{landuse_label or ''} {landuse_code or ''}".strip().lower()
    if not raw:
        return 45.0
    if any(token in raw for token in ["commercial", "mixed", "retail", "تجاري", "مختلط"]):
        return 100.0
    if any(token in raw for token in ["residential", "سكني"]):
        return 55.0
    return 70.0


def _area_fit(area_m2: float, target_area_m2: float, min_area_m2: float, max_area_m2: float) -> float:
    if area_m2 <= 0:
        return 0.0
    if area_m2 < min_area_m2 or area_m2 > max_area_m2:
        return 0.0
    span = max(max_area_m2 - min_area_m2, 1.0)
    distance = abs(area_m2 - target_area_m2)
    score = 100.0 - (distance / span) * 100.0
    return _clamp(score)


def _population_score(population_reach: float) -> float:
    # Tuned as a first deterministic heuristic; we will recalibrate later.
    return _clamp((population_reach / 18000.0) * 100.0)


def _delivery_score(delivery_listing_count: int) -> float:
    return _clamp((delivery_listing_count / 40.0) * 100.0)


def _competition_whitespace_score(competitor_count: int) -> float:
    return _clamp(100.0 - competitor_count * 9.0)


def _confidence_score(landuse_label: str | None, population_reach: float, delivery_listing_count: int) -> float:
    score = 40.0
    if landuse_label:
        score += 25.0
    if population_reach > 0:
        score += 20.0
    if delivery_listing_count > 0:
        score += 15.0
    return _clamp(score)


def _build_explanation(
    *,
    area_m2: float,
    population_reach: float,
    competitor_count: int,
    delivery_listing_count: int,
    landuse_label: str | None,
    landuse_code: str | None,
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

    return {
        "summary": f"Candidate scored {final_score:.1f}/100 using ArcGIS parcel fit, demand, whitespace, and confidence.",
        "positives": positives,
        "risks": risks,
        "inputs": {
            "area_m2": area_m2,
            "population_reach": population_reach,
            "competitor_count": competitor_count,
            "delivery_listing_count": delivery_listing_count,
            "landuse_label": landuse_label,
            "landuse_code": landuse_code,
        },
    }


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
) -> list[dict[str, Any]]:
    bbox = bbox or {}
    min_lon = bbox.get("min_lon")
    min_lat = bbox.get("min_lat")
    max_lon = bbox.get("max_lon")
    max_lat = bbox.get("max_lat")

    # ArcGIS-only candidate generation.
    sql = text(
        f"""
        WITH candidate_base AS (
            SELECT
                p.id AS parcel_id,
                p.landuse_label,
                p.landuse_code,
                p.area_m2,
                p.geom,
                ST_X(ST_Centroid(p.geom)) AS lon,
                ST_Y(ST_Centroid(p.geom)) AS lat
            FROM {ARCGIS_PARCELS_TABLE} p
            WHERE p.geom IS NOT NULL
              AND p.area_m2 BETWEEN :min_area_m2 AND :max_area_m2
              AND (:min_lon IS NULL OR ST_X(ST_Centroid(p.geom)) >= :min_lon)
              AND (:max_lon IS NULL OR ST_X(ST_Centroid(p.geom)) <= :max_lon)
              AND (:min_lat IS NULL OR ST_Y(ST_Centroid(p.geom)) >= :min_lat)
              AND (:max_lat IS NULL OR ST_Y(ST_Centroid(p.geom)) <= :max_lat)
            ORDER BY p.area_m2 DESC
            LIMIT 600
        )
        SELECT
            b.parcel_id,
            b.landuse_label,
            b.landuse_code,
            b.area_m2,
            b.lon,
            b.lat,
            COALESCE((
                SELECT SUM(pd.population)
                FROM population_density pd
                WHERE pd.lat IS NOT NULL
                  AND pd.lon IS NOT NULL
                  AND ST_DWithin(
                      ST_SetSRID(ST_MakePoint(b.lon, b.lat), 4326)::geography,
                      ST_SetSRID(ST_MakePoint(pd.lon::float, pd.lat::float), 4326)::geography,
                      :demand_radius_m
                  )
            ), 0) AS population_reach,
            COALESCE((
                SELECT COUNT(*)
                FROM restaurant_poi rp
                WHERE lower(rp.category) = lower(:category)
                  AND ST_DWithin(
                      rp.geom::geography,
                      ST_SetSRID(ST_MakePoint(b.lon, b.lat), 4326)::geography,
                      :competition_radius_m
                  )
            ), 0) AS competitor_count,
            COALESCE((
                SELECT COUNT(*)
                FROM delivery_source_record dsr
                WHERE dsr.lat IS NOT NULL
                  AND dsr.lon IS NOT NULL
                  AND (
                    lower(COALESCE(dsr.category_raw, '')) LIKE :category_like
                    OR lower(COALESCE(dsr.cuisine_raw, '')) LIKE :category_like
                  )
                  AND ST_DWithin(
                      ST_SetSRID(ST_MakePoint(dsr.lon::float, dsr.lat::float), 4326)::geography,
                      ST_SetSRID(ST_MakePoint(b.lon, b.lat), 4326)::geography,
                      :demand_radius_m
                  )
            ), 0) AS delivery_listing_count
        FROM candidate_base b
        """
    )

    rows = db.execute(
        sql,
        {
            "min_area_m2": min_area_m2,
            "max_area_m2": max_area_m2,
            "min_lon": min_lon,
            "min_lat": min_lat,
            "max_lon": max_lon,
            "max_lat": max_lat,
            "category": category,
            "category_like": f"%{category.lower()}%",
            "demand_radius_m": 1200,
            "competition_radius_m": 1000,
        },
    ).mappings().all()

    candidates: list[dict[str, Any]] = []
    for row in rows:
        area_m2 = _safe_float(row.get("area_m2"))
        population_reach = _safe_float(row.get("population_reach"))
        competitor_count = _safe_int(row.get("competitor_count"))
        delivery_listing_count = _safe_int(row.get("delivery_listing_count"))
        landuse_label = row.get("landuse_label")
        landuse_code = row.get("landuse_code")

        pop_score = _population_score(population_reach)
        delivery_score = _delivery_score(delivery_listing_count)
        demand_score = _clamp(pop_score * 0.65 + delivery_score * 0.35)

        whitespace_score = _competition_whitespace_score(competitor_count)

        area_fit = _area_fit(area_m2, target_area_m2, min_area_m2, max_area_m2)
        use_fit = _landuse_fit(landuse_label, landuse_code)
        fit_score = _clamp(area_fit * 0.55 + use_fit * 0.45)

        confidence_score = _confidence_score(landuse_label, population_reach, delivery_listing_count)
        final_score = _clamp(
            demand_score * 0.35
            + whitespace_score * 0.30
            + fit_score * 0.20
            + confidence_score * 0.15
        )

        explanation = _build_explanation(
            area_m2=area_m2,
            population_reach=population_reach,
            competitor_count=competitor_count,
            delivery_listing_count=delivery_listing_count,
            landuse_label=landuse_label,
            landuse_code=landuse_code,
            final_score=final_score,
        )

        candidates.append(
            {
                "id": str(uuid.uuid4()),
                "search_id": search_id,
                "parcel_id": str(row["parcel_id"]),
                "lat": _safe_float(row.get("lat")),
                "lon": _safe_float(row.get("lon")),
                "area_m2": area_m2,
                "landuse_label": landuse_label,
                "landuse_code": landuse_code,
                "population_reach": population_reach,
                "competitor_count": competitor_count,
                "delivery_listing_count": delivery_listing_count,
                "demand_score": round(demand_score, 2),
                "whitespace_score": round(whitespace_score, 2),
                "fit_score": round(fit_score, 2),
                "confidence_score": round(confidence_score, 2),
                "final_score": round(final_score, 2),
                "explanation": explanation,
            }
        )

    candidates.sort(key=lambda item: item["final_score"], reverse=True)
    candidates = candidates[:limit]

    insert_sql = text(
        """
        INSERT INTO expansion_candidate (
            id,
            search_id,
            parcel_id,
            lat,
            lon,
            area_m2,
            landuse_label,
            landuse_code,
            population_reach,
            competitor_count,
            delivery_listing_count,
            demand_score,
            whitespace_score,
            fit_score,
            confidence_score,
            final_score,
            explanation
        ) VALUES (
            :id,
            :search_id,
            :parcel_id,
            :lat,
            :lon,
            :area_m2,
            :landuse_label,
            :landuse_code,
            :population_reach,
            :competitor_count,
            :delivery_listing_count,
            :demand_score,
            :whitespace_score,
            :fit_score,
            :confidence_score,
            :final_score,
            CAST(:explanation AS jsonb)
        )
        """
    )

    for candidate in candidates:
        db.execute(
            insert_sql,
            {
                **candidate,
                "explanation": json.dumps(candidate["explanation"], ensure_ascii=False),
            },
        )

    return candidates


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
                notes
            FROM expansion_search
            WHERE id = :search_id
            """
        ),
        {"search_id": search_id},
    ).mappings().first()
    return dict(row) if row else None


def get_candidates(db: Session, search_id: str) -> list[dict[str, Any]]:
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
                landuse_label,
                landuse_code,
                population_reach,
                competitor_count,
                delivery_listing_count,
                demand_score,
                whitespace_score,
                fit_score,
                confidence_score,
                final_score,
                explanation,
                computed_at
            FROM expansion_candidate
            WHERE search_id = :search_id
            ORDER BY final_score DESC, computed_at DESC
            """
        ),
        {"search_id": search_id},
    ).mappings().all()
    return [dict(row) for row in rows]
