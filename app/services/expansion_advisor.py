from __future__ import annotations

import json
import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.services.aqar_district_match import normalize_district_key


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
    if distance_m is None:
        return 25.0

    if distance_m < 1000:
        base = 85.0
    elif distance_m <= 2500:
        base = 55.0
    else:
        base = 25.0

    if service_model in {"qsr", "cafe"}:
        base -= 8.0
    elif service_model == "dine_in":
        base += 10.0
    elif service_model == "delivery_first":
        base -= 3.0

    if service_model == "delivery_first" and distance_m is not None and distance_m < 500:
        base += 7.0
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
        positives.append("No existing branches provided; cannibalization assumed neutral-low")
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
        },
    }


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
) -> list[dict[str, Any]]:
    bbox = bbox or {}
    min_lon = bbox.get("min_lon")
    min_lat = bbox.get("min_lat")
    max_lon = bbox.get("max_lon")
    max_lat = bbox.get("max_lat")

    existing_branches = existing_branches or []
    target_districts = target_districts or []
    target_district_norm = {normalize_district_key(item) for item in target_districts if normalize_district_key(item)}

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
                ST_Y(ST_Centroid(p.geom)) AS lat,
                (
                    SELECT
                        COALESCE(
                            NULLIF(ef.properties->>'district', ''),
                            NULLIF(ef.properties->>'district_raw', ''),
                            NULLIF(ef.properties->>'name', ''),
                            NULLIF(ef.properties->>'district_en', '')
                        )
                    FROM external_feature ef
                    WHERE ef.layer_name IN ('osm_districts', 'aqar_district_hulls', 'rydpolygons')
                      AND ef.geometry IS NOT NULL
                      AND ST_Contains(
                          ST_SetSRID(ST_GeomFromGeoJSON(ef.geometry::text), 4326),
                          ST_Centroid(p.geom)
                      )
                    ORDER BY CASE ef.layer_name
                        WHEN 'osm_districts' THEN 1
                        WHEN 'aqar_district_hulls' THEN 2
                        ELSE 3
                    END
                    LIMIT 1
                ) AS district
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
            b.district,
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
        district = row.get("district")

        district_norm = normalize_district_key(district)
        if target_district_norm and (not district_norm or district_norm not in target_district_norm):
            continue

        pop_score = _population_score(population_reach)
        delivery_score = _delivery_score(delivery_listing_count)
        demand_score = _clamp(pop_score * 0.65 + delivery_score * 0.35)

        whitespace_score = _competition_whitespace_score(competitor_count)

        area_fit = _area_fit(area_m2, target_area_m2, min_area_m2, max_area_m2)
        use_fit = _landuse_fit(landuse_label, landuse_code)
        fit_score = _clamp(area_fit * 0.55 + use_fit * 0.45)

        confidence_score = _confidence_score(landuse_label, population_reach, delivery_listing_count)
        distance_to_nearest_branch_m = _nearest_branch_distance_m(
            _safe_float(row.get("lat")),
            _safe_float(row.get("lon")),
            existing_branches,
        )
        cannibalization_score = _cannibalization_score(distance_to_nearest_branch_m, service_model)
        cannibalization_component = 100.0 - cannibalization_score
        final_score = _clamp(
            demand_score * 0.30
            + whitespace_score * 0.25
            + fit_score * 0.20
            + confidence_score * 0.10
            + cannibalization_component * 0.15
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
                "district": district,
                "landuse_label": landuse_label,
                "landuse_code": landuse_code,
                "population_reach": population_reach,
                "competitor_count": competitor_count,
                "delivery_listing_count": delivery_listing_count,
                "demand_score": round(demand_score, 2),
                "whitespace_score": round(whitespace_score, 2),
                "fit_score": round(fit_score, 2),
                "confidence_score": round(confidence_score, 2),
                "cannibalization_score": round(cannibalization_score, 2),
                "distance_to_nearest_branch_m": round(distance_to_nearest_branch_m, 2)
                if distance_to_nearest_branch_m is not None
                else None,
                "final_score": round(final_score, 2),
                "explanation": explanation,
            }
        )

    candidates.sort(key=lambda item: item["final_score"], reverse=True)
    candidates = candidates[:limit]
    for index, candidate in enumerate(candidates, start=1):
        candidate["compare_rank"] = index

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
            cannibalization_score,
            distance_to_nearest_branch_m,
            final_score,
            compare_rank,
            explanation
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
            :cannibalization_score,
            :distance_to_nearest_branch_m,
            :final_score,
            :compare_rank,
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
                cannibalization_score,
                distance_to_nearest_branch_m,
                final_score,
                compare_rank,
                explanation,
                computed_at
            FROM expansion_candidate
            WHERE search_id = :search_id
            ORDER BY compare_rank ASC NULLS LAST, final_score DESC, computed_at DESC
            """
        ),
        {"search_id": search_id},
    ).mappings().all()
    return [dict(row) for row in rows]


def compare_candidates(db: Session, search_id: str, candidate_ids: list[str]) -> dict[str, Any]:
    search = db.execute(text("SELECT id FROM expansion_search WHERE id = :search_id"), {"search_id": search_id}).first()
    if not search:
        raise ValueError("not_found")

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
                confidence_score,
                cannibalization_score,
                distance_to_nearest_branch_m,
                competitor_count,
                delivery_listing_count,
                population_reach,
                landuse_label
            FROM expansion_candidate
            WHERE search_id = :search_id
              AND id = ANY(:candidate_ids)
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

        items.append(
            {
                "candidate_id": row["id"],
                "parcel_id": row.get("parcel_id"),
                "district": row.get("district"),
                "area_m2": row.get("area_m2"),
                "final_score": row.get("final_score"),
                "demand_score": row.get("demand_score"),
                "whitespace_score": row.get("whitespace_score"),
                "fit_score": row.get("fit_score"),
                "confidence_score": row.get("confidence_score"),
                "cannibalization_score": row.get("cannibalization_score"),
                "distance_to_nearest_branch_m": row.get("distance_to_nearest_branch_m"),
                "competitor_count": row.get("competitor_count"),
                "delivery_listing_count": row.get("delivery_listing_count"),
                "population_reach": row.get("population_reach"),
                "landuse_label": row.get("landuse_label"),
                "pros": pros,
                "cons": cons,
            }
        )

    best_overall = max(items, key=lambda item: _safe_float(item.get("final_score")))["candidate_id"]
    lowest_cannibalization = min(items, key=lambda item: _safe_float(item.get("cannibalization_score"), 9999.0))["candidate_id"]
    highest_demand = max(items, key=lambda item: _safe_float(item.get("demand_score")))["candidate_id"]
    best_fit = max(items, key=lambda item: _safe_float(item.get("fit_score")))["candidate_id"]

    return {
        "items": items,
        "summary": {
            "best_overall_candidate_id": best_overall,
            "lowest_cannibalization_candidate_id": lowest_cannibalization,
            "highest_demand_candidate_id": highest_demand,
            "best_fit_candidate_id": best_fit,
        },
    }
