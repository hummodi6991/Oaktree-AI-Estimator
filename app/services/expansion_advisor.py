from __future__ import annotations

import json
import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.services.aqar_district_match import normalize_district_key
from app.services.rent import aqar_rent_median


ARCGIS_PARCELS_TABLE = "public.riyadh_parcels_arcgis_proxy"
_EXPANSION_CITY = "riyadh"
_EXPANSION_AQAR_ASSET = "commercial"
_EXPANSION_AQAR_UNIT = "retail"
_EXPANSION_DEFAULT_RENT_SAR_M2_YEAR = 900.0


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
    economics_score: float,
    estimated_rent_sar_m2_year: float,
    estimated_annual_rent_sar: float,
    estimated_fitout_cost_sar: float,
    estimated_revenue_index: float,
    estimated_payback_months: float,
    payback_band: str,
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
            "economics_score": economics_score,
            "estimated_rent_sar_m2_year": estimated_rent_sar_m2_year,
            "estimated_annual_rent_sar": estimated_annual_rent_sar,
            "estimated_fitout_cost_sar": estimated_fitout_cost_sar,
            "estimated_revenue_index": estimated_revenue_index,
            "estimated_payback_months": estimated_payback_months,
            "payback_band": payback_band,
            "rent_source": rent_source,
        },
    }


def _estimate_rent_sar_m2_year(db: Session, district: str | None) -> tuple[float, str]:
    try:
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
        pass
    return _EXPANSION_DEFAULT_RENT_SAR_M2_YEAR, "conservative_default"


def _estimate_fitout_cost_sar(area_m2: float, service_model: str) -> float:
    cost_per_m2 = {
        "delivery_first": 1900.0,
        "qsr": 2600.0,
        "cafe": 2800.0,
        "dine_in": 3600.0,
    }.get(service_model, 2600.0)
    return max(0.0, area_m2 * cost_per_m2)


def _estimate_revenue_index(
    demand_score: float,
    delivery_listing_count: int,
    population_reach: float,
    whitespace_score: float,
) -> float:
    delivery_signal = _clamp((delivery_listing_count / 35.0) * 100.0)
    population_signal = _clamp((population_reach / 16000.0) * 100.0)
    return _clamp(demand_score * 0.45 + whitespace_score * 0.20 + delivery_signal * 0.20 + population_signal * 0.15)


def _economics_score(
    *,
    estimated_revenue_index: float,
    estimated_annual_rent_sar: float,
    estimated_fitout_cost_sar: float,
    area_m2: float,
    cannibalization_score: float,
    fit_score: float,
) -> float:
    monthly_rent_per_m2 = estimated_annual_rent_sar / max(area_m2 * 12.0, 1.0)
    rent_burden_score = _clamp(100.0 - (monthly_rent_per_m2 / 180.0) * 100.0)
    fitout_cost_per_m2 = estimated_fitout_cost_sar / max(area_m2, 1.0)
    fitout_burden_score = _clamp(100.0 - ((fitout_cost_per_m2 - 1800.0) / 2600.0) * 100.0)
    cannibalization_component = 100.0 - cannibalization_score
    return _clamp(
        estimated_revenue_index * 0.38
        + rent_burden_score * 0.20
        + fitout_burden_score * 0.14
        + cannibalization_component * 0.13
        + fit_score * 0.15
    )


def _estimate_payback_months(
    *,
    estimated_fitout_cost_sar: float,
    estimated_annual_rent_sar: float,
    estimated_revenue_index: float,
    confidence_score: float,
) -> float:
    annual_burden = estimated_annual_rent_sar + estimated_fitout_cost_sar * 0.45
    normalized_burden = _clamp((annual_burden / 1_800_000.0) * 100.0)
    quality_factor = 0.85 + (confidence_score / 100.0) * 0.3
    months = 16.0 + normalized_burden * 0.38 - estimated_revenue_index * 0.18
    return round(_clamp(months / max(quality_factor, 0.55), 9.0, 72.0), 2)


def _payback_band(estimated_payback_months: float) -> str:
    if estimated_payback_months <= 18.0:
        return "strong"
    if estimated_payback_months <= 28.0:
        return "promising"
    if estimated_payback_months <= 40.0:
        return "borderline"
    return "weak"


def _build_strengths_and_risks(
    *,
    demand_score: float,
    whitespace_score: float,
    fit_score: float,
    cannibalization_score: float,
    payback_band: str,
    rent_source: str,
) -> tuple[list[str], list[str]]:
    strengths: list[str] = []
    risks: list[str] = []
    if demand_score >= 70:
        strengths.append("High demand index supports branch throughput")
    if whitespace_score >= 65:
        strengths.append("Competitive whitespace remains attractive")
    if fit_score >= 70:
        strengths.append("Parcel characteristics align with target format")
    if payback_band in {"strong", "promising"}:
        strengths.append(f"Heuristic payback is {payback_band} for first-pass screening")
    if rent_source == "conservative_default":
        risks.append("Rent benchmark fell back to conservative city default (lower confidence)")
    if cannibalization_score >= 70:
        risks.append("High overlap risk with existing branches")
    if payback_band in {"borderline", "weak"}:
        risks.append("Payback profile is slower versus preferred expansion targets")
    if whitespace_score <= 45:
        risks.append("Competitive density may pressure launch economics")
    return strengths[:4], risks[:4]


def _recommended_use_case(service_model: str, area_m2: float) -> str:
    if service_model == "dine_in":
        return "flagship dine-in" if area_m2 >= 260 else "neighborhood dine-in"
    if service_model == "delivery_first":
        return "delivery-led branch"
    if service_model == "cafe":
        return "compact cafe" if area_m2 < 180 else "destination cafe"
    return "neighborhood qsr"


def _decision_summary(
    *,
    district: str | None,
    final_score: float,
    economics_score: float,
    payback_band: str,
    key_risks: list[str],
    service_model: str,
    area_m2: float,
) -> str:
    area_label = "compact" if area_m2 < 180 else "standard"
    district_label = district or "the target district"
    risk_text = key_risks[0] if key_risks else "execution risk should be managed during leasing and design"
    return (
        f"This {area_label} candidate in {district_label} scores {final_score:.1f}/100 overall with an economics score of {economics_score:.1f}/100. "
        f"The payback profile is {payback_band}, making it a practical first-pass option for {_recommended_use_case(service_model, area_m2)}. "
        f"The biggest commercial risk is {risk_text.lower()}."
    )


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
    rent_cache: dict[str | None, tuple[float, str]] = {}
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

        rent_cache_key = district_norm or None
        if rent_cache_key not in rent_cache:
            rent_cache[rent_cache_key] = _estimate_rent_sar_m2_year(db, district)
        estimated_rent_sar_m2_year, rent_source = rent_cache[rent_cache_key]
        estimated_annual_rent_sar = area_m2 * estimated_rent_sar_m2_year
        estimated_fitout_cost_sar = _estimate_fitout_cost_sar(area_m2, service_model)
        estimated_revenue_index = _estimate_revenue_index(
            demand_score,
            delivery_listing_count,
            population_reach,
            whitespace_score,
        )
        economics_score = _economics_score(
            estimated_revenue_index=estimated_revenue_index,
            estimated_annual_rent_sar=estimated_annual_rent_sar,
            estimated_fitout_cost_sar=estimated_fitout_cost_sar,
            area_m2=area_m2,
            cannibalization_score=cannibalization_score,
            fit_score=fit_score,
        )
        estimated_payback_months = _estimate_payback_months(
            estimated_fitout_cost_sar=estimated_fitout_cost_sar,
            estimated_annual_rent_sar=estimated_annual_rent_sar,
            estimated_revenue_index=estimated_revenue_index,
            confidence_score=confidence_score,
        )
        payback_band = _payback_band(estimated_payback_months)
        key_strengths_json, key_risks_json = _build_strengths_and_risks(
            demand_score=demand_score,
            whitespace_score=whitespace_score,
            fit_score=fit_score,
            cannibalization_score=cannibalization_score,
            payback_band=payback_band,
            rent_source=rent_source,
        )

        final_score = _clamp(
            demand_score * 0.22
            + whitespace_score * 0.18
            + fit_score * 0.18
            + confidence_score * 0.08
            + cannibalization_component * 0.12
            + economics_score * 0.22
        )

        decision_summary = _decision_summary(
            district=district,
            final_score=final_score,
            economics_score=economics_score,
            payback_band=payback_band,
            key_risks=key_risks_json,
            service_model=service_model,
            area_m2=area_m2,
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
            estimated_payback_months=estimated_payback_months,
            payback_band=payback_band,
            rent_source=rent_source,
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
                "estimated_rent_sar_m2_year": round(estimated_rent_sar_m2_year, 2),
                "estimated_annual_rent_sar": round(estimated_annual_rent_sar, 2),
                "estimated_fitout_cost_sar": round(estimated_fitout_cost_sar, 2),
                "estimated_revenue_index": round(estimated_revenue_index, 2),
                "economics_score": round(economics_score, 2),
                "estimated_payback_months": round(estimated_payback_months, 2),
                "payback_band": payback_band,
                "decision_summary": decision_summary,
                "key_risks_json": key_risks_json,
                "key_strengths_json": key_strengths_json,
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
            estimated_rent_sar_m2_year,
            estimated_annual_rent_sar,
            estimated_fitout_cost_sar,
            estimated_revenue_index,
            economics_score,
            estimated_payback_months,
            payback_band,
            decision_summary,
            key_risks_json,
            key_strengths_json,
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
            :estimated_rent_sar_m2_year,
            :estimated_annual_rent_sar,
            :estimated_fitout_cost_sar,
            :estimated_revenue_index,
            :economics_score,
            :estimated_payback_months,
            :payback_band,
            :decision_summary,
            CAST(:key_risks_json AS jsonb),
            CAST(:key_strengths_json AS jsonb),
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
                "key_risks_json": json.dumps(candidate["key_risks_json"], ensure_ascii=False),
                "key_strengths_json": json.dumps(candidate["key_strengths_json"], ensure_ascii=False),
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
                estimated_rent_sar_m2_year,
                estimated_annual_rent_sar,
                estimated_fitout_cost_sar,
                estimated_revenue_index,
                economics_score,
                estimated_payback_months,
                payback_band,
                decision_summary,
                key_risks_json,
                key_strengths_json,
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
    return dict(row) if row else {}


def list_saved_searches(
    db: Session,
    *,
    status: str | None,
    limit: int,
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
    return [dict(row) for row in rows]


def get_saved_search(db: Session, saved_id: str) -> dict[str, Any] | None:
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
    candidates = get_candidates(db, str(saved["search_id"]))
    saved["search"] = search
    saved["candidates"] = candidates
    return saved


def update_saved_search(
    db: Session,
    saved_id: str,
    payload: dict[str, Any],
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
        return dict(row) if row else None

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
    return dict(row) if row else None


def delete_saved_search(db: Session, saved_id: str) -> bool:
    row = db.execute(
        text("DELETE FROM expansion_saved_search WHERE id = :saved_id RETURNING id"),
        {"saved_id": saved_id},
    ).first()
    return bool(row)
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
                estimated_rent_sar_m2_year,
                estimated_annual_rent_sar,
                estimated_fitout_cost_sar,
                estimated_revenue_index,
                economics_score,
                estimated_payback_months,
                payback_band,
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
                "estimated_rent_sar_m2_year": row.get("estimated_rent_sar_m2_year"),
                "estimated_annual_rent_sar": row.get("estimated_annual_rent_sar"),
                "estimated_fitout_cost_sar": row.get("estimated_fitout_cost_sar"),
                "estimated_revenue_index": row.get("estimated_revenue_index"),
                "economics_score": row.get("economics_score"),
                "estimated_payback_months": row.get("estimated_payback_months"),
                "payback_band": row.get("payback_band"),
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
    best_economics = max(items, key=lambda item: _safe_float(item.get("economics_score")))["candidate_id"]
    lowest_rent_burden = min(items, key=lambda item: _safe_float(item.get("estimated_annual_rent_sar"), 10**12))["candidate_id"]
    fastest_payback = min(items, key=lambda item: _safe_float(item.get("estimated_payback_months"), 10**6))["candidate_id"]

    return {
        "items": items,
        "summary": {
            "best_overall_candidate_id": best_overall,
            "lowest_cannibalization_candidate_id": lowest_cannibalization,
            "highest_demand_candidate_id": highest_demand,
            "best_fit_candidate_id": best_fit,
            "best_economics_candidate_id": best_economics,
            "lowest_rent_burden_candidate_id": lowest_rent_burden,
            "fastest_payback_candidate_id": fastest_payback,
        },
    }


def get_candidate_memo(db: Session, candidate_id: str) -> dict[str, Any] | None:
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
                c.demand_score,
                c.whitespace_score,
                c.fit_score,
                c.confidence_score,
                c.cannibalization_score,
                c.distance_to_nearest_branch_m,
                c.estimated_rent_sar_m2_year,
                c.estimated_annual_rent_sar,
                c.estimated_fitout_cost_sar,
                c.estimated_revenue_index,
                c.estimated_payback_months,
                c.payback_band,
                c.key_strengths_json,
                c.key_risks_json,
                c.decision_summary
            FROM expansion_candidate c
            JOIN expansion_search s ON s.id = c.search_id
            WHERE c.id = :candidate_id
            """
        ),
        {"candidate_id": candidate_id},
    ).mappings().first()
    if not row:
        return None

    candidate = dict(row)
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
    )
    main_watchout = risks[0] if risks else "Validate lease and capex assumptions before commitment"
    district = candidate.get("district") or "Riyadh"
    headline = f"{verdict.upper()}: {district} parcel shows {economics_score:.1f}/100 economics for {best_use_case}"

    return {
        "candidate_id": candidate["candidate_id"],
        "search_id": candidate["search_id"],
        "brand_profile": {
            "brand_name": candidate.get("brand_name"),
            "category": candidate.get("category"),
            "service_model": candidate.get("service_model"),
        },
        "candidate": {
            "parcel_id": candidate.get("parcel_id"),
            "district": candidate.get("district"),
            "area_m2": candidate.get("area_m2"),
            "landuse_label": candidate.get("landuse_label"),
            "final_score": candidate.get("final_score"),
            "economics_score": candidate.get("economics_score"),
            "demand_score": candidate.get("demand_score"),
            "whitespace_score": candidate.get("whitespace_score"),
            "fit_score": candidate.get("fit_score"),
            "confidence_score": candidate.get("confidence_score"),
            "cannibalization_score": candidate.get("cannibalization_score"),
            "distance_to_nearest_branch_m": candidate.get("distance_to_nearest_branch_m"),
            "estimated_rent_sar_m2_year": candidate.get("estimated_rent_sar_m2_year"),
            "estimated_annual_rent_sar": candidate.get("estimated_annual_rent_sar"),
            "estimated_fitout_cost_sar": candidate.get("estimated_fitout_cost_sar"),
            "estimated_revenue_index": candidate.get("estimated_revenue_index"),
            "estimated_payback_months": candidate.get("estimated_payback_months"),
            "payback_band": candidate.get("payback_band"),
            "key_strengths": strengths,
            "key_risks": risks,
            "decision_summary": candidate.get("decision_summary"),
        },
        "recommendation": {
            "headline": headline,
            "verdict": verdict,
            "best_use_case": best_use_case,
            "main_watchout": main_watchout,
        },
    }
