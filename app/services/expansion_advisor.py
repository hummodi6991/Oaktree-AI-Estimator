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


def _default_brand_profile(brand_profile: dict[str, Any] | None = None) -> dict[str, Any]:
    base = {
        "price_tier": None,
        "average_check_sar": None,
        "primary_channel": "balanced",
        "parking_sensitivity": "medium",
        "frontage_sensitivity": "medium",
        "visibility_sensitivity": "medium",
        "target_customer": None,
        "expansion_goal": "balanced",
        "cannibalization_tolerance_m": 1800.0,
        "preferred_districts": [],
        "excluded_districts": [],
    }
    if brand_profile:
        base.update({k: v for k, v in brand_profile.items() if v is not None})
    return base


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
    visibility_signal: float, parking_signal: float, brand_profile: dict[str, Any], service_model: str) -> float:
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

    goal = (brand_profile.get("expansion_goal") or "balanced").lower()
    goal_component = 60.0
    if goal == "flagship":
        goal_component = _clamp((area_m2 / 350.0) * 60.0 + visibility_signal * 0.4 + demand_score * 0.2)
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


def _landuse_fit(landuse_label: str | None, landuse_code: str | None) -> float:
    raw = f"{landuse_label or ''} {landuse_code or ''}".strip().lower()
    if not raw:
        return 45.0
    if any(token in raw for token in ["commercial", "mixed", "retail", "تجاري", "مختلط"]):
        return 100.0
    if any(token in raw for token in ["residential", "سكني"]):
        return 55.0
    return 70.0


def _zoning_fit_score(landuse_label: str | None, landuse_code: str | None) -> float:
    return _clamp(_landuse_fit(landuse_label, landuse_code))


def _table_available(db: Session, table_name: str) -> bool:
    schema, _, table = table_name.partition(".")
    if not table:
        schema, table = "public", schema
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


def _frontage_score(*, parcel_perimeter_m: float, touches_road: bool, nearby_road_count: int, nearest_major_road_m: float | None,
    road_context_available: bool = True) -> float:
    if not road_context_available:
        return 55.0
    perimeter_signal = _clamp((parcel_perimeter_m / 260.0) * 100.0)
    touch_signal = 100.0 if touches_road else 40.0
    density_signal = _clamp((nearby_road_count / 6.0) * 100.0)
    major_road_signal = _clamp(100.0 - (_safe_float(nearest_major_road_m, 300.0) / 300.0) * 100.0)
    return _clamp(perimeter_signal * 0.30 + touch_signal * 0.30 + density_signal * 0.20 + major_road_signal * 0.20)


def _access_score(*, touches_road: bool, nearest_major_road_m: float | None, nearby_road_count: int, road_context_available: bool = True) -> float:
    if not road_context_available:
        return 55.0
    touch_signal = 100.0 if touches_road else 30.0
    major_signal = _clamp(100.0 - (_safe_float(nearest_major_road_m, 500.0) / 500.0) * 100.0)
    road_density = _clamp((nearby_road_count / 8.0) * 100.0)
    return _clamp(touch_signal * 0.40 + major_signal * 0.35 + road_density * 0.25)


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


def _access_visibility_score(*, frontage_score: float, access_score: float, brand_profile: dict[str, Any]) -> float:
    visibility_weight = _sensitivity_weight(brand_profile.get("visibility_sensitivity"))
    frontage_weight = _sensitivity_weight(brand_profile.get("frontage_sensitivity"))
    blend = 0.5 + frontage_weight * 0.2
    access_blend = 1.0 - blend
    weighted = frontage_score * blend + access_score * access_blend
    return _clamp(weighted * (0.75 + visibility_weight * 0.25))


def _candidate_feature_snapshot(db: Session, *, parcel_id: str, lat: float, lon: float, area_m2: float, district: str | None,
    landuse_label: str | None, landuse_code: str | None, provider_listing_count: int, provider_platform_count: int,
    competitor_count: int, nearest_branch_distance_m: float | None, rent_source: str, estimated_rent_sar_m2_year: float,
    economics_score: float, roads_table_available: bool, parking_table_available: bool) -> dict[str, Any]:
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

    base["context_sources"]["roads_table_available"] = roads_table_available
    base["context_sources"]["parking_table_available"] = parking_table_available

    if not parcel_id:
        base["missing_context"] = ["missing_parcel_id"]
        base["data_completeness_score"] = 50
        return base
    try:
        row = db.execute(text(
            f"""
            WITH p AS (
                SELECT id, geom, area_m2
                FROM {ARCGIS_PARCELS_TABLE}
                WHERE id::text = :parcel_id
                LIMIT 1
            )
            SELECT
                COALESCE(ST_Perimeter(p.geom::geography), 0) AS parcel_perimeter_m,
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
                ), 5000) AS nearest_major_road_distance_m,
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
                ) AS touches_road,
                COALESCE((
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
        ), {"parcel_id": str(parcel_id)}).mappings().first()
        if row:
            nearby_road_segment_count = _safe_int(row.get("nearby_road_segment_count"))
            touches_road = bool(row.get("touches_road"))
            nearest_major_road_distance_m = _safe_float(row.get("nearest_major_road_distance_m"))
            nearby_parking_amenity_count = _safe_int(row.get("nearby_parking_amenity_count"))
            base.update(
                {
                    "parcel_perimeter_m": round(_safe_float(row.get("parcel_perimeter_m")), 2),
                    "nearest_major_road_distance_m": round(nearest_major_road_distance_m, 2),
                    "nearby_road_segment_count": nearby_road_segment_count,
                    "touches_road": touches_road,
                    "nearby_parking_amenity_count": nearby_parking_amenity_count,
                }
            )
            road_context_available = roads_table_available and (
                nearby_road_segment_count > 0 or touches_road or nearest_major_road_distance_m < 5000
            )
            parking_context_available = parking_table_available and nearby_parking_amenity_count >= 0
            base["context_sources"]["road_context_available"] = road_context_available
            base["context_sources"]["parking_context_available"] = parking_context_available
    except Exception:
        pass

    missing_context: list[str] = []
    if not roads_table_available:
        missing_context.append("roads_table_unavailable")
    if not parking_table_available:
        missing_context.append("parking_table_unavailable")
    if roads_table_available and not base["context_sources"].get("road_context_available"):
        missing_context.append("road_context_unavailable")
    if parking_table_available and not base["context_sources"].get("parking_context_available"):
        missing_context.append("parking_context_unavailable")
    base["missing_context"] = missing_context

    completeness_components = [100.0]
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


def _candidate_gate_status(
    *,
    fit_score: float,
    zoning_fit_score: float,
    frontage_score: float,
    access_score: float,
    parking_score: float,
    district: str | None,
    distance_to_nearest_branch_m: float | None,
    provider_density_score: float,
    multi_platform_presence_score: float,
    economics_score: float,
    payback_band: str,
    brand_profile: dict[str, Any],
    road_context_available: bool,
    parking_context_available: bool,
) -> tuple[dict[str, bool], dict[str, Any]]:
    thresholds = {
        "area_fit_min": 55.0,
        "zoning_fit_min": 60.0,
        "frontage_access_min": 55.0,
        "parking_min": 45.0,
        "economics_min": 50.0,
        "delivery_provider_density_min": 45.0,
        "delivery_platform_presence_min": 35.0,
        "cannibalization_min_distance_m": _safe_float(brand_profile.get("cannibalization_tolerance_m"), 1800.0),
    }
    area_fit_pass = fit_score >= thresholds["area_fit_min"]
    zoning_fit_pass = zoning_fit_score >= thresholds["zoning_fit_min"]
    frontage_access_pass = (frontage_score >= thresholds["frontage_access_min"]) and (access_score >= thresholds["frontage_access_min"])
    parking_pass = parking_score >= thresholds["parking_min"]

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
        delivery_market_pass = provider_density_score >= thresholds["delivery_provider_density_min"] and multi_platform_presence_score >= thresholds["delivery_platform_presence_min"]
    else:
        delivery_market_pass = True

    economics_pass = economics_score >= thresholds["economics_min"] and str(payback_band or "").lower() != "weak"

    gate_states: dict[str, bool | None] = {
        "zoning_fit_pass": zoning_fit_pass,
        "area_fit_pass": area_fit_pass,
        "frontage_access_pass": frontage_access_pass if road_context_available else None,
        "parking_pass": parking_pass if parking_context_available else None,
        "district_pass": district_pass,
        "cannibalization_pass": cannibalization_pass,
        "delivery_market_pass": delivery_market_pass,
        "economics_pass": economics_pass,
    }
    failed = [k for k, v in gate_states.items() if v is False]
    passed = [k for k, v in gate_states.items() if v is True]
    unknown = [k for k, v in gate_states.items() if v is None]

    core_gates = ["zoning_fit_pass", "area_fit_pass", "district_pass", "cannibalization_pass", "delivery_market_pass", "economics_pass"]
    core_pass = all(gate_states[g] is True for g in core_gates)
    overall_pass = len(failed) == 0 and core_pass

    gate_status = {
        "zoning_fit_pass": bool(zoning_fit_pass),
        "area_fit_pass": bool(area_fit_pass),
        "frontage_access_pass": bool(frontage_access_pass) if road_context_available else True,
        "parking_pass": bool(parking_pass) if parking_context_available else True,
        "district_pass": bool(district_pass),
        "cannibalization_pass": bool(cannibalization_pass),
        "delivery_market_pass": bool(delivery_market_pass),
        "economics_pass": bool(economics_pass),
        "overall_pass": overall_pass,
    }
    explanations = {
        "zoning_fit_pass": "Zoning fit compares parcel land-use compatibility against threshold.",
        "area_fit_pass": "Area fit checks candidate area against requested branch range.",
        "frontage_access_pass": "Frontage/access gate depends on road context and road-adjacent signals.",
        "parking_pass": "Parking gate depends on nearby parking amenity context and parcel suitability.",
        "district_pass": "District gate fails only for explicitly excluded districts.",
        "cannibalization_pass": "Cannibalization gate checks minimum spacing from existing branches.",
        "delivery_market_pass": "Delivery-market gate applies when primary channel is delivery.",
        "economics_pass": "Economics gate requires minimum economics score and non-weak payback.",
    }
    reasons = {"passed": passed, "failed": failed, "unknown": unknown, "thresholds": thresholds, "explanations": explanations}
    return gate_status, reasons


def _score_breakdown(
    *,
    demand_score: float,
    whitespace_score: float,
    brand_fit_score: float,
    economics_score: float,
    provider_intelligence_composite: float,
    access_visibility_score: float,
    confidence_score: float,
) -> dict[str, Any]:
    component_weights = {
        "demand_potential": 25,
        "competition_whitespace": 20,
        "brand_fit": 20,
        "occupancy_economics": 15,
        "delivery_demand": 10,
        "access_visibility": 5,
        "confidence": 5,
    }
    weighted_components = {
        "demand_potential": round(_safe_float(demand_score) * 0.25, 2),
        "competition_whitespace": round(_safe_float(whitespace_score) * 0.20, 2),
        "brand_fit": round(_safe_float(brand_fit_score) * 0.20, 2),
        "occupancy_economics": round(_safe_float(economics_score) * 0.15, 2),
        "delivery_demand": round(_safe_float(provider_intelligence_composite) * 0.10, 2),
        "access_visibility": round(_safe_float(access_visibility_score) * 0.05, 2),
        "confidence": round(_safe_float(confidence_score) * 0.05, 2),
    }
    final_score = round(sum(weighted_components.values()), 2)
    return {
        "weights": component_weights,
        "inputs": {
            "demand_potential": round(_safe_float(demand_score), 2),
            "competition_whitespace": round(_safe_float(whitespace_score), 2),
            "brand_fit": round(_safe_float(brand_fit_score), 2),
            "occupancy_economics": round(_safe_float(economics_score), 2),
            "delivery_demand": round(_safe_float(provider_intelligence_composite), 2),
            "access_visibility": round(_safe_float(access_visibility_score), 2),
            "confidence": round(_safe_float(confidence_score), 2),
        },
        "weighted_components": weighted_components,
        "final_score": round(_clamp(final_score), 2),
    }


def _top_positives_and_risks(
    *,
    candidate: dict[str, Any],
    gate_reasons: dict[str, Any],
) -> tuple[list[str], list[str]]:
    positives: list[str] = []
    risks: list[str] = []
    if _safe_float(candidate.get("demand_score")) >= 70:
        positives.append("Demand potential is strong for this district.")
    if _safe_float(candidate.get("whitespace_score")) >= 65:
        positives.append("Competitive whitespace remains favorable.")
    if _safe_float(candidate.get("brand_fit_score")) >= 70:
        positives.append("Brand-fit profile aligns with site characteristics.")
    if _safe_float(candidate.get("economics_score")) >= 65:
        positives.append("Economics profile meets target screening band.")
    if bool((candidate.get("gate_status_json") or {}).get("overall_pass")):
        positives.append("All required gates pass under available context.")

    if _safe_float(candidate.get("cannibalization_score")) >= 70:
        risks.append("Cannibalization risk is elevated versus branch network.")
    if _safe_float(candidate.get("economics_score")) < 50:
        risks.append("Economics score is below preferred threshold.")
    if _safe_float(candidate.get("delivery_competition_score")) >= 65:
        risks.append("Delivery competition intensity is high.")
    for gate in gate_reasons.get("failed") or []:
        risks.append(f"Gate failed: {gate}.")
    for gate in gate_reasons.get("unknown") or []:
        risks.append(f"Gate unknown due to missing context: {gate}.")
    return positives[:5], risks[:5]


def _confidence_grade(
    *,
    confidence_score: float,
    district: str | None,
    provider_platform_count: int | None,
    multi_platform_presence_score: float | None,
    rent_source: str,
) -> str:
    adjusted = _safe_float(confidence_score)
    if district:
        adjusted += 2.5
    if (provider_platform_count is not None and provider_platform_count > 0) or (multi_platform_presence_score is not None):
        adjusted += 2.5
    if rent_source != "conservative_default":
        adjusted += 3.0

    if adjusted >= 85.0:
        return "A"
    if adjusted >= 70.0:
        return "B"
    if adjusted >= 55.0:
        return "C"
    return "D"


def _build_demand_thesis(
    *,
    demand_score: float,
    population_reach: float,
    provider_density_score: float,
    provider_whitespace_score: float,
    delivery_competition_score: float,
) -> str:
    demand_label = "strong" if demand_score >= 70 else "moderate" if demand_score >= 50 else "limited"
    provider_label = "dense" if provider_density_score >= 65 else "steady" if provider_density_score >= 45 else "thin"
    whitespace_label = "attractive" if provider_whitespace_score >= 60 else "balanced" if provider_whitespace_score >= 40 else "tight"
    competition_label = "intense" if delivery_competition_score >= 65 else "manageable"
    return (
        f"Demand is {demand_label} (score {demand_score:.1f}) with population reach around {population_reach:.0f}; "
        f"provider activity is {provider_label}, whitespace is {whitespace_label}, and delivery competition is {competition_label}."
    )


def _build_cost_thesis(
    *,
    estimated_rent_sar_m2_year: float,
    estimated_annual_rent_sar: float,
    estimated_fitout_cost_sar: float,
    estimated_payback_months: float,
    payback_band: str,
) -> str:
    return (
        f"Estimated rent is {estimated_rent_sar_m2_year:.0f} SAR/m²/year (~{estimated_annual_rent_sar:,.0f} SAR annually), "
        f"fit-out is ~{estimated_fitout_cost_sar:,.0f} SAR, and payback is {estimated_payback_months:.1f} months ({payback_band})."
    )


def _comparable_competitors(
    db: Session,
    *,
    category: str,
    lat: float | None,
    lon: float | None,
) -> list[dict[str, Any]]:
    if lat is None or lon is None:
        return []

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




def persist_brand_profile(db: Session, search_id: str, brand_profile: dict[str, Any]) -> None:
    profile = _default_brand_profile(brand_profile)
    db.execute(
        text(
            """
            INSERT INTO expansion_brand_profile (
                id, search_id, price_tier, average_check_sar, primary_channel,
                parking_sensitivity, frontage_sensitivity, visibility_sensitivity,
                target_customer, expansion_goal, cannibalization_tolerance_m,
                preferred_districts_json, excluded_districts_json
            ) VALUES (
                :id, :search_id, :price_tier, :average_check_sar, :primary_channel,
                :parking_sensitivity, :frontage_sensitivity, :visibility_sensitivity,
                :target_customer, :expansion_goal, :cannibalization_tolerance_m,
                CAST(:preferred_districts_json AS jsonb), CAST(:excluded_districts_json AS jsonb)
            )
            ON CONFLICT (search_id) DO UPDATE SET
                price_tier = EXCLUDED.price_tier,
                average_check_sar = EXCLUDED.average_check_sar,
                primary_channel = EXCLUDED.primary_channel,
                parking_sensitivity = EXCLUDED.parking_sensitivity,
                frontage_sensitivity = EXCLUDED.frontage_sensitivity,
                visibility_sensitivity = EXCLUDED.visibility_sensitivity,
                target_customer = EXCLUDED.target_customer,
                expansion_goal = EXCLUDED.expansion_goal,
                cannibalization_tolerance_m = EXCLUDED.cannibalization_tolerance_m,
                preferred_districts_json = EXCLUDED.preferred_districts_json,
                excluded_districts_json = EXCLUDED.excluded_districts_json,
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
            "target_customer": profile.get("target_customer"),
            "expansion_goal": profile.get("expansion_goal"),
            "cannibalization_tolerance_m": profile.get("cannibalization_tolerance_m"),
            "preferred_districts_json": json.dumps(profile.get("preferred_districts") or [], ensure_ascii=False),
            "excluded_districts_json": json.dumps(profile.get("excluded_districts") or [], ensure_ascii=False),
        },
    )


def get_brand_profile(db: Session, search_id: str) -> dict[str, Any] | None:
    row = db.execute(text("""
        SELECT price_tier, average_check_sar, primary_channel, parking_sensitivity, frontage_sensitivity,
               visibility_sensitivity, target_customer, expansion_goal, cannibalization_tolerance_m,
               preferred_districts_json, excluded_districts_json
        FROM expansion_brand_profile WHERE search_id = :search_id
    """), {"search_id": search_id}).mappings().first()
    if not row:
        return None
    data = dict(row)
    data["preferred_districts"] = data.pop("preferred_districts_json") or []
    data["excluded_districts"] = data.pop("excluded_districts_json") or []
    return data


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
) -> list[dict[str, Any]]:
    bbox = bbox or {}
    min_lon = bbox.get("min_lon")
    min_lat = bbox.get("min_lat")
    max_lon = bbox.get("max_lon")
    max_lat = bbox.get("max_lat")

    existing_branches = existing_branches or []
    target_districts = target_districts or []
    target_district_norm = {normalize_district_key(item) for item in target_districts if normalize_district_key(item)}
    effective_brand_profile = _default_brand_profile(brand_profile)

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
            ), 0) AS delivery_listing_count,
            COALESCE((
                SELECT COUNT(*)
                FROM delivery_source_record dsr
                WHERE dsr.lat IS NOT NULL
                  AND dsr.lon IS NOT NULL
                  AND ST_DWithin(
                      ST_SetSRID(ST_MakePoint(dsr.lon::float, dsr.lat::float), 4326)::geography,
                      ST_SetSRID(ST_MakePoint(b.lon, b.lat), 4326)::geography,
                      :provider_radius_m
                  )
            ), 0) AS provider_listing_count,
            COALESCE((
                SELECT COUNT(DISTINCT lower(COALESCE(dsr.platform, 'unknown')))
                FROM delivery_source_record dsr
                WHERE dsr.lat IS NOT NULL
                  AND dsr.lon IS NOT NULL
                  AND ST_DWithin(
                      ST_SetSRID(ST_MakePoint(dsr.lon::float, dsr.lat::float), 4326)::geography,
                      ST_SetSRID(ST_MakePoint(b.lon, b.lat), 4326)::geography,
                      :provider_radius_m
                  )
            ), 0) AS provider_platform_count,
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
                      :provider_radius_m
                  )
            ), 0) AS delivery_competition_count
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
            "provider_radius_m": 1200,
        },
    ).mappings().all()

    candidates: list[dict[str, Any]] = []
    prepared: list[dict[str, Any]] = []
    rent_cache: dict[str | None, tuple[float, str]] = {}
    roads_table_available = _table_available(db, "public.planet_osm_line")
    parking_table_available = _table_available(db, "public.planet_osm_polygon")
    for row in rows:
        area_m2 = _safe_float(row.get("area_m2"))
        population_reach = _safe_float(row.get("population_reach"))
        competitor_count = _safe_int(row.get("competitor_count"))
        delivery_listing_count = _safe_int(row.get("delivery_listing_count"))
        provider_listing_count = _safe_int(row.get("provider_listing_count"))
        provider_platform_count = _safe_int(row.get("provider_platform_count"))
        delivery_competition_count = _safe_int(row.get("delivery_competition_count"))
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
        zoning_fit_score = _zoning_fit_score(landuse_label, landuse_code)
        fit_score = _clamp(area_fit * 0.55 + zoning_fit_score * 0.45)

        provider_density_score = _clamp((provider_listing_count / 45.0) * 100.0)
        provider_whitespace_score = _clamp(100.0 - max(0.0, (delivery_competition_count - 6) * 6.0) - min(35.0, provider_density_score * 0.2))
        multi_platform_presence_score = _clamp((provider_platform_count / 5.0) * 100.0)
        delivery_competition_score = _clamp((delivery_competition_count / 35.0) * 100.0)

        confidence_score = _confidence_score(landuse_label, population_reach, delivery_listing_count)
        distance_to_nearest_branch_m = _nearest_branch_distance_m(
            _safe_float(row.get("lat")),
            _safe_float(row.get("lon")),
            existing_branches,
        )
        cannibalization_score = _cannibalization_score(distance_to_nearest_branch_m, service_model)

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
        frontage_score = 55.0
        access_score = 55.0
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
        provider_intelligence_composite = _clamp(
            provider_density_score * 0.28
            + provider_whitespace_score * 0.30
            + multi_platform_presence_score * 0.22
            + (100.0 - delivery_competition_score) * 0.20
        )

        preliminary_breakdown = _score_breakdown(
            demand_score=demand_score,
            whitespace_score=whitespace_score,
            brand_fit_score=brand_fit_score,
            economics_score=economics_score,
            provider_intelligence_composite=provider_intelligence_composite,
            access_visibility_score=access_visibility_score,
            confidence_score=confidence_score,
        )
        prepared.append(
            {
                "row": dict(row),
                "area_m2": area_m2,
                "population_reach": population_reach,
                "competitor_count": competitor_count,
                "delivery_listing_count": delivery_listing_count,
                "provider_listing_count": provider_listing_count,
                "provider_platform_count": provider_platform_count,
                "delivery_competition_count": delivery_competition_count,
                "landuse_label": landuse_label,
                "landuse_code": landuse_code,
                "district": district,
                "demand_score": demand_score,
                "whitespace_score": whitespace_score,
                "fit_score": fit_score,
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
                "estimated_annual_rent_sar": estimated_annual_rent_sar,
                "estimated_fitout_cost_sar": estimated_fitout_cost_sar,
                "estimated_revenue_index": estimated_revenue_index,
                "economics_score": economics_score,
                "estimated_payback_months": estimated_payback_months,
                "payback_band": payback_band,
                "provider_intelligence_composite": provider_intelligence_composite,
                "preliminary_final_score": _safe_float(preliminary_breakdown.get("final_score")),
            }
        )

    prepared.sort(key=lambda item: item["preliminary_final_score"], reverse=True)
    shortlist_size = min(len(prepared), max(limit, 50))
    for prepared_item in prepared[:shortlist_size]:
        row = prepared_item["row"]
        area_m2 = prepared_item["area_m2"]
        population_reach = prepared_item["population_reach"]
        competitor_count = prepared_item["competitor_count"]
        delivery_listing_count = prepared_item["delivery_listing_count"]
        provider_listing_count = prepared_item["provider_listing_count"]
        provider_platform_count = prepared_item["provider_platform_count"]
        landuse_label = prepared_item["landuse_label"]
        landuse_code = prepared_item["landuse_code"]
        district = prepared_item["district"]
        demand_score = prepared_item["demand_score"]
        whitespace_score = prepared_item["whitespace_score"]
        fit_score = prepared_item["fit_score"]
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
        estimated_annual_rent_sar = prepared_item["estimated_annual_rent_sar"]
        estimated_fitout_cost_sar = prepared_item["estimated_fitout_cost_sar"]
        estimated_revenue_index = prepared_item["estimated_revenue_index"]
        economics_score = prepared_item["economics_score"]
        estimated_payback_months = prepared_item["estimated_payback_months"]
        payback_band = prepared_item["payback_band"]
        provider_intelligence_composite = prepared_item["provider_intelligence_composite"]

        feature_snapshot_json = _candidate_feature_snapshot(
            db,
            parcel_id=str(row.get("parcel_id") or ""),
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
        )
        road_context_available = bool((feature_snapshot_json.get("context_sources") or {}).get("road_context_available"))
        parking_context_available = bool((feature_snapshot_json.get("context_sources") or {}).get("parking_context_available"))
        frontage_score = _frontage_score(
            parcel_perimeter_m=_safe_float(feature_snapshot_json.get("parcel_perimeter_m")),
            touches_road=bool(feature_snapshot_json.get("touches_road")),
            nearby_road_count=_safe_int(feature_snapshot_json.get("nearby_road_segment_count")),
            nearest_major_road_m=_safe_float(feature_snapshot_json.get("nearest_major_road_distance_m")),
            road_context_available=road_context_available,
        )
        access_score = _access_score(
            touches_road=bool(feature_snapshot_json.get("touches_road")),
            nearest_major_road_m=_safe_float(feature_snapshot_json.get("nearest_major_road_distance_m")),
            nearby_road_count=_safe_int(feature_snapshot_json.get("nearby_road_segment_count")),
            road_context_available=road_context_available,
        )
        parking_score = _parking_score(
            area_m2=area_m2,
            service_model=service_model,
            nearby_parking_count=_safe_int(feature_snapshot_json.get("nearby_parking_amenity_count")),
            access_score=access_score,
            parking_context_available=parking_context_available,
        )
        access_visibility_score = _access_visibility_score(
            frontage_score=frontage_score,
            access_score=access_score,
            brand_profile=effective_brand_profile,
        )
        brand_fit_score = _brand_fit_score(
            district=district,
            area_m2=area_m2,
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
        score_breakdown_json = _score_breakdown(
            demand_score=demand_score,
            whitespace_score=whitespace_score,
            brand_fit_score=brand_fit_score,
            economics_score=economics_score,
            provider_intelligence_composite=provider_intelligence_composite,
            access_visibility_score=access_visibility_score,
            confidence_score=confidence_score,
        )
        final_score = _safe_float(score_breakdown_json.get("final_score"))
        key_strengths_json, key_risks_json = _build_strengths_and_risks(
            demand_score=demand_score,
            whitespace_score=whitespace_score,
            fit_score=fit_score,
            cannibalization_score=cannibalization_score,
            payback_band=payback_band,
            rent_source=rent_source,
        )
        gate_status_json, gate_reasons_json = _candidate_gate_status(
            fit_score=fit_score,
            zoning_fit_score=zoning_fit_score,
            frontage_score=frontage_score,
            access_score=access_score,
            parking_score=parking_score,
            district=district,
            distance_to_nearest_branch_m=distance_to_nearest_branch_m,
            provider_density_score=provider_density_score,
            multi_platform_presence_score=multi_platform_presence_score,
            economics_score=economics_score,
            payback_band=payback_band,
            brand_profile=effective_brand_profile,
            road_context_available=road_context_available,
            parking_context_available=parking_context_available,
        )
        confidence_grade = _confidence_grade(
            confidence_score=confidence_score,
            district=district,
            provider_platform_count=provider_platform_count,
            multi_platform_presence_score=multi_platform_presence_score,
            rent_source=rent_source,
        )
        demand_thesis = _build_demand_thesis(
            demand_score=demand_score,
            population_reach=population_reach,
            provider_density_score=provider_density_score,
            provider_whitespace_score=provider_whitespace_score,
            delivery_competition_score=delivery_competition_score,
        )
        cost_thesis = _build_cost_thesis(
            estimated_rent_sar_m2_year=estimated_rent_sar_m2_year,
            estimated_annual_rent_sar=estimated_annual_rent_sar,
            estimated_fitout_cost_sar=estimated_fitout_cost_sar,
            estimated_payback_months=estimated_payback_months,
            payback_band=payback_band,
        )
        comparable_competitors_json = _comparable_competitors(
            db,
            category=category,
            lat=_safe_float(row.get("lat")),
            lon=_safe_float(row.get("lon")),
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
        seed_candidate = {
            "demand_score": demand_score,
            "whitespace_score": whitespace_score,
            "brand_fit_score": brand_fit_score,
            "economics_score": economics_score,
            "delivery_competition_score": delivery_competition_score,
            "cannibalization_score": cannibalization_score,
            "gate_status_json": gate_status_json,
        }
        top_positives_json, top_risks_json = _top_positives_and_risks(candidate=seed_candidate, gate_reasons=gate_reasons_json)
        decision_summary = _decision_summary(
            district=district,
            final_score=final_score,
            economics_score=economics_score,
            payback_band=payback_band,
            key_risks=key_risks_json,
            service_model=service_model,
            area_m2=area_m2,
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
                "estimated_payback_months": round(estimated_payback_months, 2),
                "payback_band": payback_band,
                "gate_status_json": gate_status_json,
                "gate_reasons_json": gate_reasons_json,
                "feature_snapshot_json": feature_snapshot_json,
                "score_breakdown_json": score_breakdown_json,
                "confidence_grade": confidence_grade,
                "demand_thesis": demand_thesis,
                "cost_thesis": cost_thesis,
                "top_positives_json": top_positives_json,
                "top_risks_json": top_risks_json,
                "comparable_competitors_json": comparable_competitors_json,
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
        candidate["rank_position"] = index

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
            estimated_payback_months,
            payback_band,
            gate_status_json,
            gate_reasons_json,
            feature_snapshot_json,
            score_breakdown_json,
            confidence_grade,
            demand_thesis,
            cost_thesis,
            top_positives_json,
            top_risks_json,
            comparable_competitors_json,
            decision_summary,
            key_risks_json,
            key_strengths_json,
            compare_rank,
            rank_position,
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
            :estimated_payback_months,
            :payback_band,
            CAST(:gate_status_json AS jsonb),
            CAST(:gate_reasons_json AS jsonb),
            CAST(:feature_snapshot_json AS jsonb),
            CAST(:score_breakdown_json AS jsonb),
            :confidence_grade,
            :demand_thesis,
            :cost_thesis,
            CAST(:top_positives_json AS jsonb),
            CAST(:top_risks_json AS jsonb),
            CAST(:comparable_competitors_json AS jsonb),
            :decision_summary,
            CAST(:key_risks_json AS jsonb),
            CAST(:key_strengths_json AS jsonb),
            :compare_rank,
            :rank_position,
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
                "gate_status_json": json.dumps(candidate["gate_status_json"], ensure_ascii=False),
                "gate_reasons_json": json.dumps(candidate["gate_reasons_json"], ensure_ascii=False),
                "feature_snapshot_json": json.dumps(candidate["feature_snapshot_json"], ensure_ascii=False),
                "score_breakdown_json": json.dumps(candidate["score_breakdown_json"], ensure_ascii=False),
                "top_positives_json": json.dumps(candidate["top_positives_json"], ensure_ascii=False),
                "top_risks_json": json.dumps(candidate["top_risks_json"], ensure_ascii=False),
                "comparable_competitors_json": json.dumps(candidate["comparable_competitors_json"], ensure_ascii=False),
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
    if not row:
        return None
    payload = dict(row)
    payload["brand_profile"] = get_brand_profile(db, search_id)
    return payload


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
                estimated_payback_months,
                payback_band,
                decision_summary,
                key_risks_json,
                key_strengths_json,
                final_score,
                compare_rank,
                rank_position,
                explanation,
                computed_at
            FROM expansion_candidate
            WHERE search_id = :search_id
            ORDER BY rank_position ASC NULLS LAST, compare_rank ASC NULLS LAST, final_score DESC, computed_at DESC
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
    if search and search.get("brand_profile"):
        saved["brand_profile"] = search.get("brand_profile")
        filters_json = dict(saved.get("filters_json") or {})
        filters_json["brand_profile"] = search.get("brand_profile")
        saved["filters_json"] = filters_json
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
                estimated_payback_months,
                payback_band,
                competitor_count,
                delivery_listing_count,
                population_reach,
                landuse_label,
                rank_position
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
                "zoning_fit_score": row.get("zoning_fit_score"),
                "frontage_score": row.get("frontage_score"),
                "access_score": row.get("access_score"),
                "parking_score": row.get("parking_score"),
                "access_visibility_score": row.get("access_visibility_score"),
                "confidence_score": row.get("confidence_score"),
                "confidence_grade": row.get("confidence_grade"),
                "gate_status_json": row.get("gate_status_json") or {},
                "gate_reasons_json": row.get("gate_reasons_json") or {},
                "feature_snapshot_json": row.get("feature_snapshot_json") or {},
                "score_breakdown_json": row.get("score_breakdown_json") or {},
                "demand_thesis": row.get("demand_thesis"),
                "cost_thesis": row.get("cost_thesis"),
                "top_positives_json": row.get("top_positives_json") or [],
                "top_risks_json": row.get("top_risks_json") or [],
                "comparable_competitors_json": row.get("comparable_competitors_json") or [],
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
                "estimated_payback_months": row.get("estimated_payback_months"),
                "payback_band": row.get("payback_band"),
                "competitor_count": row.get("competitor_count"),
                "delivery_listing_count": row.get("delivery_listing_count"),
                "population_reach": row.get("population_reach"),
                "landuse_label": row.get("landuse_label"),
                "rank_position": row.get("rank_position"),
                "pros": pros,
                "cons": cons,
            }
        )

    best_overall = max(items, key=lambda item: _safe_float(item.get("final_score")))["candidate_id"]
    lowest_cannibalization = min(items, key=lambda item: _safe_float(item.get("cannibalization_score"), 9999.0))["candidate_id"]
    highest_demand = max(items, key=lambda item: _safe_float(item.get("demand_score")))["candidate_id"]
    best_fit = max(items, key=lambda item: _safe_float(item.get("fit_score")))["candidate_id"]
    best_economics = max(items, key=lambda item: _safe_float(item.get("economics_score")))["candidate_id"]
    best_brand_fit = max(items, key=lambda item: _safe_float(item.get("brand_fit_score")))["candidate_id"]
    strongest_delivery_market = max(items, key=lambda item: _safe_float(item.get("provider_density_score")) + _safe_float(item.get("multi_platform_presence_score")))["candidate_id"]
    strongest_whitespace = max(items, key=lambda item: _safe_float(item.get("provider_whitespace_score")))["candidate_id"]
    lowest_rent_burden = min(items, key=lambda item: _safe_float(item.get("estimated_annual_rent_sar"), 10**12))["candidate_id"]
    fastest_payback = min(items, key=lambda item: _safe_float(item.get("estimated_payback_months"), 10**6))["candidate_id"]
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

    return {
        "items": items,
        "summary": {
            "best_overall_candidate_id": best_overall,
            "lowest_cannibalization_candidate_id": lowest_cannibalization,
            "highest_demand_candidate_id": highest_demand,
            "best_fit_candidate_id": best_fit,
            "best_economics_candidate_id": best_economics,
            "best_brand_fit_candidate_id": best_brand_fit,
            "strongest_delivery_market_candidate_id": strongest_delivery_market,
            "strongest_whitespace_candidate_id": strongest_whitespace,
            "lowest_rent_burden_candidate_id": lowest_rent_burden,
            "fastest_payback_candidate_id": fastest_payback,
            "most_confident_candidate_id": most_confident,
            "best_gate_pass_candidate_id": best_gate_pass,
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
                c.comparable_competitors_json,
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
                c.decision_summary,
                c.rank_position
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
    )
    main_watchout = risks[0] if risks else "Validate lease and capex assumptions before commitment"
    district = candidate.get("district") or "Riyadh"
    headline = f"{verdict.upper()}: {district} parcel shows {economics_score:.1f}/100 economics for {best_use_case}"
    expansion_goal = (brand_profile.get("expansion_goal") or "balanced").replace("_", " ")
    delivery_market_summary = (
        f"For a {expansion_goal} strategy, delivery activity is {'strong' if _safe_float(candidate.get('provider_density_score')) >= 65 else 'moderate'} "
        f"with platform breadth score {_safe_float(candidate.get('multi_platform_presence_score')):.1f}/100."
    )
    competitive_context = (
        f"Provider whitespace is {_safe_float(candidate.get('provider_whitespace_score')):.1f}/100 while delivery competition is "
        f"{_safe_float(candidate.get('delivery_competition_score')):.1f}/100."
    )
    district_fit_summary = (
        f"District fit is driven by brand fit {_safe_float(candidate.get('brand_fit_score')):.1f}/100 and {('delivery-led' if (brand_profile.get('primary_channel')=='delivery') else 'balanced')} channel posture."
    )

    return {
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
            "confidence_grade": candidate.get("confidence_grade"),
            "gate_status": candidate.get("gate_status_json") or {},
            "gate_reasons": candidate.get("gate_reasons_json") or {},
            "feature_snapshot": candidate.get("feature_snapshot_json") or {},
            "score_breakdown_json": candidate.get("score_breakdown_json") or {},
            "demand_thesis": candidate.get("demand_thesis"),
            "cost_thesis": candidate.get("cost_thesis"),
            "top_positives_json": candidate.get("top_positives_json") or [],
            "top_risks_json": candidate.get("top_risks_json") or [],
            "comparable_competitors": candidate.get("comparable_competitors_json") or [],
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
            "rank_position": candidate.get("rank_position"),
        },
        "recommendation": {
            "headline": headline,
            "verdict": verdict,
            "best_use_case": best_use_case,
            "main_watchout": main_watchout,
            "gate_verdict": "pass" if bool((candidate.get("gate_status_json") or {}).get("overall_pass")) else "fail",
        },
        "market_research": {
            "delivery_market_summary": delivery_market_summary,
            "competitive_context": competitive_context,
            "district_fit_summary": district_fit_summary,
        },
    }


def get_recommendation_report(db: Session, search_id: str) -> dict[str, Any] | None:
    search = get_search(db, search_id)
    if not search:
        return None
    candidates = get_candidates(db, search_id)
    if not candidates:
        return {
            "search_id": search_id,
            "brand_profile": search.get("brand_profile") or {},
            "top_candidates": [],
            "recommendation": {},
            "assumptions": {
                "parcel_source": "arcgis_only",
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
    top = sorted(candidates, key=lambda item: _safe_float(item.get("final_score")), reverse=True)[:3]
    best = top[0]
    runner = top[1] if len(top) > 1 else None
    grade_order = {"A": 4, "B": 3, "C": 2, "D": 1}
    best_confidence = max(
        candidates,
        key=lambda item: (
            grade_order.get(str(item.get("confidence_grade") or "D"), 0),
            _safe_float(item.get("confidence_score")),
        ),
    )
    pass_candidates = [c for c in candidates if bool((c.get("gate_status_json") or {}).get("overall_pass"))]
    best_pass = max(pass_candidates or candidates, key=lambda item: _safe_float(item.get("final_score")))
    top_payload = []
    for item in top:
        payload = dict(item)
        payload["comparable_competitors_json"] = (payload.get("comparable_competitors_json") or [])[:3]
        snapshot = payload.get("feature_snapshot_json") or {}
        payload["feature_snapshot_json"] = {
            "district": snapshot.get("district"),
            "parcel_area_m2": snapshot.get("parcel_area_m2"),
            "data_completeness_score": snapshot.get("data_completeness_score"),
            "missing_context": snapshot.get("missing_context") or [],
            "touches_road": snapshot.get("touches_road"),
            "nearby_road_segment_count": snapshot.get("nearby_road_segment_count"),
            "nearest_major_road_distance_m": snapshot.get("nearest_major_road_distance_m"),
            "nearby_parking_amenity_count": snapshot.get("nearby_parking_amenity_count"),
        }
        payload["gate_verdict"] = "pass" if bool((payload.get("gate_status_json") or {}).get("overall_pass")) else "fail"
        payload["top_positives_json"] = (payload.get("top_positives_json") or [])[:3]
        payload["top_risks_json"] = (payload.get("top_risks_json") or [])[:3]
        payload["rank_position"] = payload.get("rank_position") or payload.get("compare_rank")
        top_payload.append(payload)
    return {
        "search_id": search_id,
        "brand_profile": search.get("brand_profile") or {},
        "top_candidates": top_payload,
        "recommendation": {
            "best_candidate_id": best.get("id"),
            "runner_up_candidate_id": runner.get("id") if runner else None,
            "best_pass_candidate_id": best_pass.get("id"),
            "best_confidence_candidate_id": best_confidence.get("id"),
            "why_best": f"Highest blended final score with brand fit {_safe_float(best.get('brand_fit_score')):.1f}/100 and economics {_safe_float(best.get('economics_score')):.1f}/100.",
            "main_risk": (best.get("key_risks_json") or ["Validate lease and execution assumptions"])[0],
            "best_format": _recommended_use_case(str(search.get("service_model") or "qsr"), _safe_float(best.get("area_m2"))),
            "summary": f"Recommend {best.get('district') or 'the top district'} first, then sequence {runner.get('district') if runner else 'backup options'} as runner-up.",
            "report_summary": f"Recommend {best.get('district') or 'the top district'} first, then sequence {runner.get('district') if runner else 'backup options'} as runner-up.",
        },
        "assumptions": {
            "parcel_source": "arcgis_only",
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
