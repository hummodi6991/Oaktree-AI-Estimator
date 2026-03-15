from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session

from app.db.deps import get_db

logger = logging.getLogger(__name__)
from app.services.expansion_advisor import (
    clear_expansion_caches,
    compare_candidates,
    create_saved_search,
    delete_saved_search,
    get_candidate_memo,
    get_candidates,
    get_recommendation_report,
    get_saved_search,
    get_search,
    list_saved_searches,
    persist_brand_profile,
    persist_existing_branches,
    run_expansion_search,
    update_saved_search,
)


from app.services.aqar_district_match import normalize_district_key
from app.api.search import normalize_search_text

router = APIRouter(prefix="/expansion-advisor", tags=["expansion-advisor"])



class DistrictOptionResponse(BaseModel):
    value: str
    label: str
    label_ar: str
    label_en: str | None = None
    aliases: list[str] = Field(default_factory=list)


class DistrictOptionsListResponse(BaseModel):
    items: list[DistrictOptionResponse]


class BranchSuggestionItem(BaseModel):
    id: str
    name: str
    district: str = ""
    lat: float
    lon: float
    source: str = ""


class BranchSuggestionsResponse(BaseModel):
    items: list[BranchSuggestionItem]


class StrictResponseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FlexibleResponseModel(BaseModel):
    model_config = ConfigDict(extra="allow")

class ExpansionAdvisorBBox(BaseModel):
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float


class ExistingBranchInput(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=256)
    lat: float
    lon: float
    district: str | None = Field(default=None, min_length=1, max_length=128)


class ExpansionBrandProfileInput(BaseModel):
    price_tier: Literal["value", "mid", "premium"] | None = None
    average_check_sar: float | None = None
    primary_channel: Literal["dine_in", "delivery", "balanced"] | None = None
    parking_sensitivity: Literal["low", "medium", "high"] | None = None
    frontage_sensitivity: Literal["low", "medium", "high"] | None = None
    visibility_sensitivity: Literal["low", "medium", "high"] | None = None
    target_customer: str | None = Field(default=None, max_length=64)
    expansion_goal: Literal["flagship", "neighborhood", "delivery_led", "balanced"] | None = None
    cannibalization_tolerance_m: float | None = None
    preferred_districts: list[str] | None = None
    excluded_districts: list[str] | None = None





class ExpansionAdvisorSearchRequest(BaseModel):
    brand_name: str = Field(..., min_length=1, max_length=256)
    category: str = Field(..., min_length=1, max_length=128)
    service_model: Literal["qsr", "dine_in", "delivery_first", "cafe"] = "qsr"
    min_area_m2: float = Field(80, ge=20, le=5000)
    max_area_m2: float = Field(500, ge=20, le=10000)
    target_area_m2: float | None = Field(None, ge=20, le=10000)
    target_districts: list[str] = Field(default_factory=list)
    existing_branches: list[ExistingBranchInput] = Field(default_factory=list)
    comparison_candidate_ids: list[str] | None = None
    bbox: ExpansionAdvisorBBox | None = None
    limit: int = Field(25, ge=1, le=100)
    brand_profile: ExpansionBrandProfileInput | None = None



class ExpansionAdvisorMeta(StrictResponseModel):
    version: str = "expansion_advisor_v6.1"
    parcel_source: str | None = None
    excluded_sources: list[str] = Field(default_factory=list)


class ExpansionAdvisorBrandProfileResponse(StrictResponseModel):
    brand_name: str | None = None
    category: str | None = None
    service_model: str | None = None
    min_area_m2: float | None = None
    max_area_m2: float | None = None
    target_area_m2: float | None = None
    target_districts: list[str] = Field(default_factory=list)
    existing_branches: list[dict[str, Any]] = Field(default_factory=list)
    brand_profile: dict[str, Any] | None = None


class ComparableCompetitorResponse(StrictResponseModel):
    id: str | None = None
    name: str | None = None
    score: float | None = None


class CandidateFeatureSnapshotResponse(FlexibleResponseModel):
    context_sources: dict[str, Any] = Field(default_factory=dict)
    missing_context: list[Any] = Field(default_factory=list)
    data_completeness_score: int | float = 0


class CandidateGateReasonsResponse(StrictResponseModel):
    passed: list[str] = Field(default_factory=list)
    failed: list[str] = Field(default_factory=list)
    unknown: list[str] = Field(default_factory=list)
    thresholds: dict[str, Any] = Field(default_factory=dict)
    explanations: dict[str, Any] = Field(default_factory=dict)


class CandidateScoreBreakdownResponse(StrictResponseModel):
    weights: dict[str, Any] = Field(default_factory=dict)
    inputs: dict[str, Any] = Field(default_factory=dict)
    weighted_components: dict[str, Any] = Field(default_factory=dict)
    display: dict[str, Any] = Field(default_factory=dict)
    final_score: float = 0.0


class ExpansionCandidateResponse(FlexibleResponseModel):
    id: str | None = None
    candidate_id: str | None = None
    search_id: str | None = None
    rank_position: int | None = None
    confidence_grade: str = "D"
    gate_status_json: dict[str, Any] = Field(default_factory=dict)
    gate_reasons_json: CandidateGateReasonsResponse = Field(default_factory=CandidateGateReasonsResponse)
    feature_snapshot_json: CandidateFeatureSnapshotResponse = Field(default_factory=CandidateFeatureSnapshotResponse)
    score_breakdown_json: CandidateScoreBreakdownResponse = Field(default_factory=CandidateScoreBreakdownResponse)
    top_positives_json: list[Any] = Field(default_factory=list)
    top_risks_json: list[Any] = Field(default_factory=list)
    comparable_competitors_json: list[Any] = Field(default_factory=list)
    demand_thesis: str = ""
    cost_thesis: str = ""
    decision_summary: str = ""



class ExpansionSearchDetailResponse(StrictResponseModel):
    id: str
    created_at: datetime | None = None
    brand_name: str | None = None
    category: str | None = None
    service_model: str | None = None
    target_districts: list[str] = Field(default_factory=list)
    min_area_m2: float | None = None
    max_area_m2: float | None = None
    target_area_m2: float | None = None
    bbox: dict[str, Any] | None = None
    request_json: dict[str, Any] = Field(default_factory=dict)
    notes: dict[str, Any] = Field(default_factory=dict)
    existing_branches: list[dict[str, Any]] = Field(default_factory=list)
    brand_profile: dict[str, Any] | None = None
    meta: ExpansionAdvisorMeta


class ExpansionSearchResponse(StrictResponseModel):
    search_id: str | None = None
    brand_profile: ExpansionAdvisorBrandProfileResponse
    items: list[ExpansionCandidateResponse]
    meta: ExpansionAdvisorMeta


class ExpansionCandidatesListResponse(StrictResponseModel):
    items: list[ExpansionCandidateResponse]
    meta: ExpansionAdvisorMeta


class CompareCandidateItemResponse(ExpansionCandidateResponse):
    pass


class CompareSummaryResponse(StrictResponseModel):
    best_overall_candidate_id: str | None = None
    lowest_cannibalization_candidate_id: str | None = None
    highest_demand_candidate_id: str | None = None
    best_fit_candidate_id: str | None = None
    best_economics_candidate_id: str | None = None
    best_brand_fit_candidate_id: str | None = None
    strongest_delivery_market_candidate_id: str | None = None
    strongest_whitespace_candidate_id: str | None = None
    lowest_rent_burden_candidate_id: str | None = None
    fastest_payback_candidate_id: str | None = None
    most_confident_candidate_id: str | None = None
    best_gate_pass_candidate_id: str | None = None


class CompareCandidatesResponse(StrictResponseModel):
    items: list[CompareCandidateItemResponse]
    summary: CompareSummaryResponse


class CandidateMemoCandidateResponse(FlexibleResponseModel):
    rank_position: int | None = None
    score_breakdown_json: CandidateScoreBreakdownResponse = Field(default_factory=CandidateScoreBreakdownResponse)
    top_positives_json: list[Any] = Field(default_factory=list)
    top_risks_json: list[Any] = Field(default_factory=list)
    gate_status: dict[str, Any] = Field(default_factory=dict)
    gate_reasons: CandidateGateReasonsResponse = Field(default_factory=CandidateGateReasonsResponse)
    feature_snapshot: CandidateFeatureSnapshotResponse = Field(default_factory=CandidateFeatureSnapshotResponse)
    comparable_competitors: list[Any] = Field(default_factory=list)


class CandidateMemoRecommendationResponse(StrictResponseModel):
    headline: str = ""
    verdict: str = ""
    best_use_case: str = ""
    main_watchout: str = ""
    gate_verdict: str = ""


class CandidateMemoMarketResearchResponse(StrictResponseModel):
    delivery_market_summary: str = ""
    competitive_context: str = ""
    district_fit_summary: str = ""


class CandidateMemoResponse(StrictResponseModel):
    candidate_id: str | None = None
    search_id: str | None = None
    brand_profile: dict[str, Any] = Field(default_factory=dict)
    candidate: CandidateMemoCandidateResponse
    recommendation: CandidateMemoRecommendationResponse
    market_research: CandidateMemoMarketResearchResponse


class RecommendationTopCandidateResponse(StrictResponseModel):
    id: str | None = None
    final_score: float | None = None
    rank_position: int | None = None
    confidence_grade: str = "D"
    gate_verdict: str = "fail"
    top_positives_json: list[Any] = Field(default_factory=list)
    top_risks_json: list[Any] = Field(default_factory=list)
    feature_snapshot_json: dict[str, Any] = Field(default_factory=dict)
    score_breakdown_json: CandidateScoreBreakdownResponse = Field(default_factory=CandidateScoreBreakdownResponse)


class RecommendationSummaryResponse(StrictResponseModel):
    best_candidate_id: str | None = None
    runner_up_candidate_id: str | None = None
    best_pass_candidate_id: str | None = None
    best_confidence_candidate_id: str | None = None
    pass_count: int = 0
    why_best: str = ""
    main_risk: str = ""
    best_format: str = ""
    summary: str = ""
    report_summary: str = ""


class RecommendationReportResponse(StrictResponseModel):
    search_id: str | None = None
    brand_profile: dict[str, Any] = Field(default_factory=dict)
    meta: ExpansionAdvisorMeta
    top_candidates: list[RecommendationTopCandidateResponse]
    recommendation: RecommendationSummaryResponse
    assumptions: dict[str, Any] = Field(default_factory=dict)


class SavedSearchResponse(StrictResponseModel):
    id: str | None = None
    search_id: str | None = None
    title: str | None = None
    description: str | None = None
    status: str | None = None
    selected_candidate_ids: list[str] = Field(default_factory=list)
    filters_json: dict[str, Any] = Field(default_factory=dict)
    ui_state_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    search: ExpansionSearchDetailResponse | None = None
    candidates: list[ExpansionCandidateResponse] = Field(default_factory=list)
    brand_profile: dict[str, Any] | None = None


class SavedSearchListResponse(StrictResponseModel):
    items: list[SavedSearchResponse]

class CompareCandidatesRequest(BaseModel):
    search_id: str = Field(..., min_length=1, max_length=36)
    candidate_ids: list[str] = Field(..., min_length=2, max_length=6)



class SavedSearchCreateRequest(BaseModel):
    search_id: str = Field(..., min_length=1, max_length=36)
    title: str = Field(..., min_length=1, max_length=256)
    description: str | None = None
    status: Literal["draft", "final"] = "draft"
    selected_candidate_ids: list[str] = Field(default_factory=list)
    filters_json: dict[str, Any] = Field(default_factory=dict)
    ui_state_json: dict[str, Any] = Field(default_factory=dict)


class SavedSearchPatchRequest(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=256)
    description: str | None = None
    status: Literal["draft", "final"] | None = None
    selected_candidate_ids: list[str] = Field(default_factory=list)
    filters_json: dict[str, Any] = Field(default_factory=dict)
    ui_state_json: dict[str, Any] = Field(default_factory=dict)



@router.get("/districts", response_model=DistrictOptionsListResponse)
def list_districts(db: Session = Depends(get_db)) -> dict[str, Any]:
    """Return deduplicated, sorted list of Riyadh districts from external_feature polygons."""
    # Riyadh metropolitan bounding box (generous to include suburbs).
    # Used to spatially filter out non-Riyadh rows that leaked in via
    # the global OSM ingest.
    rows = db.execute(
        text(
            """
            SELECT
                ef.layer_name,
                COALESCE(
                    NULLIF(ef.properties->>'district', ''),
                    NULLIF(ef.properties->>'district_raw', ''),
                    NULLIF(ef.properties->>'name', '')
                ) AS label_ar,
                NULLIF(ef.properties->>'district_en', '') AS label_en
            FROM external_feature ef
            WHERE ef.layer_name IN ('aqar_district_hulls', 'osm_districts')
              AND COALESCE(
                    NULLIF(ef.properties->>'district', ''),
                    NULLIF(ef.properties->>'district_raw', ''),
                    NULLIF(ef.properties->>'name', '')
              ) IS NOT NULL
              AND ST_Intersects(
                    ST_GeomFromGeoJSON(ef.geometry::text),
                    ST_MakeEnvelope(46.0, 24.2, 47.5, 25.2, 4326)
              )
            """
        )
    ).fetchall()

    # Build a map keyed by normalized district value.
    # Prefer aqar_district_hulls labels when both sources exist.
    LAYER_PRIORITY = {"aqar_district_hulls": 0, "osm_districts": 1}
    district_map: dict[str, dict[str, Any]] = {}

    for row in rows:
        layer_name = row[0]
        label_ar = (row[1] or "").strip()
        label_en = (row[2] or "").strip() or None
        if not label_ar:
            continue

        norm_key = normalize_district_key(label_ar)
        if not norm_key:
            continue

        existing = district_map.get(norm_key)
        if existing is None:
            district_map[norm_key] = {
                "value": norm_key,
                "label": label_ar,
                "label_ar": label_ar,
                "label_en": label_en,
                "aliases": [],
                "_priority": LAYER_PRIORITY.get(layer_name, 99),
            }
        else:
            current_priority = LAYER_PRIORITY.get(layer_name, 99)
            # Track aliases
            if label_ar != existing["label_ar"] and label_ar not in existing["aliases"]:
                existing["aliases"].append(label_ar)
            if label_en and not existing["label_en"]:
                existing["label_en"] = label_en
            # Prefer higher-priority (lower number) layer labels
            if current_priority < existing["_priority"]:
                if label_ar != existing["label_ar"]:
                    existing["aliases"].append(existing["label_ar"])
                existing["label"] = label_ar
                existing["label_ar"] = label_ar
                existing["_priority"] = current_priority

    # Sort by Arabic label and strip internal _priority field
    items = sorted(district_map.values(), key=lambda d: d["label_ar"])
    for item in items:
        item.pop("_priority", None)

    return {"items": items}


# ---------------------------------------------------------------------------
# Riyadh bounding box for branch suggestion spatial filter
# ---------------------------------------------------------------------------
_RIYADH_BBOX = {"min_lon": 46.0, "min_lat": 24.2, "max_lon": 47.5, "max_lat": 25.2}


@router.get("/branch-suggestions", response_model=BranchSuggestionsResponse)
def search_branch_suggestions(
    q: str = "",
    limit: int = 15,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Return Riyadh-only branch/location suggestions for the Expansion Advisor
    existing-branches picker.

    Searches ``restaurant_poi`` (name, name_ar, chain_name, district) and
    ``delivery_source_record`` (restaurant_name_normalized, brand_raw,
    district_text) for matches, then deduplicates by name+location proximity.
    """
    q = (q or "").strip()
    if not q or len(q) < 2:
        return {"items": []}

    safe_limit = min(max(limit, 1), 50)
    norm_q = normalize_search_text(q).lower()
    like_pattern = f"%{norm_q}%"

    # ── 1. restaurant_poi ──
    poi_items: list[dict[str, Any]] = []
    try:
        poi_rows = db.execute(
            text(
                """
                SELECT
                    id,
                    COALESCE(NULLIF(name, ''), NULLIF(name_ar, ''), chain_name) AS display_name,
                    COALESCE(district, '') AS district,
                    lat, lon,
                    'restaurant_poi' AS source
                FROM restaurant_poi
                WHERE lat BETWEEN :min_lat AND :max_lat
                  AND lon BETWEEN :min_lon AND :max_lon
                  AND (
                    LOWER(name) LIKE :q
                    OR LOWER(COALESCE(name_ar, '')) LIKE :q
                    OR LOWER(COALESCE(chain_name, '')) LIKE :q
                    OR LOWER(COALESCE(district, '')) LIKE :q
                  )
                ORDER BY review_count DESC NULLS LAST
                LIMIT :lim
                """
            ),
            {
                "q": like_pattern,
                "lim": safe_limit * 2,
                **_RIYADH_BBOX,
            },
        ).fetchall()
        for r in poi_rows:
            name_val = (r[1] or "").strip()
            if not name_val:
                continue
            poi_items.append(
                {
                    "id": str(r[0]),
                    "name": name_val,
                    "district": (r[2] or "").strip(),
                    "lat": float(r[3]),
                    "lon": float(r[4]),
                    "source": r[5],
                }
            )
    except Exception:
        logger.warning("Branch suggestion: restaurant_poi query failed", exc_info=True)

    # ── 2. delivery_source_record ──
    dsr_items: list[dict[str, Any]] = []
    try:
        dsr_rows = db.execute(
            text(
                """
                SELECT
                    id::text,
                    COALESCE(
                        NULLIF(restaurant_name_normalized, ''),
                        NULLIF(restaurant_name_raw, ''),
                        NULLIF(brand_raw, '')
                    ) AS display_name,
                    COALESCE(district_text, '') AS district,
                    lat, lon,
                    'delivery:' || platform AS source
                FROM delivery_source_record
                WHERE lat IS NOT NULL AND lon IS NOT NULL
                  AND lat BETWEEN :min_lat AND :max_lat
                  AND lon BETWEEN :min_lon AND :max_lon
                  AND (
                    LOWER(COALESCE(restaurant_name_normalized, '')) LIKE :q
                    OR LOWER(COALESCE(restaurant_name_raw, '')) LIKE :q
                    OR LOWER(COALESCE(brand_raw, '')) LIKE :q
                    OR LOWER(COALESCE(district_text, '')) LIKE :q
                    OR LOWER(COALESCE(address_raw, '')) LIKE :q
                  )
                ORDER BY rating DESC NULLS LAST
                LIMIT :lim
                """
            ),
            {
                "q": like_pattern,
                "lim": safe_limit * 2,
                **_RIYADH_BBOX,
            },
        ).fetchall()
        for r in dsr_rows:
            name_val = (r[1] or "").strip()
            if not name_val:
                continue
            dsr_items.append(
                {
                    "id": f"dsr:{r[0]}",
                    "name": name_val,
                    "district": (r[2] or "").strip(),
                    "lat": float(r[3]),
                    "lon": float(r[4]),
                    "source": r[5],
                }
            )
    except Exception:
        logger.warning("Branch suggestion: delivery_source_record query failed", exc_info=True)

    # ── 3. Deduplicate by name+proximity ──
    merged: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for item in poi_items + dsr_items:
        # Rough dedup key: lowercase name + rounded coords (≈110m grid)
        dedup_key = f"{item['name'].lower().strip()}|{round(item['lat'], 3)}|{round(item['lon'], 3)}"
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)
        merged.append(item)
        if len(merged) >= safe_limit:
            break

    return {"items": merged}


@router.post("/searches", response_model=ExpansionSearchResponse)
def create_expansion_search(
    req: ExpansionAdvisorSearchRequest,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    if req.min_area_m2 > req.max_area_m2:
        raise HTTPException(
            status_code=400,
            detail="min_area_m2 must be less than or equal to max_area_m2",
        )
    # Clear cached lookups at the start of each new search so they pick up
    # any data changes since the last request.
    clear_expansion_caches()

    target_area_m2 = req.target_area_m2 or ((req.min_area_m2 + req.max_area_m2) / 2.0)
    search_id = str(uuid.uuid4())

    # ── Canonicalize target districts ──
    # Normalize each target district string to its canonical key so that
    # garbled or variant input strings resolve to the same clean key used
    # in spatial filtering.  Preserve order and deduplicate.
    seen_td: set[str] = set()
    canonical_target_districts: list[str] = []
    for td_raw in req.target_districts:
        norm = normalize_district_key(td_raw)
        if norm and norm not in seen_td:
            seen_td.add(norm)
            canonical_target_districts.append(norm)
        elif td_raw.strip() and td_raw.strip() not in seen_td:
            # Keep the original if normalization produced nothing,
            # so we don't silently drop user input.
            seen_td.add(td_raw.strip())
            canonical_target_districts.append(td_raw.strip())
    req_target_districts = canonical_target_districts

    request_json = req.model_dump()
    bbox_json = req.bbox.model_dump() if req.bbox else None
    existing_branches_payload = [branch.model_dump() for branch in req.existing_branches]
    brand_profile_payload = req.brand_profile.model_dump() if req.brand_profile else None

    try:
        db.execute(
            text(
                """
                INSERT INTO expansion_search (
                    id,
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
                ) VALUES (
                    :id,
                    :brand_name,
                    :category,
                    :service_model,
                    CAST(:target_districts AS jsonb),
                    :min_area_m2,
                    :max_area_m2,
                    :target_area_m2,
                    CAST(:bbox AS jsonb),
                    CAST(:request_json AS jsonb),
                    CAST(:notes AS jsonb)
                )
                """
            ),
            {
                "id": search_id,
                "brand_name": req.brand_name,
                "category": req.category,
                "service_model": req.service_model,
                "target_districts": json.dumps(req_target_districts, ensure_ascii=False),
                "min_area_m2": req.min_area_m2,
                "max_area_m2": req.max_area_m2,
                "target_area_m2": target_area_m2,
                "bbox": json.dumps(bbox_json, ensure_ascii=False) if bbox_json else None,
                "request_json": json.dumps(request_json, ensure_ascii=False),
                "notes": json.dumps(
                    {
                        "version": "expansion_advisor_v6.1",
                        "parcel_source": "arcgis_only",
                        "excluded_sources": ["suhail", "inferred_parcels"],
                    },
                    ensure_ascii=False,
                ),
            },
        )

        persist_existing_branches(db, search_id, existing_branches_payload)
        if brand_profile_payload:
            persist_brand_profile(db, search_id, brand_profile_payload)

        items = run_expansion_search(
            db=db,
            search_id=search_id,
            brand_name=req.brand_name,
            category=req.category,
            service_model=req.service_model,
            min_area_m2=req.min_area_m2,
            max_area_m2=req.max_area_m2,
            target_area_m2=target_area_m2,
            limit=req.limit,
            bbox=bbox_json,
            target_districts=req_target_districts,
            existing_branches=existing_branches_payload,
            brand_profile=brand_profile_payload,
        )
        db.commit()
    except Exception as exc:
        logger.exception(
            "Expansion search failed: search_id=%s brand=%s category=%s "
            "service_model=%s districts=%s existing_branches_count=%d "
            "brand_profile_keys=%s exc_type=%s exc_msg=%s",
            search_id,
            req.brand_name,
            req.category,
            req.service_model,
            req.target_districts,
            len(existing_branches_payload),
            list((brand_profile_payload or {}).keys()),
            type(exc).__name__,
            str(exc)[:500],
        )
        try:
            db.rollback()
        except Exception:
            logger.debug("rollback failed after search error", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Expansion search failed due to an internal error. "
            f"search_id={search_id} has been logged for investigation.",
        )

    pass_count = sum(
        1 for item in items
        if (item.get("gate_status_json") or {}).get("overall_pass") is True
    )
    logger.info(
        "Search completed: search_id=%s candidate_count=%d pass_count=%d "
        "brand=%s category=%s districts=%s",
        search_id, len(items), pass_count,
        req.brand_name, req.category, req_target_districts,
    )

    return {
        "search_id": search_id,
        "brand_profile": {
            "brand_name": req.brand_name,
            "category": req.category,
            "service_model": req.service_model,
            "min_area_m2": req.min_area_m2,
            "max_area_m2": req.max_area_m2,
            "target_area_m2": target_area_m2,
            "target_districts": req_target_districts,
            "existing_branches": existing_branches_payload,
            "brand_profile": brand_profile_payload,
        },
        "items": items,
        "meta": {
            "version": "expansion_advisor_v6.1",
            "parcel_source": "arcgis_only",
            "excluded_sources": ["suhail", "inferred_parcels"],
        },
    }


@router.get("/searches/{search_id}", response_model=ExpansionSearchDetailResponse)
def get_expansion_search(search_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    search = get_search(db, search_id)
    if not search:
        raise HTTPException(status_code=404, detail="Expansion search not found")
    return search


@router.get("/searches/{search_id}/candidates", response_model=ExpansionCandidatesListResponse)
def get_expansion_search_candidates(search_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    search = get_search(db, search_id)
    if not search:
        raise HTTPException(status_code=404, detail="Expansion search not found")
    return {"items": get_candidates(db, search_id), "meta": {"version": "expansion_advisor_v6.1"}}


@router.get("/searches/{search_id}/report", response_model=RecommendationReportResponse)
def get_expansion_search_report(search_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    import time as _time
    t0 = _time.monotonic()

    # Verify the search exists first to distinguish 404 from 500.
    search = get_search(db, search_id)
    if not search:
        logger.info("Report: search not found search_id=%s", search_id)
        raise HTTPException(status_code=404, detail="Expansion search not found")

    try:
        report = get_recommendation_report(db, search_id)
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        # Recoverable data-shape errors: return sparse report with degraded flag.
        logger.warning(
            "Report generation failed (recoverable %s): search_id=%s elapsed=%.2fs detail=%s",
            type(exc).__name__, search_id, _time.monotonic() - t0, str(exc)[:200],
            exc_info=True,
        )
        report = {
            "search_id": search_id,
            "brand_profile": search.get("brand_profile") or search.get("request_json") or {},
            "meta": {"version": "expansion_advisor_v6.1", "degraded": True, "error_class": type(exc).__name__},
            "top_candidates": [],
            "recommendation": {
                "best_candidate_id": None,
                "runner_up_candidate_id": None,
                "best_pass_candidate_id": None,
                "best_confidence_candidate_id": None,
                "pass_count": 0,
                "validation_clear_count": 0,
                "why_best": "",
                "main_risk": "Report generation encountered an error — results may be incomplete.",
                "best_format": "",
                "summary": "Report could not be fully generated. The search results are still available.",
                "report_summary": "",
            },
            "assumptions": {"parcel_source": "arcgis_only", "city": "riyadh"},
        }
    except Exception:
        # Non-recoverable errors (DB connection, infrastructure): let them surface as 500.
        logger.exception(
            "Report generation failed (non-recoverable): search_id=%s elapsed=%.2fs",
            search_id, _time.monotonic() - t0,
        )
        raise

    if not report:
        # get_recommendation_report returned None (search found but no candidates)
        logger.info("Report: no candidates for search_id=%s", search_id)
        report = {
            "search_id": search_id,
            "brand_profile": search.get("brand_profile") or {},
            "meta": {"version": "expansion_advisor_v6.1"},
            "top_candidates": [],
            "recommendation": {
                "best_candidate_id": None,
                "runner_up_candidate_id": None,
                "best_pass_candidate_id": None,
                "best_confidence_candidate_id": None,
                "pass_count": 0,
                "why_best": "",
                "main_risk": "",
                "best_format": "",
                "summary": "",
                "report_summary": "",
            },
            "assumptions": {},
        }

    rec = report.get("recommendation", {})
    elapsed = _time.monotonic() - t0
    logger.info(
        "Report served: search_id=%s pass_count=%s top_candidates=%d "
        "best_candidate=%s best_pass=%s elapsed=%.2fs",
        search_id,
        rec.get("pass_count"),
        len(report.get("top_candidates", [])),
        (rec.get("best_candidate_id") or "none")[:8],
        (rec.get("best_pass_candidate_id") or "none")[:8],
        elapsed,
    )
    return report




@router.get("/candidates/{candidate_id}/memo", response_model=CandidateMemoResponse)
def get_expansion_candidate_memo(candidate_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    memo = get_candidate_memo(db, candidate_id)
    if not memo:
        raise HTTPException(status_code=404, detail="Expansion candidate not found")
    return memo

@router.post("/candidates/compare", response_model=CompareCandidatesResponse)
def compare_expansion_candidates(
    req: CompareCandidatesRequest,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return compare_candidates(db, req.search_id, req.candidate_ids)
    except ValueError:
        raise HTTPException(status_code=404, detail="Expansion search/candidates not found")


@router.post("/saved-searches", response_model=SavedSearchResponse)
def create_expansion_saved_search(req: SavedSearchCreateRequest, db: Session = Depends(get_db)) -> dict[str, Any]:
    if not get_search(db, req.search_id):
        raise HTTPException(status_code=404, detail="Expansion search not found")
    try:
        saved = create_saved_search(
            db,
            search_id=req.search_id,
            title=req.title,
            description=req.description,
            status=req.status,
            selected_candidate_ids=req.selected_candidate_ids,
            filters_json=req.filters_json,
            ui_state_json=req.ui_state_json,
        )
        db.commit()
        return saved
    except Exception:
        db.rollback()
        raise


@router.get("/saved-searches", response_model=SavedSearchListResponse)
def list_expansion_saved_searches(
    status: Literal["draft", "final"] | None = None,
    limit: int = 20,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    safe_limit = min(max(limit, 1), 100)
    try:
        return {"items": list_saved_searches(db, status=status, limit=safe_limit)}
    except ProgrammingError:
        # Table may not exist if the migration hasn't been applied yet.
        # Return an empty list instead of a 500 so the UI shows a clean
        # empty state rather than a misleading error alert.
        db.rollback()
        logger.debug("saved-searches table not queryable, returning empty list")
        return {"items": []}


@router.get("/saved-searches/{saved_id}", response_model=SavedSearchResponse)
def get_expansion_saved_search(saved_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    saved = get_saved_search(db, saved_id)
    if not saved:
        raise HTTPException(status_code=404, detail="Saved search not found")
    return saved


@router.patch("/saved-searches/{saved_id}", response_model=SavedSearchResponse)
def patch_expansion_saved_search(
    saved_id: str,
    req: SavedSearchPatchRequest,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    payload = req.model_dump(exclude_unset=True)
    try:
        saved = update_saved_search(db, saved_id, payload)
        if not saved:
            db.rollback()
            raise HTTPException(status_code=404, detail="Saved search not found")
        db.commit()
        return saved
    except HTTPException:
        raise
    except Exception:
        db.rollback()
        raise


@router.delete("/saved-searches/{saved_id}", response_model=dict[str, bool])
def delete_expansion_saved_search(saved_id: str, db: Session = Depends(get_db)) -> dict[str, bool]:
    try:
        deleted = delete_saved_search(db, saved_id)
        if not deleted:
            db.rollback()
            raise HTTPException(status_code=404, detail="Saved search not found")
        db.commit()
        return {"deleted": True}
    except HTTPException:
        raise
    except Exception:
        db.rollback()
        raise
