from __future__ import annotations

import json
import uuid
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.deps import get_db
from app.services.expansion_advisor import (
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


router = APIRouter(prefix="/expansion-advisor", tags=["expansion-advisor"])



class FlexibleModel(BaseModel):
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



class ExpansionAdvisorMeta(FlexibleModel):
    version: str = "expansion_advisor_v6.1"
    parcel_source: str | None = None
    excluded_sources: list[str] = Field(default_factory=list)


class ExpansionAdvisorBrandProfileResponse(FlexibleModel):
    brand_name: str | None = None
    category: str | None = None
    service_model: str | None = None
    min_area_m2: float | None = None
    max_area_m2: float | None = None
    target_area_m2: float | None = None
    target_districts: list[str] = Field(default_factory=list)
    existing_branches: list[dict[str, Any]] = Field(default_factory=list)
    brand_profile: dict[str, Any] | None = None


class ComparableCompetitorResponse(FlexibleModel):
    id: str | None = None
    name: str | None = None
    score: float | None = None


class CandidateFeatureSnapshotResponse(FlexibleModel):
    context_sources: dict[str, Any] = Field(default_factory=dict)
    missing_context: list[Any] = Field(default_factory=list)
    data_completeness_score: int | float = 0


class CandidateGateReasonsResponse(FlexibleModel):
    passed: list[str] = Field(default_factory=list)
    failed: list[str] = Field(default_factory=list)
    unknown: list[str] = Field(default_factory=list)
    thresholds: dict[str, Any] = Field(default_factory=dict)
    explanations: dict[str, Any] = Field(default_factory=dict)


class CandidateScoreBreakdownResponse(FlexibleModel):
    weights: dict[str, Any] = Field(default_factory=dict)
    inputs: dict[str, Any] = Field(default_factory=dict)
    weighted_components: dict[str, Any] = Field(default_factory=dict)
    final_score: float = 0.0


class ExpansionCandidateResponse(FlexibleModel):
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


class ExpansionSearchResponse(FlexibleModel):
    search_id: str | None = None
    brand_profile: ExpansionAdvisorBrandProfileResponse
    items: list[ExpansionCandidateResponse]
    meta: ExpansionAdvisorMeta


class ExpansionCandidatesListResponse(FlexibleModel):
    items: list[ExpansionCandidateResponse]
    meta: ExpansionAdvisorMeta


class CompareCandidateItemResponse(ExpansionCandidateResponse):
    pass


class CompareSummaryResponse(FlexibleModel):
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


class CompareCandidatesResponse(FlexibleModel):
    items: list[CompareCandidateItemResponse]
    summary: CompareSummaryResponse


class CandidateMemoCandidateResponse(FlexibleModel):
    rank_position: int | None = None
    score_breakdown_json: CandidateScoreBreakdownResponse = Field(default_factory=CandidateScoreBreakdownResponse)
    top_positives_json: list[Any] = Field(default_factory=list)
    top_risks_json: list[Any] = Field(default_factory=list)
    gate_status: dict[str, Any] = Field(default_factory=dict)
    gate_reasons: CandidateGateReasonsResponse = Field(default_factory=CandidateGateReasonsResponse)
    feature_snapshot: CandidateFeatureSnapshotResponse = Field(default_factory=CandidateFeatureSnapshotResponse)
    comparable_competitors: list[Any] = Field(default_factory=list)


class CandidateMemoRecommendationResponse(FlexibleModel):
    headline: str = ""
    verdict: str = ""
    best_use_case: str = ""
    main_watchout: str = ""
    gate_verdict: str = ""


class CandidateMemoMarketResearchResponse(FlexibleModel):
    delivery_market_summary: str = ""
    competitive_context: str = ""
    district_fit_summary: str = ""


class CandidateMemoResponse(FlexibleModel):
    candidate_id: str | None = None
    search_id: str | None = None
    brand_profile: dict[str, Any] = Field(default_factory=dict)
    candidate: CandidateMemoCandidateResponse
    recommendation: CandidateMemoRecommendationResponse
    market_research: CandidateMemoMarketResearchResponse


class RecommendationTopCandidateResponse(FlexibleModel):
    id: str | None = None
    final_score: float | None = None
    rank_position: int | None = None
    confidence_grade: str = "D"
    gate_verdict: str = "fail"
    top_positives_json: list[Any] = Field(default_factory=list)
    top_risks_json: list[Any] = Field(default_factory=list)
    feature_snapshot_json: dict[str, Any] = Field(default_factory=dict)
    score_breakdown_json: CandidateScoreBreakdownResponse = Field(default_factory=CandidateScoreBreakdownResponse)


class RecommendationSummaryResponse(FlexibleModel):
    best_candidate_id: str | None = None
    runner_up_candidate_id: str | None = None
    best_pass_candidate_id: str | None = None
    best_confidence_candidate_id: str | None = None
    why_best: str = ""
    main_risk: str = ""
    best_format: str = ""
    summary: str = ""
    report_summary: str = ""


class RecommendationReportResponse(FlexibleModel):
    search_id: str | None = None
    brand_profile: dict[str, Any] = Field(default_factory=dict)
    meta: ExpansionAdvisorMeta
    top_candidates: list[RecommendationTopCandidateResponse]
    recommendation: RecommendationSummaryResponse
    assumptions: dict[str, Any] = Field(default_factory=dict)


class SavedSearchResponse(FlexibleModel):
    id: str | None = None
    search_id: str | None = None
    title: str | None = None
    description: str | None = None
    status: str | None = None
    selected_candidate_ids: list[str] | None = None
    filters_json: dict[str, Any] | None = None
    ui_state_json: dict[str, Any] | None = None
    search: dict[str, Any] | None = None
    candidates: list[dict[str, Any]] | None = None
    brand_profile: dict[str, Any] | None = None


class SavedSearchListResponse(FlexibleModel):
    items: list[SavedSearchResponse]

class CompareCandidatesRequest(BaseModel):
    search_id: str = Field(..., min_length=1, max_length=36)
    candidate_ids: list[str] = Field(..., min_length=2, max_length=6)



class SavedSearchCreateRequest(BaseModel):
    search_id: str = Field(..., min_length=1, max_length=36)
    title: str = Field(..., min_length=1, max_length=256)
    description: str | None = None
    status: Literal["draft", "final"] = "draft"
    selected_candidate_ids: list[str] | None = None
    filters_json: dict[str, Any] | None = None
    ui_state_json: dict[str, Any] | None = None


class SavedSearchPatchRequest(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=256)
    description: str | None = None
    status: Literal["draft", "final"] | None = None
    selected_candidate_ids: list[str] | None = None
    filters_json: dict[str, Any] | None = None
    ui_state_json: dict[str, Any] | None = None



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

    target_area_m2 = req.target_area_m2 or ((req.min_area_m2 + req.max_area_m2) / 2.0)
    search_id = str(uuid.uuid4())

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
                "target_districts": json.dumps(req.target_districts, ensure_ascii=False),
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
            target_districts=req.target_districts,
            existing_branches=existing_branches_payload,
            brand_profile=brand_profile_payload,
        )
        db.commit()
    except Exception:
        db.rollback()
        raise

    return {
        "search_id": search_id,
        "brand_profile": {
            "brand_name": req.brand_name,
            "category": req.category,
            "service_model": req.service_model,
            "min_area_m2": req.min_area_m2,
            "max_area_m2": req.max_area_m2,
            "target_area_m2": target_area_m2,
            "target_districts": req.target_districts,
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


@router.get("/searches/{search_id}", response_model=dict[str, Any])
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
    report = get_recommendation_report(db, search_id)
    if not report:
        raise HTTPException(status_code=404, detail="Expansion search not found")
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
    return {"items": list_saved_searches(db, status=status, limit=safe_limit)}


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
