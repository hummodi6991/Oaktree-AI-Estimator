from __future__ import annotations

import json
import uuid
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
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



@router.post("/searches")
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
                        "version": "expansion_advisor_v6",
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
            "version": "expansion_advisor_v6",
            "parcel_source": "arcgis_only",
            "excluded_sources": ["suhail", "inferred_parcels"],
        },
    }


@router.get("/searches/{search_id}")
def get_expansion_search(search_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    search = get_search(db, search_id)
    if not search:
        raise HTTPException(status_code=404, detail="Expansion search not found")
    return search


@router.get("/searches/{search_id}/candidates")
def get_expansion_search_candidates(search_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    search = get_search(db, search_id)
    if not search:
        raise HTTPException(status_code=404, detail="Expansion search not found")
    return {"items": get_candidates(db, search_id)}


@router.get("/searches/{search_id}/report")
def get_expansion_search_report(search_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    report = get_recommendation_report(db, search_id)
    if not report:
        raise HTTPException(status_code=404, detail="Expansion search not found")
    return report




@router.get("/candidates/{candidate_id}/memo")
def get_expansion_candidate_memo(candidate_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    memo = get_candidate_memo(db, candidate_id)
    if not memo:
        raise HTTPException(status_code=404, detail="Expansion candidate not found")
    return memo

@router.post("/candidates/compare")
def compare_expansion_candidates(
    req: CompareCandidatesRequest,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return compare_candidates(db, req.search_id, req.candidate_ids)
    except ValueError:
        raise HTTPException(status_code=404, detail="Expansion search/candidates not found")


@router.post("/saved-searches")
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


@router.get("/saved-searches")
def list_expansion_saved_searches(
    status: Literal["draft", "final"] | None = None,
    limit: int = 20,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    safe_limit = min(max(limit, 1), 100)
    return {"items": list_saved_searches(db, status=status, limit=safe_limit)}


@router.get("/saved-searches/{saved_id}")
def get_expansion_saved_search(saved_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    saved = get_saved_search(db, saved_id)
    if not saved:
        raise HTTPException(status_code=404, detail="Saved search not found")
    return saved


@router.patch("/saved-searches/{saved_id}")
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


@router.delete("/saved-searches/{saved_id}")
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
