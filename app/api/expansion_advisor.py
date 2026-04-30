from __future__ import annotations

import json
import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Literal

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from datetime import datetime
import time as _prewarm_time

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db import session as db_session
from app.db.deps import get_db

logger = logging.getLogger(__name__)
from app.services.expansion_advisor import (
    _cached_district_lookup,
    _resolve_district_to_ar_key,
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


from app.services.llm_decision_memo import (
    MEMO_PROMPT_VERSION,
    build_memo_context,
    generate_decision_memo,
    generate_structured_memo,
    render_structured_memo_as_text,
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
    limit: int = Field(15, ge=1, le=100)
    brand_profile: ExpansionBrandProfileInput | None = None



class ExpansionAdvisorMeta(StrictResponseModel):
    version: str = "expansion_advisor_v7"
    parcel_source: str | None = None
    excluded_sources: list[str] = Field(default_factory=list)
    degraded: bool = False
    error_class: str | None = None


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
    display_score: float | None = None
    economics_detail: dict[str, Any] = Field(default_factory=dict)


class ExpansionCandidateResponse(FlexibleResponseModel):
    id: str | None = None
    candidate_id: str | None = None
    search_id: str | None = None
    rank_position: int | None = None
    confidence_grade: str = "D"
    display_annual_rent_sar: float | None = None
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
    # Phase 3 chunk 1: rerank metadata persisted on the candidate row.
    # Declared for documentation; FlexibleResponseModel (extra="allow") would
    # surface them either way, but the explicit fields make the contract
    # discoverable via /openapi.json for the frontend.
    deterministic_rank: int | None = None
    final_rank: int | None = None
    rerank_applied: bool = False
    rerank_reason: dict[str, Any] | None = None
    rerank_delta: int = 0
    rerank_status: str | None = None
    # True iff the candidate row has either decision_memo (legacy text) or
    # decision_memo_json (structured) populated. Frontend uses this to
    # enable the "View decision memo" affordance without fetching the full
    # multi-KB memo for every list item.
    decision_memo_present: bool = False
    # value_score chip — derived "strong location at a fair price" signal.
    # Geometric mean of estimated_revenue_index and rent_burden_score; see
    # app/services/expansion_advisor.py:_value_score. Null on rows where
    # rent_burden ran in absolute_legacy / absolute_fallback / envelope
    # modes (no peer-relative comparison defensible). value_band is one of
    # "best_value" | "neutral" | "above_market" or null. The
    # *_low_confidence flag is True when the comp pool is citywide rather
    # than district-scoped — the badge is amber rather than green/red.
    value_score: float | None = None
    value_band: str | None = None
    value_band_low_confidence: bool = False
    value_downrank_applied: bool = False
    value_downrank_delta: int = 0
    value_uprank_applied: bool = False
    value_uprank_delta: int = 0



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
    notes: dict[str, Any] = {}
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
    # lowest_rent_burden_candidate_id: smallest absolute annual rent across
    # the compared set. Intentionally distinct from
    # best_value_candidate_id; the Compare panel's "Lowest Rent Burden"
    # tile keeps its existing semantics.
    lowest_rent_burden_candidate_id: str | None = None
    # best_value_candidate_id: highest derived value_score (geometric mean
    # of estimated_revenue_index and rent_burden_score). Independent peer
    # of lowest_rent_burden_candidate_id.
    best_value_candidate_id: str | None = None
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
    # Commercial-unit / listing fields. Same shape the list endpoint emits
    # via ExpansionCandidateResponse, so the memo's quick-facts row reads
    # Area / Street width from the same source as the candidate list card.
    source_type: str | None = None
    commercial_unit_id: str | None = None
    listing_url: str | None = None
    image_url: str | None = None
    unit_price_sar_annual: float | None = None
    unit_area_sqm: float | None = None
    unit_street_width_m: float | None = None
    display_annual_rent_sar: float | None = None
    # Rerank metadata persisted on expansion_candidate. Lives on the nested
    # candidate object — same shape the list endpoint exposes — so
    # DecisionLogicCard reads it from `data.candidate.*` like every other
    # candidate-scoped field. With EXPANSION_LLM_RERANK_ENABLED=False (the
    # default) deterministic_rank == final_rank and rerank_status is "flag_off".
    deterministic_rank: int | None = None
    final_rank: int | None = None
    rerank_applied: bool = False
    rerank_reason: dict[str, Any] | None = None
    rerank_delta: int = 0
    rerank_status: str | None = None
    # value_score chip (see ExpansionCandidateResponse for semantics). Lives
    # on the nested candidate object so the memo endpoint exposes the same
    # shape as the list endpoint.
    value_score: float | None = None
    value_band: str | None = None
    value_band_low_confidence: bool = False
    value_downrank_applied: bool = False
    value_downrank_delta: int = 0
    value_uprank_applied: bool = False
    value_uprank_delta: int = 0


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
    # ``decision_memo`` is the legacy rendered text; ``decision_memo_json``
    # is the structured object (headline_recommendation, ranking_explanation,
    # key_evidence, risks, comparison, bottom_line) populated by POST
    # /decision-memo or by the pre-warm background task on POST /searches.
    # Both describe the memo envelope, not a per-candidate property — they
    # stay at the top level. Per-candidate rerank metadata
    # (deterministic_rank / final_rank / rerank_*) lives on
    # CandidateMemoCandidateResponse, matching the list endpoint's shape.
    decision_memo: str | None = None
    decision_memo_json: dict[str, Any] | None = None


class RecommendationTopCandidateResponse(StrictResponseModel):
    id: str | None = None
    final_score: float | None = None
    rank_position: int | None = None
    confidence_grade: str = "D"
    gate_verdict: str = "fail"
    district: str | None = None
    district_key: str | None = None
    district_name_ar: str | None = None
    district_name_en: str | None = None
    district_display: str | None = None
    top_positives_json: list[Any] = Field(default_factory=list)
    top_risks_json: list[Any] = Field(default_factory=list)
    feature_snapshot_json: dict[str, Any] = Field(default_factory=dict)
    score_breakdown_json: CandidateScoreBreakdownResponse = Field(default_factory=CandidateScoreBreakdownResponse)
    # value_score chip surfaced on the report panel's top-3 cards.
    value_score: float | None = None
    value_band: str | None = None
    value_band_low_confidence: bool = False


class RecommendationSummaryResponse(StrictResponseModel):
    best_candidate_id: str | None = None
    runner_up_candidate_id: str | None = None
    best_pass_candidate_id: str | None = None
    best_confidence_candidate_id: str | None = None
    # Dimension Winners — populated server-side as of this PR (Bug B fix).
    # The frontend ExpansionReportPanel.tsx was already reading these and
    # rendering nothing. best_value_candidate_id is a new peer.
    highest_demand_candidate_id: str | None = None
    best_economics_candidate_id: str | None = None
    best_brand_fit_candidate_id: str | None = None
    strongest_whitespace_candidate_id: str | None = None
    most_confident_candidate_id: str | None = None
    best_value_candidate_id: str | None = None
    pass_count: int = 0
    validation_clear_count: int = 0
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


def _prewarm_decision_memos(
    search_id: str,
    candidate_specs: list[dict[str, Any]],
    brand_profile: dict[str, Any] | None,
) -> None:
    """Background task: generate structured decision memos for the top-N
    candidates so the first tap in the UI is instant.

    Runs in a fresh DB session — FastAPI's per-request session has already
    been closed by the time BackgroundTasks fires the callback. Per-candidate
    failures are caught and logged; one bad memo cannot abort the batch.

    Wall-clock budget semantics (``EXPANSION_MEMO_PREWARM_BUDGET_S``):
      * > 0 → enforced. The budget check runs at the END of each iteration,
        so the first candidate is ALWAYS attempted regardless of how small
        the budget is. Subsequent iterations abort early when
        ``elapsed >= budget`` and log "budget exhausted".
      * <= 0 → treated as UNBOUNDED (no wall-clock gate). Use the
        ``ENABLED`` / ``TOP_N`` knobs to disable pre-warm entirely; the
        budget is an LLM-stuck-call safety valve, not an on/off switch.

    ``candidate_specs`` is a list of {id, parcel_id, ...candidate-fields}
    dicts pre-built from the in-memory search result, so the task does not
    need to re-query the DB to know what to warm.
    """
    if not settings.EXPANSION_MEMO_PREWARM_ENABLED:
        return
    top_n = max(0, int(settings.EXPANSION_MEMO_PREWARM_TOP_N))
    if top_n == 0 or not candidate_specs:
        return

    targets = candidate_specs[:top_n]
    budget = float(settings.EXPANSION_MEMO_PREWARM_BUDGET_S)
    budget_enforced = budget > 0
    concurrency = max(1, int(settings.EXPANSION_MEMO_PREWARM_CONCURRENCY))
    started_at = _prewarm_time.monotonic()

    counters = {"generated": 0, "skipped": 0, "failed": 0}
    counters_lock = threading.Lock()

    logger.info(
        "expansion_memo_prewarm start: search_id=%s top_n=%d budget_s=%s",
        search_id, len(targets),
        f"{budget:.0f}" if budget_enforced else "unbounded",
    )

    def _process_one(spec: dict[str, Any]) -> None:
        # Each worker opens its own DB session — DO NOT share a session
        # across threads. SQLAlchemy Session objects are not thread-safe.
        # Per-candidate exceptions are swallowed and logged so one bad
        # memo cannot crash the batch. The daily-cost ceiling check in
        # ``llm_decision_memo._check_daily_ceiling`` is intentionally
        # unlocked: under concurrency, up to N-1 calls may slip past the
        # ceiling on the same tick (acceptable at the $5/day cap).
        parcel_id = spec.get("parcel_id")
        if not parcel_id:
            with counters_lock:
                counters["skipped"] += 1
            return
        db: Session = db_session.SessionLocal()
        try:
            cached = _decision_memo_cache_lookup(
                db, search_id, str(parcel_id)
            )
            if cached is not None and cached[1] is not None:
                with counters_lock:
                    counters["skipped"] += 1
                logger.info(
                    "expansion_memo_prewarm skip cached: "
                    "search_id=%s parcel_id=%s",
                    search_id, parcel_id,
                )
                return
            ctx = build_memo_context(
                candidate=spec,
                brief={"brand_profile": brand_profile or {}},
                lang="en",
            )
            memo_json = generate_structured_memo(ctx)
            if memo_json is None:
                with counters_lock:
                    counters["skipped"] += 1
                logger.info(
                    "expansion_memo_prewarm skip (no memo): "
                    "search_id=%s parcel_id=%s",
                    search_id, parcel_id,
                )
                return
            memo_text = render_structured_memo_as_text(memo_json, "en")
            _decision_memo_cache_write(
                db, search_id, str(parcel_id), memo_text, memo_json,
            )
            with counters_lock:
                counters["generated"] += 1
            logger.info(
                "expansion_memo_prewarm ok: "
                "search_id=%s parcel_id=%s",
                search_id, parcel_id,
            )
        except Exception:
            with counters_lock:
                counters["failed"] += 1
            logger.warning(
                "expansion_memo_prewarm fail: "
                "search_id=%s parcel_id=%s",
                search_id, parcel_id,
                exc_info=True,
            )
        finally:
            try:
                db.close()
            except Exception:
                logger.debug(
                    "expansion_memo_prewarm: db close failed",
                    exc_info=True,
                )

    budget_breached = False
    executor = ThreadPoolExecutor(max_workers=concurrency)
    try:
        futures = [executor.submit(_process_one, spec) for spec in targets]
        for future in as_completed(futures):
            # _process_one catches its own exceptions; this .result() is
            # defensive and surfaces any unexpected error from the worker.
            try:
                future.result()
            except Exception:
                logger.debug(
                    "expansion_memo_prewarm: unexpected worker exception",
                    exc_info=True,
                )
            if budget_enforced:
                elapsed = _prewarm_time.monotonic() - started_at
                if elapsed >= budget:
                    budget_breached = True
                    remaining = 0
                    for f in futures:
                        if not f.done() and f.cancel():
                            remaining += 1
                    with counters_lock:
                        snapshot = dict(counters)
                    logger.info(
                        "expansion_memo_prewarm budget exhausted: "
                        "search_id=%s elapsed=%.2fs budget=%.2fs "
                        "generated=%d skipped=%d failed=%d remaining=%d",
                        search_id, elapsed, budget,
                        snapshot["generated"], snapshot["skipped"],
                        snapshot["failed"], remaining,
                    )
                    break
    finally:
        if budget_breached:
            executor.shutdown(wait=False, cancel_futures=True)
        else:
            executor.shutdown(wait=True)

    with counters_lock:
        snapshot = dict(counters)
    logger.info(
        "expansion_memo_prewarm done: search_id=%s wall_s=%.2f "
        "generated=%d skipped=%d failed=%d",
        search_id,
        _prewarm_time.monotonic() - started_at,
        snapshot["generated"], snapshot["skipped"], snapshot["failed"],
    )


def _build_prewarm_specs(
    items: list[dict[str, Any]],
    top_n: int,
) -> list[dict[str, Any]]:
    """Pick the top-N candidates by ``final_rank`` (falling back to
    ``rank_position``) and snapshot the fields ``build_memo_context``
    needs. Snapshotting keeps the background task independent of the
    request session and avoids a second DB round-trip."""
    if top_n <= 0 or not items:
        return []

    def _key(item: dict[str, Any]) -> tuple[int, int]:
        fr = item.get("final_rank")
        rp = item.get("rank_position")
        primary = fr if isinstance(fr, int) else (rp if isinstance(rp, int) else 10**9)
        secondary = rp if isinstance(rp, int) else 10**9
        return (primary, secondary)

    ranked = sorted(items, key=_key)[:top_n]
    return [dict(item) for item in ranked]


@router.post("/searches", response_model=ExpansionSearchResponse)
def create_expansion_search(
    req: ExpansionAdvisorSearchRequest,
    background_tasks: BackgroundTasks,
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
    # ── Resolve English district names to Arabic keys ──
    # The candidate_location table stores district_ar in Arabic.  When the
    # user (or frontend) sends English names like "Al Yasmin", we need to
    # map them to the Arabic keys ("الياسمين") so the SQL filter matches.
    district_lookup = _cached_district_lookup(db)
    resolved_target_districts: list[str] = []
    for td in canonical_target_districts:
        resolved = _resolve_district_to_ar_key(td, district_lookup)
        # Pass-through on miss preserves user input so downstream SQL
        # filtering sees the raw string and misses naturally.
        resolved_target_districts.append(resolved if resolved else td)
    req_target_districts = resolved_target_districts

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
                        "version": "expansion_advisor_v7",
                        "parcel_source": "listings_only",
                        "candidate_sources": ["aqar", "wasalt", "bayut"],
                        "excluded_sources": ["arcgis_parcels", "hungerstation_poi", "suhail", "inferred_parcels"],
                    },
                    ensure_ascii=False,
                ),
            },
        )

        persist_existing_branches(db, search_id, existing_branches_payload)
        if brand_profile_payload:
            persist_brand_profile(db, search_id, brand_profile_payload)

        _search_result = run_expansion_search(
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
        # Handle both dict (new format) and list (legacy/test mocks)
        if isinstance(_search_result, dict):
            items = _search_result["items"]
            search_notes = _search_result.get("notes", {})
        else:
            items = _search_result
            search_notes = {}
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

    # Phase 3 chunk 1: pre-warm structured decision memos for the top-N
    # candidates so the first tap in the UI is instant rather than incurring
    # a 3–5s LLM cold-call. Runs AFTER the response is returned to the
    # client (FastAPI BackgroundTasks contract), so response time is
    # unaffected. Disabled when EXPANSION_MEMO_PREWARM_ENABLED=False or
    # EXPANSION_MEMO_PREWARM_TOP_N=0.
    if (
        settings.EXPANSION_MEMO_PREWARM_ENABLED
        and settings.EXPANSION_MEMO_PREWARM_TOP_N > 0
        and items
    ):
        prewarm_specs = _build_prewarm_specs(
            items, settings.EXPANSION_MEMO_PREWARM_TOP_N
        )
        if prewarm_specs:
            background_tasks.add_task(
                _prewarm_decision_memos,
                search_id,
                prewarm_specs,
                brand_profile_payload,
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
        "notes": search_notes,
        "meta": {
            "version": "expansion_advisor_v7",
            "parcel_source": "listings_only",
            "excluded_sources": ["arcgis_parcels", "hungerstation_poi", "suhail", "inferred_parcels"],
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
    return {"items": get_candidates(db, search_id), "meta": {"version": "expansion_advisor_v7"}}


@router.get("/searches/{search_id}/report", response_model=RecommendationReportResponse)
def get_expansion_search_report(search_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    import time as _time
    t0 = _time.monotonic()

    try:
        report = get_recommendation_report(db, search_id)
    except (ValueError, KeyError, TypeError, AttributeError, ProgrammingError) as exc:
        # Recoverable data-shape / query errors: return sparse report with degraded flag.
        logger.warning(
            "Report generation failed (recoverable %s): search_id=%s elapsed=%.2fs detail=%s",
            type(exc).__name__, search_id, _time.monotonic() - t0, str(exc)[:200],
            exc_info=True,
        )
        report = {
            "search_id": search_id,
            "brand_profile": {},
            "meta": {"version": "expansion_advisor_v7", "degraded": True, "error_class": type(exc).__name__},
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
            "assumptions": {"parcel_source": "listings_only", "city": "riyadh"},
        }
    except Exception:
        # Non-recoverable errors (DB connection, infrastructure): let them surface as 500.
        logger.exception(
            "Report generation failed (non-recoverable): search_id=%s elapsed=%.2fs",
            search_id, _time.monotonic() - t0,
        )
        raise

    if not report:
        # get_recommendation_report returns None when search not found or no candidates
        logger.info("Report: not found for search_id=%s", search_id)
        raise HTTPException(status_code=404, detail="Expansion report not found")

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


# ── LLM Decision Memo ───────────────────────────────────────────────


class DecisionMemoRequest(BaseModel):
    candidate: dict[str, Any]
    brief: dict[str, Any]
    lang: str = "en"
    # Additive (Phase 1) — frontend will be updated in Phase 3 to pass these
    # so the endpoint can cache memos per-candidate in ``expansion_candidate``.
    # When either is missing, the endpoint behaves as today (generate fresh,
    # no cache, no persistence).
    parcel_id: str | None = None
    search_id: str | None = None


def _structured_to_legacy_shape(memo_json: dict[str, Any]) -> dict[str, Any]:
    """Project a structured memo onto the legacy response-dict shape so
    ``response["memo"]`` is always safe for un-updated frontends.

    Canonical (new) fields are ``response["memo_json"]`` / ``response["memo_text"]``.
    ``response["memo"]`` is a backward-compat shim only.

    Evidence is routed by its ``polarity`` field: positive/neutral implications
    populate ``top_reasons_to_pursue`` (positives first), and negative
    implications are merged into ``top_risks`` after the explicit risks list.
    Missing or malformed polarity is treated as ``"neutral"`` so cached memos
    generated before polarity was required still render correctly.
    """
    if not isinstance(memo_json, dict):
        memo_json = {}
    evidence = memo_json.get("key_evidence") or []
    risks = memo_json.get("risks") or []
    if not isinstance(evidence, list):
        evidence = []
    if not isinstance(risks, list):
        risks = []

    positive_reasons: list[str] = []
    negative_implications: list[str] = []
    neutral_context: list[str] = []
    for item in evidence:
        if not isinstance(item, dict):
            continue
        impl = item.get("implication")
        if not impl:
            continue
        impl_str = str(impl)
        polarity_raw = item.get("polarity")
        polarity = str(polarity_raw).strip().lower() if polarity_raw else ""
        if polarity == "positive":
            positive_reasons.append(impl_str)
        elif polarity == "negative":
            negative_implications.append(impl_str)
        else:
            neutral_context.append(impl_str)

    explicit_risks: list[str] = []
    for item in risks:
        if not isinstance(item, dict):
            continue
        r = item.get("risk")
        if not r:
            continue
        explicit_risks.append(str(r))

    top_reasons_to_pursue = positive_reasons + neutral_context

    top_risks: list[str] = []
    seen: set[str] = set()
    for s in explicit_risks + negative_implications:
        if s in seen:
            continue
        seen.add(s)
        top_risks.append(s)

    if not top_reasons_to_pursue:
        ranking_explanation = str(memo_json.get("ranking_explanation") or "").strip()
        if ranking_explanation:
            top_reasons_to_pursue = [ranking_explanation[:200]]

    return {
        "headline": str(memo_json.get("headline_recommendation") or "—"),
        "fit_summary": str(memo_json.get("ranking_explanation") or "—"),
        "top_reasons_to_pursue": top_reasons_to_pursue,
        "top_risks": top_risks,
        "recommended_next_action": str(memo_json.get("bottom_line") or "—"),
        # Structured prompt does not produce a rent_context field; keep the
        # legacy key populated with a placeholder so callers that expect it
        # never KeyError.
        "rent_context": "—",
    }


def _text_only_to_legacy_shape(cached_text: str) -> dict[str, Any]:
    """Project a text-only cache hit (legacy-fallback memo persisted earlier)
    onto the legacy response-dict shape."""
    text_str = str(cached_text or "").strip()
    headline = text_str[:120] if text_str else "—"
    return {
        "headline": headline or "—",
        "fit_summary": text_str or "—",
        "top_reasons_to_pursue": [],
        "top_risks": [],
        "recommended_next_action": "—",
        "rent_context": "—",
    }


def _legacy_memo_to_text(memo: dict[str, Any], lang: str) -> str:
    """Render the legacy decision-memo dict to a plain text block so we
    can still populate ``expansion_candidate.decision_memo`` on the
    legacy-fallback path.
    """
    parts: list[str] = []
    headline = memo.get("headline")
    fit = memo.get("fit_summary")
    reasons = memo.get("top_reasons_to_pursue") or []
    risks = memo.get("top_risks") or []
    action = memo.get("recommended_next_action")
    rent = memo.get("rent_context")

    if headline:
        parts.append(str(headline))
    if fit:
        parts.append(str(fit))
    if reasons:
        label = "الأسباب" if lang == "ar" else "Reasons to pursue"
        parts.append(label + ":\n" + "\n".join(f"- {r}" for r in reasons))
    if risks:
        label = "المخاطر" if lang == "ar" else "Risks"
        parts.append(label + ":\n" + "\n".join(f"- {r}" for r in risks))
    if action:
        label = "الخطوة التالية" if lang == "ar" else "Next action"
        parts.append(f"{label}: {action}")
    if rent:
        parts.append(str(rent))
    return "\n\n".join(parts).strip() + "\n"


def _decision_memo_cache_lookup(
    db: Session,
    search_id: str,
    parcel_id: str,
) -> tuple[str | None, dict[str, Any] | None] | None:
    """Return (cached_text, cached_json) if a row exists with a memo
    persisted under the CURRENT ``MEMO_PROMPT_VERSION``, else None.

    A row whose ``decision_memo_prompt_version`` does not match the
    current version (including NULL — pre-versioning rows) is treated as
    a cache miss so the caller regenerates against the new prompt. Any
    DB error is swallowed and treated as a cache miss.
    """
    try:
        row = db.execute(
            text(
                "SELECT decision_memo, decision_memo_json, "
                "       decision_memo_prompt_version "
                "FROM expansion_candidate "
                "WHERE search_id = :sid AND parcel_id = :pid "
                "LIMIT 1"
            ),
            {"sid": search_id, "pid": parcel_id},
        ).fetchone()
    except Exception as exc:
        logger.warning(
            "Decision memo cache lookup failed for (%s,%s): %s",
            search_id, parcel_id, exc,
        )
        return None
    if row is None:
        return None
    cached_text = row[0]
    cached_json = row[1]
    cached_version = row[2]
    if cached_text is None and cached_json is None:
        return None
    if cached_version != MEMO_PROMPT_VERSION:
        # Stale (different version) or pre-versioning (NULL) — regenerate.
        return None
    return cached_text, cached_json


def _decision_memo_cache_write(
    db: Session,
    search_id: str,
    parcel_id: str,
    memo_text: str | None,
    memo_json: dict[str, Any] | None,
) -> None:
    """Persist the memo to expansion_candidate. Swallows errors — a persist
    failure must not break the endpoint response.

    Phase 3 chunk 1 verification: this UPDATE is bound to the request session
    via :func:`get_db` and committed inline. The 0-memos-in-prod observation
    in production today is therefore explained by no caller having hit POST
    /decision-memo against those 3,898 candidates yet — not by a missing
    write. The pre-warm background task on POST /searches will populate
    memos automatically going forward.
    """
    try:
        db.execute(
            text(
                "UPDATE expansion_candidate "
                "SET decision_memo = :txt, "
                "    decision_memo_json = CAST(:j AS JSONB), "
                "    decision_memo_prompt_version = :ver "
                "WHERE search_id = :sid AND parcel_id = :pid"
            ),
            {
                "sid": search_id,
                "pid": parcel_id,
                "txt": memo_text,
                "j": json.dumps(memo_json, ensure_ascii=False) if memo_json is not None else None,
                "ver": MEMO_PROMPT_VERSION,
            },
        )
        db.commit()
    except Exception as exc:
        logger.warning(
            "Decision memo persist failed for (%s,%s): %s",
            search_id, parcel_id, exc,
        )
        try:
            db.rollback()
        except Exception:
            pass


@router.post("/decision-memo")
def post_decision_memo(
    req: DecisionMemoRequest,
    db: Session = Depends(get_db),
):
    """Generate (or fetch cached) LLM decision memo for a candidate site.

    Flow:
      1. If both ``search_id`` and ``parcel_id`` are supplied, check the
         cache in ``expansion_candidate``. If either column has a value,
         serve it — never regenerate. (Phase 1: memos never expire;
         reruning the search produces a new search_id and a fresh memo.)
      2. Otherwise, build a MemoContext and call ``generate_structured_memo``.
         On success, render text, optionally persist both columns, and
         return the structured output.
      3. On any structured-path failure (flag off, LLM error, malformed
         JSON, renderer blow-up), fall back to the legacy generic memo
         path. Persist only the text column; leave ``decision_memo_json``
         NULL. Still return a valid response to the caller.
      4. Ceiling-breach raises ``RuntimeError`` from the legacy path and
         surfaces as HTTP 503 — same as today.

    Response shape (backward-compat contract):
      ``response["memo"]`` is ALWAYS a legacy-shape dict (keys: headline,
      fit_summary, top_reasons_to_pursue, top_risks, recommended_next_action,
      rent_context). On the structured path it is synthesized from the
      structured JSON; on legacy fallback it is the dict as-returned by
      ``generate_decision_memo``; on a text-only cache hit it is derived
      from the cached text. Un-updated frontends that read
      ``response.memo.headline`` (etc.) never crash.

      New Phase-3 frontend code should prefer ``response["memo_text"]`` and
      ``response["memo_json"]`` as the canonical fields. ``response["memo"]``
      is a backward-compat shim only.
    """
    lang = req.lang if req.lang in ("en", "ar") else "en"

    # Allow parcel_id to be inferred from the candidate dict if the frontend
    # hasn't been updated yet to pass it at the top level.
    parcel_id = req.parcel_id or (
        req.candidate.get("parcel_id") if isinstance(req.candidate, dict) else None
    )
    search_id = req.search_id
    cache_keyed = bool(search_id and parcel_id)

    # 1) Cache lookup
    if cache_keyed:
        cached = _decision_memo_cache_lookup(db, search_id, parcel_id)
        if cached is not None:
            cached_text, cached_json = cached
            if cached_json is not None:
                return {
                    "memo": _structured_to_legacy_shape(cached_json),
                    "memo_text": cached_text,
                    "memo_json": cached_json,
                    "cached": True,
                }
            if cached_text is not None:
                # Legacy-fallback memo was persisted earlier — serve as-is,
                # do not regenerate.
                return {
                    "memo": _text_only_to_legacy_shape(cached_text),
                    "memo_text": cached_text,
                    "memo_json": None,
                    "cached": True,
                }

    # 2) Cache miss / unkeyed — try structured path first
    memo_json: dict[str, Any] | None = None
    memo_text: str | None = None
    ctx = None
    try:
        ctx = build_memo_context(
            candidate=req.candidate,
            brief=req.brief,
            lang=lang,
        )
    except Exception as exc:  # pragma: no cover — build is designed not to raise
        logger.warning("build_memo_context failed: %s", exc)

    if ctx is not None:
        memo_json = generate_structured_memo(ctx)

    if memo_json is not None:
        # Renderer is wrapped defensively — a malformed-but-validation-passing
        # JSON must not 500 the endpoint. On renderer failure, drop to legacy
        # and log the offending JSON so we can tighten validation later.
        try:
            memo_text = render_structured_memo_as_text(memo_json, lang)
        except Exception as exc:
            logger.warning(
                "render_structured_memo_as_text failed for %s: %s | json=%s",
                parcel_id, exc, json.dumps(memo_json, ensure_ascii=False)[:500],
            )
            memo_json = None
            memo_text = None

    if memo_json is not None:
        response_payload = {
            "memo": _structured_to_legacy_shape(memo_json),
            "memo_text": memo_text,
            "memo_json": memo_json,
            "cached": False,
        }
    else:
        # 3) Legacy fallback (also the flag-off path — byte-for-byte legacy
        # behavior when EXPANSION_MEMO_STRUCTURED_ENABLED=False).
        try:
            legacy = generate_decision_memo(
                candidate=req.candidate,
                brief=req.brief,
                lang=lang,
            )
        except RuntimeError as exc:
            logger.warning("Decision memo generation failed: %s", exc)
            raise HTTPException(status_code=503, detail=str(exc))
        memo_text = _legacy_memo_to_text(legacy, lang)
        response_payload = {
            "memo": legacy,
            "memo_text": memo_text,
            "memo_json": None,
            "cached": False,
        }

    # 4) Persist when keyed
    if cache_keyed:
        _decision_memo_cache_write(
            db, search_id, parcel_id, memo_text, response_payload["memo_json"]
        )

    return response_payload
