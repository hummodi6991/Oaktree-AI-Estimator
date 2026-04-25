import { buildApiUrl, fetchWithAuth } from "../../api";
import { CANDIDATE_NUMERIC_FIELDS, coerceCandidateNumerics } from "./coerceNumeric";

export type ExpansionAdvisorMeta = {
  version?: string;
  generated_at?: string;
  [key: string]: unknown;
};

export type ExpansionBrandProfile = {
  price_tier?: "value" | "mid" | "premium" | null;
  average_check_sar?: number | null;
  primary_channel?: "dine_in" | "delivery" | "balanced" | null;
  parking_sensitivity?: "low" | "medium" | "high" | null;
  frontage_sensitivity?: "low" | "medium" | "high" | null;
  visibility_sensitivity?: "low" | "medium" | "high" | null;
  expansion_goal?: "flagship" | "neighborhood" | "delivery_led" | "balanced" | null;
  cannibalization_tolerance_m?: number | null;
  preferred_districts?: string[] | null;
  excluded_districts?: string[] | null;
};

export type ExpansionBrief = {
  brand_name: string;
  category: string;
  service_model: "qsr" | "dine_in" | "delivery_first" | "cafe";
  min_area_m2: number;
  max_area_m2: number;
  target_area_m2?: number | null;
  target_districts: string[];
  existing_branches: Array<{ name?: string; lat: number; lon: number; district?: string }>;
  limit: number;
  brand_profile?: ExpansionBrandProfile | null;
};

export type ExpansionAdvisorBrandProfileResponse = {
  price_tier?: string;
  average_check_sar?: number;
  primary_channel?: string;
  expansion_goal?: string;
  preferred_districts?: string[];
  excluded_districts?: string[];
  [key: string]: unknown;
};

export type CandidateGateReasons = {
  passed: string[];
  failed: string[];
  unknown: string[];
  thresholds: Record<string, unknown>;
  explanations: Record<string, unknown>;
};

// Per-gate verdicts are tri-state at runtime: true (pass), false (fail),
// or null (unknown — the gate couldn't be evaluated, e.g. parking_pass
// for Aqar listings with no parking ground truth). The backend's
// `_candidate_gate_status` emits all three values, so each field is
// `boolean | null` rather than the narrower `true | null`.
export type CandidateGateStatusJson = {
  overall_pass?: boolean | null;
  zoning_fit_pass?: boolean | null;
  area_fit_pass?: boolean | null;
  frontage_access_pass?: boolean | null;
  parking_pass?: boolean | null;
  district_pass?: boolean | null;
  cannibalization_pass?: boolean | null;
  delivery_market_pass?: boolean | null;
  economics_pass?: boolean | null;
  [key: string]: boolean | null | undefined;
};

export type CandidateFeatureSnapshot = {
  context_sources: Record<string, unknown>;
  missing_context: string[];
  data_completeness_score: number;
  [key: string]: unknown;
};

export type CandidateScoreBreakdown = {
  weights: Record<string, unknown>;
  inputs: Record<string, unknown>;
  weighted_components: Record<string, unknown>;
  final_score: number;
  display_score?: number;
};

export type ComparableCompetitor = {
  id?: string;
  name?: string;
  score?: number;
  category?: string;
  district?: string;
  rating?: number;
  review_count?: number;
  distance_m?: number;
  source?: string;
};

export type SiteFitContext = {
  road_context_available: boolean;
  parking_context_available: boolean;
  frontage_score_mode: "observed" | "estimated";
  access_score_mode: "observed" | "estimated";
  parking_score_mode: "observed" | "estimated";
};

export interface StructuredMemoEvidence {
  signal: string;
  value: string | number;
  implication: string;
  polarity?: "positive" | "negative" | "neutral";
}

export interface StructuredMemoRisk {
  risk: string;
  mitigation?: string | null;
}

export interface StructuredMemo {
  headline_recommendation: string;
  ranking_explanation: string;
  key_evidence: StructuredMemoEvidence[];
  risks: StructuredMemoRisk[];
  comparison: string;
  bottom_line: string;
}

export type RerankStatus =
  | "flag_off"
  | "shortlist_below_minimum"
  | "llm_failed"
  | "outside_rerank_cap"
  | "unchanged"
  | "applied";

export interface RerankReason {
  summary: string;
  positives_cited: string[];
  negatives_cited: string[];
  comparison_to_displaced_candidate: string;
}

export type ExpansionCandidate = {
  id: string;
  candidate_id?: string;
  search_id: string;
  parcel_id: string;
  district?: string;
  district_key?: string | null;
  district_name_ar?: string | null;
  district_name_en?: string | null;
  district_display?: string | null;
  area_m2?: number;
  lat: number;
  lon: number;
  rank_position?: number;
  final_score?: number;
  economics_score?: number;
  brand_fit_score?: number;
  provider_density_score?: number;
  provider_whitespace_score?: number;
  multi_platform_presence_score?: number;
  delivery_competition_score?: number;
  zoning_fit_score?: number;
  frontage_score?: number;
  access_score?: number;
  parking_score?: number;
  access_visibility_score?: number;
  confidence_grade?: string;
  gate_verdict?: string;
  cannibalization_score?: number;
  distance_to_nearest_branch_m?: number;
  estimated_rent_sar_m2_year?: number;
  estimated_annual_rent_sar?: number;
  display_annual_rent_sar?: number;
  estimated_fitout_cost_sar?: number;
  estimated_revenue_index?: number;
  decision_summary?: string;
  gate_status_json?: CandidateGateStatusJson;
  gate_reasons_json?: CandidateGateReasons;
  feature_snapshot_json?: CandidateFeatureSnapshot;
  score_breakdown_json?: CandidateScoreBreakdown;
  top_positives_json?: string[];
  top_risks_json?: string[];
  demand_thesis?: string;
  cost_thesis?: string;
  comparable_competitors_json?: ComparableCompetitor[];
  compare_rank?: number;
  site_fit_context?: SiteFitContext;
  // Commercial unit / tier fields
  source_type?: "parcel" | "commercial_unit" | "aqar" | "delivery_poi";
  source_tier?: number | null;
  is_vacant?: boolean | null;
  current_tenant?: string | null;
  current_category?: string | null;
  rent_confidence?: string | null;
  commercial_unit_id?: string;
  listing_url?: string;
  image_url?: string;
  unit_price_sar_annual?: number;
  unit_area_sqm?: number;
  unit_street_width_m?: number;
  unit_neighborhood?: string;
  unit_listing_type?: string;
  // Rerank + decision-memo presence metadata (Phase 3 chunk 1). The list
  // endpoint intentionally excludes `decision_memo` and `decision_memo_json`
  // for payload size — those live on the memo endpoint only.
  deterministic_rank?: number | null;
  final_rank?: number | null;
  rerank_applied?: boolean;
  rerank_reason?: RerankReason | null;
  rerank_delta?: number;
  rerank_status?: RerankStatus | null;
  decision_memo_present?: boolean;
};

export type ExpansionSearchResponse = {
  search_id: string;
  brand_profile: ExpansionAdvisorBrandProfileResponse;
  items: ExpansionCandidate[];
  notes?: Record<string, unknown>;
  meta: ExpansionAdvisorMeta;
};

export type ExpansionSearchDetailResponse = {
  id: string;
  created_at?: string;
  brand_name?: string;
  category?: string;
  service_model?: string;
  target_districts: string[];
  min_area_m2?: number;
  max_area_m2?: number;
  target_area_m2?: number;
  bbox?: Record<string, unknown> | null;
  request_json: Record<string, unknown>;
  notes: Record<string, unknown>;
  existing_branches: Array<Record<string, unknown>>;
  brand_profile?: Record<string, unknown> | null;
  meta: ExpansionAdvisorMeta;
};

export type ExpansionCandidatesListResponse = {
  items: ExpansionCandidate[];
  meta: ExpansionAdvisorMeta;
};

export type CompareCandidateItem = {
  candidate_id: string;
  id?: string;
  search_id?: string;
  district?: string;
  district_key?: string | null;
  district_name_ar?: string | null;
  district_name_en?: string | null;
  district_display?: string | null;
  parcel_id?: string;
  area_m2?: number;
  lat?: number;
  lon?: number;
  rank_position?: number;
  compare_rank?: number;
  final_score?: number;
  confidence_grade?: string;
  gate_verdict?: string;
  gate_status_json?: CandidateGateStatusJson;
  gate_reasons_json?: CandidateGateReasons;
  feature_snapshot_json?: CandidateFeatureSnapshot;
  score_breakdown_json?: CandidateScoreBreakdown;
  top_positives_json?: string[];
  top_risks_json?: string[];
  comparable_competitors_json?: ComparableCompetitor[];
  decision_summary?: string;
  demand_thesis?: string;
  cost_thesis?: string;
  zoning_fit_score?: number;
  frontage_score?: number;
  access_score?: number;
  parking_score?: number;
  access_visibility_score?: number;
  economics_score?: number;
  brand_fit_score?: number;
  provider_density_score?: number;
  provider_whitespace_score?: number;
  delivery_competition_score?: number;
  multi_platform_presence_score?: number;
  cannibalization_score?: number;
  estimated_rent_sar_m2_year?: number;
  estimated_annual_rent_sar?: number;
  estimated_fitout_cost_sar?: number;
  estimated_revenue_index?: number;
  distance_to_nearest_branch_m?: number;
  demand_score?: number;
  fit_score?: number;
};

export type CompareCandidatesResponse = {
  items: CompareCandidateItem[];
  summary: Record<string, string | null>;
};

export type CandidateMemoResponse = {
  candidate_id?: string;
  search_id?: string;
  brand_profile: Record<string, unknown>;
  recommendation: {
    headline?: string;
    verdict?: string;
    best_use_case?: string;
    main_watchout?: string;
    gate_verdict?: string;
  };
  candidate: {
    final_score?: number;
    rank_position?: number;
    confidence_grade?: string;
    economics_score?: number;
    brand_fit_score?: number;
    score_breakdown_json?: CandidateScoreBreakdown;
    top_positives_json?: string[];
    top_risks_json?: string[];
    gate_status?: Record<string, boolean>;
    gate_reasons?: CandidateGateReasons;
    feature_snapshot?: CandidateFeatureSnapshot;
    comparable_competitors?: ComparableCompetitor[];
    demand_thesis?: string;
    cost_thesis?: string;
    site_fit_context?: SiteFitContext;
    // Rerank + structured-memo fields (Phase 3 chunk 1 persistence).
    deterministic_rank?: number | null;
    final_rank?: number | null;
    rerank_applied?: boolean;
    rerank_reason?: RerankReason | null;
    rerank_delta?: number;
    rerank_status?: RerankStatus | null;
    decision_memo?: string | null;
    decision_memo_json?: StructuredMemo | null;
    [key: string]: unknown;
  };
  market_research: {
    delivery_market_summary?: string;
    competitive_context?: string;
    district_fit_summary?: string;
  };
};

export type RecommendationTopCandidate = {
  id?: string;
  final_score?: number;
  rank_position?: number;
  confidence_grade?: string;
  gate_verdict?: string;
  district?: string;
  district_key?: string | null;
  district_name_ar?: string | null;
  district_name_en?: string | null;
  district_display?: string | null;
  top_positives_json?: string[];
  top_risks_json?: string[];
  feature_snapshot_json?: Record<string, unknown>;
  score_breakdown_json?: CandidateScoreBreakdown;
};

export type RecommendationReportResponse = {
  search_id?: string;
  brand_profile: Record<string, unknown>;
  meta: ExpansionAdvisorMeta;
  recommendation: {
    best_candidate_id?: string;
    runner_up_candidate_id?: string;
    best_pass_candidate_id?: string;
    best_confidence_candidate_id?: string;
    best_economics_candidate_id?: string;
    best_brand_fit_candidate_id?: string;
    highest_demand_candidate_id?: string;
    strongest_whitespace_candidate_id?: string;
    lowest_cannibalization_candidate_id?: string;
    most_confident_candidate_id?: string;
    pass_count?: number;
    validation_clear_count?: number;
    why_best?: string;
    main_risk?: string;
    best_format?: string;
    summary?: string;
    report_summary?: string;
  };
  top_candidates: RecommendationTopCandidate[];
  assumptions: Record<string, unknown>;
};

export type SavedExpansionSearch = {
  id: string;
  search_id: string;
  title: string;
  description?: string | null;
  status: "draft" | "final";
  selected_candidate_ids?: string[] | null;
  filters_json?: Record<string, unknown> | null;
  ui_state_json?: Record<string, unknown> | null;
  created_at?: string;
  updated_at?: string;
  search?: ExpansionSearchDetailResponse | null;
  candidates?: ExpansionCandidate[];
};

export type SavedExpansionSearchListResponse = {
  items: SavedExpansionSearch[];
};

export type DistrictOption = {
  value: string;
  label: string;
  label_ar: string;
  label_en?: string | null;
  aliases: string[];
};

export type DistrictOptionsResponse = {
  items: DistrictOption[];
};

export type BranchSuggestion = {
  id: string;
  name: string;
  district: string;
  lat: number;
  lon: number;
  source: string;
};

export type BranchSuggestionsResponse = {
  items: BranchSuggestion[];
};

const DEFAULT_GATE_REASONS: CandidateGateReasons = { passed: [], failed: [], unknown: [], thresholds: {}, explanations: {} };
const DEFAULT_FEATURE_SNAPSHOT: CandidateFeatureSnapshot = { context_sources: {}, missing_context: [], data_completeness_score: 0 };
const DEFAULT_SCORE_BREAKDOWN: CandidateScoreBreakdown = { weights: {}, inputs: {}, weighted_components: {}, final_score: 0 };

export function normalizeCandidate(candidate: ExpansionCandidate): ExpansionCandidate {
  // Backend serializes Numeric columns as strings (e.g. "45.00") for
  // precision; coerce them to numbers at the boundary so downstream
  // formatters and arithmetic don't have to defend against mixed types.
  const coerced = coerceCandidateNumerics(
    candidate as unknown as Record<string, unknown>,
    CANDIDATE_NUMERIC_FIELDS,
  ) as ExpansionCandidate;
  return {
    ...coerced,
    rank_position: coerced.rank_position ?? undefined,
    confidence_grade: candidate.confidence_grade || "D",
    compare_rank: candidate.compare_rank ?? undefined,
    decision_summary: candidate.decision_summary || "",
    demand_thesis: candidate.demand_thesis || "",
    cost_thesis: candidate.cost_thesis || "",
    gate_status_json: (typeof candidate.gate_status_json === "object" && candidate.gate_status_json !== null) ? candidate.gate_status_json : {},
    gate_reasons_json: candidate.gate_reasons_json
      ? { ...DEFAULT_GATE_REASONS, ...candidate.gate_reasons_json, passed: candidate.gate_reasons_json.passed || [], failed: candidate.gate_reasons_json.failed || [], unknown: candidate.gate_reasons_json.unknown || [] }
      : DEFAULT_GATE_REASONS,
    feature_snapshot_json: candidate.feature_snapshot_json
      ? { ...DEFAULT_FEATURE_SNAPSHOT, ...candidate.feature_snapshot_json, context_sources: candidate.feature_snapshot_json.context_sources || {}, missing_context: candidate.feature_snapshot_json.missing_context || [] }
      : DEFAULT_FEATURE_SNAPSHOT,
    score_breakdown_json: candidate.score_breakdown_json
      ? { ...DEFAULT_SCORE_BREAKDOWN, ...candidate.score_breakdown_json }
      : DEFAULT_SCORE_BREAKDOWN,
    top_positives_json: Array.isArray(candidate.top_positives_json) ? candidate.top_positives_json : [],
    top_risks_json: Array.isArray(candidate.top_risks_json) ? candidate.top_risks_json : [],
    comparable_competitors_json: Array.isArray(candidate.comparable_competitors_json) ? candidate.comparable_competitors_json : [],
    site_fit_context: candidate.site_fit_context || undefined,
    deterministic_rank: candidate.deterministic_rank ?? null,
    final_rank: candidate.final_rank ?? null,
    rerank_applied: candidate.rerank_applied ?? false,
    rerank_reason: candidate.rerank_reason ?? null,
    rerank_delta: typeof candidate.rerank_delta === "number" ? candidate.rerank_delta : 0,
    rerank_status: candidate.rerank_status ?? null,
    decision_memo_present: candidate.decision_memo_present ?? false,
  };
}

export function normalizeCandidates(candidates: ExpansionCandidate[] = []): ExpansionCandidate[] {
  return candidates.map(normalizeCandidate);
}

export function normalizeSavedSearch(saved: SavedExpansionSearch): SavedExpansionSearch {
  return {
    ...saved,
    title: saved.title || "",
    status: saved.status || "draft",
    selected_candidate_ids: saved.selected_candidate_ids || [],
    filters_json: saved.filters_json || {},
    ui_state_json: saved.ui_state_json || {},
    candidates: normalizeCandidates(saved.candidates || []),
    search: saved.search
      ? {
          ...saved.search,
          target_districts: saved.search.target_districts || [],
          request_json: saved.search.request_json || {},
          notes: saved.search.notes || {},
          existing_branches: saved.search.existing_branches || [],
          brand_profile: saved.search.brand_profile || null,
          meta: saved.search.meta || {},
        }
      : saved.search,
  };
}

export function normalizeReportResponse(data: RecommendationReportResponse): RecommendationReportResponse {
  const rec = data.recommendation || {};
  return {
    ...data,
    top_candidates: (data.top_candidates || []).map((rawTc) => {
      // Coerce stringified-Decimal numeric fields (see normalizeCandidate).
      const tc = coerceCandidateNumerics(
        rawTc as unknown as Record<string, unknown>,
        CANDIDATE_NUMERIC_FIELDS,
      ) as RecommendationTopCandidate;
      return {
        ...tc,
        top_positives_json: tc.top_positives_json || [],
        top_risks_json: tc.top_risks_json || [],
        score_breakdown_json: tc.score_breakdown_json || DEFAULT_SCORE_BREAKDOWN,
        feature_snapshot_json: tc.feature_snapshot_json || {},
      };
    }),
    assumptions: data.assumptions ?? {},
    recommendation: {
      ...rec,
      best_candidate_id: rec.best_candidate_id ?? undefined,
      runner_up_candidate_id: rec.runner_up_candidate_id ?? undefined,
      best_pass_candidate_id: rec.best_pass_candidate_id ?? undefined,
      best_confidence_candidate_id: rec.best_confidence_candidate_id ?? undefined,
      pass_count: rec.pass_count ?? 0,
      validation_clear_count: rec.validation_clear_count ?? 0,
      why_best: rec.why_best ?? "",
      main_risk: rec.main_risk ?? "",
      best_format: rec.best_format ?? "",
      summary: rec.summary ?? "",
      report_summary: rec.report_summary ?? "",
    },
    brand_profile: data.brand_profile || {},
    meta: data.meta || {},
  };
}

export function normalizeMemoResponse(data: CandidateMemoResponse): CandidateMemoResponse {
  const rawCand = data.candidate || ({} as CandidateMemoResponse["candidate"]);
  // Coerce stringified-Decimal numeric fields (see normalizeCandidate).
  const cand = coerceCandidateNumerics(
    rawCand as unknown as Record<string, unknown>,
    CANDIDATE_NUMERIC_FIELDS,
  ) as CandidateMemoResponse["candidate"];
  return {
    ...data,
    brand_profile: data.brand_profile || {},
    recommendation: data.recommendation || {},
    candidate: {
      ...cand,
      top_positives_json: (cand.top_positives_json as string[] | undefined) || [],
      top_risks_json: (cand.top_risks_json as string[] | undefined) || [],
      comparable_competitors: (cand.comparable_competitors as ComparableCompetitor[] | undefined) || [],
      score_breakdown_json: (cand.score_breakdown_json as CandidateScoreBreakdown | undefined) || DEFAULT_SCORE_BREAKDOWN,
      gate_status: (cand.gate_status as Record<string, boolean> | undefined) || {},
      gate_reasons: (cand.gate_reasons as CandidateGateReasons | undefined) || DEFAULT_GATE_REASONS,
      feature_snapshot: (cand.feature_snapshot as CandidateFeatureSnapshot | undefined) || DEFAULT_FEATURE_SNAPSHOT,
      deterministic_rank: cand.deterministic_rank ?? null,
      final_rank: cand.final_rank ?? null,
      rerank_applied: cand.rerank_applied ?? false,
      rerank_reason: cand.rerank_reason ?? null,
      rerank_delta: typeof cand.rerank_delta === "number" ? cand.rerank_delta : 0,
      rerank_status: cand.rerank_status ?? null,
      decision_memo: cand.decision_memo ?? null,
      decision_memo_json: cand.decision_memo_json ?? null,
    },
    market_research: data.market_research || {},
  };
}

export function normalizeCompareResponse(data: CompareCandidatesResponse): CompareCandidatesResponse {
  return {
    ...data,
    items: (data.items || []).map((rawItem) => {
      // Coerce stringified-Decimal numeric fields (see normalizeCandidate).
      const item = coerceCandidateNumerics(
        rawItem as unknown as Record<string, unknown>,
        CANDIDATE_NUMERIC_FIELDS,
      ) as CompareCandidateItem;
      return {
      ...item,
      confidence_grade: item.confidence_grade || "D",
      gate_status_json: (typeof item.gate_status_json === "object" && item.gate_status_json !== null) ? item.gate_status_json : {},
      gate_reasons_json: item.gate_reasons_json || DEFAULT_GATE_REASONS,
      feature_snapshot_json: item.feature_snapshot_json || DEFAULT_FEATURE_SNAPSHOT,
      score_breakdown_json: item.score_breakdown_json || DEFAULT_SCORE_BREAKDOWN,
      top_positives_json: Array.isArray(item.top_positives_json) ? item.top_positives_json : [],
      top_risks_json: Array.isArray(item.top_risks_json) ? item.top_risks_json : [],
      comparable_competitors_json: Array.isArray(item.comparable_competitors_json) ? item.comparable_competitors_json : [],
      decision_summary: item.decision_summary || "",
      demand_thesis: item.demand_thesis || "",
      cost_thesis: item.cost_thesis || "",
      };
    }),
    summary: data.summary || {},
  };
}

async function readJson<T>(res: Response): Promise<T> {
  const text = await res.text();
  return (text ? JSON.parse(text) : {}) as T;
}

export async function createExpansionSearch(payload: ExpansionBrief): Promise<ExpansionSearchResponse> {
  const res = await fetchWithAuth(buildApiUrl("/v1/expansion-advisor/searches"), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
  const data = await readJson<ExpansionSearchResponse>(res);
  return { ...data, items: normalizeCandidates(data.items || []), notes: data.notes || {} };
}
export async function getExpansionSearch(searchId: string): Promise<ExpansionSearchDetailResponse> {
  const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/searches/${searchId}`));
  const data = await readJson<ExpansionSearchDetailResponse>(res);
  return {
    ...data,
    target_districts: data.target_districts || [],
    request_json: data.request_json || {},
    notes: data.notes || {},
    existing_branches: data.existing_branches || [],
    meta: data.meta || {},
  };
}
export async function getExpansionCandidates(searchId: string): Promise<ExpansionCandidatesListResponse> {
  const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/searches/${searchId}/candidates`));
  const data = await readJson<ExpansionCandidatesListResponse>(res);
  return { ...data, items: normalizeCandidates(data.items || []), meta: data.meta || {} };
}
export async function compareExpansionCandidates(searchId: string, candidateIds: string[]): Promise<CompareCandidatesResponse> { const res = await fetchWithAuth(buildApiUrl("/v1/expansion-advisor/candidates/compare"), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ search_id: searchId, candidate_ids: candidateIds }) }); const data = await readJson<CompareCandidatesResponse>(res); return normalizeCompareResponse(data); }
export async function getExpansionCandidateMemo(candidateId: string): Promise<CandidateMemoResponse> {
  const url = buildApiUrl(`/v1/expansion-advisor/candidates/${candidateId}/memo`);
  console.info("[expansion-memo] fetching memo", { url, candidateId });
  const res = await fetchWithAuth(url);
  const data = await readJson<CandidateMemoResponse>(res);
  return normalizeMemoResponse(data);
}
export async function getExpansionRecommendationReport(searchId: string): Promise<RecommendationReportResponse> {
  const url = buildApiUrl(`/v1/expansion-advisor/searches/${searchId}/report`);
  console.info("[expansion-report] fetching report", { url, searchId });
  const res = await fetchWithAuth(url);
  const data = await readJson<RecommendationReportResponse>(res);
  const normalized = normalizeReportResponse(data);
  console.info("[expansion-report] report loaded", {
    searchId,
    status: res.status,
    passCount: normalized.recommendation?.pass_count,
    topCandidates: normalized.top_candidates?.length ?? 0,
    hasSummary: !!normalized.recommendation?.summary,
  });
  // Log sparse payload normalization for observability
  if (!data.recommendation && normalized.recommendation) {
    console.info("[expansion-report] normalized sparse recommendation payload for search", searchId);
  }
  return normalized;
}
export async function createSavedExpansionSearch(payload: Omit<SavedExpansionSearch, "id">): Promise<SavedExpansionSearch> { const res = await fetchWithAuth(buildApiUrl("/v1/expansion-advisor/saved-searches"), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) }); return normalizeSavedSearch(await readJson<SavedExpansionSearch>(res)); }
export async function listSavedExpansionSearches(status?: "draft" | "final", limit = 20): Promise<SavedExpansionSearchListResponse> {
  const params = new URLSearchParams({ limit: String(limit) });
  if (status) params.set("status", status);
  try {
    const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches?${params.toString()}`));
    const data = await readJson<SavedExpansionSearchListResponse>(res);
    return { items: (data.items || []).map(normalizeSavedSearch) };
  } catch (err) {
    // A 404 means the saved-searches resource/table is not available yet —
    // treat as an empty list rather than a failure so the UI shows a clean
    // empty state instead of a misleading error alert.
    if (err instanceof Error && /^404\b/.test(err.message)) {
      return { items: [] };
    }
    throw err;
  }
}
export async function getSavedExpansionSearch(savedId: string): Promise<SavedExpansionSearch> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches/${savedId}`)); return normalizeSavedSearch(await readJson<SavedExpansionSearch>(res)); }
export async function updateSavedExpansionSearch(savedId: string, payload: Partial<SavedExpansionSearch>): Promise<SavedExpansionSearch> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches/${savedId}`), { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) }); return normalizeSavedSearch(await readJson<SavedExpansionSearch>(res)); }
export async function deleteSavedExpansionSearch(savedId: string): Promise<{ deleted: boolean }> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches/${savedId}`), { method: "DELETE" }); return readJson<{ deleted: boolean }>(res); }

export async function getExpansionDistricts(): Promise<DistrictOption[]> {
  const res = await fetchWithAuth(buildApiUrl("/v1/expansion-advisor/districts"));
  const data = await readJson<DistrictOptionsResponse>(res);
  return data.items || [];
}

export async function searchBranchSuggestions(q: string, limit = 15): Promise<BranchSuggestion[]> {
  if (!q || q.trim().length < 2) return [];
  const params = new URLSearchParams({ q: q.trim(), limit: String(limit) });
  const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/branch-suggestions?${params.toString()}`));
  const data = await readJson<BranchSuggestionsResponse>(res);
  return data.items || [];
}

// ── LLM Decision Memo ──────────────────────────────────────────────

export type LLMDecisionMemo = {
  headline: string;
  fit_summary: string;
  top_reasons_to_pursue: string[];
  top_risks: string[];
  recommended_next_action: string;
  rent_context: string;
};

export interface GeneratedDecisionMemo {
  memo: LLMDecisionMemo;
  memo_text: string | null;
  memo_json: StructuredMemo | null;
}

export async function generateDecisionMemo(
  candidate: Record<string, unknown>,
  brief: Record<string, unknown>,
  lang: string,
): Promise<GeneratedDecisionMemo> {
  const res = await fetchWithAuth(buildApiUrl("/v1/expansion-advisor/decision-memo"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ candidate, brief, lang }),
  });
  const data = await readJson<{
    memo: LLMDecisionMemo;
    memo_text?: string | null;
    memo_json?: StructuredMemo | null;
  }>(res);
  return {
    memo: data.memo,
    memo_text: data.memo_text ?? null,
    memo_json: data.memo_json ?? null,
  };
}
