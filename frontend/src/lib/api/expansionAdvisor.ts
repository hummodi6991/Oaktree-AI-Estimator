import { buildApiUrl, fetchWithAuth } from "../../api";

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
  target_customer?: string | null;
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
  target_customer?: string;
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

export type ExpansionCandidate = {
  id: string;
  candidate_id?: string;
  search_id: string;
  parcel_id: string;
  district?: string;
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
  estimated_fitout_cost_sar?: number;
  estimated_revenue_index?: number;
  estimated_payback_months?: number;
  payback_band?: string;
  decision_summary?: string;
  gate_status_json?: Record<string, boolean | null | undefined>;
  gate_reasons_json?: CandidateGateReasons;
  feature_snapshot_json?: CandidateFeatureSnapshot;
  score_breakdown_json?: CandidateScoreBreakdown;
  top_positives_json?: string[];
  top_risks_json?: string[];
  demand_thesis?: string;
  cost_thesis?: string;
  comparable_competitors_json?: ComparableCompetitor[];
  compare_rank?: number;
};

export type ExpansionSearchResponse = {
  search_id: string;
  brand_profile: ExpansionAdvisorBrandProfileResponse;
  items: ExpansionCandidate[];
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
  district?: string;
  parcel_id?: string;
  rank_position?: number;
  final_score?: number;
  confidence_grade?: string;
  gate_status_json?: Record<string, boolean | null | undefined>;
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
  estimated_payback_months?: number;
  payback_band?: string;
  estimated_rent_sar_m2_year?: number;
  estimated_annual_rent_sar?: number;
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
    payback_band?: string;
    estimated_payback_months?: number;
    score_breakdown_json?: CandidateScoreBreakdown;
    top_positives_json?: string[];
    top_risks_json?: string[];
    gate_status?: Record<string, boolean>;
    gate_reasons?: CandidateGateReasons;
    feature_snapshot?: CandidateFeatureSnapshot;
    comparable_competitors?: ComparableCompetitor[];
    demand_thesis?: string;
    cost_thesis?: string;
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
    fastest_payback_candidate_id?: string;
    lowest_cannibalization_candidate_id?: string;
    most_confident_candidate_id?: string;
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

const DEFAULT_GATE_REASONS: CandidateGateReasons = { passed: [], failed: [], unknown: [], thresholds: {}, explanations: {} };
const DEFAULT_FEATURE_SNAPSHOT: CandidateFeatureSnapshot = { context_sources: {}, missing_context: [], data_completeness_score: 0 };
const DEFAULT_SCORE_BREAKDOWN: CandidateScoreBreakdown = { weights: {}, inputs: {}, weighted_components: {}, final_score: 0 };

export function normalizeCandidate(candidate: ExpansionCandidate): ExpansionCandidate {
  return {
    ...candidate,
    gate_status_json: candidate.gate_status_json || {},
    gate_reasons_json: candidate.gate_reasons_json || DEFAULT_GATE_REASONS,
    feature_snapshot_json: candidate.feature_snapshot_json || DEFAULT_FEATURE_SNAPSHOT,
    score_breakdown_json: candidate.score_breakdown_json || DEFAULT_SCORE_BREAKDOWN,
    top_positives_json: candidate.top_positives_json || [],
    top_risks_json: candidate.top_risks_json || [],
    comparable_competitors_json: candidate.comparable_competitors_json || [],
  };
}

export function normalizeCandidates(candidates: ExpansionCandidate[] = []): ExpansionCandidate[] {
  return candidates.map(normalizeCandidate);
}

export function normalizeSavedSearch(saved: SavedExpansionSearch): SavedExpansionSearch {
  return {
    ...saved,
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
          meta: saved.search.meta || {},
        }
      : saved.search,
  };
}

export function normalizeReportResponse(data: RecommendationReportResponse): RecommendationReportResponse {
  return {
    ...data,
    top_candidates: (data.top_candidates || []).map((tc) => ({
      ...tc,
      top_positives_json: tc.top_positives_json || [],
      top_risks_json: tc.top_risks_json || [],
      score_breakdown_json: tc.score_breakdown_json || DEFAULT_SCORE_BREAKDOWN,
      feature_snapshot_json: tc.feature_snapshot_json || {},
    })),
    assumptions: data.assumptions || {},
    recommendation: {
      ...(data.recommendation || {}),
    },
    brand_profile: data.brand_profile || {},
    meta: data.meta || {},
  };
}

export function normalizeMemoResponse(data: CandidateMemoResponse): CandidateMemoResponse {
  const cand = data.candidate || {};
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
    },
    market_research: data.market_research || {},
  };
}

export function normalizeCompareResponse(data: CompareCandidatesResponse): CompareCandidatesResponse {
  return {
    ...data,
    items: (data.items || []).map((item) => ({
      ...item,
      gate_status_json: item.gate_status_json || {},
    })),
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
  return { ...data, items: normalizeCandidates(data.items || []) };
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
export async function getExpansionCandidateMemo(candidateId: string): Promise<CandidateMemoResponse> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/candidates/${candidateId}/memo`)); const data = await readJson<CandidateMemoResponse>(res); return normalizeMemoResponse(data); }
export async function getExpansionRecommendationReport(searchId: string): Promise<RecommendationReportResponse> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/searches/${searchId}/report`)); const data = await readJson<RecommendationReportResponse>(res); return normalizeReportResponse(data); }
export async function createSavedExpansionSearch(payload: Omit<SavedExpansionSearch, "id">): Promise<SavedExpansionSearch> { const res = await fetchWithAuth(buildApiUrl("/v1/expansion-advisor/saved-searches"), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) }); return normalizeSavedSearch(await readJson<SavedExpansionSearch>(res)); }
export async function listSavedExpansionSearches(status?: "draft" | "final", limit = 20): Promise<SavedExpansionSearchListResponse> { const params = new URLSearchParams({ limit: String(limit) }); if (status) params.set("status", status); const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches?${params.toString()}`)); const data = await readJson<SavedExpansionSearchListResponse>(res); return { items: (data.items || []).map(normalizeSavedSearch) }; }
export async function getSavedExpansionSearch(savedId: string): Promise<SavedExpansionSearch> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches/${savedId}`)); return normalizeSavedSearch(await readJson<SavedExpansionSearch>(res)); }
export async function updateSavedExpansionSearch(savedId: string, payload: Partial<SavedExpansionSearch>): Promise<SavedExpansionSearch> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches/${savedId}`), { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) }); return normalizeSavedSearch(await readJson<SavedExpansionSearch>(res)); }
export async function deleteSavedExpansionSearch(savedId: string): Promise<{ deleted: boolean }> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches/${savedId}`), { method: "DELETE" }); return readJson<{ deleted: boolean }>(res); }
