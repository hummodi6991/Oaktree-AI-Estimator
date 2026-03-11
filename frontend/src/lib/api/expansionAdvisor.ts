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
  meta?: ExpansionAdvisorMeta;
  profile?: ExpansionBrandProfile;
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
  weights: Record<string, number>;
  inputs: Record<string, number | string | boolean | null>;
  weighted_components: Record<string, number>;
  final_score: number;
  [key: string]: unknown;
};

export type ComparableCompetitor = {
  id?: string;
  name?: string;
  category?: string;
  district?: string;
  rating?: number;
  review_count?: number;
  distance_m?: number;
  source?: string;
};

export type ExpansionCandidate = {
  id: string;
  search_id: string;
  parcel_id: string;
  district?: string;
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
  estimated_payback_months?: number;
  payback_band?: string;
  gate_status_json?: Record<string, boolean>;
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
  meta?: ExpansionAdvisorMeta;
  search_id: string;
  items: ExpansionCandidate[];
};

export type ExpansionSearchDetailResponse = {
  meta?: ExpansionAdvisorMeta;
  search_id: string;
  brief?: ExpansionBrief;
  [key: string]: unknown;
};

export type ExpansionCandidatesListResponse = {
  meta?: ExpansionAdvisorMeta;
  search_id?: string;
  items: ExpansionCandidate[];
};

export type CompareCandidatesResponse = {
  meta?: ExpansionAdvisorMeta;
  items: ExpansionCandidate[];
  summary?: Record<string, string>;
};

export type CandidateMemoResponse = {
  meta?: ExpansionAdvisorMeta;
  recommendation?: Record<string, unknown>;
  candidate?: Record<string, unknown>;
  market_research?: Record<string, unknown>;
};

export type RecommendationReportResponse = {
  meta?: ExpansionAdvisorMeta;
  recommendation?: Record<string, unknown>;
  top_candidates?: ExpansionCandidate[];
  assumptions?: string[];
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
  search?: ExpansionSearchDetailResponse | Record<string, unknown> | null;
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
export async function getExpansionSearch(searchId: string): Promise<ExpansionSearchDetailResponse> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/searches/${searchId}`)); return readJson<ExpansionSearchDetailResponse>(res); }
export async function getExpansionCandidates(searchId: string): Promise<ExpansionCandidatesListResponse> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/searches/${searchId}/candidates`)); const data = await readJson<ExpansionCandidatesListResponse>(res); return { ...data, items: normalizeCandidates(data.items || []) }; }
export async function compareExpansionCandidates(searchId: string, candidateIds: string[]): Promise<CompareCandidatesResponse> { const res = await fetchWithAuth(buildApiUrl("/v1/expansion-advisor/candidates/compare"), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ search_id: searchId, candidate_ids: candidateIds }) }); const data = await readJson<CompareCandidatesResponse>(res); return { ...data, items: normalizeCandidates(data.items || []) }; }
export async function getExpansionCandidateMemo(candidateId: string): Promise<CandidateMemoResponse> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/candidates/${candidateId}/memo`)); return readJson<CandidateMemoResponse>(res); }
export async function getExpansionRecommendationReport(searchId: string): Promise<RecommendationReportResponse> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/searches/${searchId}/report`)); const data = await readJson<RecommendationReportResponse>(res); return { ...data, top_candidates: normalizeCandidates(data.top_candidates || []) }; }
export async function createSavedExpansionSearch(payload: Omit<SavedExpansionSearch, "id">): Promise<SavedExpansionSearch> { const res = await fetchWithAuth(buildApiUrl("/v1/expansion-advisor/saved-searches"), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) }); return normalizeSavedSearch(await readJson<SavedExpansionSearch>(res)); }
export async function listSavedExpansionSearches(status?: "draft" | "final", limit = 20): Promise<SavedExpansionSearchListResponse> { const params = new URLSearchParams({ limit: String(limit) }); if (status) params.set("status", status); const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches?${params.toString()}`)); const data = await readJson<SavedExpansionSearchListResponse>(res); return { items: (data.items || []).map(normalizeSavedSearch) }; }
export async function getSavedExpansionSearch(savedId: string): Promise<SavedExpansionSearch> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches/${savedId}`)); return normalizeSavedSearch(await readJson<SavedExpansionSearch>(res)); }
export async function updateSavedExpansionSearch(savedId: string, payload: Partial<SavedExpansionSearch>): Promise<SavedExpansionSearch> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches/${savedId}`), { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) }); return normalizeSavedSearch(await readJson<SavedExpansionSearch>(res)); }
export async function deleteSavedExpansionSearch(savedId: string): Promise<{ deleted: boolean }> { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches/${savedId}`), { method: "DELETE" }); return readJson<{ deleted: boolean }>(res); }
