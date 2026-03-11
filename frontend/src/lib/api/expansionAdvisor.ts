import { buildApiUrl, fetchWithAuth } from "../../api";

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

export type ExpansionCandidate = {
  id: string;
  search_id: string;
  parcel_id: string;
  district?: string;
  lat: number;
  lon: number;
  final_score?: number;
  economics_score?: number;
  brand_fit_score?: number;
  provider_density_score?: number;
  provider_whitespace_score?: number;
  multi_platform_presence_score?: number;
  delivery_competition_score?: number;
  confidence_grade?: string;
  gate_status_json?: Record<string, boolean>;
  demand_thesis?: string;
  cost_thesis?: string;
  comparable_competitors_json?: ComparableCompetitor[];
  compare_rank?: number;
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
  search?: Record<string, unknown> | null;
  candidates?: ExpansionCandidate[];
};

async function readJson<T>(res: Response): Promise<T> {
  const text = await res.text();
  return (text ? JSON.parse(text) : {}) as T;
}

export async function createExpansionSearch(payload: ExpansionBrief) { const res = await fetchWithAuth(buildApiUrl("/v1/expansion-advisor/searches"), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) }); return readJson<{ search_id: string; items: ExpansionCandidate[] }>(res); }
export async function getExpansionSearch(searchId: string) { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/searches/${searchId}`)); return readJson<Record<string, unknown>>(res); }
export async function getExpansionCandidates(searchId: string) { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/searches/${searchId}/candidates`)); return readJson<{ items: ExpansionCandidate[] }>(res); }
export async function compareExpansionCandidates(searchId: string, candidateIds: string[]) { const res = await fetchWithAuth(buildApiUrl("/v1/expansion-advisor/candidates/compare"), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ search_id: searchId, candidate_ids: candidateIds }) }); return readJson<Record<string, unknown>>(res); }
export async function getExpansionCandidateMemo(candidateId: string) { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/candidates/${candidateId}/memo`)); return readJson<Record<string, unknown>>(res); }
export async function createSavedExpansionSearch(payload: Omit<SavedExpansionSearch, "id">) { const res = await fetchWithAuth(buildApiUrl("/v1/expansion-advisor/saved-searches"), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) }); return readJson<SavedExpansionSearch>(res); }
export async function listSavedExpansionSearches(status?: "draft" | "final", limit = 20) { const params = new URLSearchParams({ limit: String(limit) }); if (status) params.set("status", status); const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches?${params.toString()}`)); return readJson<{ items: SavedExpansionSearch[] }>(res); }
export async function getSavedExpansionSearch(savedId: string) { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches/${savedId}`)); return readJson<SavedExpansionSearch>(res); }
export async function updateSavedExpansionSearch(savedId: string, payload: Partial<SavedExpansionSearch>) { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches/${savedId}`), { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) }); return readJson<SavedExpansionSearch>(res); }
export async function deleteSavedExpansionSearch(savedId: string) { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/saved-searches/${savedId}`), { method: "DELETE" }); return readJson<{ deleted: boolean }>(res); }
export async function getExpansionRecommendationReport(searchId: string) { const res = await fetchWithAuth(buildApiUrl(`/v1/expansion-advisor/searches/${searchId}/report`)); return readJson<Record<string, unknown>>(res); }
