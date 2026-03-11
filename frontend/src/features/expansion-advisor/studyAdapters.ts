/**
 * Centralized adapters for Expansion Advisor state shaping, payload
 * normalization, caching, and client-side sort/filter logic.
 *
 * All pure functions — no side-effects, safe for tests.
 */

import type {
  ExpansionBrief,
  ExpansionCandidate,
  CandidateMemoResponse,
  RecommendationReportResponse,
  SavedExpansionSearch,
  CandidateScoreBreakdown,
  CandidateGateReasons,
  CandidateFeatureSnapshot,
} from "../../lib/api/expansionAdvisor";
import { defaultBrief } from "./ExpansionBriefForm";

/* ─── Brief payload normalization ─── */

/** Strip blank strings and zero-valued optional fields before submit */
export function normalizeBriefPayload(raw: ExpansionBrief): ExpansionBrief {
  const profile = raw.brand_profile ? { ...raw.brand_profile } : {};

  // Coerce empty strings to null for optional profile fields
  if (!profile.price_tier) profile.price_tier = null;
  if (!profile.primary_channel) profile.primary_channel = null;
  if (!profile.target_customer) profile.target_customer = null;
  if (!profile.expansion_goal) profile.expansion_goal = null;
  if (!profile.parking_sensitivity) profile.parking_sensitivity = null;
  if (!profile.frontage_sensitivity) profile.frontage_sensitivity = null;
  if (!profile.visibility_sensitivity) profile.visibility_sensitivity = null;

  // Clean district arrays
  if (profile.preferred_districts?.length === 0) profile.preferred_districts = null;
  if (profile.excluded_districts?.length === 0) profile.excluded_districts = null;

  // Clean cannibalization
  if (!profile.cannibalization_tolerance_m) profile.cannibalization_tolerance_m = null;
  if (!profile.average_check_sar) profile.average_check_sar = null;

  // Clean target_area_m2
  const target_area_m2 = raw.target_area_m2 && raw.target_area_m2 > 0 ? raw.target_area_m2 : null;

  // Filter empty branches
  const existing_branches = (raw.existing_branches || []).filter(
    (b) => Number.isFinite(b.lat) && Number.isFinite(b.lon) && (b.lat !== 0 || b.lon !== 0),
  );

  // Clean target_districts
  const target_districts = (raw.target_districts || []).filter((d) => d && d.trim());

  return {
    ...raw,
    brand_name: (raw.brand_name || "").trim(),
    category: (raw.category || "").trim(),
    target_area_m2,
    target_districts,
    existing_branches,
    brand_profile: profile,
    limit: raw.limit || 25,
  };
}

/* ─── Candidate presentation helpers ─── */

export type SortKey =
  | "rank"
  | "payback"
  | "economics"
  | "brand_fit"
  | "cannibalization"
  | "delivery"
  | "district";

export type FilterKey =
  | "all"
  | "pass_only"
  | "fastest_payback"
  | "strongest_economics"
  | "strongest_brand_fit"
  | "lowest_cannibalization"
  | "strongest_delivery";

export function filterCandidates(
  candidates: ExpansionCandidate[],
  filter: FilterKey,
  districtFilter?: string,
): ExpansionCandidate[] {
  let result = candidates;

  if (districtFilter) {
    const d = districtFilter.toLowerCase();
    result = result.filter((c) => c.district?.toLowerCase().includes(d));
  }

  switch (filter) {
    case "pass_only":
      return result.filter((c) => c.gate_status_json?.overall_pass === true);
    case "fastest_payback":
      return [...result].sort(
        (a, b) => (a.estimated_payback_months ?? 999) - (b.estimated_payback_months ?? 999),
      );
    case "strongest_economics":
      return [...result].sort((a, b) => (b.economics_score ?? 0) - (a.economics_score ?? 0));
    case "strongest_brand_fit":
      return [...result].sort((a, b) => (b.brand_fit_score ?? 0) - (a.brand_fit_score ?? 0));
    case "lowest_cannibalization":
      return [...result].sort(
        (a, b) => (a.cannibalization_score ?? 999) - (b.cannibalization_score ?? 999),
      );
    case "strongest_delivery":
      return [...result].sort(
        (a, b) =>
          (b.provider_whitespace_score ?? 0) +
          (b.multi_platform_presence_score ?? 0) -
          ((a.provider_whitespace_score ?? 0) + (a.multi_platform_presence_score ?? 0)),
      );
    default:
      return result;
  }
}

export function sortCandidates(candidates: ExpansionCandidate[], sortKey: SortKey): ExpansionCandidate[] {
  const sorted = [...candidates];
  switch (sortKey) {
    case "rank":
      return sorted.sort((a, b) => (a.rank_position ?? 999) - (b.rank_position ?? 999));
    case "payback":
      return sorted.sort(
        (a, b) => (a.estimated_payback_months ?? 999) - (b.estimated_payback_months ?? 999),
      );
    case "economics":
      return sorted.sort((a, b) => (b.economics_score ?? 0) - (a.economics_score ?? 0));
    case "brand_fit":
      return sorted.sort((a, b) => (b.brand_fit_score ?? 0) - (a.brand_fit_score ?? 0));
    case "cannibalization":
      return sorted.sort(
        (a, b) => (a.cannibalization_score ?? 999) - (b.cannibalization_score ?? 999),
      );
    case "delivery":
      return sorted.sort(
        (a, b) =>
          (b.provider_whitespace_score ?? 0) +
          (b.multi_platform_presence_score ?? 0) -
          ((a.provider_whitespace_score ?? 0) + (a.multi_platform_presence_score ?? 0)),
      );
    case "district":
      return sorted.sort((a, b) => (a.district || "").localeCompare(b.district || ""));
    default:
      return sorted;
  }
}

/* ─── Shortlist / compare state helpers ─── */

export function restoreShortlistFromSaved(
  saved: SavedExpansionSearch,
  candidates: ExpansionCandidate[],
): string[] {
  const ids = saved.selected_candidate_ids || [];
  const candidateIdSet = new Set(candidates.map((c) => c.id));
  return ids.filter((id) => candidateIdSet.has(id));
}

export function restoreCompareFromSaved(
  saved: SavedExpansionSearch,
  candidates: ExpansionCandidate[],
): string[] {
  const uiState = (saved.ui_state_json || {}) as Record<string, unknown>;
  const ids = Array.isArray(uiState.compare_ids) ? (uiState.compare_ids as string[]) : [];
  const candidateIdSet = new Set(candidates.map((c) => c.id));
  return ids.filter((id) => candidateIdSet.has(id));
}

/* ─── Memo/report caching keys ─── */

export function memoCacheKey(candidateId: string): string {
  return `memo:${candidateId}`;
}

export function reportCacheKey(searchId: string): string {
  return `report:${searchId}`;
}

export type MemoCache = Map<string, CandidateMemoResponse>;
export type ReportCache = Map<string, RecommendationReportResponse>;

/* ─── Score breakdown presentation ─── */

export type ScoreComponent = {
  label: string;
  weight: number;
  input: number;
  weighted: number;
};

export function parseScoreBreakdown(breakdown: CandidateScoreBreakdown | undefined): ScoreComponent[] {
  if (!breakdown) return [];
  const components: ScoreComponent[] = [];
  const weights = breakdown.weights || {};
  const inputs = breakdown.inputs || {};
  const weighted = breakdown.weighted_components || {};

  for (const key of Object.keys(weights)) {
    components.push({
      label: key.replace(/_/g, " ").replace(/\bscore\b/gi, "").trim(),
      weight: Number(weights[key]) || 0,
      input: Number(inputs[key]) || 0,
      weighted: Number(weighted[key]) || 0,
    });
  }

  return components.sort((a, b) => b.weighted - a.weighted);
}

/* ─── Gate presentation ─── */

export type GateEntry = {
  name: string;
  status: "pass" | "fail" | "unknown";
  explanation?: string;
};

export function parseGateEntries(
  gateStatus: Record<string, boolean> | undefined,
  gateReasons: CandidateGateReasons | undefined,
): GateEntry[] {
  if (!gateStatus) return [];
  const explanations = gateReasons?.explanations || {};
  const entries: GateEntry[] = [];

  for (const [key, passed] of Object.entries(gateStatus)) {
    entries.push({
      name: key.replace(/_/g, " ").replace(/\bpass\b/gi, "").trim(),
      status: passed ? "pass" : "fail",
      explanation: explanations[key] ? String(explanations[key]) : undefined,
    });
  }

  // Add unknown gates from reasons
  if (gateReasons?.unknown) {
    for (const name of gateReasons.unknown) {
      if (!entries.find((e) => e.name === name.replace(/_/g, " ").trim())) {
        entries.push({
          name: name.replace(/_/g, " ").trim(),
          status: "unknown",
          explanation: explanations[name] ? String(explanations[name]) : undefined,
        });
      }
    }
  }

  return entries;
}

/* ─── Feature snapshot presentation ─── */

export function parseFeatureSnapshot(snapshot: CandidateFeatureSnapshot | undefined): {
  completeness: number;
  missingSources: string[];
  availableSources: string[];
} {
  if (!snapshot) return { completeness: 0, missingSources: [], availableSources: [] };
  return {
    completeness: snapshot.data_completeness_score || 0,
    missingSources: snapshot.missing_context || [],
    availableSources: Object.keys(snapshot.context_sources || {}),
  };
}

/* ─── Study title generation ─── */

export function generateStudyTitle(brief: ExpansionBrief): string {
  const parts: string[] = [];
  if (brief.brand_name) parts.push(brief.brand_name);
  if (brief.category) parts.push(brief.category);
  parts.push("Expansion Study");
  return parts.join(" — ");
}

/* ─── Unique district extraction from candidates ─── */

export function extractDistricts(candidates: ExpansionCandidate[]): string[] {
  const set = new Set<string>();
  for (const c of candidates) {
    if (c.district) set.add(c.district);
  }
  return Array.from(set).sort();
}

/* ─── Lead candidate helpers ─── */

export function restoreLeadCandidateId(
  uiState: Record<string, unknown> | null | undefined,
  candidates: ExpansionCandidate[],
): string | null {
  const raw = (uiState || {}) as Record<string, unknown>;
  const id = typeof raw.lead_candidate_id === "string" ? raw.lead_candidate_id : null;
  if (!id) return null;
  return candidates.some((c) => c.id === id) ? id : null;
}

export function restoreSortFilter(
  uiState: Record<string, unknown> | null | undefined,
): { activeFilter: FilterKey; activeSort: SortKey; districtFilter: string } {
  const raw = (uiState || {}) as Record<string, unknown>;
  const validFilters: FilterKey[] = ["all", "pass_only", "fastest_payback", "strongest_economics", "strongest_brand_fit", "lowest_cannibalization", "strongest_delivery"];
  const validSorts: SortKey[] = ["rank", "payback", "economics", "brand_fit", "cannibalization", "delivery", "district"];
  const f = typeof raw.active_filter === "string" && validFilters.includes(raw.active_filter as FilterKey) ? (raw.active_filter as FilterKey) : "all";
  const s = typeof raw.active_sort === "string" && validSorts.includes(raw.active_sort as SortKey) ? (raw.active_sort as SortKey) : "rank";
  const d = typeof raw.district_filter === "string" ? raw.district_filter : "";
  return { activeFilter: f, activeSort: s, districtFilter: d };
}

export function buildUiStateJson(
  selectedCandidateId: string | null,
  compareIds: string[],
  leadCandidateId: string | null,
  activeFilter: FilterKey,
  activeSort: SortKey,
  districtFilter: string,
): Record<string, unknown> {
  return {
    selected_candidate_id: selectedCandidateId,
    compare_ids: compareIds,
    lead_candidate_id: leadCandidateId,
    active_filter: activeFilter,
    active_sort: activeSort,
    district_filter: districtFilter,
  };
}

/* ─── Finalist workspace view models ─── */

export type FinalistTile = {
  id: string;
  rankPosition: number | null;
  district: string;
  gateVerdict: string;
  paybackBand: string;
  estimatedAnnualRent: number | null;
  fitoutCost: number | null;
  revenueIndex: number | null;
  bestStrength: string;
  mainRisk: string;
  finalScore: number | null;
  confidenceGrade: string;
  isLead: boolean;
};

export function buildFinalistTiles(
  candidates: ExpansionCandidate[],
  shortlistIds: string[],
  leadCandidateId: string | null,
): FinalistTile[] {
  return shortlistIds
    .map((id) => candidates.find((c) => c.id === id))
    .filter(Boolean)
    .map((c) => {
      const candidate = c!;
      const positives = candidate.top_positives_json || [];
      const risks = candidate.top_risks_json || [];
      const gatePass = candidate.gate_status_json?.overall_pass;
      return {
        id: candidate.id,
        rankPosition: candidate.rank_position ?? null,
        district: candidate.district || candidate.parcel_id || "—",
        gateVerdict: gatePass === true ? "pass" : gatePass === false ? "fail" : "unknown",
        paybackBand: candidate.payback_band || "—",
        estimatedAnnualRent: candidate.estimated_annual_rent_sar ?? null,
        fitoutCost: candidate.estimated_fitout_cost_sar ?? null,
        revenueIndex: candidate.estimated_revenue_index ?? null,
        bestStrength: positives[0] || "—",
        mainRisk: risks[0] || "—",
        finalScore: candidate.final_score ?? null,
        confidenceGrade: candidate.confidence_grade || "—",
        isLead: candidate.id === leadCandidateId,
      };
    });
}

/* ─── Decision checklist derivation ─── */

export type ChecklistCategory = "market_demand" | "site_fit" | "cannibalization" | "delivery_market" | "economics" | "unknowns";

export type ChecklistItem = {
  category: ChecklistCategory;
  label: string;
  status: "strong" | "caution" | "risk" | "verify";
};

export function deriveDecisionChecklist(
  candidate: ExpansionCandidate,
  memo?: CandidateMemoResponse | null,
): ChecklistItem[] {
  const items: ChecklistItem[] = [];
  const gates = candidate.gate_status_json || {};
  const reasons = candidate.gate_reasons_json;
  const snapshot = candidate.feature_snapshot_json;

  // Market demand
  if (candidate.brand_fit_score != null) {
    items.push({
      category: "market_demand",
      label: `Brand fit score: ${Math.round(candidate.brand_fit_score)}`,
      status: candidate.brand_fit_score >= 70 ? "strong" : candidate.brand_fit_score >= 40 ? "caution" : "risk",
    });
  }
  if (candidate.estimated_revenue_index != null) {
    items.push({
      category: "market_demand",
      label: `Revenue index: ${candidate.estimated_revenue_index.toFixed(1)}`,
      status: candidate.estimated_revenue_index >= 70 ? "strong" : candidate.estimated_revenue_index >= 40 ? "caution" : "risk",
    });
  }
  if (memo?.market_research?.district_fit_summary) {
    items.push({ category: "market_demand", label: "District fit assessed", status: "strong" });
  }

  // Site fit
  for (const key of ["zoning", "frontage", "parking", "access", "visibility"]) {
    const gateKey = `${key}_pass`;
    const scoreKey = `${key === "visibility" ? "access_visibility" : key}_score`;
    if (gateKey in gates) {
      items.push({
        category: "site_fit",
        label: `${key.charAt(0).toUpperCase() + key.slice(1)} gate`,
        status: gates[gateKey] ? "strong" : "risk",
      });
    } else if ((candidate as Record<string, unknown>)[scoreKey] != null) {
      const val = Number((candidate as Record<string, unknown>)[scoreKey]);
      items.push({
        category: "site_fit",
        label: `${key.charAt(0).toUpperCase() + key.slice(1)} score: ${Math.round(val)}`,
        status: val >= 70 ? "strong" : val >= 40 ? "caution" : "risk",
      });
    }
  }

  // Cannibalization
  if (candidate.cannibalization_score != null) {
    items.push({
      category: "cannibalization",
      label: `Cannibalization score: ${Math.round(candidate.cannibalization_score)}`,
      status: candidate.cannibalization_score <= 30 ? "strong" : candidate.cannibalization_score <= 60 ? "caution" : "risk",
    });
  }
  if (candidate.distance_to_nearest_branch_m != null) {
    const km = (candidate.distance_to_nearest_branch_m / 1000).toFixed(1);
    items.push({
      category: "cannibalization",
      label: `Nearest branch: ${km} km`,
      status: candidate.distance_to_nearest_branch_m >= 2000 ? "strong" : candidate.distance_to_nearest_branch_m >= 800 ? "caution" : "risk",
    });
  }

  // Delivery market
  if (candidate.provider_whitespace_score != null) {
    items.push({
      category: "delivery_market",
      label: `Whitespace score: ${Math.round(candidate.provider_whitespace_score)}`,
      status: candidate.provider_whitespace_score >= 70 ? "strong" : candidate.provider_whitespace_score >= 40 ? "caution" : "risk",
    });
  }
  if (candidate.multi_platform_presence_score != null) {
    items.push({
      category: "delivery_market",
      label: `Multi-platform: ${Math.round(candidate.multi_platform_presence_score)}`,
      status: candidate.multi_platform_presence_score >= 70 ? "strong" : candidate.multi_platform_presence_score >= 40 ? "caution" : "risk",
    });
  }

  // Economics
  if (candidate.economics_score != null) {
    items.push({
      category: "economics",
      label: `Economics score: ${Math.round(candidate.economics_score)}`,
      status: candidate.economics_score >= 70 ? "strong" : candidate.economics_score >= 40 ? "caution" : "risk",
    });
  }
  if (candidate.payback_band) {
    const band = candidate.payback_band.toLowerCase();
    items.push({
      category: "economics",
      label: `Payback: ${candidate.payback_band}${candidate.estimated_payback_months ? ` (${Math.round(candidate.estimated_payback_months)} mo)` : ""}`,
      status: band === "fast" || band === "promising" ? "strong" : band === "moderate" || band === "standard" ? "caution" : "risk",
    });
  }

  // Unknowns to verify
  if (reasons?.unknown) {
    for (const u of reasons.unknown) {
      items.push({ category: "unknowns", label: u.replace(/_/g, " "), status: "verify" });
    }
  }
  if (snapshot?.missing_context) {
    for (const m of snapshot.missing_context) {
      if (!items.some((i) => i.category === "unknowns" && i.label === m.replace(/_/g, " "))) {
        items.push({ category: "unknowns", label: m.replace(/_/g, " "), status: "verify" });
      }
    }
  }

  return items;
}

/* ─── Copy-summary block generation ─── */

export type CopySummary = {
  bestCandidate: string;
  topReason: string;
  mainRisk: string;
  bestFormat: string;
  nextValidation: string;
};

export function buildCopySummary(
  candidate: ExpansionCandidate | null,
  report: RecommendationReportResponse | null,
  memo: CandidateMemoResponse | null,
): CopySummary {
  const rec = report?.recommendation || {};
  const memoRec = memo?.recommendation || {};
  const positives = candidate?.top_positives_json || [];
  const risks = candidate?.top_risks_json || [];
  const unknowns = candidate?.gate_reasons_json?.unknown || [];
  const missing = candidate?.feature_snapshot_json?.missing_context || [];

  return {
    bestCandidate: candidate
      ? `#${candidate.rank_position || "?"} ${candidate.district || candidate.parcel_id || "—"}`
      : "—",
    topReason: rec.why_best || memoRec.best_use_case || positives[0] || "—",
    mainRisk: rec.main_risk || memoRec.main_watchout || risks[0] || "—",
    bestFormat: rec.best_format || memoRec.best_use_case || "—",
    nextValidation: unknowns[0]?.replace(/_/g, " ") || missing[0]?.replace(/_/g, " ") || "Site visit recommended",
  };
}

export function formatCopySummaryText(summary: CopySummary): string {
  return [
    `Lead site: ${summary.bestCandidate}`,
    `Top reason: ${summary.topReason}`,
    `Main risk: ${summary.mainRisk}`,
    `Best format: ${summary.bestFormat}`,
    `Next step: ${summary.nextValidation}`,
  ].join("\n");
}

/* ─── Runner-up helper ─── */

export function findRunnerUp(
  candidates: ExpansionCandidate[],
  shortlistIds: string[],
  leadCandidateId: string | null,
): ExpansionCandidate | null {
  if (!leadCandidateId) return null;
  const shortlisted = shortlistIds
    .filter((id) => id !== leadCandidateId)
    .map((id) => candidates.find((c) => c.id === id))
    .filter(Boolean) as ExpansionCandidate[];
  if (shortlisted.length > 0) return shortlisted[0];
  return candidates.find((c) => c.id !== leadCandidateId) || null;
}
