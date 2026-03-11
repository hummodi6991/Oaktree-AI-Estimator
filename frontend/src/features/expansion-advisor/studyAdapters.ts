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
