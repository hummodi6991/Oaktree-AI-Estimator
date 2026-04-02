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
import { humanGateLabel, humanGateSentence, candidateDistrictLabel } from "./formatHelpers";

/* ─── Brief payload normalization ─── */

/* Enum lookup maps — map display-labels and case variants to valid backend values */
const SERVICE_MODEL_MAP: Record<string, ExpansionBrief["service_model"]> = {
  qsr: "qsr",
  "quick service": "qsr",
  dine_in: "dine_in",
  "dine in": "dine_in",
  dinein: "dine_in",
  delivery_first: "delivery_first",
  "delivery first": "delivery_first",
  deliveryfirst: "delivery_first",
  cafe: "cafe",
  café: "cafe",
};

const PRICE_TIER_MAP: Record<string, NonNullable<ExpansionBrief["brand_profile"]>["price_tier"]> = {
  value: "value",
  mid: "mid",
  premium: "premium",
};

const PRIMARY_CHANNEL_MAP: Record<string, "dine_in" | "delivery" | "balanced"> = {
  dine_in: "dine_in",
  "dine in": "dine_in",
  dinein: "dine_in",
  delivery: "delivery",
  balanced: "balanced",
};

const EXPANSION_GOAL_MAP: Record<string, "flagship" | "neighborhood" | "delivery_led" | "balanced"> = {
  flagship: "flagship",
  neighborhood: "neighborhood",
  delivery_led: "delivery_led",
  "delivery led": "delivery_led",
  balanced: "balanced",
};

const SENSITIVITY_MAP: Record<string, "low" | "medium" | "high"> = {
  low: "low",
  medium: "medium",
  high: "high",
};

function normalizeEnum<T>(value: unknown, map: Record<string, T>): T | null {
  if (value == null || value === "") return null;
  const key = String(value).toLowerCase().trim();
  return map[key] ?? null;
}

/** Coerce empty/whitespace-only strings to undefined so JSON.stringify omits them */
function emptyToUndefined(v: string | undefined | null): string | undefined {
  if (v == null) return undefined;
  const trimmed = v.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/** Strip blank strings and zero-valued optional fields before submit */
export function normalizeBriefPayload(raw: ExpansionBrief): ExpansionBrief {
  const profile = raw.brand_profile ? { ...raw.brand_profile } : {};

  // Normalize enum profile fields to valid backend Literal values
  profile.price_tier = normalizeEnum(profile.price_tier, PRICE_TIER_MAP);
  profile.primary_channel = normalizeEnum(profile.primary_channel, PRIMARY_CHANNEL_MAP);
  profile.expansion_goal = normalizeEnum(profile.expansion_goal, EXPANSION_GOAL_MAP);
  profile.parking_sensitivity = normalizeEnum(profile.parking_sensitivity, SENSITIVITY_MAP);
  profile.frontage_sensitivity = normalizeEnum(profile.frontage_sensitivity, SENSITIVITY_MAP);
  profile.visibility_sensitivity = normalizeEnum(profile.visibility_sensitivity, SENSITIVITY_MAP);

  // Coerce empty strings to null for free-text optional profile fields
  if (!profile.target_customer || !profile.target_customer.trim()) profile.target_customer = null;

  // Clean district arrays
  if (profile.preferred_districts?.length === 0) profile.preferred_districts = null;
  if (profile.excluded_districts?.length === 0) profile.excluded_districts = null;

  // Clean cannibalization
  if (!profile.cannibalization_tolerance_m) profile.cannibalization_tolerance_m = null;
  profile.average_check_sar = null;

  // Clean target_area_m2
  const target_area_m2 = raw.target_area_m2 && raw.target_area_m2 > 0 ? raw.target_area_m2 : null;

  // Normalize service_model — map display labels/case variants to backend enum
  const service_model: ExpansionBrief["service_model"] =
    normalizeEnum(raw.service_model, SERVICE_MODEL_MAP) ?? "qsr";

  // Ensure category is non-empty (backend requires min_length=1)
  const category = (raw.category || "").trim() || service_model;

  // Coerce lat/lon to numbers first, then filter invalid/zero entries,
  // then clean name/district (backend ExistingBranchInput has min_length=1; empty strings → 422)
  const existing_branches = (raw.existing_branches || [])
    .map((b) => ({
      ...b,
      lat: Number(b.lat),
      lon: Number(b.lon),
      name: emptyToUndefined(b.name),
      district: emptyToUndefined(b.district),
    }))
    .filter(
      (b) => Number.isFinite(b.lat) && Number.isFinite(b.lon) && (b.lat !== 0 || b.lon !== 0),
    );

  // Clean target_districts
  const target_districts = (raw.target_districts || []).filter((d) => d && d.trim());

  return {
    ...raw,
    brand_name: (raw.brand_name || "").trim(),
    category,
    service_model,
    target_area_m2,
    target_districts,
    existing_branches,
    brand_profile: profile,
    limit: raw.limit || 15,
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
    result = result.filter((c) => candidateDistrictLabel(c, "").toLowerCase().includes(d));
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
      return sorted.sort((a, b) => candidateDistrictLabel(a, "").localeCompare(candidateDistrictLabel(b, "")));
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
  key: string;
  name: string;
  status: "pass" | "fail" | "unknown";
  explanation?: string;
};

export function parseGateEntries(
  gateStatus: Record<string, boolean | null | undefined> | undefined,
  gateReasons: CandidateGateReasons | undefined,
): GateEntry[] {
  if (!gateStatus) return [];
  const explanations = gateReasons?.explanations || {};
  const unknownSet = new Set(gateReasons?.unknown || []);
  const entries: GateEntry[] = [];
  const seenLabels = new Set<string>();

  for (const [key, passed] of Object.entries(gateStatus)) {
    if (key === "overall_pass") continue; // skip meta-key
    const isUnknown = unknownSet.has(key) || passed === null || passed === undefined;
    const label = humanGateLabel(key);
    seenLabels.add(label);
    entries.push({
      key,
      name: label,
      status: isUnknown ? "unknown" : (passed ? "pass" : "fail"),
      explanation: explanations[key] ? String(explanations[key]) : undefined,
    });
  }

  // Add unknown gates from reasons that aren't already listed
  if (gateReasons?.unknown) {
    for (const rawKey of gateReasons.unknown) {
      const label = humanGateLabel(rawKey);
      if (!seenLabels.has(label)) {
        seenLabels.add(label);
        entries.push({
          key: rawKey,
          name: label,
          status: "unknown",
          explanation: explanations[rawKey] ? String(explanations[rawKey]) : undefined,
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
    const label = candidateDistrictLabel(c, "");
    if (label) set.add(label);
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

export type MapViewState = {
  center?: [number, number] | null;
  zoom?: number | null;
};

export type DrawerKey = "none" | "memo" | "compare" | "report" | "save";

export function buildUiStateJson(
  selectedCandidateId: string | null,
  compareIds: string[],
  leadCandidateId: string | null,
  activeFilter: FilterKey,
  activeSort: SortKey,
  districtFilter: string,
  mapView?: MapViewState | null,
  activeDrawer?: DrawerKey,
): Record<string, unknown> {
  return {
    selected_candidate_id: selectedCandidateId,
    compare_ids: compareIds,
    lead_candidate_id: leadCandidateId,
    active_filter: activeFilter,
    active_sort: activeSort,
    district_filter: districtFilter,
    map_center: mapView?.center ?? null,
    map_zoom: mapView?.zoom ?? null,
    active_drawer: activeDrawer ?? "none",
  };
}

export function restoreMapViewState(
  uiState: Record<string, unknown> | null | undefined,
): MapViewState {
  const raw = (uiState || {}) as Record<string, unknown>;
  const center = Array.isArray(raw.map_center) && raw.map_center.length === 2
    ? [Number(raw.map_center[0]), Number(raw.map_center[1])] as [number, number]
    : null;
  const zoom = typeof raw.map_zoom === "number" ? raw.map_zoom : null;
  return { center, zoom };
}

export function restoreDrawerState(
  uiState: Record<string, unknown> | null | undefined,
): DrawerKey {
  const raw = (uiState || {}) as Record<string, unknown>;
  const validDrawers: DrawerKey[] = ["none", "memo", "compare", "report", "save"];
  const d = typeof raw.active_drawer === "string" && validDrawers.includes(raw.active_drawer as DrawerKey)
    ? (raw.active_drawer as DrawerKey)
    : "none";
  return d;
}

/* ─── Finalist workspace view models ─── */

export type FinalistTile = {
  id: string;
  rankPosition: number | null;
  district: string;
  gateVerdict: string;
  paybackBand: string;
  paybackMonths: number | null;
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
        district: candidateDistrictLabel(candidate, candidate.parcel_id || "—"),
        gateVerdict: gatePass === true ? "pass" : gatePass === false ? "fail" : "unknown",
        paybackBand: candidate.payback_band || "—",
        paybackMonths: candidate.estimated_payback_months ?? null,
        estimatedAnnualRent: candidate.estimated_annual_rent_sar ?? null,
        fitoutCost: candidate.estimated_fitout_cost_sar ?? null,
        revenueIndex: candidate.estimated_revenue_index ?? null,
        bestStrength: positives[0] || "—",
        mainRisk: risks[0] || "—",
        finalScore: candidate.score_breakdown_json?.display_score ?? candidate.final_score ?? null,
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

  // Site fit — check if scores are estimated (fallback) vs observed
  const sfc = candidate.site_fit_context;
  const scoreModeMap: Record<string, "observed" | "estimated" | undefined> = {
    frontage: sfc?.frontage_score_mode,
    access: sfc?.access_score_mode,
    parking: sfc?.parking_score_mode,
  };
  for (const key of ["zoning", "frontage", "parking", "access", "visibility"]) {
    const gateKey = `${key}_pass`;
    const scoreKey = `${key === "visibility" ? "access_visibility" : key}_score`;
    const isEstimated = scoreModeMap[key] === "estimated";
    if (gateKey in gates) {
      const gateVal = gates[gateKey];
      // When context is unavailable, unknown/null gates should be "verify", not "risk"
      const estimatedSuffix = isEstimated ? " (estimated)" : "";
      items.push({
        category: "site_fit",
        label: `${humanGateLabel(gateKey)} gate${estimatedSuffix}`,
        status: gateVal === true ? "strong" : gateVal === null || gateVal === undefined || isEstimated ? "verify" : "risk",
      });
    } else if ((candidate as Record<string, unknown>)[scoreKey] != null) {
      const val = Number((candidate as Record<string, unknown>)[scoreKey]);
      if (isEstimated) {
        items.push({
          category: "site_fit",
          label: `${humanGateLabel(gateKey)} score: ${Math.round(val)} (estimated)`,
          status: "verify",
        });
      } else {
        items.push({
          category: "site_fit",
          label: `${humanGateLabel(gateKey)} score: ${Math.round(val)}`,
          status: val >= 70 ? "strong" : val >= 40 ? "caution" : "risk",
        });
      }
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
    const isStrong = band === "fast" || band === "promising" || band === "strong";
    const isNeutral = band === "moderate" || band === "standard" || band === "borderline";
    items.push({
      category: "economics",
      label: `Payback: ${candidate.payback_band}${candidate.estimated_payback_months ? ` (${Math.round(candidate.estimated_payback_months)} mo)` : ""}`,
      status: isStrong ? "strong" : isNeutral ? "caution" : "risk",
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
  siteLabel: string;
  bestCandidate: string;
  topReason: string;
  mainRisk: string;
  bestFormat: string;
  nextValidation: string;
  allGatesPass: boolean;
  noPassNotice: string | null;
};

/**
 * Derive search-level pass count from candidates list for consistent summary.
 */
export function searchPassCount(candidates: ExpansionCandidate[]): number {
  return candidates.filter((c) => c.gate_status_json?.overall_pass === true).length;
}

export function buildCopySummary(
  candidate: ExpansionCandidate | null,
  report: RecommendationReportResponse | null,
  memo: CandidateMemoResponse | null,
  /** Pass count at search level — when > 0, suppress the "no pass" notice */
  searchLevelPassCount?: number,
): CopySummary {
  const rec = report?.recommendation || {};
  const memoRec = memo?.recommendation || {};
  const positives = candidate?.top_positives_json || [];
  const risks = candidate?.top_risks_json || [];
  const unknowns = candidate?.gate_reasons_json?.unknown || [];
  const missing = candidate?.feature_snapshot_json?.missing_context || [];
  const gatePass = candidate?.gate_status_json?.overall_pass;
  const allGatesPass = gatePass === true;

  // pass_count is strict (only overall_pass === true).
  // validation_clear_count tracks candidates with no blocking failures but unresolved gates.
  const recAny = rec as Record<string, unknown>;
  const reportPassCount = typeof recAny.pass_count === "number" ? recAny.pass_count : undefined;
  const reportValidationClear = typeof recAny.validation_clear_count === "number" ? recAny.validation_clear_count : 0;
  const effectivePassCount = searchLevelPassCount ?? reportPassCount ?? (allGatesPass ? 1 : 0);
  const hasStrictPasses = effectivePassCount > 0;
  // Suppress "no pass" notice when validation-clear candidates exist (no blocking failures)
  const hasValidationClear = reportValidationClear > 0;

  const nextRaw = unknowns[0] || missing[0] || null;
  const nextValidation = nextRaw ? humanGateLabel(nextRaw) + " needs field verification." : "Site visit recommended";

  return {
    siteLabel: allGatesPass ? "Lead site" : "Top ranked candidate",
    bestCandidate: candidate
      ? `#${candidate.rank_position || "?"} ${candidateDistrictLabel(candidate, candidate.parcel_id || "—")}`
      : "—",
    topReason: rec.why_best || memoRec.best_use_case || positives[0] || "—",
    mainRisk: rec.main_risk || memoRec.main_watchout || risks[0] || "—",
    bestFormat: rec.best_format || memoRec.best_use_case || "—",
    nextValidation,
    allGatesPass,
    // Show "no pass" notice only when no strict passes AND no validation-clear candidates.
    // When validation-clear candidates exist, the summary already explains the state.
    noPassNotice: hasStrictPasses || hasValidationClear ? null : "No candidate currently passes all required gates.",
  };
}

export function formatCopySummaryText(summary: CopySummary): string {
  const lines = [
    `${summary.siteLabel}: ${summary.bestCandidate}`,
  ];
  if (summary.noPassNotice) lines.push(`Note: ${summary.noPassNotice}`);
  lines.push(
    `Top reason: ${summary.topReason}`,
    `Main risk: ${summary.mainRisk}`,
    `Best format: ${summary.bestFormat}`,
    `Next step: ${summary.nextValidation}`,
  );
  return lines.join("\n");
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

/* ─── Validation plan derivation ─── */

export type ValidationPriority = "must_verify" | "nice_to_confirm" | "already_strong";

export type ValidationPlanItem = {
  priority: ValidationPriority;
  label: string;
  detail: string;
};

export function deriveValidationPlan(
  candidate: ExpansionCandidate,
  memo?: CandidateMemoResponse | null,
  report?: RecommendationReportResponse | null,
): ValidationPlanItem[] {
  const items: ValidationPlanItem[] = [];
  const gates = candidate.gate_status_json || {};
  const reasons = candidate.gate_reasons_json;
  const snapshot = candidate.feature_snapshot_json;
  const risks = candidate.top_risks_json || [];
  const memoRec = memo?.recommendation || {};
  const reportRec = report?.recommendation || {};

  // Site visit — always must-verify if lead candidate exists
  items.push({
    priority: "must_verify",
    label: "Site visit",
    detail: "Physically verify location, traffic flow, and surroundings",
  });

  // Landlord rent verification — check economics
  if (candidate.estimated_annual_rent_sar != null || candidate.estimated_rent_sar_m2_year != null) {
    items.push({
      priority: "must_verify",
      label: "Landlord rent verification",
      detail: `Confirm actual rent vs estimated ${candidate.estimated_rent_sar_m2_year ? Math.round(candidate.estimated_rent_sar_m2_year) + " SAR/m²/yr" : ""}`.trim(),
    });
  }

  // Frontage/access — based on gate status
  const frontageGate = gates.frontage_pass;
  const accessGate = gates.access_pass;
  if (frontageGate === false) {
    items.push({ priority: "must_verify", label: "Frontage/access confirmation", detail: "Frontage gate failed — verify physical street frontage" });
  } else if (frontageGate === true && accessGate === true) {
    items.push({ priority: "already_strong", label: "Frontage/access confirmation", detail: "Both frontage and access gates passed" });
  } else {
    items.push({ priority: "nice_to_confirm", label: "Frontage/access confirmation", detail: "Confirm street-level frontage and pedestrian access" });
  }

  // Parking check — sensitive for dine-in
  const parkingGate = gates.parking_pass;
  if (parkingGate === false) {
    items.push({ priority: "must_verify", label: "Parking check", detail: "Parking gate failed — confirm available parking spaces" });
  } else if (parkingGate === true) {
    items.push({ priority: "already_strong", label: "Parking check", detail: "Parking gate passed" });
  } else if (candidate.parking_score != null) {
    items.push({
      priority: candidate.parking_score >= 70 ? "already_strong" : "nice_to_confirm",
      label: "Parking check",
      detail: `Parking score: ${Math.round(candidate.parking_score)}`,
    });
  }

  // Delivery catchment
  if (candidate.provider_whitespace_score != null) {
    const strong = candidate.provider_whitespace_score >= 70;
    items.push({
      priority: strong ? "already_strong" : "nice_to_confirm",
      label: "Delivery catchment validation",
      detail: strong
        ? `Whitespace score ${Math.round(candidate.provider_whitespace_score)} — strong delivery opportunity`
        : `Whitespace score ${Math.round(candidate.provider_whitespace_score)} — verify delivery demand in catchment`,
    });
  }

  // Competitor field check
  const comps = candidate.comparable_competitors_json || [];
  if (comps.length > 0) {
    items.push({
      priority: "nice_to_confirm",
      label: "Competitor field check",
      detail: `${comps.length} comparable competitor(s) identified — verify current operating status`,
    });
  } else {
    items.push({
      priority: "nice_to_confirm",
      label: "Competitor field check",
      detail: "Verify nearby competitors on the ground",
    });
  }

  // Branch cannibalization
  if (candidate.distance_to_nearest_branch_m != null) {
    const km = (candidate.distance_to_nearest_branch_m / 1000).toFixed(1);
    const safe = candidate.distance_to_nearest_branch_m >= 2000;
    items.push({
      priority: safe ? "already_strong" : "must_verify",
      label: "Branch cannibalization sanity check",
      detail: safe
        ? `Nearest own branch ${km} km away — low cannibalization risk`
        : `Nearest own branch only ${km} km away — assess overlap impact`,
    });
  } else if (candidate.cannibalization_score != null) {
    items.push({
      priority: candidate.cannibalization_score <= 30 ? "already_strong" : "must_verify",
      label: "Branch cannibalization sanity check",
      detail: `Cannibalization score: ${Math.round(candidate.cannibalization_score)}`,
    });
  }

  // Add unknown gate items as must-verify
  if (reasons?.unknown) {
    for (const u of reasons.unknown) {
      const label = humanGateLabel(u);
      if (!items.some((i) => i.label.toLowerCase().includes(label.toLowerCase()))) {
        items.push({ priority: "must_verify", label: `Verify: ${label}`, detail: "Data unavailable — field verification required" });
      }
    }
  }

  // Add missing context sources as nice-to-confirm
  if (snapshot?.missing_context) {
    for (const m of snapshot.missing_context) {
      const label = humanGateLabel(m);
      if (!items.some((i) => i.label.toLowerCase().includes(label.toLowerCase()))) {
        items.push({ priority: "nice_to_confirm", label: `Verify: ${label}`, detail: "Not available from current data sources" });
      }
    }
  }

  return items;
}

/* ─── Assumptions & confidence derivation ─── */

export type AssumptionConfidence = "strong" | "estimated" | "missing";

export type AssumptionItem = {
  label: string;
  confidence: AssumptionConfidence;
  detail: string;
};

export function deriveAssumptions(
  candidate: ExpansionCandidate,
  report?: RecommendationReportResponse | null,
): AssumptionItem[] {
  const items: AssumptionItem[] = [];
  const snapshot = candidate.feature_snapshot_json;
  const available = Object.keys(snapshot?.context_sources || {});
  const missing = snapshot?.missing_context || [];
  const reportAssumptions = report?.assumptions || {};

  // Score/rank data
  if (candidate.final_score != null) {
    items.push({ label: "Overall score", confidence: "strong", detail: `Score ${Math.round(candidate.final_score)} from deterministic model` });
  }

  // Gate data
  const gateKeys = Object.keys(candidate.gate_status_json || {});
  if (gateKeys.length > 0) {
    items.push({ label: "Gate checks", confidence: "strong", detail: `${gateKeys.length} gates evaluated deterministically` });
  }

  // Economics
  if (candidate.economics_score != null && candidate.estimated_annual_rent_sar != null) {
    items.push({ label: "Economics model", confidence: "estimated", detail: "Rent and revenue estimates from comparable data" });
  } else if (candidate.economics_score != null) {
    items.push({ label: "Economics model", confidence: "estimated", detail: "Economics score from modeled inputs" });
  }

  // Brand fit
  if (candidate.brand_fit_score != null) {
    items.push({
      label: "Brand fit",
      confidence: available.includes("google_places") || available.includes("osm") ? "strong" : "estimated",
      detail: available.includes("google_places") ? "Based on observed market data" : "Based on modeled market characteristics",
    });
  }

  // Delivery market
  if (candidate.provider_whitespace_score != null) {
    items.push({
      label: "Delivery market",
      confidence: available.includes("delivery_platforms") ? "strong" : "estimated",
      detail: available.includes("delivery_platforms") ? "Based on observed platform data" : "Estimated from area characteristics",
    });
  }

  // Missing data
  for (const m of missing) {
    items.push({ label: m.replace(/_/g, " "), confidence: "missing", detail: "Not available from current data sources" });
  }

  // Report-level assumptions
  for (const [key, value] of Object.entries(reportAssumptions)) {
    if (!items.some((i) => i.label.toLowerCase() === key.replace(/_/g, " ").toLowerCase())) {
      items.push({ label: key.replace(/_/g, " "), confidence: "estimated", detail: String(value) });
    }
  }

  return items;
}

/* ─── Decision snapshot shaping ─── */

export type DecisionSnapshot = {
  /** Label for the lead row — "Lead Site" only when gates pass. */
  siteLabel: string;
  leadSite: string;
  leadDistrict: string;
  leadParcelId: string;
  whyItWins: string;
  whyItWinsLabel: string;
  mainRisk: string;
  bestFormat: string;
  nextValidation: string;
  confidenceGrade: string;
  gateVerdict: string;
  allGatesPass: boolean;
  finalScore: number | null;
  rankPosition: number | null;
};

export function buildDecisionSnapshot(
  candidate: ExpansionCandidate,
  report?: RecommendationReportResponse | null,
  memo?: CandidateMemoResponse | null,
  /** Search-level pass count — used to avoid contradicting search header */
  searchLevelPassCount?: number,
): DecisionSnapshot {
  const rec = report?.recommendation || {};
  const memoRec = memo?.recommendation || {};
  const positives = candidate.top_positives_json || [];
  const risks = candidate.top_risks_json || [];
  const unknowns = candidate.gate_reasons_json?.unknown || [];
  const missing = candidate.feature_snapshot_json?.missing_context || [];
  const gatePass = candidate.gate_status_json?.overall_pass;
  const allGatesPass = gatePass === true;

  // pass_count is strict. validation_clear_count tracks no-blocking-failure candidates.
  const recAny = rec as Record<string, unknown>;
  const reportPassCount = typeof recAny.pass_count === "number" ? recAny.pass_count : undefined;
  const reportValidationClear = typeof recAny.validation_clear_count === "number" ? recAny.validation_clear_count : 0;
  const effectivePassCount = searchLevelPassCount ?? reportPassCount ?? (allGatesPass ? 1 : 0);
  const hasStrictPasses = effectivePassCount > 0;
  const hasValidationClear = reportValidationClear > 0;

  const nextRaw = unknowns[0] || missing[0] || null;
  const nextValidation = nextRaw ? humanGateLabel(nextRaw) + " needs field verification." : "Site visit recommended";

  return {
    siteLabel: allGatesPass ? "Lead Site" : "Top ranked candidate",
    leadSite: `#${candidate.rank_position || "?"} ${candidateDistrictLabel(candidate, candidate.parcel_id || "—")}`,
    leadDistrict: candidateDistrictLabel(candidate, "—"),
    leadParcelId: candidate.parcel_id || "—",
    whyItWins: rec.why_best || memoRec.best_use_case || positives[0] || "—",
    whyItWinsLabel: allGatesPass ? "Why it wins" : "Top strength",
    mainRisk: rec.main_risk || memoRec.main_watchout || risks[0] || "—",
    bestFormat: rec.best_format || memoRec.best_use_case || "—",
    nextValidation,
    confidenceGrade: candidate.confidence_grade || "—",
    gateVerdict: gatePass === true ? "pass" : gatePass === false ? "fail" : "unknown",
    // Show "no pass" notice only when no strict passes AND no validation-clear candidates
    allGatesPass: hasStrictPasses || hasValidationClear,
    finalScore: candidate.score_breakdown_json?.display_score ?? candidate.final_score ?? null,
    rankPosition: candidate.rank_position ?? null,
  };
}

/* ─── Compare outcome derivation ─── */

export type CompareOutcome = {
  winnerId: string | null;
  winnerLabel: string;
  runnerUpStrengths: string[];
  whatWouldChange: string;
  leadsAligned: boolean;
};

export function deriveCompareOutcome(
  result: { items: Array<Record<string, unknown>>; summary: Record<string, string | null> } | null,
  candidates: ExpansionCandidate[],
  leadCandidateId: string | null,
): CompareOutcome {
  const fallback: CompareOutcome = { winnerId: null, winnerLabel: "—", runnerUpStrengths: [], whatWouldChange: "—", leadsAligned: true };
  if (!result || !result.items.length) return fallback;

  const bestOverall = result.summary?.best_overall_candidate_id || null;
  const bestCandidate = bestOverall ? candidates.find((c) => c.id === bestOverall) : null;
  const winnerLabel = bestCandidate
    ? `#${bestCandidate.rank_position || "?"} ${candidateDistrictLabel(bestCandidate, bestCandidate.parcel_id || "—")}`
    : bestOverall?.slice(0, 8) || "—";

  // Find dimensions where runner-up wins
  const runnerUpStrengths: string[] = [];
  const dimensionKeys = [
    "best_economics_candidate_id",
    "fastest_payback_candidate_id",
    "best_brand_fit_candidate_id",
    "highest_demand_candidate_id",
    "strongest_delivery_market_candidate_id",
    "lowest_cannibalization_candidate_id",
    "most_confident_candidate_id",
  ];
  for (const key of dimensionKeys) {
    const winner = result.summary[key];
    if (winner && winner !== bestOverall) {
      const name = key.replace(/_candidate_id$/, "").replace(/_/g, " ");
      runnerUpStrengths.push(name);
    }
  }

  // What would change the decision
  let whatWouldChange = "—";
  if (runnerUpStrengths.length > 0) {
    whatWouldChange = `If ${runnerUpStrengths[0]} were weighted more heavily`;
  }

  const leadsAligned = !leadCandidateId || leadCandidateId === bestOverall;

  return {
    winnerId: bestOverall,
    winnerLabel,
    runnerUpStrengths,
    whatWouldChange,
    leadsAligned,
  };
}

/* ─── Saved-study metadata extraction ─── */

export type SavedStudyMeta = {
  leadDistrict: string | null;
  leadParcelId: string | null;
  leadGatesPass: boolean;
  shortlistCount: number;
  compareCount: number;
  lastSort: string | null;
  lastFilter: string | null;
  isFinal: boolean;
};

export function extractSavedStudyMeta(saved: SavedExpansionSearch): SavedStudyMeta {
  const ui = (saved.ui_state_json || {}) as Record<string, unknown>;
  const compareIds = Array.isArray(ui.compare_ids) ? ui.compare_ids as string[] : [];
  const leadId = typeof ui.lead_candidate_id === "string" ? ui.lead_candidate_id : null;
  const candidates = saved.candidates || [];
  const lead = leadId ? candidates.find((c) => c.id === leadId) : null;
  const sortFilter = restoreSortFilter(saved.ui_state_json);

  return {
    leadDistrict: lead ? candidateDistrictLabel(lead, "—") : null,
    leadParcelId: lead?.parcel_id || leadId?.slice(0, 8) || null,
    leadGatesPass: lead?.gate_status_json?.overall_pass === true,
    shortlistCount: (saved.selected_candidate_ids || []).length,
    compareCount: compareIds.length,
    lastSort: sortFilter.activeSort !== "rank" ? sortFilter.activeSort : null,
    lastFilter: sortFilter.activeFilter !== "all" ? sortFilter.activeFilter : null,
    isFinal: saved.status === "final",
  };
}

/* ─── Landlord briefing text generation ─── */

export function formatLandlordBriefingText(
  candidate: ExpansionCandidate,
  report?: RecommendationReportResponse | null,
  memo?: CandidateMemoResponse | null,
): string {
  const district = candidateDistrictLabel(candidate, "—");
  const parcelId = candidate.parcel_id || "—";
  const rank = candidate.rank_position || "?";
  const rentM2 = candidate.estimated_rent_sar_m2_year ? `${Math.round(candidate.estimated_rent_sar_m2_year)} SAR/m²/yr` : "TBD";
  const annualRent = (candidate.display_annual_rent_sar ?? candidate.estimated_annual_rent_sar) ? `${Math.round(candidate.display_annual_rent_sar ?? candidate.estimated_annual_rent_sar!).toLocaleString()} SAR/yr` : "TBD";
  const format = report?.recommendation?.best_format || memo?.recommendation?.best_use_case || "F&B outlet";
  const gatePass = candidate.gate_status_json?.overall_pass;
  const gateLabel = gatePass === true ? "All gates passed" : gatePass === false ? "Some gates require review" : "Gates pending verification";

  return [
    `Site Visit Briefing`,
    `──────────────────`,
    `District: ${district}`,
    `Parcel: ${parcelId}`,
    `Rank: #${rank}`,
    `Intended use: ${format}`,
    `Estimated rent: ${rentM2} (${annualRent})`,
    `Gate status: ${gateLabel}`,
    ``,
    `Items to verify on site:`,
    `- Confirm street frontage and signage visibility`,
    `- Verify parking availability`,
    `- Check pedestrian and vehicle access points`,
    `- Assess surrounding tenant mix`,
    `- Confirm available area matches requirement`,
  ].join("\n");
}
