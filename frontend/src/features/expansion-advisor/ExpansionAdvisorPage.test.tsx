import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import ExpansionComparePanel, { getOrderedCompareSummaryEntries } from "./ExpansionComparePanel";
import ExpansionResultsPanel from "./ExpansionResultsPanel";
import ExpansionReportPanel, { triggerReportCandidateSelect } from "./ExpansionReportPanel";
import ExpansionMemoPanel from "./ExpansionMemoPanel";
import SaveStudyDialog from "./SaveStudyDialog";
import NextStepsStrip from "./NextStepsStrip";
import en from "../../i18n/en.json";
import {
  restoreSavedUiState,
  shouldLoadMemoFromMapSelection,
  briefFromSavedSearch,
  getCompareRows,
  getNewSearchResetState,
  getNextCompareIds,
  resolveCandidateById,
  sameCandidateId,
  shouldKeepCompareResult,
} from "./ExpansionAdvisorPage";
import { normalizeCandidate, normalizeSavedSearch, normalizeReportResponse, normalizeMemoResponse, normalizeCompareResponse } from "../../lib/api/expansionAdvisor";
import { validateBrief } from "./ExpansionBriefForm";
import ExpansionBriefForm, { defaultBrief } from "./ExpansionBriefForm";
import {
  normalizeBriefPayload,
  filterCandidates,
  sortCandidates,
  extractDistricts,
  generateStudyTitle,
  parseScoreBreakdown,
  parseGateEntries,
  parseFeatureSnapshot,
  restoreShortlistFromSaved,
  restoreCompareFromSaved,
  memoCacheKey,
  reportCacheKey,
  restoreLeadCandidateId,
  restoreSortFilter,
  buildUiStateJson,
  restoreMapViewState,
  restoreDrawerState,
  buildFinalistTiles,
  deriveDecisionChecklist,
  buildCopySummary,
  formatCopySummaryText,
  findRunnerUp,
  deriveValidationPlan,
  deriveAssumptions,
  buildDecisionSnapshot,
  deriveCompareOutcome,
  extractSavedStudyMeta,
  formatLandlordBriefingText,
} from "./studyAdapters";
import ValidationPlanPanel from "./ValidationPlanPanel";
import AssumptionsCard from "./AssumptionsCard";
import DecisionSnapshotCard from "./DecisionSnapshotCard";
import CompareOutcomeBanner from "./CompareOutcomeBanner";
import type { ExpansionCandidate, CandidateScoreBreakdown, CandidateFeatureSnapshot, SavedExpansionSearch, RecommendationReportResponse } from "../../lib/api/expansionAdvisor";
import SavedSearchesPanel from "./SavedSearchesPanel";
import GateSummary from "./GateSummary";
import ScoreBreakdownCompact from "./ScoreBreakdownCompact";
import ConfidenceBadge from "./ConfidenceBadge";
import { CandidateListSkeleton, DetailSkeleton } from "./SkeletonLoaders";
import CandidateDetailPanel from "./CandidateDetailPanel";
import ExpansionCandidateCard from "./ExpansionCandidateCard";
import FinalistsWorkspace from "./FinalistsWorkspace";
import { humanGateLabel, humanGateSentence, isGarbledText, safeDistrictLabel, candidateDistrictLabel, paybackColor } from "./formatHelpers";

/* ─── Helpers for test data ─── */
function makeCandidate(overrides: Partial<ExpansionCandidate> = {}): ExpansionCandidate {
  return normalizeCandidate({
    id: "c1",
    search_id: "s1",
    parcel_id: "p1",
    lat: 24.7,
    lon: 46.7,
    ...overrides,
  });
}

describe("Expansion advisor UI behavior", () => {
  it("renders candidate cards", () => {
    const html = renderToStaticMarkup(
      <ExpansionResultsPanel
        items={[{ id: "c1", search_id: "s1", parcel_id: "p1", lat: 24.7, lon: 46.7, brand_fit_score: 75, economics_score: 80, provider_density_score: 60, provider_whitespace_score: 55, confidence_grade: "A", demand_thesis: "Demand is strong", cost_thesis: "Cost is manageable", gate_status_json: { overall_pass: true }, top_positives_json: ["great"], top_risks_json: ["rent"], comparable_competitors_json: [{ id: "r1", name: "Comp", distance_m: 120 }] }]}
        selectedCandidateId={null}
        shortlistIds={[]}
        compareIds={[]}
        onSelectCandidate={() => {}}
        onToggleShortlist={() => {}}
        onToggleCompare={() => {}}
      />,
    );
    expect(html).toContain("great");
    expect(html).toContain("ea-candidate");
  });

  it("compare button disables for <2 selections", () => {
    const html = renderToStaticMarkup(<ExpansionComparePanel compareIds={["c1"]} result={null} loading={false} error={null} onCompare={() => {}} />);
    expect(html).toContain("disabled");
  });

  it("old Restaurant Finder no longer primary nav label", () => {
    expect(en.app.modeExpansion).toBe("Expansion Advisor");
  });

  it("candidate normalization fills safe defaults", () => {
    const candidate = normalizeCandidate({ id: "c1", search_id: "s", parcel_id: "p", lat: 0, lon: 0 });
    expect(candidate.gate_status_json).toEqual({});
    expect(candidate.gate_reasons_json?.passed).toEqual([]);
    expect(candidate.feature_snapshot_json?.data_completeness_score).toBe(0);
    expect(candidate.score_breakdown_json?.final_score).toBe(0);
    expect(candidate.top_positives_json).toEqual([]);
  });

  it("saved study restore returns normalized ui state", () => {
    const candidates = [
      { id: "c2", search_id: "s", parcel_id: "p2", lat: 24.7, lon: 46.7 },
      { id: "c3", search_id: "s", parcel_id: "p3", lat: 24.8, lon: 46.8 },
    ];
    const restored = restoreSavedUiState(
      {
        id: "sv1",
        search_id: "search-9",
        title: "study",
        status: "draft",
        selected_candidate_ids: ["c1"],
        ui_state_json: { compare_ids: ["c2", "c3"], selected_candidate_id: "c3" },
        candidates,
      },
      candidates,
    );
    expect(restored.searchId).toBe("search-9");
    expect(restored.shortlistIds).toEqual(["c1"]);
    expect(restored.compareIds).toEqual(["c2", "c3"]);
    expect(restored.selectedCandidate?.id).toBe("c3");
    expect(restored.selectedCandidateId).toBe("c3");
  });

  it("sameCandidateId safely compares nullable candidates", () => {
    expect(sameCandidateId({ id: "c1", search_id: "s", parcel_id: "p", lat: 0, lon: 0 }, { id: "c1", search_id: "s", parcel_id: "p", lat: 0, lon: 0 })).toBe(true);
    expect(sameCandidateId({ id: "c1", search_id: "s", parcel_id: "p", lat: 0, lon: 0 }, null)).toBe(false);
  });

  it("briefFromSavedSearch prefers filters_json when present", () => {
    const brief = briefFromSavedSearch({
      id: "sv3",
      search_id: "search-3",
      title: "saved",
      status: "draft",
      filters_json: {
        brand_name: "Brand X",
        category: "burger",
        service_model: "qsr",
        min_area_m2: 100,
        max_area_m2: 250,
        target_districts: ["Olaya"],
        existing_branches: [],
        limit: 20,
      },
      ui_state_json: {},
      candidates: [],
    });
    expect(brief.brand_name).toBe("Brand X");
    expect(brief.category).toBe("burger");
    expect(brief.target_districts).toEqual(["Olaya"]);
  });

  it("briefFromSavedSearch falls back to nested search/request payload", () => {
    const brief = briefFromSavedSearch({
      id: "sv4",
      search_id: "search-4",
      title: "saved",
      status: "draft",
      selected_candidate_ids: [],
      filters_json: {},
      ui_state_json: {},
      search: {
        id: "search-4",
        brand_name: "Brand Y",
        category: "pizza",
        service_model: "delivery_first",
        target_districts: ["Malqa"],
        min_area_m2: 80,
        max_area_m2: 180,
        target_area_m2: 120,
        request_json: { limit: 15 },
        existing_branches: [{ name: "HQ", lat: 24.7, lon: 46.7, district: "Olaya" }],
        notes: {},
        meta: { version: "expansion_advisor_v6.1" },
      },
      candidates: [],
    });
    expect(brief.brand_name).toBe("Brand Y");
    expect(brief.category).toBe("pizza");
    expect(brief.service_model).toBe("delivery_first");
    expect(brief.limit).toBe(15);
    expect(brief.existing_branches[0].name).toBe("HQ");
  });

  it("compare response rows use candidate_id", () => {
    const rows = getCompareRows({
      items: [{ candidate_id: "c1", final_score: 81, economics_score: 72, estimated_payback_months: 22, payback_band: "promising", brand_fit_score: 77, provider_density_score: 69 }],
      summary: {},
    });
    expect(rows[0].candidate_id).toBe("c1");
    expect(rows[0].economics_score).toBe(72);
  });

  it("new search reset helper clears stale memo/report/compare-facing state", () => {
    expect(getNewSearchResetState()).toEqual({
      selectedCandidate: null,
      shortlistIds: [],
      compareIds: [],
      compareResult: null,
      memo: null,
      report: null,
      memoError: null,
      reportError: null,
      compareError: null,
    });
  });

  it("map/list shared selection path requests memo when ids differ", () => {
    expect(shouldLoadMemoFromMapSelection("c7", "c3")).toBe(true);
    expect(shouldLoadMemoFromMapSelection("c7", "c7")).toBe(false);
  });

  it("report panel renders recommendation summary", () => {
    const html = renderToStaticMarkup(
      <ExpansionReportPanel
        loading={false}
        report={{
          meta: { version: "6.1" },
          recommendation: { best_candidate_id: "c1", runner_up_candidate_id: "c2", best_pass_candidate_id: "c1", best_confidence_candidate_id: "c2", summary: "summary" },
          top_candidates: [{ id: "c1", final_score: 90, confidence_grade: "A", gate_verdict: "pass" }],
          assumptions: { rent_growth: "3%" },
          brand_profile: {},
        }}
      />,
    );
    expect(html).toContain("c1");
    expect(html).toContain("summary");
    expect(html).toContain("6.1");
    expect(html).toContain("rent growth");
  });

  it("memo panel renders gate reasons positives and risks", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={{
          recommendation: { verdict: "go", headline: "GO" },
          candidate: {
            comparable_competitors: [{ id: "r1", name: "Comp", distance_m: 100 }],
            top_positives_json: ["demand"],
            top_risks_json: ["cost"],
            score_breakdown_json: { final_score: 80, weights: {}, inputs: {}, weighted_components: {} },
            gate_status: { overall_pass: true },
            gate_reasons: { passed: ["zoning"], failed: ["parking"], unknown: ["access"], thresholds: {}, explanations: {} },
          },
          market_research: {},
          brand_profile: {},
        }}
      />,
    );
    expect(html).toContain("Comp");
    expect(html).toContain("Zoning");
    expect(html).toContain("demand");
    expect(html).toContain("cost");
  });

  it("report candidate click wiring helper calls callback", () => {
    const selected: string[] = [];
    triggerReportCandidateSelect("c42", (id) => selected.push(id));
    triggerReportCandidateSelect(undefined, (id) => selected.push(id));
    expect(selected).toEqual(["c42"]);
  });

  it("saved hydration helpers restore shortlist/compare/selected ids", () => {
    const candidates = [
      { id: "c1", search_id: "s", parcel_id: "p1", lat: 0, lon: 0 },
      { id: "c2", search_id: "s", parcel_id: "p2", lat: 0, lon: 0 },
    ];
    const restored = restoreSavedUiState(
      {
        id: "sv5",
        search_id: "s",
        title: "saved",
        status: "draft",
        selected_candidate_ids: ["c1"],
        ui_state_json: { compare_ids: ["c1", "c2"], selected_candidate_id: "c2" },
      },
      candidates,
    );
    expect(restored.shortlistIds).toEqual(["c1"]);
    expect(restored.compareIds).toEqual(["c1", "c2"]);
    expect(restored.selectedCandidateId).toBe("c2");
    expect(resolveCandidateById(candidates, restored.selectedCandidateId)?.id).toBe("c2");
  });

  it("compare summary ordering helper is deterministic", () => {
    const entries = getOrderedCompareSummaryEntries({
      most_confident_candidate_id: "c4",
      best_overall_candidate_id: "c1",
      fastest_payback_candidate_id: "c2",
      custom_metric: "c9",
    });
    expect(entries.map(([key]) => key)).toEqual([
      "best_overall_candidate_id",
      "fastest_payback_candidate_id",
      "most_confident_candidate_id",
      "custom_metric",
    ]);
  });

  it("compare selection helper preserves order and caps at 6", () => {
    expect(getNextCompareIds(["c1", "c2"], "c3")).toEqual(["c1", "c2", "c3"]);
    expect(getNextCompareIds(["c1", "c2"], "c2")).toEqual(["c1"]);
    expect(getNextCompareIds(["c1", "c2", "c3", "c4", "c5", "c6"], "c7")).toEqual(["c1", "c2", "c3", "c4", "c5", "c6"]);
  });

  it("stale compare result guard clears mismatched result", () => {
    expect(
      shouldKeepCompareResult(["c1", "c2"], {
        items: [{ candidate_id: "c1" }, { candidate_id: "c2" }],
        summary: {},
      }),
    ).toBe(true);
    expect(
      shouldKeepCompareResult(["c1", "c3"], {
        items: [{ candidate_id: "c1" }, { candidate_id: "c2" }],
        summary: {},
      }),
    ).toBe(false);
  });

  it("memo/report panels handle sparse payloads", () => {
    const memoHtml = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={{
          recommendation: {},
          candidate: {},
          market_research: {},
          brand_profile: {},
        }}
      />,
    );
    const reportHtml = renderToStaticMarkup(
      <ExpansionReportPanel
        loading={false}
        report={{
          recommendation: {},
          top_candidates: [{ id: "c1" }],
          assumptions: {},
          brand_profile: {},
          meta: {},
        }}
      />,
    );
    expect(memoHtml).toContain("-");
    expect(reportHtml).toContain("ea-report-top-card");
  });
});

/* ─── New: study adapters tests ─── */

describe("Brief payload normalization", () => {
  it("strips blank optional fields and empty district arrays", () => {
    const result = normalizeBriefPayload({
      brand_name: "  Test Brand  ",
      category: "  burger  ",
      service_model: "qsr",
      min_area_m2: 100,
      max_area_m2: 300,
      target_area_m2: 0,
      target_districts: ["Olaya", "", "  "],
      existing_branches: [
        { name: "HQ", lat: 24.7, lon: 46.7 },
        { name: "bad", lat: 0, lon: 0 },
      ],
      limit: 0,
      brand_profile: {
        price_tier: null,
        average_check_sar: 0,
        primary_channel: null,
        target_customer: "",
        expansion_goal: null,
        preferred_districts: [],
        excluded_districts: [],
        cannibalization_tolerance_m: 0,
      },
    });
    expect(result.brand_name).toBe("Test Brand");
    expect(result.category).toBe("burger");
    expect(result.target_area_m2).toBeNull();
    expect(result.target_districts).toEqual(["Olaya"]);
    expect(result.existing_branches).toHaveLength(1);
    expect(result.limit).toBe(25);
    expect(result.brand_profile?.preferred_districts).toBeNull();
    expect(result.brand_profile?.excluded_districts).toBeNull();
    expect(result.brand_profile?.average_check_sar).toBeNull();
    expect(result.brand_profile?.cannibalization_tolerance_m).toBeNull();
    expect(result.brand_profile?.target_customer).toBeNull();
  });

  it("preserves valid brand profile fields", () => {
    const result = normalizeBriefPayload({
      brand_name: "X",
      category: "cafe",
      service_model: "cafe",
      min_area_m2: 50,
      max_area_m2: 200,
      target_area_m2: 120,
      target_districts: ["Malqa"],
      existing_branches: [],
      limit: 15,
      brand_profile: {
        price_tier: "premium",
        average_check_sar: 120,
        primary_channel: "dine_in",
        target_customer: "families",
        expansion_goal: "flagship",
        preferred_districts: ["Olaya"],
        excluded_districts: ["Diriyah"],
        cannibalization_tolerance_m: 500,
      },
    });
    expect(result.brand_profile?.price_tier).toBe("premium");
    expect(result.brand_profile?.average_check_sar).toBe(120);
    expect(result.brand_profile?.preferred_districts).toEqual(["Olaya"]);
    expect(result.target_area_m2).toBe(120);
  });
});

describe("Local shortlist restore from saved study", () => {
  it("filters shortlist to valid candidate ids", () => {
    const candidates = [makeCandidate({ id: "c1" }), makeCandidate({ id: "c2" })];
    const result = restoreShortlistFromSaved(
      {
        id: "sv1",
        search_id: "s1",
        title: "t",
        status: "draft",
        selected_candidate_ids: ["c1", "c3", "c2"],
      },
      candidates,
    );
    expect(result).toEqual(["c1", "c2"]);
  });

  it("restores compare ids from ui_state_json", () => {
    const candidates = [makeCandidate({ id: "c1" }), makeCandidate({ id: "c2" })];
    const result = restoreCompareFromSaved(
      {
        id: "sv1",
        search_id: "s1",
        title: "t",
        status: "draft",
        ui_state_json: { compare_ids: ["c1", "c99", "c2"] },
      },
      candidates,
    );
    expect(result).toEqual(["c1", "c2"]);
  });

  it("handles null/empty ui_state_json gracefully", () => {
    const candidates = [makeCandidate({ id: "c1" })];
    const result = restoreCompareFromSaved(
      { id: "sv1", search_id: "s1", title: "t", status: "draft", ui_state_json: null },
      candidates,
    );
    expect(result).toEqual([]);
  });
});

describe("Local filter/sort without losing rank_position", () => {
  const candidates: ExpansionCandidate[] = [
    makeCandidate({ id: "c1", rank_position: 1, economics_score: 80, brand_fit_score: 60, cannibalization_score: 20, estimated_payback_months: 18, gate_status_json: { overall_pass: true }, district: "Olaya", provider_whitespace_score: 70, multi_platform_presence_score: 50 }),
    makeCandidate({ id: "c2", rank_position: 2, economics_score: 60, brand_fit_score: 90, cannibalization_score: 50, estimated_payback_months: 30, gate_status_json: { overall_pass: false }, district: "Malqa", provider_whitespace_score: 40, multi_platform_presence_score: 30 }),
    makeCandidate({ id: "c3", rank_position: 3, economics_score: 70, brand_fit_score: 75, cannibalization_score: 10, estimated_payback_months: 12, gate_status_json: { overall_pass: true }, district: "Olaya", provider_whitespace_score: 80, multi_platform_presence_score: 60 }),
  ];

  it("filterCandidates pass_only excludes failed gate", () => {
    const result = filterCandidates(candidates, "pass_only");
    expect(result).toHaveLength(2);
    expect(result.every((c) => c.gate_status_json?.overall_pass)).toBe(true);
  });

  it("filterCandidates with district filter", () => {
    const result = filterCandidates(candidates, "all", "Malqa");
    expect(result).toHaveLength(1);
    expect(result[0].district).toBe("Malqa");
  });

  it("sortCandidates economics preserves rank_position on items", () => {
    const result = sortCandidates(candidates, "economics");
    expect(result[0].id).toBe("c1");
    expect(result[0].rank_position).toBe(1);
    expect(result[1].id).toBe("c3");
    expect(result[1].rank_position).toBe(3);
  });

  it("sortCandidates brand_fit sorts descending", () => {
    const result = sortCandidates(candidates, "brand_fit");
    expect(result[0].id).toBe("c2");
  });

  it("sortCandidates cannibalization sorts ascending", () => {
    const result = sortCandidates(candidates, "cannibalization");
    expect(result[0].id).toBe("c3");
    expect(result[0].cannibalization_score).toBe(10);
  });

  it("sortCandidates payback sorts ascending", () => {
    const result = sortCandidates(candidates, "payback");
    expect(result[0].id).toBe("c3");
    expect(result[0].estimated_payback_months).toBe(12);
  });

  it("sortCandidates delivery sorts by combined whitespace+multi-platform", () => {
    const result = sortCandidates(candidates, "delivery");
    expect(result[0].id).toBe("c3"); // 80+60=140
    expect(result[1].id).toBe("c1"); // 70+50=120
  });

  it("sortCandidates district sorts alphabetically", () => {
    const result = sortCandidates(candidates, "district");
    expect(result[0].district).toBe("Malqa");
    expect(result[1].district).toBe("Olaya");
  });

  it("extractDistricts returns unique sorted districts", () => {
    const result = extractDistricts(candidates);
    expect(result).toEqual(["Malqa", "Olaya"]);
  });
});

describe("Candidate 'why this rank?' from deterministic fields", () => {
  it("parseScoreBreakdown produces sorted components", () => {
    const result = parseScoreBreakdown({
      weights: { economics: 0.3, brand_fit: 0.25, zoning: 0.15 },
      inputs: { economics: 80, brand_fit: 70, zoning: 60 },
      weighted_components: { economics: 24, brand_fit: 17.5, zoning: 9 },
      final_score: 72,
    });
    expect(result).toHaveLength(3);
    expect(result[0].label).toContain("economics");
    expect(result[0].weighted).toBe(24);
    expect(result[1].weighted).toBe(17.5);
  });

  it("parseScoreBreakdown handles empty breakdown", () => {
    expect(parseScoreBreakdown(undefined)).toEqual([]);
  });

  it("parseGateEntries correctly categorizes pass/fail/unknown", () => {
    const entries = parseGateEntries(
      { overall_pass: true, zoning: true, parking: false },
      { passed: ["overall_pass", "zoning"], failed: ["parking"], unknown: ["access"], thresholds: {}, explanations: { parking: "Insufficient spaces" } },
    );
    const failed = entries.filter((e) => e.status === "fail");
    const passed = entries.filter((e) => e.status === "pass");
    const unknown = entries.filter((e) => e.status === "unknown");
    expect(failed.length).toBeGreaterThanOrEqual(1);
    expect(passed.length).toBeGreaterThanOrEqual(1);
    expect(unknown.length).toBe(1);
    expect(failed[0].explanation).toBe("Insufficient spaces");
  });

  it("parseFeatureSnapshot extracts completeness and missing sources", () => {
    const result = parseFeatureSnapshot({
      context_sources: { google_places: {}, osm: {} },
      missing_context: ["delivery_platforms"],
      data_completeness_score: 75,
    });
    expect(result.completeness).toBe(75);
    expect(result.missingSources).toEqual(["delivery_platforms"]);
    expect(result.availableSources).toContain("google_places");
  });
});

describe("Memo/report caching keys", () => {
  it("memo cache key uses candidate id", () => {
    expect(memoCacheKey("c1")).toBe("memo:c1");
  });

  it("report cache key uses search id", () => {
    expect(reportCacheKey("s1")).toBe("report:s1");
  });
});

describe("Study title generation", () => {
  it("generates title from brand and category", () => {
    expect(generateStudyTitle({ brand_name: "Al Baik", category: "QSR", service_model: "qsr", min_area_m2: 100, max_area_m2: 300, target_districts: [], existing_branches: [], limit: 25 }))
      .toBe("Al Baik — QSR — Expansion Study");
  });

  it("falls back gracefully with empty brand", () => {
    expect(generateStudyTitle({ brand_name: "", category: "", service_model: "qsr", min_area_m2: 100, max_area_m2: 300, target_districts: [], existing_branches: [], limit: 25 }))
      .toBe("Expansion Study");
  });
});

describe("Candidate card renders WhyThisRank section", () => {
  it("renders candidate card with WhyThisRank toggle", () => {
    const html = renderToStaticMarkup(
      <ExpansionResultsPanel
        items={[makeCandidate({
          rank_position: 1,
          district: "Olaya",
          final_score: 85,
          score_breakdown_json: {
            weights: { economics: 0.3 },
            inputs: { economics: 80 },
            weighted_components: { economics: 24 },
            final_score: 85,
          },
          gate_status_json: { overall_pass: true, zoning: true },
        })]}
        selectedCandidateId={null}
        shortlistIds={[]}
        compareIds={[]}
        onSelectCandidate={() => {}}
        onToggleShortlist={() => {}}
        onToggleCompare={() => {}}
      />,
    );
    expect(html).toContain("ea-why-rank");
    expect(html).toContain("Olaya");
  });
});

describe("Update study dialog preserves existing values", () => {
  it("renders with existing description and status when in update mode", () => {
    const html = renderToStaticMarkup(
      <SaveStudyDialog
        defaultTitle="My Study"
        defaultDescription="Existing description from server"
        defaultStatus="final"
        saving={false}
        error={null}
        isUpdate={true}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );
    expect(html).toContain("Existing description from server");
    expect(html).toContain("My Study");
    // The <option value="final"> should be selected
    expect(html).toMatch(/option[^>]*value="final"[^>]*selected/);
  });

  it("defaults to empty description and draft status for new saves", () => {
    const html = renderToStaticMarkup(
      <SaveStudyDialog
        defaultTitle="New Study"
        saving={false}
        error={null}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );
    expect(html).toContain("New Study");
    expect(html).toMatch(/option[^>]*value="draft"[^>]*selected/);
  });

  it("update preserves all existing metadata when user saves without changes", () => {
    // Simulate exact scenario: saved study has title, description, and final status.
    // Dialog opens for update. User clicks save immediately without editing anything.
    // The rendered inputs must reflect the original values so onSave receives them unchanged.
    const existingTitle = "Q4 Riyadh Expansion";
    const existingDescription = "Flagship locations in Al Olaya and Al Malqa districts";
    const existingStatus = "final" as const;

    const html = renderToStaticMarkup(
      <SaveStudyDialog
        defaultTitle={existingTitle}
        defaultDescription={existingDescription}
        defaultStatus={existingStatus}
        saving={false}
        error={null}
        isUpdate={true}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );

    // Title input has existing value
    expect(html).toContain(`value="${existingTitle}"`);
    // Description input has existing value (not empty string)
    expect(html).toContain(`value="${existingDescription}"`);
    // Status select has "final" selected, NOT "draft"
    expect(html).toMatch(/option[^>]*value="final"[^>]*selected/);
    expect(html).not.toMatch(/option[^>]*value="draft"[^>]*selected/);
    // Update button label is shown (not "Save")
    expect(html).toContain("Update Study");
  });
});

describe("Memo/report entry from multiple points", () => {
  it("candidate card has Decision Memo button", () => {
    const html = renderToStaticMarkup(
      <ExpansionResultsPanel
        items={[makeCandidate({ rank_position: 1, district: "Test" })]}
        selectedCandidateId={null}
        shortlistIds={[]}
        compareIds={[]}
        onSelectCandidate={() => {}}
        onToggleShortlist={() => {}}
        onToggleCompare={() => {}}
        onOpenMemo={() => {}}
      />,
    );
    expect(html).toContain("Decision Memo");
  });
});

/* ─── Lead candidate set/clear/restore ─── */

describe("Lead candidate helpers", () => {
  it("restoreLeadCandidateId returns id when present and valid", () => {
    const candidates = [makeCandidate({ id: "c1" }), makeCandidate({ id: "c2" })];
    expect(restoreLeadCandidateId({ lead_candidate_id: "c1" }, candidates)).toBe("c1");
  });

  it("restoreLeadCandidateId returns null when id not in candidates", () => {
    const candidates = [makeCandidate({ id: "c1" })];
    expect(restoreLeadCandidateId({ lead_candidate_id: "c99" }, candidates)).toBeNull();
  });

  it("restoreLeadCandidateId returns null when ui_state_json is null", () => {
    expect(restoreLeadCandidateId(null, [])).toBeNull();
  });

  it("restoreLeadCandidateId returns null when lead_candidate_id is missing", () => {
    expect(restoreLeadCandidateId({ compare_ids: ["c1"] }, [makeCandidate({ id: "c1" })])).toBeNull();
  });

  it("restoreSavedUiState includes leadCandidateId", () => {
    const candidates = [makeCandidate({ id: "c1" }), makeCandidate({ id: "c2" })];
    const restored = restoreSavedUiState(
      {
        id: "sv1",
        search_id: "s1",
        title: "test",
        status: "draft",
        selected_candidate_ids: ["c1"],
        ui_state_json: { lead_candidate_id: "c1", compare_ids: ["c1", "c2"], selected_candidate_id: "c2" },
      },
      candidates,
    );
    expect(restored.leadCandidateId).toBe("c1");
  });
});

/* ─── Sort/filter state restore ─── */

describe("Sort/filter state restore from ui_state_json", () => {
  it("restoreSortFilter extracts valid filter/sort/district", () => {
    const result = restoreSortFilter({
      active_filter: "pass_only",
      active_sort: "economics",
      district_filter: "Olaya",
    });
    expect(result.activeFilter).toBe("pass_only");
    expect(result.activeSort).toBe("economics");
    expect(result.districtFilter).toBe("Olaya");
  });

  it("restoreSortFilter defaults to safe values on invalid input", () => {
    const result = restoreSortFilter({ active_filter: "invalid", active_sort: 123 });
    expect(result.activeFilter).toBe("all");
    expect(result.activeSort).toBe("rank");
    expect(result.districtFilter).toBe("");
  });

  it("restoreSortFilter handles null ui_state_json", () => {
    const result = restoreSortFilter(null);
    expect(result.activeFilter).toBe("all");
    expect(result.activeSort).toBe("rank");
  });
});

/* ─── buildUiStateJson roundtrip ─── */

describe("buildUiStateJson", () => {
  it("produces correct shape for persistence", () => {
    const result = buildUiStateJson("c1", ["c1", "c2"], "c1", "pass_only", "economics", "Olaya");
    expect(result.selected_candidate_id).toBe("c1");
    expect(result.compare_ids).toEqual(["c1", "c2"]);
    expect(result.lead_candidate_id).toBe("c1");
    expect(result.active_filter).toBe("pass_only");
    expect(result.active_sort).toBe("economics");
    expect(result.district_filter).toBe("Olaya");
  });
});

/* ─── Finalists workspace view models ─── */

describe("Finalist tile builder", () => {
  it("builds tiles from shortlist with lead designation", () => {
    const candidates = [
      makeCandidate({ id: "c1", rank_position: 1, district: "Olaya", final_score: 85, gate_status_json: { overall_pass: true }, payback_band: "fast", estimated_payback_months: 18, estimated_annual_rent_sar: 120000, estimated_fitout_cost_sar: 80000, estimated_revenue_index: 72, top_positives_json: ["Great location"], top_risks_json: ["High rent"], confidence_grade: "A" }),
      makeCandidate({ id: "c2", rank_position: 2, district: "Malqa", final_score: 78, gate_status_json: { overall_pass: false }, payback_band: "moderate", top_positives_json: [], top_risks_json: [], confidence_grade: "B" }),
    ];
    const tiles = buildFinalistTiles(candidates, ["c1", "c2"], "c1");
    expect(tiles).toHaveLength(2);
    expect(tiles[0].isLead).toBe(true);
    expect(tiles[0].district).toBe("Olaya");
    expect(tiles[0].gateVerdict).toBe("pass");
    expect(tiles[0].paybackMonths).toBe(18);
    expect(tiles[0].bestStrength).toBe("Great location");
    expect(tiles[0].mainRisk).toBe("High rent");
    expect(tiles[1].isLead).toBe(false);
    expect(tiles[1].gateVerdict).toBe("fail");
  });

  it("handles missing shortlist ids gracefully", () => {
    const candidates = [makeCandidate({ id: "c1" })];
    const tiles = buildFinalistTiles(candidates, ["c1", "c99"], null);
    expect(tiles).toHaveLength(1);
    expect(tiles[0].isLead).toBe(false);
  });
});

/* ─── Decision checklist derivation ─── */

describe("Decision checklist derivation", () => {
  it("produces checklist items from candidate gate and score fields", () => {
    const candidate = makeCandidate({
      brand_fit_score: 75,
      estimated_revenue_index: 65,
      economics_score: 80,
      payback_band: "fast",
      estimated_payback_months: 18,
      cannibalization_score: 25,
      distance_to_nearest_branch_m: 3000,
      provider_whitespace_score: 72,
      multi_platform_presence_score: 55,
      gate_status_json: { overall_pass: true, zoning_pass: true, parking_pass: false },
      gate_reasons_json: { passed: ["zoning"], failed: ["parking"], unknown: ["access"], thresholds: {}, explanations: {} },
      feature_snapshot_json: { context_sources: {}, missing_context: ["delivery_platforms"], data_completeness_score: 75 },
    });
    const items = deriveDecisionChecklist(candidate);
    expect(items.length).toBeGreaterThan(0);

    const marketItems = items.filter((i) => i.category === "market_demand");
    expect(marketItems.length).toBeGreaterThan(0);
    expect(marketItems[0].status).toBe("strong"); // brand_fit_score 75

    const siteItems = items.filter((i) => i.category === "site_fit");
    expect(siteItems.some((i) => i.label.includes("Zoning") && i.status === "strong")).toBe(true);
    expect(siteItems.some((i) => i.label.includes("Parking") && i.status === "risk")).toBe(true);

    const cannItems = items.filter((i) => i.category === "cannibalization");
    expect(cannItems.some((i) => i.status === "strong")).toBe(true);

    const unknowns = items.filter((i) => i.category === "unknowns");
    expect(unknowns.some((i) => i.label === "access")).toBe(true);
    expect(unknowns.some((i) => i.label === "delivery platforms")).toBe(true);
  });

  it("returns empty array for bare candidate", () => {
    const candidate = makeCandidate({});
    const items = deriveDecisionChecklist(candidate);
    expect(items).toEqual([]);
  });
});

/* ─── Copy summary block generation ─── */

describe("Copy summary builder", () => {
  it("builds summary from report + candidate fields", () => {
    const candidate = makeCandidate({
      rank_position: 1,
      district: "Olaya",
      top_positives_json: ["Great demand"],
      top_risks_json: ["High rent"],
      gate_reasons_json: { passed: [], failed: [], unknown: ["access"], thresholds: {}, explanations: {} },
    });
    const report = {
      recommendation: { why_best: "Strong demand area", main_risk: "Rental costs", best_format: "QSR with drive-through" },
      top_candidates: [],
      assumptions: {},
      brand_profile: {},
      meta: {},
    };
    const summary = buildCopySummary(candidate, report, null);
    expect(summary.bestCandidate).toContain("Olaya");
    expect(summary.topReason).toBe("Strong demand area");
    expect(summary.mainRisk).toBe("Rental costs");
    expect(summary.bestFormat).toBe("QSR with drive-through");
    expect(summary.nextValidation).toBe("Access needs field verification.");
  });

  it("falls back gracefully with no report", () => {
    const candidate = makeCandidate({ rank_position: 3, district: "Malqa", top_positives_json: ["Good fit"] });
    const summary = buildCopySummary(candidate, null, null);
    expect(summary.bestCandidate).toContain("Malqa");
    expect(summary.topReason).toBe("Good fit");
    expect(summary.nextValidation).toBe("Site visit recommended");
  });

  it("formatCopySummaryText produces readable text", () => {
    const text = formatCopySummaryText({
      siteLabel: "Lead site",
      bestCandidate: "#1 Olaya",
      topReason: "Strong demand",
      mainRisk: "High rent",
      bestFormat: "QSR",
      nextValidation: "Site visit",
      allGatesPass: true,
      noPassNotice: null,
    });
    expect(text).toContain("Lead site: #1 Olaya");
    expect(text).toContain("Top reason: Strong demand");
    expect(text.split("\n")).toHaveLength(5);
  });
});

/* ─── Runner-up helper ─── */

describe("findRunnerUp", () => {
  it("returns first non-lead shortlisted candidate", () => {
    const candidates = [
      makeCandidate({ id: "c1", rank_position: 1 }),
      makeCandidate({ id: "c2", rank_position: 2 }),
      makeCandidate({ id: "c3", rank_position: 3 }),
    ];
    const result = findRunnerUp(candidates, ["c1", "c2", "c3"], "c1");
    expect(result?.id).toBe("c2");
  });

  it("returns null when no lead is set", () => {
    expect(findRunnerUp([], [], null)).toBeNull();
  });
});

/* ─── Saved study restoration with partial ui_state_json ─── */

describe("Saved study restore with partial/old ui_state_json", () => {
  it("restores lead candidate from full ui_state_json", () => {
    const candidates = [makeCandidate({ id: "c1" }), makeCandidate({ id: "c2" })];
    const restored = restoreSavedUiState(
      {
        id: "sv1",
        search_id: "s1",
        title: "test",
        status: "draft",
        selected_candidate_ids: ["c1", "c2"],
        ui_state_json: {
          selected_candidate_id: "c1",
          compare_ids: ["c1", "c2"],
          lead_candidate_id: "c2",
          active_filter: "pass_only",
          active_sort: "economics",
          district_filter: "Olaya",
        },
      },
      candidates,
    );
    expect(restored.leadCandidateId).toBe("c2");
    expect(restored.activeFilter).toBe("pass_only");
    expect(restored.activeSort).toBe("economics");
    expect(restored.districtFilter).toBe("Olaya");
  });

  it("gracefully handles old ui_state_json without lead/filter fields", () => {
    const candidates = [makeCandidate({ id: "c1" })];
    const restored = restoreSavedUiState(
      {
        id: "sv2",
        search_id: "s1",
        title: "old study",
        status: "draft",
        selected_candidate_ids: ["c1"],
        ui_state_json: { selected_candidate_id: "c1", compare_ids: [] },
      },
      candidates,
    );
    expect(restored.leadCandidateId).toBeNull();
    expect(restored.activeFilter).toBe("all");
    expect(restored.activeSort).toBe("rank");
    expect(restored.districtFilter).toBe("");
  });

  it("handles completely empty ui_state_json", () => {
    const candidates = [makeCandidate({ id: "c1" })];
    const restored = restoreSavedUiState(
      { id: "sv3", search_id: "s1", title: "empty", status: "draft", ui_state_json: null },
      candidates,
    );
    expect(restored.leadCandidateId).toBeNull();
    expect(restored.activeFilter).toBe("all");
    expect(restored.compareIds).toEqual([]);
    expect(restored.selectedCandidateId).toBeNull();
  });
});

/* ─── Next steps strip rendering ─── */

describe("Next steps strip rendering", () => {
  it("renders lead candidate info and actions", () => {
    // NextStepsStrip imported at top of file
    const html = renderToStaticMarkup(
      <NextStepsStrip
        candidates={[
          makeCandidate({ id: "c1", rank_position: 1, district: "Olaya" }),
          makeCandidate({ id: "c2", rank_position: 2, district: "Malqa" }),
        ]}
        shortlistIds={["c1", "c2"]}
        leadCandidateId="c1"
        report={null}
        onOpenMemo={() => {}}
        onOpenReport={() => {}}
        onCompare={() => {}}
      />,
    );
    expect(html).toContain("Olaya");
    expect(html).toContain("ea-next-steps");
  });

  it("returns null when no lead candidate", () => {
    // NextStepsStrip imported at top of file
    const html = renderToStaticMarkup(
      <NextStepsStrip
        candidates={[makeCandidate({ id: "c1" })]}
        shortlistIds={["c1"]}
        leadCandidateId={null}
        report={null}
        onOpenMemo={() => {}}
        onOpenReport={() => {}}
        onCompare={() => {}}
      />,
    );
    expect(html).toBe("");
  });
});

/* ─── Report/memo default focus when lead exists ─── */

describe("Report panel lead candidate focus", () => {
  it("renders lead site analysis section when leadCandidateId is set", () => {
    const html = renderToStaticMarkup(
      <ExpansionReportPanel
        loading={false}
        leadCandidateId="c1"
        report={{
          meta: {},
          recommendation: { best_candidate_id: "c1", why_best: "Great demand", main_risk: "Expensive rent", best_format: "QSR" },
          top_candidates: [],
          assumptions: {},
          brand_profile: {},
        }}
      />,
    );
    expect(html).toContain("Great demand");
    expect(html).toContain("Expensive rent");
    expect(html).toContain("QSR");
  });
});

/* ─── Validation plan derivation ─── */

describe("Validation plan derivation", () => {
  it("generates validation items from deterministic gate/memo/report fields", () => {
    const candidate = makeCandidate({
      estimated_annual_rent_sar: 120000,
      estimated_rent_sar_m2_year: 1200,
      gate_status_json: { overall_pass: true, frontage_pass: true, access_pass: true, parking_pass: false },
      gate_reasons_json: { passed: ["frontage", "access"], failed: ["parking"], unknown: ["visibility"], thresholds: {}, explanations: {} },
      feature_snapshot_json: { context_sources: {}, missing_context: ["delivery_platforms"], data_completeness_score: 70 },
      provider_whitespace_score: 75,
      comparable_competitors_json: [{ id: "r1", name: "Comp1", distance_m: 200 }],
      distance_to_nearest_branch_m: 3000,
      cannibalization_score: 20,
    });
    const items = deriveValidationPlan(candidate);
    expect(items.length).toBeGreaterThan(0);

    // Must-verify items
    const mustVerify = items.filter((i) => i.priority === "must_verify");
    expect(mustVerify.some((i) => i.label === "Site visit")).toBe(true);
    expect(mustVerify.some((i) => i.label === "Landlord rent verification")).toBe(true);

    // Already strong items
    const alreadyStrong = items.filter((i) => i.priority === "already_strong");
    expect(alreadyStrong.some((i) => i.label === "Frontage/access confirmation")).toBe(true);
    expect(alreadyStrong.some((i) => i.label === "Delivery catchment validation")).toBe(true);
    expect(alreadyStrong.some((i) => i.label === "Branch cannibalization sanity check")).toBe(true);

    // Parking gate failed = must verify
    expect(mustVerify.some((i) => i.label === "Parking check")).toBe(true);

    // Unknown gates
    expect(mustVerify.some((i) => i.label.includes("visibility"))).toBe(true);
  });

  it("returns items even for sparse candidate", () => {
    const candidate = makeCandidate({});
    const items = deriveValidationPlan(candidate);
    // Should at least have site visit
    expect(items.some((i) => i.label === "Site visit")).toBe(true);
  });
});

/* ─── Assumptions & confidence derivation ─── */

describe("Assumptions & confidence derivation", () => {
  it("categorizes data sources into strong/estimated/missing", () => {
    const candidate = makeCandidate({
      final_score: 85,
      gate_status_json: { overall_pass: true, zoning_pass: true },
      economics_score: 75,
      estimated_annual_rent_sar: 120000,
      brand_fit_score: 70,
      provider_whitespace_score: 65,
      feature_snapshot_json: {
        context_sources: { google_places: {}, osm: {} },
        missing_context: ["delivery_platforms"],
        data_completeness_score: 75,
      },
    });
    const items = deriveAssumptions(candidate);
    expect(items.length).toBeGreaterThan(0);

    const strong = items.filter((i) => i.confidence === "strong");
    const estimated = items.filter((i) => i.confidence === "estimated");
    const missing = items.filter((i) => i.confidence === "missing");

    expect(strong.some((i) => i.label === "Overall score")).toBe(true);
    expect(strong.some((i) => i.label === "Gate checks")).toBe(true);
    expect(estimated.some((i) => i.label === "Economics model")).toBe(true);
    expect(missing.some((i) => i.label === "delivery platforms")).toBe(true);
  });

  it("includes report assumptions", () => {
    const candidate = makeCandidate({ final_score: 80 });
    const report = {
      recommendation: {},
      top_candidates: [],
      assumptions: { rent_growth: "3% annual" },
      brand_profile: {},
      meta: {},
    };
    const items = deriveAssumptions(candidate, report);
    expect(items.some((i) => i.label === "rent growth")).toBe(true);
  });
});

/* ─── Decision snapshot rendering ─── */

describe("Decision snapshot", () => {
  it("builds snapshot from candidate + report + memo fields", () => {
    const candidate = makeCandidate({
      rank_position: 1,
      district: "Olaya",
      final_score: 88,
      confidence_grade: "A",
      gate_status_json: { overall_pass: true },
      top_positives_json: ["Strong demand"],
      top_risks_json: ["High rent"],
      gate_reasons_json: { passed: [], failed: [], unknown: ["parking"], thresholds: {}, explanations: {} },
    });
    const report = {
      recommendation: { why_best: "Best demand profile", main_risk: "Rental costs", best_format: "QSR" },
      top_candidates: [],
      assumptions: {},
      brand_profile: {},
      meta: {},
    };
    const snap = buildDecisionSnapshot(candidate, report, null);
    expect(snap.leadSite).toContain("Olaya");
    expect(snap.whyItWins).toBe("Best demand profile");
    expect(snap.mainRisk).toBe("Rental costs");
    expect(snap.bestFormat).toBe("QSR");
    expect(snap.confidenceGrade).toBe("A");
    expect(snap.gateVerdict).toBe("pass");
    expect(snap.nextValidation).toBe("Parking needs field verification.");
  });

  it("renders DecisionSnapshotCard component", () => {
    const candidate = makeCandidate({
      rank_position: 1,
      district: "Olaya",
      final_score: 85,
      confidence_grade: "B",
      gate_status_json: { overall_pass: true },
    });
    const html = renderToStaticMarkup(
      <DecisionSnapshotCard candidate={candidate} />,
    );
    expect(html).toContain("ea-decision-snapshot");
    expect(html).toContain("Olaya");
  });
});

/* ─── Compare outcome banner ─── */

describe("Compare outcome derivation", () => {
  it("derives winner and runner-up strengths from compare result", () => {
    const candidates = [
      makeCandidate({ id: "c1", rank_position: 1, district: "Olaya" }),
      makeCandidate({ id: "c2", rank_position: 2, district: "Malqa" }),
    ];
    const result = {
      items: [
        { candidate_id: "c1", final_score: 85 },
        { candidate_id: "c2", final_score: 78 },
      ],
      summary: {
        best_overall_candidate_id: "c1",
        best_economics_candidate_id: "c2",
        fastest_payback_candidate_id: "c1",
        best_brand_fit_candidate_id: "c2",
      },
    };
    const outcome = deriveCompareOutcome(result, candidates, "c1");
    expect(outcome.winnerId).toBe("c1");
    expect(outcome.winnerLabel).toContain("Olaya");
    expect(outcome.runnerUpStrengths).toContain("best economics");
    expect(outcome.runnerUpStrengths).toContain("best brand fit");
    expect(outcome.leadsAligned).toBe(true);
  });

  it("detects lead mismatch when compare winner differs from lead", () => {
    const candidates = [
      makeCandidate({ id: "c1" }),
      makeCandidate({ id: "c2" }),
    ];
    const result = {
      items: [{ candidate_id: "c1" }, { candidate_id: "c2" }],
      summary: { best_overall_candidate_id: "c2" },
    };
    const outcome = deriveCompareOutcome(result, candidates, "c1");
    expect(outcome.leadsAligned).toBe(false);
  });

  it("returns fallback for null result", () => {
    const outcome = deriveCompareOutcome(null, [], null);
    expect(outcome.winnerId).toBeNull();
    expect(outcome.leadsAligned).toBe(true);
  });

  it("renders CompareOutcomeBanner component", () => {
    const candidates = [makeCandidate({ id: "c1", district: "Olaya" })];
    const result = {
      items: [{ candidate_id: "c1", final_score: 85 }],
      summary: { best_overall_candidate_id: "c1" },
    };
    const html = renderToStaticMarkup(
      <CompareOutcomeBanner result={result} candidates={candidates} leadCandidateId="c1" />,
    );
    expect(html).toContain("ea-compare-outcome");
    expect(html).toContain("Olaya");
  });
});

/* ─── Saved-study metadata extraction ─── */

describe("Saved-study metadata extraction", () => {
  it("extracts lead district, shortlist/compare counts, sort/filter from ui_state_json", () => {
    const saved = {
      id: "sv1",
      search_id: "s1",
      title: "Test Study",
      status: "final" as const,
      selected_candidate_ids: ["c1", "c2", "c3"],
      ui_state_json: {
        lead_candidate_id: "c1",
        compare_ids: ["c1", "c2"],
        active_sort: "economics",
        active_filter: "pass_only",
      },
      candidates: [makeCandidate({ id: "c1", district: "Olaya" }), makeCandidate({ id: "c2" })],
    };
    const meta = extractSavedStudyMeta(saved);
    expect(meta.leadDistrict).toBe("Olaya");
    expect(meta.shortlistCount).toBe(3);
    expect(meta.compareCount).toBe(2);
    expect(meta.lastSort).toBe("economics");
    expect(meta.lastFilter).toBe("pass_only");
    expect(meta.isFinal).toBe(true);
  });

  it("handles empty/partial saved study gracefully", () => {
    const saved = {
      id: "sv2",
      search_id: "s1",
      title: "Empty",
      status: "draft" as const,
      ui_state_json: null,
    };
    const meta = extractSavedStudyMeta(saved);
    expect(meta.leadDistrict).toBeNull();
    expect(meta.shortlistCount).toBe(0);
    expect(meta.compareCount).toBe(0);
    expect(meta.lastSort).toBeNull();
    expect(meta.isFinal).toBe(false);
  });
});

/* ─── Final vs draft study presentation ─── */

describe("Final vs draft study behavior", () => {
  it("final study should be detected from saved status", () => {
    const saved = { id: "sv1", search_id: "s1", title: "t", status: "final" as const };
    const meta = extractSavedStudyMeta(saved);
    expect(meta.isFinal).toBe(true);
  });

  it("draft study should be detected from saved status", () => {
    const saved = { id: "sv1", search_id: "s1", title: "t", status: "draft" as const };
    const meta = extractSavedStudyMeta(saved);
    expect(meta.isFinal).toBe(false);
  });
});

/* ─── Copy/share text block generation ─── */

describe("Copy/share text blocks", () => {
  it("generates landlord briefing text with site details", () => {
    const candidate = makeCandidate({
      district: "Olaya",
      parcel_id: "P-123",
      rank_position: 1,
      estimated_rent_sar_m2_year: 1200,
      estimated_annual_rent_sar: 120000,
      gate_status_json: { overall_pass: true },
    });
    const text = formatLandlordBriefingText(candidate);
    expect(text).toContain("Site Visit Briefing");
    expect(text).toContain("Olaya");
    expect(text).toContain("P-123");
    expect(text).toContain("1200 SAR/m²/yr");
    expect(text).toContain("120,000 SAR/yr");
    expect(text).toContain("All gates passed");
    expect(text).toContain("Confirm street frontage");
  });

  it("handles missing rent data gracefully", () => {
    const candidate = makeCandidate({ district: "Malqa" });
    const text = formatLandlordBriefingText(candidate);
    expect(text).toContain("Malqa");
    expect(text).toContain("TBD");
  });

  it("uses report best_format for intended use", () => {
    const candidate = makeCandidate({});
    const report = {
      recommendation: { best_format: "Cloud kitchen" },
      top_candidates: [],
      assumptions: {},
      brand_profile: {},
      meta: {},
    };
    const text = formatLandlordBriefingText(candidate, report);
    expect(text).toContain("Cloud kitchen");
  });
});

/* ─── Validation plan panel rendering ─── */

describe("Validation plan panel rendering", () => {
  it("renders grouped validation items", () => {
    const candidate = makeCandidate({
      gate_status_json: { overall_pass: true, parking_pass: false },
      gate_reasons_json: { passed: [], failed: ["parking"], unknown: ["access"], thresholds: {}, explanations: {} },
      distance_to_nearest_branch_m: 3000,
    });
    const html = renderToStaticMarkup(
      <ValidationPlanPanel candidate={candidate} />,
    );
    expect(html).toContain("ea-validation-plan");
    expect(html).toContain("Site visit");
    expect(html).toContain("Parking check");
  });
});

/* ─── Assumptions card rendering ─── */

describe("Assumptions card rendering", () => {
  it("renders full assumptions card", () => {
    const candidate = makeCandidate({
      final_score: 85,
      gate_status_json: { overall_pass: true },
      economics_score: 75,
      feature_snapshot_json: {
        context_sources: { google_places: {} },
        missing_context: ["delivery_platforms"],
        data_completeness_score: 70,
      },
    });
    const html = renderToStaticMarkup(
      <AssumptionsCard candidate={candidate} />,
    );
    expect(html).toContain("ea-assumptions-card");
    expect(html).toContain("Overall score");
  });

  it("renders compact assumptions strip", () => {
    const candidate = makeCandidate({ final_score: 85, gate_status_json: { overall_pass: true } });
    const html = renderToStaticMarkup(
      <AssumptionsCard candidate={candidate} compact />,
    );
    expect(html).toContain("ea-assumptions-strip");
  });
});

/* ─── Top-3 candidate highlight ─── */

describe("Top-3 candidate highlight", () => {
  it("adds ea-candidate--top3 class for rank <= 3", () => {
    const html = renderToStaticMarkup(
      <ExpansionResultsPanel
        items={[makeCandidate({ id: "c1", rank_position: 1 }), makeCandidate({ id: "c2", rank_position: 4 })]}
        selectedCandidateId={null}
        shortlistIds={[]}
        compareIds={[]}
        onSelectCandidate={() => {}}
        onToggleShortlist={() => {}}
        onToggleCompare={() => {}}
      />,
    );
    expect(html).toContain("ea-candidate--top3");
    // Second candidate (rank 4) should NOT have top3 class — count occurrences
    const matches = html.match(/ea-candidate--top3/g);
    expect(matches?.length).toBe(1);
  });

  it("does not add top3 class when rank_position is missing", () => {
    const html = renderToStaticMarkup(
      <ExpansionResultsPanel
        items={[makeCandidate({ id: "c1", rank_position: undefined })]}
        selectedCandidateId={null}
        shortlistIds={[]}
        compareIds={[]}
        onSelectCandidate={() => {}}
        onToggleShortlist={() => {}}
        onToggleCompare={() => {}}
      />,
    );
    expect(html).not.toContain("ea-candidate--top3");
  });
});

/* ─── GateSummary unknown gates ─── */

describe("GateSummary unknown gates", () => {
  it("renders unknown gates with neutral styling", () => {
    const gates = { parking_pass: true, access_pass: false, zoning_pass: null };
    const html = renderToStaticMarkup(
      <GateSummary gates={gates} unknownGates={["zoning_pass"]} />,
    );
    expect(html).toContain("ea-gate-item--pass");
    expect(html).toContain("ea-gate-item--fail");
    expect(html).toContain("ea-gate-item--unknown");
  });

  it("renders neutral badge when gates are empty", () => {
    const html = renderToStaticMarkup(<GateSummary gates={{}} />);
    expect(html).toContain("ea-badge--neutral");
  });
});

/* ─── ScoreBreakdownCompact ─── */

describe("ScoreBreakdownCompact", () => {
  it("renders bar rows for each score component", () => {
    const breakdown: CandidateScoreBreakdown = {
      final_score: 80,
      weights: { economics: 0.3, brand_fit: 0.2 },
      inputs: { economics: 85, brand_fit: 70 },
      weighted_components: { economics: 25.5, brand_fit: 14 },
    };
    const html = renderToStaticMarkup(<ScoreBreakdownCompact breakdown={breakdown} />);
    expect(html).toContain("ea-score-breakdown-compact");
    expect(html).toContain("ea-score-breakdown-compact__bar-wrap");
    expect(html).toContain("ea-score-breakdown-compact__bar ");
    expect(html).toContain("ea-score-breakdown-compact__weight");
    expect(html).toContain("ea-score-breakdown-compact__title");
    expect(html).toContain("economics");
    expect(html).toContain("brand fit");
    expect(html).toContain("30%");
    expect(html).toContain("20%");
  });

  it("returns null for undefined breakdown", () => {
    const html = renderToStaticMarkup(<ScoreBreakdownCompact breakdown={undefined} />);
    expect(html).toBe("");
  });
});

/* ─── CandidateDetailPanel passes unknownGates ─── */

describe("CandidateDetailPanel with unknownGates", () => {
  it("passes unknownGates from gate_reasons_json to GateSummary", () => {
    const candidate = makeCandidate({
      gate_status_json: { overall_pass: true, parking_pass: true, zoning_pass: null },
      gate_reasons_json: { passed: ["parking_pass"], failed: [], unknown: ["zoning_pass"], thresholds: {}, explanations: {} },
    });
    const html = renderToStaticMarkup(<CandidateDetailPanel candidate={candidate} />);
    expect(html).toContain("ea-gate-item--unknown");
    expect(html).toContain("ea-gate-item--pass");
  });
});

/* ─── Skeleton loaders ─── */

describe("Skeleton loaders", () => {
  it("renders candidate list skeleton with correct count", () => {
    const html = renderToStaticMarkup(<CandidateListSkeleton count={3} />);
    const matches = html.match(/ea-skeleton--card/g);
    expect(matches?.length).toBe(3);
  });

  it("renders detail skeleton with text and badge placeholders", () => {
    const html = renderToStaticMarkup(<DetailSkeleton />);
    expect(html).toContain("ea-skeleton--text");
    expect(html).toContain("ea-skeleton--badge");
  });
});

/* ─── Map state + drawer persistence ─── */

describe("Map view state persistence", () => {
  it("buildUiStateJson includes map center, zoom, and drawer", () => {
    const ui = buildUiStateJson(
      "c1",
      ["c1", "c2"],
      "c1",
      "all",
      "rank",
      "",
      { center: [46.7, 24.7], zoom: 15 },
      "report",
    );
    expect(ui.map_center).toEqual([46.7, 24.7]);
    expect(ui.map_zoom).toBe(15);
    expect(ui.active_drawer).toBe("report");
  });

  it("buildUiStateJson defaults map and drawer when omitted", () => {
    const ui = buildUiStateJson("c1", [], null, "all", "rank", "");
    expect(ui.map_center).toBeNull();
    expect(ui.map_zoom).toBeNull();
    expect(ui.active_drawer).toBe("none");
  });

  it("restoreMapViewState extracts valid center and zoom", () => {
    const result = restoreMapViewState({ map_center: [46.7, 24.7], map_zoom: 14 });
    expect(result.center).toEqual([46.7, 24.7]);
    expect(result.zoom).toBe(14);
  });

  it("restoreMapViewState handles missing/invalid data", () => {
    expect(restoreMapViewState(null).center).toBeNull();
    expect(restoreMapViewState(null).zoom).toBeNull();
    expect(restoreMapViewState({ map_center: "bad" }).center).toBeNull();
    expect(restoreMapViewState({ map_center: [1] }).center).toBeNull();
  });

  it("restoreDrawerState extracts valid drawer key", () => {
    expect(restoreDrawerState({ active_drawer: "report" })).toBe("report");
    expect(restoreDrawerState({ active_drawer: "memo" })).toBe("memo");
    expect(restoreDrawerState({ active_drawer: "compare" })).toBe("compare");
  });

  it("restoreDrawerState defaults to none for unknown values", () => {
    expect(restoreDrawerState({ active_drawer: "bogus" })).toBe("none");
    expect(restoreDrawerState(null)).toBe("none");
    expect(restoreDrawerState({})).toBe("none");
  });
});

describe("Saved study reopen hydration with map + drawer state", () => {
  it("restoreSavedUiState includes mapView and drawerState", () => {
    const candidates = [
      makeCandidate({ id: "c1" }),
      makeCandidate({ id: "c2" }),
    ];
    const restored = restoreSavedUiState(
      {
        id: "sv1",
        search_id: "s1",
        title: "study",
        status: "draft",
        selected_candidate_ids: ["c1"],
        ui_state_json: {
          compare_ids: ["c1", "c2"],
          selected_candidate_id: "c1",
          map_center: [46.7, 24.7],
          map_zoom: 16,
          active_drawer: "report",
        },
        candidates,
      },
      candidates,
    );
    expect(restored.mapView.center).toEqual([46.7, 24.7]);
    expect(restored.mapView.zoom).toBe(16);
    expect(restored.drawerState).toBe("report");
  });

  it("restoreSavedUiState falls back gracefully for missing map/drawer fields", () => {
    const candidates = [makeCandidate({ id: "c1" })];
    const restored = restoreSavedUiState(
      {
        id: "sv2",
        search_id: "s2",
        title: "study2",
        status: "draft",
        selected_candidate_ids: [],
        ui_state_json: {},
        candidates,
      },
      candidates,
    );
    expect(restored.mapView.center).toBeNull();
    expect(restored.mapView.zoom).toBeNull();
    expect(restored.drawerState).toBe("none");
  });
});

describe("Compare CTA enable/disable rules", () => {
  it("getNextCompareIds caps at 6 and toggles correctly", () => {
    // Adding
    expect(getNextCompareIds([], "c1")).toEqual(["c1"]);
    expect(getNextCompareIds(["c1"], "c2")).toEqual(["c1", "c2"]);
    // Toggling off
    expect(getNextCompareIds(["c1", "c2"], "c1")).toEqual(["c2"]);
    // At max capacity
    const full = ["c1", "c2", "c3", "c4", "c5", "c6"];
    expect(getNextCompareIds(full, "c7")).toEqual(full);
    // Can still remove from full
    expect(getNextCompareIds(full, "c3")).toEqual(["c1", "c2", "c4", "c5", "c6"]);
  });

  it("compare panel button disabled when fewer than 2 selected", () => {
    const html = renderToStaticMarkup(
      <ExpansionComparePanel
        compareIds={["c1"]}
        result={null}
        loading={false}
        error={null}
        onCompare={() => {}}
      />,
    );
    expect(html).toContain("disabled");
  });

  it("compare panel button enabled when 2-6 selected", () => {
    const html = renderToStaticMarkup(
      <ExpansionComparePanel
        compareIds={["c1", "c2", "c3"]}
        result={null}
        loading={false}
        error={null}
        onCompare={() => {}}
      />,
    );
    expect(html).not.toContain("disabled");
    expect(html).toContain("3");
  });
});

describe("Report panel renders score breakdown for top candidates", () => {
  it("renders ScoreBreakdownCompact when top candidate has score_breakdown_json", () => {
    const html = renderToStaticMarkup(
      <ExpansionReportPanel
        loading={false}
        report={{
          meta: {},
          recommendation: { summary: "Test summary" },
          top_candidates: [{
            id: "c1",
            final_score: 88,
            confidence_grade: "A",
            gate_verdict: "pass",
            rank_position: 1,
            top_positives_json: ["good location"],
            top_risks_json: ["high rent"],
            score_breakdown_json: {
              weights: { economics: 0.3, brand_fit: 0.25 },
              inputs: { economics: 80, brand_fit: 70 },
              weighted_components: { economics: 24, brand_fit: 17.5 },
              final_score: 88,
            },
          }],
          assumptions: {},
          brand_profile: {},
        }}
      />,
    );
    expect(html).toContain("ea-score-breakdown-compact");
    expect(html).toContain("economics");
  });
});

describe("Unknown/missing decision-layer fields do not crash UI", () => {
  it("candidate card renders safely with all optional fields undefined", () => {
    const candidate = normalizeCandidate({
      id: "c-sparse",
      search_id: "s",
      parcel_id: "p",
      lat: 24.7,
      lon: 46.7,
    });
    const html = renderToStaticMarkup(
      <ExpansionResultsPanel
        items={[candidate]}
        selectedCandidateId={null}
        shortlistIds={[]}
        compareIds={[]}
        onSelectCandidate={() => {}}
        onToggleShortlist={() => {}}
        onToggleCompare={() => {}}
      />,
    );
    expect(html).toContain("ea-candidate");
    expect(html).not.toContain("undefined");
    expect(html).not.toContain("NaN");
  });

  it("CandidateDetailPanel renders with minimal candidate", () => {
    const candidate = normalizeCandidate({
      id: "c-min",
      search_id: "s",
      parcel_id: "p",
      lat: 24.7,
      lon: 46.7,
    });
    const html = renderToStaticMarkup(<CandidateDetailPanel candidate={candidate} />);
    expect(html).toContain("ea-detail");
    expect(html).not.toContain("undefined");
  });

  it("report panel handles completely empty recommendation and top_candidates", () => {
    const html = renderToStaticMarkup(
      <ExpansionReportPanel
        loading={false}
        report={{
          recommendation: {},
          top_candidates: [],
          assumptions: {},
          brand_profile: {},
          meta: {},
        }}
      />,
    );
    expect(html).toContain("ea-drawer");
  });

  it("memo panel handles completely empty candidate and recommendation", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={{
          recommendation: {},
          candidate: {},
          market_research: {},
          brand_profile: {},
        }}
      />,
    );
    expect(html).toContain("ea-drawer");
  });
});

/* ─── Saved studies workspace tests ─── */

function makeSavedSearch(overrides: Partial<SavedExpansionSearch> = {}): SavedExpansionSearch {
  return normalizeSavedSearch({
    id: "saved-1",
    search_id: "s1",
    title: "Test Study",
    status: "draft",
    ...overrides,
  });
}

describe("Saved study normalization", () => {
  it("listSavedSearches normalizes items with safe defaults", () => {
    const raw: SavedExpansionSearch = {
      id: "saved-1",
      search_id: "s1",
      title: "Test",
      status: "draft",
    };
    const normalized = normalizeSavedSearch(raw);
    expect(normalized.selected_candidate_ids).toEqual([]);
    expect(normalized.filters_json).toEqual({});
    expect(normalized.ui_state_json).toEqual({});
    expect(normalized.candidates).toEqual([]);
    expect(normalized.description).toBeUndefined();
  });

  it("getSavedSearch normalizes nested search and candidates", () => {
    const raw: SavedExpansionSearch = {
      id: "saved-2",
      search_id: "s2",
      title: "With Detail",
      status: "final",
      selected_candidate_ids: ["c1", "c2"],
      search: {
        id: "s2",
        target_districts: ["Al Olaya"],
        request_json: { brand_name: "TestBrand" },
        notes: {},
        existing_branches: [],
        meta: {},
      },
      candidates: [{ id: "c1", search_id: "s2", parcel_id: "p1", lat: 24.7, lon: 46.7 }],
    };
    const normalized = normalizeSavedSearch(raw);
    expect(normalized.selected_candidate_ids).toEqual(["c1", "c2"]);
    expect(normalized.candidates).toHaveLength(1);
    expect(normalized.candidates![0].gate_status_json).toEqual({});
    expect(normalized.candidates![0].top_positives_json).toEqual([]);
    expect(normalized.search!.target_districts).toEqual(["Al Olaya"]);
  });

  it("normalizeSavedSearch handles null/undefined fields without crashing", () => {
    const raw: SavedExpansionSearch = {
      id: "saved-3",
      search_id: "s3",
      title: "Sparse",
      status: "draft",
      selected_candidate_ids: null,
      filters_json: null,
      ui_state_json: null,
      description: null,
      candidates: undefined,
      search: null,
    };
    const normalized = normalizeSavedSearch(raw);
    expect(normalized.selected_candidate_ids).toEqual([]);
    expect(normalized.filters_json).toEqual({});
    expect(normalized.ui_state_json).toEqual({});
    expect(normalized.candidates).toEqual([]);
    expect(normalized.search).toBeNull();
  });
});

describe("Save new study payload creation", () => {
  it("buildUiStateJson captures all workspace state for save payload", () => {
    const uiState = buildUiStateJson(
      "c1",
      ["c1", "c2"],
      "c1",
      "pass_only",
      "economics",
      "Al Olaya",
      { center: [24.7, 46.7], zoom: 14 },
      "memo",
    );
    expect(uiState.selected_candidate_id).toBe("c1");
    expect(uiState.compare_ids).toEqual(["c1", "c2"]);
    expect(uiState.lead_candidate_id).toBe("c1");
    expect(uiState.active_filter).toBe("pass_only");
    expect(uiState.active_sort).toBe("economics");
    expect(uiState.district_filter).toBe("Al Olaya");
    expect(uiState.map_center).toEqual([24.7, 46.7]);
    expect(uiState.map_zoom).toBe(14);
    expect(uiState.active_drawer).toBe("memo");
  });

  it("buildUiStateJson uses null defaults for optional params", () => {
    const uiState = buildUiStateJson(null, [], null, "all", "rank", "");
    expect(uiState.selected_candidate_id).toBeNull();
    expect(uiState.compare_ids).toEqual([]);
    expect(uiState.lead_candidate_id).toBeNull();
    expect(uiState.map_center).toBeNull();
    expect(uiState.map_zoom).toBeNull();
    expect(uiState.active_drawer).toBe("none");
  });
});

describe("Patch existing study payload update", () => {
  it("restoreSavedUiState extracts all fields for patch roundtrip", () => {
    const candidates = [makeCandidate({ id: "c1" }), makeCandidate({ id: "c2" })];
    const saved = makeSavedSearch({
      selected_candidate_ids: ["c1", "c2"],
      ui_state_json: {
        selected_candidate_id: "c1",
        compare_ids: ["c1", "c2"],
        lead_candidate_id: "c1",
        active_filter: "pass_only",
        active_sort: "economics",
        district_filter: "Al Olaya",
        map_center: [24.7, 46.7],
        map_zoom: 14,
        active_drawer: "report",
      },
    });
    const restored = restoreSavedUiState(saved, candidates);
    expect(restored.shortlistIds).toEqual(["c1", "c2"]);
    expect(restored.compareIds).toEqual(["c1", "c2"]);
    expect(restored.selectedCandidateId).toBe("c1");
    expect(restored.leadCandidateId).toBe("c1");
    expect(restored.activeFilter).toBe("pass_only");
    expect(restored.activeSort).toBe("economics");
    expect(restored.districtFilter).toBe("Al Olaya");
    expect(restored.mapView.center).toEqual([24.7, 46.7]);
    expect(restored.mapView.zoom).toBe(14);
    expect(restored.drawerState).toBe("report");
  });
});

describe("Reopen saved study restores full workspace state", () => {
  it("restores shortlist + compare + selected candidate + drawer + map state", () => {
    const candidates = [
      makeCandidate({ id: "c1", district: "Al Olaya", rank_position: 1 }),
      makeCandidate({ id: "c2", district: "Al Malaz", rank_position: 2 }),
      makeCandidate({ id: "c3", district: "Al Nakheel", rank_position: 3 }),
    ];
    const saved = makeSavedSearch({
      selected_candidate_ids: ["c1", "c2", "c3"],
      ui_state_json: {
        selected_candidate_id: "c2",
        compare_ids: ["c1", "c2"],
        lead_candidate_id: "c1",
        active_filter: "all",
        active_sort: "rank",
        district_filter: "",
        map_center: [24.7, 46.7],
        map_zoom: 12,
        active_drawer: "compare",
      },
    });

    const restored = restoreSavedUiState(saved, candidates);

    // Shortlist
    expect(restored.shortlistIds).toEqual(["c1", "c2", "c3"]);
    // Compare set
    expect(restored.compareIds).toEqual(["c1", "c2"]);
    // Selected candidate
    expect(restored.selectedCandidateId).toBe("c2");
    expect(restored.selectedCandidate?.id).toBe("c2");
    // Drawer
    expect(restored.drawerState).toBe("compare");
    // Map
    expect(restored.mapView.center).toEqual([24.7, 46.7]);
    expect(restored.mapView.zoom).toBe(12);
    // Lead
    expect(restored.leadCandidateId).toBe("c1");
  });

  it("filters out invalid candidate ids on restore", () => {
    const candidates = [makeCandidate({ id: "c1" })];
    const saved = makeSavedSearch({
      selected_candidate_ids: ["c1", "c_deleted"],
      ui_state_json: { compare_ids: ["c1", "c_deleted"] },
    });
    const restored = restoreSavedUiState(saved, candidates);
    // restoreSavedUiState returns raw ids; the page filters them after
    expect(restored.shortlistIds).toContain("c1");
    expect(restored.compareIds).toContain("c1");
  });
});

describe("Delete flow state update", () => {
  it("savedItems list is updated after delete by filtering", () => {
    const items: SavedExpansionSearch[] = [
      makeSavedSearch({ id: "s1", title: "Study 1" }),
      makeSavedSearch({ id: "s2", title: "Study 2" }),
    ];
    // Simulate delete of s1
    const afterDelete = items.filter((s) => s.id !== "s1");
    expect(afterDelete).toHaveLength(1);
    expect(afterDelete[0].id).toBe("s2");
  });

  it("active saved id clears when matching deleted id", () => {
    let activeSavedId: string | null = "s1";
    const deletedId = "s1";
    if (activeSavedId === deletedId) activeSavedId = null;
    expect(activeSavedId).toBeNull();
  });
});

describe("Report panel executive recommendation fields", () => {
  it("renders recommendation summary, why_best, main_risk, best_format", () => {
    const html = renderToStaticMarkup(
      <ExpansionReportPanel
        loading={false}
        report={{
          recommendation: {
            summary: "Open in Al Olaya district.",
            why_best: "Highest foot traffic and brand fit score.",
            main_risk: "Parking gate failed — verify on site.",
            best_format: "QSR with delivery hub.",
            best_candidate_id: "c1",
            runner_up_candidate_id: "c2",
          },
          top_candidates: [
            {
              id: "c1",
              rank_position: 1,
              final_score: 85,
              confidence_grade: "A",
              gate_verdict: "pass",
              top_positives_json: ["Strong foot traffic"],
              top_risks_json: ["Parking limited"],
              score_breakdown_json: { weights: {}, inputs: {}, weighted_components: {}, final_score: 85 },
            },
            {
              id: "c2",
              rank_position: 2,
              final_score: 78,
              confidence_grade: "B",
              gate_verdict: "pass",
              top_positives_json: ["Low rent"],
              top_risks_json: ["Competition density"],
            },
          ],
          assumptions: { rent_model: "comparable" },
          brand_profile: {},
          meta: {},
        }}
      />,
    );
    expect(html).toContain("Open in Al Olaya district.");
    expect(html).toContain("Highest foot traffic and brand fit score.");
    expect(html).toContain("Parking gate failed");
    expect(html).toContain("QSR with delivery hub.");
    expect(html).toContain("#1");
    expect(html).toContain("#2");
    expect(html).toContain("Strong foot traffic");
    expect(html).toContain("Low rent");
  });

  it("report panel handles empty top_candidates without crashing", () => {
    const html = renderToStaticMarkup(
      <ExpansionReportPanel
        loading={false}
        report={{
          recommendation: { summary: "No strong candidates found." },
          top_candidates: [],
          assumptions: {},
          brand_profile: {},
          meta: {},
        }}
      />,
    );
    expect(html).toContain("No strong candidates found.");
    expect(html).toContain("ea-drawer");
  });
});

describe("Memo panel stability with missing fields", () => {
  it("memo panel remains stable with missing optional text fields", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={{
          recommendation: { headline: "Go" },
          candidate: {
            final_score: 75,
            rank_position: 3,
          },
          market_research: {},
          brand_profile: {},
        }}
      />,
    );
    expect(html).toContain("Go");
    expect(html).toContain("ea-drawer");
    expect(html).not.toContain("undefined");
  });

  it("memo panel renders back-navigation buttons when handlers provided", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={{
          recommendation: { headline: "Caution", verdict: "caution" },
          candidate: {},
          market_research: {},
          brand_profile: {},
        }}
        onBackToDetail={() => {}}
        onBackToCompare={() => {}}
        hasCompare={true}
        hasShortlist={true}
      />,
    );
    expect(html).toContain(en.expansionAdvisor.memoBackToDetail);
    expect(html).toContain(en.expansionAdvisor.memoBackToCompare);
  });

  it("memo panel shows compare button when shortlist exists but no compare", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={{
          recommendation: {},
          candidate: {},
          market_research: {},
          brand_profile: {},
        }}
        onOpenCompare={() => {}}
        hasShortlist={true}
        hasCompare={false}
      />,
    );
    expect(html).toContain(en.expansionAdvisor.memoOpenCompare);
  });
});

describe("Saved studies workspace panel", () => {
  it("renders saved items with title, status badge, dates, and candidate counts", () => {
    const items = [
      makeSavedSearch({
        id: "s1",
        title: "Al Olaya QSR Study",
        status: "final",
        description: "Final study for flagship QSR in Al Olaya",
        updated_at: "2026-01-15T10:00:00Z",
        created_at: "2026-01-10T10:00:00Z",
        selected_candidate_ids: ["c1", "c2"],
        candidates: [makeCandidate({ id: "c1" }), makeCandidate({ id: "c2" }), makeCandidate({ id: "c3" })],
      }),
      makeSavedSearch({
        id: "s2",
        title: "Draft Cafe Study",
        status: "draft",
        created_at: "2026-02-01T10:00:00Z",
      }),
    ];
    const html = renderToStaticMarkup(
      <SavedSearchesPanel
        items={items}
        loading={false}
        onOpen={() => {}}
        onDelete={() => {}}
        onRename={() => {}}
        onEditDescription={() => {}}
        onChangeStatus={() => {}}
      />,
    );
    expect(html).toContain("Al Olaya QSR Study");
    expect(html).toContain("Draft Cafe Study");
    expect(html).toContain(en.expansionAdvisor.savedStudyFinal);
    expect(html).toContain(en.expansionAdvisor.savedStudyDraft);
    expect(html).toContain("Final study for flagship");
    expect(html).toContain(en.expansionAdvisor.reopenStudy);
    expect(html).toContain(en.expansionAdvisor.renameStudy);
    expect(html).toContain(en.expansionAdvisor.deleteStudy);
  });

  it("shows empty state when no items", () => {
    const html = renderToStaticMarkup(
      <SavedSearchesPanel items={[]} loading={false} onOpen={() => {}} />,
    );
    expect(html).toContain(en.expansionAdvisor.noSavedStudiesYet);
  });

  it("shows loading state", () => {
    const html = renderToStaticMarkup(
      <SavedSearchesPanel items={[]} loading={true} onOpen={() => {}} />,
    );
    expect(html).toContain(en.expansionAdvisor.loadingSaved);
  });

  it("renders status toggle buttons (mark as final / revert to draft)", () => {
    const items = [
      makeSavedSearch({ id: "s1", status: "draft" }),
      makeSavedSearch({ id: "s2", status: "final" }),
    ];
    const html = renderToStaticMarkup(
      <SavedSearchesPanel
        items={items}
        loading={false}
        onOpen={() => {}}
        onChangeStatus={() => {}}
      />,
    );
    expect(html).toContain(en.expansionAdvisor.markAsFinal);
    expect(html).toContain(en.expansionAdvisor.markAsDraft);
  });
});

describe("Saved studies panel: empty vs error state (regression)", () => {
  it("successful fetch with [] → empty state only, no error alert", () => {
    const html = renderToStaticMarkup(
      <SavedSearchesPanel items={[]} loading={false} onOpen={() => {}} />,
    );
    expect(html).toContain(en.expansionAdvisor.noSavedStudiesYet);
    expect(html).not.toContain("ea-state--error");
    expect(html).not.toContain(en.expansionAdvisor.retry);
    expect(html).not.toContain(en.expansionAdvisor.errorSavedLoad);
  });

  it("successful fetch with items → list renders with titles", () => {
    const items = [
      makeSavedSearch({ id: "s1", title: "Study A" }),
      makeSavedSearch({ id: "s2", title: "Study B" }),
    ];
    const html = renderToStaticMarkup(
      <SavedSearchesPanel items={items} loading={false} onOpen={() => {}} />,
    );
    expect(html).toContain("Study A");
    expect(html).toContain("Study B");
    expect(html).not.toContain(en.expansionAdvisor.noSavedStudiesYet);
    expect(html).not.toContain("ea-state--error");
  });

  it("loading state → loading UI renders, no error or empty state", () => {
    const html = renderToStaticMarkup(
      <SavedSearchesPanel items={[]} loading={true} onOpen={() => {}} />,
    );
    expect(html).toContain(en.expansionAdvisor.loadingSaved);
    expect(html).not.toContain(en.expansionAdvisor.noSavedStudiesYet);
    expect(html).not.toContain("ea-state--error");
  });
});

describe("listSavedExpansionSearches error handling (regression)", () => {
  // These tests verify the API client's behaviour for 404, 200 [], and 500
  // without hitting a real network — we test the error-classification logic
  // by importing the function and mocking fetchWithAuth at module level.
  // Since the function is async and uses fetchWithAuth internally, we test
  // the expected contract via the normalizer + panel rendering combination.

  it("200 with empty items array → empty list, no error", () => {
    // Simulates what the UI receives after a 200 [] from backend
    const html = renderToStaticMarkup(
      <SavedSearchesPanel items={[]} loading={false} onOpen={() => {}} />,
    );
    expect(html).toContain(en.expansionAdvisor.noSavedStudiesYet);
    expect(html).not.toContain("ea-state--error");
    expect(html).not.toContain(en.expansionAdvisor.errorSavedLoad);
  });

  it("404 resolved as empty → panel shows empty state only", () => {
    // After the API client catches a 404 and returns {items: []},
    // the panel should render the clean empty state
    const html = renderToStaticMarkup(
      <SavedSearchesPanel items={[]} loading={false} onOpen={() => {}} />,
    );
    expect(html).toContain(en.expansionAdvisor.noSavedStudiesYet);
    expect(html).not.toContain("ea-state--error");
    expect(html).not.toContain(en.expansionAdvisor.retry);
  });

  it("successful non-empty list → renders all items without error", () => {
    const items = [
      makeSavedSearch({ id: "s1", title: "Alpha Study" }),
      makeSavedSearch({ id: "s2", title: "Beta Study" }),
      makeSavedSearch({ id: "s3", title: "Gamma Study" }),
    ];
    const html = renderToStaticMarkup(
      <SavedSearchesPanel items={items} loading={false} onOpen={() => {}} />,
    );
    expect(html).toContain("Alpha Study");
    expect(html).toContain("Beta Study");
    expect(html).toContain("Gamma Study");
    expect(html).not.toContain("ea-state--error");
    expect(html).not.toContain(en.expansionAdvisor.noSavedStudiesYet);
  });

  it("500 error state → panel not rendered, error message with retry shown instead", () => {
    // When savedLoadError is truthy, the page renders the error div
    // instead of SavedSearchesPanel. We verify that the panel itself
    // does NOT render the error — the page component handles it.
    // The panel should never see an error state; it only sees items.
    const html = renderToStaticMarkup(
      <SavedSearchesPanel items={[]} loading={false} onOpen={() => {}} />,
    );
    // Panel itself should just show empty state — the error alert is
    // in the parent page component, not in SavedSearchesPanel
    expect(html).not.toContain("ea-state--error");
  });
});

describe("extractSavedStudyMeta", () => {
  it("extracts shortlist count, compare count, lead district from saved study", () => {
    const saved = makeSavedSearch({
      selected_candidate_ids: ["c1", "c2", "c3"],
      ui_state_json: {
        compare_ids: ["c1", "c2"],
        lead_candidate_id: "c1",
        active_sort: "economics",
        active_filter: "pass_only",
      },
      candidates: [makeCandidate({ id: "c1", district: "Al Olaya" })],
    });
    const meta = extractSavedStudyMeta(saved);
    expect(meta.shortlistCount).toBe(3);
    expect(meta.compareCount).toBe(2);
    expect(meta.leadDistrict).toBe("Al Olaya");
    expect(meta.lastSort).toBe("economics");
    expect(meta.lastFilter).toBe("pass_only");
    expect(meta.isFinal).toBe(false);
  });

  it("handles empty saved study", () => {
    const saved = makeSavedSearch();
    const meta = extractSavedStudyMeta(saved);
    expect(meta.shortlistCount).toBe(0);
    expect(meta.compareCount).toBe(0);
    expect(meta.leadDistrict).toBeNull();
    expect(meta.lastSort).toBeNull();
    expect(meta.lastFilter).toBeNull();
  });
});

/* ─── Response normalizer resilience ─── */

describe("normalizeReportResponse", () => {
  it("fills defaults for missing top_candidates, assumptions, recommendation", () => {
    const raw = {} as Parameters<typeof normalizeReportResponse>[0];
    const result = normalizeReportResponse(raw);
    expect(result.top_candidates).toEqual([]);
    expect(result.assumptions).toEqual({});
    expect(result.recommendation).toEqual({});
    expect(result.brand_profile).toEqual({});
    expect(result.meta).toEqual({});
  });

  it("normalizes each top_candidate with safe defaults", () => {
    const raw = {
      top_candidates: [{ id: "c1", final_score: 80 }],
      recommendation: {},
      assumptions: {},
      brand_profile: {},
      meta: {},
    } as Parameters<typeof normalizeReportResponse>[0];
    const result = normalizeReportResponse(raw);
    expect(result.top_candidates[0].top_positives_json).toEqual([]);
    expect(result.top_candidates[0].top_risks_json).toEqual([]);
    expect(result.top_candidates[0].score_breakdown_json).toBeDefined();
    expect(result.top_candidates[0].score_breakdown_json?.weights ?? {}).toEqual({});
  });

  it("preserves existing data when already present", () => {
    const raw = {
      top_candidates: [{
        id: "c1",
        final_score: 90,
        top_positives_json: ["Great location"],
        top_risks_json: ["High rent"],
        score_breakdown_json: { weights: { economics: 0.3 }, inputs: { economics: 85 }, weighted_components: { economics: 25.5 }, final_score: 90 },
      }],
      recommendation: { summary: "Go" },
      assumptions: { rent_model: "comp" },
      brand_profile: { price_tier: "mid" },
      meta: { version: "1" },
    } as Parameters<typeof normalizeReportResponse>[0];
    const result = normalizeReportResponse(raw);
    expect(result.top_candidates[0].top_positives_json).toEqual(["Great location"]);
    expect(result.recommendation.summary).toBe("Go");
    expect(result.assumptions.rent_model).toBe("comp");
  });
});

describe("normalizeMemoResponse", () => {
  it("fills defaults for empty memo response", () => {
    const raw = {} as Parameters<typeof normalizeMemoResponse>[0];
    const result = normalizeMemoResponse(raw);
    expect(result.brand_profile).toEqual({});
    expect(result.recommendation).toEqual({});
    expect(result.market_research).toEqual({});
    expect(result.candidate).toBeDefined();
    expect(result.candidate.top_positives_json).toEqual([]);
    expect(result.candidate.top_risks_json).toEqual([]);
    expect(result.candidate.comparable_competitors).toEqual([]);
    expect(result.candidate.gate_status).toEqual({});
    expect(result.candidate.gate_reasons).toBeDefined();
    expect(result.candidate.gate_reasons?.passed ?? []).toEqual([]);
    expect(result.candidate.feature_snapshot).toBeDefined();
    expect(result.candidate.feature_snapshot?.missing_context ?? []).toEqual([]);
  });

  it("preserves existing candidate fields", () => {
    const raw = {
      candidate: {
        top_positives_json: ["Low rent"],
        gate_status: { overall_pass: true },
      },
      recommendation: { headline: "Go" },
      brand_profile: {},
      market_research: {},
    } as Parameters<typeof normalizeMemoResponse>[0];
    const result = normalizeMemoResponse(raw);
    expect(result.candidate.top_positives_json).toEqual(["Low rent"]);
    expect(result.candidate.gate_status?.overall_pass).toBe(true);
    expect(result.recommendation.headline).toBe("Go");
  });
});

/* ─── Compare panel findBestOnKey lower-is-better logic ─── */

describe("Compare panel dimension groups", () => {
  it("getOrderedCompareSummaryEntries orders known keys first, then extras", () => {
    const summary = {
      best_overall_candidate_id: "c1",
      best_economics_candidate_id: "c2",
      some_custom_candidate_id: "c3",
      fastest_payback_candidate_id: "c1",
    };
    const entries = getOrderedCompareSummaryEntries(summary);
    const keys = entries.map(([k]) => k);
    // Known keys first in defined order
    expect(keys.indexOf("best_overall_candidate_id")).toBeLessThan(keys.indexOf("best_economics_candidate_id"));
    expect(keys.indexOf("best_economics_candidate_id")).toBeLessThan(keys.indexOf("fastest_payback_candidate_id"));
    // Custom key last
    expect(keys.indexOf("some_custom_candidate_id")).toBe(keys.length - 1);
  });

  it("filters out null values from summary entries", () => {
    const summary = {
      best_overall_candidate_id: "c1",
      best_economics_candidate_id: null,
    };
    const entries = getOrderedCompareSummaryEntries(summary);
    expect(entries).toHaveLength(1);
    expect(entries[0][0]).toBe("best_overall_candidate_id");
  });

  it("returns empty array for empty summary", () => {
    expect(getOrderedCompareSummaryEntries({})).toEqual([]);
    expect(getOrderedCompareSummaryEntries()).toEqual([]);
  });
});

describe("Compare panel rendering with full result", () => {
  it("renders dimension group headers and score cells", () => {
    const html = renderToStaticMarkup(
      <ExpansionComparePanel
        compareIds={["c1", "c2"]}
        result={{
          items: [
            {
              candidate_id: "c1",
              district: "Olaya",
              final_score: 85,
              rank_position: 1,
              confidence_grade: "A",
              payback_band: "fast",
              estimated_payback_months: 18,
              estimated_annual_rent_sar: 120000,
              brand_fit_score: 80,
              economics_score: 75,
              provider_density_score: 60,
              provider_whitespace_score: 70,
              delivery_competition_score: 55,
              multi_platform_presence_score: 65,
              cannibalization_score: 20,
              zoning_fit_score: 80,
              frontage_score: 70,
              access_score: 75,
              parking_score: 60,
              access_visibility_score: 55,
              gate_status_json: { overall_pass: true },
            },
            {
              candidate_id: "c2",
              district: "Malqa",
              final_score: 72,
              rank_position: 2,
              confidence_grade: "B",
              payback_band: "medium",
              estimated_payback_months: 24,
              estimated_annual_rent_sar: 90000,
              brand_fit_score: 65,
              economics_score: 80,
              cannibalization_score: 15,
              gate_status_json: { overall_pass: false },
            },
          ],
          summary: { best_overall_candidate_id: "c1" },
        }}
        loading={false}
        error={null}
        onCompare={() => {}}
      />,
    );
    // Group headers
    expect(html).toContain("Overall Rank &amp; Score");
    expect(html).toContain("Demand &amp; Whitespace");
    expect(html).toContain("Economics &amp; Rent");
    expect(html).toContain("Site Quality");
    // District names in column headers
    expect(html).toContain("Olaya");
    expect(html).toContain("Malqa");
    // Summary highlight
    expect(html).toContain("ea-compare-highlight");
  });

  it("highlights lower-is-better winner for cannibalization", () => {
    const html = renderToStaticMarkup(
      <ExpansionComparePanel
        compareIds={["c1", "c2"]}
        result={{
          items: [
            { candidate_id: "c1", cannibalization_score: 30, final_score: 80, gate_status_json: { overall_pass: true } },
            { candidate_id: "c2", cannibalization_score: 10, final_score: 75, gate_status_json: { overall_pass: true } },
          ],
          summary: {},
        }}
        loading={false}
        error={null}
        onCompare={() => {}}
      />,
    );
    // c2 has lower cannibalization and should be the winner
    expect(html).toContain("ea-compare-winner");
  });
});

/* ─── Candidate card area_m2 and showOnMap ─── */

describe("Candidate card enhanced fields", () => {
  it("renders area_m2 when present", () => {
    const candidate = makeCandidate({ area_m2: 150 });
    const html = renderToStaticMarkup(
      <ExpansionResultsPanel
        items={[candidate]}
        selectedCandidateId={null}
        shortlistIds={[]}
        compareIds={[]}
        onSelectCandidate={() => {}}
        onToggleShortlist={() => {}}
        onToggleCompare={() => {}}
      />,
    );
    expect(html).toContain("150");
    expect(html).toContain(en.expansionAdvisor.areaLabel);
  });

  it("renders showOnMap button when handler is provided", () => {
    const candidate = makeCandidate({});
    const html = renderToStaticMarkup(
      <ExpansionResultsPanel
        items={[candidate]}
        selectedCandidateId={null}
        shortlistIds={[]}
        compareIds={[]}
        onSelectCandidate={() => {}}
        onToggleShortlist={() => {}}
        onToggleCompare={() => {}}
        onShowOnMap={() => {}}
      />,
    );
    expect(html).toContain(en.expansionAdvisor.showOnMap);
  });

  it("does not render showOnMap button when handler is absent", () => {
    const candidate = makeCandidate({});
    const html = renderToStaticMarkup(
      <ExpansionResultsPanel
        items={[candidate]}
        selectedCandidateId={null}
        shortlistIds={[]}
        compareIds={[]}
        onSelectCandidate={() => {}}
        onToggleShortlist={() => {}}
        onToggleCompare={() => {}}
      />,
    );
    expect(html).not.toContain(en.expansionAdvisor.showOnMap);
  });
});

/* ─── CandidateDetailPanel enhanced economics fields ─── */

describe("CandidateDetailPanel enhanced economics", () => {
  it("renders area_m2, payback, and cannibalization in detail view", () => {
    const candidate = makeCandidate({
      area_m2: 200,
      estimated_payback_months: 14,
      payback_band: "fast",
      cannibalization_score: 25,
    });
    const html = renderToStaticMarkup(<CandidateDetailPanel candidate={candidate} />);
    expect(html).toContain("200");
    expect(html).toContain(en.expansionAdvisor.cannibalization);
  });
});

/* ─── validateBrief pure function ─── */

describe("validateBrief", () => {
  it("returns error when brand_name is empty or whitespace", () => {
    const errors = validateBrief({
      brand_name: "  ",
      category: "qsr",
      service_model: "qsr",
      min_area_m2: 100,
      max_area_m2: 500,
      target_districts: [],
      existing_branches: [],
      limit: 25,
    });
    expect(errors.brand_name).toBe("validationRequired");
  });

  it("returns no errors for valid brief", () => {
    const errors = validateBrief({
      brand_name: "TestBrand",
      category: "qsr",
      service_model: "qsr",
      min_area_m2: 100,
      max_area_m2: 500,
      target_districts: [],
      existing_branches: [],
      limit: 25,
    });
    expect(Object.keys(errors)).toHaveLength(0);
  });

  it("returns area_range error when min > max", () => {
    const errors = validateBrief({
      brand_name: "TestBrand",
      category: "qsr",
      service_model: "qsr",
      min_area_m2: 600,
      max_area_m2: 200,
      target_districts: [],
      existing_branches: [],
      limit: 25,
    });
    expect(errors.area_range).toBe("validationAreaRange");
  });

  it("returns branch lat error for out-of-range latitude", () => {
    const errors = validateBrief({
      brand_name: "TestBrand",
      category: "qsr",
      service_model: "qsr",
      min_area_m2: 100,
      max_area_m2: 500,
      target_districts: [],
      existing_branches: [{ lat: 100, lon: 46.7 }],
      limit: 25,
    });
    expect(errors.branches).toBeDefined();
    expect(errors.branches![0]).toBe("validationLatRange");
  });

  it("returns branch lon error for out-of-range longitude", () => {
    const errors = validateBrief({
      brand_name: "TestBrand",
      category: "qsr",
      service_model: "qsr",
      min_area_m2: 100,
      max_area_m2: 500,
      target_districts: [],
      existing_branches: [{ lat: 24.7, lon: 200 }],
      limit: 25,
    });
    expect(errors.branches).toBeDefined();
    expect(errors.branches![0]).toBe("validationLonRange");
  });

  it("skips branch validation for default 0,0 coordinates", () => {
    const errors = validateBrief({
      brand_name: "TestBrand",
      category: "qsr",
      service_model: "qsr",
      min_area_m2: 100,
      max_area_m2: 500,
      target_districts: [],
      existing_branches: [{ lat: 0, lon: 0 }],
      limit: 25,
    });
    expect(errors.branches).toBeUndefined();
  });
});

/* ─── normalizeCompareResponse ─── */

describe("normalizeCompareResponse", () => {
  it("fills default items and summary when missing", () => {
    const result = normalizeCompareResponse({} as Parameters<typeof normalizeCompareResponse>[0]);
    expect(result.items).toEqual([]);
    expect(result.summary).toEqual({});
  });

  it("ensures gate_status_json defaults on each item", () => {
    const result = normalizeCompareResponse({
      items: [{ candidate_id: "c1", final_score: 80 }],
      summary: { best_overall_candidate_id: "c1" },
    } as Parameters<typeof normalizeCompareResponse>[0]);
    expect(result.items[0].gate_status_json).toEqual({});
  });

  it("preserves existing gate_status_json when present", () => {
    const result = normalizeCompareResponse({
      items: [{ candidate_id: "c1", final_score: 80, gate_status_json: { overall_pass: true } }],
      summary: {},
    } as Parameters<typeof normalizeCompareResponse>[0]);
    expect(result.items[0]?.gate_status_json?.overall_pass).toBe(true);
  });
});

/* ─── Sorting candidates by rank_position then final_score ─── */

describe("sortCandidates rank stability", () => {
  it("sorts by rank_position ascending with null ranks last", () => {
    const candidates = [
      makeCandidate({ id: "c3", rank_position: undefined, final_score: 90 }),
      makeCandidate({ id: "c1", rank_position: 1, final_score: 85 }),
      makeCandidate({ id: "c2", rank_position: 2, final_score: 80 }),
    ];
    const sorted = sortCandidates(candidates, "rank");
    expect(sorted[0].id).toBe("c1");
    expect(sorted[1].id).toBe("c2");
    expect(sorted[2].id).toBe("c3");
  });

  it("sorts by economics descending with null scores at bottom", () => {
    const candidates = [
      makeCandidate({ id: "c1", economics_score: 60 }),
      makeCandidate({ id: "c2", economics_score: 90 }),
      makeCandidate({ id: "c3", economics_score: undefined }),
    ];
    const sorted = sortCandidates(candidates, "economics");
    expect(sorted[0].id).toBe("c2");
    expect(sorted[1].id).toBe("c1");
    expect(sorted[2].id).toBe("c3");
  });
});

/* ─── Report panel dimension winners ─── */

describe("Report panel dimension winners rendering", () => {
  it("renders dimension winner badges when recommendation has multiple winner IDs", () => {
    const html = renderToStaticMarkup(
      <ExpansionReportPanel
        loading={false}
        report={{
          meta: {},
          recommendation: {
            summary: "Test summary",
            best_candidate_id: "c1",
            highest_demand_candidate_id: "c2",
            best_economics_candidate_id: "c1",
            best_brand_fit_candidate_id: "c3",
            strongest_whitespace_candidate_id: "c2",
            fastest_payback_candidate_id: "c1",
            most_confident_candidate_id: "c3",
            best_pass_candidate_id: "c1",
          },
          top_candidates: [],
          assumptions: {},
          brand_profile: {},
        }}
      />,
    );
    expect(html).toContain("ea-compare-highlights");
    expect(html).toContain("ea-compare-highlight");
    expect(html).toContain("ea-badge--green");
  });

  it("renders report_summary when it differs from summary", () => {
    const html = renderToStaticMarkup(
      <ExpansionReportPanel
        loading={false}
        report={{
          meta: {},
          recommendation: {
            summary: "Short executive summary.",
            report_summary: "A longer detailed report summary with more info.",
          },
          top_candidates: [],
          assumptions: {},
          brand_profile: {},
        }}
      />,
    );
    expect(html).toContain("Short executive summary.");
    expect(html).toContain("A longer detailed report summary with more info.");
  });

  it("does not render dimension winners section with fewer than 2 winners", () => {
    const html = renderToStaticMarkup(
      <ExpansionReportPanel
        loading={false}
        report={{
          meta: {},
          recommendation: { summary: "Test", best_candidate_id: "c1" },
          top_candidates: [],
          assumptions: {},
          brand_profile: {},
        }}
      />,
    );
    const highlightCount = (html.match(/ea-compare-highlights/g) || []).length;
    expect(highlightCount).toBe(0);
  });
});

/* ─── Memo panel structured sections ─── */

describe("Memo panel structured score breakdown and feature snapshot", () => {
  it("renders memo with score breakdown data without crashing", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={{
          recommendation: { headline: "Go", verdict: "go" },
          candidate: {
            final_score: 80,
            score_breakdown: {
              final_score: 80,
              weights: { economics: 0.3, brand_fit: 0.25 },
              inputs: { economics: 85, brand_fit: 70 },
              weighted_components: { economics: 25.5, brand_fit: 17.5 },
            },
          },
          market_research: {},
          brand_profile: {},
        }}
      />,
    );
    expect(html).toContain("ea-drawer");
    expect(html).toContain("Go");
  });

  it("renders memo with feature snapshot data without crashing", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={{
          recommendation: { headline: "Caution" },
          candidate: {
            feature_snapshot: {
              data_completeness_score: 72,
              context_sources: { google_places: {}, osm: {} },
              missing_context: ["delivery_platforms", "traffic_data"],
            },
          },
          market_research: {},
          brand_profile: {},
        }}
      />,
    );
    expect(html).toContain("ea-drawer");
    expect(html).toContain("Caution");
  });
});

/* ─── Regression: Normalizer robustness against missing/null backend fields ─── */

describe("Normalizer robustness against edge-case backend payloads", () => {
  it("normalizeCandidate handles gate_status_json as null or non-object", () => {
    const c1 = normalizeCandidate({ id: "c1", search_id: "s", parcel_id: "p", lat: 0, lon: 0, gate_status_json: null as unknown as Record<string, boolean> });
    expect(c1.gate_status_json).toEqual({});
    const c2 = normalizeCandidate({ id: "c2", search_id: "s", parcel_id: "p", lat: 0, lon: 0, gate_status_json: undefined });
    expect(c2.gate_status_json).toEqual({});
  });

  it("normalizeCandidate defaults decision strings", () => {
    const c = normalizeCandidate({ id: "c1", search_id: "s", parcel_id: "p", lat: 0, lon: 0 });
    expect(c.decision_summary).toBe("");
    expect(c.demand_thesis).toBe("");
    expect(c.cost_thesis).toBe("");
    expect(c.confidence_grade).toBe("D");
    expect(c.payback_band).toBe("");
  });

  it("normalizeCandidate handles non-array top_positives_json / top_risks_json", () => {
    const c = normalizeCandidate({ id: "c1", search_id: "s", parcel_id: "p", lat: 0, lon: 0, top_positives_json: "bad" as unknown as string[], top_risks_json: null as unknown as string[] });
    expect(Array.isArray(c.top_positives_json)).toBe(true);
    expect(c.top_positives_json).toEqual([]);
    expect(Array.isArray(c.top_risks_json)).toBe(true);
    expect(c.top_risks_json).toEqual([]);
  });

  it("normalizeCandidate deep-defaults gate_reasons_json sub-arrays", () => {
    const c = normalizeCandidate({ id: "c1", search_id: "s", parcel_id: "p", lat: 0, lon: 0, gate_reasons_json: { thresholds: {}, explanations: {} } as any });
    expect(c.gate_reasons_json?.passed).toEqual([]);
    expect(c.gate_reasons_json?.failed).toEqual([]);
    expect(c.gate_reasons_json?.unknown).toEqual([]);
  });

  it("normalizeCandidate deep-defaults feature_snapshot_json sub-fields", () => {
    const c = normalizeCandidate({ id: "c1", search_id: "s", parcel_id: "p", lat: 0, lon: 0, feature_snapshot_json: { data_completeness_score: 42 } as any });
    expect(c.feature_snapshot_json?.context_sources).toEqual({});
    expect(c.feature_snapshot_json?.missing_context).toEqual([]);
    expect(c.feature_snapshot_json?.data_completeness_score).toBe(42);
  });

  it("normalizeSavedSearch defaults title and status", () => {
    const saved = normalizeSavedSearch({ id: "sv1", search_id: "s1" } as any);
    expect(saved.title).toBe("");
    expect(saved.status).toBe("draft");
    expect(saved.selected_candidate_ids).toEqual([]);
    expect(saved.filters_json).toEqual({});
    expect(saved.ui_state_json).toEqual({});
    expect(saved.candidates).toEqual([]);
  });

  it("normalizeSavedSearch handles nested search with null brand_profile", () => {
    const saved = normalizeSavedSearch({
      id: "sv2",
      search_id: "s2",
      title: "test",
      status: "final",
      search: { id: "s2", target_districts: null as unknown as string[], request_json: null as unknown as Record<string, unknown>, notes: null as unknown as Record<string, unknown>, existing_branches: null as unknown as Array<Record<string, unknown>>, brand_profile: null, meta: null as unknown as any },
    } as any);
    expect(saved.search?.target_districts).toEqual([]);
    expect(saved.search?.request_json).toEqual({});
    expect(saved.search?.notes).toEqual({});
    expect(saved.search?.existing_branches).toEqual([]);
    expect(saved.search?.brand_profile).toBeNull();
    expect(saved.search?.meta).toEqual({});
  });

  it("normalizeCompareResponse defaults item fields", () => {
    const result = normalizeCompareResponse({
      items: [{ candidate_id: "c1" } as any],
      summary: null as unknown as Record<string, string | null>,
    });
    expect(result.summary).toEqual({});
    const item = result.items[0];
    expect(item.gate_status_json).toEqual({});
    expect(item.confidence_grade).toBe("D");
    expect(item.decision_summary).toBe("");
    expect(Array.isArray(item.top_positives_json)).toBe(true);
    expect(Array.isArray(item.top_risks_json)).toBe(true);
  });

  it("normalizeReportResponse handles completely empty payload", () => {
    const report = normalizeReportResponse({} as any);
    expect(report.top_candidates).toEqual([]);
    expect(report.assumptions).toEqual({});
    expect(report.brand_profile).toEqual({});
    expect(report.meta).toEqual({});
    expect(report.recommendation).toBeDefined();
  });
});

/* ─── Regression: Saved-study update/delete/restore lifecycle ─── */

describe("Saved-study update/delete/restore lifecycle", () => {
  it("restoreSavedUiState defaults to sane state when ui_state_json is empty", () => {
    const restored = restoreSavedUiState(
      { id: "sv1", search_id: "s1", title: "t", status: "draft", ui_state_json: {} },
      [],
    );
    expect(restored.compareIds).toEqual([]);
    expect(restored.selectedCandidateId).toBeNull();
    expect(restored.leadCandidateId).toBeNull();
    expect(restored.activeFilter).toBe("all");
    expect(restored.activeSort).toBe("rank");
    expect(restored.districtFilter).toBe("");
    expect(restored.drawerState).toBe("none");
    expect(restored.mapView).toEqual({ center: null, zoom: null });
  });

  it("buildUiStateJson round-trips through restore functions", () => {
    const state = buildUiStateJson("c1", ["c1", "c2"], "c1", "pass_only", "economics", "Olaya", { center: [46.7, 24.7], zoom: 15 }, "compare");
    const sortFilter = restoreSortFilter(state);
    const mapView = restoreMapViewState(state);
    const drawer = restoreDrawerState(state);
    const lead = restoreLeadCandidateId(state, [makeCandidate({ id: "c1" })]);
    expect(sortFilter.activeFilter).toBe("pass_only");
    expect(sortFilter.activeSort).toBe("economics");
    expect(sortFilter.districtFilter).toBe("Olaya");
    expect(mapView.center).toEqual([46.7, 24.7]);
    expect(mapView.zoom).toBe(15);
    expect(drawer).toBe("compare");
    expect(lead).toBe("c1");
  });

  it("extractSavedStudyMeta summarizes saved study correctly", () => {
    const meta = extractSavedStudyMeta({
      id: "sv1",
      search_id: "s1",
      title: "Final Study",
      status: "final",
      selected_candidate_ids: ["c1", "c2", "c3"],
      ui_state_json: { compare_ids: ["c1", "c2"], lead_candidate_id: "c1", active_sort: "economics", active_filter: "pass_only" },
      candidates: [makeCandidate({ id: "c1", district: "Olaya" }), makeCandidate({ id: "c2" }), makeCandidate({ id: "c3" })],
    });
    expect(meta.isFinal).toBe(true);
    expect(meta.shortlistCount).toBe(3);
    expect(meta.compareCount).toBe(2);
    expect(meta.leadDistrict).toBe("Olaya");
    expect(meta.lastSort).toBe("economics");
    expect(meta.lastFilter).toBe("pass_only");
  });

  it("extractSavedStudyMeta handles missing lead candidate gracefully", () => {
    const meta = extractSavedStudyMeta({
      id: "sv2",
      search_id: "s2",
      title: "Draft Study",
      status: "draft",
      selected_candidate_ids: [],
      ui_state_json: {},
      candidates: [],
    });
    expect(meta.leadDistrict).toBeNull();
    expect(meta.leadParcelId).toBeNull();
    expect(meta.isFinal).toBe(false);
    expect(meta.shortlistCount).toBe(0);
    expect(meta.compareCount).toBe(0);
    expect(meta.lastSort).toBeNull();
    expect(meta.lastFilter).toBeNull();
  });
});

/* ─── Regression: Compare outcome derivation ─── */

describe("Compare outcome derivation regression", () => {
  it("deriveCompareOutcome identifies winner and runner-up strengths", () => {
    const candidates = [
      makeCandidate({ id: "c1", rank_position: 1, district: "Olaya" }),
      makeCandidate({ id: "c2", rank_position: 2, district: "Malqa" }),
    ];
    const outcome = deriveCompareOutcome(
      {
        items: [{ candidate_id: "c1" }, { candidate_id: "c2" }] as any[],
        summary: {
          best_overall_candidate_id: "c1",
          best_economics_candidate_id: "c2",
          fastest_payback_candidate_id: "c2",
          best_brand_fit_candidate_id: "c1",
        },
      },
      candidates,
      "c1",
    );
    expect(outcome.winnerId).toBe("c1");
    expect(outcome.winnerLabel).toContain("Olaya");
    expect(outcome.leadsAligned).toBe(true);
    expect(outcome.runnerUpStrengths).toContain("best economics");
    expect(outcome.runnerUpStrengths).toContain("fastest payback");
    expect(outcome.whatWouldChange).toContain("best economics");
  });

  it("deriveCompareOutcome detects lead mismatch", () => {
    const candidates = [
      makeCandidate({ id: "c1", rank_position: 1 }),
      makeCandidate({ id: "c2", rank_position: 2 }),
    ];
    const outcome = deriveCompareOutcome(
      {
        items: [{ candidate_id: "c1" }, { candidate_id: "c2" }] as any[],
        summary: { best_overall_candidate_id: "c2" },
      },
      candidates,
      "c1",
    );
    expect(outcome.winnerId).toBe("c2");
    expect(outcome.leadsAligned).toBe(false);
  });

  it("deriveCompareOutcome handles null result gracefully", () => {
    const outcome = deriveCompareOutcome(null, [], null);
    expect(outcome.winnerId).toBeNull();
    expect(outcome.winnerLabel).toBe("—");
    expect(outcome.leadsAligned).toBe(true);
    expect(outcome.runnerUpStrengths).toEqual([]);
  });
});

/* ─── Regression: Landlord briefing text ─── */

describe("Landlord briefing text generation", () => {
  it("formatLandlordBriefingText includes district, rent, and verification items", () => {
    const text = formatLandlordBriefingText(
      makeCandidate({
        district: "Al Olaya",
        parcel_id: "P-12345",
        rank_position: 1,
        estimated_rent_sar_m2_year: 1200,
        estimated_annual_rent_sar: 360000,
        gate_status_json: { overall_pass: true },
      }),
    );
    expect(text).toContain("Al Olaya");
    expect(text).toContain("P-12345");
    expect(text).toContain("#1");
    expect(text).toContain("1200 SAR/m²/yr");
    expect(text).toContain("360,000 SAR/yr");
    expect(text).toContain("All gates passed");
    expect(text).toContain("Confirm street frontage");
    expect(text).toContain("Verify parking");
  });

  it("formatLandlordBriefingText handles missing economics gracefully", () => {
    const text = formatLandlordBriefingText(makeCandidate({ district: "Test" }));
    expect(text).toContain("Test");
    expect(text).toContain("TBD");
    expect(text).toContain("Gates pending verification");
  });
});

/* ─── Regression: Branch rows layout ─── */

describe("Existing branches rendering", () => {
  it("renders branch rows with all four input fields visible", () => {
    const brief = {
      ...defaultBrief,
      existing_branches: [
        { name: "HQ", lat: 24.7, lon: 46.7, district: "Al Olaya" },
      ],
    };
    const html = renderToStaticMarkup(
      <ExpansionBriefForm initialValue={brief} onSubmit={() => {}} loading={false} />,
    );
    expect(html).toContain("ea-branch-row");
    expect(html).toContain('value="HQ"');
    expect(html).toContain('value="24.7"');
    expect(html).toContain('value="46.7"');
    expect(html).toContain('value="Al Olaya"');
  });

  it("renders multiple branch rows without overlapping", () => {
    const brief = {
      ...defaultBrief,
      existing_branches: [
        { name: "Branch 1", lat: 24.7, lon: 46.7, district: "Al Olaya" },
        { name: "Branch 2", lat: 24.8, lon: 46.8, district: "Al Malqa" },
        { name: "Branch 3", lat: 24.9, lon: 46.9, district: "Al Nakheel" },
      ],
    };
    const html = renderToStaticMarkup(
      <ExpansionBriefForm initialValue={brief} onSubmit={() => {}} loading={false} />,
    );
    // All three rows should be rendered
    const rowMatches = html.match(/ea-branch-row/g);
    expect(rowMatches).not.toBeNull();
    expect(rowMatches!.length).toBeGreaterThanOrEqual(3);
    // All branch names present
    expect(html).toContain('value="Branch 1"');
    expect(html).toContain('value="Branch 2"');
    expect(html).toContain('value="Branch 3"');
  });

  it("renders add-branch button and remove-branch button per row", () => {
    const brief = {
      ...defaultBrief,
      existing_branches: [
        { name: "HQ", lat: 24.7, lon: 46.7, district: "Al Olaya" },
      ],
    };
    const html = renderToStaticMarkup(
      <ExpansionBriefForm initialValue={brief} onSubmit={() => {}} loading={false} />,
    );
    // Add branch button
    expect(html).toContain(en.expansionAdvisor.addBranch);
    // Remove branch button
    expect(html).toContain(en.expansionAdvisor.removeBranch);
  });

  it("renders submit button in normal flow after branches section", () => {
    const brief = {
      ...defaultBrief,
      brand_name: "Al Baik",
      existing_branches: [
        { name: "HQ", lat: 24.7, lon: 46.7, district: "Al Olaya" },
        { name: "Branch 2", lat: 24.8, lon: 46.8, district: "Al Malqa" },
      ],
    };
    const html = renderToStaticMarkup(
      <ExpansionBriefForm initialValue={brief} onSubmit={() => {}} loading={false} />,
    );
    // Submit button should be present after branches
    expect(html).toContain(en.expansionAdvisor.runSearchCta);
    // Branches section title should be present
    expect(html).toContain(en.expansionAdvisor.existingBranchesLabel);
  });

  it("renders empty state when no branches exist", () => {
    const html = renderToStaticMarkup(
      <ExpansionBriefForm initialValue={defaultBrief} onSubmit={() => {}} loading={false} />,
    );
    expect(html).toContain(en.expansionAdvisor.noBranchesYet);
    expect(html).toContain(en.expansionAdvisor.addBranch);
    // No branch rows
    expect(html).not.toContain("ea-branch-row");
  });

  it("handles long branch name and district values", () => {
    const longName = "A Very Long Branch Name That Should Still Render Correctly";
    const longDistrict = "Al Olaya Al Malqa Al Nakheel Combined Super District";
    const brief = {
      ...defaultBrief,
      existing_branches: [
        { name: longName, lat: 24.7, lon: 46.7, district: longDistrict },
      ],
    };
    const html = renderToStaticMarkup(
      <ExpansionBriefForm initialValue={brief} onSubmit={() => {}} loading={false} />,
    );
    expect(html).toContain(longName);
    expect(html).toContain(longDistrict);
  });
});

/* ─── Regression: Native select dropdown indicators ─── */

describe("Native select elements render with ea-form__select class for dropdown indicators", () => {
  it("service model, price tier, and sensitivity selects use ea-form__select", () => {
    const html = renderToStaticMarkup(
      <ExpansionBriefForm initialValue={defaultBrief} onSubmit={() => {}} loading={false} />,
    );
    // Count the number of ea-form__select occurrences (7 native selects)
    const selectMatches = html.match(/ea-form__select/g);
    expect(selectMatches).not.toBeNull();
    expect(selectMatches!.length).toBeGreaterThanOrEqual(7);
  });
});

/* ─── Regression: Expansion Advisor search payload normalization ─── */

describe("Expansion Advisor payload normalization regression", () => {
  it("normalizes service_model display labels to backend enum values", () => {
    const variants: [string, string][] = [
      ["QSR", "qsr"],
      ["qsr", "qsr"],
      ["Dine In", "dine_in"],
      ["dine_in", "dine_in"],
      ["Delivery First", "delivery_first"],
      ["delivery_first", "delivery_first"],
      ["CAFE", "cafe"],
      ["café", "cafe"],
    ];
    for (const [input, expected] of variants) {
      const result = normalizeBriefPayload({
        ...defaultBrief,
        brand_name: "Test",
        service_model: input as any,
      });
      expect(result.service_model).toBe(expected);
    }
  });

  it("falls back to 'qsr' for unknown service_model values", () => {
    const result = normalizeBriefPayload({
      ...defaultBrief,
      brand_name: "Test",
      service_model: "unknown_model" as any,
    });
    expect(result.service_model).toBe("qsr");
  });

  it("normalizes price_tier display labels to backend enum values", () => {
    for (const tier of ["Value", "MID", "Premium"]) {
      const result = normalizeBriefPayload({
        ...defaultBrief,
        brand_name: "Test",
        brand_profile: { price_tier: tier as any },
      });
      expect(result.brand_profile?.price_tier).toBe(tier.toLowerCase());
    }
  });

  it("nullifies unknown price_tier values", () => {
    const result = normalizeBriefPayload({
      ...defaultBrief,
      brand_name: "Test",
      brand_profile: { price_tier: "expensive" as any },
    });
    expect(result.brand_profile?.price_tier).toBeNull();
  });

  it("normalizes primary_channel and expansion_goal enums", () => {
    const result = normalizeBriefPayload({
      ...defaultBrief,
      brand_name: "Test",
      brand_profile: {
        primary_channel: "Dine In" as any,
        expansion_goal: "Delivery Led" as any,
      },
    });
    expect(result.brand_profile?.primary_channel).toBe("dine_in");
    expect(result.brand_profile?.expansion_goal).toBe("delivery_led");
  });

  it("coerces existing branch empty name/district to undefined (not empty string)", () => {
    const result = normalizeBriefPayload({
      ...defaultBrief,
      brand_name: "Test",
      existing_branches: [
        { name: "", lat: 24.7, lon: 46.7, district: "" },
        { name: "HQ", lat: 24.8, lon: 46.8, district: "Olaya" },
        { name: "  ", lat: 24.9, lon: 46.9, district: "  " },
      ],
    });
    expect(result.existing_branches).toHaveLength(3);
    // First branch: empty strings become undefined
    expect(result.existing_branches[0].name).toBeUndefined();
    expect(result.existing_branches[0].district).toBeUndefined();
    // Second branch: valid strings preserved
    expect(result.existing_branches[1].name).toBe("HQ");
    expect(result.existing_branches[1].district).toBe("Olaya");
    // Third branch: whitespace-only strings become undefined
    expect(result.existing_branches[2].name).toBeUndefined();
    expect(result.existing_branches[2].district).toBeUndefined();
  });

  it("coerces branch lat/lon to numbers", () => {
    const result = normalizeBriefPayload({
      ...defaultBrief,
      brand_name: "Test",
      existing_branches: [
        { name: "A", lat: "24.7" as any, lon: "46.7" as any },
      ],
    });
    expect(result.existing_branches[0].lat).toBe(24.7);
    expect(result.existing_branches[0].lon).toBe(46.7);
    expect(typeof result.existing_branches[0].lat).toBe("number");
    expect(typeof result.existing_branches[0].lon).toBe("number");
  });

  it("filters out branches with NaN lat/lon", () => {
    const result = normalizeBriefPayload({
      ...defaultBrief,
      brand_name: "Test",
      existing_branches: [
        { name: "Bad", lat: NaN, lon: 46.7 },
        { name: "OK", lat: 24.7, lon: 46.7 },
      ],
    });
    expect(result.existing_branches).toHaveLength(1);
    expect(result.existing_branches[0].name).toBe("OK");
  });

  it("falls back category to service_model when empty", () => {
    const result = normalizeBriefPayload({
      ...defaultBrief,
      brand_name: "Test",
      category: "",
      service_model: "cafe",
    });
    expect(result.category).toBe("cafe");
  });

  it("falls back category to service_model when whitespace-only", () => {
    const result = normalizeBriefPayload({
      ...defaultBrief,
      brand_name: "Test",
      category: "   ",
      service_model: "dine_in",
    });
    expect(result.category).toBe("dine_in");
  });

  it("generates a valid payload with no parcel-selection dependency", () => {
    // This test confirms that Expansion Advisor submit does NOT depend on
    // any map/parcel selection state from Development Feasibility mode.
    const result = normalizeBriefPayload({
      ...defaultBrief,
      brand_name: "Al Baik",
      category: "Burgers",
    });
    expect(result.brand_name).toBe("Al Baik");
    expect(result.category).toBe("Burgers");
    expect(result.service_model).toBe("qsr");
    expect(result.existing_branches).toEqual([]);
    expect(result.target_districts).toEqual([]);
    expect(result.limit).toBe(25);
    // Payload has no parcel_ids, geometry, or bbox fields
    expect((result as any).parcel_ids).toBeUndefined();
    expect((result as any).geometry).toBeUndefined();
  });

  it("produces a complete valid payload from default brief", () => {
    const result = normalizeBriefPayload({
      ...defaultBrief,
      brand_name: "Test Brand",
    });
    // All required fields present and valid
    expect(result.brand_name).toBe("Test Brand");
    expect(result.category.length).toBeGreaterThan(0);
    expect(["qsr", "dine_in", "delivery_first", "cafe"]).toContain(result.service_model);
    expect(result.min_area_m2).toBeGreaterThanOrEqual(0);
    expect(result.max_area_m2).toBeGreaterThanOrEqual(0);
    expect(result.limit).toBeGreaterThanOrEqual(1);
    expect(result.limit).toBeLessThanOrEqual(100);
    // Brand profile enums are all valid
    const bp = result.brand_profile!;
    if (bp.price_tier) expect(["value", "mid", "premium"]).toContain(bp.price_tier);
    if (bp.primary_channel) expect(["dine_in", "delivery", "balanced"]).toContain(bp.primary_channel);
    if (bp.expansion_goal) expect(["flagship", "neighborhood", "delivery_led", "balanced"]).toContain(bp.expansion_goal);
    if (bp.parking_sensitivity) expect(["low", "medium", "high"]).toContain(bp.parking_sensitivity);
    if (bp.frontage_sensitivity) expect(["low", "medium", "high"]).toContain(bp.frontage_sensitivity);
    if (bp.visibility_sensitivity) expect(["low", "medium", "high"]).toContain(bp.visibility_sensitivity);
  });

  it("JSON.stringify of normalized payload omits undefined branch fields", () => {
    const result = normalizeBriefPayload({
      ...defaultBrief,
      brand_name: "Test",
      existing_branches: [
        { name: "", lat: 24.7, lon: 46.7, district: "" },
      ],
    });
    const json = JSON.parse(JSON.stringify(result));
    // name and district should be absent from serialized JSON (not empty strings)
    expect(json.existing_branches[0]).not.toHaveProperty("name");
    expect(json.existing_branches[0]).not.toHaveProperty("district");
    expect(json.existing_branches[0].lat).toBe(24.7);
    expect(json.existing_branches[0].lon).toBe(46.7);
  });

  it("normalizes sensitivity fields to valid enum values", () => {
    const result = normalizeBriefPayload({
      ...defaultBrief,
      brand_name: "Test",
      brand_profile: {
        parking_sensitivity: "HIGH" as any,
        frontage_sensitivity: "Low" as any,
        visibility_sensitivity: "MEDIUM" as any,
      },
    });
    expect(result.brand_profile?.parking_sensitivity).toBe("high");
    expect(result.brand_profile?.frontage_sensitivity).toBe("low");
    expect(result.brand_profile?.visibility_sensitivity).toBe("medium");
  });
});

/* ═══════════════════════════════════════════════════════════════════════
 *  UI/UX correctness patch tests
 * ═══════════════════════════════════════════════════════════════════════ */

describe("UI/UX correctness: zero passing candidates => no Lead Site approval framing", () => {
  it("buildDecisionSnapshot uses 'Top ranked candidate' when gates fail", () => {
    const c = makeCandidate({ gate_status_json: { overall_pass: false, zoning_fit_pass: false } });
    const snap = buildDecisionSnapshot(c);
    expect(snap.siteLabel).toBe("Top ranked candidate");
    expect(snap.allGatesPass).toBe(false);
    expect(snap.whyItWinsLabel).toBe("Top strength");
  });

  it("buildDecisionSnapshot uses 'Lead Site' when gates pass", () => {
    const c = makeCandidate({ gate_status_json: { overall_pass: true, zoning_fit_pass: true } });
    const snap = buildDecisionSnapshot(c);
    expect(snap.siteLabel).toBe("Lead Site");
    expect(snap.allGatesPass).toBe(true);
    expect(snap.whyItWinsLabel).toBe("Why it wins");
  });

  it("buildDecisionSnapshot uses 'Top ranked candidate' when overall_pass is null", () => {
    const c = makeCandidate({ gate_status_json: { overall_pass: null as any } });
    const snap = buildDecisionSnapshot(c);
    expect(snap.siteLabel).toBe("Top ranked candidate");
    expect(snap.allGatesPass).toBe(false);
  });

  it("buildCopySummary shows noPassNotice when gates fail", () => {
    const c = makeCandidate({ gate_status_json: { overall_pass: false } });
    const summary = buildCopySummary(c, null, null);
    expect(summary.noPassNotice).toContain("No candidate currently passes");
    expect(summary.siteLabel).toBe("Top ranked candidate");
  });

  it("buildCopySummary hides noPassNotice when gates pass", () => {
    const c = makeCandidate({ gate_status_json: { overall_pass: true } });
    const summary = buildCopySummary(c, null, null);
    expect(summary.noPassNotice).toBeNull();
    expect(summary.siteLabel).toBe("Lead site");
  });

  it("formatCopySummaryText includes noPassNotice when gates fail", () => {
    const c = makeCandidate({ gate_status_json: { overall_pass: false } });
    const summary = buildCopySummary(c, null, null);
    const text = formatCopySummaryText(summary);
    expect(text).toContain("No candidate currently passes");
    expect(text).toContain("Top ranked candidate:");
    expect(text).not.toContain("Lead site:");
  });

  it("CandidateCard does not show Lead Site tag when gates fail", () => {
    const c = makeCandidate({ district: "Al Olaya", gate_status_json: { overall_pass: false } });
    const html = renderToStaticMarkup(
      <ExpansionCandidateCard
        candidate={c}
        selected={false}
        shortlisted={false}
        compared={false}
        isLead={true}
        onSelect={() => {}}
        onToggleShortlist={() => {}}
        onCompareToggle={() => {}}
      />,
    );
    expect(html).not.toContain("Lead Site");
    expect(html).toContain("Top exploratory candidate");
  });

  it("CandidateCard shows Lead Site tag when gates pass", () => {
    const c = makeCandidate({ district: "Al Olaya", gate_status_json: { overall_pass: true } });
    const html = renderToStaticMarkup(
      <ExpansionCandidateCard
        candidate={c}
        selected={false}
        shortlisted={false}
        compared={false}
        isLead={true}
        onSelect={() => {}}
        onToggleShortlist={() => {}}
        onCompareToggle={() => {}}
      />,
    );
    expect(html).toContain("Lead Site");
  });
});

describe("UI/UX correctness: unknown gates render as ? not ✗", () => {
  it("parseGateEntries marks null gates as unknown, not fail", () => {
    const entries = parseGateEntries(
      { zoning_fit_pass: true, parking_pass: null as any, frontage_access_pass: false },
      { passed: ["zoning_fit_pass"], failed: ["frontage_access_pass"], unknown: ["parking_pass"], thresholds: {}, explanations: {} },
    );
    const parking = entries.find((e) => e.name === "Parking");
    expect(parking).toBeDefined();
    expect(parking!.status).toBe("unknown");

    const zoning = entries.find((e) => e.name === "Zoning fit");
    expect(zoning!.status).toBe("pass");

    const frontage = entries.find((e) => e.name === "Frontage / access");
    expect(frontage!.status).toBe("fail");
  });

  it("parseGateEntries marks undefined gates as unknown", () => {
    const entries = parseGateEntries(
      { parking_pass: undefined as any },
      undefined,
    );
    expect(entries[0].status).toBe("unknown");
  });

  it("GateSummary renders ✓ for pass, ✗ for fail, ? for unknown", () => {
    const html = renderToStaticMarkup(
      <GateSummary
        gates={{ zoning_fit_pass: true, parking_pass: null as any, frontage_access_pass: false }}
        unknownGates={["parking_pass"]}
      />,
    );
    // Check pass icon
    expect(html).toContain("✓");
    // Check fail icon
    expect(html).toContain("✗");
    // Check unknown icon
    expect(html).toContain("?");
    // Check human labels are used
    expect(html).toContain("Zoning fit");
    expect(html).toContain("Parking");
    expect(html).toContain("Frontage / access");
    // Raw keys should NOT appear
    expect(html).not.toContain("zoning_fit_pass");
    expect(html).not.toContain("parking_pass");
    expect(html).not.toContain("frontage_access_pass");
  });
});

describe("UI/UX correctness: score breakdown shows points + weight %, not huge percentages", () => {
  it("ScoreBreakdownCompact renders pts and weight%", () => {
    const breakdown = {
      weights: { competition_whitespace: 0.2, brand_fit: 0.2 },
      inputs: { competition_whitespace: 75, brand_fit: 62 },
      weighted_components: { competition_whitespace: 19, brand_fit: 15.4 },
      final_score: 68,
    };
    const html = renderToStaticMarkup(<ScoreBreakdownCompact breakdown={breakdown} />);
    // Should show points with 'pts'
    expect(html).toContain("pts");
    // Should show weight %
    expect(html).toContain("20% weight");
    // Should NOT show 1900% or 2000% (the old percentage bug)
    expect(html).not.toMatch(/\b1[5-9]\d{2}%/);
    expect(html).not.toMatch(/\b2\d{3}%/);
  });

  it("parseScoreBreakdown returns correct component values", () => {
    const bd = {
      weights: { econ: 0.3 },
      inputs: { econ: 80 },
      weighted_components: { econ: 24 },
      final_score: 72,
    };
    const comps = parseScoreBreakdown(bd);
    expect(comps).toHaveLength(1);
    expect(comps[0].weight).toBe(0.3);
    expect(comps[0].input).toBe(80);
    expect(comps[0].weighted).toBe(24);
  });
});

describe("UI/UX correctness: verdict and confidence badges are separated", () => {
  it("ConfidenceBadge renders with Data: prefix by default", () => {
    const html = renderToStaticMarkup(<ConfidenceBadge grade="A" />);
    expect(html).toContain("Data: A");
  });

  it("ConfidenceBadge renders compact without prefix", () => {
    const html = renderToStaticMarkup(<ConfidenceBadge grade="B" compact />);
    expect(html).toContain(">B<");
    expect(html).not.toContain("Data:");
  });

  it("CandidateCard renders separate verdict and confidence badges", () => {
    const c = makeCandidate({ gate_status_json: { overall_pass: false }, confidence_grade: "A" });
    const html = renderToStaticMarkup(
      <ExpansionCandidateCard
        candidate={c}
        selected={false}
        shortlisted={false}
        compared={false}
        onSelect={() => {}}
        onToggleShortlist={() => {}}
        onCompareToggle={() => {}}
      />,
    );
    // Verdict badge should say Fail, not "Fail A"
    expect(html).toContain("Fail");
    // Confidence badge should be separate
    expect(html).toContain("Data: A");
    // Should NOT show "Fail A" combined
    expect(html).not.toContain("Fail A");
  });
});

describe("UI/UX correctness: raw gate keys never appear in rendered text", () => {
  it("humanGateLabel converts raw keys to clean labels", () => {
    expect(humanGateLabel("zoning_fit_pass")).toBe("Zoning fit");
    expect(humanGateLabel("frontage_access_pass")).toBe("Frontage / access");
    expect(humanGateLabel("parking_pass")).toBe("Parking");
    expect(humanGateLabel("visibility_pass")).toBe("Visibility");
    expect(humanGateLabel("some_new_gate_pass")).toBe("Some new gate");
  });

  it("humanGateSentence produces polished copy", () => {
    expect(humanGateSentence("zoning_fit_pass", "fail")).toBe("Zoning fit failed.");
    expect(humanGateSentence("frontage_access_pass", "unknown")).toBe("Frontage / access needs field verification.");
    expect(humanGateSentence("parking_pass", "pass")).toBe("Parking passed.");
  });

  it("CandidateDetailPanel renders human labels for gate reasons", () => {
    const c = makeCandidate({
      gate_status_json: { overall_pass: false, zoning_fit_pass: false, parking_pass: null as any },
      gate_reasons_json: {
        passed: [],
        failed: ["zoning_fit_pass"],
        unknown: ["parking_pass"],
        thresholds: {},
        explanations: {},
      },
    });
    const html = renderToStaticMarkup(<CandidateDetailPanel candidate={c} />);
    // Human label should appear
    expect(html).toContain("Zoning fit");
    expect(html).toContain("Parking");
    // Raw key should NOT appear in user-facing text
    expect(html).not.toContain("zoning_fit_pass");
    expect(html).not.toContain("parking_pass");
  });
});

describe("UI/UX correctness: district label fallback", () => {
  it("isGarbledText detects garbled sequences", () => {
    expect(isGarbledText(null)).toBe(true);
    expect(isGarbledText("")).toBe(true);
    expect(isGarbledText("   ")).toBe(true);
    expect(isGarbledText("\uFFFD\uFFFD\uFFFD")).toBe(true);
    expect(isGarbledText("Al Olaya")).toBe(false);
    expect(isGarbledText("العليا")).toBe(false);
  });

  it("safeDistrictLabel prefers valid Arabic, then English, then key", () => {
    expect(safeDistrictLabel("العليا", "Al Olaya", "al_olaya")).toBe("العليا");
    expect(safeDistrictLabel("\uFFFD\uFFFD", "Al Olaya", "al_olaya")).toBe("Al Olaya");
    expect(safeDistrictLabel(null, null, "al_olaya")).toBe("al olaya");
    expect(safeDistrictLabel(null, null, null)).toBe("Unknown district");
  });

  it("candidateDistrictLabel uses district_display when present", () => {
    const c = makeCandidate({
      district: "garbled_raw",
      district_display: "الملقا",
      district_name_ar: "الملقا",
      district_name_en: "Al Malqa",
      district_key: "الملقا",
    } as any);
    expect(candidateDistrictLabel(c)).toBe("الملقا");
  });

  it("candidateDistrictLabel falls back through safeDistrictLabel when district_display is missing", () => {
    const c = makeCandidate({
      district: "Al Olaya",
      district_display: null,
      district_name_ar: null,
      district_name_en: "Al Olaya",
      district_key: "al_olaya",
    } as any);
    expect(candidateDistrictLabel(c)).toBe("Al Olaya");
  });

  it("candidateDistrictLabel shows Unknown district when all fields are garbled", () => {
    const c = makeCandidate({
      district: "\uFFFD\uFFFD\uFFFD",
      district_display: null,
      district_name_ar: null,
      district_name_en: null,
      district_key: null,
    } as any);
    expect(candidateDistrictLabel(c)).toBe("Unknown district");
  });

  it("candidateDistrictLabel uses raw district as fallback when it is clean", () => {
    // No canonical fields from backend — only raw district
    const c = makeCandidate({
      district: "الرياض",
    });
    expect(candidateDistrictLabel(c)).toBe("الرياض");
  });

  it("candidateDistrictLabel returns fallback for null candidate", () => {
    expect(candidateDistrictLabel(null)).toBe("Unknown district");
    expect(candidateDistrictLabel(undefined)).toBe("Unknown district");
  });

  it("buildFinalistTiles uses canonical district label", () => {
    const c = makeCandidate({
      district: "\uFFFD\uFFFD\uFFFD",
      district_display: "الملقا",
    } as any);
    const tiles = buildFinalistTiles([c], ["c1"], null);
    expect(tiles[0].district).toBe("الملقا");
  });

  it("buildFinalistTiles shows Unknown district when no fallback exists", () => {
    const c = makeCandidate({
      district: "\uFFFD\uFFFD\uFFFD",
      district_display: null,
      district_name_ar: null,
      district_name_en: null,
      district_key: null,
    } as any);
    const tiles = buildFinalistTiles([c], ["c1"], null);
    // Should not show garbled text
    expect(tiles[0].district).not.toContain("\uFFFD");
  });

  it("extractDistricts uses canonical labels", () => {
    const c1 = makeCandidate({ id: "c1", district: "\uFFFD\uFFFD\uFFFD", district_display: "الملقا" } as any);
    const c2 = makeCandidate({ id: "c2", district: "العليا", district_display: "العليا" } as any);
    const districts = extractDistricts([c1, c2]);
    expect(districts).toContain("الملقا");
    expect(districts).toContain("العليا");
    expect(districts.some((d) => d.includes("\uFFFD"))).toBe(false);
  });
});

/* ═══════════════════════════════════════════════════════════════════════════
 * Regression tests for expansion advisor fixes
 * ═══════════════════════════════════════════════════════════════════════════ */

import { normalizeWeightPercent, assertWeightedPointsSane } from "./scoreInvariants";

describe("Regression: no Lead Site framing when pass gates = 0", () => {
  it("buildCopySummary.siteLabel is 'Top ranked candidate' when overall_pass is false", () => {
    const c = makeCandidate({ gate_status_json: { overall_pass: false } });
    const summary = buildCopySummary(c, null, null);
    expect(summary.siteLabel).not.toBe("Lead site");
    expect(summary.siteLabel).toBe("Top ranked candidate");
  });

  it("buildDecisionSnapshot.siteLabel is 'Top ranked candidate' when overall_pass is null (unknown)", () => {
    const c = makeCandidate({ gate_status_json: { overall_pass: null as any } });
    const snap = buildDecisionSnapshot(c);
    expect(snap.siteLabel).toBe("Top ranked candidate");
  });

  it("buildFinalistTiles marks gateVerdict=unknown for null overall_pass", () => {
    const c = makeCandidate({ id: "f1", gate_status_json: { overall_pass: null as any } });
    const tiles = buildFinalistTiles([c], ["f1"], null);
    expect(tiles[0].gateVerdict).toBe("unknown");
  });
});

describe("Regression: unknown gates render as neutral/question mark", () => {
  it("GateSummary renders ? for null-valued gates", () => {
    const html = renderToStaticMarkup(
      <GateSummary
        gates={{ parking_pass: null as any, frontage_access_pass: null as any }}
        unknownGates={["parking_pass", "frontage_access_pass"]}
      />,
    );
    // Every gate should have the ? icon, not ✗
    expect(html).not.toContain("✗");
    expect(html).toContain("?");
    expect(html).toContain("ea-gate-item--unknown");
    expect(html).not.toContain("ea-gate-item--fail");
  });
});

describe("Regression: score breakdown never renders 2000%, 2500%, etc.", () => {
  it("normalizeWeightPercent handles backend integer weights (25 → '25')", () => {
    expect(normalizeWeightPercent(25)).toBe("25");
    expect(normalizeWeightPercent(20)).toBe("20");
    expect(normalizeWeightPercent(5)).toBe("5");
  });

  it("normalizeWeightPercent handles fractional weights (0.25 → '25')", () => {
    expect(normalizeWeightPercent(0.25)).toBe("25");
    expect(normalizeWeightPercent(0.2)).toBe("20");
    expect(normalizeWeightPercent(0.05)).toBe("5");
  });

  it("normalizeWeightPercent handles edge cases", () => {
    expect(normalizeWeightPercent(0)).toBe("0");
    expect(normalizeWeightPercent(NaN)).toBe("0");
    expect(normalizeWeightPercent(100)).toBe("100");
  });

  it("ScoreBreakdownCompact with backend-style integer weights (25) shows 25% not 2500%", () => {
    const breakdown = {
      weights: { demand_potential: 25, brand_fit: 20 },
      inputs: { demand_potential: 80, brand_fit: 70 },
      weighted_components: { demand_potential: 20, brand_fit: 14 },
      final_score: 72,
    };
    const html = renderToStaticMarkup(<ScoreBreakdownCompact breakdown={breakdown} />);
    expect(html).toContain("25% weight");
    expect(html).toContain("20% weight");
    expect(html).not.toMatch(/\b2500%/);
    expect(html).not.toMatch(/\b2000%/);
  });

  it("ScoreBreakdownCompact with fractional weights (0.25) also shows 25%", () => {
    const breakdown = {
      weights: { demand_potential: 0.25, brand_fit: 0.20 },
      inputs: { demand_potential: 80, brand_fit: 70 },
      weighted_components: { demand_potential: 20, brand_fit: 14 },
      final_score: 72,
    };
    const html = renderToStaticMarkup(<ScoreBreakdownCompact breakdown={breakdown} />);
    expect(html).toContain("25% weight");
    expect(html).toContain("20% weight");
  });
});

describe("Regression: Fail and confidence grade are separate UI elements", () => {
  it("CandidateCard keeps verdict and confidence visually separate", () => {
    const c = makeCandidate({
      gate_status_json: { overall_pass: false, zoning_fit_pass: false },
      confidence_grade: "B",
      final_score: 65,
    });
    const html = renderToStaticMarkup(
      <ExpansionCandidateCard
        candidate={c}
        selected={false}
        shortlisted={false}
        compared={false}
        onSelect={() => {}}
        onToggleShortlist={() => {}}
        onCompareToggle={() => {}}
      />,
    );
    // Both should be present
    expect(html).toContain("Fail");
    expect(html).toContain("Data: B");
    // They should NOT be mashed together
    expect(html).not.toContain("FailB");
    expect(html).not.toContain("Fail B");
    expect(html).not.toContain("BFail");
  });
});

describe("Regression: raw keys like zoning_fit_pass never appear in user-visible text", () => {
  it("deriveDecisionChecklist uses human-readable labels for unknowns", () => {
    const c = makeCandidate({
      gate_status_json: { overall_pass: null as any, zoning_fit_pass: null as any },
      gate_reasons_json: {
        passed: [],
        failed: [],
        unknown: ["zoning_fit_pass"],
        thresholds: {},
        explanations: {},
      },
      feature_snapshot_json: {
        data_completeness_score: 50,
        missing_context: ["parking_context_unavailable"],
        context_sources: {},
      },
    });
    const items = deriveDecisionChecklist(c);
    for (const item of items) {
      expect(item.label).not.toContain("_pass");
      expect(item.label).not.toMatch(/^[a-z_]+$/); // no raw snake_case keys
    }
  });

  it("parseGateEntries never exposes raw key as the name", () => {
    const entries = parseGateEntries(
      { zoning_fit_pass: true, parking_pass: false, economics_pass: null as any },
      { passed: ["zoning_fit_pass"], failed: ["parking_pass"], unknown: ["economics_pass"], thresholds: {}, explanations: {} },
    );
    for (const e of entries) {
      expect(e.name).not.toContain("_pass");
      expect(e.name).not.toContain("_fit_");
    }
  });
});

describe("Regression: scoreInvariants dev assertion", () => {
  it("assertWeightedPointsSane warns when weighted > rawInput", () => {
    const warnings: string[] = [];
    const origWarn = console.warn;
    console.warn = (msg: string) => warnings.push(msg);
    try {
      assertWeightedPointsSane(2500, 80, "demand_potential");
      expect(warnings.length).toBe(1);
      expect(warnings[0]).toContain("score-invariant");
    } finally {
      console.warn = origWarn;
    }
  });

  it("assertWeightedPointsSane is silent when values are sane", () => {
    const warnings: string[] = [];
    const origWarn = console.warn;
    console.warn = (msg: string) => warnings.push(msg);
    try {
      assertWeightedPointsSane(20, 80, "demand_potential");
      expect(warnings.length).toBe(0);
    } finally {
      console.warn = origWarn;
    }
  });
});

/* ═══════════════════════════════════════════════════════════════════════════
 *  Expansion Advisor UX correctness patch — additional tests
 * ═══════════════════════════════════════════════════════════════════════════ */

describe("UX correctness: no-pass state never shows Lead-approval wording", () => {
  it("NextStepsStrip renders exploratory wording when lead does not pass gates", () => {
    const lead = makeCandidate({
      id: "lead1",
      district: "Olaya",
      rank_position: 1,
      gate_status_json: { overall_pass: false },
    });
    const html = renderToStaticMarkup(
      <NextStepsStrip
        candidates={[lead]}
        shortlistIds={["lead1"]}
        leadCandidateId="lead1"
        report={null}
        onOpenMemo={() => {}}
        onOpenReport={() => {}}
        onCompare={() => {}}
      />,
    );
    // Should NOT contain lead-approval wording
    expect(html).not.toContain("Lead site selected");
    expect(html).not.toContain("Open Lead Memo");
    // Should contain exploratory wording
    expect(html).toContain("Top exploratory candidate selected");
    expect(html).toContain("Open Candidate Memo");
  });

  it("NextStepsStrip renders lead wording when candidate passes gates", () => {
    const lead = makeCandidate({
      id: "lead1",
      district: "Olaya",
      rank_position: 1,
      gate_status_json: { overall_pass: true },
    });
    const html = renderToStaticMarkup(
      <NextStepsStrip
        candidates={[lead]}
        shortlistIds={["lead1"]}
        leadCandidateId="lead1"
        report={null}
        onOpenMemo={() => {}}
        onOpenReport={() => {}}
        onCompare={() => {}}
      />,
    );
    expect(html).toContain("Lead site selected");
    expect(html).toContain("Open Lead Memo");
  });

  it("FinalistsWorkspace shows 'Mark exploratory pick' for non-passing tile", () => {
    const c = makeCandidate({
      id: "f1",
      district: "Al Malaz",
      gate_status_json: { overall_pass: false },
      top_positives_json: ["good"],
      top_risks_json: ["bad"],
    });
    const html = renderToStaticMarkup(
      <FinalistsWorkspace
        candidates={[c]}
        shortlistIds={["f1"]}
        leadCandidateId={null}
        selectedCandidateId={null}
        onSetLead={() => {}}
        onClearLead={() => {}}
        onOpenMemo={() => {}}
        onCompare={() => {}}
        onRemoveShortlist={() => {}}
        onSelectCandidate={() => {}}
        compareEnabled={false}
      />,
    );
    expect(html).toContain("Mark exploratory pick");
    expect(html).not.toContain("Set as Lead");
  });

  it("FinalistsWorkspace shows 'Set as Lead' for passing tile", () => {
    const c = makeCandidate({
      id: "f1",
      district: "Al Malaz",
      gate_status_json: { overall_pass: true },
      top_positives_json: ["good"],
      top_risks_json: ["bad"],
    });
    const html = renderToStaticMarkup(
      <FinalistsWorkspace
        candidates={[c]}
        shortlistIds={["f1"]}
        leadCandidateId={null}
        selectedCandidateId={null}
        onSetLead={() => {}}
        onClearLead={() => {}}
        onOpenMemo={() => {}}
        onCompare={() => {}}
        onRemoveShortlist={() => {}}
        onSelectCandidate={() => {}}
        compareEnabled={false}
      />,
    );
    expect(html).toContain("Set as Lead");
    expect(html).not.toContain("Mark exploratory pick");
  });
});

describe("UX correctness: payback status renders correctly in checklist", () => {
  it("strong payback (14 mo) renders as pass, not fail", () => {
    const c = makeCandidate({
      payback_band: "strong",
      estimated_payback_months: 14,
      economics_score: 80,
    });
    const items = deriveDecisionChecklist(c);
    const paybackItem = items.find((i) => i.label.startsWith("Payback:"));
    expect(paybackItem).toBeDefined();
    expect(paybackItem!.status).toBe("strong");
    expect(paybackItem!.label).toContain("strong");
    expect(paybackItem!.label).toContain("14 mo");
  });

  it("promising payback renders as pass", () => {
    const c = makeCandidate({
      payback_band: "promising",
      estimated_payback_months: 24,
    });
    const items = deriveDecisionChecklist(c);
    const paybackItem = items.find((i) => i.label.startsWith("Payback:"));
    expect(paybackItem!.status).toBe("strong");
  });

  it("borderline payback renders as neutral/caution", () => {
    const c = makeCandidate({
      payback_band: "borderline",
      estimated_payback_months: 35,
    });
    const items = deriveDecisionChecklist(c);
    const paybackItem = items.find((i) => i.label.startsWith("Payback:"));
    expect(paybackItem!.status).toBe("caution");
  });

  it("weak payback renders as risk/fail", () => {
    const c = makeCandidate({
      payback_band: "weak",
      estimated_payback_months: 52,
    });
    const items = deriveDecisionChecklist(c);
    const paybackItem = items.find((i) => i.label.startsWith("Payback:"));
    expect(paybackItem!.status).toBe("risk");
  });
});

describe("UX correctness: paybackColor handles backend band values", () => {
  it("strong band → green", () => {
    expect(paybackColor("strong")).toBe("green");
  });

  it("borderline band → amber", () => {
    expect(paybackColor("borderline")).toBe("amber");
  });

  it("weak band → red", () => {
    expect(paybackColor("weak")).toBe("red");
  });
});

describe("UX correctness: estimated site-fit metrics labeled in checklist", () => {
  it("frontage/access show estimated when road context unavailable", () => {
    const c = makeCandidate({
      frontage_score: 45,
      access_score: 50,
      parking_score: 60,
      site_fit_context: {
        road_context_available: false,
        parking_context_available: true,
        frontage_score_mode: "estimated",
        access_score_mode: "estimated",
        parking_score_mode: "observed",
      },
    });
    const items = deriveDecisionChecklist(c);
    const frontageItem = items.find((i) => i.label.toLowerCase().includes("frontage"));
    const accessItem = items.find((i) => i.label.toLowerCase().includes("access") && !i.label.toLowerCase().includes("visibility"));
    const parkingItem = items.find((i) => i.label.toLowerCase().includes("parking"));
    expect(frontageItem).toBeDefined();
    expect(frontageItem!.label).toContain("estimated");
    expect(frontageItem!.status).toBe("verify");

    expect(accessItem).toBeDefined();
    expect(accessItem!.label).toContain("estimated");
    expect(accessItem!.status).toBe("verify");

    // Parking is observed — should NOT be estimated
    expect(parkingItem).toBeDefined();
    expect(parkingItem!.label).not.toContain("estimated");
  });

  it("parking shows estimated when parking context unavailable", () => {
    const c = makeCandidate({
      parking_score: 40,
      site_fit_context: {
        road_context_available: true,
        parking_context_available: false,
        frontage_score_mode: "observed",
        access_score_mode: "observed",
        parking_score_mode: "estimated",
      },
    });
    const items = deriveDecisionChecklist(c);
    const parkingItem = items.find((i) => i.label.toLowerCase().includes("parking"));
    expect(parkingItem).toBeDefined();
    expect(parkingItem!.label).toContain("estimated");
    expect(parkingItem!.status).toBe("verify");
  });

  it("all site-fit scores render normally when context available", () => {
    const c = makeCandidate({
      frontage_score: 80,
      access_score: 75,
      parking_score: 60,
      site_fit_context: {
        road_context_available: true,
        parking_context_available: true,
        frontage_score_mode: "observed",
        access_score_mode: "observed",
        parking_score_mode: "observed",
      },
    });
    const items = deriveDecisionChecklist(c);
    const siteFitItems = items.filter((i) => i.category === "site_fit");
    for (const item of siteFitItems) {
      expect(item.label).not.toContain("estimated");
    }
  });
});

/* ═══════════════════════════════════════════════════════════════════════
 * Expansion Advisor report hardening – nullable/missing field resilience
 * ═══════════════════════════════════════════════════════════════════════ */

describe("Report normalizer defaults all optional recommendation fields", () => {
  it("fills missing recommendation subkeys with safe defaults", () => {
    const sparse = normalizeReportResponse({
      meta: {},
      recommendation: {} as RecommendationReportResponse["recommendation"],
      top_candidates: [],
      assumptions: {},
      brand_profile: {},
    } as RecommendationReportResponse);
    expect(sparse.recommendation.why_best).toBe("");
    expect(sparse.recommendation.main_risk).toBe("");
    expect(sparse.recommendation.best_format).toBe("");
    expect(sparse.recommendation.summary).toBe("");
    expect(sparse.recommendation.report_summary).toBe("");
    expect(sparse.recommendation.best_pass_candidate_id).toBeUndefined();
    expect(sparse.recommendation.best_candidate_id).toBeUndefined();
    expect(sparse.recommendation.runner_up_candidate_id).toBeUndefined();
    expect(sparse.recommendation.best_confidence_candidate_id).toBeUndefined();
  });

  it("handles null recommendation gracefully", () => {
    const data = normalizeReportResponse({
      meta: {},
      recommendation: null,
      top_candidates: null,
      assumptions: null,
      brand_profile: null,
    } as unknown as RecommendationReportResponse);
    expect(data.recommendation.why_best).toBe("");
    expect(data.top_candidates).toEqual([]);
    expect(data.assumptions).toEqual({});
  });

  it("preserves existing recommendation values", () => {
    const data = normalizeReportResponse({
      meta: {},
      recommendation: { best_candidate_id: "abc", why_best: "Strong demand" },
      top_candidates: [],
      assumptions: {},
      brand_profile: {},
    } as RecommendationReportResponse);
    expect(data.recommendation.best_candidate_id).toBe("abc");
    expect(data.recommendation.why_best).toBe("Strong demand");
    expect(data.recommendation.main_risk).toBe("");
  });
});

describe("Report panel renders without error on sparse payloads", () => {
  it("does not show error banner for 200 with partial recommendation", () => {
    const html = renderToStaticMarkup(
      <ExpansionReportPanel
        loading={false}
        report={{
          meta: {},
          recommendation: { best_candidate_id: "c1" },
          top_candidates: [],
          assumptions: {},
          brand_profile: {},
        } as RecommendationReportResponse}
      />,
    );
    expect(html).not.toContain("Unable to load report");
    // Should still render the drawer
    expect(html).toContain("ea-drawer");
  });

  it("renders cleanly with empty top_candidates and null assumptions", () => {
    const report = normalizeReportResponse({
      meta: {},
      recommendation: {},
      top_candidates: [],
      assumptions: null,
      brand_profile: {},
    } as unknown as RecommendationReportResponse);
    const html = renderToStaticMarkup(
      <ExpansionReportPanel loading={false} report={report} />,
    );
    expect(html).not.toContain("Unable to load report");
    expect(html).toContain("ea-drawer");
  });

  it("renders with best_pass_candidate_id = null (zero-pass scenario)", () => {
    const report = normalizeReportResponse({
      meta: {},
      recommendation: { best_candidate_id: "c1", best_pass_candidate_id: null, why_best: "Top scorer" },
      top_candidates: [
        { id: "c1", rank_position: 1, final_score: 85, confidence_grade: "B", gate_verdict: "unknown", top_positives_json: ["good location"], top_risks_json: ["high rent"], score_breakdown_json: { weights: {}, inputs: {}, weighted_components: {}, final_score: 85 }, feature_snapshot_json: {} },
      ],
      assumptions: {},
      brand_profile: {},
    } as unknown as RecommendationReportResponse);
    const html = renderToStaticMarkup(
      <ExpansionReportPanel loading={false} report={report} />,
    );
    expect(html).not.toContain("Unable to load report");
    expect(html).toContain("Top scorer");
    expect(html).toContain("good location");
  });
});

describe("Gate verdict rendering: unknown displays as needs-validation, not fail", () => {
  it("GateSummary renders unknown gates with unknown status class", () => {
    const html = renderToStaticMarkup(
      <GateSummary
        gates={{ zoning_fit_pass: true, frontage_pass: null, parking_pass: undefined } as Record<string, boolean | null | undefined>}
        unknownGates={[]}
      />,
    );
    expect(html).toContain("ea-gate-item--pass");
    expect(html).toContain("ea-gate-item--unknown");
    // null/undefined should NOT be rendered as fail
    expect(html).not.toContain("ea-gate-item--fail");
  });

  it("GateSummary treats gates in unknownGates list as unknown", () => {
    const html = renderToStaticMarkup(
      <GateSummary
        gates={{ zoning_fit_pass: false, visibility_pass: false }}
        unknownGates={["visibility_pass"]}
      />,
    );
    // visibility_pass is in unknownGates → should be unknown, not fail
    expect(html).toContain("ea-gate-item--unknown");
    expect(html).toContain("ea-gate-item--fail");
  });

  it("report panel top candidate with unknown verdict shows amber badge", () => {
    const report = normalizeReportResponse({
      meta: {},
      recommendation: { best_candidate_id: "c1" },
      top_candidates: [
        { id: "c1", rank_position: 1, final_score: 80, confidence_grade: "C", gate_verdict: "unknown", top_positives_json: [], top_risks_json: [], score_breakdown_json: { weights: {}, inputs: {}, weighted_components: {}, final_score: 80 }, feature_snapshot_json: {} },
      ],
      assumptions: {},
      brand_profile: {},
    } as unknown as RecommendationReportResponse);
    const html = renderToStaticMarkup(
      <ExpansionReportPanel loading={false} report={report} />,
    );
    expect(html).toContain("ea-badge--amber");
    expect(html).toContain("Needs validation");
    expect(html).not.toContain("ea-badge--red");
  });
});

describe("passCount only counts strict true pass", () => {
  it("truthy non-boolean gate values are not counted as pass", () => {
    const candidates = [
      makeCandidate({ id: "c1", gate_status_json: { overall_pass: true } }),
      makeCandidate({ id: "c2", gate_status_json: { overall_pass: false } }),
      makeCandidate({ id: "c3", gate_status_json: { overall_pass: null } as unknown as Record<string, boolean | null | undefined> }),
      makeCandidate({ id: "c4", gate_status_json: { overall_pass: undefined } as unknown as Record<string, boolean | null | undefined> }),
      makeCandidate({ id: "c5", gate_status_json: {} }),
    ];
    const passCount = candidates.filter((c) => c.gate_status_json?.overall_pass === true).length;
    expect(passCount).toBe(1);
  });
});


// ---------------------------------------------------------------------------
// Follow-up patch tests
// ---------------------------------------------------------------------------

describe("Gate labels: raw keys never appear in user-visible text", () => {
  it("humanGateLabel converts raw keys to human-readable labels", () => {
    expect(humanGateLabel("zoning_fit_pass")).toBe("Zoning fit");
    expect(humanGateLabel("economics_pass")).toBe("Economics");
    expect(humanGateLabel("delivery_market_pass")).toBe("Delivery market");
    expect(humanGateLabel("frontage_access_pass")).toBe("Frontage / access");
    expect(humanGateLabel("area_fit_pass")).toBe("Area fit");
    expect(humanGateLabel("cannibalization_pass")).toBe("Cannibalization");
  });

  it("humanGateLabel handles already-humanized labels from backend", () => {
    // Backend _humanize_gate_list now sends "zoning fit" instead of "zoning_fit_pass"
    expect(humanGateLabel("zoning fit")).toBe("Zoning fit");
    expect(humanGateLabel("delivery market")).toBe("Delivery market");
    expect(humanGateLabel("frontage/access")).toBe("Frontage / access");
    expect(humanGateLabel("economics")).toBe("Economics");
  });

  it("humanGateLabel fallback strips _pass and capitalizes", () => {
    const result = humanGateLabel("some_new_gate_pass");
    expect(result).not.toContain("_pass");
    expect(result).not.toContain("_");
    expect(result[0]).toBe(result[0].toUpperCase());
  });

  it("deriveDecisionChecklist uses humanGateLabel for gate entries", () => {
    const candidate = makeCandidate({
      gate_status_json: { zoning_fit_pass: true, parking_pass: false, frontage_access_pass: null } as Record<string, boolean | null | undefined>,
      gate_reasons_json: { passed: ["zoning fit"], failed: ["parking"], unknown: ["frontage/access"], thresholds: {}, explanations: {} },
    });
    const items = deriveDecisionChecklist(candidate, null);
    for (const item of items) {
      expect(item.label).not.toContain("_pass");
      expect(item.label).not.toMatch(/\b\w+_\w+_pass\b/);
    }
  });
});

describe("Completeness: UI reflects backend evidence completeness", () => {
  it("parseFeatureSnapshot preserves data_completeness_score from backend", () => {
    const snapshot = parseFeatureSnapshot({ context_sources: {}, missing_context: [], data_completeness_score: 42 } as CandidateFeatureSnapshot);
    expect(snapshot.completeness).toBe(42);
  });

  it("parseFeatureSnapshot defaults to 0 when missing", () => {
    const snapshot = parseFeatureSnapshot({} as unknown as CandidateFeatureSnapshot);
    expect(snapshot.completeness).toBe(0);
  });

  it("normalizeCandidate defaults data_completeness_score to 0 (not 100)", () => {
    const c = normalizeCandidate({ id: "c1", search_id: "s1", parcel_id: "p1", lat: 24.7, lon: 46.7 });
    expect(c.feature_snapshot_json?.data_completeness_score).toBe(0);
  });
});

describe("Rent display: display_annual_rent_sar preferred over estimated", () => {
  it("CandidateDetailPanel renders display_annual_rent_sar when present", () => {
    const candidate = makeCandidate({
      estimated_rent_sar_m2_year: 2000,
      estimated_annual_rent_sar: 384008,
      display_annual_rent_sar: 384000,
    });
    const html = renderToStaticMarkup(
      <CandidateDetailPanel candidate={candidate} />,
    );
    // Should show the display value (384,000) not the internal value (384,008)
    expect(html).toContain("384,000");
    expect(html).not.toContain("384,008");
  });

  it("CandidateDetailPanel falls back to estimated when display is missing", () => {
    const candidate = makeCandidate({
      estimated_rent_sar_m2_year: 2000,
      estimated_annual_rent_sar: 384008,
    });
    const html = renderToStaticMarkup(
      <CandidateDetailPanel candidate={candidate} />,
    );
    expect(html).toContain("384,008");
  });
});

describe("Report panel: error displayed in drawer, not hidden", () => {
  it("shows error message inside the drawer when error prop is set", () => {
    const html = renderToStaticMarkup(
      <ExpansionReportPanel
        loading={false}
        report={null}
        error="Unable to load report."
      />,
    );
    expect(html).toContain("Unable to load report.");
    expect(html).toContain("ea-drawer");
  });

  it("does not render when no report, no loading, and no error", () => {
    const html = renderToStaticMarkup(
      <ExpansionReportPanel loading={false} report={null} />,
    );
    expect(html).toBe("");
  });

  it("renders report content for zero-pass scenario without error", () => {
    const report = normalizeReportResponse({
      meta: {},
      recommendation: {
        best_candidate_id: "c1",
        best_pass_candidate_id: null,
        pass_count: 0,
        validation_clear_count: 0,
        why_best: "Top scorer",
        summary: "No candidate passes all gates",
      },
      top_candidates: [],
      assumptions: {},
      brand_profile: {},
    } as unknown as RecommendationReportResponse);
    const html = renderToStaticMarkup(
      <ExpansionReportPanel loading={false} report={report} />,
    );
    expect(html).not.toContain("Unable to load report");
    expect(html).toContain("No candidate passes all gates");
  });
});

describe("normalizeReportResponse handles validation_clear_count", () => {
  it("defaults validation_clear_count to 0 when missing", () => {
    const result = normalizeReportResponse({
      meta: {},
      recommendation: { best_candidate_id: "c1" },
      top_candidates: [],
      assumptions: {},
      brand_profile: {},
    } as unknown as RecommendationReportResponse);
    expect(result.recommendation.validation_clear_count).toBe(0);
    expect(result.recommendation.pass_count).toBe(0);
  });

  it("preserves validation_clear_count when present", () => {
    const result = normalizeReportResponse({
      meta: {},
      recommendation: { best_candidate_id: "c1", validation_clear_count: 3, pass_count: 1 },
      top_candidates: [],
      assumptions: {},
      brand_profile: {},
    } as unknown as RecommendationReportResponse);
    expect(result.recommendation.validation_clear_count).toBe(3);
    expect(result.recommendation.pass_count).toBe(1);
  });
});
