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
import { normalizeCandidate } from "../../lib/api/expansionAdvisor";
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
  buildFinalistTiles,
  deriveDecisionChecklist,
  buildCopySummary,
  formatCopySummaryText,
  findRunnerUp,
} from "./studyAdapters";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";

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
    expect(html).toContain("zoning");
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
    expect(reportHtml).toContain("c1");
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
    expect(summary.nextValidation).toBe("access");
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
      bestCandidate: "#1 Olaya",
      topReason: "Strong demand",
      mainRisk: "High rent",
      bestFormat: "QSR",
      nextValidation: "Site visit",
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
