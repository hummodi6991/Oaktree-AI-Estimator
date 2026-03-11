import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import ExpansionComparePanel from "./ExpansionComparePanel";
import ExpansionResultsPanel from "./ExpansionResultsPanel";
import ExpansionReportPanel from "./ExpansionReportPanel";
import ExpansionMemoPanel from "./ExpansionMemoPanel";
import en from "../../i18n/en.json";
import {
  restoreSavedUiState,
  shouldLoadMemoFromMapSelection,
  getCompareRows,
} from "./ExpansionAdvisorPage";
import { normalizeCandidate } from "../../lib/api/expansionAdvisor";

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
    expect(html).toContain("p1");
    expect(html).toContain("Demand is strong");
    expect(html).toContain("Comp");
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
    const restored = restoreSavedUiState({
      id: "sv1",
      search_id: "search-9",
      title: "study",
      status: "draft",
      selected_candidate_ids: ["c1"],
      ui_state_json: { compare_ids: ["c2", "c3"], selected_candidate_id: "c3" },
      candidates: [
        { id: "c2", search_id: "s", parcel_id: "p2", lat: 24.7, lon: 46.7 },
        { id: "c3", search_id: "s", parcel_id: "p3", lat: 24.8, lon: 46.8 },
      ],
    });
    expect(restored.searchId).toBe("search-9");
    expect(restored.compareIds).toEqual(["c2", "c3"]);
    expect(restored.selectedCandidate?.id).toBe("c3");
  });

  it("compare response rows use candidate_id", () => {
    const rows = getCompareRows({
      items: [{ candidate_id: "c1", final_score: 81, economics_score: 72, estimated_payback_months: 22, payback_band: "promising", brand_fit_score: 77, provider_density_score: 69 }],
      summary: {},
    });
    expect(rows[0].candidate_id).toBe("c1");
    expect(rows[0].economics_score).toBe(72);
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
    expect(html).toContain("rent_growth");
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
});
