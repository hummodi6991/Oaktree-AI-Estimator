import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import ExpansionComparePanel from "./ExpansionComparePanel";
import ExpansionResultsPanel from "./ExpansionResultsPanel";
import ExpansionReportPanel from "./ExpansionReportPanel";
import ExpansionMemoPanel from "./ExpansionMemoPanel";
import en from "../../i18n/en.json";
import {
  getCompareRows,
  restoreSavedUiState,
  shouldLoadMemoFromMapSelection,
} from "./ExpansionAdvisorPage";

describe("Expansion advisor UI behavior", () => {
  it("renders candidate cards", () => {
    const html = renderToStaticMarkup(
      <ExpansionResultsPanel
        items={[{ id: "c1", search_id: "s1", parcel_id: "p1", lat: 24.7, lon: 46.7, brand_fit_score: 75, provider_density_score: 60, provider_whitespace_score: 55, confidence_grade: "A", demand_thesis: "Demand is strong", cost_thesis: "Cost is manageable", gate_status_json: { overall_pass: true }, comparable_competitors_json: [{ id: "r1", name: "Comp", distance_m: 120 }] }]}
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
    expect(html).toContain("Cost is manageable");
  });

  it("compare button disables for <2 selections", () => {
    const html = renderToStaticMarkup(<ExpansionComparePanel compareIds={["c1"]} onCompare={() => {}} />);
    expect(html).toContain("disabled");
  });

  it("old Restaurant Finder no longer primary nav label", () => {
    expect(en.app.modeExpansion).toBe("Expansion Advisor");
  });

  it("saved study restore returns search id and ui state compare ids", () => {
    const restored = restoreSavedUiState({
      search_id: "search-9",
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

  it("compare response rows are available for rendering", () => {
    const rows = getCompareRows({
      items: [
        {
          candidate_id: "c1",
          final_score: 81,
          economics_score: 72,
          estimated_payback_months: 22,
          payback_band: "promising",
          brand_fit_score: 77,
          provider_density_score: 69,
        },
      ],
    });
    expect(rows[0].candidate_id).toBe("c1");
    expect(rows[0].economics_score).toBe(72);
  });

  it("map/list shared selection path requests memo when ids differ", () => {
    expect(shouldLoadMemoFromMapSelection("c7", "c3")).toBe(true);
    expect(shouldLoadMemoFromMapSelection("c7", "c7")).toBe(false);
  });

  it("report panel renders recommendation summary", () => {
    const html = renderToStaticMarkup(<ExpansionReportPanel report={{ recommendation: { best_candidate_id: "c1", runner_up_candidate_id: "c2", best_pass_candidate_id: "c1", best_confidence_candidate_id: "c2", report_summary: "summary" }, top_candidates: [{ id: "c1", final_score: 90, confidence_grade: "A", gate_status_json: { overall_pass: true } }] }} />);
    expect(html).toContain("c1");
    expect(html).toContain("summary");
    expect(html).toContain("c2");
  });
  it("memo panel renders comparable competitors text", () => {
    const html = renderToStaticMarkup(<ExpansionMemoPanel loading={false} memo={{ recommendation: { verdict: "go", headline: "GO" }, candidate: { comparable_competitors: [{ id: "r1", name: "Comp", distance_m: 100 }], key_strengths: [], key_risks: [], gate_status: { overall_pass: true } }, market_research: {} }} />);
    expect(html).toContain("Comp");
  });

});
