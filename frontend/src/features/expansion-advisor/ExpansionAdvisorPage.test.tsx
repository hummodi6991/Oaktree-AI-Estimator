import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import ExpansionComparePanel from "./ExpansionComparePanel";
import ExpansionResultsPanel from "./ExpansionResultsPanel";
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
        items={[{ id: "c1", search_id: "s1", parcel_id: "p1", lat: 24.7, lon: 46.7 }]}
        selectedCandidateId={null}
        shortlistIds={[]}
        compareIds={[]}
        onSelectCandidate={() => {}}
        onToggleShortlist={() => {}}
        onToggleCompare={() => {}}
      />,
    );
    expect(html).toContain("p1");
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
});
