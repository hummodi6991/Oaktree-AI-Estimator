import { describe, expect, it } from "vitest";
import { buildExpansionOverlayData } from "./expansionOverlay";

describe("buildExpansionOverlayData", () => {
  it("builds candidate and branch collections with selected/shortlisted flags", () => {
    const { candidateFc, branchFc } = buildExpansionOverlayData(
      [
        { id: "a", lat: 24.7, lon: 46.7 } as any,
        { id: "b", lat: 24.8, lon: 46.8 } as any,
      ],
      "b",
      ["a"],
      [{ lat: 24.6, lon: 46.6 }],
    );

    expect(candidateFc.features).toHaveLength(2);
    expect(candidateFc.features[0].properties).toMatchObject({ candidate_id: "a", selected: false, shortlisted: true });
    expect(candidateFc.features[1].properties).toMatchObject({ candidate_id: "b", selected: true, shortlisted: false });
    expect(branchFc.features).toHaveLength(1);
  });

  it("returns empty collections when inputs are empty or invalid", () => {
    const { candidateFc, branchFc } = buildExpansionOverlayData(
      [{ id: "bad", lat: Number.NaN, lon: 46.7 } as any],
      null,
      [],
      [{ lat: Number.POSITIVE_INFINITY, lon: 46.6 }],
    );

    expect(candidateFc.features).toEqual([]);
    expect(branchFc.features).toEqual([]);
  });
});
