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

  it("sets compared flag for candidates in compareExpansionCandidateIds", () => {
    const { candidateFc } = buildExpansionOverlayData(
      [
        { id: "a", lat: 24.7, lon: 46.7, rank_position: 1, district: "Olaya" } as any,
        { id: "b", lat: 24.8, lon: 46.8, rank_position: 2, district: "Malqa" } as any,
        { id: "c", lat: 24.9, lon: 46.9 } as any,
      ],
      null,
      [],
      [],
      ["a", "c"],
    );

    expect(candidateFc.features).toHaveLength(3);
    expect(candidateFc.features[0].properties).toMatchObject({ candidate_id: "a", compared: true, rank_position: 1, district: "Olaya" });
    expect(candidateFc.features[1].properties).toMatchObject({ candidate_id: "b", compared: false, rank_position: 2, district: "Malqa" });
    expect(candidateFc.features[2].properties).toMatchObject({ candidate_id: "c", compared: true, rank_position: 0, district: "" });
  });

  it("defaults compareExpansionCandidateIds to empty when omitted", () => {
    const { candidateFc } = buildExpansionOverlayData(
      [{ id: "x", lat: 24.7, lon: 46.7 } as any],
      null,
      [],
      [],
    );

    expect(candidateFc.features[0].properties.compared).toBe(false);
  });
});
