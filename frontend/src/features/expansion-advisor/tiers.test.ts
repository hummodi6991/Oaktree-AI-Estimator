import { describe, expect, it } from "vitest";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";
import {
  classifyCandidateTier,
  groupCandidatesByTier,
  PREMIER_MIN_SCORE,
  EXPLORATORY_MAX_SCORE,
} from "./tiers";

function makeCandidate(overrides: Partial<ExpansionCandidate> = {}): ExpansionCandidate {
  return {
    id: "c1",
    search_id: "s1",
    parcel_id: "p1",
    lat: 24.7,
    lon: 46.7,
    ...overrides,
  };
}

describe("classifyCandidateTier", () => {
  it("Premier — grade A, pass=true, score >= threshold", () => {
    const c = makeCandidate({
      confidence_grade: "A",
      final_score: 80,
      gate_status_json: { overall_pass: true },
    });
    expect(classifyCandidateTier(c)).toBe("premier");
  });

  it("Premier exact threshold (score === PREMIER_MIN_SCORE)", () => {
    const c = makeCandidate({
      confidence_grade: "A",
      final_score: PREMIER_MIN_SCORE,
      gate_status_json: { overall_pass: true },
    });
    expect(classifyCandidateTier(c)).toBe("premier");
  });

  it("Exploratory wins when gate fails even if score is high (precedence)", () => {
    const c = makeCandidate({
      confidence_grade: "A",
      final_score: 82,
      gate_status_json: { overall_pass: false },
    });
    expect(classifyCandidateTier(c)).toBe("exploratory");
  });

  it("Exploratory when grade is D, regardless of score", () => {
    const c = makeCandidate({
      confidence_grade: "D",
      final_score: 90,
      gate_status_json: { overall_pass: true },
    });
    expect(classifyCandidateTier(c)).toBe("exploratory");
  });

  it("Exploratory when score < EXPLORATORY_MAX_SCORE", () => {
    const c = makeCandidate({
      confidence_grade: "B",
      final_score: EXPLORATORY_MAX_SCORE - 0.01,
      gate_status_json: { overall_pass: true },
    });
    expect(classifyCandidateTier(c)).toBe("exploratory");
  });

  it("Standard when grade A + pass=true but score is in the middle band", () => {
    const c = makeCandidate({
      confidence_grade: "A",
      final_score: 70,
      gate_status_json: { overall_pass: true },
    });
    expect(classifyCandidateTier(c)).toBe("standard");
  });

  it("Standard when all tier fields are null/undefined", () => {
    const c = makeCandidate({});
    expect(classifyCandidateTier(c)).toBe("standard");
  });

  it("Premier when overall_pass is null (unknown) with grade A + high score — unknown does not block", () => {
    // Aqar Tier 1 candidates structurally have overall_pass = null because
    // parking_pass is null (no parking ground truth for Aqar listings).
    // Premier treats null as "not a blocker" so Tier 1 candidates can
    // qualify. Explicit failure (overall_pass = false) still demotes to
    // Exploratory via the precedence rule.
    const c = makeCandidate({
      confidence_grade: "A",
      final_score: 80,
      gate_status_json: { overall_pass: null },
    });
    expect(classifyCandidateTier(c)).toBe("premier");
  });

  it("Premier with explicit overall_pass=true, grade A, high score — canonical path still works", () => {
    const c = makeCandidate({
      confidence_grade: "A",
      final_score: 80,
      gate_status_json: { overall_pass: true },
    });
    expect(classifyCandidateTier(c)).toBe("premier");
  });

  it("Standard at the exploratory boundary (score === EXPLORATORY_MAX_SCORE)", () => {
    // Strict less-than — equal to the ceiling is NOT exploratory.
    const c = makeCandidate({
      confidence_grade: "B",
      final_score: EXPLORATORY_MAX_SCORE,
      gate_status_json: { overall_pass: true },
    });
    expect(classifyCandidateTier(c)).toBe("standard");
  });

  it("Standard when grade is B and score is between thresholds", () => {
    const c = makeCandidate({
      confidence_grade: "B",
      final_score: 65,
      gate_status_json: { overall_pass: true },
    });
    expect(classifyCandidateTier(c)).toBe("standard");
  });
});

describe("groupCandidatesByTier", () => {
  it("preserves backend ranker order within each tier", () => {
    const a = makeCandidate({
      id: "a",
      confidence_grade: "A",
      final_score: 80,
      gate_status_json: { overall_pass: true },
    });
    const b = makeCandidate({
      id: "b",
      confidence_grade: "B",
      final_score: 65,
      gate_status_json: { overall_pass: true },
    });
    const c = makeCandidate({
      id: "c",
      confidence_grade: "A",
      final_score: 78,
      gate_status_json: { overall_pass: true },
    });
    const d = makeCandidate({
      id: "d",
      final_score: 40,
    });
    const grouped = groupCandidatesByTier([a, b, c, d]);
    expect(grouped.premier.map((x) => x.id)).toEqual(["a", "c"]);
    expect(grouped.standard.map((x) => x.id)).toEqual(["b"]);
    expect(grouped.exploratory.map((x) => x.id)).toEqual(["d"]);
  });

  it("handles empty input", () => {
    expect(groupCandidatesByTier([])).toEqual({
      premier: [],
      standard: [],
      exploratory: [],
    });
  });
});
