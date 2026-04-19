import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import "../../i18n";
import i18n from "../../i18n";
import en from "../../i18n/en.json";
import ar from "../../i18n/ar.json";
import DecisionLogicCard from "./DecisionLogicCard";
import type {
  CandidateGateReasons,
  CandidateScoreBreakdown,
  RerankReason,
  RerankStatus,
} from "../../lib/api/expansionAdvisor";

/* ─── Fixtures ──────────────────────────────────────────────────────────── */

// Production shape: parking unknown, 7 passed, 0 failed, flag_off,
// final_rank == deterministic_rank == 1.
function productionGateReasons(): CandidateGateReasons {
  return {
    passed: [
      "zoning_fit_pass",
      "area_fit_pass",
      "frontage_access_pass",
      "district_pass",
      "cannibalization_pass",
      "delivery_market_pass",
      "economics_pass",
    ],
    failed: [],
    unknown: ["parking_pass"],
    thresholds: {},
    explanations: {
      zoning_fit_pass: "Zoning fit compares parcel land-use compatibility against threshold.",
      area_fit_pass: "Area fit checks candidate area against requested branch range.",
      frontage_access_pass: "Frontage/access gate depends on road context and road-adjacent signals.",
      parking_pass: "Parking context is not available for Aqar listings — cannot evaluate.",
      district_pass: "District gate fails only for explicitly excluded districts.",
      cannibalization_pass: "Cannibalization gate checks minimum spacing from existing branches.",
      delivery_market_pass: "Delivery-market gate auto-passes for non-delivery channels.",
      economics_pass: "Economics gate requires minimum economics score.",
    },
  };
}

function fullBreakdown(overrides?: Partial<CandidateScoreBreakdown>): CandidateScoreBreakdown {
  // 9-component fixture; weighted_components sum to final_score (78.05).
  const weighted_components: Record<string, number> = {
    occupancy_economics: 22.5,
    listing_quality: 8.25,
    brand_fit: 9.35,
    landlord_signal: 5.6,
    competition_whitespace: 8.0,
    demand_potential: 7.0,
    access_visibility: 7.5,
    delivery_demand: 4.0,
    confidence: 5.85,
  };
  const final_score = Object.values(weighted_components).reduce((a, b) => a + b, 0);
  return {
    weights: {
      occupancy_economics: 30,
      listing_quality: 11,
      brand_fit: 11,
      landlord_signal: 8,
      competition_whitespace: 10,
      demand_potential: 10,
      access_visibility: 10,
      delivery_demand: 5,
      confidence: 5,
    },
    inputs: {},
    weighted_components,
    final_score,
    display_score: final_score,
    ...overrides,
  } as CandidateScoreBreakdown;
}

function mixedGateReasons(): CandidateGateReasons {
  return {
    passed: ["zoning_fit_pass", "district_pass"],
    failed: ["economics_pass"],
    unknown: ["parking_pass"],
    thresholds: {},
    explanations: {
      zoning_fit_pass: "Zoning fit OK.",
      district_pass: "Not excluded.",
      economics_pass: "Below economics threshold.",
      parking_pass: "Parking context unavailable.",
    },
  };
}

function rerankReasonFull(overrides?: Partial<RerankReason>): RerankReason {
  return {
    summary: "stronger delivery signal",
    positives_cited: ["dense delivery grid", "rent below ceiling"],
    negatives_cited: ["narrow frontage"],
    comparison_to_displaced_candidate: "displaced site has weaker landlord responsiveness",
    ...overrides,
  };
}

beforeEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

afterEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

/* ─── 1. Three subsections present ──────────────────────────────────────── */

describe("DecisionLogicCard structure", () => {
  it("renders all three subsections", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        scoreBreakdown={fullBreakdown()}
        deterministicRank={1}
        finalRank={1}
        rerankStatus={"flag_off" as RerankStatus}
      />,
    );
    expect(html).toContain("ea-decision-logic__subsection--gates");
    expect(html).toContain("ea-decision-logic__subsection--contributions");
    expect(html).toContain("ea-decision-logic__subsection--ranking");
  });
});

/* ─── 2. Failed → Unknown → Passed bucket ordering ──────────────────────── */

describe("DecisionLogicCard gates bucket ordering", () => {
  it("renders failed bucket before unknown bucket before passed bucket", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={{
          passed: ["zoning_fit_pass", "district_pass"],
          failed: ["economics_pass"],
          unknown: ["parking_pass"],
          thresholds: {},
          explanations: {},
        }}
        deterministicRank={1}
        finalRank={1}
      />,
    );
    const failIdx = html.indexOf("ea-decision-logic__bucket--fail");
    const unknownIdx = html.indexOf("ea-decision-logic__bucket--unknown");
    const passIdx = html.indexOf("ea-decision-logic__bucket--pass");
    expect(failIdx).toBeGreaterThan(-1);
    expect(unknownIdx).toBeGreaterThan(-1);
    expect(passIdx).toBeGreaterThan(-1);
    expect(failIdx).toBeLessThan(unknownIdx);
    expect(unknownIdx).toBeLessThan(passIdx);
  });

  it("renders a failed gate earlier in the HTML than unknown, and unknown earlier than passed", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={mixedGateReasons()}
        deterministicRank={1}
        finalRank={1}
      />,
    );
    // Gate names (title-cased). "Economics" → failed; "Parking" → unknown;
    // "Zoning Fit" / "District" → passed.
    const econIdx = html.indexOf("Economics");
    const parkingIdx = html.indexOf("Parking");
    const zoningIdx = html.indexOf("Zoning Fit");
    expect(econIdx).toBeGreaterThan(-1);
    expect(parkingIdx).toBeGreaterThan(-1);
    expect(zoningIdx).toBeGreaterThan(-1);
    expect(econIdx).toBeLessThan(parkingIdx);
    expect(parkingIdx).toBeLessThan(zoningIdx);
  });
});

/* ─── 3. Empty bucket headers suppressed ────────────────────────────────── */

describe("DecisionLogicCard empty buckets", () => {
  it("omits the Failed header when failed[] is empty (production fixture)", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        deterministicRank={1}
        finalRank={1}
      />,
    );
    expect(html).not.toContain("ea-decision-logic__bucket--fail");
  });

  it("omits the Unknown header when unknown[] is empty", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={{
          passed: ["zoning_fit_pass"],
          failed: ["economics_pass"],
          unknown: [],
          thresholds: {},
          explanations: {},
        }}
        deterministicRank={1}
        finalRank={1}
      />,
    );
    expect(html).not.toContain("ea-decision-logic__bucket--unknown");
  });
});

/* ─── 4. Gate explanations render when present, omitted when empty ─────── */

describe("DecisionLogicCard gate explanations", () => {
  it("renders the explanation span when explanation string is non-empty", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        deterministicRank={1}
        finalRank={1}
      />,
    );
    expect(html).toContain("ea-decision-logic__gate-explanation");
    expect(html).toContain("Parking context is not available for Aqar listings — cannot evaluate.");
  });

  it("omits the explanation span for a gate with no explanation string", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={{
          passed: ["zoning_fit_pass"],
          failed: [],
          unknown: [],
          thresholds: {},
          // No explanation for zoning_fit_pass at all.
          explanations: {},
        }}
        deterministicRank={1}
        finalRank={1}
      />,
    );
    // Gate name still present but no explanation wrapper.
    expect(html).toContain("Zoning Fit");
    expect(html).not.toContain("ea-decision-logic__gate-explanation");
  });
});

/* ─── 5. Score contributions: 9 segments + legend totals final_score ───── */

describe("DecisionLogicCard score contributions", () => {
  it("renders 9 bar segments and 9 legend items, with summed weighted_points matching final_score", () => {
    const breakdown = fullBreakdown();
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        scoreBreakdown={breakdown}
        deterministicRank={1}
        finalRank={1}
      />,
    );
    const segMatches = html.match(/ea-decision-logic__bar-segment(?!--| ea-decision-logic__legend-swatch)/g) ?? [];
    // Count the actual bar segments by matching the data-component attribute.
    const dataComponentMatches = html.match(/data-component="/g) ?? [];
    // Each component renders one segment and one legend item, so data-component
    // should appear exactly 9 segments + 9 legend items = 18 times.
    expect(segMatches.length).toBeGreaterThanOrEqual(9);
    expect(dataComponentMatches.length).toBe(18);

    // Parse out the weighted_points from the legend's rendered text and sum.
    const legendValueMatches = html.match(/>(\d+\.\d)\s+pts</g) ?? [];
    expect(legendValueMatches.length).toBe(9);
    const sum = legendValueMatches
      .map((m) => parseFloat(m.replace(/[^\d.]/g, "")))
      .reduce((a, b) => a + b, 0);
    // Rounded-to-1-decimal legend values should sum within 0.5 of final_score.
    expect(Math.abs(sum - (breakdown.final_score as number))).toBeLessThan(0.5);
  });
});

/* ─── 6. Ranking decision: flag_off ─────────────────────────────────────── */

describe("DecisionLogicCard ranking decision — flag_off", () => {
  it("renders 'Deterministic #1' and 'Deterministic ranking only.'", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        deterministicRank={1}
        finalRank={1}
        rerankStatus={"flag_off" as RerankStatus}
      />,
    );
    expect(html).toContain("Deterministic #1");
    expect(html).toContain(en.expansionAdvisor.decisionLogicDeterministicOnly);
  });

  it("does not render rerank / LLM / accepted / review-window language when flag_off", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        deterministicRank={1}
        finalRank={1}
        rerankStatus={"flag_off" as RerankStatus}
      />,
    );
    expect(html).not.toContain("reranked");
    expect(html).not.toContain("LLM");
    expect(html).not.toContain("accepted by model review");
    expect(html).not.toContain("Outside LLM review window");
    expect(html).not.toContain("ea-decision-logic__reason-block");
    expect(html).not.toContain("ea-decision-logic__delta");
  });
});

/* ─── 7. Ranking decision: applied ──────────────────────────────────────── */

describe("DecisionLogicCard ranking decision — applied", () => {
  it("renders redirect arrow, reason summary, and delta indicator", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        deterministicRank={5}
        finalRank={3}
        rerankStatus={"applied" as RerankStatus}
        rerankDelta={-2}
        rerankReason={rerankReasonFull({ summary: "stronger delivery signal" })}
      />,
    );
    expect(html).toContain("Deterministic #5");
    expect(html).toContain("Final #3");
    expect(html).toContain("stronger delivery signal");
    // delta = -2 means the candidate moved UP (from 5 to 3), so the visual
    // marker is an up-arrow with magnitude 2.
    expect(html).toContain("ea-decision-logic__delta--up");
    expect(html).toContain("2</span>");
  });

  it("renders sub-labels for positives_cited, negatives_cited and comparison when each is non-empty", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        deterministicRank={5}
        finalRank={3}
        rerankStatus={"applied" as RerankStatus}
        rerankDelta={-2}
        rerankReason={rerankReasonFull()}
      />,
    );
    expect(html).toContain(en.expansionAdvisor.decisionLogicPositivesCited);
    expect(html).toContain("dense delivery grid, rent below ceiling");
    expect(html).toContain(en.expansionAdvisor.decisionLogicNegativesCited);
    expect(html).toContain("narrow frontage");
    expect(html).toContain(en.expansionAdvisor.decisionLogicComparisonLabel);
    expect(html).toContain("displaced site has weaker landlord responsiveness");
  });

  it("when rerank_reason.summary is empty, renders only the redirect line — no reason block", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        deterministicRank={5}
        finalRank={3}
        rerankStatus={"applied" as RerankStatus}
        rerankDelta={-2}
        rerankReason={rerankReasonFull({ summary: "" })}
      />,
    );
    expect(html).toContain("Deterministic #5");
    expect(html).toContain("Final #3");
    expect(html).not.toContain("ea-decision-logic__reason-block");
    expect(html).not.toContain(en.expansionAdvisor.decisionLogicPositivesCited);
  });
});

/* ─── 8. Ranking decision: outside_rerank_cap ───────────────────────────── */

describe("DecisionLogicCard ranking decision — outside_rerank_cap", () => {
  it("renders the outside-window note and no reason block", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        deterministicRank={12}
        finalRank={12}
        rerankStatus={"outside_rerank_cap" as RerankStatus}
      />,
    );
    expect(html).toContain("Deterministic #12");
    expect(html).toContain(en.expansionAdvisor.decisionLogicOutsideWindowNote);
    expect(html).not.toContain("ea-decision-logic__reason-block");
  });
});

/* ─── 9. Unknown / missing rerank_status falls back to flag_off ─────────── */

describe("DecisionLogicCard ranking decision — unknown status fallback", () => {
  it("renders the flag_off (deterministic-only) line for null status", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        deterministicRank={4}
        finalRank={4}
        rerankStatus={null}
      />,
    );
    expect(html).toContain("Deterministic #4");
    expect(html).toContain(en.expansionAdvisor.decisionLogicDeterministicOnly);
  });

  it("renders the flag_off line for an unexpected string value", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        deterministicRank={4}
        finalRank={4}
        rerankStatus={"something_new" as unknown as RerankStatus}
      />,
    );
    expect(html).toContain("Deterministic #4");
    expect(html).toContain(en.expansionAdvisor.decisionLogicDeterministicOnly);
    expect(html).not.toContain("Outside LLM review window");
    expect(html).not.toContain("accepted by model review");
  });
});

/* ─── 10. Default-open state of <details> elements ──────────────────────── */

describe("DecisionLogicCard default open states", () => {
  it("Gates subsection has the `open` attribute; contributions and ranking do not", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        scoreBreakdown={fullBreakdown()}
        deterministicRank={1}
        finalRank={1}
        rerankStatus={"flag_off" as RerankStatus}
      />,
    );
    const gatesMatch = html.match(
      /<details[^>]*ea-decision-logic__subsection--gates[^>]*>/,
    );
    const contribMatch = html.match(
      /<details[^>]*ea-decision-logic__subsection--contributions[^>]*>/,
    );
    const rankMatch = html.match(
      /<details[^>]*ea-decision-logic__subsection--ranking[^>]*>/,
    );
    expect(gatesMatch).not.toBeNull();
    expect(contribMatch).not.toBeNull();
    expect(rankMatch).not.toBeNull();
    expect(gatesMatch![0]).toMatch(/\bopen\b/);
    expect(contribMatch![0]).not.toMatch(/\bopen\b/);
    expect(rankMatch![0]).not.toMatch(/\bopen\b/);
  });
});

/* ─── 11. i18n-resolved text (no raw keys) ──────────────────────────────── */

describe("DecisionLogicCard i18n rendering", () => {
  it("renders resolved English strings, not raw keys", () => {
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        scoreBreakdown={fullBreakdown()}
        deterministicRank={1}
        finalRank={1}
        rerankStatus={"flag_off" as RerankStatus}
      />,
    );
    expect(html).toContain(en.expansionAdvisor.decisionLogicTitle);
    expect(html).toContain(en.expansionAdvisor.decisionLogicGates);
    expect(html).toContain(en.expansionAdvisor.decisionLogicContributions);
    expect(html).toContain(en.expansionAdvisor.decisionLogicRanking);
    expect(html).not.toContain("expansionAdvisor.decisionLogic");
  });

  it("renders resolved Arabic strings under the AR locale", async () => {
    await i18n.changeLanguage("ar");
    const html = renderToStaticMarkup(
      <DecisionLogicCard
        gateReasons={productionGateReasons()}
        scoreBreakdown={fullBreakdown()}
        deterministicRank={1}
        finalRank={1}
        rerankStatus={"flag_off" as RerankStatus}
      />,
    );
    expect(html).toContain(ar.expansionAdvisor.decisionLogicTitle);
    expect(html).toContain(ar.expansionAdvisor.decisionLogicGates);
    expect(html).toContain(ar.expansionAdvisor.decisionLogicContributions);
    expect(html).toContain(ar.expansionAdvisor.decisionLogicRanking);
    expect(html).toContain(ar.expansionAdvisor.decisionLogicDeterministicOnly);
  });
});
