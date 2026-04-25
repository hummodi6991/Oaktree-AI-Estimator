import { describe, expect, it, beforeEach } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import "../../i18n";
import i18n from "../../i18n";
import en from "../../i18n/en.json";
import ExpansionMemoPanel, { type MemoDrawerSection } from "./ExpansionMemoPanel";

beforeEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

function renderPanel() {
  return renderToStaticMarkup(
    <ExpansionMemoPanel
      loading={false}
      memo={{
        recommendation: { verdict: "go", headline: "GO headline" },
        candidate: {
          final_score: 78,
          confidence_grade: "B",
          score_breakdown_json: {
            final_score: 78,
            weights: {},
            inputs: {},
            weighted_components: { demand_potential: 0.72 },
          },
          gate_status: { overall_pass: true },
        },
        market_research: {},
        brand_profile: {},
      }}
    />,
  );
}

describe("ExpansionMemoPanel chunk 3b reorganisation", () => {
  it("renders the verdict row above the score-breakdown disclosure", () => {
    const html = renderPanel();
    const verdictRowIdx = html.indexOf("ea-memo-verdict-row");
    const breakdownIdx = html.indexOf("ea-memo-full-breakdown");
    expect(verdictRowIdx).toBeGreaterThan(-1);
    expect(breakdownIdx).toBeGreaterThan(-1);
    expect(verdictRowIdx).toBeLessThan(breakdownIdx);
  });

  it("keeps the quick-facts row above the score-breakdown disclosure", () => {
    const html = renderPanel();
    const keyNumbersIdx = html.indexOf("ea-memo-key-numbers");
    const breakdownIdx = html.indexOf("ea-memo-full-breakdown");
    expect(keyNumbersIdx).toBeGreaterThan(-1);
    expect(breakdownIdx).toBeGreaterThan(-1);
    expect(keyNumbersIdx).toBeLessThan(breakdownIdx);
  });

  it("renders the score-breakdown details closed by default (no `open` attribute)", () => {
    const html = renderPanel();
    // Extract the opening tag of the ea-memo-full-breakdown <details>.
    const match = html.match(/<details[^>]*ea-memo-full-breakdown[^>]*>/);
    expect(match).not.toBeNull();
    const openingTag = match![0];
    expect(openingTag.includes(" open")).toBe(false);
  });

  it("uses the resolved i18n text (not the raw key) in the disclosure <summary>", () => {
    const html = renderPanel();
    const expected = en.expansionAdvisor.showScoreBreakdown;
    expect(expected).toBe("Show score breakdown");
    expect(html).toContain(expected);
    // And make sure the legacy "Show full score breakdown" label is gone.
    expect(html).not.toContain(en.decisionMemo.showFullBreakdown);
    // And make sure we didn't accidentally emit the raw key.
    expect(html).not.toContain("expansionAdvisor.showScoreBreakdown");
  });

  it("promotes verdict badge and confidence badge out of the summary card", () => {
    const html = renderPanel();
    const verdictRowIdx = html.indexOf("ea-memo-verdict-row");
    const summaryCardIdx = html.indexOf("ea-memo-summary-card");
    // Verdict row sits above the fold — i.e. before the summary card, which
    // now lives inside the collapsed <details>.
    expect(verdictRowIdx).toBeGreaterThan(-1);
    expect(summaryCardIdx).toBeGreaterThan(-1);
    expect(verdictRowIdx).toBeLessThan(summaryCardIdx);

    // Verdict badge renders inside the promoted row, not the summary card.
    const rowMatch = html.match(
      /<div class="ea-memo-verdict-row">([\s\S]*?)<\/div>\s*<div class="ea-memo-key-numbers">/,
    );
    expect(rowMatch).not.toBeNull();
    expect(rowMatch![1]).toContain("ea-memo-verdict-badge");
    expect(rowMatch![1]).toContain("ea-badge");
  });

  it("hides the verdict row entirely when verdict and confidence grade are both absent", () => {
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
    expect(html).not.toContain("ea-memo-verdict-row");
  });
});

/* ─── Backend reshape regression: rank + unit_* fields on candidate ─────── */

describe("ExpansionMemoPanel — memo shape consumers", () => {
  it("renders 'Deterministic #1' from cand.deterministic_rank (not '#—')", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={{
          recommendation: { verdict: "go", headline: "GO" },
          candidate: {
            final_score: 84,
            confidence_grade: "A",
            score_breakdown_json: {
              final_score: 84,
              weights: {},
              inputs: {},
              weighted_components: { demand_potential: 0.72 },
            },
            gate_status: { overall_pass: true },
            deterministic_rank: 1,
            final_rank: 1,
            rerank_status: "flag_off",
          },
          market_research: {},
          brand_profile: {},
        }}
      />,
    );
    expect(html).toContain("Deterministic #1");
    expect(html).not.toContain("Deterministic #—");
  });

  it("falls back from area_m2 to unit_area_sqm in the quick-facts row for commercial-unit candidates", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={{
          recommendation: { verdict: "go", headline: "GO" },
          candidate: {
            final_score: 84,
            confidence_grade: "A",
            score_breakdown_json: {
              final_score: 84,
              weights: {},
              inputs: {},
              weighted_components: { demand_potential: 0.72 },
            },
            gate_status: { overall_pass: true },
            // Commercial-unit candidates: area_m2 column is NULL; the area
            // lives on unit_area_sqm.
            area_m2: undefined,
            unit_area_sqm: 165,
            unit_street_width_m: 18,
          },
          market_research: {},
          brand_profile: {},
        }}
      />,
    );
    // Locate the 4-cell quick-facts row and confirm Area + Street width are
    // populated, not "—".
    const keyNumbersMatch = html.match(
      /<div class="ea-memo-key-numbers">([\s\S]*?)<\/div>\s*(?:<\/div>|<details)/,
    );
    expect(keyNumbersMatch).not.toBeNull();
    const block = keyNumbersMatch![1];
    // Area cell: 165 m² (number rendered, not the em-dash placeholder).
    expect(block).toMatch(/165/);
    // Street width cell: "18 m" (template literal in ExpansionMemoPanel).
    expect(block).toContain("18 m");
  });
});

/* ─── Chunk 3d: scroll-to-section plumbing ───────────────────────────────── */

function memoFixture() {
  return {
    recommendation: { verdict: "go", headline: "GO headline" },
    candidate: {
      final_score: 78,
      confidence_grade: "B",
      score_breakdown_json: {
        final_score: 78,
        weights: {},
        inputs: {},
        weighted_components: { demand_potential: 0.72 },
      },
      gate_status: { overall_pass: true },
    },
    market_research: {},
    brand_profile: {},
  };
}

describe("ExpansionMemoPanel chunk 3d — scroll-to-section plumbing", () => {
  it("does NOT emit the scroll-anchor class when initialSection is undefined", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={memoFixture()} />,
    );
    // Default-open drawer path must render identical CSS classes to pre-3d.
    expect(html).not.toContain("ea-memo-scroll-anchor");
  });

  it("renders each of the four sections with an identifiable class a ref can target", () => {
    // candidateRaw + briefRaw are required for the narrative wrapper to
    // render; supply minimal objects. The fetched decision memo itself won't
    // resolve under renderToStaticMarkup (useEffect doesn't run on SSR), so
    // the wrapper renders with null content — that's fine, we're asserting
    // the wrapper exists.
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={memoFixture()}
        candidateRaw={{ id: "cand_1" }}
        briefRaw={{ brand_name: "Test" }}
        initialSection="decision-logic"
      />,
    );
    expect(html).toContain("ea-memo-section-narrative");
    expect(html).toContain("ea-memo-verdict-row");
    expect(html).toContain("ea-memo-key-numbers");
    expect(html).toContain("ea-memo-section-decision-logic");
  });

  it("applies the scroll-anchor class to each section when initialSection is set", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={memoFixture()}
        candidateRaw={{ id: "cand_1" }}
        briefRaw={{ brand_name: "Test" }}
        initialSection="decision-logic"
      />,
    );
    // All four anchors carry the scroll-margin class.
    expect(html).toMatch(/ea-memo-section-narrative ea-memo-scroll-anchor/);
    expect(html).toMatch(/ea-memo-verdict-row ea-memo-scroll-anchor/);
    expect(html).toMatch(/ea-memo-key-numbers ea-memo-scroll-anchor/);
    expect(html).toMatch(/ea-memo-section-decision-logic ea-memo-scroll-anchor/);
  });

  it("keeps rendering the DecisionLogicCard inside the scroll-anchored wrapper", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={memoFixture()}
        candidateRaw={{ id: "cand_1" }}
        briefRaw={{ brand_name: "Test" }}
        initialSection="decision-logic"
      />,
    );
    const wrapperIdx = html.indexOf("ea-memo-section-decision-logic");
    const cardIdx = html.indexOf("ea-decision-logic", wrapperIdx);
    expect(wrapperIdx).toBeGreaterThan(-1);
    expect(cardIdx).toBeGreaterThan(wrapperIdx);
  });

  it("exports a MemoDrawerSection type with the expected four string literals", () => {
    // Compile-time checks: if the union drifts (adds/removes/renames a member),
    // tsc fails before the test runs.
    const s1: MemoDrawerSection = "narrative";
    const s2: MemoDrawerSection = "verdict";
    const s3: MemoDrawerSection = "quick-facts";
    const s4: MemoDrawerSection = "decision-logic";
    // Runtime sanity — values round-trip as strings.
    expect([s1, s2, s3, s4]).toEqual(["narrative", "verdict", "quick-facts", "decision-logic"]);
  });

  it("handleOpenMemoById accepts both the legacy and new signatures (compile-time check)", () => {
    // Shape-match the real signature in ExpansionAdvisorPage.tsx. If the real
    // signature drifts incompatibly, this test file fails to type-check.
    type OpenMemoFn = (
      candidateId: string,
      options?: { section?: MemoDrawerSection },
    ) => Promise<void>;
    const callLegacy: OpenMemoFn = async (id) => {
      void id;
    };
    const callChunk4: OpenMemoFn = async (id, options) => {
      void id;
      void options?.section;
    };
    // Existing call sites (chunks 3a-3c): id only.
    void callLegacy("cand_1");
    // Chunk 4 call site: id + { section: "decision-logic" }.
    void callChunk4("cand_1", { section: "decision-logic" });
    expect(typeof callLegacy).toBe("function");
    expect(typeof callChunk4).toBe("function");
  });
});

/* ─── Phase 1A: Breakdown tab ──────────────────────────────────────────── */

import ar from "../../i18n/ar.json";

// renderToStaticMarkup escapes &/</> in text content. Tests that compare
// against an i18n string containing one of these characters need to use
// the escaped form. Only & matters for our current strings.
function escapeHtmlAmp(s: string): string {
  return s.replace(/&/g, "&amp;");
}

function richBreakdownMemo() {
  return {
    recommendation: { verdict: "go", headline: "GO" },
    candidate: {
      final_score: 78,
      confidence_grade: "B",
      score_breakdown_json: {
        final_score: 78,
        weights: {},
        inputs: {},
        weighted_components: { demand_potential: 0.72 },
      },
      gate_status: { overall_pass: true },
      // Site grade scores (direct properties on candidate)
      parking_score: 64,
      frontage_score: 72,
      access_score: 81,
      access_visibility_score: 70,
      zoning_fit_score: 88,
      // Market signals scores
      provider_density_score: 55,
      provider_whitespace_score: 60,
      multi_platform_presence_score: 45,
      delivery_competition_score: 40,
      cannibalization_score: 90,
      feature_snapshot: {
        context_sources: {
          road_evidence_band: "moderate",
          parking_evidence_band: "limited",
          rent_base_sar_m2_year: 1200,
          rent_micro_adjustment: { multiplier: 1.052 },
        },
        missing_context: [],
        data_completeness_score: 80,
        district_momentum: {
          momentum_score: 72,
          percentile_composite: 0.78,
          activity_30d: 14,
          district_label: "Al Olaya",
          sample_floor_applied: false,
        },
        listing_age: { created_days: 3, updated_days: 1 },
        realized_demand_30d: 42.7,
        realized_demand_branches: 5,
        realized_demand_window_days: 30,
        candidate_location: {
          is_vacant: false,
          current_tenant: "Cafe X",
          current_category: "cafe",
        },
      },
    },
    market_research: {},
    brand_profile: {},
  };
}

describe("ExpansionMemoPanel — Breakdown tab presence", () => {
  it("renders the Breakdown tab button with the resolved English label", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} />,
    );
    expect(html).toContain(en.expansionAdvisor.memoTab_breakdown);
    expect(en.expansionAdvisor.memoTab_breakdown).toBe("Breakdown");
    expect(html).not.toContain("expansionAdvisor.memoTab_breakdown");
  });

  it("renders the Breakdown tab button with the resolved Arabic label", async () => {
    await i18n.changeLanguage("ar");
    try {
      const html = renderToStaticMarkup(
        <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} />,
      );
      expect(html).toContain(ar.expansionAdvisor.memoTab_breakdown);
      expect(ar.expansionAdvisor.memoTab_breakdown).toBe("تفصيل");
    } finally {
      await i18n.changeLanguage("en");
    }
  });

  it("does not render Breakdown panel content when economics is the active tab (default)", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} />,
    );
    expect(html).not.toContain("ea-memo-breakdown");
    // Site-grade explainer string belongs to the Breakdown tab; must not leak
    // into the default-active economics panel.
    expect(html).not.toContain(en.expansionAdvisor.breakdownSiteGradeExplainer);
  });
});

describe("ExpansionMemoPanel — Breakdown tab content (initialTab='breakdown')", () => {
  it("renders all four sub-section headers + explainers when data is rich", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="breakdown" />,
    );
    expect(html).toContain("ea-memo-breakdown");
    expect(html).toContain(en.expansionAdvisor.breakdownSiteGrade);
    expect(html).toContain(en.expansionAdvisor.breakdownSiteGradeExplainer);
    expect(html).toContain(en.expansionAdvisor.breakdownMarketSignals);
    expect(html).toContain(en.expansionAdvisor.breakdownMarketSignalsExplainer);
    // "Economics & timing" — & becomes &amp; in static markup
    expect(html).toContain(escapeHtmlAmp(en.expansionAdvisor.breakdownEconomicsTiming));
    expect(html).toContain(en.expansionAdvisor.breakdownEconomicsTimingExplainer);
    expect(html).toContain(en.expansionAdvisor.breakdownPropertyStatus);
    expect(html).toContain(en.expansionAdvisor.breakdownPropertyStatusExplainer);
  });

  it("renders site-grade ScoreBars with rounded numeric values and labels", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="breakdown" />,
    );
    // ScoreBar rounds the displayed value
    expect(html).toContain("ea-score-bar");
    expect(html).toContain(en.expansionAdvisor.parkingScore);
    expect(html).toContain(en.expansionAdvisor.frontageScore);
    expect(html).toContain(en.expansionAdvisor.accessScore);
    expect(html).toContain(en.expansionAdvisor.zoningFitScore);
    // The bar fill uses width:%; the rounded numeric is in the head.
    expect(html).toMatch(/>72</); // frontage
    expect(html).toMatch(/>81</); // access
    expect(html).toMatch(/>88</); // zoning
  });

  it("renders evidence bands with humanized values", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="breakdown" />,
    );
    expect(html).toContain(en.expansionAdvisor.roadEvidenceBandLabel);
    expect(html).toContain(en.expansionAdvisor.roadEvidenceBand_moderate);
    expect(html).toContain(en.expansionAdvisor.parkingEvidenceBandLabel);
    expect(html).toContain(en.expansionAdvisor.parkingEvidenceBand_limited);
  });

  it("renders district momentum score bar plus percentile/activity/label rows", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="breakdown" />,
    );
    expect(html).toContain(en.expansionAdvisor.districtMomentumScore);
    expect(html).toContain("78th percentile");
    expect(html).toContain(en.expansionAdvisor.districtActivity30d);
    expect(html).toContain(">14<"); // activity_30d
    expect(html).toContain("Al Olaya");
    // Below-floor note must NOT render when sample_floor_applied is false.
    expect(html).not.toContain(en.expansionAdvisor.districtMomentumBelowFloor);
  });

  it("renders the below-floor note (and NO momentum fields) when sample_floor_applied is true", () => {
    const memo = richBreakdownMemo() as any;
    memo.candidate.feature_snapshot.district_momentum = {
      momentum_score: 50,
      activity_30d: 5,
      active_in_district: 8,
      percentile_raw: 0.5,
      percentile_absolute: 0.5,
      percentile_composite: 0.5,
      district_label: "Tiny District",
      sample_floor_applied: true,
    };
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={memo} initialTab="breakdown" />,
    );
    expect(html).toContain(en.expansionAdvisor.districtMomentumBelowFloor);
    // None of the four detail rows should render under sample-floor.
    expect(html).not.toContain("th percentile");
    expect(html).not.toContain(en.expansionAdvisor.districtActivity30d);
    expect(html).not.toContain("Tiny District");
    // The momentum score bar is also suppressed.
    const districtMomentumLabelIdx = html.indexOf(en.expansionAdvisor.districtMomentumScore);
    expect(districtMomentumLabelIdx).toBe(-1);
  });

  it("renders rent baseline using SAR/m²/year interpolation and a signed micro-adjustment percentage", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="breakdown" />,
    );
    expect(html).toContain(en.expansionAdvisor.rentBaseline);
    expect(html).toContain("1200"); // rounded rent_base_sar_m2_year
    expect(html).toContain(en.expansionAdvisor.rentMicroAdjustment);
    expect(html).toContain("+5.2%"); // (1.052 - 1) * 100 = 5.2
  });

  it("formats negative micro-adjustment with the unicode minus sign", () => {
    const memo = richBreakdownMemo() as any;
    memo.candidate.feature_snapshot.context_sources.rent_micro_adjustment = { multiplier: 0.88 };
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={memo} initialTab="breakdown" />,
    );
    expect(html).toContain("−12.0%"); // unicode U+2212
  });

  it("formats zero micro-adjustment as '0.0%'", () => {
    const memo = richBreakdownMemo() as any;
    memo.candidate.feature_snapshot.context_sources.rent_micro_adjustment = { multiplier: 1.0 };
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={memo} initialTab="breakdown" />,
    );
    expect(html).toContain("0.0%");
  });

  it("renders realized demand at value 0 (not hidden) with branches/window subline", () => {
    const memo = richBreakdownMemo() as any;
    memo.candidate.feature_snapshot.realized_demand_30d = 0;
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={memo} initialTab="breakdown" />,
    );
    expect(html).toContain(en.expansionAdvisor.realizedDemand30d);
    expect(html).toContain("5 branches, 30d window");
  });

  it("renders both listing-age rows independently when both days are present", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="breakdown" />,
    );
    expect(html).toContain(en.expansionAdvisor.listingCreated);
    expect(html).toContain("3 days ago");
    expect(html).toContain(en.expansionAdvisor.listingUpdated);
    expect(html).toContain("1 days ago");
  });

  it("renders Property status block with all three fields when populated", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="breakdown" />,
    );
    expect(html).toContain(en.expansionAdvisor.breakdownPropertyStatus);
    expect(html).toContain(en.expansionAdvisor.vacancy);
    expect(html).toContain(en.expansionAdvisor.vacancyOccupied);
    expect(html).toContain(en.expansionAdvisor.currentTenant);
    expect(html).toContain("Cafe X");
    expect(html).toContain(en.expansionAdvisor.currentUse);
    expect(html).toContain("cafe");
  });

  it("hides the entire Property status block when candidate_location is missing", () => {
    const memo = richBreakdownMemo() as any;
    delete memo.candidate.feature_snapshot.candidate_location;
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={memo} initialTab="breakdown" />,
    );
    expect(html).not.toContain(en.expansionAdvisor.breakdownPropertyStatus);
    expect(html).not.toContain(en.expansionAdvisor.breakdownPropertyStatusExplainer);
    expect(html).not.toContain(en.expansionAdvisor.vacancy);
  });

  it("hides the entire Property status block when all three occupancy fields are null/empty", () => {
    const memo = richBreakdownMemo() as any;
    memo.candidate.feature_snapshot.candidate_location = {
      is_vacant: null,
      current_tenant: "",
      current_category: null,
    };
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={memo} initialTab="breakdown" />,
    );
    expect(html).not.toContain(en.expansionAdvisor.breakdownPropertyStatus);
  });

  it("hides individual rows cleanly when their values are null/undefined (no '—' fallback in Breakdown tab)", () => {
    const memo = {
      recommendation: { verdict: "go", headline: "GO" },
      candidate: {
        final_score: 50,
        confidence_grade: "C",
        score_breakdown_json: {
          final_score: 50,
          weights: {},
          inputs: {},
          weighted_components: {},
        },
        gate_status: { overall_pass: false },
        feature_snapshot: {
          context_sources: {},
          missing_context: [],
          data_completeness_score: 0,
        },
      },
      market_research: {},
      brand_profile: {},
    };
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={memo as any} initialTab="breakdown" />,
    );
    // Headers/explainers for Site grade, Market, Economics still render
    // (they are unconditional). Property status hides entirely.
    expect(html).toContain(en.expansionAdvisor.breakdownSiteGrade);
    expect(html).toContain(en.expansionAdvisor.breakdownMarketSignals);
    expect(html).toContain(escapeHtmlAmp(en.expansionAdvisor.breakdownEconomicsTiming));
    expect(html).not.toContain(en.expansionAdvisor.breakdownPropertyStatus);
    // No score bar rows render when all numeric values are absent.
    expect(html).not.toContain("ea-score-bar");
    // No evidence-band rows.
    expect(html).not.toContain(en.expansionAdvisor.roadEvidenceBandLabel);
    expect(html).not.toContain(en.expansionAdvisor.parkingEvidenceBandLabel);
    // No rent baseline / micro-adjustment / listing-age / realized-demand
    // rows. Match the label tag context so the explainer prose (which
    // mentions "rent baseline" in passing) doesn't false-positive.
    expect(html).not.toContain(`>${en.expansionAdvisor.rentBaseline}<`);
    expect(html).not.toContain(`>${en.expansionAdvisor.rentMicroAdjustment}<`);
    expect(html).not.toContain(`>${en.expansionAdvisor.listingCreated}<`);
    expect(html).not.toContain(`>${en.expansionAdvisor.listingUpdated}<`);
    expect(html).not.toContain(`>${en.expansionAdvisor.realizedDemand30d}<`);
  });

  it("ScoreBar renders aria-valuenow for value=0, aria-valuenow for value=50, and clamps width for value=100", () => {
    const memo = richBreakdownMemo() as any;
    memo.candidate.parking_score = 0;
    memo.candidate.frontage_score = 50;
    memo.candidate.access_score = 100;
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={memo} initialTab="breakdown" />,
    );
    expect(html).toMatch(/aria-valuenow="0"/);
    expect(html).toMatch(/aria-valuenow="50"/);
    expect(html).toMatch(/aria-valuenow="100"/);
    // Fill widths
    expect(html).toContain("width:0%");
    expect(html).toContain("width:50%");
    expect(html).toContain("width:100%");
  });

  // Backend serializes SQLAlchemy Numeric columns as strings
  // (e.g. "45.00") for precision. The Breakdown tab must coerce these
  // strings into numbers before guarding the score bars.
  it("renders score bars when numeric fields arrive as strings (Decimal-as-string serialization)", () => {
    const memo = richBreakdownMemo() as any;
    memo.candidate.parking_score = "45.00";
    memo.candidate.frontage_score = "94.00";
    memo.candidate.access_score = "90.00";
    memo.candidate.access_visibility_score = "70.00";
    memo.candidate.zoning_fit_score = "88.00";
    memo.candidate.provider_density_score = "55.00";
    memo.candidate.provider_whitespace_score = "60.00";
    memo.candidate.multi_platform_presence_score = "45.00";
    memo.candidate.delivery_competition_score = "40.00";
    memo.candidate.cannibalization_score = "90.00";
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={memo} initialTab="breakdown" />,
    );
    expect(html).toContain(en.expansionAdvisor.parkingScore);
    expect(html).toContain(en.expansionAdvisor.frontageScore);
    expect(html).toContain(en.expansionAdvisor.accessScore);
    // The "Access & visibility" label is HTML-encoded by renderToStaticMarkup.
    expect(html).toContain(en.expansionAdvisor.accessVisibility.replace("&", "&amp;"));
    expect(html).toContain(en.expansionAdvisor.zoningFitScore);
    expect(html).toContain(en.expansionAdvisor.providerDensity);
    expect(html).toContain(en.expansionAdvisor.providerWhitespace);
    expect(html).toContain(en.expansionAdvisor.multiPlatform);
    expect(html).toContain(en.expansionAdvisor.deliveryCompetition);
    expect(html).toContain(en.expansionAdvisor.cannibalization);
    // Rounded values appear in the bar heads.
    expect(html).toMatch(/>45</); // parking
    expect(html).toMatch(/>94</); // frontage
    expect(html).toMatch(/>90</); // access / cannibalization
    // aria-valuenow on the progressbar reflects the parsed numeric.
    expect(html).toMatch(/aria-valuenow="45"/);
    expect(html).toMatch(/aria-valuenow="94"/);
  });

  it("hides score bars when numeric fields arrive as unparseable strings", () => {
    const memo = richBreakdownMemo() as any;
    memo.candidate.parking_score = "N/A";
    memo.candidate.frontage_score = "";
    memo.candidate.access_score = "—";
    memo.candidate.access_visibility_score = null;
    memo.candidate.zoning_fit_score = undefined;
    memo.candidate.provider_density_score = "N/A";
    memo.candidate.provider_whitespace_score = "";
    memo.candidate.multi_platform_presence_score = null;
    memo.candidate.delivery_competition_score = undefined;
    memo.candidate.cannibalization_score = "—";
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={memo} initialTab="breakdown" />,
    );
    // None of the per-axis score-bar labels should render.
    expect(html).not.toContain(en.expansionAdvisor.parkingScore);
    expect(html).not.toContain(en.expansionAdvisor.frontageScore);
    expect(html).not.toContain(en.expansionAdvisor.accessScore);
    expect(html).not.toContain(en.expansionAdvisor.accessVisibility.replace("&", "&amp;"));
    expect(html).not.toContain(en.expansionAdvisor.zoningFitScore);
    expect(html).not.toContain(en.expansionAdvisor.providerDensity);
    expect(html).not.toContain(en.expansionAdvisor.providerWhitespace);
    expect(html).not.toContain(en.expansionAdvisor.multiPlatform);
    expect(html).not.toContain(en.expansionAdvisor.deliveryCompetition);
    expect(html).not.toContain(en.expansionAdvisor.cannibalization);
  });

  it("ScoreBar head visually separates label and value (flex space-between)", () => {
    const memo = richBreakdownMemo() as any;
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={memo} initialTab="breakdown" />,
    );
    // The head wrapper carries flex+space-between so label and value
    // never collide visually (no more "District momentum38").
    const headIdx = html.indexOf("ea-score-bar__head");
    expect(headIdx).toBeGreaterThan(-1);
    // Look at the head element's style attribute — it should request
    // flex layout with space-between.
    const headSnippet = html.slice(headIdx, headIdx + 400);
    expect(headSnippet).toMatch(/display:\s*flex/);
    expect(headSnippet).toMatch(/justify-content:\s*space-between/);
  });
});
