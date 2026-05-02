import { describe, expect, it, beforeEach } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import "../../i18n";
import i18n from "../../i18n";
import en from "../../i18n/en.json";
import ExpansionMemoPanel, { type MemoDrawerSection } from "./ExpansionMemoPanel";
import {
  _seedDecisionMemoCacheForTest,
  _clearDecisionMemoCacheForTest,
} from "./DecisionMemoNarrative";
import type { StructuredMemo } from "../../lib/api/expansionAdvisor";

beforeEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
  _clearDecisionMemoCacheForTest();
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

describe("ExpansionMemoPanel decision-drawer tabs (Memo / Diagnostics)", () => {
  it("defaults to the Memo tab and renders verdict + property facts row", () => {
    const html = renderPanel();
    expect(html).toContain("ea-drawer-tabs__nav");
    // Memo tab is active by default
    expect(html).toMatch(/ea-drawer-tabs__tab ea-drawer-tabs__tab--active[^>]*>Memo</);
    // Verdict row renders on Memo tab
    expect(html).toContain("ea-memo-verdict-row");
  });

  it("does NOT render Diagnostics-only content on the default Memo tab", () => {
    const html = renderPanel();
    // The 5-sub-tab inner strip (Breakdown label) lives on Diagnostics
    expect(html).not.toContain(en.expansionAdvisor.memoTab_breakdown);
    // The score breakdown wrapper is gated on Diagnostics
    expect(html).not.toContain("ea-memo-full-breakdown");
    // The DecisionLogicCard is gated on Diagnostics
    expect(html).not.toContain("ea-memo-section-decision-logic");
    // The 4-stat strip is removed entirely
    expect(html).not.toContain("ea-memo-key-numbers");
  });

  it("renders score with 1 decimal next to the verdict chip on Memo tab", () => {
    const html = renderPanel();
    expect(html).toContain("ea-memo-verdict-score");
    // 78 → "78.0" with 1 decimal
    expect(html).toMatch(/ea-memo-verdict-score">78\.0</);
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

/* ─── PR #3: AdvisorySectionCards mount + graceful degradation ───────────── */

describe("ExpansionMemoPanel — PR #3 advisory cards", () => {
  function structuredMemoWithV5Sections(): StructuredMemo {
    return {
      headline_recommendation: "Recommend",
      ranking_explanation: "rx",
      key_evidence: [],
      risks: [],
      comparison: "c",
      bottom_line: "bl",
      property_overview: {
        summary: "180 m² unit on a primary artery.",
        area_m2: 180,
        frontage_width_m: 24,
        street_type: "primary",
        parking_evidence: "shared",
        visibility_score: 82,
        listing_age_days: 64,
        vacancy_status: "vacant",
      },
      financial_framing: {
        summary: "SAR 432,000/yr below median.",
        thesis: "Rent is the spine.",
        annual_rent_sar: 432000,
        comparable_median_annual_rent_sar: 542000,
        rent_percentile_vs_comparables: 0.28,
        comparable_n: 14,
        comparable_scope: "district",
        spread_to_median_sar: -110000,
      },
      market_context: {
        summary: "41,000 catchment with rising momentum.",
        demand_thesis: "Demand is observable.",
        population_reach: 41000,
        district_momentum: "rising",
        realized_demand_30d: 380,
        realized_demand_branches: 6,
        delivery_listing_count: 22,
      },
      competitive_landscape: {
        summary: "Three chains within 500 m.",
        saturation_thesis: "Saturated.",
        top_chains: [{ display_name_en: "Peer A", display_name_ar: null, branch_count: 2, nearest_distance_m: 180 }],
        comparable_competitors: [],
        next_candidate_summary: null,
      },
    } as StructuredMemo;
  }

  function memoFixtureBare() {
    return {
      recommendation: { verdict: "go", headline: "GO" },
      candidate: {
        final_score: 78,
        confidence_grade: "B",
        score_breakdown_json: {
          final_score: 78, weights: {}, inputs: {}, weighted_components: {},
        },
        gate_status: { overall_pass: true },
      },
      market_research: {},
      brand_profile: {},
    };
  }

  function seedFetchedMemo(candidateId: string, structured: StructuredMemo) {
    _seedDecisionMemoCacheForTest(candidateId, {
      memo: {
        headline: "GO",
        fit_summary: "",
        top_reasons_to_pursue: [],
        top_risks: [],
        recommended_next_action: "",
        rent_context: "",
      },
      memo_text: null,
      memo_json: structured,
    });
  }

  it("does NOT mount AdvisorySectionCards even when the fetched memo has v5 sections (cut by directive)", () => {
    seedFetchedMemo("cand_1", structuredMemoWithV5Sections());
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={memoFixtureBare() as any}
        candidateRaw={{ id: "cand_1" }}
        briefRaw={{ brand_name: "Test" }}
      />,
    );
    expect(html).not.toContain("ea-memo-advisory-cards");
  });

  it("renders no advisory <details> sections (cards removed)", () => {
    seedFetchedMemo("cand_1", structuredMemoWithV5Sections());
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={memoFixtureBare() as any}
        candidateRaw={{ id: "cand_1" }}
        briefRaw={{ brand_name: "Test" }}
      />,
    );
    const tags = html.match(/<details[^>]*ea-memo-section[^>]*>/g) ?? [];
    expect(tags.length).toBe(0);
  });

  it("does NOT render advisory cards when the fetched memo lacks v5 sections (graceful degradation)", () => {
    // Seed the cache with a structured memo that has narrative fields but no
    // v5 advisory sections. AdvisorySectionCards should return null.
    _seedDecisionMemoCacheForTest("cand_1", {
      memo: {
        headline: "GO",
        fit_summary: "",
        top_reasons_to_pursue: [],
        top_risks: [],
        recommended_next_action: "",
        rent_context: "",
      },
      memo_text: null,
      memo_json: {
        headline_recommendation: "Recommend",
        ranking_explanation: "",
        key_evidence: [],
        risks: [],
        comparison: "",
        bottom_line: "",
      } as StructuredMemo,
    });
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={memoFixtureBare() as any}
        candidateRaw={{ id: "cand_1" }}
        briefRaw={{ brand_name: "Test" }}
      />,
    );
    expect(html).not.toContain("ea-memo-advisory-cards");
  });

  it("does NOT render advisory cards when the fetched memo hasn't resolved (no cache hit)", () => {
    // No seed → DecisionMemoNarrative renders null under SSR (useEffect skipped).
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={memoFixtureBare() as any}
        candidateRaw={{ id: "cand_unfetched" }}
        briefRaw={{ brand_name: "Test" }}
      />,
    );
    expect(html).not.toContain("ea-memo-advisory-cards");
  });

  it("ignores cand.decision_memo_json (cards source from the POST /decision-memo response)", () => {
    // Regression: the previous mount read cand.decision_memo_json. Confirm it's
    // no longer the source — even when populated, no cache seed → no cards.
    const memo = memoFixtureBare() as any;
    memo.candidate.decision_memo_json = structuredMemoWithV5Sections();
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={memo}
        candidateRaw={{ id: "cand_2" }}
        briefRaw={{ brand_name: "Test" }}
      />,
    );
    expect(html).not.toContain("ea-memo-advisory-cards");
  });
});

/* ─── Backend reshape regression: rank + unit_* fields on candidate ─────── */

describe("ExpansionMemoPanel — memo shape consumers", () => {
  it("renders 'Deterministic #1' from cand.deterministic_rank when on Diagnostics tab", () => {
    // DecisionLogicCard moved to the Diagnostics tab; pass initialTab to flip
    // the drawer-tab selector into Diagnostics so the card renders.
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        initialTab="economics"
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

  it("falls back from area_m2 to unit_area_sqm in the property-facts row for commercial-unit candidates", () => {
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
            area_m2: undefined,
            unit_area_sqm: 165,
            unit_street_width_m: 18,
          },
          market_research: {},
          brand_profile: {},
        }}
      />,
    );
    // The new property-facts row carries area, frontage, rent, vacancy on the
    // Memo tab. Validate area falls back to unit_area_sqm and frontage shows.
    expect(html).toContain("ea-memo-property-facts");
    expect(html).toMatch(/165 m²/);
    // Frontage formatted as "{w} m frontage" via i18n.
    expect(html).toContain("18 m frontage");
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

  it("renders the Memo-tab section anchors when initialSection is set", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={memoFixture()}
        candidateRaw={{ id: "cand_1" }}
        briefRaw={{ brand_name: "Test" }}
        initialSection="narrative"
      />,
    );
    // narrative + verdict-row live on the default Memo tab.
    expect(html).toContain("ea-memo-section-narrative");
    expect(html).toContain("ea-memo-verdict-row");
  });

  it("renders the Diagnostics-tab anchor (decision-logic) when initialTab flips drawer to diagnostics", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={memoFixture()}
        candidateRaw={{ id: "cand_1" }}
        briefRaw={{ brand_name: "Test" }}
        initialSection="decision-logic"
        initialTab="economics"
      />,
    );
    expect(html).toContain("ea-memo-section-decision-logic");
  });

  it("applies the scroll-anchor class to Memo-tab sections when initialSection is set", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={memoFixture()}
        candidateRaw={{ id: "cand_1" }}
        briefRaw={{ brand_name: "Test" }}
        initialSection="narrative"
      />,
    );
    expect(html).toMatch(/ea-memo-section-narrative ea-memo-scroll-anchor/);
    expect(html).toMatch(/ea-memo-verdict-row ea-memo-scroll-anchor/);
  });

  it("keeps rendering the DecisionLogicCard inside the scroll-anchored wrapper on Diagnostics", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel
        loading={false}
        memo={memoFixture()}
        candidateRaw={{ id: "cand_1" }}
        briefRaw={{ brand_name: "Test" }}
        initialSection="decision-logic"
        initialTab="economics"
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
      comparable_competitors: [
        {
          id: "comp1",
          name: "بيرجر كنج",
          canonical_brand_id: "burger_king",
          display_name_en: "Burger King",
          display_name_ar: "بيرجر كنج",
          district: "Al Olaya",
          district_display: "Al Olaya",
          distance_m: 310,
        },
        {
          id: "comp2",
          name: "Local Diner",
          district: "Al Olaya",
          district_display: "Al Olaya",
          distance_m: 580,
        },
      ],
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
        brand_presence: {
          radius_m: 500,
          unique_brands: 4,
          total_branches: 14,
          top_chains: [
            { canonical_brand_id: "starbucks", display_name_en: "Starbucks", display_name_ar: "ستاربكس", branch_count: 8, nearest_distance_m: 120 },
            { canonical_brand_id: "kfc", display_name_en: "KFC", display_name_ar: "كنتاكي", branch_count: 3, nearest_distance_m: 240 },
            { canonical_brand_id: "burger_king", display_name_en: "Burger King", display_name_ar: "بيرجر كنج", branch_count: 2, nearest_distance_m: 310 },
            { canonical_brand_id: "dominos", display_name_en: "Domino's Pizza", display_name_ar: "دومينوز بيتزا", branch_count: 1, nearest_distance_m: 460 },
          ],
        },
      },
    },
    market_research: {},
    brand_profile: {},
  };
}

describe("ExpansionMemoPanel — Breakdown tab presence", () => {
  it("renders the Breakdown tab button with the resolved English label (Diagnostics drawer-tab)", () => {
    // Inner sub-tabs live on the Diagnostics drawer tab. Pass initialTab to
    // flip the drawer to Diagnostics so the inner-tab nav renders.
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="economics" />,
    );
    expect(html).toContain(en.expansionAdvisor.memoTab_breakdown);
    expect(en.expansionAdvisor.memoTab_breakdown).toBe("Breakdown");
    expect(html).not.toContain("expansionAdvisor.memoTab_breakdown");
  });

  it("renders the Breakdown tab button with the resolved Arabic label (Diagnostics drawer-tab)", async () => {
    await i18n.changeLanguage("ar");
    try {
      const html = renderToStaticMarkup(
        <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="economics" />,
      );
      expect(html).toContain(ar.expansionAdvisor.memoTab_breakdown);
      expect(ar.expansionAdvisor.memoTab_breakdown).toBe("تفصيل");
    } finally {
      await i18n.changeLanguage("en");
    }
  });

  it("does not render Breakdown panel content when economics is the active inner tab (default)", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="economics" />,
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

  it("renders the Brand Presence block with top chains and counts", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="breakdown" />,
    );
    expect(html).toContain(en.expansionAdvisor.breakdownBrandPresence);
    expect(html).toContain(en.expansionAdvisor.breakdownBrandPresenceExplainer);
    // Top chain (Starbucks, 8 branches) renders in EN
    expect(html).toContain("Starbucks");
    expect(html).toContain("(8)");
    // Branch count summary
    expect(html).toContain("14");
  });

  it("hides the Brand Presence block when top_chains is empty", () => {
    const memo = richBreakdownMemo() as any;
    memo.candidate.feature_snapshot.brand_presence = {
      radius_m: 500, unique_brands: 0, total_branches: 0, top_chains: [],
    };
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={memo} initialTab="breakdown" />,
    );
    expect(html).not.toContain(en.expansionAdvisor.breakdownBrandPresence);
  });
});

describe("ExpansionMemoPanel — Comparable Competitors locale rendering", () => {
  it("renders display_name_en in EN locale", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="market" lang="en" />,
    );
    // Burger King row: display_name_en = "Burger King" should be rendered
    expect(html).toContain("Burger King");
  });

  it("renders display_name_ar in AR locale", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="market" lang="ar" />,
    );
    expect(html).toContain("بيرجر كنج");
  });

  it("falls back to c.name when display_name_en/ar are absent", () => {
    const html = renderToStaticMarkup(
      <ExpansionMemoPanel loading={false} memo={richBreakdownMemo() as any} initialTab="market" lang="en" />,
    );
    // Local Diner row has no display_name_en — should fall through to c.name
    expect(html).toContain("Local Diner");
  });
});
