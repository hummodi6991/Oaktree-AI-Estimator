import { describe, expect, it, beforeEach } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import "../../i18n";
import i18n from "../../i18n";
import en from "../../i18n/en.json";
import ar from "../../i18n/ar.json";
import AdvisorySectionCards from "./AdvisorySectionCards";
import type { StructuredMemo } from "../../lib/api/expansionAdvisor";

beforeEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

function fullMemo(): StructuredMemo {
  return {
    headline_recommendation: "Recommend",
    ranking_explanation: "rx",
    key_evidence: [],
    risks: [],
    comparison: "c",
    bottom_line: "bl",
    property_overview: {
      summary: "180 m² unit on a primary artery, 24 m frontage, listed 64 days ago.",
      area_m2: 180,
      frontage_width_m: 24,
      street_type: "primary",
      parking_evidence: "shared",
      visibility_score: 82,
      listing_age_days: 64,
      vacancy_status: "vacant",
    },
    financial_framing: {
      summary: "SAR 432,000/yr at the 28th percentile vs 14 district comparables.",
      thesis: "Rent is the spine of the case at this site.",
      annual_rent_sar: 432000,
      comparable_median_annual_rent_sar: 542000,
      rent_percentile_vs_comparables: 0.28,
      comparable_n: 14,
      comparable_scope: "district",
      spread_to_median_sar: -110000,
    },
    market_context: {
      summary: "41,000 walking-catchment with rising momentum.",
      demand_thesis: "Demand is observable.",
      population_reach: 41000,
      district_momentum: "rising",
      realized_demand_30d: 380,
      realized_demand_branches: 6,
      delivery_listing_count: 22,
    },
    competitive_landscape: {
      summary: "3 named chains within 500 m; rank 2 sits at 47th percentile.",
      saturation_thesis: "Three named chains operate within 500 m.",
      top_chains: [
        { display_name_en: "Peer Chain A", display_name_ar: null, branch_count: 2, nearest_distance_m: 180 },
      ],
      comparable_competitors: [
        { id: "c1", name: "Peer Chain A", score: 0.78 },
      ],
      next_candidate_summary: {
        rank: 2,
        candidate_id: "cand-rank-2",
        district: "Al Olaya",
        annual_rent_sar: 488000,
        rent_percentile_vs_comparables: 0.47,
        access_visibility_score: 71,
      },
    },
  };
}

describe("AdvisorySectionCards — PR #3 typed sections", () => {
  it("renders all four cards when every section is present", () => {
    const html = renderToStaticMarkup(<AdvisorySectionCards memo={fullMemo()} lang="en" />);
    expect(html).toContain(en.expansionAdvisor.advisorySection.propertyOverview.title);
    expect(html).toContain(en.expansionAdvisor.advisorySection.financialFraming.title);
    expect(html).toContain(en.expansionAdvisor.advisorySection.marketContext.title);
    expect(html).toContain(en.expansionAdvisor.advisorySection.competitiveLandscape.title);
    // Each card uses <details> and is collapsed by default (no "open" attr).
    const detailsTags = html.match(/<details[^>]*ea-memo-section[^>]*>/g) ?? [];
    expect(detailsTags.length).toBe(4);
    for (const tag of detailsTags) {
      expect(tag.includes(" open")).toBe(false);
    }
  });

  it("hides a card whose section is null", () => {
    const memo = fullMemo();
    memo.financial_framing = null;
    const html = renderToStaticMarkup(<AdvisorySectionCards memo={memo} lang="en" />);
    expect(html).not.toContain(en.expansionAdvisor.advisorySection.financialFraming.title);
    // The other three still render.
    expect(html).toContain(en.expansionAdvisor.advisorySection.propertyOverview.title);
    expect(html).toContain(en.expansionAdvisor.advisorySection.marketContext.title);
    expect(html).toContain(en.expansionAdvisor.advisorySection.competitiveLandscape.title);
  });

  it("renders nothing when all four sections are absent (legacy v4.2 memo)", () => {
    const memo: StructuredMemo = {
      headline_recommendation: "Recommend",
      ranking_explanation: "rx",
      key_evidence: [],
      risks: [],
      comparison: "c",
      bottom_line: "bl",
    };
    const html = renderToStaticMarkup(<AdvisorySectionCards memo={memo} lang="en" />);
    expect(html).toBe("");
  });

  it("body shows typed fields with non-null values only", () => {
    const memo = fullMemo();
    if (memo.property_overview) {
      memo.property_overview.frontage_width_m = null;
      memo.property_overview.street_type = null;
    }
    const html = renderToStaticMarkup(<AdvisorySectionCards memo={memo} lang="en" />);
    // area is still present; frontage / street type are hidden cleanly.
    expect(html).toContain(en.expansionAdvisor.areaLabel);
    expect(html).not.toContain(en.expansionAdvisor.advisorySection.frontage);
    expect(html).not.toContain(en.expansionAdvisor.advisorySection.streetType);
  });

  it("inherits RTL layout when lang='ar'", async () => {
    await i18n.changeLanguage("ar");
    try {
      const html = renderToStaticMarkup(<AdvisorySectionCards memo={fullMemo()} lang="ar" />);
      expect(html).toContain('dir="rtl"');
      // Arabic title for property overview is rendered.
      expect(html).toContain(ar.expansionAdvisor.advisorySection.propertyOverview.title);
    } finally {
      await i18n.changeLanguage("en");
    }
  });
});
