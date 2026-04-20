import { describe, expect, it, beforeEach } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import "../../i18n";
import i18n from "../../i18n";
import en from "../../i18n/en.json";
import ExpansionMemoPanel from "./ExpansionMemoPanel";

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
