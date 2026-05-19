import { describe, expect, it, beforeEach, afterEach } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import "../../i18n";
import i18n from "../../i18n";
import ConfidenceBadge from "./ConfidenceBadge";
import ExpansionMemoPanel, { humanizeScoreLabel } from "./ExpansionMemoPanel";

beforeEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

afterEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

/* ─── §1 ConfidenceBadge ──────────────────────────────────────────────── */

describe("PR #4b — ConfidenceBadge i18n", () => {
  it("renders 'Data: A' with the English tooltip in EN", () => {
    const html = renderToStaticMarkup(<ConfidenceBadge grade="A" />);
    expect(html).toContain("Data: A");
    expect(html).toContain('title="Data confidence grade"');
  });

  it("renders 'البيانات: A' with the Arabic tooltip in AR", async () => {
    await i18n.changeLanguage("ar");
    const html = renderToStaticMarkup(<ConfidenceBadge grade="A" />);
    expect(html).toContain("البيانات: A");
    expect(html).toContain("درجة موثوقية البيانات");
    expect(html).not.toContain("Data confidence grade");
  });

  it("renders the locale-neutral em-dash fallback when grade is null", () => {
    const html = renderToStaticMarkup(<ConfidenceBadge grade={null} />);
    expect(html).toContain("Data: —");
  });

  it("emits the bare letter in compact mode (no prefix)", () => {
    const html = renderToStaticMarkup(<ConfidenceBadge grade="B" compact />);
    expect(html).not.toContain("Data:");
    expect(html).toMatch(/>B</);
  });
});

/* ─── §2 Verdict pill ─────────────────────────────────────────────────── */

function renderVerdictPanel(verdict: string) {
  return renderToStaticMarkup(
    <ExpansionMemoPanel
      loading={false}
      memo={{
        recommendation: { verdict, headline: `${verdict} headline` },
        candidate: {},
        market_research: {},
        brand_profile: {},
      } as any}
    />,
  );
}

describe("PR #4b — verdict pill i18n", () => {
  const cases: Array<[string, string, string, string]> = [
    ["go", "Go", "موصى به", "green"],
    ["consider", "Consider", "بتحفظ", "amber"],
    ["caution", "Caution", "تجنّب", "red"],
  ];

  for (const [token, enLabel, arLabel, color] of cases) {
    it(`renders '${enLabel}' in EN for token '${token}' with the ${color} badge class`, () => {
      const html = renderVerdictPanel(token);
      expect(html).toMatch(new RegExp(`ea-memo-verdict-badge[^"]*ea-badge--${color}`));
      expect(html).toMatch(new RegExp(`ea-memo-verdict-badge[^>]*>${enLabel}<`));
    });

    it(`renders '${arLabel}' in AR for token '${token}' with the ${color} badge class`, async () => {
      await i18n.changeLanguage("ar");
      const html = renderVerdictPanel(token);
      expect(html).toMatch(new RegExp(`ea-memo-verdict-badge[^"]*ea-badge--${color}`));
      expect(html).toContain(arLabel);
    });
  }

  it("falls back to the raw token for an unexpected verdict and does not crash", () => {
    const html = renderVerdictPanel("unknown");
    expect(html).toMatch(/ea-memo-verdict-badge[^>]*>unknown</);
    // Unknown token → not "go"/"consider" → red badge.
    expect(html).toMatch(/ea-memo-verdict-badge[^"]*ea-badge--red/);
  });
});

/* ─── §3 Score-breakdown labels ───────────────────────────────────────── */

describe("PR #4b — humanizeScoreLabel i18n", () => {
  it("returns the English label for a known key", () => {
    expect(humanizeScoreLabel("competition_whitespace", i18n.getFixedT("en"))).toBe(
      "Competitor Openness",
    );
  });

  it("returns the Arabic label for a known key", () => {
    expect(humanizeScoreLabel("competition_whitespace", i18n.getFixedT("ar"))).toBe(
      "انفتاح المنافسة",
    );
  });

  it("strips a trailing _score suffix before lookup", () => {
    expect(humanizeScoreLabel("parking_score", i18n.getFixedT("en"))).toBe("Parking");
  });

  it("falls back to a humanized form for an unknown key", () => {
    expect(humanizeScoreLabel("some_unknown_axis", i18n.getFixedT("en"))).toBe(
      "Some unknown axis",
    );
  });
});
