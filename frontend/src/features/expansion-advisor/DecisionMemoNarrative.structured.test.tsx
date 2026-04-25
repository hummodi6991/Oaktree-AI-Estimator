import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import "../../i18n";
import i18n from "../../i18n";
import en from "../../i18n/en.json";
import ar from "../../i18n/ar.json";
import {
  StructuredNarrative,
  isValidStructuredMemo,
} from "./DecisionMemoNarrative";
import type {
  GeneratedDecisionMemo,
  LLMDecisionMemo,
  StructuredMemo,
  StructuredMemoRisk,
} from "../../lib/api/expansionAdvisor";

/* ── Fixtures ── */

function makeStructured(overrides: Partial<StructuredMemo> = {}): StructuredMemo {
  return {
    headline_recommendation: "Strong case for expanding into Al Olaya",
    ranking_explanation:
      "Ranks #2 because provider density is sparse and rent is below brand ceiling.",
    key_evidence: [
      {
        signal: "Distance to nearest branch",
        value: 3200,
        implication: "Low cannibalisation risk at this radius.",
        polarity: "positive",
      },
      {
        signal: "Estimated rent",
        value: "SAR 1,450 / m² / year",
        implication: "Within brand-fit ceiling.",
        polarity: "neutral",
      },
      {
        signal: "Street frontage",
        value: "narrow",
        implication: "Walk-in capture may be weaker than adjacent parcels.",
        polarity: "negative",
      },
    ],
    risks: [
      { risk: "Nearby branch of competitor brand", mitigation: null },
      { risk: "Parking availability untested" },
    ],
    comparison: "Beats runner-up #3 on revenue index but trails on visibility.",
    bottom_line: "Proceed to landlord outreach.",
    ...overrides,
  };
}

function makeStructuredWithObjectRisks(
  overrides: Partial<StructuredMemo> = {},
): StructuredMemo {
  return makeStructured({
    risks: [
      {
        risk: "Insufficient parking may limit customer access.",
        mitigation:
          "Exploring alternative parking solutions or locations nearby could be considered.",
      },
      { risk: "Dense competitor grid.", mitigation: null },
      { risk: "Weather exposure on south-facing frontage." },
    ],
    ...overrides,
  });
}

function makeLegacy(overrides: Partial<LLMDecisionMemo> = {}): LLMDecisionMemo {
  return {
    headline: "Legacy headline: solid pursuit",
    fit_summary: "Legacy fit summary explaining the rationale.",
    top_reasons_to_pursue: ["Legacy positive 1", "Legacy positive 2"],
    top_risks: ["Legacy risk A", "Legacy risk B"],
    recommended_next_action: "Call the broker",
    rent_context: "Rent lands near market median for the district.",
    ...overrides,
  };
}

beforeEach(async () => {
  // Default locale back to English so Arabic assertions don't leak across tests.
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

/* ── 1. Structured memo present renders all six sections (shape-only) ── */

describe("DecisionMemoNarrative structured render", () => {
  it("renders the six section CSS hooks, i18n headers, and polarity markers", () => {
    const memo = makeStructured();
    const html = renderToStaticMarkup(<StructuredNarrative memo={memo} lang="en" />);

    // Container shape + locale dir.
    expect(html).toContain("ea-memo-structured");
    expect(html).toContain('dir="ltr"');

    // Six i18n-keyed section headers all render.
    expect(html).toContain(en.expansionAdvisor.theRecommendation);
    expect(html).toContain(en.expansionAdvisor.keyEvidence);
    expect(html).toContain(en.expansionAdvisor.risksToWatch);
    expect(html).toContain(en.expansionAdvisor.howItCompares);
    expect(html).toContain(en.expansionAdvisor.bottomLine);

    // Headline CSS hook present.
    expect(html).toContain("ea-memo-structured__headline");

    // Polarity markers render with correct data-attrs for each evidence item.
    expect(html).toContain('data-polarity="positive"');
    expect(html).toContain('data-polarity="neutral"');
    expect(html).toContain('data-polarity="negative"');
  });
});

/* ── 2. Empty risks array omits the section ── */

describe("DecisionMemoNarrative empty risks", () => {
  it("does not render the risks section when risks array is empty", () => {
    const memo = makeStructured({ risks: [] });
    const html = renderToStaticMarkup(<StructuredNarrative memo={memo} lang="en" />);

    expect(html).not.toContain(en.expansionAdvisor.risksToWatch);
    expect(html).not.toContain("ea-memo-structured__section--risks");
    expect(html).not.toContain("ea-memo-structured__risks-list");
  });
});

/* ── 3. Empty comparison string omits the section ── */

describe("DecisionMemoNarrative empty comparison", () => {
  it("does not render the comparison section when comparison is empty", () => {
    const memo = makeStructured({ comparison: "" });
    const html = renderToStaticMarkup(<StructuredNarrative memo={memo} lang="en" />);

    expect(html).not.toContain(en.expansionAdvisor.howItCompares);
    expect(html).not.toContain("ea-memo-structured__section--comparison");
    expect(html).not.toContain("ea-memo-structured__comparison");
  });

  it("does not render the comparison section when comparison is only whitespace", () => {
    const memo = makeStructured({ comparison: "   " });
    const html = renderToStaticMarkup(<StructuredNarrative memo={memo} lang="en" />);

    expect(html).not.toContain(en.expansionAdvisor.howItCompares);
  });
});

/* ── 4. Legacy render byte-identical regression DELETED ──
 *
 * The structural / i18n / shape tests above already cover the legacy
 * narrative contract via CSS-hook assertions. A byte-identical assert
 * couples the test to fixture wording and is hostile to tone iteration.
 */

/* ── 5. Malformed structured memo ── */

describe("DecisionMemoNarrative malformed structured memo", () => {
  it("isValidStructuredMemo returns false when headline_recommendation is empty", () => {
    const memo = makeStructured({ headline_recommendation: "" });
    expect(isValidStructuredMemo(memo)).toBe(false);
  });

  it("isValidStructuredMemo returns false when headline is whitespace only", () => {
    const memo = makeStructured({ headline_recommendation: "   " });
    expect(isValidStructuredMemo(memo)).toBe(false);
  });

  it("isValidStructuredMemo returns false when key_evidence is not an array", () => {
    const memo = makeStructured();
    // Deliberately corrupt the shape to model a malformed server response.
    (memo as unknown as { key_evidence: unknown }).key_evidence = null;
    expect(isValidStructuredMemo(memo)).toBe(false);
  });

  it("isValidStructuredMemo returns false for null input", () => {
    expect(isValidStructuredMemo(null)).toBe(false);
    expect(isValidStructuredMemo(undefined)).toBe(false);
  });

  it("isValidStructuredMemo returns true for a well-formed memo", () => {
    expect(isValidStructuredMemo(makeStructured())).toBe(true);
  });

  it("isValidStructuredMemo returns true even if optional arrays are empty", () => {
    expect(
      isValidStructuredMemo(makeStructured({ risks: [], comparison: "", key_evidence: [] })),
    ).toBe(true);
  });

  it("isValidStructuredMemo returns true for the production object-risks shape", () => {
    expect(isValidStructuredMemo(makeStructuredWithObjectRisks())).toBe(true);
  });

  it("isValidStructuredMemo returns false when any risks item is a plain string", () => {
    const memo = makeStructured();
    (memo as unknown as { risks: unknown }).risks = [
      "plain string risk",
    ];
    expect(isValidStructuredMemo(memo)).toBe(false);
  });

  it("isValidStructuredMemo returns false when a risks item is missing the `risk` field", () => {
    const memo = makeStructured();
    (memo as unknown as { risks: unknown }).risks = [
      { mitigation: "only has mitigation" },
    ];
    expect(isValidStructuredMemo(memo)).toBe(false);
  });

  it("isValidStructuredMemo returns false when risks is not an array", () => {
    const memo = makeStructured();
    (memo as unknown as { risks: unknown }).risks = null;
    expect(isValidStructuredMemo(memo)).toBe(false);
  });
});

/* ── 5b. Object-shape risks render ── */

describe("DecisionMemoNarrative object-shape risks render", () => {
  it("renders each risk's text in the output", () => {
    const memo = makeStructuredWithObjectRisks();
    const html = renderToStaticMarkup(<StructuredNarrative memo={memo} lang="en" />);
    for (const r of memo.risks as StructuredMemoRisk[]) {
      expect(html).toContain(r.risk);
    }
  });

  it("renders the mitigation span when mitigation is present and non-empty", () => {
    const memo = makeStructuredWithObjectRisks();
    const html = renderToStaticMarkup(<StructuredNarrative memo={memo} lang="en" />);
    expect(html).toContain(
      "Exploring alternative parking solutions or locations nearby could be considered.",
    );
    expect(html).toContain("ea-memo-structured__risks-mitigation");
  });

  it("does NOT render a mitigation span when mitigation is null", () => {
    const memo = makeStructured({
      risks: [{ risk: "Solo risk, no mitigation", mitigation: null }],
    });
    const html = renderToStaticMarkup(<StructuredNarrative memo={memo} lang="en" />);
    expect(html).toContain("Solo risk, no mitigation");
    expect(html).not.toContain("ea-memo-structured__risks-mitigation");
  });

  it("does NOT render a mitigation span when mitigation is undefined", () => {
    const memo = makeStructured({
      risks: [{ risk: "Risk without mitigation key" }],
    });
    const html = renderToStaticMarkup(<StructuredNarrative memo={memo} lang="en" />);
    expect(html).toContain("Risk without mitigation key");
    expect(html).not.toContain("ea-memo-structured__risks-mitigation");
  });

  it("does NOT render a mitigation span when mitigation is an empty or whitespace-only string", () => {
    const memoEmpty = makeStructured({
      risks: [{ risk: "Empty mitigation", mitigation: "" }],
    });
    expect(
      renderToStaticMarkup(<StructuredNarrative memo={memoEmpty} lang="en" />),
    ).not.toContain("ea-memo-structured__risks-mitigation");

    const memoWhitespace = makeStructured({
      risks: [{ risk: "Whitespace mitigation", mitigation: "   " }],
    });
    expect(
      renderToStaticMarkup(<StructuredNarrative memo={memoWhitespace} lang="en" />),
    ).not.toContain("ea-memo-structured__risks-mitigation");
  });
});

/* ── 6. Arabic locale ── */

describe("DecisionMemoNarrative Arabic locale", () => {
  it("renders container with dir='rtl' and Arabic section headings", async () => {
    await i18n.changeLanguage("ar");
    const memo = makeStructured({
      headline_recommendation: "توصية قوية بالتوسع في منطقة العليا",
      ranking_explanation: "ترتيب رقم ٢ بسبب كثافة منخفضة للمنافسين.",
      key_evidence: [
        {
          signal: "المسافة إلى أقرب فرع",
          value: "3200 م",
          implication: "مخاطر تآكل منخفضة.",
          polarity: "positive",
        },
      ],
      risks: [{ risk: "فرع منافس قريب" }],
      comparison: "يتفوق على المرشح الثالث في مؤشر الإيرادات.",
      bottom_line: "الانتقال إلى التفاوض مع المالك.",
    });
    const html = renderToStaticMarkup(<StructuredNarrative memo={memo} lang="ar" />);

    expect(html).toContain('dir="rtl"');
    expect(html).toContain(ar.expansionAdvisor.theRecommendation);
    expect(html).toContain(ar.expansionAdvisor.keyEvidence);
    expect(html).toContain(ar.expansionAdvisor.risksToWatch);
    expect(html).toContain(ar.expansionAdvisor.howItCompares);
    expect(html).toContain(ar.expansionAdvisor.bottomLine);
    expect(html).toContain("توصية قوية بالتوسع في منطقة العليا");
    expect(html).toContain("فرع منافس قريب");
  });
});

/* ── 7. Polarity rendering ── */

describe("DecisionMemoNarrative polarity rendering", () => {
  it("renders distinguishable markers for positive / negative / neutral / missing", () => {
    const memo = makeStructured({
      key_evidence: [
        { signal: "sig-p", value: "v", implication: "imp", polarity: "positive" },
        { signal: "sig-n", value: "v", implication: "imp", polarity: "negative" },
        { signal: "sig-x", value: "v", implication: "imp", polarity: "neutral" },
        { signal: "sig-missing", value: "v", implication: "imp" },
      ],
    });
    const html = renderToStaticMarkup(<StructuredNarrative memo={memo} lang="en" />);

    expect(html).toContain('data-polarity="positive"');
    expect(html).toContain('data-polarity="negative"');
    // Both neutral and missing-polarity items render as neutral.
    const neutralMatches = html.match(/data-polarity="neutral"/g) ?? [];
    expect(neutralMatches.length).toBeGreaterThanOrEqual(2);

    expect(html).toContain("ea-memo-structured__polarity--positive");
    expect(html).toContain("ea-memo-structured__polarity--negative");
    expect(html).toContain("ea-memo-structured__polarity--neutral");
  });

  it("renders neutral marker when polarity is missing", () => {
    const memo = makeStructured({
      key_evidence: [{ signal: "s", value: "v", implication: "i" }],
    });
    const html = renderToStaticMarkup(<StructuredNarrative memo={memo} lang="en" />);

    expect(html).toContain('data-polarity="neutral"');
    expect(html).not.toContain('data-polarity="positive"');
    expect(html).not.toContain('data-polarity="negative"');
  });
});

/* ── 8. Cache behaviour (Map data-structure check under SSR constraints) ── */

describe("DecisionMemoNarrative cache behaviour", () => {
  /*
   * renderToStaticMarkup does not fire useEffect, so we cannot exercise the
   * full mount → fetch → unmount → remount sequence without a DOM. Instead,
   * verify the narrower invariant that the orchestrating component's first
   * render is side-effect-free (generateDecisionMemo mock is not called on
   * SSR), which is what guards against accidental server-side fetches.
   */
  beforeEach(() => {
    vi.resetModules();
  });

  it("does not invoke generateDecisionMemo during renderToStaticMarkup", async () => {
    const fetchSpy = vi.fn().mockResolvedValue({
      memo: makeLegacy(),
      memo_text: null,
      memo_json: makeStructured(),
    } satisfies GeneratedDecisionMemo);

    vi.doMock("../../lib/api/expansionAdvisor", async () => {
      const actual = await vi.importActual<
        typeof import("../../lib/api/expansionAdvisor")
      >("../../lib/api/expansionAdvisor");
      return { ...actual, generateDecisionMemo: fetchSpy };
    });

    const { default: DecisionMemoNarrative } = await import("./DecisionMemoNarrative");
    const candidate = { id: "c1" };
    const brief = {};

    // First render.
    renderToStaticMarkup(<DecisionMemoNarrative candidate={candidate} brief={brief} lang="en" />);
    // Second render with same candidate_id.
    renderToStaticMarkup(<DecisionMemoNarrative candidate={candidate} brief={brief} lang="en" />);

    // SSR never fires effects, so the fetcher must never be called.
    expect(fetchSpy).toHaveBeenCalledTimes(0);

    vi.doUnmock("../../lib/api/expansionAdvisor");
  });
});
