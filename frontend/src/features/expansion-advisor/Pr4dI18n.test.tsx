import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import "../../i18n";
import i18n from "../../i18n";
import en from "../../i18n/en.json";
import ar from "../../i18n/ar.json";
import ExpansionMemoPanel from "./ExpansionMemoPanel";
import ExpansionBriefForm, { defaultBrief } from "./ExpansionBriefForm";
import CategorySelect from "./CategorySelect";
import SearchBar from "../../ui-v2/SearchBar";
import ScoreBreakdownCompact from "./ScoreBreakdownCompact";
import { formatLandlordBriefingText } from "./studyAdapters";

beforeEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

afterEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

/* ─── §2 gate_verdict on the Site tab ─────────────────────────────────── */

function renderSiteTab(gate_verdict: string) {
  return renderToStaticMarkup(
    <ExpansionMemoPanel
      loading={false}
      memo={{
        recommendation: { verdict: "go", headline: "GO headline", gate_verdict },
        candidate: {},
        market_research: {},
        brand_profile: {},
      } as any}
      initialTab="site"
    />,
  );
}

describe("PR #4d §2 — gate_verdict i18n on the Site tab", () => {
  const cases: Array<[string, string, string]> = [
    ["pass", en.expansionAdvisor.gatePass, ar.expansionAdvisor.gatePass],
    ["fail", en.expansionAdvisor.gateFail, ar.expansionAdvisor.gateFail],
    ["unknown", en.expansionAdvisor.gateNeedsValidation, ar.expansionAdvisor.gateNeedsValidation],
  ];

  for (const [verdict, enLabel, arLabel] of cases) {
    it(`renders '${enLabel}' in EN for verdict '${verdict}'`, () => {
      const html = renderSiteTab(verdict);
      expect(html).toContain(enLabel);
      expect(html).not.toMatch(/>pass</);
    });

    it(`renders '${arLabel}' in AR for verdict '${verdict}'`, async () => {
      await i18n.changeLanguage("ar");
      const html = renderSiteTab(verdict);
      expect(html).toContain(arLabel);
      expect(html).not.toMatch(/>pass</);
      expect(html).not.toMatch(/>fail</);
      expect(html).not.toMatch(/>unknown</);
    });
  }
});

/* ─── §3 brief-form placeholders ──────────────────────────────────────── */

describe("PR #4d §3 — brief-form placeholders", () => {
  it("renders the brand-examples placeholder localized (EN)", () => {
    const html = renderToStaticMarkup(
      <ExpansionBriefForm initialValue={defaultBrief} onSubmit={() => {}} loading={false} />,
    );
    expect(html).toContain("e.g. Al Baik, Kudu");
    expect(html).toContain("Select a restaurant category");
    expect(html).toContain("Choose the closest match for better search quality");
  });

  it("renders the brand-examples placeholder localized (AR)", async () => {
    await i18n.changeLanguage("ar");
    const html = renderToStaticMarkup(
      <ExpansionBriefForm initialValue={defaultBrief} onSubmit={() => {}} loading={false} />,
    );
    expect(html).toContain("مثال: البيك، كودو");
    expect(html).toContain("اختر فئة المطعم");
    expect(html).toContain("اختر أقرب تطابق للحصول على نتائج بحث أفضل");
    expect(html).not.toContain("e.g. Al Baik, Kudu");
    expect(html).not.toContain("Select a restaurant category");
  });

  it("CategorySelect defaults its placeholder to the localized key", () => {
    const html = renderToStaticMarkup(<CategorySelect value="" onChange={() => {}} />);
    expect(html).toContain("Select a restaurant category");
    expect(html).not.toContain("expansionAdvisor.categorySelectPlaceholder");
  });

  it("§3e — searchLimit AR value is translated, not the English fallback", () => {
    expect(en.expansionAdvisor.searchLimit).toBe("Search limit");
    expect(ar.expansionAdvisor.searchLimit).toBe("حد البحث");
    expect(i18n.getFixedT("ar")("expansionAdvisor.searchLimit")).toBe("حد البحث");
  });
});

/* ─── §4 SearchBar + CategorySelect literals ──────────────────────────── */

describe("PR #4d §4 — SearchBar i18n", () => {
  it("renders the search placeholder localized (EN)", () => {
    const html = renderToStaticMarkup(<SearchBar onSelect={() => {}} />);
    expect(html).toContain("Search by parcels, streets, districts");
  });

  it("renders the search placeholder localized (AR)", async () => {
    await i18n.changeLanguage("ar");
    const html = renderToStaticMarkup(<SearchBar onSelect={() => {}} />);
    expect(html).toContain("ابحث عن القطع والشوارع والأحياء");
    expect(html).not.toContain("Search by parcels, streets, districts");
  });

  it("resolves all search-state keys in both locales", () => {
    const enT = i18n.getFixedT("en");
    const arT = i18n.getFixedT("ar");
    expect(enT("search.loading")).toBe("Searching…");
    expect(arT("search.loading")).toBe("جارٍ البحث…");
    expect(enT("search.unavailable")).toBe("Search unavailable");
    expect(arT("search.unavailable")).toBe("البحث غير متاح");
    expect(enT("search.noMatches")).toBe("No matches found");
    expect(arT("search.noMatches")).toBe("لا توجد نتائج مطابقة");
    expect(enT("search.resultsAriaLabel")).toBe("Search results");
    expect(arT("search.resultsAriaLabel")).toBe("نتائج البحث");
  });
});

describe("PR #4d §4e — CategorySelect clear aria-label", () => {
  it("interpolates the localized clear label (EN)", () => {
    const html = renderToStaticMarkup(<CategorySelect value="burger" onChange={() => {}} />);
    expect(html).toContain('aria-label="Clear Burger"');
  });

  it("interpolates the localized clear label (AR)", async () => {
    await i18n.changeLanguage("ar");
    const html = renderToStaticMarkup(<CategorySelect value="burger" onChange={() => {}} />);
    expect(html).toContain('aria-label="مسح Burger"');
  });
});

/* ─── i18n parity ─────────────────────────────────────────────────────── */

describe("PR #4d — i18n parity", () => {
  function flatKeys(obj: Record<string, unknown>, prefix = ""): Set<string> {
    const out = new Set<string>();
    for (const [k, v] of Object.entries(obj)) {
      const p = prefix ? `${prefix}.${k}` : k;
      if (v && typeof v === "object") {
        for (const nk of flatKeys(v as Record<string, unknown>, p)) out.add(nk);
      } else {
        out.add(p);
      }
    }
    return out;
  }

  it("every new PR #4d key exists in both locales", () => {
    const newKeys = [
      "search.placeholder",
      "search.loading",
      "search.unavailable",
      "search.noMatches",
      "search.resultsAriaLabel",
      "expansionAdvisor.categorySelectPlaceholder",
      "expansionAdvisor.brandExamplesPlaceholder",
      "expansionAdvisor.categorySelectHelp",
      "expansionAdvisor.clearCategoryAriaLabel",
      "expansionAdvisor.decisionLogic.weightPercent",
      "expansionAdvisor.landlordBriefing.fallbackFormat",
      "expansionAdvisor.advisorySection.rentPositioningLow",
      "expansionAdvisor.advisorySection.rentPositioningMid",
      "expansionAdvisor.advisorySection.rentPositioningHigh",
    ];
    const enKeys = flatKeys(en as Record<string, unknown>);
    const arKeys = flatKeys(ar as Record<string, unknown>);
    for (const k of newKeys) {
      expect(enKeys.has(k)).toBe(true);
      expect(arKeys.has(k)).toBe(true);
    }
  });

  it("en.json and ar.json have identical key sets", () => {
    const enKeys = flatKeys(en as Record<string, unknown>);
    const arKeys = flatKeys(ar as Record<string, unknown>);
    expect([...enKeys].filter((k) => !arKeys.has(k))).toEqual([]);
    expect([...arKeys].filter((k) => !enKeys.has(k))).toEqual([]);
  });
});

/* ─── PR #4e Tier 2 cosmetic AR cleanup ─────────────────────────────── */

describe("PR #4e §1/§2 — pts literal consumes decisionLogicWeightedPoints", () => {
  const breakdown = {
    weights: { economics: 0.4 },
    inputs: { economics: 50 },
    weighted_components: { economics: 12.3 },
  } as any;

  it("renders the pts label in EN with the existing key", () => {
    const html = renderToStaticMarkup(<ScoreBreakdownCompact breakdown={breakdown} />);
    expect(html).toContain("12.3 pts");
  });

  it("renders the pts label in AR with the existing key", async () => {
    await i18n.changeLanguage("ar");
    const html = renderToStaticMarkup(<ScoreBreakdownCompact breakdown={breakdown} />);
    expect(html).toContain(" نقطة");
    expect(html).not.toContain(" pts");
  });
});

describe("PR #4e §3 — % weight uses new decisionLogic.weightPercent key", () => {
  it("resolves both locales", () => {
    expect(i18n.getFixedT("en")("expansionAdvisor.decisionLogic.weightPercent", { value: 25 })).toBe("25% weight");
    expect(i18n.getFixedT("ar")("expansionAdvisor.decisionLogic.weightPercent", { value: 25 })).toBe("25٪ وزن");
  });
});

describe("PR #4e §4 — F&B outlet fallback localizes via threaded t", () => {
  it("returns English fallback when t is omitted (back-compat)", () => {
    const out = formatLandlordBriefingText(
      { parcel_id: "P1", rank_position: 1 } as any,
      null,
      null,
    );
    expect(out).toContain("F&B outlet");
  });

  it("returns localized AR fallback when t resolves AR", async () => {
    await i18n.changeLanguage("ar");
    const t = i18n.getFixedT("ar");
    const out = formatLandlordBriefingText(
      { parcel_id: "P1", rank_position: 1 } as any,
      null,
      null,
      t as any,
    );
    expect(out).toContain("منشأة أغذية ومشروبات");
    expect(out).not.toContain("F&B outlet");
  });

  it("prefers report.best_format over fallback when present", () => {
    const t = i18n.getFixedT("ar");
    const out = formatLandlordBriefingText(
      { parcel_id: "P1", rank_position: 1 } as any,
      { recommendation: { best_format: "مقهى صغير" } } as any,
      null,
      t as any,
    );
    expect(out).toContain("مقهى صغير");
    expect(out).not.toContain("منشأة أغذية ومشروبات");
  });
});

/* ─── PR #4f percentile rephrase ────────────────────────────────────── */

describe("PR #4f — pctFromFraction polarity-keyed templates", () => {
  it("low percentile renders 'cheaper than ~N%' (EN)", () => {
    expect(i18n.getFixedT("en")(
      "expansionAdvisor.advisorySection.rentPositioningLow", { value: 72 }
    )).toBe("cheaper than ~72% of nearby comparables");
  });

  it("low percentile renders the AR template", () => {
    expect(i18n.getFixedT("ar")(
      "expansionAdvisor.advisorySection.rentPositioningLow", { value: 72 }
    )).toBe("أقل من حوالي 72٪ من المقارنات في الحي");
  });

  it("mid percentile renders 'around district median' (EN)", () => {
    expect(i18n.getFixedT("en")(
      "expansionAdvisor.advisorySection.rentPositioningMid"
    )).toBe("around the district median rent");
  });

  it("mid percentile renders the AR template", () => {
    expect(i18n.getFixedT("ar")(
      "expansionAdvisor.advisorySection.rentPositioningMid"
    )).toBe("قريب من الإيجار الوسيط للحي");
  });

  it("high percentile renders 'more expensive than ~N%' (EN)", () => {
    expect(i18n.getFixedT("en")(
      "expansionAdvisor.advisorySection.rentPositioningHigh", { value: 88 }
    )).toBe("more expensive than ~88% of nearby comparables");
  });

  it("high percentile renders the AR template", () => {
    expect(i18n.getFixedT("ar")(
      "expansionAdvisor.advisorySection.rentPositioningHigh", { value: 88 }
    )).toBe("أعلى من حوالي 88٪ من المقارنات في الحي");
  });

  it("ar.json rentPercentile label is no longer the English literal", () => {
    expect(i18n.getFixedT("ar")(
      "expansionAdvisor.advisorySection.rentPercentile"
    )).toBe("موقع الإيجار");
    expect(i18n.getFixedT("ar")(
      "expansionAdvisor.advisorySection.rentPercentile"
    )).not.toBe("Rent percentile");
  });
});

/* ─── language-change reload behaviour ──────────────────────────────── */

describe("restartInLocale — persist + reload from the click site", () => {
  // The reload is driven from the UI click handlers (not from an i18next
  // listener), so it works the same on Safari and Chrome. The test env is
  // plain node (no jsdom), so we stub `window` for the duration of the
  // test and assert the helper persists the chosen locale and calls
  // reload exactly once per invocation.

  it("persists the normalized locale and calls window.location.reload()", async () => {
    const { restartInLocale, LOCALE_STORAGE_KEY } = await import("../../i18n");
    const reload = vi.fn();
    const store: Record<string, string> = {};
    const fakeWindow = {
      localStorage: {
        getItem: (key: string) => (key in store ? store[key] : null),
        setItem: (key: string, value: string) => {
          store[key] = value;
        },
        removeItem: (key: string) => {
          delete store[key];
        },
      },
      location: { reload },
    };
    const g = globalThis as any;
    const originalWindow = g.window;
    g.window = fakeWindow;
    try {
      restartInLocale("ar");
      expect(store[LOCALE_STORAGE_KEY]).toBe("ar");
      expect(reload).toHaveBeenCalledTimes(1);

      restartInLocale("en-US");
      expect(store[LOCALE_STORAGE_KEY]).toBe("en");
      expect(reload).toHaveBeenCalledTimes(2);
    } finally {
      if (originalWindow === undefined) delete g.window;
      else g.window = originalWindow;
    }
  });
});
