import { describe, expect, it, beforeEach, afterEach } from "vitest";
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
