import { describe, expect, it, beforeEach, afterEach } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import "../../i18n";
import i18n from "../../i18n";
import GateSummary from "./GateSummary";
import { humanGateLabel } from "./formatHelpers";

beforeEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

afterEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

/* ─── F5 Stage 1 — Arabic gate labels (frontend) ──────────────────────── */

describe("F5 Stage 1 — humanGateLabel localization", () => {
  // Backend GATE_LABELS["ar"] parity — see app/services/expansion_advisor_i18n.py
  const AR: Record<string, string> = {
    zoning_fit_pass: "ملاءمة التنطيق",
    area_fit_pass: "ملاءمة المساحة",
    frontage_access_pass: "الواجهة/الوصول",
    parking_pass: "مواقف السيارات",
    district_pass: "الحي",
    cannibalization_pass: "التهام المبيعات",
    delivery_market_pass: "سوق التوصيل",
    economics_pass: "الجدوى الاقتصادية",
    radiance_growth_pass: "إشارة نمو السوق",
    population_floor_pass: "الحد الأدنى للوصول السكاني",
    commercial_floor_pass: "الحد الأدنى للنشاط التجاري",
    construction_proximity_pass: "الحد الأدنى للقرب الإنشائي",
  };

  it("returns the Arabic label for every canonical gate key", () => {
    const t = i18n.getFixedT("ar");
    for (const [key, label] of Object.entries(AR)) {
      expect(humanGateLabel(key, t)).toBe(label);
    }
  });

  it("returns the legacy English label when t is the EN TFunction", () => {
    const t = i18n.getFixedT("en");
    expect(humanGateLabel("zoning_fit_pass", t)).toBe("Zoning fit");
    expect(humanGateLabel("frontage_access_pass", t)).toBe("Frontage / access");
    expect(humanGateLabel("delivery_market_pass", t)).toBe("Delivery market");
    expect(humanGateLabel("economics_pass", t)).toBe("Economics");
  });

  it("is byte-identical to the no-arg English derivation in EN", () => {
    const t = i18n.getFixedT("en");
    for (const key of Object.keys(AR)) {
      expect(humanGateLabel(key, t)).toBe(humanGateLabel(key));
    }
  });

  it("falls back to the legacy English derivation for an unknown key — no crash", () => {
    const arT = i18n.getFixedT("ar");
    expect(humanGateLabel("some_new_gate_pass", arT)).toBe("Some new gate");
    expect(humanGateLabel("some_new_gate_pass")).toBe("Some new gate");
  });
});

describe("F5 Stage 1 — GateSummary renders localized chips", () => {
  const gates = { zoning_fit_pass: true, parking_pass: false };

  it("renders Arabic gate chips in AR locale", async () => {
    await i18n.changeLanguage("ar");
    const html = renderToStaticMarkup(<GateSummary gates={gates} />);
    expect(html).toContain("ملاءمة التنطيق");
    expect(html).toContain("مواقف السيارات");
    expect(html).not.toContain("Zoning fit");
  });

  it("renders English gate chips in EN locale", () => {
    const html = renderToStaticMarkup(<GateSummary gates={gates} />);
    expect(html).toContain("Zoning fit");
    expect(html).toContain("Parking");
  });
});
