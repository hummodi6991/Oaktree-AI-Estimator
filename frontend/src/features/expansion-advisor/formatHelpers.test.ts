import { beforeEach, describe, expect, it } from "vitest";
import "../../i18n";
import i18n from "../../i18n";
import {
  fmtM2,
  fmtMeters,
  fmtMonths,
  fmtPct,
  fmtSARCompact,
  fmtSarPerM2,
  fmtSarPerM2Year,
  scoreColor,
  toNumeric,
} from "./formatHelpers";

beforeEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

const FALLBACK = "—";

describe("toNumeric", () => {
  it("returns finite numbers unchanged", () => {
    expect(toNumeric(42)).toBe(42);
    expect(toNumeric(0)).toBe(0);
    expect(toNumeric(-3.5)).toBe(-3.5);
  });

  it("parses numeric strings produced by SQLAlchemy Decimal serialization", () => {
    expect(toNumeric("45.00")).toBe(45);
    expect(toNumeric("  150.5  ")).toBe(150.5);
    expect(toNumeric("0")).toBe(0);
  });

  it("returns null for null/undefined/empty/whitespace", () => {
    expect(toNumeric(null)).toBeNull();
    expect(toNumeric(undefined)).toBeNull();
    expect(toNumeric("")).toBeNull();
    expect(toNumeric("   ")).toBeNull();
  });

  it("returns null for unparseable strings and NaN/Infinity", () => {
    expect(toNumeric("N/A")).toBeNull();
    expect(toNumeric("—")).toBeNull();
    expect(toNumeric(NaN)).toBeNull();
    expect(toNumeric(Infinity)).toBeNull();
  });
});

describe("fmtM2", () => {
  it("formats numeric input", () => {
    expect(fmtM2(150)).toBe("150 m²");
  });
  it("formats numeric-string input (Decimal serialization)", () => {
    expect(fmtM2("150.00")).toBe("150 m²");
  });
  it("falls back for null/undefined/empty/unparseable", () => {
    expect(fmtM2(null)).toBe(FALLBACK);
    expect(fmtM2(undefined)).toBe(FALLBACK);
    expect(fmtM2("")).toBe(FALLBACK);
    expect(fmtM2("N/A")).toBe(FALLBACK);
  });
});

describe("fmtMeters", () => {
  it("formats sub-km values as meters", () => {
    expect(fmtMeters(450)).toBe("450 m");
    expect(fmtMeters("450.0")).toBe("450 m");
  });
  it("formats >= 1000 as km with one decimal", () => {
    expect(fmtMeters(1500)).toBe("1.5 km");
    expect(fmtMeters("1500.00")).toBe("1.5 km");
  });
  it("falls back for invalid input", () => {
    expect(fmtMeters(null)).toBe(FALLBACK);
    expect(fmtMeters("not-a-number")).toBe(FALLBACK);
  });
});

describe("fmtSARCompact", () => {
  it("formats <1k as exact integer", () => {
    expect(fmtSARCompact(750)).toBe("SAR 750");
    expect(fmtSARCompact("750.00")).toBe("SAR 750");
  });
  it("formats thousands with K suffix", () => {
    expect(fmtSARCompact(168000)).toBe("SAR 168K");
    expect(fmtSARCompact("168000.00")).toBe("SAR 168K");
  });
  it("formats millions with M suffix", () => {
    expect(fmtSARCompact(1_200_000)).toBe("SAR 1.2M");
    expect(fmtSARCompact("1200000.00")).toBe("SAR 1.2M");
  });
  it("falls back for invalid input", () => {
    expect(fmtSARCompact(null)).toBe(FALLBACK);
    expect(fmtSARCompact("")).toBe(FALLBACK);
    expect(fmtSARCompact("garbage")).toBe(FALLBACK);
  });
});

describe("fmtMonths", () => {
  it("formats numeric and string input", () => {
    expect(fmtMonths(12)).toBe("12 mo");
    expect(fmtMonths("12.0")).toBe("12 mo");
  });
  it("falls back for invalid input", () => {
    expect(fmtMonths(undefined)).toBe(FALLBACK);
    expect(fmtMonths("N/A")).toBe(FALLBACK);
  });
});

describe("fmtPct", () => {
  it("formats numeric and string input", () => {
    expect(fmtPct(85)).toBe("85%");
    expect(fmtPct("85.00")).toBe("85%");
  });
  it("respects digits param", () => {
    expect(fmtPct("12.345", 1)).toBe("12.3%");
  });
  it("falls back for invalid input", () => {
    expect(fmtPct(null)).toBe(FALLBACK);
    expect(fmtPct("")).toBe(FALLBACK);
  });
});

describe("fmtSarPerM2 / fmtSarPerM2Year", () => {
  it("formats numeric and string input", () => {
    expect(fmtSarPerM2(1400)).toBe("1,400 SAR/m²");
    expect(fmtSarPerM2("1400.00")).toBe("1,400 SAR/m²");
    expect(fmtSarPerM2Year(1400)).toBe("1,400 SAR/m²/yr");
    expect(fmtSarPerM2Year("1400.00")).toBe("1,400 SAR/m²/yr");
  });
  it("falls back for invalid input", () => {
    expect(fmtSarPerM2(null)).toBe(FALLBACK);
    expect(fmtSarPerM2Year(undefined)).toBe(FALLBACK);
  });
});

describe("scoreColor", () => {
  it("returns green for numeric >=70 (incl. boundary)", () => {
    expect(scoreColor(85)).toBe("green");
    expect(scoreColor(70)).toBe("green");
  });
  it("returns amber for [60, 70)", () => {
    expect(scoreColor(65)).toBe("amber");
    expect(scoreColor(60)).toBe("amber");
  });
  it("returns red for <60", () => {
    expect(scoreColor(50)).toBe("red");
    expect(scoreColor(0)).toBe("red");
  });
  it("uses the same thresholds for stringified scores", () => {
    expect(scoreColor("85.00")).toBe("green");
    expect(scoreColor("70.00")).toBe("green");
    expect(scoreColor("60.00")).toBe("amber");
    expect(scoreColor("59.99")).toBe("red");
  });
  it("returns neutral for null/undefined/empty/unparseable", () => {
    expect(scoreColor(null)).toBe("neutral");
    expect(scoreColor(undefined)).toBe("neutral");
    expect(scoreColor("")).toBe("neutral");
    expect(scoreColor("N/A")).toBe("neutral");
  });
});
