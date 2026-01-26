import { describe, expect, it } from "vitest";

import { formatPercentDraftFromFraction, resolveFractionFromDraftPercent } from "./opex";

describe("opex percent helpers", () => {
  it("formats fractions as clean percent strings", () => {
    expect(formatPercentDraftFromFraction(0.05)).toBe("5");
    expect(formatPercentDraftFromFraction(0.055)).toBe("5.5");
    expect(formatPercentDraftFromFraction(0.1)).toBe("10");
  });

  it("parses percent drafts into fractions", () => {
    expect(resolveFractionFromDraftPercent("5")).toBeCloseTo(0.05);
    expect(resolveFractionFromDraftPercent("105")).toBeCloseTo(1);
    expect(resolveFractionFromDraftPercent("-10")).toBeCloseTo(0);
  });

  it("returns null for empty or invalid drafts", () => {
    expect(resolveFractionFromDraftPercent("")).toBeNull();
    expect(resolveFractionFromDraftPercent("abc")).toBeNull();
  });
});
