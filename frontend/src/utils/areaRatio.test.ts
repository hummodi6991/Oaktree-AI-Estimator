import { describe, expect, it } from "vitest";

import { resolveAreaRatioBase, scaleAboveGroundAreaRatio } from "./areaRatio";

describe("scaleAboveGroundAreaRatio", () => {
  it("scales above-ground FAR while keeping basement untouched", () => {
    const current = { residential: 1.6, retail: 0.4, basement: 1.2 };
    const targetFar = 3;
    const scaled = scaleAboveGroundAreaRatio(current, targetFar);
    expect(scaled).not.toBeNull();
    if (!scaled) return;
    const next = scaled.nextAreaRatio as Record<string, number>;
    expect(next.basement).toBe(1.2);
    const nextAboveGround = (next.residential ?? 0) + (next.retail ?? 0);
    expect(nextAboveGround).toBeCloseTo(targetFar);
  });

  it("falls back to the template base ratio when current overrides are empty", () => {
    const current = {};
    const fallback = { residential: 1.2, retail: 0.3, basement: 0.5 };
    const resolved = resolveAreaRatioBase(current, fallback);
    expect(resolved).toBe(fallback);
    const scaled = scaleAboveGroundAreaRatio(resolved, 2.5);
    expect(scaled).not.toBeNull();
  });
});
