import { describe, expect, it } from "vitest";

import { scaleAboveGroundAreaRatio } from "./areaRatio";

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
});
