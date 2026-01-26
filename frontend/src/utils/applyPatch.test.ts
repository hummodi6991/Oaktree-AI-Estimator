import { describe, expect, it } from "vitest";

import { applyPatch } from "./applyPatch";

describe("applyPatch", () => {
  it("deep merges nested patches without clobbering other overrides", () => {
    const base = {
      y1_income_effective_pct: 90,
      opex_pct: 0.05,
      area_ratio: { residential: 1.1, office: 0.4 },
    };
    const overridesStep1 = applyPatch({}, { area_ratio: { residential: 1.4 } });
    const overridesStep2 = applyPatch(overridesStep1, { opex_pct: 0.08 });
    const merged = applyPatch(base, overridesStep2);

    expect(merged.area_ratio.residential).toBe(1.4);
    expect(merged.area_ratio.office).toBe(0.4);
    expect(merged.opex_pct).toBe(0.08);
    expect(merged.y1_income_effective_pct).toBe(90);
  });

  it("preserves nested overrides when patching a sibling key", () => {
    const base = {
      unit_cost: { residential: 2500, retail: 3000 },
      land_price_sar_m2: 2000,
    };
    const overridesStep1 = applyPatch({}, { unit_cost: { residential: 2800 } });
    const overridesStep2 = applyPatch(overridesStep1, { land_price_sar_m2: 2200 });
    const merged = applyPatch(base, overridesStep2);

    expect(merged.unit_cost.residential).toBe(2800);
    expect(merged.unit_cost.retail).toBe(3000);
    expect(merged.land_price_sar_m2).toBe(2200);
  });
});
