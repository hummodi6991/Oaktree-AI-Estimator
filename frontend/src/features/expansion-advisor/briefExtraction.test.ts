import { describe, expect, it } from "vitest";
import type { BriefExtractionResult, ExpansionBrief } from "../../lib/api/expansionAdvisor";
import {
  applyExtractionToBrief,
  buildBriefChips,
  confidenceBadgeColor,
  deltaTouchesAdvancedSection,
  editedFieldsSinceApply,
  proposalToProfileDelta,
} from "./briefExtraction";

const BASE_BRIEF: ExpansionBrief = {
  brand_name: "Ward Roasters",
  category: "coffee",
  service_model: "cafe",
  min_area_m2: 100,
  max_area_m2: 500,
  target_area_m2: 200,
  target_districts: [],
  existing_branches: [],
  limit: 15,
  brand_profile: { brand_archetype: null, preferred_districts: ["النخيل"] },
};

const RESULT: BriefExtractionResult = {
  proposal: {
    brand_archetype: { value: "neighborhood_local", confidence: "high", evidence: "quiet neighborhood" },
    price_tier: { value: "premium", confidence: "high", evidence: "premium pricing" },
    preferred_districts: { value: ["الياسمين", "النرجس"], confidence: "high" },
    cannibalization_tolerance_m: { value: 2000, confidence: "medium", evidence: "2 km" },
  },
  unrecognized_districts: [],
  conflicts: [],
  memo_color: ["family seating"],
  model: "gpt-4o-mini-2024-07-18",
  prompt_version: "brief-extract-v1.0-2026-06",
};

describe("proposalToProfileDelta", () => {
  it("flattens values and skips discarded chips", () => {
    const delta = proposalToProfileDelta(RESULT.proposal, new Set(["price_tier"]));
    expect(delta.brand_archetype).toBe("neighborhood_local");
    expect(delta.price_tier).toBeUndefined();
    expect(delta.preferred_districts).toEqual(["الياسمين", "النرجس"]);
    expect(delta.cannibalization_tolerance_m).toBe(2000);
  });

  it("ignores fields outside the applyable surface", () => {
    const delta = proposalToProfileDelta({
      hacked_field: { value: "x" },
      price_tier: { value: "mid", confidence: "high" },
    });
    expect(Object.keys(delta)).toEqual(["price_tier"]);
  });
});

describe("applyExtractionToBrief", () => {
  it("writes the delta into brand_profile and records audit metadata", () => {
    const { next, delta } = applyExtractionToBrief(BASE_BRIEF, RESULT, "my brief text");
    expect(next.brand_profile?.brand_archetype).toBe("neighborhood_local");
    expect(next.brand_profile?.price_tier).toBe("premium");
    expect(next.brand_profile?.brief_text).toBe("my brief text");
    expect(next.brand_profile?.brief_extraction?.accepted).toBe(true);
    expect(next.brand_profile?.brief_extraction?.prompt_version).toBe(
      "brief-extract-v1.0-2026-06",
    );
    expect(delta.brand_archetype).toBe("neighborhood_local");
    // Pure: the input brief is untouched (L2 — only Apply writes state).
    expect(BASE_BRIEF.brand_profile?.brand_archetype).toBeNull();
    expect(BASE_BRIEF.brand_profile?.brief_text).toBeUndefined();
  });

  it("merges districts with existing selections instead of replacing", () => {
    const { next } = applyExtractionToBrief(BASE_BRIEF, RESULT, "t");
    expect(next.brand_profile?.preferred_districts).toEqual([
      "النخيل",
      "الياسمين",
      "النرجس",
    ]);
  });

  it("respects discarded chips", () => {
    const { next } = applyExtractionToBrief(
      BASE_BRIEF,
      RESULT,
      "t",
      new Set(["preferred_districts"]),
    );
    expect(next.brand_profile?.preferred_districts).toEqual(["النخيل"]);
  });
});

describe("editedFieldsSinceApply", () => {
  it("reports fields the user changed after Apply", () => {
    const { next, delta } = applyExtractionToBrief(BASE_BRIEF, RESULT, "t");
    const profile = { ...next.brand_profile, price_tier: "mid" as const };
    expect(editedFieldsSinceApply(profile, delta)).toEqual(["price_tier"]);
  });

  it("is empty when nothing changed", () => {
    const { next, delta } = applyExtractionToBrief(BASE_BRIEF, RESULT, "t");
    expect(editedFieldsSinceApply(next.brand_profile, delta)).toEqual([]);
  });
});

describe("deltaTouchesAdvancedSection", () => {
  it("is true for advanced-section fields and false for archetype only", () => {
    expect(deltaTouchesAdvancedSection({ price_tier: "mid" })).toBe(true);
    expect(deltaTouchesAdvancedSection({ brand_archetype: "balanced" })).toBe(false);
  });
});

describe("buildBriefChips", () => {
  it("builds chips with confidence and label keys in surface order", () => {
    const chips = buildBriefChips(RESULT.proposal);
    expect(chips.map((c) => c.field)).toEqual([
      "brand_archetype",
      "price_tier",
      "cannibalization_tolerance_m",
      "preferred_districts",
    ]);
    const archetype = chips[0];
    expect(archetype.labelKey).toBe("expansionAdvisor.brandArchetype");
    expect(archetype.valueLabelKeys).toEqual([
      "expansionAdvisor.archetypeNeighborhoodLocal",
    ]);
    expect(archetype.confidence).toBe("high");
    expect(archetype.evidence).toBe("quiet neighborhood");
    expect(chips[2].valueText).toBe("2000 m");
    expect(chips[3].valueText).toBe("الياسمين، النرجس");
  });

  it("downgrades unknown confidence to low", () => {
    const chips = buildBriefChips({ price_tier: { value: "mid" } });
    expect(chips[0].confidence).toBe("low");
  });
});

describe("confidenceBadgeColor", () => {
  it("maps the ConfidenceBadge visual grammar", () => {
    expect(confidenceBadgeColor("high")).toBe("green");
    expect(confidenceBadgeColor("medium")).toBe("amber");
    expect(confidenceBadgeColor("low")).toBe("neutral");
  });
});
