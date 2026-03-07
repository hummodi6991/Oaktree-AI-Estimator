import { describe, expect, it } from "vitest";
import en from "../i18n/en.json";
import ar from "../i18n/ar.json";

// ---------------------------------------------------------------------------
// Confidence label i18n keys — regression tests
// ---------------------------------------------------------------------------

describe("Restaurant Finder confidence labels", () => {
  it("en.json has distinct cellConfidence and parcelConfidence keys", () => {
    expect(en.restaurant.cellConfidence).toBe("Cell confidence");
    expect(en.restaurant.parcelConfidence).toBe("Parcel confidence");
  });

  it("ar.json has distinct cellConfidence and parcelConfidence keys", () => {
    expect(ar.restaurant.cellConfidence).toBe("ثقة الخلية");
    expect(ar.restaurant.parcelConfidence).toBe("ثقة القطعة");
  });

  it("cellConfidence and parcelConfidence are not equal to the generic confidence key", () => {
    expect(en.restaurant.cellConfidence).not.toBe(en.restaurant.confidence);
    expect(en.restaurant.parcelConfidence).not.toBe(en.restaurant.confidence);
    expect(ar.restaurant.cellConfidence).not.toBe(ar.restaurant.confidence);
    expect(ar.restaurant.parcelConfidence).not.toBe(ar.restaurant.confidence);
  });

  it("generic confidence key is preserved for backward compatibility", () => {
    expect(en.restaurant.confidence).toBe("Confidence");
    expect(ar.restaurant.confidence).toBe("مستوى الثقة");
  });
});

// ---------------------------------------------------------------------------
// Opportunity label i18n keys — regression tests
// ---------------------------------------------------------------------------

describe("Restaurant Finder opportunity labels", () => {
  it("en.json has distinct cellOpportunity and parcelOpportunity keys", () => {
    expect(en.restaurant.cellOpportunity).toBe("Cell opportunity");
    expect(en.restaurant.parcelOpportunity).toBe("Parcel opportunity");
  });

  it("ar.json has distinct cellOpportunity and parcelOpportunity keys", () => {
    expect(ar.restaurant.cellOpportunity).toBe("فرصة الخلية");
    expect(ar.restaurant.parcelOpportunity).toBe("فرصة القطعة");
  });

  it("cellOpportunity and parcelOpportunity are distinct from each other", () => {
    expect(en.restaurant.cellOpportunity).not.toBe(en.restaurant.parcelOpportunity);
    expect(ar.restaurant.cellOpportunity).not.toBe(ar.restaurant.parcelOpportunity);
  });
});
