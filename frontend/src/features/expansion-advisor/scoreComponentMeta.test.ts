import { describe, expect, it } from "vitest";
import { PER_COMPONENT_INPUTS } from "./scoreComponentMeta";
import type { ResolveCtx } from "./scoreComponentMeta";

function ctx(overrides: Partial<ResolveCtx>): ResolveCtx {
  return {
    candidate: {},
    scoreBreakdown: undefined,
    featureSnapshot: undefined,
    contextSources: {},
    ...overrides,
  };
}

const rentInput = PER_COMPONENT_INPUTS.occupancy_economics.find(
  (d) => d.key === "estimated_annual_rent_sar",
)!;
const hasImageInput = PER_COMPONENT_INPUTS.listing_quality.find(
  (d) => d.key === "has_image",
)!;

describe("scoreComponentMeta — listing-derived source attributes to the candidate platform", () => {
  it("maps commercial_unit_actual rent_source to bayut when platform is bayut", () => {
    const out = rentInput.resolve(
      ctx({
        candidate: { platform: "bayut" },
        contextSources: { rent_source: "commercial_unit_actual" },
      }),
    );
    expect(out.source).toBe("bayut");
  });

  it("maps commercial_unit_actual+micro rent_source to bayut when platform is bayut", () => {
    const out = rentInput.resolve(
      ctx({
        candidate: { platform: "bayut" },
        contextSources: { rent_source: "commercial_unit_actual+micro" },
      }),
    );
    expect(out.source).toBe("bayut");
  });

  it("maps commercial_unit_actual rent_source to aqar when platform is aqar", () => {
    const out = rentInput.resolve(
      ctx({
        candidate: { platform: "aqar" },
        contextSources: { rent_source: "commercial_unit_actual" },
      }),
    );
    expect(out.source).toBe("aqar");
  });

  it("falls back to aqar rent_source when platform is missing", () => {
    const out = rentInput.resolve(
      ctx({ candidate: {}, contextSources: { rent_source: "commercial_unit_actual" } }),
    );
    expect(out.source).toBe("aqar");
  });

  it("attributes listing_quality has_image to the candidate platform", () => {
    const out = hasImageInput.resolve(
      ctx({ candidate: { platform: "bayut", image_url: "https://example.com/x.jpg" } }),
    );
    expect(out.source).toBe("bayut");
  });

  it("attributes listing_quality has_image to aqar when platform is missing", () => {
    const out = hasImageInput.resolve(ctx({ candidate: { image_url: "https://example.com/x.jpg" } }));
    expect(out.source).toBe("aqar");
  });
});
