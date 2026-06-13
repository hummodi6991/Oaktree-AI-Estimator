import { describe, expect, it } from "vitest";
import {
  DEMAND_DG_INPUTS,
  PER_COMPONENT_INPUTS,
  isDgIndexDemand,
} from "./scoreComponentMeta";
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

const populationReachInput = PER_COMPONENT_INPUTS.demand_potential.find(
  (d) => d.key === "population_reach",
)!;

describe("scoreComponentMeta — population_reach distinguishes unmeasured from zero", () => {
  it("resolves a measured population_reach (including 0) to that number", () => {
    expect(
      populationReachInput.resolve(ctx({ featureSnapshot: { population_reach: 41000 } })).value,
    ).toBe(41000);
    // A measured zero stays a number (not coerced to null/em-dash).
    expect(
      populationReachInput.resolve(ctx({ featureSnapshot: { population_reach: 0 } })).value,
    ).toBe(0);
  });

  it("resolves a null population_reach (no grid coverage) to null, not 0", () => {
    // PR-3: an unmeasured candidate carries null; the demand card renders an
    // em-dash for null and must never paper over it with a fabricated 0.
    expect(
      populationReachInput.resolve(ctx({ featureSnapshot: { population_reach: null } })).value,
    ).toBeNull();
  });
});

/* ─── PR-D: dg_index demand inputs ───────────────────────────────────────── */

function dgSnapshot(): Record<string, unknown> {
  return {
    demand_score_source: "dg_index",
    demand_generator_index: {
      composite_0_100: 61.45,
      weights_version: "l1_v2_2026-06",
      radius_m: 3500,
      population_reach: 180000,
      pop_radius_m: 1500,
      population_local_reach: 42000,
      osm_generators: {
        offices: 12,
        malls_retail: 3,
        transit: 5,
        mosques: 8,
        schools: 4,
        hospitals: 1,
        hotels: 2,
      },
      building_floors_proxy_sum: 18234.5,
      fnb_review_weighted_density: 98321.25,
      fnb_venue_count: 57,
      subscores: {
        population: 55.1,
        osm_generators: 62.3,
        building_floors: 48.7,
        fnb_review_weighted: 70.2,
      },
    },
    demand_blend: {
      pop_or_index_weight: 0.75,
      delivery_weight: 0.25,
      delivery_score: 64.2,
      listing_realized_split: 0.7,
    },
  };
}

function dgInput(key: string) {
  return DEMAND_DG_INPUTS.find((d) => d.key === key)!;
}

describe("scoreComponentMeta — DEMAND_DG_INPUTS resolvers", () => {
  it("resolves every descriptor from a full dg snapshot", () => {
    const c = ctx({ featureSnapshot: dgSnapshot() });
    expect(dgInput("dg_composite").resolve(c).value).toBe(61.45);
    expect(dgInput("fnb_review_weighted_density").resolve(c).value).toBe(98321.25);
    expect(dgInput("fnb_venue_count").resolve(c).value).toBe(57);
    // osm_generators_total = sum of the seven per-kind counts.
    expect(dgInput("osm_generators_total").resolve(c).value).toBe(35);
    expect(dgInput("building_floors_proxy_sum").resolve(c).value).toBe(18234.5);
    expect(dgInput("population_local_reach").resolve(c).value).toBe(42000);
    expect(dgInput("delivery_score").resolve(c).value).toBe(64.2);
    expect(dgInput("blend_weights").resolve(c).value).toBe("0.75 / 0.25");
    expect(dgInput("listing_realized_split").resolve(c).value).toBe(0.7);
    expect(dgInput("radius_m").resolve(c).value).toBe(3500);
    expect(dgInput("weights_version").resolve(c).value).toBe("l1_v2_2026-06");
  });

  it("attributes sources per descriptor", () => {
    const c = ctx({ featureSnapshot: dgSnapshot() });
    expect(dgInput("dg_composite").resolve(c).source).toBe("oaktree_internal");
    expect(dgInput("fnb_review_weighted_density").resolve(c).source).toBe("fnb_reviews");
    expect(dgInput("fnb_venue_count").resolve(c).source).toBe("fnb_reviews");
    expect(dgInput("osm_generators_total").resolve(c).source).toBe("osm");
    expect(dgInput("building_floors_proxy_sum").resolve(c).source).toBe("building_density");
    expect(dgInput("population_local_reach").resolve(c).source).toBe("population_grid");
    expect(dgInput("delivery_score").resolve(c).source).toBe("expansion_delivery_market");
  });

  it("honors the delivery_source override for the delivery leg", () => {
    const c = ctx({
      featureSnapshot: dgSnapshot(),
      contextSources: { delivery_source: "hungerstation" },
    });
    expect(dgInput("delivery_score").resolve(c).source).toBe("hungerstation");
  });

  it("resolves every descriptor to null when the dg blocks are absent", () => {
    const c = ctx({ featureSnapshot: { demand_score_source: "dg_index" } });
    for (const d of DEMAND_DG_INPUTS) {
      expect(d.resolve(c).value).toBeNull();
    }
  });

  it("resolves a partially-missing dg block field-by-field", () => {
    const c = ctx({
      featureSnapshot: {
        demand_score_source: "dg_index",
        demand_generator_index: { composite_0_100: 40.0 },
      },
    });
    expect(dgInput("dg_composite").resolve(c).value).toBe(40.0);
    expect(dgInput("fnb_review_weighted_density").resolve(c).value).toBeNull();
    expect(dgInput("osm_generators_total").resolve(c).value).toBeNull();
    // demand_blend absent (pre-rider dg_index rows) → delivery rows null.
    expect(dgInput("delivery_score").resolve(c).value).toBeNull();
    expect(dgInput("blend_weights").resolve(c).value).toBeNull();
  });
});

describe("scoreComponentMeta — isDgIndexDemand engine selector", () => {
  it("is true only for demand_score_source === 'dg_index'", () => {
    expect(isDgIndexDemand({ demand_score_source: "dg_index" })).toBe(true);
    expect(isDgIndexDemand({ demand_score_source: "pop_score" })).toBe(false);
  });

  it("treats absence as pop_score — never an error", () => {
    expect(isDgIndexDemand({})).toBe(false);
    expect(isDgIndexDemand(undefined)).toBe(false);
  });
});
