import { describe, expect, it } from "vitest";
import {
  normalizeCandidate,
  normalizeCompareResponse,
  normalizeMemoResponse,
  normalizeReportResponse,
} from "./expansionAdvisor";
import type {
  CandidateMemoResponse,
  CompareCandidatesResponse,
  ExpansionCandidate,
  RecommendationReportResponse,
} from "./expansionAdvisor";

const baseCandidate: ExpansionCandidate = {
  id: "cand-1",
  search_id: "search-1",
  parcel_id: "parcel-1",
  lat: 24.7,
  lon: 46.7,
};

describe("normalizeCandidate — Numeric coercion at the API boundary", () => {
  it("coerces stringified Decimals into numbers", () => {
    // SQLAlchemy serializes Numeric columns as strings (e.g. "45.00").
    const raw = {
      ...baseCandidate,
      area_m2: "150.00" as unknown as number,
      final_score: "78.50" as unknown as number,
      cannibalization_score: "12.00" as unknown as number,
      distance_to_nearest_branch_m: "2300.50" as unknown as number,
      estimated_annual_rent_sar: "168000.00" as unknown as number,
      estimated_fitout_cost_sar: "50000.00" as unknown as number,
      estimated_rent_sar_m2_year: "1400.00" as unknown as number,
      unit_price_sar_annual: "750000.00" as unknown as number,
      unit_area_sqm: "120.00" as unknown as number,
      unit_street_width_m: "8.50" as unknown as number,
    };
    const out = normalizeCandidate(raw);
    expect(out.area_m2).toBe(150);
    expect(out.final_score).toBe(78.5);
    expect(out.cannibalization_score).toBe(12);
    expect(out.distance_to_nearest_branch_m).toBe(2300.5);
    expect(out.estimated_annual_rent_sar).toBe(168000);
    expect(out.estimated_fitout_cost_sar).toBe(50000);
    expect(out.estimated_rent_sar_m2_year).toBe(1400);
    expect(out.unit_price_sar_annual).toBe(750000);
    expect(out.unit_area_sqm).toBe(120);
    expect(out.unit_street_width_m).toBe(8.5);
  });

  it("passes already-numeric input through unchanged", () => {
    const raw = {
      ...baseCandidate,
      area_m2: 150,
      final_score: 78.5,
      estimated_annual_rent_sar: 168000,
    };
    const out = normalizeCandidate(raw);
    expect(out.area_m2).toBe(150);
    expect(out.final_score).toBe(78.5);
    expect(out.estimated_annual_rent_sar).toBe(168000);
  });

  it("preserves null/undefined for missing fields", () => {
    const out = normalizeCandidate({ ...baseCandidate });
    expect(out.area_m2).toBeUndefined();
    expect(out.final_score).toBeUndefined();
    expect(out.estimated_annual_rent_sar).toBeUndefined();
  });

  it("coerces unparseable numeric strings to null", () => {
    const raw = {
      ...baseCandidate,
      area_m2: "" as unknown as number,
      final_score: "N/A" as unknown as number,
    };
    const out = normalizeCandidate(raw);
    expect(out.area_m2).toBeNull();
    expect(out.final_score).toBeNull();
  });

  it("matches a real-world API response shape (mirrors المونسية candidate)", () => {
    // Mirrors the field shape the backend actually emits: a mix of stringified
    // Decimals, ints, and nulls.
    const raw = {
      ...baseCandidate,
      district: "المونسية",
      area_m2: "150.00" as unknown as number,
      final_score: "73.25" as unknown as number,
      economics_score: "68.00" as unknown as number,
      brand_fit_score: "82.50" as unknown as number,
      cannibalization_score: "5.00" as unknown as number,
      distance_to_nearest_branch_m: "3200.00" as unknown as number,
      estimated_annual_rent_sar: "210000.00" as unknown as number,
      estimated_fitout_cost_sar: "85000.00" as unknown as number,
      estimated_rent_sar_m2_year: "1400.00" as unknown as number,
      display_annual_rent_sar: "210000.00" as unknown as number,
    };
    const out = normalizeCandidate(raw);
    expect(typeof out.area_m2).toBe("number");
    expect(typeof out.final_score).toBe("number");
    expect(typeof out.estimated_annual_rent_sar).toBe("number");
    expect(typeof out.distance_to_nearest_branch_m).toBe("number");
    // The card renders `area_m2`, `display_annual_rent_sar`, and the chip
    // shows `estimated_fitout_cost_sar`. All three must be numbers post-norm.
    expect(out.area_m2).toBe(150);
    expect(out.display_annual_rent_sar).toBe(210000);
    expect(out.estimated_fitout_cost_sar).toBe(85000);
  });
});

describe("normalizeMemoResponse — Numeric coercion on candidate sub-object", () => {
  it("coerces stringified Decimals on memo.candidate", () => {
    const raw: CandidateMemoResponse = {
      brand_profile: {},
      recommendation: {},
      candidate: {
        // CandidateMemoResponse.candidate is loosely typed (`[k: string]: unknown`),
        // so we can pass strings through and assert the post-coerce shape.
        final_score: "78.50" as unknown as number,
        economics_score: "65.00" as unknown as number,
        brand_fit_score: "82.00" as unknown as number,
      },
      market_research: {},
    };
    const out = normalizeMemoResponse(raw);
    expect(out.candidate.final_score).toBe(78.5);
    expect(out.candidate.economics_score).toBe(65);
    expect(out.candidate.brand_fit_score).toBe(82);
  });

  it("leaves rerank metadata defaults intact alongside coerced numerics", () => {
    const raw: CandidateMemoResponse = {
      brand_profile: {},
      recommendation: {},
      candidate: {
        final_score: "70.00" as unknown as number,
      },
      market_research: {},
    };
    const out = normalizeMemoResponse(raw);
    expect(out.candidate.final_score).toBe(70);
    expect(out.candidate.rerank_applied).toBe(false);
    expect(out.candidate.decision_memo_json).toBeNull();
  });
});

describe("normalizeCompareResponse — Numeric coercion on items", () => {
  it("coerces stringified Decimals on each item", () => {
    const raw = {
      items: [
        {
          candidate_id: "c1",
          area_m2: "150.00",
          final_score: "78.00",
          estimated_annual_rent_sar: "168000.00",
          estimated_fitout_cost_sar: "50000.00",
          distance_to_nearest_branch_m: "2300.00",
        },
      ],
      summary: {},
    } as unknown as CompareCandidatesResponse;
    const out = normalizeCompareResponse(raw);
    const item = out.items[0];
    expect(item.area_m2).toBe(150);
    expect(item.final_score).toBe(78);
    expect(item.estimated_annual_rent_sar).toBe(168000);
    expect(item.estimated_fitout_cost_sar).toBe(50000);
    expect(item.distance_to_nearest_branch_m).toBe(2300);
  });

  it("preserves existing default-fill behaviour", () => {
    const raw = {
      items: [{ candidate_id: "c1", final_score: "85.00" }],
      summary: { best_overall_candidate_id: "c1" },
    } as unknown as CompareCandidatesResponse;
    const out = normalizeCompareResponse(raw);
    expect(out.items[0].gate_status_json).toEqual({});
    expect(out.items[0].confidence_grade).toBe("D");
    expect(out.items[0].final_score).toBe(85);
  });
});

describe("normalizeReportResponse — Numeric coercion on top_candidates", () => {
  it("coerces stringified Decimals on each top_candidate", () => {
    const raw = {
      top_candidates: [
        { id: "c1", final_score: "85.00" },
        { id: "c2", final_score: "72.50" },
      ],
      recommendation: {},
      assumptions: {},
      brand_profile: {},
      meta: {},
    } as unknown as RecommendationReportResponse;
    const out = normalizeReportResponse(raw);
    expect(out.top_candidates[0].final_score).toBe(85);
    expect(out.top_candidates[1].final_score).toBe(72.5);
  });

  it("preserves default-fill behaviour for missing top_candidates", () => {
    const raw = {} as RecommendationReportResponse;
    const out = normalizeReportResponse(raw);
    expect(out.top_candidates).toEqual([]);
    // recommendation.* defaults are filled (pass_count: 0, summary: "", etc.)
    expect(out.recommendation.pass_count).toBe(0);
    expect(out.recommendation.summary).toBe("");
  });
});
