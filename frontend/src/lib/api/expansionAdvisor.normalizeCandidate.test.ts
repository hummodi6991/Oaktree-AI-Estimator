import { describe, expect, it } from "vitest";
import { normalizeCandidate } from "./expansionAdvisor";
import type {
  ExpansionCandidate,
  RerankReason,
  RerankStatus,
} from "./expansionAdvisor";

// Minimal base to satisfy the ExpansionCandidate required fields.
const baseCandidate: ExpansionCandidate = {
  id: "cand-1",
  search_id: "search-1",
  parcel_id: "parcel-1",
  lat: 24.7,
  lon: 46.7,
};

describe("normalizeCandidate — rerank + decision-memo metadata", () => {
  it("passes all new rerank fields through unchanged when populated", () => {
    const rerankReason: RerankReason = {
      summary: "Promoted by LLM for stronger delivery whitespace.",
      positives_cited: ["high whitespace", "low cannibalization"],
      negatives_cited: ["moderate parking"],
      comparison_to_displaced_candidate: "Beats #4 on whitespace.",
    };
    const raw: ExpansionCandidate = {
      ...baseCandidate,
      deterministic_rank: 3,
      final_rank: 1,
      rerank_applied: true,
      rerank_reason: rerankReason,
      rerank_delta: 2,
      rerank_status: "applied",
      decision_memo_present: true,
    };
    const out = normalizeCandidate(raw);
    expect(out.deterministic_rank).toBe(3);
    expect(out.final_rank).toBe(1);
    expect(out.rerank_applied).toBe(true);
    expect(out.rerank_reason).toEqual(rerankReason);
    expect(out.rerank_delta).toBe(2);
    expect(out.rerank_status).toBe("applied");
    expect(out.decision_memo_present).toBe(true);
  });

  it("applies documented defaults for a legacy row with no rerank fields", () => {
    const out = normalizeCandidate({ ...baseCandidate });
    expect(out.rerank_applied).toBe(false);
    expect(out.rerank_delta).toBe(0);
    expect(out.decision_memo_present).toBe(false);
    expect(out.deterministic_rank).toBeNull();
    expect(out.final_rank).toBeNull();
    expect(out.rerank_reason).toBeNull();
    expect(out.rerank_status).toBeNull();
  });

  it("keeps an explicit null rerank_reason as null", () => {
    const out = normalizeCandidate({ ...baseCandidate, rerank_reason: null });
    expect(out.rerank_reason).toBeNull();
  });

  it("passes a fully-populated rerank_reason through as a typed RerankReason", () => {
    const rerankReason: RerankReason = {
      summary: "Strong demand + whitespace combination.",
      positives_cited: ["a", "b"],
      negatives_cited: [],
      comparison_to_displaced_candidate: "Edges #2 narrowly.",
    };
    const out = normalizeCandidate({ ...baseCandidate, rerank_reason: rerankReason });
    expect(out.rerank_reason).toEqual(rerankReason);
    expect(out.rerank_reason?.positives_cited).toEqual(["a", "b"]);
    expect(out.rerank_reason?.negatives_cited).toEqual([]);
  });

  it("accepts each canonical RerankStatus value without coercion", () => {
    const statuses: RerankStatus[] = [
      "flag_off",
      "shortlist_below_minimum",
      "llm_failed",
      "outside_rerank_cap",
      "unchanged",
      "applied",
    ];
    for (const status of statuses) {
      const out = normalizeCandidate({ ...baseCandidate, rerank_status: status });
      expect(out.rerank_status).toBe(status);
    }
  });
});
