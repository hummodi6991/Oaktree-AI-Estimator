import { describe, expect, it } from "vitest";
import { normalizeMemoResponse } from "./expansionAdvisor";
import type {
  CandidateMemoResponse,
  StructuredMemo,
} from "./expansionAdvisor";

const structuredMemo: StructuredMemo = {
  headline_recommendation: "Strong lead — proceed to site visit.",
  ranking_explanation: "Wins on whitespace and economics.",
  key_evidence: [
    { signal: "whitespace", value: 82, implication: "low saturation", polarity: "positive" },
    { signal: "rent", value: "SAR 1,400/m²/yr", implication: "below district median", polarity: "positive" },
  ],
  risks: ["Parking confirmation required"],
  comparison: "Edges runner-up on economics.",
  bottom_line: "Recommend proceed.",
};

describe("normalizeMemoResponse — structured memo passthrough", () => {
  it("returns decision_memo_json typed as StructuredMemo when populated", () => {
    const raw: CandidateMemoResponse = {
      brand_profile: {},
      recommendation: { headline: "Pick #1" },
      candidate: {
        decision_memo: "Rendered text body.",
        decision_memo_json: structuredMemo,
      },
      market_research: {},
    };
    const out = normalizeMemoResponse(raw);
    expect(out.candidate.decision_memo).toBe("Rendered text body.");
    expect(out.candidate.decision_memo_json).toEqual(structuredMemo);
    expect(out.candidate.decision_memo_json?.key_evidence).toHaveLength(2);
    expect(out.candidate.decision_memo_json?.key_evidence[0].polarity).toBe("positive");
  });

  it("keeps decision_memo text and leaves decision_memo_json null when absent", () => {
    const raw: CandidateMemoResponse = {
      brand_profile: {},
      recommendation: {},
      candidate: {
        decision_memo: "Legacy rendered memo text only.",
      },
      market_research: {},
    };
    const out = normalizeMemoResponse(raw);
    expect(out.candidate.decision_memo).toBe("Legacy rendered memo text only.");
    expect(out.candidate.decision_memo_json).toBeNull();
  });

  it("defaults rerank metadata for legacy rows without those fields", () => {
    const raw: CandidateMemoResponse = {
      brand_profile: {},
      recommendation: {},
      candidate: {},
      market_research: {},
    };
    const out = normalizeMemoResponse(raw);
    expect(out.candidate.rerank_applied).toBe(false);
    expect(out.candidate.rerank_delta).toBe(0);
    expect(out.candidate.rerank_reason).toBeNull();
    expect(out.candidate.rerank_status).toBeNull();
    expect(out.candidate.deterministic_rank).toBeNull();
    expect(out.candidate.final_rank).toBeNull();
    expect(out.candidate.decision_memo).toBeNull();
    expect(out.candidate.decision_memo_json).toBeNull();
  });

  it("preserves legacy LLMDecisionMemo-style fields alongside decision_memo_json", () => {
    // Legacy LLMDecisionMemo payload lives in candidate.llm_decision_memo (or
    // any adjacent field). The normaliser should not drop unknown fields —
    // it should pass them through via the spread — while still populating
    // the new typed decision_memo_json.
    const raw: CandidateMemoResponse = {
      brand_profile: {},
      recommendation: {},
      candidate: {
        llm_decision_memo: {
          headline: "Legacy headline",
          fit_summary: "Legacy fit summary",
          top_reasons_to_pursue: ["r1"],
          top_risks: ["risk1"],
          recommended_next_action: "Site visit",
          rent_context: "Within range",
        },
        decision_memo: "Rendered text",
        decision_memo_json: structuredMemo,
      },
      market_research: {},
    };
    const out = normalizeMemoResponse(raw);
    expect(out.candidate.decision_memo_json).toEqual(structuredMemo);
    expect(out.candidate.decision_memo).toBe("Rendered text");
    expect(
      (out.candidate as unknown as Record<string, unknown>).llm_decision_memo,
    ).toBeDefined();
  });
});
