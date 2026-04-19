import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { generateDecisionMemo } from "./expansionAdvisor";
import type { LLMDecisionMemo, StructuredMemo } from "./expansionAdvisor";

const legacyMemo: LLMDecisionMemo = {
  headline: "Strong candidate",
  fit_summary: "Matches brand brief closely.",
  top_reasons_to_pursue: ["reason-1"],
  top_risks: ["risk-1"],
  recommended_next_action: "Schedule site visit",
  rent_context: "In line with district median",
};

const structuredMemo: StructuredMemo = {
  headline_recommendation: "Proceed",
  ranking_explanation: "Best on whitespace + economics.",
  key_evidence: [
    { signal: "whitespace", value: 82, implication: "low saturation" },
  ],
  risks: [{ risk: "parking", mitigation: null }],
  comparison: "Edges runner-up.",
  bottom_line: "Recommend proceed.",
};

function mockJsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  }) as unknown as Response;
}

describe("generateDecisionMemo — memo_json / memo_text passthrough", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("returns memo, memo_text, and memo_json when the server includes all three", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      mockJsonResponse({
        memo: legacyMemo,
        memo_text: "Rendered text body.",
        memo_json: structuredMemo,
      }),
    );
    const result = await generateDecisionMemo({ id: "cand-1" }, { brand_name: "X" }, "en");
    expect(result.memo).toEqual(legacyMemo);
    expect(result.memo_text).toBe("Rendered text body.");
    expect(result.memo_json).toEqual(structuredMemo);
  });

  it("leaves memo_text and memo_json null for legacy responses that only include memo", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      mockJsonResponse({ memo: legacyMemo }),
    );
    const result = await generateDecisionMemo({ id: "cand-1" }, { brand_name: "X" }, "en");
    expect(result.memo).toEqual(legacyMemo);
    expect(result.memo_text).toBeNull();
    expect(result.memo_json).toBeNull();
  });
});
