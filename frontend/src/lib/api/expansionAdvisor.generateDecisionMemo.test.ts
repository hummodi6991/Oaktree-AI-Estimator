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

/* ─── Cache-key plumbing: search_id / parcel_id reach the request body ─── */

describe("generateDecisionMemo — search_id / parcel_id body plumbing", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  function lastBody(): Record<string, unknown> {
    const fetchMock = globalThis.fetch as ReturnType<typeof vi.fn>;
    const init = fetchMock.mock.calls[0][1] as RequestInit;
    return JSON.parse(init.body as string) as Record<string, unknown>;
  }

  it("includes explicit search_id and parcel_id at the top level when both are passed", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      mockJsonResponse({ memo: legacyMemo }),
    );
    await generateDecisionMemo(
      { id: "cand-1", parcel_id: "parcel-from-candidate" },
      { brand_name: "X" },
      "en",
      "search-123",
      "parcel-explicit",
    );
    const body = lastBody();
    expect(body.search_id).toBe("search-123");
    // Explicit parcel_id wins over candidate.parcel_id
    expect(body.parcel_id).toBe("parcel-explicit");
    expect(body.candidate).toBeDefined();
    expect(body.brief).toBeDefined();
    expect(body.lang).toBe("en");
  });

  it("falls back to candidate.parcel_id when parcel_id arg is omitted", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      mockJsonResponse({ memo: legacyMemo }),
    );
    await generateDecisionMemo(
      { id: "cand-1", parcel_id: "parcel-from-candidate" },
      { brand_name: "X" },
      "en",
      "search-123",
    );
    const body = lastBody();
    expect(body.search_id).toBe("search-123");
    expect(body.parcel_id).toBe("parcel-from-candidate");
  });

  it("omits search_id and parcel_id from the body (not null) when neither is available", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      mockJsonResponse({ memo: legacyMemo }),
    );
    await generateDecisionMemo({ id: "cand-1" }, { brand_name: "X" }, "en");
    const body = lastBody();
    expect("search_id" in body).toBe(false);
    expect("parcel_id" in body).toBe(false);
    expect(body.candidate).toBeDefined();
    expect(body.brief).toBeDefined();
    expect(body.lang).toBe("en");
  });

  it("omits null/empty search_id and parcel_id rather than sending null", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      mockJsonResponse({ memo: legacyMemo }),
    );
    await generateDecisionMemo(
      { id: "cand-1" },
      { brand_name: "X" },
      "en",
      null,
      "",
    );
    const body = lastBody();
    expect("search_id" in body).toBe(false);
    expect("parcel_id" in body).toBe(false);
  });
});
