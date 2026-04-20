import { describe, expect, it, beforeEach } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import "../../i18n";
import i18n from "../../i18n";
import ExpansionCandidateCard from "./ExpansionCandidateCard";
import type { ExpansionCandidate, RerankStatus } from "../../lib/api/expansionAdvisor";

beforeEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

function baseCandidate(overrides: Partial<ExpansionCandidate> = {}): ExpansionCandidate {
  return {
    id: "cand_1",
    search_id: "search_1",
    parcel_id: "parcel_1",
    lat: 24.7,
    lon: 46.7,
    final_rank: 3,
    deterministic_rank: 3,
    rerank_applied: false,
    rerank_delta: 0,
    rerank_status: null,
    rerank_reason: null,
    ...overrides,
  };
}

function renderCard(
  candidate: ExpansionCandidate,
  opts: { onOpenMemo?: (options?: unknown) => void } = {},
) {
  const onOpenMemo = "onOpenMemo" in opts ? opts.onOpenMemo : () => undefined;
  return renderToStaticMarkup(
    <ExpansionCandidateCard
      candidate={candidate}
      selected={false}
      shortlisted={false}
      compared={false}
      onSelect={() => undefined}
      onCompareToggle={() => undefined}
      onOpenMemo={onOpenMemo as never}
    />,
  );
}

describe("ExpansionCandidateCard — Why #N chip (chunk 4)", () => {
  it("renders the base label when final_rank is present and no rerank applied", () => {
    const html = renderCard(baseCandidate({ final_rank: 3 }));
    expect(html).toContain("ea-candidate__why-chip");
    expect(html).toContain("Why #3");
    expect(html).not.toContain("↑");
    expect(html).not.toContain("↓");
  });

  it("renders the up-arrow + magnitude when rerank_applied and delta < 0", () => {
    const html = renderCard(
      baseCandidate({
        final_rank: 2,
        rerank_applied: true,
        rerank_status: "applied",
        rerank_delta: -2,
        rerank_reason: {
          summary: "Strong demand signals lifted this candidate.",
          positives_cited: [],
          negatives_cited: [],
          comparison_to_displaced_candidate: "",
        },
      }),
    );
    expect(html).toContain("Why #2");
    expect(html).toContain("↑2");
  });

  it("renders the down-arrow + magnitude when rerank_applied and delta > 0", () => {
    const html = renderCard(
      baseCandidate({
        final_rank: 5,
        rerank_applied: true,
        rerank_status: "applied",
        rerank_delta: 3,
        rerank_reason: {
          summary: "Negative risk factors outweighed the score.",
          positives_cited: [],
          negatives_cited: [],
          comparison_to_displaced_candidate: "",
        },
      }),
    );
    expect(html).toContain("Why #5");
    expect(html).toContain("↓3");
  });

  it.each<RerankStatus | null>([
    "unchanged",
    "outside_rerank_cap",
    "flag_off",
    "shortlist_below_minimum",
    "llm_failed",
    null,
  ])("does NOT render an arrow when rerank_status is %s", (status) => {
    const html = renderCard(
      baseCandidate({
        final_rank: 4,
        rerank_applied: true,
        rerank_status: status,
        rerank_delta: -2,
        rerank_reason: {
          summary: "x",
          positives_cited: [],
          negatives_cited: [],
          comparison_to_displaced_candidate: "",
        },
      }),
    );
    expect(html).toContain("Why #4");
    expect(html).not.toContain("↑");
    expect(html).not.toContain("↓");
  });

  it("does NOT render an arrow when rerank_delta is 0 even if applied", () => {
    const html = renderCard(
      baseCandidate({
        final_rank: 2,
        rerank_applied: true,
        rerank_status: "applied",
        rerank_delta: 0,
      }),
    );
    expect(html).toContain("Why #2");
    expect(html).not.toContain("↑");
    expect(html).not.toContain("↓");
  });

  it("does NOT render the chip at all when final_rank is null", () => {
    const html = renderCard(baseCandidate({ final_rank: null }));
    expect(html).not.toContain("ea-candidate__why-chip");
    expect(html).not.toContain("Why #");
  });

  it("does NOT render the chip at all when final_rank is undefined", () => {
    const c = baseCandidate();
    delete (c as { final_rank?: number | null }).final_rank;
    const html = renderCard(c);
    expect(html).not.toContain("ea-candidate__why-chip");
    expect(html).not.toContain("Why #");
  });

  it("sets the title attribute to rerank_reason.summary when the arrow is rendered", () => {
    const html = renderCard(
      baseCandidate({
        final_rank: 1,
        rerank_applied: true,
        rerank_status: "applied",
        rerank_delta: -1,
        rerank_reason: {
          summary: "Better fit than the deterministic winner.",
          positives_cited: [],
          negatives_cited: [],
          comparison_to_displaced_candidate: "",
        },
      }),
    );
    expect(html).toMatch(/title="Better fit than the deterministic winner\."/);
  });

  it("omits the title attribute when the arrow is not rendered", () => {
    const html = renderCard(
      baseCandidate({
        final_rank: 3,
        rerank_applied: false,
        rerank_reason: {
          summary: "Unused because no arrow.",
          positives_cited: [],
          negatives_cited: [],
          comparison_to_displaced_candidate: "",
        },
      }),
    );
    // The chip exists but has no title attribute — scope the check to the chip element.
    const match = html.match(/<button[^>]*ea-candidate__why-chip[^>]*>/);
    expect(match).not.toBeNull();
    expect(match![0]).not.toContain("title=");
  });

  it("renders the chip as a <button> so it's keyboard-accessible (not a <div>)", () => {
    const html = renderCard(baseCandidate({ final_rank: 3 }));
    expect(html).toMatch(/<button[^>]*ea-candidate__why-chip/);
  });

  it("renders the chip as disabled with aria-disabled='true' when onOpenMemo is undefined", () => {
    const html = renderCard(baseCandidate({ final_rank: 3 }), { onOpenMemo: undefined });
    const match = html.match(/<button[^>]*ea-candidate__why-chip[^>]*>/);
    expect(match).not.toBeNull();
    expect(match![0]).toContain('aria-disabled="true"');
    expect(match![0]).toContain("disabled");
    expect(match![0]).toContain("ea-candidate__why-chip--disabled");
  });

  it("is nested inside the card root (not identical to it) so card-level taps remain reachable", () => {
    const html = renderCard(baseCandidate({ final_rank: 3 }));
    const rootIdx = html.indexOf('<div class="ea-candidate');
    const chipIdx = html.indexOf("ea-candidate__why-chip");
    expect(rootIdx).toBeGreaterThan(-1);
    expect(chipIdx).toBeGreaterThan(rootIdx);
  });

  it("renders the English label 'Why' under English locale", () => {
    const html = renderCard(baseCandidate({ final_rank: 7 }));
    expect(html).toContain("Why #7");
  });

  it("renders the Arabic label 'لماذا' under Arabic locale", async () => {
    await i18n.changeLanguage("ar");
    try {
      const html = renderCard(baseCandidate({ final_rank: 7 }));
      expect(html).toContain("لماذا");
      expect(html).toContain("#7");
    } finally {
      await i18n.changeLanguage("en");
    }
  });
});
