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

describe("ExpansionCandidateCard — Phase 4 pills (New / Updated / Active market)", () => {
  // Matrix states (spec §1(i)):
  //   #1 baseline     — no pills
  //   #2 New          — green "New" only
  //   #3 Updated      — green "Updated" only
  //   #5 Active       — amber "Active market" only
  //   #6 New + Active — both green "New" and amber "Active market"
  //   #7 Upd + Active — both green "Updated" and amber "Active market"
  // States #4, #8 are constant-fold impossible (source ∉ {aqar_created,
  // aqar_updated} ⇒ no freshness pill, by spec locked decision #1).

  function withSnapshot(
    extras: { listing_age?: unknown; district_momentum?: unknown },
    base: Partial<ExpansionCandidate> = {},
  ): ExpansionCandidate {
    return baseCandidate({
      ...base,
      feature_snapshot_json: {
        context_sources: {},
        missing_context: [],
        data_completeness_score: 0,
        ...extras,
      } as never,
    });
  }

  const NEUTRAL_MOMENTUM = { momentum_score: 50, sample_floor_applied: true };
  const ACTIVE_MOMENTUM = { momentum_score: 82, sample_floor_applied: false };

  it("state #1 (baseline): renders no Phase 4 pill", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: 30, source: "aqar_created" },
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).not.toContain("ea-candidate__freshness-pill");
    expect(html).not.toContain("ea-candidate__momentum-pill");
    expect(html).not.toContain(">New<");
    expect(html).not.toContain(">Updated<");
    expect(html).not.toContain(">Active market<");
  });

  it("state #2 (New only): renders the green New pill, not Updated", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: 2, source: "aqar_created" },
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).toContain("ea-candidate__freshness-pill");
    expect(html).toContain("ea-badge--green");
    expect(html).toContain(">New<");
    expect(html).not.toContain(">Updated<");
    expect(html).not.toContain(">Active market<");
  });

  it("state #3 (Updated only): renders the green Updated pill, not New", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: 4, source: "aqar_updated" },
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).toContain("ea-candidate__freshness-pill");
    expect(html).toContain("ea-badge--green");
    expect(html).toContain(">Updated<");
    expect(html).not.toContain(">New<");
    expect(html).not.toContain(">Active market<");
  });

  it("state #5 (Active market only): renders the amber pill, no freshness pill", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: 120, source: "aqar_updated" },
        district_momentum: ACTIVE_MOMENTUM,
      }),
    );
    expect(html).toContain("ea-candidate__momentum-pill");
    expect(html).toContain("ea-badge--amber");
    expect(html).toContain(">Active market<");
    expect(html).not.toContain("ea-candidate__freshness-pill");
    expect(html).not.toContain(">New<");
    expect(html).not.toContain(">Updated<");
  });

  it("state #6 (New + Active): renders both green New and amber Active market", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: 1, source: "aqar_created" },
        district_momentum: { momentum_score: 90, sample_floor_applied: false },
      }),
    );
    expect(html).toContain("ea-candidate__freshness-pill");
    expect(html).toContain("ea-candidate__momentum-pill");
    expect(html).toContain(">New<");
    expect(html).toContain(">Active market<");
    expect(html).not.toContain(">Updated<");
  });

  it("state #7 (Updated + Active): renders both green Updated and amber Active market", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: 6, source: "aqar_updated" },
        district_momentum: { momentum_score: 75, sample_floor_applied: false },
      }),
    );
    expect(html).toContain("ea-candidate__freshness-pill");
    expect(html).toContain("ea-candidate__momentum-pill");
    expect(html).toContain(">Updated<");
    expect(html).toContain(">Active market<");
    expect(html).not.toContain(">New<");
  });

  // ── Negative / boundary cases ──

  it("renders no pill when feature_snapshot_json is missing", () => {
    const html = renderCard(baseCandidate({ feature_snapshot_json: undefined as never }));
    expect(html).not.toContain("ea-candidate__freshness-pill");
    expect(html).not.toContain("ea-candidate__momentum-pill");
  });

  it("renders no freshness pill when effective_age_days is null and source is unknown", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: null, source: "unknown" },
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).not.toContain("ea-candidate__freshness-pill");
  });

  it("renders no freshness pill when source is first_seen even within 7 days", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: 3, source: "first_seen" },
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).not.toContain("ea-candidate__freshness-pill");
  });

  it("boundary: effective_age_days === 7 with aqar_created still renders New", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: 7, source: "aqar_created" },
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).toContain(">New<");
  });

  it("boundary: effective_age_days === 8 with aqar_created does NOT render New", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: 8, source: "aqar_created" },
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).not.toContain(">New<");
  });

  it("renders no active-market pill at momentum_score 69.99", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: 120, source: "aqar_updated" },
        district_momentum: { momentum_score: 69.99, sample_floor_applied: false },
      }),
    );
    expect(html).not.toContain("ea-candidate__momentum-pill");
    expect(html).not.toContain(">Active market<");
  });

  it("renders the active-market pill at momentum_score 70.00 exactly", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: 120, source: "aqar_updated" },
        district_momentum: { momentum_score: 70.0, sample_floor_applied: false },
      }),
    );
    expect(html).toContain("ea-candidate__momentum-pill");
    expect(html).toContain(">Active market<");
  });

  it("renders no active-market pill when sample_floor_applied is true", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: 120, source: "aqar_updated" },
        district_momentum: { momentum_score: 85, sample_floor_applied: true },
      }),
    );
    expect(html).not.toContain("ea-candidate__momentum-pill");
    expect(html).not.toContain(">Active market<");
  });

  it("renders Arabic 'جديد' / 'حي نشط' under Arabic locale", async () => {
    await i18n.changeLanguage("ar");
    try {
      const html = renderCard(
        withSnapshot({
          listing_age: { effective_age_days: 1, source: "aqar_created" },
          district_momentum: { momentum_score: 90, sample_floor_applied: false },
        }),
      );
      expect(html).toContain("جديد");
      expect(html).toContain("حي نشط");
      expect(html).not.toContain(">New<");
      expect(html).not.toContain(">Active market<");
    } finally {
      await i18n.changeLanguage("en");
    }
  });

  it("attaches the English tooltip text via title attribute on the New pill", () => {
    const html = renderCard(
      withSnapshot({
        listing_age: { effective_age_days: 2, source: "aqar_created" },
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).toMatch(/title="Listing newly created on Aqar within the last 7 days"/);
  });
});
