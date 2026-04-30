import { describe, expect, it, beforeEach } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import "../../i18n";
import i18n from "../../i18n";
import ExpansionCandidateCard from "./ExpansionCandidateCard";
import { normalizeCandidate } from "../../lib/api/expansionAdvisor";
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

describe("ExpansionCandidateCard — Phase 4 pills (New / Updated / Top-tier market)", () => {
  // Matrix states (Phase 4.1):
  //   #1 baseline     — no pills
  //   #2 New          — green "New" only
  //   #3 Updated      — green "Updated" only (created > window, updated ≤ window)
  //   #5 Top-tier     — amber "Top-tier market" only
  //   #6 New + Top    — both green "New" and amber "Top-tier market"
  //   #7 Upd + Top    — both green "Updated" and amber "Top-tier market"
  // Phase 4.1: created_days and updated_days are surfaced as independent
  // fields on listing_age; the pill logic reads them directly instead of
  // branching on the GREATEST()-derived source tag. effective_age_days
  // and source are still populated for memo/rerank back-compat.

  function buildListingAge(
    createdDays: number | null,
    updatedDays: number | null,
  ) {
    // Populate effective_age_days and source for parity with production,
    // so we can assert the pill logic ignores them.
    const ageCandidates = [createdDays, updatedDays].filter(
      (x): x is number => x !== null,
    );
    const effective = ageCandidates.length ? Math.min(...ageCandidates) : null;
    const source =
      updatedDays !== null && updatedDays === effective
        ? "aqar_updated"
        : createdDays !== null && createdDays === effective
          ? "aqar_created"
          : "unknown";
    return {
      effective_age_days: effective,
      source,
      created_days: createdDays,
      updated_days: updatedDays,
    };
  }

  function withSnapshot(
    extras: {
      created_days?: number | null;
      updated_days?: number | null;
      listing_age?: unknown;
      district_momentum?: unknown;
    },
    base: Partial<ExpansionCandidate> = {},
  ): ExpansionCandidate {
    const { created_days, updated_days, listing_age, district_momentum, ...rest } = extras;
    const listingAge =
      listing_age !== undefined
        ? listing_age
        : created_days !== undefined || updated_days !== undefined
          ? buildListingAge(
              created_days === undefined ? null : created_days,
              updated_days === undefined ? null : updated_days,
            )
          : undefined;
    return baseCandidate({
      ...base,
      feature_snapshot_json: {
        context_sources: {},
        missing_context: [],
        data_completeness_score: 0,
        ...(listingAge !== undefined ? { listing_age: listingAge } : {}),
        ...(district_momentum !== undefined ? { district_momentum } : {}),
        ...rest,
      } as never,
    });
  }

  const NEUTRAL_MOMENTUM = { momentum_score: 50, sample_floor_applied: true };
  const ACTIVE_MOMENTUM = { momentum_score: 82, sample_floor_applied: false };

  it("state #1 (baseline): renders no Phase 4 pill", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 30,
        updated_days: 30,
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).not.toContain("ea-candidate__freshness-pill");
    expect(html).not.toContain("ea-candidate__momentum-pill");
    expect(html).not.toContain(">New<");
    expect(html).not.toContain(">Updated<");
    expect(html).not.toContain(">Top-tier market<");
  });

  it("state #2 (New only): renders the green New pill, not Updated", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 2,
        updated_days: 2,
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).toContain("ea-candidate__freshness-pill");
    expect(html).toContain("ea-badge--green");
    expect(html).toContain(">New<");
    expect(html).not.toContain(">Updated<");
    expect(html).not.toContain(">Top-tier market<");
  });

  it("state #3 (Updated only): renders the green Updated pill, not New", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 30,
        updated_days: 4,
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).toContain("ea-candidate__freshness-pill");
    expect(html).toContain("ea-badge--green");
    expect(html).toContain(">Updated<");
    expect(html).not.toContain(">New<");
    expect(html).not.toContain(">Top-tier market<");
  });

  it("state #5 (Top-tier market only): renders the amber pill, no freshness pill", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 120,
        updated_days: 120,
        district_momentum: ACTIVE_MOMENTUM,
      }),
    );
    expect(html).toContain("ea-candidate__momentum-pill");
    expect(html).toContain("ea-badge--amber");
    expect(html).toContain(">Top-tier market<");
    expect(html).not.toContain("ea-candidate__freshness-pill");
    expect(html).not.toContain(">New<");
    expect(html).not.toContain(">Updated<");
  });

  it("state #6 (New + Top-tier): renders both green New and amber Top-tier market", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 1,
        updated_days: 1,
        district_momentum: { momentum_score: 90, sample_floor_applied: false },
      }),
    );
    expect(html).toContain("ea-candidate__freshness-pill");
    expect(html).toContain("ea-candidate__momentum-pill");
    expect(html).toContain(">New<");
    expect(html).toContain(">Top-tier market<");
    expect(html).not.toContain(">Updated<");
  });

  it("state #7 (Updated + Top-tier): renders both green Updated and amber Top-tier market", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 30,
        updated_days: 6,
        district_momentum: { momentum_score: 75, sample_floor_applied: false },
      }),
    );
    expect(html).toContain("ea-candidate__freshness-pill");
    expect(html).toContain("ea-candidate__momentum-pill");
    expect(html).toContain(">Updated<");
    expect(html).toContain(">Top-tier market<");
    expect(html).not.toContain(">New<");
  });

  // ── Negative / boundary cases ──

  it("renders no pill when feature_snapshot_json is missing", () => {
    const html = renderCard(baseCandidate({ feature_snapshot_json: undefined as never }));
    expect(html).not.toContain("ea-candidate__freshness-pill");
    expect(html).not.toContain("ea-candidate__momentum-pill");
  });

  it("renders no freshness pill when both created_days and updated_days are null", () => {
    const html = renderCard(
      withSnapshot({
        created_days: null,
        updated_days: null,
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).not.toContain("ea-candidate__freshness-pill");
  });

  it("boundary: created_days === 7 still renders New", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 7,
        updated_days: 7,
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).toContain(">New<");
  });

  it("boundary: created_days === 8 (and updated_days > window) does NOT render New", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 8,
        updated_days: 8,
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).not.toContain(">New<");
  });

  it("renders no top-tier-market pill at momentum_score 69.99", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 120,
        updated_days: 120,
        district_momentum: { momentum_score: 69.99, sample_floor_applied: false },
      }),
    );
    expect(html).not.toContain("ea-candidate__momentum-pill");
    expect(html).not.toContain(">Top-tier market<");
  });

  it("renders the top-tier-market pill at momentum_score 70.00 exactly", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 120,
        updated_days: 120,
        district_momentum: { momentum_score: 70.0, sample_floor_applied: false },
      }),
    );
    expect(html).toContain("ea-candidate__momentum-pill");
    expect(html).toContain(">Top-tier market<");
  });

  it("renders no top-tier-market pill when sample_floor_applied is true", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 120,
        updated_days: 120,
        district_momentum: { momentum_score: 85, sample_floor_applied: true },
      }),
    );
    expect(html).not.toContain("ea-candidate__momentum-pill");
    expect(html).not.toContain(">Top-tier market<");
  });

  it("renders Arabic 'جديد' / 'حي ضمن الفئة الأعلى' under Arabic locale", async () => {
    await i18n.changeLanguage("ar");
    try {
      const html = renderCard(
        withSnapshot({
          created_days: 1,
          updated_days: 1,
          district_momentum: { momentum_score: 90, sample_floor_applied: false },
        }),
      );
      expect(html).toContain("جديد");
      expect(html).toContain("حي ضمن الفئة الأعلى");
      expect(html).not.toContain(">New<");
      expect(html).not.toContain(">Top-tier market<");
    } finally {
      await i18n.changeLanguage("en");
    }
  });

  it("attaches the English tooltip text via title attribute on the New pill", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 2,
        updated_days: 2,
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).toMatch(/title="Listing newly created on Aqar within the last 7 days"/);
  });

  it("attaches the English tooltip text via title attribute on the Top-tier market pill", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 120,
        updated_days: 120,
        district_momentum: ACTIVE_MOMENTUM,
      }),
    );
    expect(html).toMatch(
      /title="Ranks in the top tier of Riyadh districts by recent listing activity"/,
    );
  });

  // ── Phase 4.1 regression pins ──

  it("state: new wins when both days are fresh (scraper-cadence regression)", () => {
    // Before 4.1, `source` was set by GREATEST() which the scraper's
    // daily cadence biased toward aqar_updated on ~93% of rows. That
    // pushed this row to "Updated" even though it was genuinely new.
    // After 4.1, created_days within the window wins unconditionally.
    const html = renderCard(
      withSnapshot({
        created_days: 2,
        updated_days: 2,
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).toContain(">New<");
    expect(html).not.toContain(">Updated<");
  });

  it("state: updated fires only when created is older than window", () => {
    const html = renderCard(
      withSnapshot({
        created_days: 30,
        updated_days: 3,
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).toContain(">Updated<");
    expect(html).not.toContain(">New<");
  });

  it("state: new fires even when updated is also fresh", () => {
    // Pins the scraper-cadence regression: even when updated_days=1
    // would have won the old GREATEST() tie-break, created_days=3
    // within the window makes this "New".
    const html = renderCard(
      withSnapshot({
        created_days: 3,
        updated_days: 1,
        district_momentum: NEUTRAL_MOMENTUM,
      }),
    );
    expect(html).toContain(">New<");
    expect(html).not.toContain(">Updated<");
  });
});

describe("ExpansionCandidateCard — stringified-Decimal coercion at boundary", () => {
  it("renders area, annual rent and fitout cost as real values when API returns strings", () => {
    // Mirrors the real backend response shape: SQLAlchemy Numeric columns
    // come over the wire as strings (e.g. "150.00"). Pre-coercion this
    // produced "—" placeholders for area / annual-rent / fitout chips.
    const apiShape = {
      ...baseCandidate({ final_rank: 1 }),
      area_m2: "150.00" as unknown as number,
      display_annual_rent_sar: "210000.00" as unknown as number,
      estimated_annual_rent_sar: "210000.00" as unknown as number,
      estimated_fitout_cost_sar: "85000.00" as unknown as number,
      distance_to_nearest_branch_m: "2300.00" as unknown as number,
    };
    const candidate = normalizeCandidate(apiShape);
    const html = renderCard(candidate);
    // Area chip
    expect(html).toContain("150 m²");
    // Annual rent chip — formatted as compact "SAR 210K"
    expect(html).toContain("SAR 210K");
    // Should not show the fallback in the metrics row.
    const metricsBlock = html.split("ea-candidate__metrics")[1] || "";
    expect(metricsBlock).not.toMatch(/—\/yr/);
  });
});

describe("ExpansionCandidateCard — value_score badge", () => {
  it("renders the green Best value badge when value_band is best_value (high confidence)", () => {
    const html = renderCard(
      baseCandidate({
        value_band: "best_value",
        value_band_low_confidence: false,
        value_score: 82,
      }),
    );
    expect(html).toContain("ea-candidate__value-pill");
    expect(html).toContain("ea-badge--green");
    expect(html).toContain(">Best value");
    expect(html).not.toContain("ⓘ");
  });

  it("renders the green Best value badge with an ⓘ mark when value_band_low_confidence is true", () => {
    const html = renderCard(
      baseCandidate({
        value_band: "best_value",
        value_band_low_confidence: true,
        value_score: 78,
      }),
    );
    expect(html).toContain("ea-candidate__value-pill");
    expect(html).toContain("ea-badge--green");
    expect(html).toContain("ⓘ");
  });

  it("renders the red Above market badge when value_band is above_market and high confidence", () => {
    const html = renderCard(
      baseCandidate({
        value_band: "above_market",
        value_band_low_confidence: false,
        value_score: 18,
      }),
    );
    expect(html).toContain("ea-candidate__value-pill");
    expect(html).toContain("ea-badge--red");
    expect(html).toContain(">Above market<");
  });

  it("renders the AMBER (not red) Above market badge when value_band is above_market and low confidence", () => {
    // Per product override 2: low-confidence above_market keeps the badge
    // visible but with amber styling — the directional signal is preserved
    // while the visual penalty is muted.
    const html = renderCard(
      baseCandidate({
        value_band: "above_market",
        value_band_low_confidence: true,
        value_score: 22,
      }),
    );
    expect(html).toContain("ea-candidate__value-pill");
    expect(html).toContain("ea-badge--amber");
    expect(html).not.toContain("ea-badge--red");
    expect(html).toContain("Above market (citywide est.)");
  });

  it("renders no value badge when value_band is neutral or null", () => {
    const htmlNeutral = renderCard(
      baseCandidate({ value_band: "neutral", value_score: 55 }),
    );
    expect(htmlNeutral).not.toContain("ea-candidate__value-pill");
    const htmlNull = renderCard(baseCandidate());
    expect(htmlNull).not.toContain("ea-candidate__value-pill");
  });
});
