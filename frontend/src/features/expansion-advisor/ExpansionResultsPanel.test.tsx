import { describe, expect, it, beforeEach } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import "../../i18n";
import i18n from "../../i18n";
import ExpansionResultsPanel from "./ExpansionResultsPanel";
import type { ExpansionCandidate } from "../../lib/api/expansionAdvisor";

beforeEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

function baseCandidate(overrides: Partial<ExpansionCandidate> = {}): ExpansionCandidate {
  return {
    id: "c1",
    search_id: "s1",
    parcel_id: "p1",
    lat: 24.7,
    lon: 46.7,
    rank_position: 1,
    ...overrides,
  };
}

function render(items: ExpansionCandidate[]) {
  return renderToStaticMarkup(
    <ExpansionResultsPanel
      items={items}
      selectedCandidateId={null}
      shortlistIds={[]}
      compareIds={[]}
      onSelectCandidate={() => undefined}
      onToggleCompare={() => undefined}
    />,
  );
}

const PREMIER_OVERRIDES: Partial<ExpansionCandidate> = {
  confidence_grade: "A",
  final_score: 80,
  gate_status_json: { overall_pass: true },
};

const EXPLORATORY_OVERRIDES: Partial<ExpansionCandidate> = {
  confidence_grade: "C",
  final_score: 40,
  gate_status_json: { overall_pass: false },
};

describe("ExpansionResultsPanel — flat rank-ordered render", () => {
  it("never renders tier section headers (no grouping in the panel)", () => {
    const items = [
      baseCandidate({ id: "c1", rank_position: 1, ...PREMIER_OVERRIDES }),
      baseCandidate({ id: "c2", rank_position: 2 }),
      baseCandidate({ id: "c3", rank_position: 3, ...EXPLORATORY_OVERRIDES }),
    ];
    const html = render(items);
    expect(html).not.toContain("ea-candidate-list__section-header");
    expect(html).not.toContain("Premier — best of the best");
    expect(html).not.toContain("Also consider");
  });

  it("renders candidates in the exact order supplied by the backend", () => {
    // Mixed tiers in backend rank order: standard, standard, premier,
    // standard, exploratory. The panel must preserve this order — a
    // Premier card at rank 3 sits at DOM position 3, not lifted to the top.
    const items = [
      baseCandidate({ id: "c1", rank_position: 1 }),
      baseCandidate({ id: "c2", rank_position: 2 }),
      baseCandidate({ id: "c3", rank_position: 3, ...PREMIER_OVERRIDES }),
      baseCandidate({ id: "c4", rank_position: 4 }),
      baseCandidate({ id: "c5", rank_position: 5, ...EXPLORATORY_OVERRIDES }),
    ];
    const html = render(items);
    const positions = ["c1", "c2", "c3", "c4", "c5"].map((id) =>
      html.indexOf(`data-candidate-id="${id}"`),
    );
    positions.forEach((p) => expect(p).toBeGreaterThan(-1));
    for (let i = 1; i < positions.length; i += 1) {
      expect(positions[i]).toBeGreaterThan(positions[i - 1]);
    }
  });

  it("renders the Premier chip on Premier cards (in-card, not under a header)", () => {
    const items = [baseCandidate({ id: "c1", rank_position: 1, ...PREMIER_OVERRIDES })];
    const html = render(items);
    expect(html).toContain("ea-candidate--premier");
    expect(html).toContain("ea-candidate__premier-pill");
    expect(html).toContain(">Premier<");
  });

  it("renders the Exploratory chip on Exploratory cards", () => {
    const items = [baseCandidate({ id: "c1", rank_position: 1, ...EXPLORATORY_OVERRIDES })];
    const html = render(items);
    expect(html).toContain("ea-candidate--exploratory");
    expect(html).toContain("ea-candidate__exploratory-pill");
    expect(html).toContain(">Exploratory<");
  });

  it("renders no tier chip on Standard cards", () => {
    const items = [baseCandidate({ id: "c1", rank_position: 1 })];
    const html = render(items);
    expect(html).not.toContain("ea-candidate__premier-pill");
    expect(html).not.toContain("ea-candidate__exploratory-pill");
  });

  it("handles an empty shortlist", () => {
    const html = render([]);
    expect(html).not.toContain("ea-candidate-list__section-header");
    expect(html).toContain("ea-candidate-list");
  });

  it("preserves global rank numbers across mixed tiers", () => {
    const items = [
      baseCandidate({ id: "c1", rank_position: 1, ...PREMIER_OVERRIDES }),
      baseCandidate({ id: "c2", rank_position: 2 }),
      baseCandidate({ id: "c3", rank_position: 3, ...EXPLORATORY_OVERRIDES }),
    ];
    const html = render(items);
    expect(html).toContain(">#1<");
    expect(html).toContain(">#2<");
    expect(html).toContain(">#3<");
  });
});
