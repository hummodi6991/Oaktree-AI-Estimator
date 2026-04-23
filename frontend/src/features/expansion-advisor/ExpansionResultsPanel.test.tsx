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

describe("ExpansionResultsPanel — tier section headers", () => {
  it("renders no section headers when every candidate is Standard", () => {
    const items = [
      baseCandidate({ id: "c1", rank_position: 1 }),
      baseCandidate({ id: "c2", rank_position: 2 }),
      baseCandidate({ id: "c3", rank_position: 3 }),
    ];
    const html = render(items);
    expect(html).not.toContain("ea-candidate-list__section-header");
    expect(html).not.toContain("Premier — best of the best");
    expect(html).not.toContain("Also consider");
  });

  it("renders the Premier header only when a Premier candidate exists", () => {
    const items = [
      baseCandidate({ id: "c1", rank_position: 1, ...PREMIER_OVERRIDES }),
      baseCandidate({ id: "c2", rank_position: 2 }),
    ];
    const html = render(items);
    expect(html).toContain("Premier — best of the best");
    expect(html).not.toContain("Also consider");
  });

  it("renders the Exploratory header only when an Exploratory candidate exists", () => {
    const items = [
      baseCandidate({ id: "c1", rank_position: 1 }),
      baseCandidate({ id: "c2", rank_position: 2, ...EXPLORATORY_OVERRIDES }),
    ];
    const html = render(items);
    expect(html).not.toContain("Premier — best of the best");
    expect(html).toContain("Also consider");
  });

  it("renders both headers when both tiers are populated", () => {
    const items = [
      baseCandidate({ id: "c1", rank_position: 1, ...PREMIER_OVERRIDES }),
      baseCandidate({ id: "c2", rank_position: 2 }),
      baseCandidate({ id: "c3", rank_position: 3, ...EXPLORATORY_OVERRIDES }),
    ];
    const html = render(items);
    expect(html).toContain("Premier — best of the best");
    expect(html).toContain("Also consider");
    // Premier section must appear before Exploratory section in the DOM.
    const premierIdx = html.indexOf("Premier — best of the best");
    const exploratoryIdx = html.indexOf("Also consider");
    expect(premierIdx).toBeGreaterThan(-1);
    expect(exploratoryIdx).toBeGreaterThan(premierIdx);
  });

  it("renders the Premier pill on Premier cards", () => {
    const items = [baseCandidate({ id: "c1", rank_position: 1, ...PREMIER_OVERRIDES })];
    const html = render(items);
    expect(html).toContain("ea-candidate--premier");
    expect(html).toContain("ea-candidate__premier-pill");
    expect(html).toContain(">Premier<");
  });

  it("applies the exploratory class (muted) on Exploratory cards", () => {
    const items = [baseCandidate({ id: "c1", rank_position: 1, ...EXPLORATORY_OVERRIDES })];
    const html = render(items);
    expect(html).toContain("ea-candidate--exploratory");
    // No "Exploratory" label on the card itself — the section header carries it.
    expect(html).not.toContain(">Exploratory<");
  });

  it("handles an empty shortlist", () => {
    const html = render([]);
    expect(html).not.toContain("ea-candidate-list__section-header");
    expect(html).toContain("ea-candidate-list");
  });

  it("preserves global rank numbers across tier groups (not reset per tier)", () => {
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
