import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import "../../i18n";
import type { BriefExtractionResult } from "../../lib/api/expansionAdvisor";
import BriefExtractionPanel from "./BriefExtractionPanel";

const noop = () => {};

function render(result: BriefExtractionResult, discarded: Set<string> = new Set()) {
  return renderToStaticMarkup(
    <BriefExtractionPanel
      result={result}
      discarded={discarded}
      onToggleDiscard={noop}
      onApply={noop}
      onDismiss={noop}
    />,
  );
}

const PROPOSAL_RESULT: BriefExtractionResult = {
  proposal: {
    brand_archetype: { value: "street_flagship", confidence: "high", evidence: "flagship presence" },
    parking_sensitivity: { value: "high", confidence: "medium", evidence: "parking is critical" },
  },
  unrecognized_districts: [],
  conflicts: [],
  memo_color: ["drive-thru format"],
};

describe("BriefExtractionPanel", () => {
  it("renders proposal chips with confidence grammar and evidence tooltip", () => {
    const html = render(PROPOSAL_RESULT);
    expect(html).toContain("Reading your brief as:");
    expect(html).toContain("Street flagship");
    expect(html).toContain("ea-brief-chip--high");
    expect(html).toContain("ea-brief-chip--medium");
    expect(html).toContain("ea-badge--green");
    expect(html).toContain("ea-badge--amber");
    expect(html).toContain('title="flagship presence"');
    expect(html).toContain("Apply");
    // Unhomed traits surface as memo color, explicitly marked as unscored.
    expect(html).toContain("Noted for the memo (not scored):");
    expect(html).toContain("drive-thru format");
  });

  it("marks discarded chips", () => {
    const html = render(PROPOSAL_RESULT, new Set(["parking_sensitivity"]));
    expect(html).toContain("ea-brief-chip--discarded");
  });

  it("renders the conflict callout instead of silently overriding", () => {
    const html = render({
      proposal: {},
      unrecognized_districts: [],
      conflicts: [
        {
          field: "service_model",
          evidence: "مقهى قهوة مختصة",
          note: "Text describes a specialty café but the form selects quick service (qsr).",
        },
      ],
      memo_color: [],
    });
    expect(html).toContain("Your description conflicts with the form");
    expect(html).toContain("specialty café");
    expect(html).toContain("مقهى قهوة مختصة");
  });

  it("renders unrecognized districts as non-applyable warning chips", () => {
    const html = render({
      proposal: {},
      unrecognized_districts: ["شمال الرياض", "Dubai Marina"],
      conflicts: [],
      memo_color: [],
    });
    expect(html).toContain("Not recognized as Riyadh districts:");
    expect(html).toContain("ea-district-ms__chip--fallback");
    expect(html).toContain("شمال الرياض");
    expect(html).toContain("Dubai Marina");
  });

  it("renders the empty state when nothing was extracted", () => {
    const html = render({
      proposal: {},
      unrecognized_districts: [],
      conflicts: [],
      memo_color: [],
    });
    expect(html).toContain("brief-panel-empty");
    expect(html).toContain("fill the form manually");
    expect(html).not.toContain("Apply");
  });
});
