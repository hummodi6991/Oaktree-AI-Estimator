import { describe, expect, it, beforeEach } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import "../../i18n";
import i18n from "../../i18n";
import TierBadge from "./TierBadge";

beforeEach(async () => {
  if (i18n.language !== "en") await i18n.changeLanguage("en");
});

function render(props: Partial<React.ComponentProps<typeof TierBadge>>) {
  return renderToStaticMarkup(
    <TierBadge sourceTier={1} listingUrl="https://example.com/listing" {...props} />,
  );
}

describe("TierBadge — Tier 1 listing link label", () => {
  it("renders 'View on Aqar' for an Aqar platform", () => {
    const html = render({ platform: "aqar" });
    expect(html).toContain("View on Aqar");
  });

  it("renders 'View on Bayut' for a Bayut platform", () => {
    const html = render({ platform: "bayut" });
    expect(html).toContain("View on Bayut");
  });

  it("renders the generic 'View listing' when platform is null", () => {
    const html = render({ platform: null });
    expect(html).toContain("View listing");
    expect(html).not.toContain("View on");
  });
});
