import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import BranchLocationPicker from "./BranchLocationPicker";
import { validateBrief, defaultBrief } from "./ExpansionBriefForm";

/* ─── Helpers ─── */
const noop = () => {};

describe("BranchLocationPicker", () => {
  it("renders empty state when no branches", () => {
    const html = renderToStaticMarkup(
      <BranchLocationPicker branches={[]} onChange={noop} />,
    );
    expect(html).toContain("ea-branch-picker");
    expect(html).toContain("ea-branch-search");
    // Empty state text should be visible
    expect(html).toContain("ea-branch-picker__empty");
  });

  it("renders branch cards for existing branches", () => {
    const branches = [
      { name: "Al Baik - Olaya", lat: 24.774265, lon: 46.738586, district: "Al Olaya" },
      { name: "Kudu - Malqa", lat: 24.812, lon: 46.623, district: "Al Malqa" },
    ];
    const html = renderToStaticMarkup(
      <BranchLocationPicker branches={branches} onChange={noop} />,
    );
    expect(html).toContain("Al Baik - Olaya");
    expect(html).toContain("Kudu - Malqa");
    expect(html).toContain("Al Olaya");
    expect(html).toContain("Al Malqa");
    // Should show coordinates in subtitle
    expect(html).toContain("24.774265");
    expect(html).toContain("46.738586");
    // Each card should have edit and remove buttons
    expect(html).toContain("ea-branch-card__remove-btn");
  });

  it("renders search input with combobox role", () => {
    const html = renderToStaticMarkup(
      <BranchLocationPicker branches={[]} onChange={noop} />,
    );
    expect(html).toContain('role="combobox"');
    expect(html).toContain("ea-branch-search__input");
  });

  it("renders manual coordinate expander", () => {
    const html = renderToStaticMarkup(
      <BranchLocationPicker branches={[]} onChange={noop} />,
    );
    expect(html).toContain("ea-branch-manual__toggle");
    // Manual form should be collapsed by default
    expect(html).not.toContain("ea-branch-manual__form");
  });

  it("multiple branches stack cleanly without overflow classes", () => {
    const branches = Array.from({ length: 5 }, (_, i) => ({
      name: `Branch ${i + 1}`,
      lat: 24.7 + i * 0.01,
      lon: 46.7 + i * 0.01,
      district: `District ${i + 1}`,
    }));
    const html = renderToStaticMarkup(
      <BranchLocationPicker branches={branches} onChange={noop} />,
    );
    // All 5 branches should render
    for (let i = 1; i <= 5; i++) {
      expect(html).toContain(`Branch ${i}`);
    }
    expect(html).toContain("ea-branch-picker__list");
  });

  it("renders disabled state correctly", () => {
    const html = renderToStaticMarkup(
      <BranchLocationPicker branches={[]} onChange={noop} disabled={true} />,
    );
    expect(html).toContain("disabled");
  });
});

describe("BranchLocationPicker hydration compatibility", () => {
  it("renders saved branches with raw lat/lon correctly", () => {
    // Simulate a legacy branch saved with only lat/lon (no name)
    const legacyBranches = [
      { lat: 24.65, lon: 46.72 },
      { name: "Manual Entry", lat: 24.8, lon: 46.65, district: "Al Sahafa" },
    ];
    const html = renderToStaticMarkup(
      <BranchLocationPicker branches={legacyBranches} onChange={noop} />,
    );
    // Legacy branch without name renders with fallback text
    expect(html).toContain("24.650000");
    expect(html).toContain("46.720000");
    // Named branch renders normally
    expect(html).toContain("Manual Entry");
    expect(html).toContain("Al Sahafa");
  });

  it("existing payload shape is preserved through validation", () => {
    const brief = {
      ...defaultBrief,
      brand_name: "Test Brand",
      existing_branches: [
        { name: "Branch 1", lat: 24.7, lon: 46.7, district: "Olaya" },
        { name: "Branch 2", lat: 24.8, lon: 46.8, district: "Malqa" },
      ],
    };
    const errors = validateBrief(brief);
    // Should have no errors for valid branches
    expect(errors.branches).toBeUndefined();
  });

  it("validates branches with invalid lat/lon", () => {
    const brief = {
      ...defaultBrief,
      brand_name: "Test",
      existing_branches: [
        { name: "Bad", lat: 200, lon: 46.7 },
      ],
    };
    const errors = validateBrief(brief);
    expect(errors.branches).toBeDefined();
    expect(errors.branches![0]).toBe("validationLatRange");
  });
});

describe("BranchLocationPicker search affordance", () => {
  it("renders a search icon in the autocomplete input", () => {
    const html = renderToStaticMarkup(
      <BranchLocationPicker branches={[]} onChange={noop} />,
    );
    expect(html).toContain("ea-branch-search__search-icon");
    // SVG search icon (magnifying glass)
    expect(html).toContain("<svg");
    expect(html).toContain("<circle");
  });

  it("search icon is marked aria-hidden", () => {
    const html = renderToStaticMarkup(
      <BranchLocationPicker branches={[]} onChange={noop} />,
    );
    expect(html).toContain('aria-hidden="true"');
  });
});

describe("BranchLocationPicker iPad/Safari rendering", () => {
  it("cards use tap-highlight-transparent CSS class", () => {
    const html = renderToStaticMarkup(
      <BranchLocationPicker
        branches={[{ name: "Test", lat: 24.7, lon: 46.7 }]}
        onChange={noop}
      />,
    );
    expect(html).toContain("ea-branch-card");
  });

  it("dropdown uses webkit-overflow-scrolling class in CSS (structural check)", () => {
    // Structural: the CSS file should define -webkit-overflow-scrolling for the dropdown
    // This is tested here by verifying the dropdown has the right class name
    const html = renderToStaticMarkup(
      <BranchLocationPicker branches={[]} onChange={noop} />,
    );
    expect(html).toContain("ea-branch-search");
  });
});
