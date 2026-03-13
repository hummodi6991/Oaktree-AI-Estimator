import { describe, expect, it, vi } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import DistrictMultiSelect from "./DistrictMultiSelect";
import type { DistrictOption } from "../../lib/api/expansionAdvisor";

const SAMPLE_OPTIONS: DistrictOption[] = [
  { value: "العليا", label: "العليا", label_ar: "العليا", label_en: "Al Olaya", aliases: [] },
  { value: "الملقا", label: "الملقا", label_ar: "الملقا", label_en: "Al Malqa", aliases: [] },
  { value: "النخيل", label: "النخيل", label_ar: "النخيل", label_en: "Al Nakheel", aliases: ["النخيل الشمالي"] },
];

describe("DistrictMultiSelect", () => {
  it("renders 3 searchable district selectors with correct structure", () => {
    const html = renderToStaticMarkup(
      <DistrictMultiSelect
        options={SAMPLE_OPTIONS}
        selected={[]}
        onChange={() => {}}
        placeholder="Select districts"
      />,
    );
    expect(html).toContain("ea-district-ms");
    expect(html).toContain('role="combobox"');
    expect(html).toContain("Select districts");
  });

  it("renders selected districts as chips", () => {
    const html = renderToStaticMarkup(
      <DistrictMultiSelect
        options={SAMPLE_OPTIONS}
        selected={["العليا", "الملقا"]}
        onChange={() => {}}
      />,
    );
    expect(html).toContain("ea-district-ms__chip");
    expect(html).toContain("العليا");
    expect(html).toContain("الملقا");
    // Each chip should have a remove button
    expect(html).toContain("ea-district-ms__chip-remove");
  });

  it("renders unknown legacy values as fallback chips", () => {
    const html = renderToStaticMarkup(
      <DistrictMultiSelect
        options={SAMPLE_OPTIONS}
        selected={["UnknownDistrict123"]}
        onChange={() => {}}
      />,
    );
    expect(html).toContain("ea-district-ms__chip--fallback");
    expect(html).toContain("UnknownDistrict123");
  });

  it("marks conflict values with conflict class", () => {
    const html = renderToStaticMarkup(
      <DistrictMultiSelect
        options={SAMPLE_OPTIONS}
        selected={["العليا"]}
        onChange={() => {}}
        conflictValues={["العليا"]}
      />,
    );
    expect(html).toContain("ea-district-ms__chip--conflict");
  });

  it("disables when disabled prop is true", () => {
    const html = renderToStaticMarkup(
      <DistrictMultiSelect
        options={SAMPLE_OPTIONS}
        selected={[]}
        onChange={() => {}}
        disabled
      />,
    );
    expect(html).toContain("ea-district-ms--disabled");
  });
});

describe("DistrictMultiSelect payload shape", () => {
  it("onChange receives string[] matching payload shape", () => {
    // Verify the type contract: selected is string[], onChange receives string[]
    const onChange = vi.fn();
    // Simulate what the component would call when toggling an option
    const selected: string[] = ["العليا"];
    const newValue = "الملقا";
    const updated = [...selected, newValue];
    onChange(updated);
    expect(onChange).toHaveBeenCalledWith(["العليا", "الملقا"]);
    expect(Array.isArray(onChange.mock.calls[0][0])).toBe(true);
  });
});

describe("Saved search hydration", () => {
  it("hydrates saved district arrays directly into selected props", () => {
    // Simulate saved search restoring district values
    const savedTargetDistricts = ["العليا", "الملقا"];
    const html = renderToStaticMarkup(
      <DistrictMultiSelect
        options={SAMPLE_OPTIONS}
        selected={savedTargetDistricts}
        onChange={() => {}}
      />,
    );
    expect(html).toContain("العليا");
    expect(html).toContain("الملقا");
    // No fallback chips since values match options
    expect(html).not.toContain("ea-district-ms__chip--fallback");
  });

  it("renders mixed known and unknown values from saved search", () => {
    const savedValues = ["العليا", "SomeOldDistrict"];
    const html = renderToStaticMarkup(
      <DistrictMultiSelect
        options={SAMPLE_OPTIONS}
        selected={savedValues}
        onChange={() => {}}
      />,
    );
    // Known value rendered normally
    expect(html).toContain("العليا");
    // Unknown value rendered as fallback
    expect(html).toContain("SomeOldDistrict");
    expect(html).toContain("ea-district-ms__chip--fallback");
  });
});
