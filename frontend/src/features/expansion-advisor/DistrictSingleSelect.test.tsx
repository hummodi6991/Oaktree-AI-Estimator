import { describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import DistrictSingleSelect from "./DistrictSingleSelect";
import type { DistrictOption } from "../../lib/api/expansionAdvisor";

const SAMPLE_OPTIONS: DistrictOption[] = [
  { value: "العليا", label: "العليا", label_ar: "العليا", label_en: "Al Olaya", aliases: [] },
  { value: "الملقا", label: "الملقا", label_ar: "الملقا", label_en: "Al Malqa", aliases: [] },
  { value: "النخيل", label: "النخيل", label_ar: "النخيل", label_en: "Al Nakheel", aliases: ["النخيل الشمالي"] },
];

describe("DistrictSingleSelect", () => {
  it("renders with correct structure and combobox role", () => {
    const html = renderToStaticMarkup(
      <DistrictSingleSelect
        options={SAMPLE_OPTIONS}
        value=""
        onChange={() => {}}
        placeholder="Select district"
      />,
    );
    expect(html).toContain("ea-district-ss");
    expect(html).toContain("ea-district-ss__control");
    expect(html).toContain("ea-district-ss__input");
    expect(html).toContain('role="combobox"');
    expect(html).toContain("Select district");
  });

  it("renders chevron indicator", () => {
    const html = renderToStaticMarkup(
      <DistrictSingleSelect
        options={SAMPLE_OPTIONS}
        value=""
        onChange={() => {}}
      />,
    );
    expect(html).toContain("ea-district-ss__chevron");
    expect(html).toContain("▼");
  });

  it("shows selected district label in input", () => {
    const html = renderToStaticMarkup(
      <DistrictSingleSelect
        options={SAMPLE_OPTIONS}
        value="العليا"
        onChange={() => {}}
      />,
    );
    // When not focused/open, should show the Arabic label as the input value
    expect(html).toContain("العليا");
  });

  it("shows clear button when value is selected", () => {
    const html = renderToStaticMarkup(
      <DistrictSingleSelect
        options={SAMPLE_OPTIONS}
        value="العليا"
        onChange={() => {}}
      />,
    );
    expect(html).toContain("ea-district-ss__clear");
  });

  it("does not show clear button when no value selected", () => {
    const html = renderToStaticMarkup(
      <DistrictSingleSelect
        options={SAMPLE_OPTIONS}
        value=""
        onChange={() => {}}
      />,
    );
    expect(html).not.toContain("ea-district-ss__clear");
  });

  it("renders disabled state", () => {
    const html = renderToStaticMarkup(
      <DistrictSingleSelect
        options={SAMPLE_OPTIONS}
        value=""
        onChange={() => {}}
        disabled
      />,
    );
    expect(html).toContain("ea-district-ss--disabled");
  });

  it("does not render dropdown when closed (server-rendered)", () => {
    const html = renderToStaticMarkup(
      <DistrictSingleSelect
        options={SAMPLE_OPTIONS}
        value=""
        onChange={() => {}}
      />,
    );
    expect(html).not.toContain("ea-district-ss__dropdown");
  });

  it("renders raw value when option not found", () => {
    const html = renderToStaticMarkup(
      <DistrictSingleSelect
        options={SAMPLE_OPTIONS}
        value="UnknownDistrict"
        onChange={() => {}}
      />,
    );
    // Falls back to rendering the raw value string
    expect(html).toContain("UnknownDistrict");
  });
});

describe("DistrictSingleSelect in manual branch form", () => {
  it("renders within branch manual form context", () => {
    // Verify the component can render alongside other form elements
    const html = renderToStaticMarkup(
      <div className="ea-branch-manual__row">
        <input className="ea-form__input" placeholder="Branch name" />
        <DistrictSingleSelect
          options={SAMPLE_OPTIONS}
          value="الملقا"
          onChange={() => {}}
          placeholder="District"
        />
      </div>,
    );
    expect(html).toContain("ea-form__input");
    expect(html).toContain("ea-district-ss");
    expect(html).toContain("الملقا");
  });
});
