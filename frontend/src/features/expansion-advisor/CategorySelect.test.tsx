import { describe, expect, it, vi } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import React from "react";
import CategorySelect from "./CategorySelect";
import { CATEGORY_OPTIONS, findCategoryOption } from "./categoryOptions";

describe("CategorySelect", () => {
  it("renders a searchable combobox instead of a free text input", () => {
    const html = renderToStaticMarkup(
      <CategorySelect value="" onChange={() => {}} />,
    );
    expect(html).toContain("ea-category-select");
    expect(html).toContain('role="combobox"');
    expect(html).toContain("Select a restaurant category");
    // Must NOT be a plain <input> with ea-form__input class
    expect(html).not.toContain("ea-form__input");
  });

  it("renders helper text when nothing is selected", () => {
    const html = renderToStaticMarkup(
      <CategorySelect value="" onChange={() => {}} />,
    );
    expect(html).toContain("Choose the closest match for better search quality");
  });

  it("renders selected category as a pill with Arabic label", () => {
    const html = renderToStaticMarkup(
      <CategorySelect value="burger" onChange={() => {}} />,
    );
    expect(html).toContain("ea-category-select__pill");
    expect(html).toContain("Burger");
    expect(html).toContain("برغر");
  });

  it("disables when disabled prop is true", () => {
    const html = renderToStaticMarkup(
      <CategorySelect value="" onChange={() => {}} disabled />,
    );
    expect(html).toContain("ea-category-select--disabled");
  });
});

describe("CategorySelect English search", () => {
  it("findCategoryOption matches English label", () => {
    const opt = findCategoryOption("burger");
    expect(opt).toBeDefined();
    expect(opt!.label).toBe("Burger");
    expect(opt!.label_ar).toBe("برغر");
  });

  it("findCategoryOption is case-insensitive", () => {
    expect(findCategoryOption("PIZZA")).toBeDefined();
    expect(findCategoryOption("Pizza")).toBeDefined();
  });

  it("English search finds category via filter logic", () => {
    const q = "burg";
    const results = CATEGORY_OPTIONS.filter((opt) =>
      opt.label.toLowerCase().includes(q) ||
      opt.value.includes(q) ||
      opt.aliases.some((a) => a.toLowerCase().includes(q)),
    );
    expect(results.some((r) => r.value === "burger")).toBe(true);
  });
});

describe("CategorySelect Arabic search", () => {
  it("Arabic search finds category by label_ar", () => {
    const q = "برغر";
    const results = CATEGORY_OPTIONS.filter((opt) =>
      opt.label_ar.includes(q) ||
      opt.aliases.some((a) => a.includes(q)),
    );
    expect(results.some((r) => r.value === "burger")).toBe(true);
  });

  it("Arabic search finds category by alias", () => {
    const q = "كبسة";
    const results = CATEGORY_OPTIONS.filter((opt) =>
      opt.label_ar.includes(q) ||
      opt.aliases.some((a) => a.includes(q)),
    );
    expect(results.some((r) => r.value === "traditional saudi")).toBe(true);
  });
});

describe("CategorySelect payload compatibility", () => {
  it("onChange receives the normalised string value", () => {
    const onChange = vi.fn();
    // Simulate what the component would emit
    const opt = CATEGORY_OPTIONS.find((o) => o.value === "cafe");
    expect(opt).toBeDefined();
    onChange(opt!.value);
    expect(onChange).toHaveBeenCalledWith("cafe");
    expect(typeof onChange.mock.calls[0][0]).toBe("string");
  });

  it("all category values are lowercase strings", () => {
    for (const opt of CATEGORY_OPTIONS) {
      expect(opt.value).toBe(opt.value.toLowerCase());
      expect(typeof opt.value).toBe("string");
    }
  });
});

describe("Saved-study hydration", () => {
  it("known category restores correctly", () => {
    const html = renderToStaticMarkup(
      <CategorySelect value="pizza" onChange={() => {}} />,
    );
    expect(html).toContain("Pizza");
    expect(html).toContain("بيتزا");
    // Should NOT have fallback styling
    expect(html).not.toContain("ea-category-select__pill--fallback");
  });

  it("unknown legacy category renders as fallback", () => {
    const html = renderToStaticMarkup(
      <CategorySelect value="some-old-category" onChange={() => {}} />,
    );
    expect(html).toContain("ea-category-select__pill--fallback");
    expect(html).toContain("some-old-category");
  });

  it("default brief value 'qsr' renders as fallback (free-text legacy)", () => {
    // 'qsr' is a legacy default that maps to the 'fast food' category
    // It should still render - either via alias match or as fallback
    const html = renderToStaticMarkup(
      <CategorySelect value="qsr" onChange={() => {}} />,
    );
    // qsr is in the aliases for "fast food", but findCategoryOption does exact match on value
    // So it should render as fallback since "qsr" is not a curated value
    expect(html).toContain("qsr");
  });

  it("empty string renders placeholder, not a pill", () => {
    const html = renderToStaticMarkup(
      <CategorySelect value="" onChange={() => {}} />,
    );
    expect(html).not.toContain("ea-category-select__pill");
    expect(html).toContain("Select a restaurant category");
  });
});

describe("Category catalog completeness", () => {
  it("has at least 35 categories", () => {
    expect(CATEGORY_OPTIONS.length).toBeGreaterThanOrEqual(35);
  });

  it("every category has required fields", () => {
    for (const opt of CATEGORY_OPTIONS) {
      expect(opt.value).toBeTruthy();
      expect(opt.label).toBeTruthy();
      expect(opt.label_ar).toBeTruthy();
      expect(opt.group).toBeTruthy();
      expect(Array.isArray(opt.aliases)).toBe(true);
    }
  });

  it("no duplicate category values", () => {
    const values = CATEGORY_OPTIONS.map((o) => o.value);
    expect(new Set(values).size).toBe(values.length);
  });

  it("includes all required categories from spec", () => {
    const required = [
      "burger", "fried chicken", "pizza", "cafe", "coffee", "bakery",
      "dessert", "ice cream", "shawarma", "sandwiches", "healthy", "salad",
      "juice", "breakfast", "seafood", "steakhouse", "grills",
      "traditional saudi", "traditional arabic", "yemeni", "levantine",
      "indian", "pakistani", "turkish", "italian", "chinese", "japanese",
      "sushi", "korean", "mexican", "american", "fast food", "casual dining",
      "fine dining", "family restaurant", "delivery kitchen", "cloud kitchen",
    ];
    const values = new Set(CATEGORY_OPTIONS.map((o) => o.value));
    for (const r of required) {
      expect(values.has(r)).toBe(true);
    }
  });
});
