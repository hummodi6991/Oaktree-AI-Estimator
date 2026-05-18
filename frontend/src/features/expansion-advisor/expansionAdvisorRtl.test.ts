import { describe, expect, it } from "vitest";
import css from "./expansion-advisor.css?raw";

/*
 * RTL audit guard (PR-FE-AR). Vitest runs in Node with no browser, so
 * computed-style resolution of logical properties is out of scope here
 * (verified via the manual browser checklist + deferred Playwright per
 * Q3-b). What we CAN assert deterministically is that the physical-
 * direction declarations in expansion-advisor.css were converted to
 * logical properties so the layout mirrors under dir="rtl".
 */

describe("expansion-advisor.css — RTL logical-property audit", () => {
  it("uses logical properties for the converted declarations", () => {
    expect(css).toContain("border-inline-start: 3px solid var(--oak-primary");
    expect(css).toContain("padding-inline-end: 30px");
    expect(css).toContain("inset-inline-start: 10px");
    expect(css).toContain("inset-inline-end: 10px");
    expect(css).toContain("padding-inline-start: 30px !important");
    expect(css).toContain("padding-inline-end: 32px !important");
    expect(css).toContain("margin-inline-start: auto");
    expect(css).toContain("margin-inline-start: 2px");
  });

  it("removed the premier-card [dir=rtl] override (logical border mirrors automatically)", () => {
    expect(css).not.toContain('[dir="rtl"] .ea-candidate--premier');
  });

  it("documents the single scoped [dir=rtl] exception for the select chevron", () => {
    // The chevron is painted via background-position (no logical equivalent).
    expect(css).toContain('[dir="rtl"] .ea-form__select');
    expect(css).toContain("background-position: left 10px center");
  });

  it("no longer uses physical margin-left/right on the converted Expansion Advisor controls", () => {
    // The chevron/pill rules were the only margin-left users; all converted.
    expect(css).not.toMatch(/\n\s*margin-left:/);
    expect(css).not.toMatch(/\n\s*margin-right:/);
  });
});
