import { describe, expect, it } from "vitest";
import { makeApiUrl } from "./api";

describe("makeApiUrl", () => {
  it("returns same-origin path when API base is empty", () => {
    expect(makeApiUrl("", "/v1/search")).toBe("/v1/search");
  });
});
