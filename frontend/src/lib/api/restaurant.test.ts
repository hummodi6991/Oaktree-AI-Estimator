import { describe, expect, it } from "vitest";
import { normalizeScore01To100, normalizeTopCell } from "./restaurant";

// ---------------------------------------------------------------------------
// normalizeScore01To100
// ---------------------------------------------------------------------------

describe("normalizeScore01To100", () => {
  it("passes through 0..100 values unchanged", () => {
    expect(normalizeScore01To100(45.3)).toBe(45.3);
    expect(normalizeScore01To100(100)).toBe(100);
    expect(normalizeScore01To100(2)).toBe(2);
  });

  it("scales 0..1 values to 0..100", () => {
    expect(normalizeScore01To100(0.5)).toBeCloseTo(50);
    expect(normalizeScore01To100(1.0)).toBeCloseTo(100);
    expect(normalizeScore01To100(0.0)).toBe(0);
  });

  it("returns 0 for undefined/null/NaN", () => {
    expect(normalizeScore01To100(undefined)).toBe(0);
    expect(normalizeScore01To100(null)).toBe(0);
    expect(normalizeScore01To100(NaN)).toBe(0);
  });

  it("returns 0 for non-number types", () => {
    expect(normalizeScore01To100("45")).toBe(0);
    expect(normalizeScore01To100(true)).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// normalizeTopCell — confidence regression tests
// ---------------------------------------------------------------------------

describe("normalizeTopCell", () => {
  it("preserves confidence_score from top-cells API response", () => {
    // Simulates the response shape from /restaurant/opportunity-top-cells
    const raw = {
      h3: "882a10a1cbfffff",
      lat: 24.7,
      lon: 46.7,
      opportunity_score: 72.5,
      confidence_score: 45.3,
      final_score: 58.0,
      competitor_count: 3,
      population: 4500,
      underserved_index: 320.5,
    };
    const cell = normalizeTopCell(raw);
    expect(cell).not.toBeNull();
    expect(cell!.confidence_score).toBe(45.3);
    expect(cell!.opportunity_score).toBe(72.5);
  });

  it("falls back to confidence field when confidence_score is missing", () => {
    const raw = {
      lat: 24.7,
      lon: 46.7,
      confidence: 0.65, // 0..1 scale
      final_score: 58.0,
    };
    const cell = normalizeTopCell(raw);
    expect(cell).not.toBeNull();
    // 0.65 should be scaled to 65
    expect(cell!.confidence_score).toBeCloseTo(65);
  });

  it("returns 0 confidence when both confidence_score and confidence are missing", () => {
    // Simulates stale cached data that lacks confidence fields
    const raw = {
      lat: 24.7,
      lon: 46.7,
      final_score: 58.0,
    };
    const cell = normalizeTopCell(raw);
    expect(cell).not.toBeNull();
    expect(cell!.confidence_score).toBe(0);
  });

  it("handles GeoJSON Feature format", () => {
    const raw = {
      type: "Feature",
      geometry: { type: "Point", coordinates: [46.7, 24.7] },
      properties: {
        h3: "882a10a1cbfffff",
        opportunity_score: 72.5,
        confidence_score: 45.3,
        final_score: 58.0,
      },
    };
    const cell = normalizeTopCell(raw);
    expect(cell).not.toBeNull();
    expect(cell!.confidence_score).toBe(45.3);
    expect(cell!.lat).toBe(24.7);
    expect(cell!.lon).toBe(46.7);
  });

  it("skips entries with zero lat and lon", () => {
    const raw = { lat: 0, lon: 0, confidence_score: 50 };
    expect(normalizeTopCell(raw)).toBeNull();
  });
});
