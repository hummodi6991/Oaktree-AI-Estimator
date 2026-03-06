import { describe, expect, it, vi, beforeEach } from "vitest";
import {
  RESTAURANT_HEAT_SOURCE_ID,
  RESTAURANT_HEAT_LAYER_ID,
  RESTAURANT_POINTS_LAYER_ID,
  setRestaurantHeatmapData,
  removeRestaurantHeatmap,
  isRestaurantHeatmapVisible,
} from "./restaurantHeatLayer";

// ---------------------------------------------------------------------------
// Minimal MapLibre mock
// ---------------------------------------------------------------------------

function createMockMap() {
  const sources = new Map<string, { setData: ReturnType<typeof vi.fn> }>();
  const layers = new Set<string>();

  return {
    getSource: vi.fn((id: string) => sources.get(id)),
    addSource: vi.fn((id: string, _opts: unknown) => {
      sources.set(id, { setData: vi.fn() });
    }),
    removeSource: vi.fn((id: string) => {
      sources.delete(id);
    }),
    getLayer: vi.fn((id: string) => (layers.has(id) ? { id } : undefined)),
    addLayer: vi.fn((spec: { id: string }) => {
      layers.add(spec.id);
    }),
    removeLayer: vi.fn((id: string) => {
      layers.delete(id);
    }),
    // Expose internals for assertions
    _sources: sources,
    _layers: layers,
  };
}

type MockMap = ReturnType<typeof createMockMap>;

const POINT_FC: GeoJSON.FeatureCollection = {
  type: "FeatureCollection",
  features: [
    {
      type: "Feature",
      geometry: { type: "Point", coordinates: [46.7, 24.7] },
      properties: { final_score: 72, confidence_score: 45 },
    },
  ],
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("restaurantHeatLayer", () => {
  let map: MockMap;

  beforeEach(() => {
    map = createMockMap();
  });

  it("exports stable source and layer IDs", () => {
    expect(RESTAURANT_HEAT_SOURCE_ID).toBe("restaurant-opportunity-heatmap-source");
    expect(RESTAURANT_HEAT_LAYER_ID).toBe("restaurant-opportunity-heatmap-layer");
    expect(RESTAURANT_POINTS_LAYER_ID).toBe("restaurant-opportunity-points-layer");
  });

  it("adds a heatmap layer and a circle layer for Point GeoJSON", () => {
    setRestaurantHeatmapData(map as any, POINT_FC);

    expect(map.addSource).toHaveBeenCalledOnce();
    expect(map.addSource.mock.calls[0][0]).toBe(RESTAURANT_HEAT_SOURCE_ID);

    // Should create exactly 2 layers: heatmap + circle
    expect(map.addLayer).toHaveBeenCalledTimes(2);
    const layerIds = map.addLayer.mock.calls.map((c: any) => c[0].id);
    expect(layerIds).toContain(RESTAURANT_HEAT_LAYER_ID);
    expect(layerIds).toContain(RESTAURANT_POINTS_LAYER_ID);

    // Verify layer types
    const heatmapCall = map.addLayer.mock.calls.find((c: any) => c[0].id === RESTAURANT_HEAT_LAYER_ID);
    expect(heatmapCall).toBeDefined();
    expect((heatmapCall![0] as unknown as { type: string }).type).toBe("heatmap");
    const circleCall = map.addLayer.mock.calls.find((c: any) => c[0].id === RESTAURANT_POINTS_LAYER_ID);
    expect(circleCall).toBeDefined();
    expect((circleCall![0] as unknown as { type: string }).type).toBe("circle");
  });

  it("updates existing source data without re-adding layers", () => {
    setRestaurantHeatmapData(map as any, POINT_FC);
    map.addSource.mockClear();
    map.addLayer.mockClear();

    const updatedFc = { ...POINT_FC, features: [] };
    setRestaurantHeatmapData(map as any, updatedFc);

    expect(map.addSource).not.toHaveBeenCalled();
    expect(map.addLayer).not.toHaveBeenCalled();
    const src = map._sources.get(RESTAURANT_HEAT_SOURCE_ID);
    expect(src?.setData).toHaveBeenCalledWith(updatedFc);
  });

  it("removes layers and source when called with null", () => {
    setRestaurantHeatmapData(map as any, POINT_FC);
    setRestaurantHeatmapData(map as any, null);

    expect(map.removeLayer).toHaveBeenCalledWith(RESTAURANT_HEAT_LAYER_ID);
    expect(map.removeLayer).toHaveBeenCalledWith(RESTAURANT_POINTS_LAYER_ID);
    expect(map.removeSource).toHaveBeenCalledWith(RESTAURANT_HEAT_SOURCE_ID);
  });

  it("removeRestaurantHeatmap is safe when layers don't exist", () => {
    // Should not throw
    removeRestaurantHeatmap(map as any);
    expect(map.removeLayer).not.toHaveBeenCalled();
    expect(map.removeSource).not.toHaveBeenCalled();
  });

  it("isRestaurantHeatmapVisible returns correct state", () => {
    expect(isRestaurantHeatmapVisible(map as any)).toBe(false);
    setRestaurantHeatmapData(map as any, POINT_FC);
    expect(isRestaurantHeatmapVisible(map as any)).toBe(true);
  });

  it("cleans up legacy polygon-based layers if they exist", () => {
    // Simulate legacy layers from old code
    map._layers.add("restaurant-heatmap-fill");
    map._layers.add("restaurant-heatmap-outline");
    map._sources.set("restaurant-heatmap", { setData: vi.fn() });

    setRestaurantHeatmapData(map as any, POINT_FC);

    // Legacy layers should have been removed
    expect(map.removeLayer).toHaveBeenCalledWith("restaurant-heatmap-fill");
    expect(map.removeLayer).toHaveBeenCalledWith("restaurant-heatmap-outline");
    expect(map.removeSource).toHaveBeenCalledWith("restaurant-heatmap");
  });
});
