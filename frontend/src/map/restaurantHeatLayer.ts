import maplibregl from "maplibre-gl";

export const RESTAURANT_HEAT_SOURCE_ID = "restaurant-opportunity-heatmap-source";
export const RESTAURANT_HEAT_LAYER_ID = "restaurant-opportunity-heatmap-layer";
export const RESTAURANT_POINTS_LAYER_ID = "restaurant-opportunity-points-layer";

// Keep old IDs for cleanup of any stale layers from previous code
const LEGACY_SOURCE_ID = "restaurant-heatmap";
const LEGACY_LAYER_IDS = ["restaurant-heatmap-fill", "restaurant-heatmap-outline"];

/** Score expression: coalesce final_score / score, default 0 */
const SCORE_EXPR: maplibregl.ExpressionSpecification = [
  "coalesce",
  ["get", "final_score"],
  ["get", "score"],
  0,
];

/**
 * Add or update the restaurant opportunity heatmap on the map.
 * Expects a GeoJSON FeatureCollection of **Point** features with score properties.
 *
 * Renders two layers from the same source:
 * - A `heatmap` layer visible at lower zoom levels for density overview
 * - A `circle` layer that fades in at higher zoom levels for individual points
 */
export function setRestaurantHeatmapData(
  map: maplibregl.Map,
  geojson: GeoJSON.FeatureCollection | null,
) {
  // Clean up any legacy layers from previous polygon-based implementation
  removeLegacyLayers(map);

  if (!geojson) {
    removeRestaurantHeatmap(map);
    return;
  }

  const source = map.getSource(RESTAURANT_HEAT_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
  if (source) {
    source.setData(geojson);
    return;
  }

  map.addSource(RESTAURANT_HEAT_SOURCE_ID, {
    type: "geojson",
    data: geojson,
  });

  // --- Heatmap layer (visible at lower zooms, fades out by zoom 15) ---
  map.addLayer({
    id: RESTAURANT_HEAT_LAYER_ID,
    type: "heatmap",
    source: RESTAURANT_HEAT_SOURCE_ID,
    maxzoom: 16,
    paint: {
      // Weight each point by its score (normalized 0-100 → 0-1).
      // Exponential base 2 makes high-score areas stand out sharply.
      "heatmap-weight": [
        "interpolate",
        ["exponential", 2],
        SCORE_EXPR,
        0, 0,
        100, 1,
      ],
      // Increase intensity as zoom increases
      "heatmap-intensity": [
        "interpolate",
        ["linear"],
        ["zoom"],
        10, 0.8,
        15, 1.5,
      ],
      // Cool cyan → blue color ramp for high contrast on satellite imagery
      "heatmap-color": [
        "interpolate",
        ["linear"],
        ["heatmap-density"],
        0, "rgba(0,0,0,0)",
        0.1, "rgba(200,240,255,0.55)",   // pale ice-blue
        0.3, "rgba(100,210,255,0.65)",   // light cyan
        0.5, "rgba(0,200,220,0.75)",     // aqua
        0.7, "rgba(0,140,255,0.85)",     // electric blue
        1.0, "rgba(0,60,200,0.92)",      // deep blue
      ],
      // Radius increases with zoom for smooth coverage
      "heatmap-radius": [
        "interpolate",
        ["linear"],
        ["zoom"],
        10, 15,
        13, 25,
        15, 35,
      ],
      // Fade out as we approach circle layer visibility
      "heatmap-opacity": [
        "interpolate",
        ["linear"],
        ["zoom"],
        13, 0.85,
        15, 0.4,
        16, 0,
      ],
    },
  });

  // --- Circle layer (fades in at higher zoom for individual point visibility) ---
  map.addLayer({
    id: RESTAURANT_POINTS_LAYER_ID,
    type: "circle",
    source: RESTAURANT_HEAT_SOURCE_ID,
    minzoom: 13,
    paint: {
      // Radius driven by score
      "circle-radius": [
        "interpolate",
        ["linear"],
        SCORE_EXPR,
        0, 4,
        50, 7,
        100, 12,
      ],
      // Cool cyan → blue ramp matching the heatmap palette
      "circle-color": [
        "interpolate",
        ["linear"],
        SCORE_EXPR,
        0, "#c8f0ff",
        25, "#64d2ff",
        50, "#00c8dc",
        75, "#008cff",
        100, "#003cc8",
      ],
      // Fade in as heatmap fades out
      "circle-opacity": [
        "interpolate",
        ["linear"],
        ["zoom"],
        13, 0,
        15, 0.7,
        16, 0.85,
      ],
      "circle-stroke-color": "#fff",
      "circle-stroke-width": 1,
      "circle-stroke-opacity": [
        "interpolate",
        ["linear"],
        ["zoom"],
        13, 0,
        15, 0.5,
        16, 0.8,
      ],
    },
  });
}

/**
 * Remove the restaurant heatmap layers and source from the map.
 */
export function removeRestaurantHeatmap(map: maplibregl.Map) {
  if (map.getLayer(RESTAURANT_HEAT_LAYER_ID)) {
    map.removeLayer(RESTAURANT_HEAT_LAYER_ID);
  }
  if (map.getLayer(RESTAURANT_POINTS_LAYER_ID)) {
    map.removeLayer(RESTAURANT_POINTS_LAYER_ID);
  }
  if (map.getSource(RESTAURANT_HEAT_SOURCE_ID)) {
    map.removeSource(RESTAURANT_HEAT_SOURCE_ID);
  }
}

/**
 * Check if the restaurant heatmap is currently visible.
 */
export function isRestaurantHeatmapVisible(map: maplibregl.Map): boolean {
  return !!map.getLayer(RESTAURANT_HEAT_LAYER_ID);
}

/** Remove any layers/source from the old polygon-based implementation. */
function removeLegacyLayers(map: maplibregl.Map) {
  for (const id of LEGACY_LAYER_IDS) {
    if (map.getLayer(id)) map.removeLayer(id);
  }
  if (map.getSource(LEGACY_SOURCE_ID)) map.removeSource(LEGACY_SOURCE_ID);
}
