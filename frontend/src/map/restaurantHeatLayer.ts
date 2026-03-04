import maplibregl from "maplibre-gl";

export const RESTAURANT_HEAT_SOURCE_ID = "restaurant-heatmap";
export const RESTAURANT_HEAT_LAYER_ID = "restaurant-heatmap-fill";
export const RESTAURANT_HEAT_OUTLINE_LAYER_ID = "restaurant-heatmap-outline";

/**
 * Add or update the restaurant heatmap layer on the map.
 * Expects a GeoJSON FeatureCollection of H3 hex polygons with a `score` property.
 */
export function setRestaurantHeatmapData(
  map: maplibregl.Map,
  geojson: GeoJSON.FeatureCollection | null,
) {
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

  // Fill layer with color interpolated by score (0-100)
  map.addLayer({
    id: RESTAURANT_HEAT_LAYER_ID,
    type: "fill",
    source: RESTAURANT_HEAT_SOURCE_ID,
    paint: {
      "fill-color": [
        "interpolate",
        ["linear"],
        ["get", "score"],
        0, "#d73027",    // red — poor
        25, "#fc8d59",   // orange
        50, "#fee08b",   // yellow
        75, "#91cf60",   // light green
        100, "#1a9850",  // green — excellent
      ],
      "fill-opacity": 0.55,
    },
  });

  // Outline layer for hex cell borders
  map.addLayer({
    id: RESTAURANT_HEAT_OUTLINE_LAYER_ID,
    type: "line",
    source: RESTAURANT_HEAT_SOURCE_ID,
    paint: {
      "line-color": "#333",
      "line-width": 0.5,
      "line-opacity": 0.4,
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
  if (map.getLayer(RESTAURANT_HEAT_OUTLINE_LAYER_ID)) {
    map.removeLayer(RESTAURANT_HEAT_OUTLINE_LAYER_ID);
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
