import type { LayerSpecification, Map as MapLibreMap } from "maplibre-gl";

const SOURCE_ID = "oaktree-parcels";
const LAYER_MIXED_USE_ID = "oaktree-parcels-mixeduse";
const LAYER_CASING_ID = "oaktree-parcels-outline-casing";
const LAYER_OUTLINE_ID = "oaktree-parcels-outline";
const SOURCE_LAYER = "parcels";

function apiBase(): string {
  const raw = (import.meta as any)?.env?.VITE_API_BASE_URL ?? "";
  return String(raw).replace(/\/+$/, "");
}

function parcelsTileUrl(): string {
  const base = apiBase();
  return `${base}/v1/tiles/parcels/{z}/{x}/{y}.pbf`;
}

function findBeforeId(map: MapLibreMap): string | undefined {
  const layers = map.getStyle()?.layers ?? [];
  const drawLayer = layers.find((layer) => layer.id?.startsWith("gl-draw"))?.id;
  return drawLayer;
}

export function ensureParcelsOutline(map: MapLibreMap): void {
  if (!map.getSource(SOURCE_ID)) {
    const source = {
      type: "vector",
      tiles: [parcelsTileUrl()],
      minzoom: 0,
      maxzoom: 22,
    };
    try {
      map.addSource(SOURCE_ID, source as any);
    } catch (error) {
      console.warn("Could not add parcel source", error);
    }
  }

  const beforeId = findBeforeId(map);

  const mixedUse: LayerSpecification = {
    id: LAYER_MIXED_USE_ID,
    type: "fill",
    source: SOURCE_ID,
    "source-layer": SOURCE_LAYER,
    filter: ["==", ["get", "classification"], "m"],
    paint: {
      "fill-color": "#ff0000",
      "fill-opacity": 0.25,
    },
  };

  const casing: LayerSpecification = {
    id: LAYER_CASING_ID,
    type: "line",
    source: SOURCE_ID,
    "source-layer": SOURCE_LAYER,
    layout: { "line-join": "round", "line-cap": "round" },
    paint: {
      "line-opacity": [
        "interpolate",
        ["linear"],
        ["zoom"],
        9,
        0.25,
        12,
        0.45,
        16,
        0.7,
      ],
      "line-width": [
        "interpolate",
        ["linear"],
        ["zoom"],
        10,
        0.8,
        14,
        1.3,
        18,
        2.4,
      ],
      "line-color": "rgba(0,0,0,0.55)",
    },
  };

  const outline: LayerSpecification = {
    id: LAYER_OUTLINE_ID,
    type: "line",
    source: SOURCE_ID,
    "source-layer": SOURCE_LAYER,
    layout: { "line-join": "round", "line-cap": "round" },
    paint: {
      "line-opacity": [
        "interpolate",
        ["linear"],
        ["zoom"],
        9,
        0.35,
        12,
        0.6,
        16,
        0.85,
      ],
      "line-width": [
        "interpolate",
        ["linear"],
        ["zoom"],
        10,
        0.4,
        14,
        0.8,
        18,
        1.6,
      ],
      "line-color": "rgba(255,255,255,0.85)",
    },
  };

  if (!map.getLayer(LAYER_MIXED_USE_ID)) {
    try {
      map.addLayer(mixedUse, beforeId);
    } catch (error) {
      console.warn("Could not add parcel mixed-use layer", error);
    }
  }

  if (!map.getLayer(LAYER_CASING_ID)) {
    try {
      map.addLayer(casing, beforeId);
    } catch (error) {
      console.warn("Could not add parcel casing layer", error);
    }
  }

  if (!map.getLayer(LAYER_OUTLINE_ID)) {
    try {
      map.addLayer(outline, beforeId);
    } catch (error) {
      console.warn("Could not add parcel outline layer", error);
    }
  }
}
