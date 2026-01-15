import type { Map as MapLibreMap } from "maplibre-gl";

const SOURCE_ID = "oaktree-parcels";
const LAYER_CASING_ID = "oaktree-parcels-outline-casing";
const LAYER_OUTLINE_ID = "oaktree-parcels-outline";
const SOURCE_LAYER = "parcels";

function apiBase(): string | null {
  const raw = (import.meta as any)?.env?.VITE_API_BASE_URL;
  if (raw == null) return null;
  const v = String(raw).trim();
  if (!v) return null;
  return v.replace(/\/+$/, "");
}

function parcelsTileUrl(): string {
  const base = apiBase();
  return base ? `${base}/v1/tiles/parcels/{z}/{x}/{y}.pbf` : "/v1/tiles/parcels/{z}/{x}/{y}.pbf";
}

function findBeforeId(map: MapLibreMap): string | undefined {
  const layers = map.getStyle()?.layers ?? [];
  const drawLayer = layers.find((layer) => layer.id?.startsWith("gl-draw-"))?.id;
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

  const casing = {
    id: LAYER_CASING_ID,
    type: "line",
    source: SOURCE_ID,
    "source-layer": SOURCE_LAYER,
    layout: { "line-join": "round", "line-cap": "round" },
    paint: {
      "line-opacity": 0.35,
      "line-width": [
        "interpolate",
        ["linear"],
        ["zoom"],
        12,
        0.8,
        14,
        1.2,
        16,
        1.8,
        18,
        2.6,
        20,
        3.2,
      ],
      "line-color": "rgba(0,0,0,0.9)",
    },
  } as any;

  const outline = {
    id: LAYER_OUTLINE_ID,
    type: "line",
    source: SOURCE_ID,
    "source-layer": SOURCE_LAYER,
    layout: { "line-join": "round", "line-cap": "round" },
    paint: {
      "line-opacity": 0.8,
      "line-width": [
        "interpolate",
        ["linear"],
        ["zoom"],
        12,
        0.4,
        14,
        0.7,
        16,
        1.1,
        18,
        1.6,
        20,
        2.0,
      ],
      "line-color": "rgba(255,255,255,0.95)",
    },
  } as any;

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
