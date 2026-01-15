import maplibregl from "maplibre-gl";

export const PARCELS_SOURCE_ID = "parcels";
export const PARCELS_SOURCE_LAYER = "parcels";
export const PARCELS_OUTLINE_LAYER_ID = "parcels-outline";
export const PARCELS_MIXEDUSE_LAYER_ID = "parcels-mixeduse-fill";

export function apiBase(): string {
  const raw = import.meta.env.VITE_API_BASE_URL as string | undefined;
  return (raw || "").replace(/\/+$/, "");
}

export function parcelTileUrlTemplate(): string {
  const base = apiBase();
  return `${base}/v1/tiles/parcels/{z}/{x}/{y}.pbf`;
}

export function ensureParcelLayers(map: maplibregl.Map) {
  if (!map.getSource(PARCELS_SOURCE_ID)) {
    map.addSource(PARCELS_SOURCE_ID, {
      type: "vector",
      tiles: [parcelTileUrlTemplate()],
    });
  }

  if (!map.getLayer(PARCELS_MIXEDUSE_LAYER_ID)) {
    map.addLayer({
      id: PARCELS_MIXEDUSE_LAYER_ID,
      type: "fill",
      source: PARCELS_SOURCE_ID,
      "source-layer": PARCELS_SOURCE_LAYER,
      filter: [
        "any",
        ["==", ["get", "classification"], 7500],
        ["==", ["get", "classification"], "7500"],
      ],
      paint: {
        "fill-color": "#ff0000",
        "fill-opacity": 0.25,
      },
    });
  }

  if (!map.getLayer(PARCELS_OUTLINE_LAYER_ID)) {
    map.addLayer({
      id: PARCELS_OUTLINE_LAYER_ID,
      type: "line",
      source: PARCELS_SOURCE_ID,
      "source-layer": PARCELS_SOURCE_LAYER,
      layout: { "line-join": "round", "line-cap": "round" },
      paint: {
        "line-color": "rgba(255,255,255,0.85)",
        "line-width": ["interpolate", ["linear"], ["zoom"], 10, 0.4, 14, 0.9, 18, 1.8],
        "line-opacity": ["interpolate", ["linear"], ["zoom"], 9, 0.0, 10, 0.2, 12, 0.4, 16, 0.7],
      },
    });
  }

  if (map.getLayer(PARCELS_MIXEDUSE_LAYER_ID) && map.getLayer(PARCELS_OUTLINE_LAYER_ID)) {
    map.moveLayer(PARCELS_OUTLINE_LAYER_ID);
  }
}

export function installParcelLayerPersistence(map: maplibregl.Map) {
  const guard = map as maplibregl.Map & { __oaktreeParcelPersistenceInstalled?: boolean };
  if (guard.__oaktreeParcelPersistenceInstalled) return;
  guard.__oaktreeParcelPersistenceInstalled = true;

  const reapply = () => {
    if (!map.isStyleLoaded()) return;
    try {
      ensureParcelLayers(map);
    } catch (error) {
      console.warn("Failed to ensure parcel layers", error);
    }
  };

  map.on("load", reapply);
  map.on("styledata", reapply);
}

export function installParcelDebugLogging(map: maplibregl.Map) {
  const guard = map as maplibregl.Map & { __oaktreeParcelDebugInstalled?: boolean };
  if (guard.__oaktreeParcelDebugInstalled) return;
  guard.__oaktreeParcelDebugInstalled = true;

  let loggedLoaded = false;

  map.on("error", (event) => {
    const ev = event as any;
    const message = String(ev?.error?.message ?? "");
    if (ev?.sourceId === PARCELS_SOURCE_ID || /tiles\/parcels/.test(message)) {
      console.warn("Parcel tiles error", ev?.error ?? event);
    }
  });

  map.on("sourcedata", (event) => {
    const ev = event as any;
    if (ev?.sourceId === PARCELS_SOURCE_ID && ev?.isSourceLoaded && !loggedLoaded) {
      loggedLoaded = true;
      console.info("parcels source loaded");
    }
  });
}
