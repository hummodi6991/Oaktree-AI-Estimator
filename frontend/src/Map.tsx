import { useEffect, useMemo, useRef, useState } from "react";
import maplibregl, { Map as MapLibreMap, NavigationControl } from "maplibre-gl";
import MapboxDraw from "@mapbox/mapbox-gl-draw";
import type { Feature, Polygon } from "geojson";
import type { IControl, LngLatLike } from "maplibre-gl";
import { useTranslation } from "react-i18next";
import "maplibre-gl/dist/maplibre-gl.css";
import "@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css";
import "./Map.css";
import { buildApiUrl } from "./api";
import { formatInteger, formatNumber } from "./i18n/format";

type MapProps = { polygon?: Polygon | null; onPolygon: (geometry: Polygon | null) => void; };

type ToolbarState = { isDrawing: boolean; hasPolygon: boolean };
type ToolbarLabels = { start: string; finish: string; clear: string; hint: string };
type ToolbarControl = IControl & {
  setState: (state: ToolbarState) => void;
  setLabels: (labels: ToolbarLabels) => void;
};
type OverlayDiagnostics = {
  zoom: number;
  maxZoom: number;
  parcelBase: {
    layerExists: boolean;
    sourceExists: boolean;
    visibility: string;
    renderedCount: number | null;
  };
};

const SITE_FEATURE_ID = "site";
const PARCEL_SOURCE_ID = "parcel-outlines";
const PARCEL_LINE_BASE_LAYER_ID = "parcels-line-base";
const PARCEL_LINE_LAYER_ID = "parcel-outlines-line";
const PARCEL_FILL_LAYER_ID = "parcel-outlines-fill";
const PARCEL_MIN_ZOOM = 15;
const PARCEL_LINE_WIDTH: any = ["interpolate", ["linear"], ["zoom"], 15, 0.7, 20, 2.0];
const PARCEL_OUTLINE_VISIBILITY: any = ["case", [">=", ["zoom"], 16], "visible", "none"];

const DEFAULT_MAP_STYLE = "/esri-style.json";

const FALLBACK_RASTER_STYLE: any = {
  version: 8,
  sources: {
    esri_world_imagery: {
      type: "raster",
      tiles: [
        "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
      ],
      tileSize: 256,
      attribution: "Tiles © Esri — Source: Esri, Maxar, Earthstar Geographics, and the GIS User Community",
    },
  },
  layers: [
    { id: "bg", type: "background", paint: { "background-color": "#000000" } },
    { id: "esri", type: "raster", source: "esri_world_imagery" },
  ],
};

/**
 * Compute a LngLatBounds from a (Multi)Polygon without adding turf as a dep.
 */
function boundsOfFeature(f: GeoJSON.Feature): maplibregl.LngLatBounds {
  const b = new maplibregl.LngLatBounds();
  const g = f.geometry;
  if (g.type === "Polygon") {
    (g.coordinates[0] || []).forEach(([lng, lat]) => b.extend([lng, lat]));
  } else if (g.type === "MultiPolygon") {
    g.coordinates.forEach((poly) => (poly[0] || []).forEach(([lng, lat]) => b.extend([lng, lat])));
  }
  return b;
}

// --- Camera behavior constants ---
const MAX_SELECT_ZOOM = 17.0; // never zoom past this when focusing
const MAX_ZOOM_DELTA = 1.5; // don't jump more than +1.5 on a single click
const VIEWPAD = {
  // keep selection clear of the bottom info panel
  top: 24,
  right: 24,
  bottom: 280,
  left: 24,
} as const;

/**
 * Smoothly bring a feature into view without over-zooming small parcels.
 * - Limits the absolute zoom (MAX_SELECT_ZOOM)
 * - Limits the step size per click (MAX_ZOOM_DELTA)
 * - Adds padding so the bottom sheet doesn't cover the parcel
 */
export function smartFocus(map: MapLibreMap, feature: GeoJSON.Feature) {
  const bounds = boundsOfFeature(feature);
  // Ask MapLibre what the camera should be for those bounds, then clamp
  const cam = map.cameraForBounds(bounds, { padding: VIEWPAD, maxZoom: MAX_SELECT_ZOOM }) || {};
  const current = map.getZoom();
  const suggested = typeof (cam as any).zoom === "number" ? (cam as any).zoom : current;
  const clamped = Math.min(
    MAX_SELECT_ZOOM,
    Math.max(current, Math.min(suggested, current + MAX_ZOOM_DELTA))
  );
  const targetCenter = (cam as any).center ?? bounds.getCenter();

  map.easeTo({
    center: targetCenter,
    zoom: clamped,
    bearing: map.getBearing(),
    pitch: map.getPitch(),
    duration: 650,
  });
}

function createToolbarControl(
  actions: {
  onStart: () => void;
  onFinish: () => void;
  onClear: () => void;
  getState: () => ToolbarState;
  },
  labels: ToolbarLabels,
): ToolbarControl {
  let container: HTMLDivElement | null = null;
  let startButton: HTMLButtonElement | null = null;
  let finishButton: HTMLButtonElement | null = null;
  let clearButton: HTMLButtonElement | null = null;
  let hintEl: HTMLParagraphElement | null = null;
  let currentLabels = labels;

  const control: ToolbarControl = {
    onAdd() {
      container = document.createElement("div");
      container.className = "map-toolbar maplibregl-ctrl";

      const group = document.createElement("div");
      group.className = "map-toolbar__group";

      startButton = document.createElement("button");
      startButton.type = "button";
      startButton.className = "map-toolbar__button";
      startButton.textContent = currentLabels.start;
      startButton.addEventListener("click", actions.onStart);

      finishButton = document.createElement("button");
      finishButton.type = "button";
      finishButton.className = "map-toolbar__button";
      finishButton.textContent = currentLabels.finish;
      finishButton.addEventListener("click", actions.onFinish);

      clearButton = document.createElement("button");
      clearButton.type = "button";
      clearButton.className = "map-toolbar__button";
      clearButton.textContent = currentLabels.clear;
      clearButton.addEventListener("click", actions.onClear);

      group.append(startButton, finishButton, clearButton);
      container.append(group);

      hintEl = document.createElement("p");
      hintEl.className = "map-toolbar__hint";
      hintEl.textContent = currentLabels.hint;
      container.append(hintEl);

      control.setState(actions.getState());
      return container;
    },
    onRemove() {
      startButton?.removeEventListener("click", actions.onStart);
      finishButton?.removeEventListener("click", actions.onFinish);
      clearButton?.removeEventListener("click", actions.onClear);
      container?.remove();
      container = null;
      startButton = null;
      finishButton = null;
      clearButton = null;
    },
    setState(state) {
      if (startButton) {
        startButton.disabled = state.isDrawing;
        startButton.classList.toggle("is-active", state.isDrawing);
      }
      if (finishButton) {
        finishButton.disabled = !state.isDrawing;
      }
      if (clearButton) {
        clearButton.disabled = !state.hasPolygon;
      }
    },
    setLabels(nextLabels) {
      currentLabels = nextLabels;
      if (startButton) startButton.textContent = nextLabels.start;
      if (finishButton) finishButton.textContent = nextLabels.finish;
      if (clearButton) clearButton.textContent = nextLabels.clear;
      if (hintEl) hintEl.textContent = nextLabels.hint;
    },
  };

  return control;
}

export default function MapView({ polygon, onPolygon }: MapProps) {
  const { t } = useTranslation();
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<MapLibreMap | null>(null);
  const drawRef = useRef<MapboxDraw | null>(null);
  const toolbarRef = useRef<ToolbarControl | null>(null);
  const callbackRef = useRef(onPolygon);
  const isDrawingRef = useRef(false);
  const suppressDeleteRef = useRef(false);
  const finishDrawingRef = useRef<() => void>(() => undefined);
  const [isDrawing, setIsDrawing] = useState(false);
  const [zoomLevel, setZoomLevel] = useState(0);
  const [overlayDiagnostics, setOverlayDiagnostics] = useState<OverlayDiagnostics>({
    zoom: 0,
    maxZoom: 0,
    parcelBase: {
      layerExists: false,
      sourceExists: false,
      visibility: "unknown",
      renderedCount: null,
    },
  });
  const parcelTileUrl = useMemo(() => buildApiUrl("/v1/tiles/suhail/{z}/{x}/{y}.pbf"), []);
  const toolbarLabels = useMemo(
    () => ({
      start: t("mapDraw.toolbar.startPolygon"),
      finish: t("mapDraw.toolbar.finishShape"),
      clear: t("mapDraw.toolbar.clear"),
      hint: t("mapDraw.toolbar.hint"),
    }),
    [t],
  );
  const parcelSourceLayer = "parcels";

  const deleteAll = (suppressCallback = false) => {
    if (!drawRef.current) return;
    const draw = drawRef.current;
    const features = draw.getAll().features || [];
    const hadFeatures = features.length > 0;
    if (suppressCallback && hadFeatures) {
      suppressDeleteRef.current = true;
    }
    draw.deleteAll();
    if (suppressCallback && !hadFeatures) {
      suppressDeleteRef.current = false;
    }
  };

  useEffect(() => {
    callbackRef.current = onPolygon;
  }, [onPolygon]);

  useEffect(() => {
    if (!containerRef.current) return;

    const configuredStyle = import.meta.env.VITE_MAP_STYLE || DEFAULT_MAP_STYLE;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: configuredStyle,
      center: [46.675, 24.713],
      zoom: 15,
      maxZoom: 22,
    });

    mapRef.current = map;

    map.on("error", (event) => {
      if (event?.error) {
        console.warn("Map error", event.error, {
          sourceId: (event as any)?.sourceId,
          tile: (event as any)?.tile,
        });
      }
      const message = String((event as any)?.error?.message || "");
      if (
        message.includes("Failed to load") ||
        message.includes("style") ||
        message.includes("glyph") ||
        message.includes("sprite")
      ) {
        try {
          map.setStyle(FALLBACK_RASTER_STYLE as any);
        } catch (err) {
          console.warn("Could not apply fallback map style", err);
        }
      }
    });

    const getBeforeLayerId = () =>
      map.getStyle()?.layers?.find((layer) => layer.type === "symbol")?.id;

    const reorderOverlayLayers = () => {
      const beforeLayerId = getBeforeLayerId();
      const outlineLayerIds = [PARCEL_LINE_BASE_LAYER_ID, PARCEL_LINE_LAYER_ID];
      outlineLayerIds.forEach((layerId) => {
        if (!map.getLayer(layerId)) return;
        if (beforeLayerId && map.getLayer(beforeLayerId)) {
          map.moveLayer(layerId, beforeLayerId);
        } else {
          map.moveLayer(layerId);
        }
      });

      if (map.getLayer(PARCEL_FILL_LAYER_ID) && map.getLayer(PARCEL_LINE_BASE_LAYER_ID)) {
        map.moveLayer(PARCEL_FILL_LAYER_ID, PARCEL_LINE_BASE_LAYER_ID);
      }
    };

    const forceOutlineVisibility = () => {
      const outlineIds = [PARCEL_LINE_BASE_LAYER_ID, PARCEL_LINE_LAYER_ID];
      outlineIds.forEach((id) => {
        if (map.getLayer(id)) {
          map.setLayoutProperty(id, "visibility", PARCEL_OUTLINE_VISIBILITY);
        }
      });
      if (map.getLayer(PARCEL_FILL_LAYER_ID)) {
        map.setLayoutProperty(PARCEL_FILL_LAYER_ID, "visibility", "visible");
      }
    };

    const ensureParcelOverlay = () => {
      if (!parcelTileUrl) return;
      if (!map.getSource(PARCEL_SOURCE_ID)) {
        map.addSource(PARCEL_SOURCE_ID, {
          type: "vector",
          tiles: [parcelTileUrl],
          minzoom: 10,
          maxzoom: 22,
        });
      }

      const beforeLayerId = getBeforeLayerId();
      if (!map.getLayer(PARCEL_LINE_BASE_LAYER_ID)) {
        map.addLayer(
          {
            id: PARCEL_LINE_BASE_LAYER_ID,
            type: "line",
            source: PARCEL_SOURCE_ID,
            "source-layer": parcelSourceLayer,
            minzoom: PARCEL_MIN_ZOOM,
            layout: { visibility: PARCEL_OUTLINE_VISIBILITY },
            paint: {
              "line-color": "#00AEEF",
              "line-width": 1,
              "line-opacity": ["interpolate", ["linear"], ["zoom"], 16, 0.6, 18, 0.75],
            },
          },
          beforeLayerId
        );
      } else {
        map.setLayoutProperty(PARCEL_LINE_BASE_LAYER_ID, "visibility", PARCEL_OUTLINE_VISIBILITY);
        if (beforeLayerId) {
          map.moveLayer(PARCEL_LINE_BASE_LAYER_ID, beforeLayerId);
        }
      }
      if (!map.getLayer(PARCEL_FILL_LAYER_ID)) {
        map.addLayer(
          {
            id: PARCEL_FILL_LAYER_ID,
            type: "fill",
            source: PARCEL_SOURCE_ID,
            "source-layer": parcelSourceLayer,
            minzoom: PARCEL_MIN_ZOOM,
            layout: { visibility: "visible" },
            paint: {
              "fill-color": "#a18af5",
              "fill-opacity": ["interpolate", ["linear"], ["zoom"], 15, 0.04, 16, 0.06, 18, 0.08],
            },
          },
          beforeLayerId
        );
      } else {
        map.setLayoutProperty(PARCEL_FILL_LAYER_ID, "visibility", "visible");
        if (beforeLayerId) {
          map.moveLayer(PARCEL_FILL_LAYER_ID, beforeLayerId);
        }
      }

      if (!map.getLayer(PARCEL_LINE_LAYER_ID)) {
        map.addLayer(
          {
            id: PARCEL_LINE_LAYER_ID,
            type: "line",
            source: PARCEL_SOURCE_ID,
            "source-layer": parcelSourceLayer,
            minzoom: PARCEL_MIN_ZOOM,
            layout: { visibility: PARCEL_OUTLINE_VISIBILITY },
            paint: {
              "line-color": "#8a5dff",
              "line-width": PARCEL_LINE_WIDTH,
              "line-opacity": ["interpolate", ["linear"], ["zoom"], 16, 0.65, 18, 0.8],
            },
          },
          beforeLayerId
        );
      } else {
        map.setLayoutProperty(PARCEL_LINE_LAYER_ID, "visibility", PARCEL_OUTLINE_VISIBILITY);
        if (beforeLayerId) {
          map.moveLayer(PARCEL_LINE_LAYER_ID, beforeLayerId);
        }
      }

      if (map.getLayer(PARCEL_FILL_LAYER_ID) && map.getLayer(PARCEL_LINE_BASE_LAYER_ID)) {
        map.moveLayer(PARCEL_FILL_LAYER_ID, PARCEL_LINE_BASE_LAYER_ID);
      }
      forceOutlineVisibility();
      reorderOverlayLayers();
    };

    const logVectorTilesLoaded = (event: any) => {
      if (event?.sourceId === PARCEL_SOURCE_ID && event?.isSourceLoaded) {
        console.debug("Parcels vector tiles loaded", { tileId: event?.tile?.id, dataType: event?.dataType });
      }
    };

    const logOverlayStatus = (reason: string) => {
      const parcelLayerExists = Boolean(map.getLayer(PARCEL_LINE_BASE_LAYER_ID));
      console.info("Overlay status", {
        reason,
        parcel: {
          sourceExists: Boolean(map.getSource(PARCEL_SOURCE_ID)),
          layerExists: parcelLayerExists,
          visibility: parcelLayerExists
            ? String(map.getLayoutProperty(PARCEL_LINE_BASE_LAYER_ID, "visibility"))
            : "missing",
        },
      });
    };

    const updateParcelDiagnostics = (reason: string) => {
      const zoom = map.getZoom();
      const maxZoom = map.getMaxZoom();
      const parcelLayerExists = Boolean(map.getLayer(PARCEL_LINE_BASE_LAYER_ID));
      const parcelSourceExists = Boolean(map.getSource(PARCEL_SOURCE_ID));
      const parcelVisibility = parcelLayerExists
        ? String(map.getLayoutProperty(PARCEL_LINE_BASE_LAYER_ID, "visibility"))
        : "missing";
      let parcelRenderedCount: number | null = null;
      if (parcelLayerExists && zoom >= PARCEL_MIN_ZOOM) {
        try {
          parcelRenderedCount = map.queryRenderedFeatures({ layers: [PARCEL_LINE_BASE_LAYER_ID] }).length;
        } catch (err) {
          console.warn("Parcel base queryRenderedFeatures failed", err);
        }
      }

      const diagnostics = {
        zoom,
        maxZoom,
        parcelBase: {
          layerExists: parcelLayerExists,
          sourceExists: parcelSourceExists,
          visibility: parcelVisibility,
          renderedCount: parcelRenderedCount,
        },
      };
      setOverlayDiagnostics(diagnostics);
      console.info("Parcel diagnostics", { reason, ...diagnostics });
    };

    const handleDiagnosticsLoad = () => {
      logOverlayStatus("load");
      updateParcelDiagnostics("load");
    };
    const handleDiagnosticsStyleLoad = () => updateParcelDiagnostics("style.load");
    const handleDiagnosticsZoom = () => updateParcelDiagnostics("zoomend");
    const handleDiagnosticsMove = () => updateParcelDiagnostics("moveend");

    map.on("load", ensureParcelOverlay);
    map.on("load", forceOutlineVisibility);
    map.on("load", handleDiagnosticsLoad);
    map.on("style.load", ensureParcelOverlay);
    map.on("style.load", handleDiagnosticsStyleLoad);
    map.on("sourcedata", logVectorTilesLoaded);
    map.on("zoomend", handleDiagnosticsZoom);
    map.on("moveend", handleDiagnosticsMove);
    let forced = false;
    const handleIdleForce = () => {
      if (forced) return;
      forced = true;
      forceOutlineVisibility();
    };
    map.on("idle", reorderOverlayLayers);
    map.on("idle", handleIdleForce);

    const draw = new MapboxDraw({
      displayControlsDefault: false,
      defaultMode: "simple_select",
    });

    drawRef.current = draw;
    map.addControl(draw as unknown as IControl);
    map.addControl(new NavigationControl({ showCompass: false }), "top-left");

    let raf = 0;
    const updateZoomHud = () => {
      cancelAnimationFrame(raf);
      raf = requestAnimationFrame(() => setZoomLevel(map.getZoom()));
    };
    updateZoomHud();
    map.on("move", updateZoomHud);
    map.on("zoom", updateZoomHud);

    const updateDrawingState = (value: boolean) => {
      isDrawingRef.current = value;
      setIsDrawing(value);
    };

    const currentState = (): ToolbarState => ({
      isDrawing: isDrawingRef.current,
      hasPolygon:
        (drawRef.current?.getAll().features || []).filter(
          (feature: Feature): feature is Feature<Polygon> => (feature.geometry as any)?.type === "Polygon"
        ).length > 0,
    });

    const emitPolygon = () => {
      if (!drawRef.current) return;
      const collection = drawRef.current.getAll();
      const polygonFeature = collection.features.find(
        (feature: Feature): feature is Feature<Polygon> => (feature.geometry as any)?.type === "Polygon"
      );
      callbackRef.current(polygonFeature ? polygonFeature.geometry : null);
    };

    const finishDrawing = () => {
      if (!drawRef.current) return;
      drawRef.current.changeMode("simple_select");
      const firstPolygon = drawRef.current
        .getAll()
        .features.find(
          (feature: Feature): feature is Feature<Polygon> => (feature.geometry as any)?.type === "Polygon"
        );
      if (firstPolygon) {
        const id = firstPolygon.id as string | undefined;
        if (id) {
          drawRef.current.changeMode("simple_select", { featureIds: [id] as any });
        }
      }
      updateDrawingState(false);
      toolbarRef.current?.setState(currentState());
      emitPolygon();
    };

    finishDrawingRef.current = finishDrawing;

    const toolbar = createToolbarControl(
      {
        onStart: () => {
          if (!drawRef.current) return;
          deleteAll(true);
          drawRef.current.changeMode("draw_polygon");
          updateDrawingState(true);
          toolbarRef.current?.setState({ isDrawing: true, hasPolygon: false });
        },
        onFinish: finishDrawing,
        onClear: () => {
          if (!drawRef.current) return;
          deleteAll(true);
          updateDrawingState(false);
          toolbarRef.current?.setState({ isDrawing: false, hasPolygon: false });
          callbackRef.current(null);
        },
        getState: currentState,
      },
      toolbarLabels,
    );

    toolbarRef.current = toolbar;
    map.addControl(toolbar, "top-right");

    const syncToolbar = () => {
      toolbarRef.current?.setState(currentState());
    };

    map.on("draw.modechange", (event: any) => {
      const isDrawing = event.mode === "draw_polygon";
      updateDrawingState(isDrawing);
      if (isDrawing) {
        map.doubleClickZoom.disable();
      } else {
        map.doubleClickZoom.enable();
      }
      syncToolbar();
    });

    map.on("draw.create", (event) => {
      const draw = drawRef.current;
      if (!draw) return;

      const polygonFeature = event.features.find(
        (feature: Feature): feature is Feature<Polygon> => (feature.geometry as any)?.type === "Polygon"
      );
      if (!polygonFeature) return;

      // Keep the newly created feature; drop any others without emitting a null selection.
      const createdId = (polygonFeature as any).id;
      try {
        const all = draw.getAll();
        if (all?.features?.length) {
          suppressDeleteRef.current = true;
          for (const f of all.features) {
            const id = (f as any).id;
            if (id && id !== createdId) {
              try {
                draw.delete(id);
              } catch {}
            }
          }
        }
      } catch {}
      suppressDeleteRef.current = false;

      // Send the geometry up to the React state
      callbackRef.current(polygonFeature.geometry);

      if (createdId) {
        try {
          draw.changeMode("simple_select", { featureIds: [createdId] as any });
        } catch {}
      } else {
        try {
          draw.changeMode("simple_select");
        } catch {}
      }

      updateDrawingState(false);
      syncToolbar();
    });

    map.on("draw.update", () => {
      emitPolygon();
      syncToolbar();
    });

    map.on("draw.delete", () => {
      if (suppressDeleteRef.current) {
        suppressDeleteRef.current = false;
        return;
      }
      callbackRef.current(null);
      updateDrawingState(false);
      syncToolbar();
    });

    return () => {
      finishDrawingRef.current = () => undefined;
      toolbarRef.current = null;
      map.doubleClickZoom.enable();
      map.off("load", ensureParcelOverlay);
      map.off("load", forceOutlineVisibility);
      map.off("load", handleDiagnosticsLoad);
      map.off("style.load", ensureParcelOverlay);
      map.off("style.load", handleDiagnosticsStyleLoad);
      map.off("sourcedata", logVectorTilesLoaded);
      map.off("zoomend", handleDiagnosticsZoom);
      map.off("moveend", handleDiagnosticsMove);
      map.off("idle", reorderOverlayLayers);
      map.off("idle", handleIdleForce);
      map.off("move", updateZoomHud);
      map.off("zoom", updateZoomHud);
      cancelAnimationFrame(raf);
      map.remove();
      drawRef.current = null;
      mapRef.current = null;
    };
  }, [parcelTileUrl]);

  useEffect(() => {
    if (!drawRef.current) return;

    const draw = drawRef.current;
    deleteAll(true);

    if (polygon) {
      const feature: Feature<Polygon> = {
        id: SITE_FEATURE_ID,
        type: "Feature",
        properties: {},
        geometry: polygon,
      };
      draw.add(feature as any);

      const id = (feature.id ?? draw.getAll().features[0]?.id) as string | undefined;
      if (id) {
        draw.changeMode("simple_select", { featureIds: [id] as any });
      }

      toolbarRef.current?.setState({ isDrawing: false, hasPolygon: true });
      setIsDrawing(false);

      if (mapRef.current) {
        const bounds = polygon.coordinates[0].reduce<maplibregl.LngLatBounds | null>((acc, coord) => {
          if (!acc) {
            return new maplibregl.LngLatBounds(coord as LngLatLike, coord as LngLatLike);
          }
          acc.extend(coord as LngLatLike);
          return acc;
        }, null);

        if (bounds && !bounds.isEmpty()) {
          smartFocus(mapRef.current, feature as Feature);
        }
      }
    } else {
      toolbarRef.current?.setState({ isDrawing: false, hasPolygon: false });
      setIsDrawing(false);
    }
  }, [polygon]);

  useEffect(() => {
    toolbarRef.current?.setLabels(toolbarLabels);
  }, [toolbarLabels]);

  const outlinesEligible = zoomLevel >= PARCEL_MIN_ZOOM;
  const outlinesOn =
    outlinesEligible &&
    overlayDiagnostics.parcelBase.visibility === "visible";

  return (
    <div className="map-wrapper">
      <div ref={containerRef} className="map-canvas" />
      <div className="map-zoom-hud">
        <div className="map-zoom-hud__row">
          <span>{t("mapDraw.zoomHud.zoomLabel")}</span>
          <span className="numeric-value">{formatNumber(zoomLevel, { maximumFractionDigits: 2, minimumFractionDigits: 2 })}</span>
        </div>
        <div className="map-zoom-hud__row">
          <span>{t("mapDraw.zoomHud.outlinesLabel")}</span>
          <span>
            {outlinesOn ? t("mapDraw.zoomHud.on") : t("mapDraw.zoomHud.off")}{" "}
            (
            {outlinesEligible
              ? t("mapDraw.zoomHud.thresholdAbove", { value: formatInteger(PARCEL_MIN_ZOOM) })
              : t("mapDraw.zoomHud.thresholdBelow", { value: formatInteger(PARCEL_MIN_ZOOM) })}
            )
          </span>
        </div>
      </div>
      <div className="map-overlay">
        <span className="map-overlay__badge">{t("mapDraw.guidanceBadge")}</span>
        <p>
          {t("mapDraw.guidanceText")}
        </p>
        {isDrawing && (
          <button type="button" className="map-overlay__finish" onClick={() => finishDrawingRef.current()}>
            {t("mapDraw.finishShape")}
          </button>
        )}
      </div>
    </div>
  );
}
