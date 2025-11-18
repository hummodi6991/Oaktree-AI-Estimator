import { useEffect, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import MapboxDraw from "@mapbox/mapbox-gl-draw";
import type { Feature, Polygon } from "geojson";
import type { IControl, LngLatLike, Map } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import "@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css";
import "./Map.css";

type MapProps = { polygon?: Polygon | null; onPolygon: (geometry: Polygon | null) => void; };

type ToolbarState = { isDrawing: boolean; hasPolygon: boolean };
type ToolbarControl = IControl & { setState: (state: ToolbarState) => void };

const SITE_FEATURE_ID = "site";

const DEFAULT_MAP_STYLE = "https://demotiles.maplibre.org/style.json";

const FALLBACK_RASTER_STYLE: any = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: ["/tiles/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution: "© OpenStreetMap contributors",
    },
  },
  layers: [
    { id: "bg", type: "background", paint: { "background-color": "#f8f9fb" } },
    { id: "osm", type: "raster", source: "osm" },
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
export function smartFocus(map: Map, feature: GeoJSON.Feature) {
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

function createToolbarControl(actions: {
  onStart: () => void;
  onFinish: () => void;
  onClear: () => void;
  getState: () => ToolbarState;
}): ToolbarControl {
  let container: HTMLDivElement | null = null;
  let startButton: HTMLButtonElement | null = null;
  let finishButton: HTMLButtonElement | null = null;
  let clearButton: HTMLButtonElement | null = null;

  const control: ToolbarControl = {
    onAdd() {
      container = document.createElement("div");
      container.className = "map-toolbar maplibregl-ctrl";

      const group = document.createElement("div");
      group.className = "map-toolbar__group";

      startButton = document.createElement("button");
      startButton.type = "button";
      startButton.className = "map-toolbar__button";
      startButton.textContent = "Start polygon";
      startButton.addEventListener("click", actions.onStart);

      finishButton = document.createElement("button");
      finishButton.type = "button";
      finishButton.className = "map-toolbar__button";
      finishButton.textContent = "Finish shape";
      finishButton.addEventListener("click", actions.onFinish);

      clearButton = document.createElement("button");
      clearButton.type = "button";
      clearButton.className = "map-toolbar__button";
      clearButton.textContent = "Clear";
      clearButton.addEventListener("click", actions.onClear);

      group.append(startButton, finishButton, clearButton);
      container.append(group);

      const hint = document.createElement("p");
      hint.className = "map-toolbar__hint";
      hint.textContent = "Click to add vertices. Double-click or press Finish shape to close the polygon.";
      container.append(hint);

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
  };

  return control;
}

export default function Map({ polygon, onPolygon }: MapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const drawRef = useRef<MapboxDraw | null>(null);
  const toolbarRef = useRef<ToolbarControl | null>(null);
  const callbackRef = useRef(onPolygon);
  const isDrawingRef = useRef(false);
  const suppressDeleteRef = useRef(false);
  const finishDrawingRef = useRef<() => void>(() => undefined);
  const [isDrawing, setIsDrawing] = useState(false);

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
      maxZoom: 19,
    });

    mapRef.current = map;

    map.on("error", (event) => {
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

    const draw = new MapboxDraw({
      displayControlsDefault: false,
      defaultMode: "simple_select",
    });

    drawRef.current = draw;
    map.addControl(draw as unknown as IControl);
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-left");

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

    const toolbar = createToolbarControl({
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
    });

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
      map.remove();
      drawRef.current = null;
      mapRef.current = null;
    };
  }, []);

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

  return (
    <div className="map-wrapper">
      <div ref={containerRef} className="map-canvas" />
      <div className="map-overlay">
        <span className="map-overlay__badge">Guidance</span>
        <p>
          Click to add vertices. Double-click or press Finish shape to close the polygon, then drag
          vertices to adjust the parcel or press Clear to remove it.
        </p>
        {isDrawing && (
          <button type="button" className="map-overlay__finish" onClick={() => finishDrawingRef.current()}>
            Finish shape
          </button>
        )}
      </div>
    </div>
  );
}
