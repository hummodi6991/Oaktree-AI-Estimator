import { useEffect, useRef } from "react";
import maplibregl from "maplibre-gl";
import MapboxDraw from "@mapbox/mapbox-gl-draw";
import type { Feature, Polygon } from "geojson";
import type { IControl, LngLatLike } from "maplibre-gl";
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
      tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution: "© OpenStreetMap contributors",
    },
  },
  layers: [
    { id: "bg", type: "background", paint: { "background-color": "#f8f9fb" } },
    { id: "osm", type: "raster", source: "osm" },
  ],
};

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
      hint.textContent = "Click to add vertices, double-click to close the polygon.";
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

    const toolbar = createToolbarControl({
      onStart: () => {
        if (!drawRef.current) return;
        drawRef.current.deleteAll();
        drawRef.current.changeMode("draw_polygon");
        isDrawingRef.current = true;
        toolbarRef.current?.setState({ isDrawing: true, hasPolygon: false });
      },
      onFinish: () => {
        if (!drawRef.current) return;
        drawRef.current.changeMode("simple_select");
        const firstPolygon = drawRef.current
          .getAll()
          .features.find((feature: Feature): feature is Feature<Polygon> => (feature.geometry as any)?.type === "Polygon");
        if (firstPolygon) {
          const id = firstPolygon.id as string | undefined;
          if (id) {
            drawRef.current.changeMode("simple_select", { featureIds: [id] as any });
          }
        }
        isDrawingRef.current = false;
        toolbarRef.current?.setState(currentState());
        emitPolygon();
      },
      onClear: () => {
        if (!drawRef.current) return;
        drawRef.current.deleteAll();
        isDrawingRef.current = false;
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
      isDrawingRef.current = event.mode === "draw_polygon";
      syncToolbar();
    });

    map.on("draw.create", (event) => {
      if (!drawRef.current) return;
      const polygonFeature = event.features.find(
        (feature: Feature): feature is Feature<Polygon> => (feature.geometry as any)?.type === "Polygon"
      );
      if (!polygonFeature) return;

      drawRef.current.deleteAll();
      const added = drawRef.current.add(polygonFeature) as any;
      const id = (added?.features?.[0]?.id || polygonFeature.id) as string | undefined;
      if (id) {
        drawRef.current.changeMode("simple_select", { featureIds: [id] as any });
      }

      isDrawingRef.current = false;
      syncToolbar();
      emitPolygon();
    });

    map.on("draw.update", () => {
      emitPolygon();
      syncToolbar();
    });

    map.on("draw.delete", () => {
      callbackRef.current(null);
      isDrawingRef.current = false;
      syncToolbar();
    });

    return () => {
      toolbarRef.current = null;
      map.remove();
      drawRef.current = null;
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!drawRef.current) return;

    const draw = drawRef.current;
    draw.deleteAll();

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

      if (mapRef.current) {
        const bounds = polygon.coordinates[0].reduce<maplibregl.LngLatBounds | null>((acc, coord) => {
          if (!acc) {
            return new maplibregl.LngLatBounds(coord as LngLatLike, coord as LngLatLike);
          }
          acc.extend(coord as LngLatLike);
          return acc;
        }, null);

        if (bounds && !bounds.isEmpty()) {
          mapRef.current.fitBounds(bounds, { padding: 36, duration: 300 });
        }
      }
    } else {
      toolbarRef.current?.setState({ isDrawing: false, hasPolygon: false });
    }
  }, [polygon]);

  return (
    <div className="map-wrapper">
      <div ref={containerRef} className="map-canvas" />
      <div className="map-overlay">
        <span className="map-overlay__badge">Guidance</span>
        <p>Drag vertices to adjust the parcel or press Clear to remove the polygon.</p>
      </div>
    </div>
  );
}
