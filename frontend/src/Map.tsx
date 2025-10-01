import { useEffect, useRef } from "react";
import maplibregl from "maplibre-gl";
import MapboxDraw from "@mapbox/mapbox-gl-draw";
import type {
  DrawCreateEvent,
  DrawDeleteEvent,
  DrawUpdateEvent
} from "@mapbox/mapbox-gl-draw";
import type { Feature, Polygon } from "geojson";
import type { IControl, LngLatLike } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import "@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css";

type MapProps = {
  polygon?: Polygon | null;
  onPolygon: (geometry: Polygon | null) => void;
};

const SITE_FEATURE_ID = "site";
const DEFAULT_MAP_STYLE = "https://demotiles.maplibre.org/style.json";

export default function Map({ polygon, onPolygon }: MapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const drawRef = useRef<MapboxDraw | null>(null);
  const callbackRef = useRef(onPolygon);

  useEffect(() => {
    callbackRef.current = onPolygon;
  }, [onPolygon]);

  useEffect(() => {
    if (!containerRef.current) return;

    const style = import.meta.env.VITE_MAP_STYLE || DEFAULT_MAP_STYLE;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style,
      center: [46.675, 24.713],
      zoom: 13,
    });

    mapRef.current = map;

    const draw = new MapboxDraw({
      displayControlsDefault: false,
      controls: { polygon: true, trash: true },
    });

    drawRef.current = draw;
    map.addControl(draw as unknown as IControl);

    const emitPolygon = () => {
      if (!drawRef.current) return;
      const collection = drawRef.current.getAll();
      const firstPolygon = collection.features.find(
        (feature): feature is Feature<Polygon> => feature.geometry.type === "Polygon"
      );
      callbackRef.current(firstPolygon ? firstPolygon.geometry : null);
    };

    map.on("draw.create", (event: DrawCreateEvent) => {
      if (!drawRef.current) return;
      const polygonFeature = event.features.find(
        (feature): feature is Feature<Polygon> => feature.geometry.type === "Polygon"
      );
      if (!polygonFeature) return;
      drawRef.current.deleteAll();
      drawRef.current.add(polygonFeature);
      callbackRef.current(polygonFeature.geometry);
    });

    map.on("draw.update", (_event: DrawUpdateEvent) => emitPolygon());
    map.on("draw.delete", (_event: DrawDeleteEvent) => callbackRef.current(null));

    return () => {
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

      if (mapRef.current) {
        const bounds = polygon.coordinates[0].reduce<maplibregl.LngLatBounds | null>((acc, coord) => {
          if (!acc) {
            return new maplibregl.LngLatBounds(coord as LngLatLike, coord as LngLatLike);
          }
          acc.extend(coord as LngLatLike);
          return acc;
        }, null);

        if (bounds && !bounds.isEmpty()) {
          mapRef.current.fitBounds(bounds, { padding: 24, duration: 300 });
        }
      }
    }
  }, [polygon]);

  return (
    <div
      ref={containerRef}
      style={{ width: "100%", height: 480, borderRadius: 8, border: "1px solid #d0d5dd", overflow: "hidden" }}
    />
  );
}
