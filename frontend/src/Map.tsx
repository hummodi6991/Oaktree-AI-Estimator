import { useEffect, useRef } from "react";
import maplibregl from "maplibre-gl";
import MapboxDraw from "@mapbox/mapbox-gl-draw";
import "@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css";

type Props = {
  initial?: any;
  onChange: (geojson: any) => void;
};

export default function Map({ initial, onChange }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const drawRef = useRef<MapboxDraw | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: "https://demotiles.maplibre.org/style.json",
      center: [46.675, 24.713],
      zoom: 13
    });
    mapRef.current = map;

    const draw = new MapboxDraw({
      displayControlsDefault: false,
      controls: { polygon: true, trash: true }
    });
    drawRef.current = draw;
    map.addControl(draw);

    map.on("load", () => {
      if (initial) {
        try {
          draw.deleteAll();
          draw.add({
            id: "site",
            type: "Feature",
            properties: {},
            geometry: initial
          } as any);
        } catch (error) {
          console.warn("Failed to load initial geometry", error);
        }
      }
    });

    const update = () => {
      if (!drawRef.current) return;
      const fc = drawRef.current.getAll();
      const poly = fc.features.find(f => f.geometry.type === "Polygon");
      if (poly) {
        onChange(poly.geometry);
      }
    };

    map.on("draw.create", update);
    map.on("draw.update", update);
    map.on("draw.delete", () => onChange(null));

    return () => {
      map.remove();
      mapRef.current = null;
      drawRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!drawRef.current) return;
    const draw = drawRef.current;
    draw.deleteAll();

    if (initial) {
      try {
        draw.add({
          id: "site",
          type: "Feature",
          properties: {},
          geometry: initial
        } as any);
      } catch (error) {
        console.warn("Failed to update geometry", error);
      }
    }
  }, [initial]);

  return (
    <div
      ref={containerRef}
      style={{ width: "100%", height: 360, borderRadius: 6, border: "1px solid #ddd" }}
    />
  );
}
