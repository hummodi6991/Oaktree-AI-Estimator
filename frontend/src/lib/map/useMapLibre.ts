import { useEffect, useRef } from "react";
import maplibregl, { Map } from "maplibre-gl";
import { API_BASE } from "../api";
import type { Geometry } from "geojson";

export function useMapLibre(opts: {
  containerId: string;
  onClick?: (lng: number, lat: number) => void;
  selectedGeometry?: Geometry | null;
}) {
  const mapRef = useRef<Map | null>(null);

  useEffect(() => {
    const el = document.getElementById(opts.containerId);
    if (!el) return;
    if (mapRef.current) return;

    const styleUrl = (import.meta.env.VITE_MAP_STYLE as string) || "/esri-style.json";

    const map = new maplibregl.Map({
      container: el,
      style: styleUrl,
      center: [46.675, 24.713],
      zoom: 12,
      attributionControl: false
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: true }), "bottom-right");

    map.on("load", () => {
      map.addSource("oaktree-parcels", {
        type: "vector",
        tiles: [`${API_BASE}/v1/tiles/parcels/{z}/{x}/{y}.pbf`],
        minzoom: 0,
        maxzoom: 20
      });

      map.addLayer({
        id: "oaktree-parcels-line",
        type: "line",
        source: "oaktree-parcels",
        "source-layer": "parcels",
        paint: { "line-color": "#93c5fd", "line-width": 1, "line-opacity": 0.6 }
      });

      map.addSource("oaktree-selected", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] }
      });

      map.addLayer({
        id: "oaktree-selected-line",
        type: "line",
        source: "oaktree-selected",
        paint: { "line-color": "#a78bfa", "line-width": 3, "line-opacity": 0.95 }
      });
    });

    map.on("click", (e) => opts.onClick?.(e.lngLat.lng, e.lngLat.lat));

    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [opts.containerId]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const src: any = map.getSource("oaktree-selected");
    if (!src) return;
    if (!opts.selectedGeometry) {
      src.setData({ type: "FeatureCollection", features: [] });
      return;
    }
    src.setData({ type: "FeatureCollection", features: [{ type: "Feature", properties: {}, geometry: opts.selectedGeometry }] });
  }, [opts.selectedGeometry]);
}
