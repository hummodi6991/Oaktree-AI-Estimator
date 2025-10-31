import { useEffect, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import type { FeatureCollection, Geometry, GeoJsonProperties } from "geojson";

import "maplibre-gl/dist/maplibre-gl.css";

import { identify } from "../api";

type MapProps = {
  onParcel: (parcel: any) => void;
};

export default function Map({ onParcel }: MapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [sourceId] = useState("parcel-src");
  const [layerId] = useState("parcel-layer");

  useEffect(() => {
    if (!containerRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: import.meta.env.VITE_MAP_STYLE || "/static-style.json",
      center: [46.675, 24.713],
      zoom: 15,
    });

    map.on("click", async (e) => {
      try {
        const parcel = await identify(e.lngLat.lng, e.lngLat.lat);
        onParcel(parcel);
        const featureCollection: FeatureCollection<Geometry, GeoJsonProperties> = {
          type: "FeatureCollection",
          features: [
            {
              type: "Feature",
              geometry: parcel.geometry as Geometry,
              properties: {},
            },
          ],
        };
        if (!map.getSource(sourceId)) {
          map.addSource(sourceId, { type: "geojson", data: featureCollection });
          map.addLayer({
            id: layerId,
            type: "line",
            source: sourceId,
            paint: { "line-width": 3, "line-color": "#ff6b00" },
          });
        } else {
          (map.getSource(sourceId) as maplibregl.GeoJSONSource).setData(featureCollection);
        }
      } catch (err) {
        console.error(err);
        alert("No parcel found at that point.");
      }
    });

    return () => {
      map.remove();
    };
  }, [layerId, onParcel, sourceId]);

  return <div ref={containerRef} style={{ width: "100%", height: "60vh" }} />;
}
