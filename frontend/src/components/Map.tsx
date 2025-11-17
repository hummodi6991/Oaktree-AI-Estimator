import { useEffect, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import type { FeatureCollection, Geometry, GeoJsonProperties } from "geojson";

import "maplibre-gl/dist/maplibre-gl.css";

import { identify } from "../api";
import type { IdentifyResponse, ParcelSummary } from "../api";

type MapProps = {
  onParcel: (parcel: ParcelSummary) => void;
};

const NOT_FOUND_HINT =
  "لم يتم العثور على قطعة في هذا الموضع — حاول التكبير أو النقر داخل حدود القطعة.";

const SELECT_SOURCE_ID = "selected-parcel-src";
const SELECT_FILL_LAYER_ID = "selected-parcel-fill";
const SELECT_LINE_LAYER_ID = "selected-parcel-line";

function ensureSelectionLayers(map: maplibregl.Map) {
  if (!map.getSource(SELECT_SOURCE_ID)) {
    map.addSource(SELECT_SOURCE_ID, {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
    });

    map.addLayer({
      id: SELECT_FILL_LAYER_ID,
      type: "fill",
      source: SELECT_SOURCE_ID,
      paint: {
        "fill-color": "#3b82f6",
        "fill-opacity": 0.25,
      },
    });

    map.addLayer({
      id: SELECT_LINE_LAYER_ID,
      type: "line",
      source: SELECT_SOURCE_ID,
      paint: {
        "line-color": "#2563eb",
        "line-width": 2,
      },
    });
  }
}

function geometryToBounds(geometry?: Geometry | null) {
  if (!geometry) return null;

  let bounds: maplibregl.LngLatBounds | null = null;

  const addCoord = (coord: number[]) => {
    if (coord.length < 2) return;
    const [lng, lat] = coord;
    if (!bounds) {
      bounds = new maplibregl.LngLatBounds([lng, lat], [lng, lat]);
    } else {
      bounds.extend([lng, lat]);
    }
  };

  const walk = (coords: any) => {
    if (!coords) return;
    if (typeof coords[0] === "number") {
      addCoord(coords as number[]);
      return;
    }
    for (const child of coords as any[]) {
      walk(child);
    }
  };

  walk((geometry as any).coordinates);
  return bounds;
}

export default function Map({ onParcel }: MapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [status, setStatus] = useState<string | null>(
    "انقر على الخريطة لتحديد قطعة أرض.",
  );
  const onParcelRef = useRef(onParcel);

  useEffect(() => {
    onParcelRef.current = onParcel;
  }, [onParcel]);

  useEffect(() => {
    if (!containerRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: import.meta.env.VITE_MAP_STYLE || "/static-style.json",
      center: [46.675, 24.713],
      zoom: 15,
    });

    let disposed = false;

    map.on("load", () => {
      ensureSelectionLayers(map);
    });

    map.on("click", async (e) => {
      setStatus("جارٍ التحقق من القطعة…");
      try {
        const data: IdentifyResponse = await identify(e.lngLat.lng, e.lngLat.lat);
        if (disposed) return;

        if (!data?.found || !data.parcel) {
          setStatus(NOT_FOUND_HINT);
          return;
        }

        const parcel = data.parcel;
        onParcelRef.current(parcel);

        if (!parcel.geometry) {
          setStatus("تم العثور على القطعة لكن دون بيانات هندسية.");
          return;
        }

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

        ensureSelectionLayers(map);
        (map.getSource(SELECT_SOURCE_ID) as maplibregl.GeoJSONSource).setData(
          featureCollection,
        );

        const bounds = geometryToBounds(parcel.geometry as Geometry);
        if (bounds) {
          const camera = map.cameraForBounds(bounds, { padding: 40 });
          const currentZoom = map.getZoom();
          map.easeTo({
            center: camera?.center ?? map.getCenter(),
            zoom:
              camera?.zoom !== undefined && camera.zoom > currentZoom
                ? camera.zoom
                : currentZoom,
            duration: 700,
            essential: true,
          });
        }

        if (parcel.parcel_id) {
          setStatus(`تم تحديد القطعة ${parcel.parcel_id}.`);
        } else {
          setStatus("تم تحديد القطعة.");
        }
      } catch (err) {
        if (disposed) return;
        console.error(err);
        setStatus("تعذر تحميل بيانات القطعة. يرجى المحاولة مرة أخرى.");
      }
    });

    return () => {
      disposed = true;
      map.remove();
    };
  }, []);

  return (
    <div>
      <div
        ref={containerRef}
        style={{
          width: "100%",
          height: "60vh",
          borderRadius: 12,
          overflow: "hidden",
          boxShadow: "0 1px 2px rgba(16, 24, 40, 0.08)",
          cursor: "crosshair",
        }}
      />
      {status && (
        <div
          role="status"
          aria-live="polite"
          style={{
            marginTop: 8,
            padding: "6px 10px",
            borderRadius: 8,
            background: "rgba(17, 24, 39, 0.04)",
            color: "#475467",
            fontSize: "0.95rem",
          }}
        >
          {status}
        </div>
      )}
    </div>
  );
}
