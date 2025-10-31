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

export default function Map({ onParcel }: MapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [sourceId] = useState("parcel-src");
  const [layerId] = useState("parcel-layer");
  const [status, setStatus] = useState<string | null>(
    "انقر على الخريطة لتحديد قطعة أرض.",
  );

  useEffect(() => {
    if (!containerRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: import.meta.env.VITE_MAP_STYLE || "/static-style.json",
      center: [46.675, 24.713],
      zoom: 15,
    });

    let disposed = false;

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
        onParcel(parcel);

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
  }, [layerId, onParcel, sourceId, setStatus]);

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
