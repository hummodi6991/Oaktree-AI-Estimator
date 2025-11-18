import { useEffect, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import proj4 from "proj4";
import type { FeatureCollection, Geometry, GeoJsonProperties, Polygon, MultiPolygon } from "geojson";

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

const SOURCE_CRS = "EPSG:32638";
proj4.defs(SOURCE_CRS, "+proj=utm +zone=38 +datum=WGS84 +units=m +no_defs");

function toWgs84(coord: [number, number]) {
  return proj4(SOURCE_CRS, "WGS84", coord) as [number, number];
}

function transformPolygonCoords(coords: number[][][]) {
  return coords.map((ring) => ring.map((coord) => toWgs84(coord as [number, number])));
}

function transformGeometryToWgs84(geometry?: Geometry | null): Geometry | null {
  if (!geometry) return null;
  if (geometry.type === "Polygon") {
    return { type: "Polygon", coordinates: transformPolygonCoords(geometry.coordinates as number[][][]) } as Polygon;
  }
  if (geometry.type === "MultiPolygon") {
    return {
      type: "MultiPolygon",
      coordinates: (geometry.coordinates as number[][][][]).map(transformPolygonCoords),
    } as MultiPolygon;
  }
  return geometry;
}

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
        "fill-color": "#356fe9",
        "fill-opacity": 0.25,
      },
    });

    map.addLayer({
      id: SELECT_LINE_LAYER_ID,
      type: "line",
      source: SELECT_SOURCE_ID,
      paint: {
        "line-color": "#2b66b3",
        "line-width": 2,
      },
    });
  }
}

function fitToGeometry(map: maplibregl.Map, geometry: Geometry) {
  const bounds = new maplibregl.LngLatBounds();
  const pushCoords = (coords: any) => {
    if (!coords) return;
    if (typeof coords[0] === "number" && typeof coords[1] === "number") {
      bounds.extend(coords as [number, number]);
      return;
    }
    if (Array.isArray(coords)) {
      coords.forEach(pushCoords);
    }
  };

  pushCoords((geometry as any).coordinates);

  if (bounds.isEmpty()) return;

  map.fitBounds(bounds, {
    padding: 20,
    maxZoom: 19,
    duration: 250,
  });
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
        const data: IdentifyResponse = await identify(e.lngLat.lng, e.lngLat.lat, 25);
        if (disposed) return;

        if (!data?.found || !data.parcel) {
          setStatus(NOT_FOUND_HINT);
          const source = map.getSource(SELECT_SOURCE_ID) as maplibregl.GeoJSONSource;
          source?.setData({ type: "FeatureCollection", features: [] });
          return;
        }

        const parcel = data.parcel;
        const geometry = transformGeometryToWgs84(parcel.geometry as Geometry | null);
        const nextParcel = geometry ? { ...parcel, geometry } : parcel;
        onParcelRef.current(nextParcel);

        if (!geometry) {
          setStatus("تم العثور على القطعة لكن دون بيانات هندسية.");
          return;
        }

        const featureCollection: FeatureCollection<Geometry, GeoJsonProperties> = {
          type: "FeatureCollection",
          features: [
            {
              type: "Feature",
              geometry: geometry as Geometry,
              properties: {},
            },
          ],
        };

        ensureSelectionLayers(map);
        (map.getSource(SELECT_SOURCE_ID) as maplibregl.GeoJSONSource).setData(
          featureCollection,
        );

        fitToGeometry(map, geometry);

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
