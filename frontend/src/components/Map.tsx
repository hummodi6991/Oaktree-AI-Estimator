import { useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import proj4 from "proj4";
import type { Feature, FeatureCollection, Geometry, GeoJsonProperties, Polygon, MultiPolygon } from "geojson";
import { useTranslation } from "react-i18next";
import { formatInteger, formatNumber } from "../i18n/format";

import "maplibre-gl/dist/maplibre-gl.css";

import { buildApiUrl, collateParcels, identify } from "../api";
import type { CollateResponse, IdentifyResponse, ParcelSummary } from "../api";

type MapProps = {
  onParcel: (parcel: ParcelSummary | null) => void;
};

type StatusMessage = { key: string; options?: Record<string, unknown> } | { raw: string };

const SELECT_SOURCE_ID = "selected-parcel-src";
const SELECT_FILL_LAYER_ID = "selected-parcel-fill";
const SELECT_LINE_LAYER_ID = "selected-parcel-line";
const OVERTURE_SOURCE_ID = "overture-footprints";
const OVERTURE_LAYER_ID = "overture-footprints-outline";
const PARCEL_SOURCE_ID = "parcel-outlines";
const PARCEL_LINE_BASE_LAYER_ID = "parcels-line-base";
const PARCEL_LINE_LAYER_ID = "parcel-outlines-line";
const OVT_MIN_ZOOM = 15;

const SOURCE_CRS = "EPSG:32638";
proj4.defs(SOURCE_CRS, "+proj=utm +zone=38 +datum=WGS84 +units=m +no_defs");

function toWgs84(coord: [number, number]) {
  return proj4(SOURCE_CRS, "WGS84", coord) as [number, number];
}

function transformPolygonCoords(coords: number[][][]) {
  return coords.map((ring) => ring.map((coord) => toWgs84(coord as [number, number])));
}

function isLikelyWgs84(coord: [number, number]) {
  const [lng, lat] = coord;
  return Math.abs(lng) <= 180 && Math.abs(lat) <= 90;
}

function geometryAlreadyWgs84(geometry: Geometry) {
  if (geometry.type === "Polygon") {
    const first = geometry.coordinates?.[0]?.[0] as [number, number] | undefined;
    return first ? isLikelyWgs84(first) : false;
  }
  if (geometry.type === "MultiPolygon") {
    const first = geometry.coordinates?.[0]?.[0]?.[0] as [number, number] | undefined;
    return first ? isLikelyWgs84(first) : false;
  }
  return false;
}

function featureFromParcel(parcel: ParcelSummary): Feature<Geometry> | null {
  const geometry = parcel.geometry as Geometry | null | undefined;
  if (!geometry) return null;
  return {
    type: "Feature",
    geometry,
    properties: {
      id: parcel.parcel_id || undefined,
    },
  };
}

function transformGeometryToWgs84(geometry?: Geometry | null): Geometry | null {
  if (!geometry) return null;
  if (geometryAlreadyWgs84(geometry)) return geometry;
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
        "fill-color": "#2f7bff",
        "fill-opacity": 0.35,
        "fill-outline-color": "#1f5fd6",
      },
    });

    map.addLayer({
      id: SELECT_LINE_LAYER_ID,
      type: "line",
      source: SELECT_SOURCE_ID,
      paint: {
        "line-color": "#1f5fd6",
        "line-width": 2.5,
        "line-opacity": 0.9,
      },
    });
  }
}

function getBeforeLayerId(map: maplibregl.Map) {
  return map.getStyle()?.layers?.find((layer) => layer.type === "symbol")?.id;
}

function ensureOvertureOverlay(map: maplibregl.Map) {
  const overtureTileUrl = buildApiUrl("/v1/tiles/ovt/{z}/{x}/{y}.pbf");
  if (!map.getSource(OVERTURE_SOURCE_ID)) {
    map.addSource(OVERTURE_SOURCE_ID, {
      type: "vector",
      tiles: [overtureTileUrl],
      minzoom: OVT_MIN_ZOOM,
      maxzoom: 22,
    });
  }

  const beforeLayerId = getBeforeLayerId(map);
  if (!map.getLayer(OVERTURE_LAYER_ID)) {
    map.addLayer(
      {
        id: OVERTURE_LAYER_ID,
        type: "line",
        source: OVERTURE_SOURCE_ID,
        "source-layer": "buildings",
        minzoom: OVT_MIN_ZOOM,
        layout: {
          visibility: "visible",
          "line-join": "round",
          "line-cap": "round",
        },
        paint: {
          "line-color": "#2b6cb0",
          "line-width": ["interpolate", ["linear"], ["zoom"], 15, 1.2, 20, 3],
          "line-opacity": 0.9,
        },
      },
      beforeLayerId
    );
  } else {
    map.setLayoutProperty(OVERTURE_LAYER_ID, "visibility", "visible");
  }
}

function ensureParcelOverlay(map: maplibregl.Map) {
  const parcelTileUrl = buildApiUrl("/v1/tiles/parcels/{z}/{x}/{y}.pbf");
  if (!map.getSource(PARCEL_SOURCE_ID)) {
    map.addSource(PARCEL_SOURCE_ID, {
      type: "vector",
      tiles: [parcelTileUrl],
      minzoom: 10,
      maxzoom: 22,
    });
  }

  const beforeLayerId = getBeforeLayerId(map);
  if (!map.getLayer(PARCEL_LINE_BASE_LAYER_ID)) {
    map.addLayer(
      {
        id: PARCEL_LINE_BASE_LAYER_ID,
        type: "line",
        source: PARCEL_SOURCE_ID,
        "source-layer": "parcels",
        minzoom: 15,
        layout: { visibility: "visible" },
        paint: {
          "line-color": "#00AEEF",
          "line-width": 1,
          "line-opacity": 0.35,
        },
      },
      beforeLayerId
    );
  } else {
    map.setLayoutProperty(PARCEL_LINE_BASE_LAYER_ID, "visibility", "visible");
  }

  if (!map.getLayer(PARCEL_LINE_LAYER_ID)) {
    map.addLayer(
      {
        id: PARCEL_LINE_LAYER_ID,
        type: "line",
        source: PARCEL_SOURCE_ID,
        "source-layer": "parcels",
        minzoom: 15,
        layout: { visibility: "visible" },
        paint: {
          "line-color": "#8a5dff",
          "line-width": ["interpolate", ["linear"], ["zoom"], 15, 0.7, 20, 2.0],
          "line-opacity": 0.85,
        },
      },
      beforeLayerId
    );
  } else {
    map.setLayoutProperty(PARCEL_LINE_LAYER_ID, "visibility", "visible");
  }
}

export default function Map({ onParcel }: MapProps) {
  const { t } = useTranslation();
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const tRef = useRef(t);
  const [status, setStatus] = useState<StatusMessage | null>({ key: "map.status.prompt" });
  const [multiSelectMode, setMultiSelectMode] = useState(false);
  const [selectedParcelIds, setSelectedParcelIds] = useState<string[]>([]);
  const [selectedParcelsGeojson, setSelectedParcelsGeojson] = useState<
    FeatureCollection<Geometry, GeoJsonProperties>
  >({ type: "FeatureCollection", features: [] });
  const [collateStatus, setCollateStatus] = useState<StatusMessage | null>(null);
  const [collating, setCollating] = useState(false);
  const onParcelRef = useRef(onParcel);
  const multiSelectModeRef = useRef(multiSelectMode);
  const selectedParcelIdsRef = useRef(selectedParcelIds);

  const renderStatus = useMemo(() => {
    if (!status) return null;
    if ("raw" in status) return status.raw;
    return t(status.key, status.options);
  }, [status, t]);

  const renderCollateStatus = useMemo(() => {
    if (!collateStatus) return null;
    if ("raw" in collateStatus) return collateStatus.raw;
    return t(collateStatus.key, collateStatus.options);
  }, [collateStatus, t]);

  useEffect(() => {
    tRef.current = t;
  }, [t]);

  useEffect(() => {
    onParcelRef.current = onParcel;
  }, [onParcel]);

  useEffect(() => {
    multiSelectModeRef.current = multiSelectMode;
  }, [multiSelectMode]);

  useEffect(() => {
    selectedParcelIdsRef.current = selectedParcelIds;
  }, [selectedParcelIds]);

  const formatParcelId = (value: string) => {
    const numericValue = Number(value);
    if (Number.isFinite(numericValue)) {
      return formatNumber(numericValue, { maximumFractionDigits: 0, minimumFractionDigits: 0 }, value);
    }
    return value;
  };

  useEffect(() => {
    if (!containerRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: import.meta.env.VITE_MAP_STYLE || "/static-style.json",
      center: [46.675, 24.713],
      zoom: 15,
    });
    mapRef.current = map;

    let disposed = false;

    map.on("load", () => {
      ensureOvertureOverlay(map);
      ensureParcelOverlay(map);
      ensureSelectionLayers(map);
    });

    map.on("style.load", () => {
      ensureOvertureOverlay(map);
      ensureParcelOverlay(map);
      ensureSelectionLayers(map);
    });

    map.on("click", async (e) => {
      setStatus({ key: "map.status.checking" });
      setCollateStatus(null);
      try {
        const data: IdentifyResponse = await identify(e.lngLat.lng, e.lngLat.lat);
        if (disposed) return;

        if (!data?.found || !data.parcel) {
          setStatus({ key: "map.status.notFound" });
          const source = map.getSource(SELECT_SOURCE_ID) as maplibregl.GeoJSONSource;
          source?.setData({ type: "FeatureCollection", features: [] });
          return;
        }

        const parcel = data.parcel;
        const geometry = transformGeometryToWgs84(parcel.geometry as Geometry | null);
        const nextParcel = geometry ? { ...parcel, geometry } : parcel;
        onParcelRef.current(nextParcel);

        if (!geometry) {
          setStatus({ key: "map.status.noGeometry" });
          return;
        }

        if (multiSelectModeRef.current) {
          if (!nextParcel.parcel_id) {
            setStatus({ key: "map.status.missingId" });
            return;
          }
          setSelectedParcelsGeojson((current) => {
            const currentIds = selectedParcelIdsRef.current;
            const alreadySelected = currentIds.includes(nextParcel.parcel_id as string);
            const nextIds = alreadySelected
              ? currentIds.filter((id) => id !== nextParcel.parcel_id)
              : [...currentIds, nextParcel.parcel_id as string];
            setSelectedParcelIds(nextIds);

            if (alreadySelected) {
              setStatus({ key: "map.status.removedSelection", options: { id: formatParcelId(nextParcel.parcel_id) } });
              return {
                type: "FeatureCollection",
                features: current.features.filter(
                  (f) => (f.properties as any)?.id !== nextParcel.parcel_id,
                ),
              };
            }

            const nextFeature = featureFromParcel(nextParcel);
            setStatus({
              key: "map.status.addedSelection",
              options: { id: formatParcelId(nextParcel.parcel_id), count: formatInteger(nextIds.length) },
            });
            if (!nextFeature) {
              return current;
            }
            return {
              type: "FeatureCollection",
              features: [...current.features, nextFeature],
            };
          });
        } else {
          const nextFeature = featureFromParcel(nextParcel);
          setSelectedParcelIds(nextParcel.parcel_id ? [nextParcel.parcel_id] : []);
          setSelectedParcelsGeojson(
            nextFeature
              ? { type: "FeatureCollection", features: [nextFeature] }
              : { type: "FeatureCollection", features: [] },
          );
          if (parcel.parcel_id) {
            setStatus({ key: "map.status.selectedWithId", options: { id: formatParcelId(parcel.parcel_id) } });
          } else {
            setStatus({ key: "map.status.selected" });
          }
        }
      } catch (err) {
        if (disposed) return;
        console.error(err);
        setStatus({ key: "map.status.loadError" });
      }
    });

    return () => {
      disposed = true;
      mapRef.current = null;
      map.remove();
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const source = map.getSource(SELECT_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
    if (source) {
      source.setData(selectedParcelsGeojson);
    }
  }, [selectedParcelsGeojson]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const source = map.getSource(SELECT_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
    source?.setData(selectedParcelsGeojson);
  }, [multiSelectMode, selectedParcelsGeojson]);

  const handleClearSelection = () => {
    setSelectedParcelIds([]);
    setSelectedParcelsGeojson({ type: "FeatureCollection", features: [] });
    onParcelRef.current(null);
    setStatus({ key: "map.status.cleared" });
    setCollateStatus(null);
  };

  const handleCollate = async () => {
    if (selectedParcelIds.length < 2) return;
    setCollating(true);
    setCollateStatus({ key: "map.status.collating" });
    try {
      const res: CollateResponse = await collateParcels(selectedParcelIds);
      if (!res?.found || !res.parcel || !res.parcel.geometry) {
        setCollateStatus(res?.message ? { raw: res.message } : { key: "map.status.collateFailed" });
        return;
      }
      const geometry = transformGeometryToWgs84(res.parcel.geometry as Geometry | null);
      const mergedParcel = geometry ? { ...res.parcel, geometry } : res.parcel;
      onParcelRef.current(mergedParcel);
      setStatus({ key: "map.status.mergeApplied" });
      setCollateStatus({ key: "map.status.mergeSuccess" });
      const mergedFeature = featureFromParcel(mergedParcel);
      setSelectedParcelIds(mergedParcel.parcel_id ? [mergedParcel.parcel_id] : []);
      setSelectedParcelsGeojson(
        mergedFeature
          ? { type: "FeatureCollection", features: [mergedFeature] }
          : { type: "FeatureCollection", features: [] },
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error || tRef.current("map.status.collateFailed"));
      setCollateStatus({ raw: message });
    } finally {
      setCollating(false);
    }
  };

  const selectedList = selectedParcelIds.length
    ? t("map.status.selectedList", { list: selectedParcelIds.map((id) => formatParcelId(id)).join(", ") })
    : t("map.status.noneSelected");

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
      {renderStatus && (
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
          {renderStatus}
        </div>
      )}
      <div
        style={{
          marginTop: 12,
          display: "flex",
          flexWrap: "wrap",
          gap: 10,
          alignItems: "center",
        }}
      >
        <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <input
            type="checkbox"
            checked={multiSelectMode}
            onChange={(event) => {
              setMultiSelectMode(event.target.checked);
              setCollateStatus(null);
              if (!event.target.checked && selectedParcelIds.length > 1) {
                // When leaving multi-select, keep only the most recent selection if available
                const lastId = selectedParcelIds[selectedParcelIds.length - 1];
                const lastFeature = selectedParcelsGeojson.features.find(
                  (f) => (f.properties as any)?.id === lastId,
                );
                setSelectedParcelIds(lastId ? [lastId] : []);
                setSelectedParcelsGeojson(
                  lastFeature
                    ? { type: "FeatureCollection", features: [lastFeature] }
                    : { type: "FeatureCollection", features: [] },
                );
              }
            }}
          />
          <span>{t("map.controls.multiSelect")}</span>
        </label>
        <button type="button" onClick={handleClearSelection} disabled={!selectedParcelIds.length}>
          {t("map.controls.clearSelection")}
        </button>
        <button
          type="button"
          onClick={handleCollate}
          disabled={selectedParcelIds.length < 2 || collating}
        >
          {collating ? t("map.controls.merging") : t("map.controls.mergeParcels")}
        </button>
        <span style={{ fontSize: "0.9rem", color: "#475467" }}>
          {selectedList}
        </span>
      </div>
      {renderCollateStatus && (
        <div style={{ marginTop: 8, color: "#475467", fontSize: "0.95rem" }}>
          {renderCollateStatus}
        </div>
      )}
    </div>
  );
}
