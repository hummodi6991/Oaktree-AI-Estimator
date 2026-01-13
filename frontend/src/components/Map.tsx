import { useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import proj4 from "proj4";
import type { Feature, FeatureCollection, Geometry, GeoJsonProperties, Polygon, MultiPolygon } from "geojson";
import { useTranslation } from "react-i18next";
import { formatInteger, formatNumber } from "../i18n/format";

import "maplibre-gl/dist/maplibre-gl.css";

import { buildApiUrl, collateParcels, inferParcel, landuse } from "../api";
import type { CollateResponse, InferParcelResponse, LanduseResponse, ParcelSummary } from "../api";

type MapProps = {
  onParcel: (parcel: ParcelSummary | null) => void;
};

type StatusMessage = { key: string; options?: Record<string, unknown> } | { raw: string };

const SELECT_SOURCE_ID = "selected-parcel-src";
const SELECT_FILL_LAYER_ID = "selected-parcel-fill";
const SELECT_LINE_LAYER_ID = "selected-parcel-line";
const PARCEL_SOURCE_ID = "parcel-tiles-src";
const PARCEL_FILL_LAYER_ID = "parcel-tiles-fill";
const PARCEL_LINE_LAYER_ID = "parcel-tiles-line";
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

function parseMsParcelId(value?: string | null): { buildingId: number; partIndex: number } | null {
  if (!value) return null;
  const cleaned = value.startsWith("ms:") ? value.slice(3) : value;
  const [buildingRaw, partRaw] = cleaned.split(":");
  if (!buildingRaw || !partRaw) return null;
  const buildingId = Number(buildingRaw);
  const partIndex = Number(partRaw);
  if (!Number.isFinite(buildingId) || !Number.isFinite(partIndex)) return null;
  if (partIndex < 1) return null;
  return { buildingId, partIndex };
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

function ensureParcelLayers(map: maplibregl.Map) {
  if (!map.getSource(PARCEL_SOURCE_ID)) {
    map.addSource(PARCEL_SOURCE_ID, {
      type: "vector",
      tiles: [buildApiUrl("/v1/tiles/parcels/{z}/{x}/{y}.pbf")],
      minzoom: 0,
      maxzoom: 18,
    });
  }

  if (!map.getLayer(PARCEL_FILL_LAYER_ID)) {
    map.addLayer({
      id: PARCEL_FILL_LAYER_ID,
      type: "fill",
      source: PARCEL_SOURCE_ID,
      "source-layer": "parcels",
      layout: { visibility: "visible" },
      paint: {
        "fill-color": "#2f7bff",
        "fill-opacity": [
          "interpolate",
          ["linear"],
          ["zoom"],
          0,
          0.0,
          14.9,
          0.0,
          15.5,
          0.03,
          16,
          0.08,
        ],
      },
    });
  }

  if (!map.getLayer(PARCEL_LINE_LAYER_ID)) {
    map.addLayer({
      id: PARCEL_LINE_LAYER_ID,
      type: "line",
      source: PARCEL_SOURCE_ID,
      "source-layer": "parcels",
      layout: { visibility: "visible" },
      paint: {
        "line-color": "#2f7bff",
        "line-width": ["interpolate", ["linear"], ["zoom"], 0, 0.3, 14, 0.3, 16, 1.2, 18, 2.0],
        "line-opacity": ["interpolate", ["linear"], ["zoom"], 0, 0.4, 14, 0.4, 16, 0.9],
      },
    });
  }
}

function featureArea(feature: maplibregl.MapGeoJSONFeature): number | null {
  const props = feature.properties || {};
  const rawArea = props.parcel_area_m2 ?? props.area_m2;
  const area = rawArea != null ? Number(rawArea) : null;
  return Number.isFinite(area) ? area : null;
}

function pickBestFeature(features: maplibregl.MapGeoJSONFeature[]) {
  if (!features.length) return null;
  let best = features[0];
  let bestArea = featureArea(best);
  for (const feature of features.slice(1)) {
    const area = featureArea(feature);
    if (area == null) continue;
    if (bestArea == null || area < bestArea) {
      best = feature;
      bestArea = area;
    }
  }
  return best;
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
  const [selectionMethod, setSelectionMethod] = useState<"feature" | null>(null);
  const [zoomLevel, setZoomLevel] = useState<number | null>(null);
  const onParcelRef = useRef(onParcel);
  const multiSelectModeRef = useRef(multiSelectMode);
  const selectedParcelIdsRef = useRef(selectedParcelIds);
  const landuseRequestRef = useRef(0);
  const inferRequestRef = useRef(0);
  const lastSelectedIdRef = useRef<string | null>(null);
  const currentParcelRef = useRef<ParcelSummary | null>(null);
  const inferCacheRef = useRef(
    new globalThis.Map<string, { geometry: Geometry; area_m2: number | null; perimeter_m: number | null }>(),
  );

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

  const formatParcelId = (value?: string | null) => {
    if (!value) {
      return "";
    }
    const numericValue = Number(value);
    if (Number.isFinite(numericValue)) {
      return formatNumber(numericValue, { maximumFractionDigits: 0, minimumFractionDigits: 0 }, value);
    }
    return value;
  };

  const applyParcelSelection = (parcel: ParcelSummary, geometry: Geometry | null, method: "feature") => {
    const nextParcel = geometry ? { ...parcel, geometry } : parcel;
    currentParcelRef.current = nextParcel;
    onParcelRef.current(nextParcel);
    setSelectionMethod(method);
    lastSelectedIdRef.current = nextParcel.parcel_id ?? null;

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
      if (nextParcel.parcel_id) {
        setStatus({ key: "map.status.selectedWithId", options: { id: formatParcelId(nextParcel.parcel_id) } });
      } else {
        setStatus({ key: "map.status.selected" });
      }
    }
  };

  const applyInferredSelection = (
    parcel: ParcelSummary,
    inferredGeometry: Geometry,
    inferredArea: number | null,
    inferredPerimeter: number | null,
  ) => {
    const updatedParcel: ParcelSummary = {
      ...parcel,
      area_m2: inferredArea,
      parcel_area_m2: inferredArea,
      perimeter_m: inferredPerimeter,
      parcel_method: "click_fallback",
      geometry: inferredGeometry,
    };
    if (multiSelectModeRef.current) {
      currentParcelRef.current = updatedParcel;
      lastSelectedIdRef.current = updatedParcel.parcel_id ?? null;
      onParcelRef.current(updatedParcel);
      setSelectedParcelsGeojson((current) => ({
        type: "FeatureCollection",
        features: current.features.map((f) =>
          (f.properties as any)?.id === updatedParcel.parcel_id ? { ...f, geometry: inferredGeometry } : f,
        ),
      }));
    } else {
      applyParcelSelection(updatedParcel, inferredGeometry, "feature");
    }
  };

  useEffect(() => {
    if (!containerRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: import.meta.env.VITE_MAP_STYLE || "/esri-style.json",
      center: [46.675, 24.713],
      zoom: 15,
    });
    mapRef.current = map;

    let disposed = false;

    const handleZoom = () => {
      setZoomLevel(map.getZoom());
    };

    map.on("load", () => {
      ensureParcelLayers(map);
      ensureSelectionLayers(map);
      handleZoom();
    });

    map.on("style.load", () => {
      ensureParcelLayers(map);
      ensureSelectionLayers(map);
    });

    map.on("zoom", handleZoom);

    map.on("click", async (e) => {
      setCollateStatus(null);
      try {
        let features = map.queryRenderedFeatures(e.point, {
          layers: [PARCEL_FILL_LAYER_ID],
        });
        if (!features.length && map.getLayer(PARCEL_LINE_LAYER_ID)) {
          features = map.queryRenderedFeatures(e.point, {
            layers: [PARCEL_LINE_LAYER_ID],
          });
        }
        if (!features.length) {
          setStatus({ key: "map.status.notFound" });
          const source = map.getSource(SELECT_SOURCE_ID) as maplibregl.GeoJSONSource;
          source?.setData({ type: "FeatureCollection", features: [] });
          return;
        }

        const feature = pickBestFeature(features);
        if (!feature) {
          setStatus({ key: "map.status.notFound" });
          return;
        }
        const geometry = feature.geometry as Geometry | null;
        if (!geometry) {
          setStatus({ key: "map.status.notFound" });
          return;
        }

        const props = feature.properties || {};
        const rawId = props.parcel_id;
        const parcelId = rawId != null ? String(rawId) : null;
        const buildingIdRaw = props.building_id != null ? Number(props.building_id) : null;
        const partIndexRaw = props.part_index != null ? Number(props.part_index) : null;
        const parsedId = parcelId ? parseMsParcelId(parcelId) : null;
        const buildingId = Number.isFinite(buildingIdRaw) ? Number(buildingIdRaw) : parsedId?.buildingId ?? null;
        const partIndex = Number.isFinite(partIndexRaw) ? Number(partIndexRaw) : parsedId?.partIndex ?? null;
        const areaM2 = props.area_m2 != null ? Number(props.area_m2) : null;
        const parcelAreaM2 = props.parcel_area_m2 != null ? Number(props.parcel_area_m2) : null;
        const footprintAreaM2 = props.footprint_area_m2 != null ? Number(props.footprint_area_m2) : null;
        const perimeterM = props.perimeter_m != null ? Number(props.perimeter_m) : null;
        const methodRaw = props.method != null ? String(props.method) : null;
        const isPrecomputedParcel = methodRaw === "road_block_voronoi_v1";
        const parcel: ParcelSummary = {
          parcel_id: parcelId,
          area_m2: Number.isFinite(areaM2) ? areaM2 : null,
          parcel_area_m2: Number.isFinite(parcelAreaM2) ? parcelAreaM2 : null,
          footprint_area_m2: Number.isFinite(footprintAreaM2) ? footprintAreaM2 : null,
          perimeter_m: Number.isFinite(perimeterM) ? perimeterM : null,
          parcel_method: isPrecomputedParcel ? "inferred_parcels_v1" : null,
          geometry,
        };

        applyParcelSelection(parcel, geometry, "feature");

        try {
          const requestId = ++landuseRequestRef.current;
          const landuseData: LanduseResponse = await landuse(e.lngLat.lng, e.lngLat.lat);
          if (disposed) return;
          if (landuseRequestRef.current !== requestId) return;
          if (lastSelectedIdRef.current !== parcelId) return;
          const latestParcel = currentParcelRef.current ?? parcel;
          onParcelRef.current({ ...latestParcel, ...landuseData });
        } catch (error) {
          if (!disposed) {
            console.warn("Landuse lookup failed", error);
          }
        }

        const shouldInfer = !isPrecomputedParcel;
        if (shouldInfer && parcelId && inferCacheRef.current.has(parcelId)) {
          const cached = inferCacheRef.current.get(parcelId);
          if (cached) {
            const inferredArea = cached.area_m2 ?? parcel.area_m2 ?? null;
            const inferredPerimeter = cached.perimeter_m ?? parcel.perimeter_m ?? null;
            applyInferredSelection(parcel, cached.geometry, inferredArea, inferredPerimeter);
          }
          return;
        }

        if (shouldInfer && buildingId != null && partIndex != null) {
          try {
            const inferRequestId = ++inferRequestRef.current;
            const inferred: InferParcelResponse = await inferParcel({
              lng: e.lngLat.lng,
              lat: e.lngLat.lat,
              buildingId,
              partIndex,
            });
            if (disposed) return;
            if (inferRequestRef.current !== inferRequestId) return;
            if (lastSelectedIdRef.current !== parcelId) return;
            if (!inferred?.found || !inferred.geom) return;
            if (inferred.method === "inferred_parcels_v1") {
              return;
            }
            const inferredGeometry = inferred.geom as Geometry;
            const inferredArea = inferred.area_m2 ?? parcel.area_m2 ?? null;
            const inferredPerimeter = inferred.perimeter_m ?? parcel.perimeter_m ?? null;
            if (parcelId) {
              inferCacheRef.current.set(parcelId, {
                geometry: inferredGeometry,
                area_m2: inferredArea,
                perimeter_m: inferredPerimeter,
              });
            }
            applyInferredSelection(parcel, inferredGeometry, inferredArea, inferredPerimeter);
          } catch (error) {
            if (!disposed) {
              console.warn("Infer parcel lookup failed", error);
            }
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
      map.off("zoom", handleZoom);
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
    setSelectionMethod(null);
    lastSelectedIdRef.current = null;
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
      <div style={{ position: "relative" }}>
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
        {zoomLevel != null && (
          <div
            style={{
              position: "absolute",
              right: 10,
              bottom: 10,
              padding: "2px 6px",
              borderRadius: 6,
              background: "rgba(17, 24, 39, 0.55)",
              color: "white",
              fontSize: "0.75rem",
              letterSpacing: "0.02em",
              pointerEvents: "none",
            }}
          >
            Zoom: {zoomLevel.toFixed(1)}
          </div>
        )}
      </div>
      <div style={{ marginTop: 6, fontSize: "0.85rem", color: "rgba(71, 84, 103, 0.9)" }}>
        {t("map.disclaimer")}
      </div>
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
        <span style={{ fontSize: "0.9rem", color: "#475467" }}>{t("map.controls.parcelOutlines")}</span>
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
