import { useEffect, useMemo, useRef, useState, type MutableRefObject } from "react";
import maplibregl from "maplibre-gl";
import proj4 from "proj4";
import type { Feature, FeatureCollection, Geometry, GeoJsonProperties, Polygon, MultiPolygon } from "geojson";
import { useTranslation } from "react-i18next";
import { formatInteger, formatNumber } from "../i18n/format";

import "maplibre-gl/dist/maplibre-gl.css";

import { buildApiUrl, collateParcels, identify } from "../api";
import type { CollateResponse, ParcelSummary } from "../api";

type MapProps = {
  onParcel: (parcel: ParcelSummary | null) => void;
};

type StatusMessage = { key: string; options?: Record<string, unknown> } | { raw: string };

const SELECT_SOURCE_ID = "selected-parcel-src";
const SELECT_FILL_LAYER_ID = "selected-parcel-fill";
const SELECT_LINE_LAYER_ID = "selected-parcel-line";
const PARCEL_SOURCE_ID = "parcel-tiles-src";
const PARCEL_FILL_LAYER_ID = "parcel-tiles-fill";
const PARCEL_OUTLINE_CASING_LAYER_ID = "parcel-tiles-outline-casing";
const PARCEL_OUTLINE_LAYER_ID = "parcel-tiles-outline";
const HOVER_SOURCE_ID = "parcel-hover-src";
const HOVER_CASING_LAYER_ID = "parcel-hover-casing";
const HOVER_LINE_LAYER_ID = "parcel-hover-line";
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

function setHoverGeometry(
  map: maplibregl.Map,
  geometry: Geometry | null,
  hoverDataRef: MutableRefObject<FeatureCollection<Geometry, GeoJsonProperties>>,
  parcelId?: string | null,
) {
  const source = map.getSource(HOVER_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
  const data = geometry
    ? {
        type: "FeatureCollection",
        features: [
          {
            type: "Feature",
            geometry,
            properties: { parcel_id: parcelId ?? null },
          },
        ],
      }
    : EMPTY_FEATURE_COLLECTION;
  hoverDataRef.current = data;
  source?.setData(data);
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

const EMPTY_FEATURE_COLLECTION: FeatureCollection<Geometry, GeoJsonProperties> = {
  type: "FeatureCollection",
  features: [],
};

const PARCEL_OUTLINE_WIDTH = ["interpolate", ["linear"], ["zoom"], 0, 2.2, 12, 1.6, 16, 1.1, 20, 0.9] as any;

function ensureParcelLayers(map: maplibregl.Map, outlineVisible: boolean) {
  if (!map.getSource(PARCEL_SOURCE_ID)) {
    map.addSource(PARCEL_SOURCE_ID, {
      type: "vector",
      tiles: [buildApiUrl("/v1/tiles/parcels/{z}/{x}/{y}.pbf")],
      minzoom: 0,
      maxzoom: 22,
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

  if (!map.getLayer(PARCEL_OUTLINE_CASING_LAYER_ID)) {
    map.addLayer({
      id: PARCEL_OUTLINE_CASING_LAYER_ID,
      type: "line",
      source: PARCEL_SOURCE_ID,
      "source-layer": "parcels",
      layout: { "line-join": "round", "line-cap": "round", visibility: outlineVisible ? "visible" : "none" },
      paint: {
        "line-color": "rgba(0,0,0,0.55)",
        "line-opacity": 0.9,
        "line-width": ["+", PARCEL_OUTLINE_WIDTH, 1.2],
      },
    });
  }

  if (!map.getLayer(PARCEL_OUTLINE_LAYER_ID)) {
    map.addLayer({
      id: PARCEL_OUTLINE_LAYER_ID,
      type: "line",
      source: PARCEL_SOURCE_ID,
      "source-layer": "parcels",
      layout: { "line-join": "round", "line-cap": "round", visibility: outlineVisible ? "visible" : "none" },
      paint: {
        "line-color": "rgba(255,255,255,0.85)",
        "line-opacity": 0.9,
        "line-width": PARCEL_OUTLINE_WIDTH,
      },
    });
  }
}

function ensureHoverLayers(map: maplibregl.Map) {
  if (!map.getSource(HOVER_SOURCE_ID)) {
    map.addSource(HOVER_SOURCE_ID, {
      type: "geojson",
      data: EMPTY_FEATURE_COLLECTION,
    });
  }

  if (!map.getLayer(HOVER_CASING_LAYER_ID)) {
    map.addLayer({
      id: HOVER_CASING_LAYER_ID,
      type: "line",
      source: HOVER_SOURCE_ID,
      layout: { "line-join": "round", "line-cap": "round" },
      paint: {
        "line-color": "rgba(0,0,0,0.7)",
        "line-opacity": 0.95,
        "line-width": 5,
      },
    });
  }

  if (!map.getLayer(HOVER_LINE_LAYER_ID)) {
    map.addLayer({
      id: HOVER_LINE_LAYER_ID,
      type: "line",
      source: HOVER_SOURCE_ID,
      layout: { "line-join": "round", "line-cap": "round" },
      paint: {
        "line-color": "rgba(0,255,255,0.95)",
        "line-opacity": 0.95,
        "line-width": 3,
      },
    });
  }
}

function ensureLayerOrder(map: maplibregl.Map) {
  const order = [
    PARCEL_FILL_LAYER_ID,
    PARCEL_OUTLINE_CASING_LAYER_ID,
    PARCEL_OUTLINE_LAYER_ID,
    HOVER_CASING_LAYER_ID,
    HOVER_LINE_LAYER_ID,
    SELECT_FILL_LAYER_ID,
    SELECT_LINE_LAYER_ID,
  ];

  order.forEach((layerId) => {
    if (map.getLayer(layerId)) {
      map.moveLayer(layerId);
    }
  });
}

function tolForZoom(zoom: number) {
  if (zoom <= 12) return 40;
  if (zoom <= 14) return 25;
  if (zoom <= 16) return 15;
  return 8;
}

function wireHover(
  map: maplibregl.Map,
  hoverDataRef: MutableRefObject<FeatureCollection<Geometry, GeoJsonProperties>>,
) {
  let lastId: string | null = null;
  let abortController: AbortController | null = null;
  let timer: number | null = null;

  const clearHover = () => {
    lastId = null;
    setHoverGeometry(map, null, hoverDataRef);
  };

  const runIdentify = async (lng: number, lat: number) => {
    const tol = tolForZoom(map.getZoom());
    abortController?.abort();
    abortController = new AbortController();

    const params = new URLSearchParams({
      lng: String(lng),
      lat: String(lat),
      tol_m: String(tol),
    });

    try {
      const res = await fetch(buildApiUrl(`/v1/geo/identify?${params.toString()}`), {
        signal: abortController.signal,
      });
      if (!res.ok) return;
      const json = await res.json();
      const parcel = json?.parcel as ParcelSummary | null | undefined;
      if (!json?.found || !parcel?.geometry) {
        clearHover();
        return;
      }

      const geometry = transformGeometryToWgs84(parcel.geometry as Geometry | null);
      if (!geometry) {
        clearHover();
        return;
      }

      if (parcel.parcel_id && parcel.parcel_id === lastId) {
        return;
      }

      lastId = parcel.parcel_id ?? null;
      setHoverGeometry(map, geometry, hoverDataRef, parcel.parcel_id ?? null);
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") return;
      console.error(error);
    }
  };

  const scheduleIdentify = (lng: number, lat: number) => {
    if (timer) window.clearTimeout(timer);
    timer = window.setTimeout(() => runIdentify(lng, lat), 80);
  };

  const handleMove = (e: maplibregl.MapMouseEvent) => {
    scheduleIdentify(e.lngLat.lng, e.lngLat.lat);
  };

  const handleLeave = () => {
    clearHover();
  };

  map.on("mousemove", handleMove);
  map.on("touchmove", handleMove);
  map.on("mouseleave", handleLeave);

  return () => {
    if (timer) window.clearTimeout(timer);
    abortController?.abort();
    map.off("mousemove", handleMove);
    map.off("touchmove", handleMove);
    map.off("mouseleave", handleLeave);
  };
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
  const [showArcgisOutlines, setShowArcgisOutlines] = useState(true);
  const onParcelRef = useRef(onParcel);
  const multiSelectModeRef = useRef(multiSelectMode);
  const selectedParcelIdsRef = useRef(selectedParcelIds);
  const currentParcelRef = useRef<ParcelSummary | null>(null);
  const hoverDataRef = useRef<FeatureCollection<Geometry, GeoJsonProperties>>(EMPTY_FEATURE_COLLECTION);

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

  const isArcgisParcel = (parcel: ParcelSummary | null | undefined) => {
    const sourceUrl = parcel?.source_url?.toLowerCase() ?? "";
    return sourceUrl.includes("arcgis");
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
    let disposeHover: (() => void) | null = null;

    const handleZoom = () => {
      setZoomLevel(map.getZoom());
    };

    map.on("load", () => {
      ensureParcelLayers(map, showArcgisOutlines);
      ensureHoverLayers(map);
      ensureSelectionLayers(map);
      ensureLayerOrder(map);
      handleZoom();
      disposeHover = wireHover(map, hoverDataRef);
    });

    map.on("style.load", () => {
      ensureParcelLayers(map, showArcgisOutlines);
      ensureHoverLayers(map);
      ensureSelectionLayers(map);
      const selectSource = map.getSource(SELECT_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
      selectSource?.setData(selectedParcelsGeojson);
      const hoverSource = map.getSource(HOVER_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
      hoverSource?.setData(hoverDataRef.current);
      ensureLayerOrder(map);
    });

    map.on("zoom", handleZoom);

    map.on("click", async (e) => {
      setCollateStatus(null);
      try {
        const identifyResult = await identify(e.lngLat.lng, e.lngLat.lat);
        if (!identifyResult?.found || !identifyResult.parcel || !isArcgisParcel(identifyResult.parcel)) {
          setStatus({ key: "map.status.notFound" });
          const source = map.getSource(SELECT_SOURCE_ID) as maplibregl.GeoJSONSource;
          source?.setData({ type: "FeatureCollection", features: [] });
          return;
        }

        const identifyParcel = identifyResult.parcel;
        const geometry = transformGeometryToWgs84(identifyParcel.geometry as Geometry | null);
        const parcel: ParcelSummary = {
          ...identifyParcel,
          geometry,
          parcel_area_m2: identifyParcel.parcel_area_m2 ?? identifyParcel.area_m2 ?? null,
          parcel_method: identifyParcel.parcel_method ?? null,
        };

        applyParcelSelection(parcel, geometry, "feature");
      } catch (err) {
        if (disposed) return;
        console.error(err);
        setStatus({ key: "map.status.loadError" });
      }
    });

    return () => {
      disposed = true;
      map.off("zoom", handleZoom);
      disposeHover?.();
      mapRef.current = null;
      map.remove();
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    if (map.getLayer(PARCEL_OUTLINE_LAYER_ID) || map.getLayer(PARCEL_OUTLINE_CASING_LAYER_ID)) {
      [PARCEL_OUTLINE_LAYER_ID, PARCEL_OUTLINE_CASING_LAYER_ID].forEach((layerId) => {
        if (map.getLayer(layerId)) {
          map.setLayoutProperty(layerId, "visibility", showArcgisOutlines ? "visible" : "none");
        }
      });
    } else if (map.isStyleLoaded()) {
      ensureParcelLayers(map, showArcgisOutlines);
      ensureLayerOrder(map);
    }
  }, [showArcgisOutlines]);

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
            checked={showArcgisOutlines}
            onChange={(event) => {
              setShowArcgisOutlines(event.target.checked);
            }}
          />
          <span>{showArcgisOutlines ? t("map.controls.on") : t("map.controls.off")}</span>
        </label>
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
