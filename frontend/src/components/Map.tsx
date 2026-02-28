import { useEffect, useMemo, useRef, useState, type MutableRefObject } from "react";
import maplibregl from "maplibre-gl";
import proj4 from "proj4";
import type { Feature, FeatureCollection, Geometry, GeoJsonProperties, Polygon, MultiPolygon } from "geojson";
import { useTranslation } from "react-i18next";
import { formatInteger, formatNumber } from "../i18n/format";
import {
  installParcelDebugLogging,
  installParcelLayerPersistence,
  PARCELS_MIXEDUSE_LAYER_ID,
  PARCELS_OUTLINE_LAYER_ID,
} from "../map/parcelLayers";

import "maplibre-gl/dist/maplibre-gl.css";

import { buildApiUrl, trackEvent } from "../api";
import type { ParcelSummary } from "../api";
import { collateParcels, identifyPoint, type CollateResponse } from "../lib/api/geo";
import MapSearchBar from "./MapSearchBar";
import type { SearchItem } from "../types/search";

type MapProps = {
  onParcel: (parcel: ParcelSummary | null) => void;
  showSearchBar?: boolean;
  showSelectionUi?: boolean;
  focusTarget?: SearchItem | null;
  mapHeight?: string | number;
  mapContainerClassName?: string;
  uiVariant?: "legacy" | "v2";
};

type StatusMessage = { key: string; options?: Record<string, unknown> } | { raw: string };

const SELECT_SOURCE_ID = "selected-parcel-src";
const SELECT_FILL_LAYER_ID = "selected-parcel-fill";
const SELECT_LINE_LAYER_ID = "selected-parcel-line";
const PARCELS_CLASS_FILL_LAYER_ID = "oaktree-parcels-class-fill";
const HOVER_SOURCE_ID = "parcel-hover-src";
const HOVER_CASING_LAYER_ID = "parcel-hover-casing";
const HOVER_LINE_LAYER_ID = "parcel-hover-line";
const DISTRICT_LABELS_LAYER_ID = "oaktree-district-labels";
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
  const data: FeatureCollection<Geometry, GeoJsonProperties> = geometry
    ? ({
        type: "FeatureCollection",
        features: [
          {
            type: "Feature",
            geometry,
            properties: { parcel_id: parcelId ?? null },
          },
        ],
      } satisfies FeatureCollection<Geometry, GeoJsonProperties>)
    : EMPTY_FEATURE_COLLECTION;
  hoverDataRef.current = data;
  source?.setData(data);
}

function ensureSelectionLayers(map: maplibregl.Map, variant: "legacy" | "v2") {
  if (!map.getSource(SELECT_SOURCE_ID)) {
    map.addSource(SELECT_SOURCE_ID, {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
    });

    const isV2 = variant === "v2";

    map.addLayer({
      id: SELECT_FILL_LAYER_ID,
      type: "fill",
      source: SELECT_SOURCE_ID,
      paint: {
        "fill-color": isV2 ? "#335c4f" : "#2f7bff",
        "fill-opacity": isV2 ? 0.18 : 0.35,
        "fill-outline-color": isV2 ? "#21443a" : "#1f5fd6",
      },
    });

    map.addLayer({
      id: SELECT_LINE_LAYER_ID,
      type: "line",
      source: SELECT_SOURCE_ID,
      paint: {
        "line-color": isV2 ? "#21443a" : "#1f5fd6",
        "line-width": isV2 ? 2 : 2.5,
        "line-opacity": isV2 ? 0.85 : 0.9,
      },
    });
  }
}

const EMPTY_FEATURE_COLLECTION: FeatureCollection<Geometry, GeoJsonProperties> = {
  type: "FeatureCollection",
  features: [],
};

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
    PARCELS_CLASS_FILL_LAYER_ID,
    PARCELS_MIXEDUSE_LAYER_ID,
    PARCELS_OUTLINE_LAYER_ID,
    HOVER_CASING_LAYER_ID,
    HOVER_LINE_LAYER_ID,
    SELECT_FILL_LAYER_ID,
    SELECT_LINE_LAYER_ID,
    DISTRICT_LABELS_LAYER_ID,
  ];

  order.forEach((layerId) => {
    if (map.getLayer(layerId)) {
      map.moveLayer(layerId);
    }
  });
}

function getSafeMaxZoom(map: maplibregl.Map) {
  const maxZ = map.getMaxZoom?.() ?? 18;
  return Math.min(maxZ, 18);
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

export default function Map({
  onParcel,
  showSearchBar = true,
  showSelectionUi = true,
  focusTarget = null,
  mapHeight = "60vh",
  mapContainerClassName,
  uiVariant = "legacy",
}: MapProps) {
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
  const uiVariantRef = useRef<"legacy" | "v2">(uiVariant);
  const suppressNextClickRef = useRef(false);
  const multiSelectModeRef = useRef(multiSelectMode);
  const selectedParcelIdsRef = useRef(selectedParcelIds);
  const currentParcelRef = useRef<ParcelSummary | null>(null);
  const hoverDataRef = useRef<FeatureCollection<Geometry, GeoJsonProperties>>(EMPTY_FEATURE_COLLECTION);
  const parcelPropertiesLoggedRef = useRef(false);

  const [showMultiHint, setShowMultiHint] = useState(true);

  const isMultiSelectModifier = (evt: unknown): boolean => {
    if (!evt || typeof evt !== "object") return false;
    const keyEvent = evt as { shiftKey?: boolean; ctrlKey?: boolean; metaKey?: boolean };
    return Boolean(keyEvent.shiftKey || keyEvent.ctrlKey || keyEvent.metaKey);
  };

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

  useEffect(() => {
    uiVariantRef.current = uiVariant;
  }, [uiVariant]);

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

  const applyParcelSelection = (
    parcel: ParcelSummary,
    geometry: Geometry | null,
    method: "feature",
    options?: { appendSelection?: boolean },
  ) => {
    const appendSelection = Boolean(options?.appendSelection);
    const nextParcel = geometry ? { ...parcel, geometry } : parcel;
    if (!appendSelection) {
      currentParcelRef.current = nextParcel;
      onParcelRef.current(nextParcel);
      setSelectionMethod(method);
    }
    if (!appendSelection && nextParcel.parcel_id) {
      void trackEvent("ui_parcel_selected", {
        meta: {
          parcel_id: nextParcel.parcel_id,
          landuse_code: nextParcel.landuse_code ?? null,
          landuse_method: nextParcel.landuse_method ?? null,
        },
      });
    }

    if (!geometry) {
      setStatus({ key: "map.status.noGeometry" });
      return;
    }

    if (appendSelection || multiSelectModeRef.current) {
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

        if (uiVariantRef.current === "v2" && nextIds.length >= 2) setShowMultiHint(false);

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

  const transformRequest = (url: string): maplibregl.RequestParameters => {
    if (typeof window === "undefined") {
      return { url };
    }
    const apiKey = window.localStorage.getItem("oaktree_api_key");
    if (!apiKey) {
      return { url };
    }
    let resolved: URL | null = null;
    try {
      resolved = new URL(url, window.location.origin);
    } catch {
      resolved = null;
    }
    const isRelativeBackend = url.startsWith("/v1/");
    const isSameOrigin = resolved?.origin === window.location.origin;
    if (isRelativeBackend || isSameOrigin) {
      return { url, headers: { "X-API-Key": apiKey } };
    }
    return { url };
  };

  useEffect(() => {
    if (!containerRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: import.meta.env.VITE_MAP_STYLE || "/esri-style.json",
      center: [46.675, 24.713],
      zoom: 15,
      transformRequest,
    });
    mapRef.current = map;
    installParcelLayerPersistence(map);
    installParcelDebugLogging(map);

    // Keep style-defined parcel and district layers above runtime overlays after layer/source churn.
    const reapplyLayerOrder = () => ensureLayerOrder(map);
    map.on("idle", reapplyLayerOrder);
    map.on("styledata", reapplyLayerOrder);

    let disposed = false;
    let disposeHover: (() => void) | null = null;

    const handleZoom = () => {
      setZoomLevel(map.getZoom());
    };

    const logParcelPropertiesOnce = () => {
      if (parcelPropertiesLoggedRef.current) return;
      const features = map.queryRenderedFeatures({
        layers: [PARCELS_MIXEDUSE_LAYER_ID, PARCELS_OUTLINE_LAYER_ID],
      });
      if (!features.length) return;
      const keys = Object.keys(features[0]?.properties ?? {});
      console.info("Parcel feature properties keys:", keys);
      parcelPropertiesLoggedRef.current = true;
    };

    map.on("load", () => {
      ensureHoverLayers(map);
      ensureSelectionLayers(map, uiVariantRef.current);
      ensureLayerOrder(map);
      handleZoom();
      disposeHover = wireHover(map, hoverDataRef);
      map.on("idle", logParcelPropertiesOnce);
    });

    map.on("style.load", () => {
      ensureHoverLayers(map);
      ensureSelectionLayers(map, uiVariantRef.current);
      const selectSource = map.getSource(SELECT_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
      selectSource?.setData(selectedParcelsGeojson);
      const hoverSource = map.getSource(HOVER_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
      hoverSource?.setData(hoverDataRef.current);
      ensureLayerOrder(map);
      console.info("style.load reattached selection and hover layers");
      map.on("idle", logParcelPropertiesOnce);
    });

    map.on("zoom", handleZoom);

    map.on("click", async (e) => {
      setCollateStatus(null);
      try {
        if (suppressNextClickRef.current) {
          suppressNextClickRef.current = false;
          return;
        }

        const identifyResult = await identifyPoint(e.lngLat.lng, e.lngLat.lat);
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

        const shiftMultiSelect =
          uiVariantRef.current === "v2" && isMultiSelectModifier((e as { originalEvent?: unknown })?.originalEvent);
        if (uiVariantRef.current === "v2" && !shiftMultiSelect) {
          setSelectedParcelIds([]);
          setSelectedParcelsGeojson({ type: "FeatureCollection", features: [] });
        }
        applyParcelSelection(parcel, geometry, "feature", { appendSelection: shiftMultiSelect });
      } catch (err) {
        if (disposed) return;
        console.error(err);
        setStatus({ key: "map.status.loadError" });
      }
    });

    let touchTimer: number | null = null;
    let touchStartPoint: { x: number; y: number } | null = null;
    let touchMoved = false;

    const clearTouchTimer = () => {
      if (touchTimer != null) window.clearTimeout(touchTimer);
      touchTimer = null;
      touchStartPoint = null;
      touchMoved = false;
    };

    const handleTouchStart = (e: maplibregl.MapTouchEvent) => {
      if (uiVariantRef.current !== "v2") return;
      const oe = e?.originalEvent as TouchEvent | undefined;
      if (oe?.touches && oe.touches.length !== 1) return;

      touchMoved = false;
      touchStartPoint = { x: e.point.x, y: e.point.y };

      touchTimer = window.setTimeout(async () => {
        if (touchMoved) return;
        suppressNextClickRef.current = true;

        try {
          const identifyResult = await identifyPoint(e.lngLat.lng, e.lngLat.lat);
          if (!identifyResult?.found || !identifyResult.parcel || !isArcgisParcel(identifyResult.parcel)) {
            setStatus({ key: "map.status.notFound" });
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

          applyParcelSelection(parcel, geometry, "feature", { appendSelection: true });
        } catch (err) {
          console.error(err);
          setStatus({ key: "map.status.loadError" });
        } finally {
          window.setTimeout(() => {
            suppressNextClickRef.current = false;
          }, 300);
        }
      }, 480);
    };

    const handleTouchMove = (e: maplibregl.MapTouchEvent) => {
      if (uiVariantRef.current !== "v2") return;
      if (!touchStartPoint) return;
      const dx = e.point.x - touchStartPoint.x;
      const dy = e.point.y - touchStartPoint.y;
      if (Math.sqrt(dx * dx + dy * dy) > 18) {
        touchMoved = true;
        clearTouchTimer();
      }
    };

    const handleTouchEnd = () => {
      clearTouchTimer();
    };

    map.on("touchstart", handleTouchStart);
    map.on("touchmove", handleTouchMove);
    map.on("touchend", handleTouchEnd);
    map.on("touchcancel", handleTouchEnd);
    map.on("dragstart", clearTouchTimer);

    return () => {
      disposed = true;
      map.off("zoom", handleZoom);
      map.off("idle", logParcelPropertiesOnce);
      map.off("idle", reapplyLayerOrder);
      map.off("styledata", reapplyLayerOrder);
      map.off("touchstart", handleTouchStart);
      map.off("touchmove", handleTouchMove);
      map.off("touchend", handleTouchEnd);
      map.off("touchcancel", handleTouchEnd);
      map.off("dragstart", clearTouchTimer);
      disposeHover?.();
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

  useEffect(() => {
    if (!focusTarget) return;
    const map = mapRef.current;
    if (!map) return;
    const safeMax = getSafeMaxZoom(map);
    if (focusTarget.bbox && focusTarget.bbox.length === 4) {
      map.fitBounds(
        [
          [focusTarget.bbox[0], focusTarget.bbox[1]],
          [focusTarget.bbox[2], focusTarget.bbox[3]],
        ],
        { padding: 40, duration: 600, maxZoom: safeMax },
      );
      return;
    }
    map.flyTo({ center: focusTarget.center, zoom: Math.min(16, safeMax), duration: 600 });
  }, [focusTarget]);

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

  const showV2Chip = uiVariant === "v2" && selectedParcelIds.length >= 2;
  const showV2Hint = uiVariant === "v2" && showMultiHint && !showV2Chip;

  return (
    <div>
      <div style={{ position: "relative" }}>
        {showSearchBar ? <MapSearchBar mapRef={mapRef} /> : null}
        <div
          ref={containerRef}
          className={mapContainerClassName}
          style={{
            width: "100%",
            height: mapHeight,
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
        {showV2Hint && (
          <div className="v2-map-hint" role="note">
            <span className="v2-map-hint__text">
              {t("map.controls.multiSelectHint", {
                defaultValue: "Tip: Ctrl+Click (desktop) or Long-press (touch) to multi-select",
              })}
            </span>
            <button
              type="button"
              className="v2-map-hint__dismiss"
              onClick={() => {
                setShowMultiHint(false);
              }}
              aria-label={t("map.controls.dismissHint", { defaultValue: "Dismiss hint" })}
              title={t("map.controls.dismissHint", { defaultValue: "Dismiss" })}
            >
              ×
            </button>
          </div>
        )}
        {showV2Chip && (
          <div className="v2-map-chip" role="status" aria-live="polite">
            <span className="v2-map-chip__count">{t("map.controls.selectedCount", { count: selectedParcelIds.length, defaultValue: `Selected: ${selectedParcelIds.length}` })}</span>
            <button
              type="button"
              className="v2-map-chip__btn v2-map-chip__btn--primary"
              onClick={handleCollate}
              disabled={collating}
            >
              {collating ? t("map.controls.collating", { defaultValue: "Collating…" }) : t("map.controls.collate", { defaultValue: "Collate" })}
            </button>
            <button
              type="button"
              className="v2-map-chip__btn v2-map-chip__btn--ghost"
              onClick={handleClearSelection}
              disabled={collating}
            >
              {t("map.controls.clearSelection", { defaultValue: "Clear" })}
            </button>
          </div>
        )}
      </div>
      {showSelectionUi ? (
        <>
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
        </>
      ) : null}
    </div>
  );
}
