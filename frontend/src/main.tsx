import React, { useCallback, useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import { useTranslation } from "react-i18next";
import type { Geometry } from "geojson";

import Map from "./components/Map";
import ExcelForm from "./components/ExcelForm";
import AccessCodeModal from "./components/AccessCodeModal";
import AdminAnalyticsModal from "./components/AdminAnalyticsModal";
import RestaurantLocationPanel from "./components/RestaurantLocationPanel";
import "./i18n";
import "./App.css";
import "./index.css";
import "./ui-fixes.css";
import "./styles/figma-tokens.css";
import "./styles/theme.css";
import "./styles/global.css";
import "./styles/design-system.css";
import type { ParcelSummary } from "./api";
import { getAdminUsageSummary } from "./api";
import LanguageSwitcher from "./components/LanguageSwitcher";
import { formatAreaM2 } from "./i18n/format";
import DesignTokenPreview from "./dev/DesignTokenPreview";
import AppShell from "./ui-v2/AppShell";
import HeaderBar from "./ui-v2/HeaderBar";
import EmptyState from "./ui-v2/EmptyState";
import ParcelInfoBar from "./ui-v2/ParcelInfoBar";
import AnalysisLayout from "./ui-v2/AnalysisLayout";
import type { SearchItem } from "./types/search";
import "./styles/ui-v2.css";
import "./styles/ui-figma.css";
import "./styles/atlas-ui.css";
import i18n from "i18next";

function centroidFromGeometry(geometry?: Geometry | null): [number, number] | null {
  if (!geometry) return null;
  if (geometry.type === "Point") return geometry.coordinates as [number, number];
  if (geometry.type === "Polygon") {
    const ring = geometry.coordinates?.[0];
    if (!ring || ring.length < 3) return null;
    let crossSum = 0, cxSum = 0, cySum = 0;
    for (let i = 0; i < ring.length; i++) {
      const [x0, y0] = ring[i];
      const [x1, y1] = ring[(i + 1) % ring.length];
      const cross = x0 * y1 - x1 * y0;
      crossSum += cross;
      cxSum += (x0 + x1) * cross;
      cySum += (y0 + y1) * cross;
    }
    if (!crossSum) return null;
    return [cxSum / (3 * crossSum), cySum / (3 * crossSum)];
  }
  if (geometry.type === "MultiPolygon") {
    const coords = geometry.coordinates as number[][][][];
    let totalArea = 0, cx = 0, cy = 0;
    for (const poly of coords) {
      const ring = poly[0];
      if (!ring || ring.length < 3) continue;
      let crossSum = 0, cxS = 0, cyS = 0;
      for (let i = 0; i < ring.length; i++) {
        const [x0, y0] = ring[i];
        const [x1, y1] = ring[(i + 1) % ring.length];
        const cross = x0 * y1 - x1 * y0;
        crossSum += cross;
        cxS += (x0 + x1) * cross;
        cyS += (y0 + y1) * cross;
      }
      if (!crossSum) continue;
      const area = Math.abs(crossSum) / 2;
      totalArea += area;
      cx += (cxS / (3 * crossSum)) * area;
      cy += (cyS / (3 * crossSum)) * area;
    }
    if (!totalArea) return null;
    return [cx / totalArea, cy / totalArea];
  }
  return null;
}

function applyLocaleAttrs() {
  const lng = i18n.language || "en";
  document.documentElement.lang = lng;
  document.documentElement.dir = lng === "ar" ? "rtl" : "ltr";
}

applyLocaleAttrs();
i18n.on("languageChanged", applyLocaleAttrs);

function App() {
  const [parcel, setParcel] = useState<ParcelSummary | null>(null);
  const [apiKey, setApiKey] = useState(() => {
    if (typeof window === "undefined") return "";
    return window.localStorage.getItem("oaktree_api_key") ?? "";
  });
  const [isAdmin, setIsAdmin] = useState(false);
  const [isAdminModalOpen, setIsAdminModalOpen] = useState(false);
  const hasApiKey = Boolean(apiKey);
  const { t } = useTranslation();
  const [searchTarget, setSearchTarget] = useState<SearchItem | null>(null);
  const [isMapHidden, setIsMapHidden] = useState(false);
  const [restaurantHeatmapData, setRestaurantHeatmapData] = useState<GeoJSON.FeatureCollection | null>(null);

  const parcelCentroid = useMemo(() => centroidFromGeometry(parcel?.geometry as Geometry | null), [parcel]);
  const parcelLon = parcelCentroid?.[0] ?? null;
  const parcelLat = parcelCentroid?.[1] ?? null;

  const uiV2 = useMemo(() => {
    const params = new URLSearchParams(window.location.search);
    const uiParam = (params.get("ui") || "").toLowerCase();

    if (uiParam === "legacy") return false;
    if (uiParam === "v2") return true;

    if (import.meta.env.VITE_UI_V2 === "0") return false;
    if (import.meta.env.VITE_UI_V2 === "1") return true;

    return true;
  }, []);

  const extractStatus = (error: unknown): number | null => {
    if (error instanceof Error) {
      const match = error.message.match(/^(\d{3})\\s/);
      if (match) return Number(match[1]);
    }
    return null;
  };

  const formatLanduseMethod = (method?: string | null): string => {
    switch (method) {
      case "parcel_label":
        return "ArcGIS parcel label";
      case "suhail_overlay":
        return "Suhail zoning";
      case "osm_overlay":
        return "OSM overlay";
      default:
        return "—";
    }
  };

  const codeLabel = (() => {
    const selectedLandUse = parcel?.landuse_code?.trim().toLowerCase();
    if (selectedLandUse === "s") {
      return t("app.landUseCodeLabel", {
        code: "s",
        label: t("app.landUse.residential"),
      });
    }
    if (selectedLandUse === "m") {
      return t("app.landUseCodeLabel", {
        code: "m",
        label: t("app.landUse.mixed"),
      });
    }
    return t("common.notAvailable");
  })();

  const methodLabel = formatLanduseMethod(parcel?.landuse_method);

  useEffect(() => {
    function handleStorage(event: StorageEvent) {
      if (event.key === "oaktree_api_key") {
        setApiKey(event.newValue ?? "");
        if (!event.newValue) {
          setParcel(null);
        }
      }
    }
    window.addEventListener("storage", handleStorage);
    return () => window.removeEventListener("storage", handleStorage);
  }, []);

  useEffect(() => {
    if (!apiKey) {
      setIsAdmin(false);
      return;
    }
    let isActive = true;
    setIsAdmin(false);
    const checkAdmin = async () => {
      try {
        await getAdminUsageSummary();
        if (!isActive) return;
        setIsAdmin(true);
      } catch (error) {
        if (!isActive) return;
        const status = extractStatus(error);
        if (status === 401 || status === 403) {
          setIsAdmin(false);
          return;
        }
        setIsAdmin(false);
      }
    };
    void checkAdmin();
    return () => {
      isActive = false;
    };
  }, [apiKey]);

  useEffect(() => {
    setIsAdminModalOpen(false);
  }, [apiKey]);

  useEffect(() => {
    if (!isAdmin) {
      setIsAdminModalOpen(false);
    }
  }, [isAdmin]);

  useEffect(() => {
    if (!parcel) {
      setIsMapHidden(false);
      setRestaurantHeatmapData(null);
    }
  }, [parcel]);

  const handleAccessCodeSubmit = useCallback((code: string) => {
    window.localStorage.setItem("oaktree_api_key", code);
    setApiKey(code);
    setIsAdmin(false);
    setIsAdminModalOpen(false);
  }, []);

  const handleAccessCodeClear = useCallback(() => {
    window.localStorage.removeItem("oaktree_api_key");
    setApiKey("");
    setIsAdmin(false);
    setIsAdminModalOpen(false);
    setParcel(null);
  }, []);

  const legacyContent = (
    <>
      <header className="app-header">
        <div className="app-header__actions">
          {hasApiKey && (
            <button type="button" className="tertiary-button access-code-control" onClick={handleAccessCodeClear}>
              Change access code
            </button>
          )}
          {isAdmin && (
            <button type="button" className="tertiary-button access-code-control" onClick={() => setIsAdminModalOpen(true)}>
              Admin Analytics
            </button>
          )}
        </div>
        <div className="app-header__spacer" />
        <LanguageSwitcher />
      </header>
      <Map
        onParcel={(selectedParcel) => {
          setParcel(selectedParcel);
        }}
      />
      <div style={{ padding: 12 }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 6, color: "var(--sand-200)", marginBottom: 10 }}>
          {!parcel && <em>{t("app.clickToStart")}</em>}
          <span style={{ fontSize: "0.95rem", color: "rgba(247, 241, 230, 0.85)" }}>
            <strong>{t("app.tipLabel")}</strong> {t("app.tipMessage")}
          </span>
        </div>
        {parcel ? (
          <>
            {(() => {
              const primaryArea = parcel.parcel_area_m2 ?? parcel.area_m2 ?? null;
              const footprintArea = parcel.footprint_area_m2 ?? null;
              const showFootprint =
                footprintArea != null && Number.isFinite(footprintArea) && parcel.parcel_area_m2 != null;
              const methodLabel = formatLanduseMethod(parcel.landuse_method);
              return (
                <div>
                  <b>{t("app.parcelLabel")}:</b> {parcel.parcel_id} | <b>{t("app.areaLabel")}:</b>{" "}
                  {formatAreaM2(primaryArea, { maximumFractionDigits: 0 }, t("common.notAvailable"))}
                  {showFootprint
                    ? ` (${t("app.footprintLabel")}: ${formatAreaM2(
                        footprintArea,
                        { maximumFractionDigits: 0 },
                        t("common.notAvailable"),
                      )})`
                    : ""}{" "}
                  | <b>{t("app.landUseLabel")}:</b> {codeLabel} | <b>{t("app.methodLabel")}:</b>{" "}
                  {methodLabel}
                </div>
              );
            })()}
            <ExcelForm parcel={parcel} />
            <RestaurantLocationPanel
              lat={parcelLat}
              lon={parcelLon}
              onHeatmapData={setRestaurantHeatmapData}
            />
          </>
        ) : null}
      </div>
    </>
  );

  return (
    <>
      {uiV2 ? (
        <AppShell
          header={<HeaderBar onSearchSelect={(item) => setSearchTarget(item)} />}
          map={
            <div className={`ui-v2-map-wrap ${isMapHidden ? "ui-v2-map-wrap--hidden" : ""}`}>
              <Map
                onParcel={(selectedParcel) => {
                  setParcel(selectedParcel);
                }}
                showSearchBar={false}
                showSelectionUi={false}
                uiVariant="v2"
                focusTarget={searchTarget}
                mapHeight={isMapHidden ? "0px" : "52vh"}
                mapContainerClassName="ui-v2-map-canvas"
                restaurantHeatmapData={restaurantHeatmapData}
              />
            </div>
          }
          content={
            parcel ? (
              <AnalysisLayout
                top={<ParcelInfoBar
                  parcel={parcel}
                  landUseLabel={codeLabel}
                  methodLabel={methodLabel}
                  onToggleMap={() => setIsMapHidden((current) => !current)}
                  isMapHidden={isMapHidden}
                />}
                controls={<ExcelForm parcel={parcel} mode="v2" />}
                extra={
                  <RestaurantLocationPanel
                    lat={parcelLat}
                    lon={parcelLon}
                    onHeatmapData={setRestaurantHeatmapData}
                  />
                }
              />
            ) : (
              <EmptyState />
            )
          }
        />
      ) : (
        legacyContent
      )}
      <AccessCodeModal isOpen={!hasApiKey} onSubmit={handleAccessCodeSubmit} />
      <AdminAnalyticsModal isOpen={isAdminModalOpen} onClose={() => setIsAdminModalOpen(false)} />
      {import.meta.env.DEV ? <DesignTokenPreview /> : null}
    </>
  );
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
