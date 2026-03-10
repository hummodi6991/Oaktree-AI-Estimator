import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { useTranslation } from "react-i18next";
import maplibregl from "maplibre-gl";

import Map from "./components/Map";
import ExcelForm from "./components/ExcelForm";
import AccessCodeModal from "./components/AccessCodeModal";
import AdminAnalyticsModal from "./components/AdminAnalyticsModal";
import ExpansionAdvisorPage from "./features/expansion-advisor/ExpansionAdvisorPage";
import type { ExpansionCandidate, ExpansionBrief } from "./lib/api/expansionAdvisor";
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

type AnalysisMode = "feasibility" | "expansion";

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
  const [analysisMode, setAnalysisMode] = useState<AnalysisMode>("expansion");
  const [restaurantHeatmapData, setRestaurantHeatmapData] = useState<GeoJSON.FeatureCollection | null>(null);
  const [restaurantClickedLocation, setRestaurantClickedLocation] = useState<{ lat: number; lng: number } | null>(null);
  const [restaurantHighlightCell, setRestaurantHighlightCell] = useState<{ lng: number; lat: number } | null>(null);
  const [expansionCandidates, setExpansionCandidates] = useState<ExpansionCandidate[]>([]);
  const [expansionSelectedCandidateId, setExpansionSelectedCandidateId] = useState<string | null>(null);
  const [expansionShortlistIds, setExpansionShortlistIds] = useState<string[]>([]);
  const [expansionBranches, setExpansionBranches] = useState<ExpansionBrief["existing_branches"]>([]);
  const mapInstanceRef = useRef<maplibregl.Map | null>(null);

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
    }
  }, [parcel]);

  const handleRestaurantClick = useCallback((lat: number, lng: number) => {
    setRestaurantClickedLocation({ lat, lng });
  }, []);

  const handleExpansionFlyTo = useCallback((lng: number, lat: number) => {
    const map = mapInstanceRef.current;
    if (!map) return;
    map.flyTo({ center: [lng, lat], zoom: 16, duration: 800 });
  }, []);

  const handleRestaurantHighlight = useCallback((lng: number, lat: number) => {
    setRestaurantHighlightCell({ lng, lat });
  }, []);

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
            <>
              {/* Analysis mode toggle */}
              <div
                style={{
                  display: "flex",
                  gap: 0,
                  padding: "8px 24px",
                  background: "var(--ui2-bg, #f5f7f6)",
                  borderBottom: "1px solid var(--ui2-border, #e4e8e6)",
                }}
              >
                <button
                  type="button"
                  className={`oak-btn oak-btn--sm ${analysisMode === "feasibility" ? "oak-btn--primary" : "oak-btn--tertiary"}`}
                  style={{ borderRadius: "var(--oak-radius) 0 0 var(--oak-radius)" }}
                  onClick={() => {
                    setAnalysisMode("feasibility");
                    setRestaurantHeatmapData(null);
                    setRestaurantClickedLocation(null);
                  }}
                >
                  {t("app.modeFeasibility", { defaultValue: "Development Feasibility" })}
                </button>
                <button
                  type="button"
                  className={`oak-btn oak-btn--sm ${analysisMode === "expansion" ? "oak-btn--primary" : "oak-btn--tertiary"}`}
                  style={{ borderRadius: "0 var(--oak-radius) var(--oak-radius) 0" }}
                  onClick={() => setAnalysisMode("expansion")}
                >
                  {t("app.modeExpansion", { defaultValue: "Expansion Advisor" })}
                </button>
              </div>
              <div className={`ui-v2-map-wrap ${isMapHidden ? "ui-v2-map-wrap--hidden" : ""}`}>
                <Map
                  onParcel={(selectedParcel) => {
                    if (analysisMode !== "expansion") {
                      setParcel(selectedParcel);
                    }
                  }}
                  showSearchBar={false}
                  showSelectionUi={false}
                  uiVariant="v2"
                  focusTarget={searchTarget}
                  mapHeight={isMapHidden ? "0px" : "52vh"}
                  mapContainerClassName="ui-v2-map-canvas"
                  restaurantHeatmapData={analysisMode === "expansion" ? restaurantHeatmapData : null}
                  restaurantMode={analysisMode === "expansion"}
                  onRestaurantClick={handleRestaurantClick}
                  highlightCell={restaurantHighlightCell}
                  mapInstanceRef={mapInstanceRef}
                  expansionCandidates={analysisMode === "expansion" ? expansionCandidates : []}
                  selectedExpansionCandidateId={expansionSelectedCandidateId}
                  shortlistExpansionCandidateIds={expansionShortlistIds}
                  existingBranches={expansionBranches}
                  onExpansionCandidateClick={(candidateId) => {
                    const candidate = expansionCandidates.find((item) => item.id === candidateId);
                    if (!candidate) return;
                    setExpansionSelectedCandidateId(candidateId);
                    handleExpansionFlyTo(candidate.lon, candidate.lat);
                  }}
                />
              </div>
            </>
          }
          content={
            analysisMode === "expansion" ? (
              <ExpansionAdvisorPage
                externalSelectedCandidateId={expansionSelectedCandidateId}
                onCandidatesChange={(candidates, shortlistIds, selectedId, branches) => {
                  setExpansionCandidates(candidates);
                  setExpansionShortlistIds(shortlistIds);
                  setExpansionSelectedCandidateId(selectedId);
                  setExpansionBranches(branches);
                }}
                onSelectedCandidateChange={(candidate) => {
                  if (!candidate) return;
                  setExpansionSelectedCandidateId(candidate.id);
                  handleExpansionFlyTo(candidate.lon, candidate.lat);
                  handleRestaurantHighlight(candidate.lon, candidate.lat);
                }}
              />
            ) : parcel ? (
              <AnalysisLayout
                top={<ParcelInfoBar
                  parcel={parcel}
                  landUseLabel={codeLabel}
                  methodLabel={methodLabel}
                  onToggleMap={() => setIsMapHidden((current) => !current)}
                  isMapHidden={isMapHidden}
                />}
                controls={<ExcelForm parcel={parcel} mode="v2" />}
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
