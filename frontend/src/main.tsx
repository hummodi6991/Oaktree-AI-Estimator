import React, { useCallback, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { useTranslation } from "react-i18next";

import Map from "./components/Map";
import ExcelForm from "./components/ExcelForm";
import AccessCodeModal from "./components/AccessCodeModal";
import AdminAnalyticsModal from "./components/AdminAnalyticsModal";
import "./i18n";
import "./App.css";
import "./index.css";
import type { ParcelSummary } from "./api";
import { getAdminUsageSummary } from "./api";
import LanguageSwitcher from "./components/LanguageSwitcher";
import { formatAreaM2 } from "./i18n/format";

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

  return (
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
      <AccessCodeModal isOpen={!hasApiKey} onSubmit={handleAccessCodeSubmit} />
      <AdminAnalyticsModal isOpen={isAdminModalOpen} onClose={() => setIsAdminModalOpen(false)} />
    </>
  );
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
