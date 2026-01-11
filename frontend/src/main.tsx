import React, { useState } from "react";
import { createRoot } from "react-dom/client";
import { useTranslation } from "react-i18next";

import Map from "./components/Map";
import ExcelForm from "./components/ExcelForm";
import "./i18n";
import "./index.css";
import type { ParcelSummary } from "./api";
import LanguageSwitcher from "./components/LanguageSwitcher";
import { formatAreaM2 } from "./i18n/format";

function App() {
  const [parcel, setParcel] = useState<ParcelSummary | null>(null);
  const { t } = useTranslation();

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

  return (
    <>
      <header className="app-header">
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
                  | <b>{t("app.landUseLabel")}:</b> {codeLabel}
                </div>
              );
            })()}
            <ExcelForm parcel={parcel} />
          </>
        ) : null}
      </div>
    </>
  );
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
