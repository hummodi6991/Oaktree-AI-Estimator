import React, { useState } from "react";
import { createRoot } from "react-dom/client";

import Map from "./components/Map";
import ExcelForm from "./components/ExcelForm";
import "./index.css";
import type { ParcelSummary } from "./api";

function App() {
  const [parcel, setParcel] = useState<ParcelSummary | null>(null);
  const [landUseOverride, setLandUseOverride] = useState("");

  const selectedLandUse = parcel
    ? landUseOverride.trim() || parcel.landuse_code || ""
    : "";
  const codeLabel = selectedLandUse === "s"
    ? "s — Residential"
    : selectedLandUse === "m"
      ? "m — Mixed/Commercial"
      : "—";

  return (
    <>
      <Map
        onParcel={(selectedParcel) => {
          setParcel(selectedParcel);
          setLandUseOverride("");
        }}
      />
      <div style={{ padding: 12 }}>
        {parcel ? (
          <>
            <div>
              <b>Parcel:</b> {parcel.parcel_id} | <b>Area:</b> {parcel.area_m2?.toFixed(0)} m² | <b>Land-use:</b>{" "}
              {codeLabel}
            </div>
            <div style={{ margin: "10px 0", display: "flex", gap: 8, alignItems: "center" }}>
              <label style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
                <span style={{ fontWeight: 600 }}>Override land use (optional):</span>
                <select
                  value={selectedLandUse}
                  onChange={(event) => setLandUseOverride(event.target.value)}
                  style={{ padding: "6px 10px", minWidth: 220 }}
                >
                  <option value="">— choose —</option>
                  <option value="s">s — Residential</option>
                  <option value="m">m — Mixed</option>
                </select>
              </label>
              <span style={{ color: "#475467" }}>
                Defaults to detected code; pick s or m to force a template.
              </span>
            </div>
            <ExcelForm parcel={parcel} landUseOverride={landUseOverride.trim() || undefined} />
          </>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 6, color: "var(--sand-200)" }}>
            <em>Click a parcel on the map to begin…</em>
            <span style={{ fontSize: "0.95rem", color: "rgba(247, 241, 230, 0.85)" }}>
              <strong>Tip:</strong> Some parcel outlines appear only when you zoom in. If parcels look missing, zoom closer to reveal the blue parcel boundaries.
            </span>
          </div>
        )}
      </div>
    </>
  );
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
