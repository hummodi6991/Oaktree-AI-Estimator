import React, { useState } from "react";
import { createRoot } from "react-dom/client";

import Map from "./components/Map";
import ExcelForm from "./components/ExcelForm";
import "./index.css";
import type { ParcelSummary } from "./api";

function App() {
  const [parcel, setParcel] = useState<ParcelSummary | null>(null);
  const [landUseOverride, setLandUseOverride] = useState("");

  const effectiveLandUse = parcel
    ? landUseOverride.trim() || parcel.landuse_code || parcel.landuse_raw || parcel.classification_raw || "—"
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
              {effectiveLandUse}
            </div>
            <div style={{ margin: "10px 0", display: "flex", gap: 8, alignItems: "center" }}>
              <label style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
                <span style={{ fontWeight: 600 }}>Override land use (optional):</span>
                <input
                  type="text"
                  value={landUseOverride}
                  onChange={(event) => setLandUseOverride(event.target.value)}
                  placeholder="e.g., Residential"
                  style={{ padding: "6px 10px", minWidth: 220 }}
                />
              </label>
              <span style={{ color: "#475467" }}>
                Leave blank to use the detected classification ({parcel.landuse_code || parcel.landuse_raw || "غير متوفر"}).
              </span>
            </div>
            <ExcelForm parcel={parcel} landUseOverride={landUseOverride.trim() || undefined} />
          </>
        ) : (
          <em>Click a parcel on the map to begin…</em>
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
