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
  const needsOverride = parcel ? !parcel.landuse_code : false;

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
              <b>Parcel:</b> {parcel.parcel_id} | <b>Area:</b> {Math.round(parcel.area_m2 || 0).toLocaleString()} m² | <b>Land-use:</b>{" "}
              {effectiveLandUse}
            </div>
            {needsOverride && (
              <div style={{ margin: "10px 0", display: "flex", gap: 8, alignItems: "center" }}>
                <label style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
                  <span style={{ fontWeight: 600 }}>Override land use (optional):</span>
                  <select
                    value={landUseOverride}
                    onChange={(event) => setLandUseOverride(event.target.value)}
                    style={{ padding: "6px 10px", minWidth: 220 }}
                  >
                    <option value="">— اختر —</option>
                    <option value="house">House</option>
                    <option value="residential">Residential</option>
                    <option value="commercial">Commercial</option>
                    <option value="retail">Retail</option>
                    <option value="industrial">Industrial</option>
                    <option value="public">Public</option>
                    <option value="religious">Religious</option>
                  </select>
                </label>
                <span style={{ color: "#475467" }}>
                  Optional if land-use is missing; otherwise detected values are used.
                </span>
              </div>
            )}
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
